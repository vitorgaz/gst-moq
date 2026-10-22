//! moqsrc -- subscribe to a Media-over-QUIC track and push buffers using the
//! libmoq service tier (moq_endpoint_t + moq_media_receiver_t).
//!
//! The service tier owns the network thread, the catalog-track subscription,
//! MSF catalog parsing, per-track auto-subscription, and a bounded object
//! queue. This element is a thin adapter: start() opens a WebTransport
//! endpoint to a MoQ relay and attaches a media receiver for the configured
//! namespace; create() drains track-discovery events (TRACK_ADDED /
//! CATALOG_READY) and media objects directly from the receiver and turns
//! each object into a GstBuffer. No transport pump, no manual subscriber,
//! no LOC parsing in this element -- the receiver delivers parsed timing and
//! keyframe flags.
//!
//! Copy semantics: a polled object owns its backing buffers until it is
//! dropped, which must happen on the polling thread (MediaObject is not
//! Send). We COPY the payload into a GstBuffer and drop the object at once,
//! so no libmoq rcbuf is ever freed on an arbitrary downstream thread.
//!
//! Topology: the service endpoint is a CLIENT. moqsrc and moqsink both dial
//! a relay (e.g. moqx over WebTransport) rather than each other.
//!
//! Timestamps: LOC presentation times are publisher-relative (moqsink rebases
//! them to its first frame, other publishers may use wall-clock epochs), so
//! they are meaningless on this pipeline's clock. The first delivered object
//! is pinned to the pipeline's running time at arrival and every later PTS
//! keeps its offset from that anchor, which preserves the publisher's frame
//! cadence. The "latency" property is reported through the LATENCY query so
//! synced sinks absorb network jitter instead of dropping late frames.
//!
//! Interrupt: unlock() latches the endpoint interrupt so a blocked poll/wait
//! returns immediately; unlock_stop() clears it.

use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;
use gst_base::prelude::*;
use gst_base::subclass::base_src::CreateSuccess;
use gst_base::subclass::prelude::*;

use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::Duration;

use moq5::sys::{MOQ_DONE, MOQ_ERR_CLOSED, MOQ_ERR_INTERRUPTED, MOQ_OK};
use moq5::{Error, Namespace, Packaging, TerminalReason, TrackEventKind};

use crate::service::{Desc, Endpoint, Receiver, ReceiverCfg, Track};

use crate::clock;
use crate::moq::{endpoint_config, relay_url};

pub(crate) static CAT: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
    gst::DebugCategory::new("moqsrc", gst::DebugColorFlags::empty(), Some("MoQ source"))
});

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: u32 = 4443;
const DEFAULT_RELAY_PATH: &str = "/moq-relay";
const DEFAULT_NAMESPACE: &str = "example";
const DEFAULT_TRACK_NAME: &str = "video";
const WAIT_TIMEOUT_US: u64 = 100 * 1000;
const DEFAULT_LATENCY_MS: u32 = 200;
/// browsers (playa) default to draft-16; 0 = offer all
const DEFAULT_DRAFT: u32 = 16;
const DEFAULT_DISCOVERY_TIMEOUT_MS: u32 = 10000;

#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub(crate) host: String,
    pub(crate) port: u32,
    pub(crate) relay_path: String,
    pub(crate) insecure: bool,
    pub(crate) namespace: String,
    pub(crate) track_name: String,
    pub(crate) caps: Option<gst::Caps>,
    pub(crate) latency_ms: u32,
    /// MoQT draft to negotiate, 0 = auto
    pub(crate) draft: u32,
    pub(crate) discovery_timeout_ms: u32,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            host: DEFAULT_HOST.into(),
            port: DEFAULT_PORT,
            relay_path: DEFAULT_RELAY_PATH.into(),
            insecure: false,
            namespace: DEFAULT_NAMESPACE.into(),
            track_name: DEFAULT_TRACK_NAME.into(),
            caps: None,
            latency_ms: DEFAULT_LATENCY_MS,
            draft: DEFAULT_DRAFT,
            discovery_timeout_ms: DEFAULT_DISCOVERY_TIMEOUT_MS,
        }
    }
}

#[derive(Default)]
pub(crate) struct State {
    /// holds the endpoint too, so it is destroyed before the endpoint stops
    pub(crate) receiver: Option<Receiver>,
    /// handle matching track-name, if any
    pub(crate) want_track: Option<Track>,
    pub(crate) want_track_ended: bool,
    pub(crate) selection_failed: bool,
    pub(crate) started: bool,
    pub(crate) caps_pushed: bool,
    pub(crate) catalog_ready: bool,

    pub(crate) objects_recv: u64,

    /// the wanted track's CMAF init segment, copied from the catalog
    pub(crate) want_init: Vec<u8>,
    pub(crate) init_pushed: bool,
    /// fragment held back behind the init
    pub(crate) pending: Option<gst::Buffer>,
    /// names announced, for error text; None outside start..stop
    pub(crate) seen_tracks: Option<String>,
    pub(crate) discover_deadline: i64,
    /// caps inferred from the catalog when the "caps" property is unset;
    /// never written into the property
    pub(crate) derived_caps: Option<gst::Caps>,
    /// latched CORE/NEGOTIATION error
    pub(crate) negotiation_failed: bool,
    /// wanted track announced at least once; suppresses discovery-timeout
    /// after a later TRACK_REMOVED
    pub(crate) track_ever_seen: bool,

    /// timestamp anchor: first object's LOC time -> pipeline running time
    pub(crate) have_anchor: bool,
    pub(crate) anchor_loc_us: u64,
    pub(crate) anchor_running: gst::ClockTime,
}

#[derive(Default)]
pub struct MoqSrc {
    pub(crate) settings: Mutex<Settings>,
    pub(crate) state: Mutex<State>,
    /// The endpoint, outside `state` so unlock() can interrupt a create()
    /// that is blocked in a receiver wait while holding the state lock.
    pub(crate) ep: Mutex<Option<Endpoint>>,
}

fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

impl MoqSrc {
    fn endpoint(&self) -> Option<Endpoint> {
        self.ep.lock().unwrap().clone()
    }

    pub(crate) fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    fn log_terminal(&self, ep: &Endpoint) {
        match ep.terminal() {
            Err(e) => gst::info!(
                CAT,
                imp = self,
                "endpoint terminal unavailable getter_rc={}",
                e.code()
            ),
            Ok(t) if t.reason == TerminalReason::None => gst::info!(
                CAT,
                imp = self,
                "endpoint terminal unclassified reason=0 detail={}",
                t.detail_code
            ),
            Ok(t) => gst::info!(
                CAT,
                imp = self,
                "endpoint terminal reason={} detail={}",
                t.reason.code(),
                t.detail_code
            ),
        }
    }

    /// Map a LOC presentation time onto this pipeline's running time.
    fn map_pts(&self, st: &mut State, loc_us: u64) -> gst::ClockTime {
        if !st.have_anchor {
            let obj = self.obj();
            let mut now = None;
            if let Some(clock) = obj.clock() {
                if let Some(base) = obj.base_time() {
                    let abs = clock.time();
                    if abs >= base {
                        now = Some(abs - base);
                    }
                }
            }
            st.anchor_running = now.unwrap_or(gst::ClockTime::ZERO);
            st.anchor_loc_us = loc_us;
            st.have_anchor = true;
            gst::info!(
                CAT,
                imp = self,
                "anchored LOC time {}us at running time {}",
                loc_us,
                st.anchor_running
            );
        }
        if loc_us >= st.anchor_loc_us {
            return st.anchor_running + gst::ClockTime::from_useconds(loc_us - st.anchor_loc_us);
        }
        // Earlier than the anchor (reordered / late object): clamp.
        let back = gst::ClockTime::from_useconds(st.anchor_loc_us - loc_us);
        st.anchor_running
            .checked_sub(back)
            .unwrap_or(gst::ClockTime::ZERO)
    }

    /// Drain pending track-discovery events. Logs catalog discovery and binds
    /// want_track to the handle whose catalog name matches track-name.
    pub(crate) fn drain_track_events(&self, st: &mut State) {
        let (track_name, caps) = {
            let s = self.settings.lock().unwrap();
            (s.track_name.clone(), s.caps.clone())
        };
        let Some(receiver) = st.receiver.as_ref() else {
            return;
        };
        while let Ok(Some(ev)) = receiver.poll_track() {
            match ev.kind() {
                TrackEventKind::Added => {
                    let d = ev.description().ok().flatten();
                    gst::info!(
                        CAT,
                        imp = self,
                        "TRACK_ADDED name={} codec={} packaging={}",
                        d.as_ref().map(|d| lossy(d.name())).unwrap_or_default(),
                        d.as_ref().map(|d| lossy(d.codec())).unwrap_or_default(),
                        if d.as_ref().is_some_and(|d| d.packaging() == Some(Packaging::Cmaf)) {
                            "cmaf"
                        } else {
                            "loc"
                        }
                    );
                    let Some(d) = d else { continue };
                    if st.want_track.is_none() {
                        if let Some(seen) = st.seen_tracks.as_mut() {
                            if !seen.is_empty() {
                                seen.push_str(", ");
                            }
                            seen.push_str(&lossy(d.name()));
                        }
                    }
                    if !d.has_name()
                        || st.want_track.is_some()
                        || d.name() != track_name.as_bytes()
                    {
                        continue;
                    }
                    let subscribed = match ev.track() {
                        Some(t) => receiver.subscribe_track(t, None).map(|()| t),
                        None => Err(Error::INVAL),
                    };
                    let track = match subscribed {
                        Ok(t) => t,
                        Err(e) => {
                            gst::element_imp_error!(
                                self,
                                gst::ResourceError::Read,
                                (
                                    "could not subscribe to track \"{}\" (rc={})",
                                    track_name,
                                    e.code()
                                )
                            );
                            st.selection_failed = true;
                            continue;
                        }
                    };
                    gst::info!(
                        CAT,
                        imp = self,
                        "selected endpoint negotiated draft={}",
                        receiver.negotiated_draft()
                    );
                    st.want_track = Some(track);
                    st.want_track_ended = false;
                    st.track_ever_seen = true;
                    st.want_init = d.init_data().to_vec();
                    st.init_pushed = false;
                    if d.packaging() == Some(Packaging::Raw) && d.codec().starts_with(b"mp4a") {
                        st.want_init.clear();
                    }
                    if caps.as_ref().is_none_or(|c| c.is_any()) {
                        let c = if d.packaging() == Some(Packaging::Cmaf) {
                            Some(
                                gst::Caps::builder("video/quicktime")
                                    .field("variant", "iso")
                                    .build(),
                            )
                        } else if d.codec().starts_with(b"avc1") || d.codec().starts_with(b"avc3") {
                            Some(
                                gst::Caps::builder("video/x-h264")
                                    .field("stream-format", "byte-stream")
                                    .field("alignment", "au")
                                    .build(),
                            )
                        } else if d.codec().starts_with(b"mp4a") {
                            // The catalog init is the codec_data, not a media
                            // object: pushing it downstream would look like an
                            // AAC frame.
                            st.want_init.clear();
                            Some(Self::loc_aac_caps(d))
                        } else {
                            gst::element_imp_error!(
                                self,
                                gst::CoreError::Negotiation,
                                (
                                    "track \"{}\" is a LOC track with codec {}; set the \"caps\" property for it",
                                    track_name,
                                    lossy(d.codec())
                                )
                            );
                            st.negotiation_failed = true;
                            None
                        };
                        if let Some(c) = c {
                            st.derived_caps = Some(c);
                        }
                    }
                }
                TrackEventKind::Ended | TrackEventKind::Removed => {
                    if ev.track().is_some() && ev.track() == st.want_track {
                        st.want_track_ended = true;
                        gst::info!(
                            CAT,
                            imp = self,
                            "selected-track terminal event kind={} name={}",
                            ev.raw_kind(),
                            track_name
                        );
                        // Handles and descriptors remain stable until receiver
                        // teardown. Preserve CMAF init and pending/queued tails
                        // after removal too.
                    }
                }
                TrackEventKind::CatalogReady => {
                    st.catalog_ready = true;
                    gst::info!(CAT, imp = self, "CATALOG_READY");
                }
                _ => {}
            }
        }
    }

    /// LOC audio: raw AAC access units. The decoder config travels in the
    /// catalog, so it becomes codec_data rather than a pushed init segment --
    /// unlike CMAF, where qtdemux reads it from the moov.
    fn loc_aac_caps(d: Desc<'_>) -> gst::Caps {
        let mut b = gst::Caps::builder("audio/mpeg")
            .field("mpegversion", 4i32)
            .field("stream-format", "raw");
        if let Some(rate) = d.samplerate().filter(|&r| r != 0) {
            b = b.field("rate", rate as i32);
        }
        // channelConfig is a catalog span, not a C string, so it is parsed
        // bounded rather than with atoi().
        let mut ch: i32 = 0;
        for &ci in d.channel_config() {
            if ch >= 256 {
                break;
            }
            if !ci.is_ascii_digit() {
                ch = 0;
                break;
            }
            ch = ch * 10 + (ci - b'0') as i32;
        }
        if ch > 0 {
            b = b.field("channels", ch);
        }
        // Prefer the parsed decoder extradata; fall back to the raw initData
        // the publisher sent (they are the same ASC here).
        let config = d.codec_config();
        let asc = if config.is_empty() {
            d.init_data()
        } else {
            config
        };
        if !asc.is_empty() {
            b = b.field("codec_data", gst::Buffer::from_slice(asc.to_vec()));
        }
        b.build()
    }

    pub(crate) fn create_buffer(&self) -> Result<gst::Buffer, gst::FlowError> {
        let mut st = self.lock_state();
        let st = &mut *st;

        if let Some(pending) = st.pending.take() {
            // fragment queued behind the init
            return Ok(pending);
        }

        let (track_name, caps, discovery_timeout_ms) = {
            let s = self.settings.lock().unwrap();
            (s.track_name.clone(), s.caps.clone(), s.discovery_timeout_ms)
        };

        loop {
            self.drain_track_events(st);

            if st.negotiation_failed || st.selection_failed {
                return Err(gst::FlowError::Error);
            }

            if !st.caps_pushed {
                let effective = match &caps {
                    Some(c) if !c.is_any() => Some(c.clone()),
                    _ => st.derived_caps.clone(),
                };
                if let Some(effective) = effective {
                    let _ = self.obj().set_caps(&effective);
                    st.caps_pushed = true;
                }
            }

            let Some(receiver) = st.receiver.as_ref() else {
                return Err(gst::FlowError::Flushing);
            };
            let polled = receiver.poll_object();
            let rc = match &polled {
                Ok(Some(_)) => MOQ_OK,
                Ok(None) => MOQ_DONE, // nothing queued now
                Err(e) => e.code(),
            };

            if rc == MOQ_ERR_INTERRUPTED {
                return Err(gst::FlowError::Flushing);
            }

            if st.want_track.is_none()
                && !st.track_ever_seen
                && (rc == MOQ_OK || rc == MOQ_DONE)
                && clock::monotonic_us() >= st.discover_deadline
            {
                drop(polled);
                let seen = st
                    .seen_tracks
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("none");
                gst::element_imp_error!(
                    self,
                    gst::ResourceError::NotFound,
                    (
                        "track \"{}\" was not announced within {} ms (catalog tracks: {})",
                        track_name,
                        discovery_timeout_ms,
                        seen
                    )
                );
                return Err(gst::FlowError::Error);
            }

            if let Ok(Some(obj)) = polled {
                // Every object's track is announced before it is pollable, so
                // a missing want_track here means the requested track-name has
                // not matched anything yet -- drop until it resolves (or the
                // discovery deadline fails the pipeline) rather than guessing
                // at an unrelated track.
                if st.want_track.is_none() || obj.track() != st.want_track {
                    continue;
                }
                if !st.caps_pushed {
                    // object before caps: not usable
                    continue;
                }

                if obj.packaging() == Some(Packaging::Cmaf) {
                    if obj.fragment().is_empty() {
                        gst::warning!(CAT, imp = self, "CMAF object with empty fragment, skipping");
                        continue;
                    }
                    let mut buf = gst::Buffer::from_slice(obj.fragment().to_vec());
                    if !obj.is_keyframe() {
                        buf.get_mut()
                            .unwrap()
                            .set_flags(gst::BufferFlags::DELTA_UNIT);
                    }
                    st.objects_recv += 1;
                    gst::log!(
                        CAT,
                        imp = self,
                        "received CMAF {} fragment ({} B)",
                        if obj.is_keyframe() { "sync" } else { "delta" },
                        obj.fragment().len()
                    );
                    drop(obj);

                    if !st.init_pushed {
                        if st.want_init.is_empty() {
                            gst::element_imp_error!(
                                self,
                                gst::StreamError::Format,
                                (
                                    "CMAF track \"{}\" has no init segment in the catalog",
                                    track_name
                                )
                            );
                            return Err(gst::FlowError::Error);
                        }
                        let mut init = gst::Buffer::from_slice(st.want_init.clone());
                        init.get_mut().unwrap().set_flags(gst::BufferFlags::HEADER);
                        st.init_pushed = true;
                        st.pending = Some(buf); // delivered on the next create()
                        gst::info!(
                            CAT,
                            imp = self,
                            "pushing CMAF init segment ({} B)",
                            st.want_init.len()
                        );
                        return Ok(init);
                    }
                    return Ok(buf);
                }

                // LOC path
                if obj.payload().is_empty() {
                    continue;
                }
                let mut buf = gst::Buffer::from_slice(obj.payload().to_vec());
                {
                    let pts = self.map_pts(st, obj.presentation_time_us());
                    let b = buf.get_mut().unwrap();
                    b.set_pts(pts);
                    if !obj.is_keyframe() {
                        b.set_flags(gst::BufferFlags::DELTA_UNIT);
                    }
                }

                st.objects_recv += 1;
                gst::log!(
                    CAT,
                    imp = self,
                    "received {} pts={}us ({} B)",
                    if obj.is_keyframe() {
                        "keyframe"
                    } else {
                        "delta"
                    },
                    obj.presentation_time_us(),
                    obj.payload().len()
                );
                return Ok(buf);
            }

            let receiver = st.receiver.as_ref().expect("checked above");
            let fatal = receiver.is_fatal();
            if rc == MOQ_ERR_CLOSED || fatal {
                if let Some(ep) = self.endpoint() {
                    self.log_terminal(&ep);
                }
            }

            if fatal {
                gst::element_imp_error!(
                    self,
                    gst::ResourceError::Read,
                    ("media receiver failed (code={})", receiver.fatal_code())
                );
                return Err(gst::FlowError::Error);
            }

            if rc == MOQ_ERR_CLOSED {
                gst::info!(CAT, imp = self, "endpoint closed; sending EOS");
                return Err(gst::FlowError::Eos);
            }

            if rc == MOQ_DONE && st.want_track_ended {
                gst::info!(
                    CAT,
                    imp = self,
                    "selected-track queue drained; sending EOS name={}",
                    track_name
                );
                return Err(gst::FlowError::Eos);
            }

            // Nothing queued. WaitResult for the next wakeup.
            if receiver.wait(Duration::from_micros(WAIT_TIMEOUT_US)) == Err(Error::INTERRUPTED) {
                return Err(gst::FlowError::Flushing);
            }
            // Ready (woke) / TimedOut / CLOSED: loop; the next poll classifies
            // terminal vs more data deterministically.
        }
    }

    fn clear_session(st: &mut State) {
        st.pending = None;
        st.seen_tracks = None;
        st.derived_caps = None;
    }
}

#[glib::object_subclass]
impl ObjectSubclass for MoqSrc {
    const NAME: &'static str = "GstMoqSrc";
    type Type = super::MoqSrc;
    type ParentType = gst_base::PushSrc;
}

impl ObjectImpl for MoqSrc {
    fn properties() -> &'static [glib::ParamSpec] {
        static PROPS: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| {
            vec![
                glib::ParamSpecString::builder("host")
                    .nick("Host")
                    .blurb("MoQ relay host to connect to")
                    .default_value(Some(DEFAULT_HOST))
                    .build(),
                glib::ParamSpecUInt::builder("port")
                    .nick("Port")
                    .blurb("MoQ relay UDP/QUIC port")
                    .minimum(1)
                    .maximum(65535)
                    .default_value(DEFAULT_PORT)
                    .build(),
                glib::ParamSpecString::builder("relay-path")
                    .nick("Relay path")
                    .blurb("WebTransport endpoint path on the relay")
                    .default_value(Some(DEFAULT_RELAY_PATH))
                    .build(),
                glib::ParamSpecBoolean::builder("insecure")
                    .nick("Insecure")
                    .blurb("Skip TLS verification (demo/test only)")
                    .default_value(false)
                    .build(),
                glib::ParamSpecString::builder("namespace")
                    .nick("Track namespace")
                    .blurb("Slash-separated MoQ track namespace")
                    .default_value(Some(DEFAULT_NAMESPACE))
                    .build(),
                glib::ParamSpecString::builder("track-name")
                    .nick("Track name")
                    .blurb("MoQ track name to deliver (matched against the catalog)")
                    .default_value(Some(DEFAULT_TRACK_NAME))
                    .build(),
                glib::ParamSpecBoxed::builder::<gst::Caps>("caps")
                    .nick("Caps")
                    .blurb(
                        "Output caps describing the subscribed track's media (optional; \
                         derived from the catalog when unset)",
                    )
                    .build(),
                glib::ParamSpecUInt::builder("latency")
                    .nick("Latency")
                    .blurb("Latency reported to the pipeline (ms) to absorb network jitter")
                    .minimum(0)
                    .maximum(10000)
                    .default_value(DEFAULT_LATENCY_MS)
                    .build(),
                glib::ParamSpecUInt::builder("draft")
                    .nick("MoQT draft")
                    .blurb(
                        "MoQ Transport draft version to negotiate (16 or 18); \
                         0 offers every draft libmoq supports",
                    )
                    .minimum(0)
                    .maximum(18)
                    .default_value(DEFAULT_DRAFT)
                    .build(),
                glib::ParamSpecUInt::builder("discovery-timeout")
                    .nick("Discovery timeout")
                    .blurb(
                        "Fail if the track is not announced within this many ms \
                         (0 fails immediately)",
                    )
                    .minimum(0)
                    .maximum(600000)
                    .default_value(DEFAULT_DISCOVERY_TIMEOUT_MS)
                    .build(),
            ]
        });
        PROPS.as_ref()
    }

    fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
        let mut s = self.settings.lock().unwrap();
        match pspec.name() {
            "host" => s.host = value.get::<Option<String>>().unwrap().unwrap_or_default(),
            "port" => s.port = value.get().unwrap(),
            "relay-path" => {
                s.relay_path = value.get::<Option<String>>().unwrap().unwrap_or_default()
            }
            "insecure" => s.insecure = value.get().unwrap(),
            "namespace" => s.namespace = value.get::<Option<String>>().unwrap().unwrap_or_default(),
            "track-name" => {
                s.track_name = value.get::<Option<String>>().unwrap().unwrap_or_default()
            }
            "caps" => s.caps = value.get().unwrap(),
            "latency" => s.latency_ms = value.get().unwrap(),
            "draft" => s.draft = value.get().unwrap(),
            "discovery-timeout" => s.discovery_timeout_ms = value.get().unwrap(),
            _ => unimplemented!(),
        }
    }

    fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        let s = self.settings.lock().unwrap();
        match pspec.name() {
            "host" => s.host.to_value(),
            "port" => s.port.to_value(),
            "relay-path" => s.relay_path.to_value(),
            "insecure" => s.insecure.to_value(),
            "namespace" => s.namespace.to_value(),
            "track-name" => s.track_name.to_value(),
            "caps" => s.caps.to_value(),
            "latency" => s.latency_ms.to_value(),
            "draft" => s.draft.to_value(),
            "discovery-timeout" => s.discovery_timeout_ms.to_value(),
            _ => unimplemented!(),
        }
    }

    fn constructed(&self) {
        self.parent_constructed();
        let obj = self.obj();
        obj.set_live(true);
        obj.set_format(gst::Format::Time);
    }
}

impl GstObjectImpl for MoqSrc {}

impl ElementImpl for MoqSrc {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
        static META: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
            gst::subclass::ElementMetadata::new(
                "MoQ source",
                "Source/Network",
                "Subscribe to a Media-over-QUIC track (libmoq service tier)",
                "Ray L <ray@raylucke.com>",
            )
        });
        Some(&*META)
    }

    fn pad_templates() -> &'static [gst::PadTemplate] {
        static TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
            vec![gst::PadTemplate::new(
                "src",
                gst::PadDirection::Src,
                gst::PadPresence::Always,
                &gst::Caps::new_any(),
            )
            .unwrap()]
        });
        TEMPLATES.as_ref()
    }
}

impl BaseSrcImpl for MoqSrc {
    fn start(&self) -> Result<(), gst::ErrorMessage> {
        let s = self.settings.lock().unwrap().clone();
        let namespace = Namespace::parse(&s.namespace).map_err(|e| {
            gst::error!(CAT, imp = self, "{}", e);
            gst::error_msg!(gst::ResourceError::Settings, ("{}", e))
        })?;

        let mut st = self.lock_state();
        st.receiver = None;
        *self.ep.lock().unwrap() = None;
        st.want_track = None;
        st.want_track_ended = false;
        st.selection_failed = false;
        st.caps_pushed = false;
        st.catalog_ready = false;
        st.objects_recv = 0;
        st.have_anchor = false;
        st.want_init.clear();
        st.init_pushed = false;
        st.pending = None;
        st.seen_tracks = Some(String::new());
        st.derived_caps = None;
        st.negotiation_failed = false;
        st.track_ever_seen = false;
        st.discover_deadline = clock::monotonic_us() + s.discovery_timeout_ms as i64 * 1000;

        let url = relay_url(&s.host, s.port, &s.relay_path);
        let ep = match Endpoint::connect(&endpoint_config(&url, s.insecure, s.draft)) {
            Ok(ep) => ep,
            Err(e) => {
                Self::clear_session(&mut st);
                return Err(gst::error_msg!(
                    gst::ResourceError::OpenRead,
                    (
                        "could not open MoQ endpoint to relay {}:{} (rc={})",
                        s.host,
                        s.port,
                        e.code()
                    )
                ));
            }
        };

        // Attach, not create: we own the endpoint above. Tracks are selected
        // by name, so nothing is auto-subscribed.
        let cfg = ReceiverCfg::live(namespace).auto_subscribe(false);
        let receiver = match Receiver::attach(&ep, &cfg) {
            Ok(r) => r,
            Err(e) => {
                self.log_terminal(&ep);
                // The last handle: stops and destroys the endpoint.
                drop(ep);
                Self::clear_session(&mut st);
                return Err(gst::error_msg!(
                    gst::ResourceError::OpenRead,
                    ("could not attach media receiver (rc={})", e.code())
                ));
            }
        };
        *self.ep.lock().unwrap() = Some(ep);
        st.receiver = Some(receiver);

        st.started = true;
        gst::info!(
            CAT,
            imp = self,
            "started: receiving ns={} name={} via relay {}:{}{}",
            s.namespace,
            s.track_name,
            s.host,
            s.port,
            s.relay_path
        );
        Ok(())
    }

    fn stop(&self) -> Result<(), gst::ErrorMessage> {
        let mut st = self.lock_state();
        if !st.started {
            return Ok(());
        }

        if let Some(ep) = self.endpoint() {
            ep.set_interrupted(true);
        }

        // The receiver goes first; dropping the last endpoint handle after it
        // stops and destroys the endpoint.
        if st.receiver.take().is_some() {
            st.want_track = None;
        }
        let ep = self.ep.lock().unwrap().take();
        drop(ep);

        st.started = false;
        Self::clear_session(&mut st);

        gst::info!(
            CAT,
            imp = self,
            "stopped (received {} objects)",
            st.objects_recv
        );
        Ok(())
    }

    fn unlock(&self) -> Result<(), gst::ErrorMessage> {
        if let Some(ep) = self.ep.lock().unwrap().as_ref() {
            ep.set_interrupted(true);
        }
        Ok(())
    }

    fn unlock_stop(&self) -> Result<(), gst::ErrorMessage> {
        if let Some(ep) = self.ep.lock().unwrap().as_ref() {
            ep.set_interrupted(false);
        }
        // A fragment queued behind an init segment before the flush is now
        // stale (its track/init state may no longer match); drop it instead
        // of pushing it out after the flush completes.
        self.lock_state().pending = None;
        Ok(())
    }

    fn query(&self, query: &mut gst::QueryRef) -> bool {
        if let gst::QueryViewMut::Latency(q) = query.view_mut() {
            let lat =
                gst::ClockTime::from_mseconds(self.settings.lock().unwrap().latency_ms as u64);
            q.set(true, lat, gst::ClockTime::NONE);
            return true;
        }
        BaseSrcImplExt::parent_query(self, query)
    }
}

impl PushSrcImpl for MoqSrc {
    fn create(
        &self,
        _buffer: Option<&mut gst::BufferRef>,
    ) -> Result<CreateSuccess, gst::FlowError> {
        self.create_buffer().map(CreateSuccess::NewBuffer)
    }
}

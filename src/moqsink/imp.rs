//! moqsink -- publish GStreamer streams over Media-over-QUIC using the
//! libmoq service tier (moq_endpoint_t + moq_media_sender_t).
//!
//! One element owns one endpoint and one media sender, so every pad's track
//! lands in a single MSF/CMSF catalog. Pads:
//!
//!   sink       (always)  H.264 Annex B access units -> LOC objects. The
//!                        track is added on the first keyframe so the catalog
//!                        carries an avcC initData and an SPS-derived codec.
//!   audio      (always)  raw AAC access units -> LOC objects.
//!   video_%u   (request) fragmented MP4 -> CMAF objects (one moof+mdat each);
//!   audio_%u   (request) ftyp+moov becomes the catalog init segment.
//!
//! The service tier owns the network thread, version negotiation, the sticky
//! catalog and the bounded per-track send queues; chain functions only
//! translate buffers into media objects. Sender::write() takes the
//! payload on success and libmoq drops our buffer reference when it releases
//! it (on the network thread).
//!
//! Sync: with sync=true (default) each chain waits on the pipeline clock for
//! the buffer's running time plus the configured latency, so non-live
//! sources are paced like a normal sink. Live sources already arrive at
//! rate. GST_EVENT_LATENCY is flagged UPSTREAM, so sending it to one of our
//! (sink-direction) pads is refused outright ("sending latency event in wrong
//! direction") without ever calling the pad's event function -- the same
//! reason GstBaseSink intercepts it in its own send_event override instead of
//! routing it through a pad. send_event does the same here: it stores the
//! latency directly, and only forwards other events onto the sink pads.
//!
//! GstElement, not GstBaseSink: this is a plain GstElement, so it never
//! preroll waits on the first buffer -- READY_TO_PAUSED returns immediately --
//! and reaching PAUSED does not pause publishing; only GstBaseSink's "sync"
//! property has an equivalent here ("async", "qos", "max-lateness",
//! "ts-offset", "blocksize", "enable-last-sample" and "render-delay" do not
//! apply).
//!
//! Thread safety: libmoq's media sender expects every call
//! (add_track/write/end_track/get_stats/is_fatal/fatal_code) to come from a
//! single application thread. With request pads several GStreamer streaming
//! threads share one sender, so all sender calls are serialized by the `send`
//! lock; it is never held while waiting on the pipeline clock. stop()
//! destroys the sender and clears it inside that same lock (one critical
//! section together with the final get_stats), and every streaming-thread
//! caller re-checks the sender under the lock before touching it, so a late
//! call can never use a freed sender.
//!
//! Lock order: `track` -> pad state -> `send` -> `obj` -> pad clock id. The
//! `obj` lock plays the role of the C element's OBJECT_LOCK.

use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Condvar, LazyLock, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use moq5::cmaf::{self, CmafInit, CodecKind};
use moq5::sys::{
    moq_result_t, MOQ_DONE, MOQ_ERR_CLOSED, MOQ_ERR_INTERRUPTED, MOQ_ERR_INVAL,
    MOQ_ERR_WOULD_BLOCK, MOQ_ERR_WRONG_STATE, MOQ_OK,
};
use moq5::{
    Buffer, CmsfUrl, ContentProtection, DrmSystem, MediaType, Namespace, Packaging, SapType,
    SendObject, WaitResult,
};

use crate::service::{Endpoint, SendTrack, Sender, SenderCfg, TrackCfg};

use super::pad::{MoqSinkPad, PadKind};
use crate::clock;
use crate::codec;
use crate::fmp4;
use crate::moq::{endpoint_config, rc, relay_url};

pub(crate) static CAT: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
    gst::DebugCategory::new("moqsink", gst::DebugColorFlags::empty(), Some("MoQ sink"))
});

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: u32 = 4443;
const DEFAULT_RELAY_PATH: &str = "/moq-relay";
const DEFAULT_NAMESPACE: &str = "example";
const DEFAULT_TRACK_NAME: &str = "video";
/// browsers (playa) default to draft-16; 0 = offer all
const DEFAULT_DRAFT: u32 = 16;
/// catalog max bitrate (MSF-01 5.2.22)
const DEFAULT_BITRATE: u64 = 2_000_000;
const DEFAULT_AUDIO_BITRATE: u64 = 128_000;
const DEFAULT_AUDIO_TRACK_NAME: &str = "audio";
/// LOC audio group cadence. Every AAC frame is independently decodable, so
/// any object could open a group; one group per ~21 ms frame would give a
/// relay ~47 groups/s per track. A group per second matches both the video
/// path's usual GOP cadence and what the CMAF path produces (one fragment,
/// one group).
const LOC_AUDIO_GROUP_US: u32 = 1_000_000;
const DEFAULT_MAX_FRAGMENT: u64 = 16 << 20;
/// How long the first pad to produce a track waits for the remaining LINKED
/// pads to produce theirs before the sender is attached anyway. It bounds the
/// case where a pad is linked but never delivers (a silent or absent
/// elementary stream that also never sends EOS); a pad that EOSes or flushes
/// drops out of the barrier immediately, so the normal cost is the gap
/// between the pads' first keyframes, not this timeout.
const DEFAULT_CATALOG_WAIT_MS: u32 = 3000;

pub(crate) const EOS_DRAIN_TIMEOUT_US: i64 = 3 * 1_000_000;

type Flow = Result<gst::FlowSuccess, gst::FlowError>;

#[derive(Clone, Debug)]
pub(crate) struct Settings {
    host: String,
    port: u32,
    relay_path: String,
    insecure: bool,
    namespace: String,
    /// LOC only; None = derive from the SPS
    codec: Option<String>,
    /// MoQT draft to negotiate, 0 = auto
    draft: u32,
    sync: bool,
    sap_timeline: bool,
    max_fragment_size: u64,
    /// DRM systems spec; see the property docs
    drm_systems: Option<String>,
    /// push (PUBLISH) instead of announce-and-wait
    publish_tracks: bool,
    /// 0 = never republish the catalog
    catalog_refresh_ms: u32,
    /// how long to wait for every linked pad's track
    catalog_wait_ms: u32,
    /// LOC audio group length
    loc_audio_group_ms: u32,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            host: DEFAULT_HOST.into(),
            port: DEFAULT_PORT,
            relay_path: DEFAULT_RELAY_PATH.into(),
            insecure: false,
            namespace: DEFAULT_NAMESPACE.into(),
            codec: None,
            draft: DEFAULT_DRAFT,
            sync: true,
            sap_timeline: false,
            max_fragment_size: DEFAULT_MAX_FRAGMENT,
            drm_systems: None,
            publish_tracks: false,
            catalog_refresh_ms: 0,
            catalog_wait_ms: DEFAULT_CATALOG_WAIT_MS,
            loc_audio_group_ms: LOC_AUDIO_GROUP_US / 1000,
        }
    }
}

/// What the C element kept under OBJECT_LOCK.
pub(crate) struct ObjState {
    /// from the last GST_EVENT_LATENCY
    pub(crate) latency: gst::ClockTime,
    /// concurrent FLUSH_START/STOP pads
    pub(crate) flush_count: i32,
    pub(crate) state_flushing: bool,
    /// Endpoint publication and removal share this lock (and `send`); no
    /// callback can touch a freed endpoint.
    pub(crate) ep: Option<Endpoint>,
    pub(crate) eos_posted: bool,

    /// Wall-clock anchor for the LOC Capture Timestamp (LOC-01 2.3.1.1 is
    /// epoch microseconds, not stream-relative). Taken once, from whichever
    /// LOC pad sees a timestamped buffer first, and shared by both: anchoring
    /// each pad separately would offset audio from video by the gap between
    /// their first buffers.
    loc_anchored: bool,
    loc_anchor_epoch_us: i64,
    loc_anchor_pts: gst::ClockTime,
    /// Raw PTS of the most recent VIDEO group start, published so the audio
    /// pad can open its group at the same instant (see chain_loc). Compared
    /// by identity only -- the two pads rebase presentation time separately,
    /// so magnitudes are not comparable across them.
    loc_video_group_pts: Option<gst::ClockTime>,
}

impl Default for ObjState {
    fn default() -> Self {
        ObjState {
            latency: gst::ClockTime::ZERO,
            flush_count: 0,
            state_flushing: false,
            ep: None,
            eos_posted: false,
            loc_anchored: false,
            loc_anchor_epoch_us: 0,
            loc_anchor_pts: gst::ClockTime::ZERO,
            loc_video_group_pts: None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct EndRequest {
    track: SendTrack,
    deadline: i64,
    accepted: bool,
}

/// Everything serialized by the C element's send_lock.
#[derive(Default)]
pub(crate) struct SendState {
    /// Holds the endpoint too; its on_ready context lives as long as it.
    pub(crate) sender: Option<Sender>,
    pub(crate) eos_failed: bool,
    pub(crate) sender_generation: u64,
    /// sender-owned; survives pad release
    pub(crate) end_requests: Vec<EndRequest>,
}

/// One authored CMSF 4.1.1 contentProtections entry: a (scheme, defaultKID,
/// DRM system) triple. CMSF carries ONE drmSystem per entry, so a stream
/// offered under several systems produces one entry per system, all sharing
/// the KID, and the track references them all. The strings own the bytes that
/// back the spans handed to libmoq.
#[derive(Clone, Debug, Default)]
pub(crate) struct CpEntry {
    /// refID, generated: "cp0", "cp1", ...
    ref_id: String,
    /// defaultKID as the CMSF "8-4-4-4-12" UUID string
    kid_uuid: String,
    /// "cenc" or "cbcs", from the init segment's schm
    scheme: String,
    /// drmSystem.systemID, a UUID string
    system_id: String,
    la_url: Option<String>,
    cert_url: Option<String>,
    auth_url: Option<String>,
    /// base64, opaque to us and to libmoq
    pssh: Option<String>,
    robustness: Option<String>,
}

#[derive(Default)]
pub(crate) struct TrackState {
    /// app-thread snapshot, collected while committing: each pad with the
    /// track the startup task adds for it
    pub(crate) preparing_tracks: Option<Vec<(MoqSinkPad, TrackCfg)>>,
    pub(crate) tracks_committed: bool,
    pub(crate) tracks_failed: bool,
    /// CMSF 4.1.1 root contentProtections authored for this session, built at
    /// the barrier (build_content_protections) from every protected CMAF
    /// pad's init segment crossed with drm-systems. libmoq deep-copies them at
    /// attach and at add_track, so they only have to outlive commit_tracks --
    /// they are kept until stop so the values stay inspectable.
    pub(crate) cp_entries: Vec<CpEntry>,
}

// -- EOS helpers (callers serialize sender mutations and keep the endpoint
// alive; no endpoint tasks are posted by them) --------------------------------

fn eos_remaining(deadline: i64) -> u64 {
    let remaining = deadline - clock::monotonic_us();
    remaining.max(0) as u64
}

fn eos_end(ep: &Endpoint, sender: &Sender, track: SendTrack, deadline: i64) -> moq_result_t {
    loop {
        if sender.is_fatal() {
            return MOQ_ERR_CLOSED;
        }
        if eos_remaining(deadline) == 0 {
            return MOQ_DONE;
        }
        let r = rc(&sender.end_track(track));
        if r != MOQ_ERR_WOULD_BLOCK {
            return r;
        }
        let remaining = eos_remaining(deadline);
        if remaining == 0 {
            return MOQ_DONE;
        }
        // Endpoint activity, rather than sender write readiness, avoids a
        // level-triggered spin when an end marker needs more queue capacity.
        if let Err(e) = ep.wait(Duration::from_micros(remaining.min(5000))) {
            return e.code();
        }
        if eos_remaining(deadline) == 0 {
            return MOQ_DONE;
        }
    }
}

fn eos_drain(ep: &Endpoint, deadline: i64) -> moq_result_t {
    match eos_remaining(deadline) {
        0 => MOQ_DONE,
        remaining => match ep.drain(Duration::from_micros(remaining)) {
            Ok(WaitResult::Ready) => MOQ_OK,
            Ok(WaitResult::TimedOut) => MOQ_DONE,
            Err(e) => e.code(),
        },
    }
}

// -- H.264 parameter sets -----------------------------------------------------

fn find_sps_pps(data: &[u8]) -> Option<(&[u8], &[u8])> {
    let len = data.len();
    let mut sps: Option<&[u8]> = None;
    let mut pps: Option<&[u8]> = None;
    let mut i = 0;
    let mut nal_start = 0;
    let mut in_nal = false;

    fn note<'a>(nal: &'a [u8], sps: &mut Option<&'a [u8]>, pps: &mut Option<&'a [u8]>) {
        let ty = nal.first().copied().unwrap_or(0) & 0x1f;
        if ty == 7 && sps.is_none() {
            *sps = Some(nal);
        } else if ty == 8 && pps.is_none() {
            *pps = Some(nal);
        }
    }

    while i + 3 <= len {
        if data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1 {
            if in_nal {
                let mut end = i;
                while end > nal_start && data[end - 1] == 0 {
                    end -= 1;
                }
                note(&data[nal_start..end], &mut sps, &mut pps);
            }
            i += 3;
            nal_start = i;
            in_nal = true;
            continue;
        }
        i += 1;
    }
    if in_nal && nal_start < len {
        note(&data[nal_start..len], &mut sps, &mut pps);
    }
    match (sps, pps) {
        (Some(s), Some(p)) if s.len() >= 4 && !p.is_empty() => Some((s, p)),
        _ => None,
    }
}

fn build_avcc(sps: &[u8], pps: &[u8]) -> Vec<u8> {
    let mut a = Vec::with_capacity(11 + sps.len() + pps.len());
    a.extend_from_slice(&[1, sps[1], sps[2], sps[3], 0xfc | 3, 0xe0 | 1]);
    a.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    a.extend_from_slice(sps);
    a.push(1);
    a.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    a.extend_from_slice(pps);
    a
}

/// Byte offset of the first NAL that is not an Access Unit Delimiter.
///
/// h264parse emits an AUD at the head of every access unit when it outputs
/// byte-stream/au, whatever the encoder was asked for. In LOC the object
/// boundary already delimits the access unit, so the AUD carries nothing --
/// and it costs: a consumer that walks the payload as length-prefixed NAL
/// units (the form the avc1 codec string and the avcC initData imply) reads
/// the 4-byte start code as a length, and the AUD's own bytes as the next
/// one, so the walk desynchronises and the keyframe fails to validate.
/// Dropping it puts the SPS first, where every parser expects it.
fn loc_skip_leading_aud(d: &[u8]) -> usize {
    let len = d.len();
    let sc = if len >= 5 && d[..4] == [0, 0, 0, 1] {
        4
    } else if len >= 4 && d[..3] == [0, 0, 1] {
        3
    } else {
        return 0;
    };
    if d[sc] & 0x1f != 9 {
        return 0;
    }
    // Skip to the next start code; without one the AU is only an AUD, so keep
    // the buffer as it is rather than publishing an empty object.
    let mut i = sc;
    while i + 3 < len {
        if d[i] == 0 && d[i + 1] == 0 && (d[i + 2] == 1 || (d[i + 2] == 0 && d[i + 3] == 1)) {
            return i;
        }
        i += 1;
    }
    0
}

// -- CMSF content protection (CMSF 4) -----------------------------------------

/// tenc default_KID is 16 raw bytes; CMSF 4.1.1.2 wants the UUID STRING form,
/// and libmoq validates that syntax on encode, so the hyphens are mandatory.
/// None for anything that is not a 16-byte KID.
fn kid_to_uuid(kid: &[u8]) -> Option<String> {
    if kid.len() != 16 {
        return None;
    }
    let h: String = kid.iter().map(|b| format!("{b:02x}")).collect();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    ))
}

pub struct MoqSink {
    pub(crate) settings: Mutex<Settings>,
    pub(crate) obj_state: Mutex<ObjState>,
    pub(crate) send: Mutex<SendState>,
    pub(crate) track: Mutex<TrackState>,
    pub(crate) track_cond: Condvar,
    pub(crate) started: AtomicBool,
    pub(crate) namespace: Mutex<Option<Namespace>>,
    pub(crate) locpad: OnceLock<MoqSinkPad>,
    pub(crate) locpad_audio: OnceLock<MoqSinkPad>,
    pad_serial: AtomicU32,
}

impl Default for MoqSink {
    fn default() -> Self {
        MoqSink {
            settings: Mutex::new(Settings::default()),
            obj_state: Mutex::new(ObjState::default()),
            send: Mutex::new(SendState::default()),
            track: Mutex::new(TrackState::default()),
            track_cond: Condvar::new(),
            started: AtomicBool::new(false),
            namespace: Mutex::new(None),
            locpad: OnceLock::new(),
            locpad_audio: OnceLock::new(),
            pad_serial: AtomicU32::new(0),
        }
    }
}

impl MoqSink {
    pub(crate) fn obj_lock(&self) -> MutexGuard<'_, ObjState> {
        self.obj_state.lock().unwrap()
    }
    pub(crate) fn send_lock(&self) -> MutexGuard<'_, SendState> {
        self.send.lock().unwrap()
    }
    pub(crate) fn track_lock(&self) -> MutexGuard<'_, TrackState> {
        self.track.lock().unwrap()
    }
    fn started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }
    fn settings(&self) -> Settings {
        self.settings.lock().unwrap().clone()
    }
    fn notify_tracks(&self) {
        let _g = self.track_lock();
        self.track_cond.notify_all();
    }

    pub(crate) fn sink_pads(&self) -> Vec<MoqSinkPad> {
        self.obj()
            .sink_pads()
            .into_iter()
            .map(|p| {
                p.downcast::<MoqSinkPad>()
                    .expect("moqsink pads are MoqSinkPads")
            })
            .collect()
    }

    fn new_pad(
        &self,
        templ: &gst::PadTemplate,
        name: &str,
        kind: PadKind,
        track_name: &str,
        bitrate: u64,
        max_fragment_size: usize,
    ) -> MoqSinkPad {
        let pad = gst::PadBuilder::<MoqSinkPad>::from_template(templ)
            .name(name)
            .chain_function(|pad, parent, buffer| {
                MoqSink::catch_panic_pad_function(
                    parent,
                    || Err(gst::FlowError::Error),
                    |imp| imp.chain(pad, buffer),
                )
            })
            .event_function(|pad, parent, event| {
                MoqSink::catch_panic_pad_function(
                    parent,
                    || false,
                    |imp| imp.sink_event(pad, event),
                )
            })
            .build();
        pad.init_kind(kind, track_name, bitrate, max_fragment_size);
        pad
    }

    // -- shared write path ---------------------------------------------------

    /// Submit one object and map the libmoq result onto a flow return. On any
    /// failure the object (and so its payload) is released here.
    pub(crate) fn write_object(&self, pad: &MoqSinkPad, obj: SendObject, len: usize) -> Flow {
        let sync = obj.is_sync;
        let rc = {
            let send = self.send_lock();
            let Some(sender) = send.sender.as_ref() else {
                return Err(gst::FlowError::Flushing);
            };
            let (flushing, ended) = {
                let st = self.obj_lock();
                (
                    pad.flushing() || st.state_flushing,
                    pad.eos() || st.eos_posted,
                )
            };
            if flushing {
                return Err(gst::FlowError::Flushing);
            }
            if ended {
                return Err(gst::FlowError::Eos);
            }
            match pad.track() {
                Some(track) => sender.write(track, obj).map_err(|e| e.error.code()),
                None => Err(MOQ_ERR_INVAL),
            }
        };
        match rc {
            Ok(()) => {
                pad.count_sent();
                gst::log!(
                    CAT,
                    obj = pad,
                    "wrote {} object ({} B)",
                    if sync { "sync" } else { "delta" },
                    len
                );
                Ok(gst::FlowSuccess::Ok)
            }
            Err(MOQ_ERR_WOULD_BLOCK) => {
                gst::log!(CAT, obj = pad, "dropped object under backpressure");
                Ok(gst::FlowSuccess::Ok)
            }
            Err(MOQ_ERR_INTERRUPTED) => Err(gst::FlowError::Flushing),
            Err(MOQ_ERR_CLOSED) => {
                gst::element_imp_error!(
                    self,
                    gst::ResourceError::Write,
                    ("endpoint closed before publisher EOS")
                );
                Err(gst::FlowError::Error)
            }
            Err(MOQ_ERR_INVAL) => {
                gst::element_imp_error!(
                    self,
                    gst::ResourceError::Write,
                    (
                        "libmoq rejected object {} on track \"{}\" (rc={}: invalid object, e.g. CMAF validation failed or a group start that is not a SAP)",
                        pad.objects_sent(),
                        pad.track_name(),
                        MOQ_ERR_INVAL
                    )
                );
                Err(gst::FlowError::Error)
            }
            Err(rc) => {
                gst::element_imp_error!(
                    self,
                    gst::ResourceError::Write,
                    ("media write failed (rc={})", rc)
                );
                Err(gst::FlowError::Error)
            }
        }
    }

    // -- clock sync ----------------------------------------------------------

    /// WaitResult until the buffer's running time when sync is on. Returns FLUSHING
    /// if the wait was unscheduled by a flush or state change.
    fn sync(&self, pad: &MoqSinkPad, ts: Option<gst::ClockTime>) -> Flow {
        if !self.settings.lock().unwrap().sync {
            return Ok(gst::FlowSuccess::Ok);
        }
        let Some(ts) = ts else {
            return Ok(gst::FlowSuccess::Ok);
        };
        let rt = {
            let s = pad.state();
            if s.segment.format() != gst::Format::Time {
                return Ok(gst::FlowSuccess::Ok);
            }
            s.segment
                .downcast_ref::<gst::ClockTime>()
                .and_then(|seg| seg.to_running_time(ts))
        };
        let Some(rt) = rt else {
            return Ok(gst::FlowSuccess::Ok);
        };

        let obj = self.obj();
        let Some(clock) = obj.clock() else {
            return Ok(gst::FlowSuccess::Ok);
        };
        let Some(base) = obj.base_time() else {
            return Ok(gst::FlowSuccess::Ok);
        };

        let id = {
            let st = self.obj_lock();
            if pad.flushing() {
                return Err(gst::FlowError::Flushing);
            }
            let id = clock.new_single_shot_id(base + rt + st.latency);
            *pad.clock_id() = Some(id.clone());
            id
        };

        let (cr, _) = id.wait();

        {
            let _st = self.obj_lock();
            *pad.clock_id() = None;
        }

        match cr {
            Err(gst::ClockError::Unscheduled) => Err(gst::FlowError::Flushing),
            _ => Ok(gst::FlowSuccess::Ok),
        }
    }

    fn unschedule(&self, pad: &MoqSinkPad, flushing: bool) {
        {
            let _st = self.obj_lock();
            pad.set_flushing(flushing);
            if let Some(id) = pad.clock_id().as_ref() {
                id.unschedule();
            }
        }
        // A flushing pad will not produce a track, so let the barrier
        // re-evaluate instead of waiting out its timeout for this pad.
        if flushing {
            self.notify_tracks();
        }
    }

    /// Cancellation owns the object-state lock independently of sender
    /// serialization, so flush can interrupt an in-progress final drain.
    /// Endpoint publication and removal share this lock; no callback can
    /// touch a freed endpoint.
    pub(crate) fn set_pad_flush(&self, pad: &MoqSinkPad, pending: bool) {
        {
            let mut st = self.obj_lock();
            if pad.flush_pending() != pending {
                st.flush_count += if pending { 1 } else { -1 };
                pad.set_flush_pending(pending);
            }
            pad.set_flushing(pending || st.state_flushing);
            if pad.flushing() {
                if let Some(id) = pad.clock_id().as_ref() {
                    id.unschedule();
                }
            }
            if let Some(ep) = &st.ep {
                ep.set_interrupted(st.state_flushing || st.flush_count > 0);
            }
        }
        self.notify_tracks();
    }

    // -- LOC path (always pads) ----------------------------------------------

    /// Derive the LOC audio track's catalog fields and stash them on the pad.
    /// Unlike video there is nothing to scavenge from the payload: AAC access
    /// units carry no in-band config, so everything comes from the caps
    /// (codec_data = AudioSpecificConfig), which GStreamer guarantees before
    /// the first buffer. Deriving is split from adding so the sender can be
    /// attached only once every linked pad has its track (await_tracks).
    fn prepare_loc_audio_track(&self, pad: &MoqSinkPad) -> bool {
        let ok = {
            let mut s = pad.state();
            let asc = s.codec_data.clone().unwrap_or_default();
            // Caps values, when present, must agree with the ASC.
            let agrees = |caps: i32, asc: i32| caps <= 0 || caps == asc;
            match codec::aac_asc_params(&asc) {
                Some((rate, channels)) if agrees(s.rate, rate) && agrees(s.channels, channels) => {
                    s.rate = rate;
                    s.channels = channels;
                    s.pend_codec = codec::codec_string_from_aac_asc(&asc);
                    s.pend_chan = Some(format!("{}", s.channels));
                    true
                }
                _ => false,
            }
        };
        if !ok {
            gst::element_imp_error!(
                self,
                gst::StreamError::Format,
                ("LOC audio requires a two-byte AAC-LC AudioSpecificConfig with matching rate/channels"),
                ["Explicit-frequency, HE-AAC and program-config-element forms are not supported"]
            );
        }
        ok
    }

    /// Defer the track to the sender startup task while committing, or add
    /// it to the running sender.
    fn register_track(
        &self,
        ts: &mut TrackState,
        pad: &MoqSinkPad,
        cfg: TrackCfg,
    ) -> moq_result_t {
        if let Some(prep) = ts.preparing_tracks.as_mut() {
            prep.push((pad.clone(), cfg));
            return MOQ_OK;
        }
        let send = self.send_lock();
        let st = self.obj_lock();
        match send.sender.as_ref() {
            None => MOQ_ERR_CLOSED,
            Some(_) if pad.eos() || pad.flushing() || st.eos_posted => MOQ_ERR_WRONG_STATE,
            Some(sender) => {
                let track = sender.add_track(&cfg);
                let r = rc(&track);
                pad.set_track(track.ok());
                r
            }
        }
    }

    /// Add the prepared LOC audio track. Caller holds the track lock.
    fn add_loc_audio_track(&self, ts: &mut TrackState, pad: &MoqSinkPad) -> bool {
        let track_name = pad.track_name();
        let bitrate = pad.bitrate();
        let (rc, codec, rate, channels, asc_len) = {
            let s = pad.state();
            let asc = s.codec_data.as_deref().unwrap_or_default();
            let codec = s.pend_codec.clone().unwrap_or_default();
            let chan_cfg = s.pend_chan.clone().unwrap_or_default();

            // The ASC is this track's decoder config, so it belongs in
            // initData for the same reason avcC does on the video pad: a
            // subscriber cannot configure a decoder from the access units
            // alone.
            let tc = TrackCfg::new(&track_name, MediaType::Audio, Packaging::Raw, &codec)
                .bitrate(bitrate)
                .samplerate(s.rate as u32)
                .channel_config(chan_cfg)
                .init_data(asc);
            (
                self.register_track(ts, pad, tc),
                codec,
                s.rate,
                s.channels,
                asc.len(),
            )
        };
        if rc != MOQ_OK {
            gst::element_imp_error!(
                self,
                gst::ResourceError::OpenWrite,
                (
                    "could not add LOC audio track \"{}\" (rc={})",
                    track_name,
                    rc
                )
            );
        } else {
            gst::debug!(
                CAT,
                obj = pad,
                "prepared LOC track {} codec={} sr={} ch={} initData={} B",
                track_name,
                codec,
                rate,
                channels,
                asc_len
            );
        }
        rc == MOQ_OK
    }

    /// Derive the LOC video track's catalog fields (avcC + codec string) from
    /// the caps or the first keyframe and stash them on the pad. Split from
    /// the add for the same reason as the audio pad: see await_tracks.
    fn prepare_loc_track(&self, pad: &MoqSinkPad, au: &[u8]) -> bool {
        let codec_prop = self.settings.lock().unwrap().codec.clone();
        let mut s = pad.state();
        let mut avcc = None;
        let mut codec = None;

        // stream-format=avc: the AVCDecoderConfigurationRecord is already in
        // the caps, and the access units carry no in-band parameter sets to
        // scavenge.
        if s.avc {
            let cd = s.codec_data.clone().unwrap_or_default();
            if cd.len() < 4 || cd[0] != 1 {
                drop(s);
                gst::element_imp_error!(
                    self,
                    gst::StreamError::Format,
                    (
                        "stream-format=avc on pad {} without a usable codec_data (avcC)",
                        pad.name()
                    )
                );
                return false;
            }
            if codec_prop.is_none() {
                codec = Some(format!("avc1.{:02x}{:02x}{:02x}", cd[1], cd[2], cd[3]));
            }
            avcc = Some(cd);
        } else if let Some((sps, pps)) = find_sps_pps(au) {
            avcc = Some(build_avcc(sps, pps));
            if codec_prop.is_none() {
                codec = Some(format!("avc1.{:02x}{:02x}{:02x}", sps[1], sps[2], sps[3]));
            }
        } else {
            gst::warning!(
                CAT,
                obj = pad,
                "keyframe carries no SPS/PPS; publishing without initData (use h264parse config-interval=-1)"
            );
        }
        let codec = codec.unwrap_or_else(|| codec_prop.unwrap_or_else(|| "avc1.42e01e".into()));

        s.pend_codec = Some(codec);
        s.pend_avcc = avcc; // may be None: no initData
        true
    }

    /// Add the prepared LOC video track. Caller holds the track lock.
    fn add_loc_track(&self, ts: &mut TrackState, pad: &MoqSinkPad) -> bool {
        let track_name = pad.track_name();
        let bitrate = pad.bitrate();
        let (rc, codec, width, height, init_len) = {
            let s = pad.state();
            let codec = s.pend_codec.clone().unwrap_or_default();

            let mut tc = TrackCfg::new(&track_name, MediaType::Video, Packaging::Raw, &codec)
                .bitrate(bitrate);
            let init_len = s.pend_avcc.as_ref().map_or(0, |a| a.len());
            if let Some(avcc) = s.pend_avcc.clone() {
                tc = tc.init_data(avcc);
            }
            if s.width > 0 && s.height > 0 {
                tc = tc.dimensions(s.width as u32, s.height as u32);
            }
            if s.fps_n > 0 && s.fps_d > 0 {
                tc = tc.framerate_millis((1000u64 * s.fps_n as u64) / s.fps_d as u64);
            }
            (
                self.register_track(ts, pad, tc),
                codec,
                s.width,
                s.height,
                init_len,
            )
        };
        if rc != MOQ_OK {
            gst::element_imp_error!(
                self,
                gst::ResourceError::OpenWrite,
                ("could not add track \"{}\" (rc={})", track_name, rc)
            );
        } else {
            gst::debug!(
                CAT,
                obj = pad,
                "prepared LOC track {} codec={} {}x{} initData={} B",
                track_name,
                codec,
                width,
                height,
                init_len
            );
        }
        rc == MOQ_OK
    }

    pub(crate) fn pad_cancelled(&self, pad: &MoqSinkPad) -> bool {
        let _st = self.obj_lock();
        pad.flushing() || pad.eos() || !self.started()
    }

    /// TRUE when every LINKED pad has either prepared its track or dropped out
    /// (EOS/flushing means it will never deliver one), and at least one did
    /// prepare. Unlinked pads are excluded exactly as in claim_eos: the always
    /// "video"/"audio" pads exist even in a CMAF-only pipeline that uses only
    /// the request pads. Caller holds the track lock.
    fn tracks_ready(&self) -> bool {
        let pads = self.sink_pads();
        let (mut all, mut any) = (true, false);
        let _st = self.obj_lock();
        for p in &pads {
            if !p.is_linked() {
                continue;
            }
            if p.pend_ready() {
                any = true;
            } else if !p.eos() && !p.flushing() {
                all = false;
            }
        }
        all && any
    }

    /// Attach the sender and add every prepared track to it. Caller holds the
    /// track lock, which keeps this to one pass and keeps the add_track calls
    /// together.
    pub(crate) fn commit_tracks(&self, ts: &mut TrackState) {
        if ts.tracks_committed || ts.tracks_failed {
            return;
        }

        ts.preparing_tracks = Some(Vec::new());

        // Author the root contentProtections BEFORE the pad walk, not at
        // attach time: it is what fills each protected pad's pend_cp_ref_ids,
        // and the walk below reads them into every track cfg. Authoring it
        // later (where the attach happens) leaves every track declaring no
        // refIDs while the root entries are published -- a catalog that
        // advertises DRM systems no track references, so a subscriber reads
        // the tracks as clear while the objects are encrypted. Safe this
        // early: it needs only each pad's parsed init_info, which
        // prepare_cmaf_track filled before the barrier released.
        self.build_content_protections(ts);

        let pads = self.sink_pads();
        let (mut any, mut failed) = (false, false);
        for p in &pads {
            if failed {
                break;
            }
            if !p.pend_ready() || p.track().is_some() || self.pad_cancelled(p) {
                continue;
            }
            let kind = p.kind();
            let ok = if kind.is_cmaf() {
                self.add_cmaf_track(ts, p).is_ok()
            } else if kind.is_audio() {
                self.add_loc_audio_track(ts, p)
            } else {
                self.add_loc_track(ts, p)
            };
            if ok {
                any = true;
            } else {
                failed = true;
            }
        }

        if failed || !any || !self.open_sender(ts) {
            ts.tracks_failed = true;
        } else {
            ts.tracks_committed = true;
        }
        ts.preparing_tracks = None;
        self.track_cond.notify_all();
    }

    /// WaitResult for all linked pads, then create one immutable track snapshot.
    /// The public endpoint task attaches/adds that snapshot in one managed
    /// turn before sender hooks run. Failure to prepare a linked stream is an
    /// error, never a successful partial catalog. Put queues on branches
    /// sharing an upstream task.
    pub(crate) fn await_tracks(&self, pad: &MoqSinkPad) -> bool {
        let catalog_wait_ms = self.settings.lock().unwrap().catalog_wait_ms;
        let mut ts = self.track_lock();
        pad.set_pend_ready(true);
        self.track_cond.notify_all();

        let deadline = clock::monotonic_us() + catalog_wait_ms as i64 * 1000;
        while !ts.tracks_committed && !ts.tracks_failed {
            if self.pad_cancelled(pad) {
                break;
            }
            if self.tracks_ready() {
                self.commit_tracks(&mut ts);
                break;
            }
            if self.pad_cancelled(pad) {
                break;
            }
            gst::debug!(CAT, imp = self, "waiting for linked track set");
            let remaining = deadline - clock::monotonic_us();
            let timed_out = if remaining <= 0 {
                true
            } else {
                let (g, res) = self
                    .track_cond
                    .wait_timeout(ts, Duration::from_micros(remaining as u64))
                    .unwrap();
                ts = g;
                res.timed_out()
            };
            if timed_out {
                if self.pad_cancelled(pad) {
                    break;
                }
                ts.tracks_failed = true;
                gst::element_imp_error!(
                    self,
                    gst::ResourceError::OpenWrite,
                    ("linked pads did not prepare all tracks within {} ms", catalog_wait_ms),
                    ["Add queues before moqsink pads; every linked stream must supply its first buffer"]
                );
                self.track_cond.notify_all();
                break;
            }
        }
        if ts.tracks_committed
            && pad.track().is_none()
            && pad.pend_ready()
            && !self.pad_cancelled(pad)
        {
            gst::element_imp_error!(
                self,
                gst::ResourceError::OpenWrite,
                ("pad {} was not in the initial track set", pad.name()),
                ["Link all publishing pads before the first buffer; restart through READY to add streams"]
            );
        }

        ts.tracks_committed && pad.track().is_some()
    }

    fn chain_loc(&self, pad: &MoqSinkPad, buffer: gst::Buffer) -> Flow {
        let audio = pad.kind().is_audio();
        // Every AAC access unit is independently decodable, so an audio
        // object is always a sync point; video waits for a real keyframe.
        let keyframe = audio || !buffer.flags().contains(gst::BufferFlags::DELTA_UNIT);
        let pts = buffer.pts();
        let dts = buffer.dts();
        let dts_or_pts = buffer.dts_or_pts();

        // Map before the barrier: preparing the video track needs the access
        // unit when the caps carry no avcC (Annex B), and the barrier can
        // block.
        let Ok(frame) = buffer.into_mapped_buffer_readable() else {
            gst::element_imp_error!(self, gst::StreamError::Format, ("failed to map buffer"));
            return Err(gst::FlowError::Error);
        };

        if pad.track().is_none() {
            if !keyframe {
                gst::log!(
                    CAT,
                    obj = pad,
                    "dropping delta frame before the first keyframe"
                );
                return Ok(gst::FlowSuccess::Ok);
            }
            let prepared = if audio {
                self.prepare_loc_audio_track(pad)
            } else {
                self.prepare_loc_track(pad, frame.as_slice())
            };
            if !prepared {
                return Err(gst::FlowError::Error);
            }
            // Attaches the sender once every linked pad has a track.
            if !self.await_tracks(pad) {
                return Err(if self.pad_cancelled(pad) {
                    gst::FlowError::Flushing
                } else {
                    gst::FlowError::Error
                });
            }
        }

        let (fatal, fatal_code) = match self.send_lock().sender.as_ref() {
            None => (true, 0),
            Some(sender) if sender.is_fatal() => (true, sender.fatal_code()),
            Some(_) => (false, 0),
        };
        if fatal {
            gst::element_imp_error!(
                self,
                gst::ResourceError::Write,
                ("media sender failed (code={})", fatal_code)
            );
            return Err(gst::FlowError::Error);
        }

        self.sync(pad, dts_or_pts)?;

        // Rebase to the first PTS so LOC carries small, stream-relative times.
        let (pts_us, dts_us, avc, segment) = {
            let mut s = pad.state();
            if pts.is_some() && s.base_pts.is_none() {
                s.base_pts = pts;
            }
            let rel = |t: Option<gst::ClockTime>| match (t, s.base_pts) {
                (Some(t), Some(b)) if t >= b => Some((t - b).useconds()),
                _ => None,
            };
            let pts_us = rel(pts).unwrap_or(0);
            let dts_us = rel(dts).unwrap_or(pts_us);
            (pts_us, dts_us, s.avc, s.segment.clone())
        };

        // Video only: audio access units have no AUD to strip.
        let skip = if audio || avc {
            0
        } else {
            loc_skip_leading_aud(frame.as_slice())
        };
        let len = frame.len() - skip;
        // Zero-copy: libmoq drops the mapped buffer when it releases the
        // payload.
        let end = frame.len();
        let payload = match crate::payload::wrap_owner(frame, skip..end) {
            Ok(p) => p,
            Err(_) => {
                gst::element_imp_error!(self, gst::ResourceError::Write, ("rcbuf wrap failed"));
                return Err(gst::FlowError::Error);
            }
        };

        // LOC-01 2.3.1.1: the Capture Timestamp is wall-clock microseconds
        // since the Unix epoch. presentation_time_us above is rebased to the
        // start of the stream, and libmoq would emit that verbatim if
        // has_capture_time were false, so a subscriber computing
        // `now - capture` would see the whole epoch as latency and treat every
        // frame as ancient. Map this buffer's PTS onto the shared anchor
        // instead.
        let mut have_capture = false;
        let mut capture_us = 0u64;
        let running = if segment.format() == gst::Format::Time {
            segment
                .downcast_ref::<gst::ClockTime>()
                .and_then(|seg| pts.and_then(|p| seg.to_running_time(p)))
        } else {
            None
        };
        if let Some(running) = running {
            let mut st = self.obj_lock();
            if !st.loc_anchored {
                st.loc_anchor_epoch_us = glib::real_time();
                st.loc_anchor_pts = running;
                st.loc_anchored = true;
                gst::info!(
                    CAT,
                    obj = pad,
                    "LOC capture-timestamp anchor: pts {} = {} us since the epoch",
                    pts.display(),
                    st.loc_anchor_epoch_us
                );
            }
            let d = running.nseconds() as i64 - st.loc_anchor_pts.nseconds() as i64;
            let c = st.loc_anchor_epoch_us + d / 1000;
            if c > 0 {
                capture_us = c as u64;
                have_capture = true;
            }
        }

        let mut starts_group = keyframe;
        if audio {
            // Open an audio group at every VIDEO group start (keyframe), so
            // both tracks are joinable at the same instant. A subscriber
            // starts each track at that track's next group boundary, and the
            // two cadences are otherwise unrelated (video = GOP, audio = a
            // fixed period), so their join points drift against each other:
            // measured on this pipeline, audio arrived up to a full
            // audio-period after video, and a player that cannot start video
            // before it has an audio reference then renders nothing at all.
            // Aligning collapses that gap to network jitter.
            //
            // loc-audio-group-ms stays as an UPPER bound so audio still opens
            // groups when video stalls or is absent (audio-only). An extra
            // boundary between keyframes is harmless here: it only makes audio
            // available EARLIER, which is the safe direction.
            let vgrp = self.obj_lock().loc_video_group_pts;
            let group_ms = self.settings.lock().unwrap().loc_audio_group_ms;
            let mut s = pad.state();
            if vgrp.is_some() && vgrp != s.aligned_to_video_pts {
                s.aligned_to_video_pts = vgrp; // one audio group per video group
                s.group_start = Some(pts_us);
            } else if s
                .group_start
                .is_none_or(|g| pts_us >= g + group_ms as u64 * 1000)
            {
                s.group_start = Some(pts_us);
            } else {
                starts_group = false;
            }
        } else if starts_group && pts.is_some() {
            // Publish this video group start for the audio pad to align to.
            self.obj_lock().loc_video_group_pts = pts;
        }

        let o = SendObject {
            is_sync: keyframe,
            starts_group,
            decode_time_us: dts_us,
            presentation_time_us: pts_us,
            capture_time_us: have_capture.then_some(capture_us),
            ..SendObject::new(payload)
        };

        self.write_object(pad, o, len)
    }

    // -- CMAF path (request pads) --------------------------------------------

    /// Parse a CMAF pad's init segment and stash what the track needs on the
    /// pad. Split from the add for the same reason as the LOC pads -- see
    /// await_tracks -- and additionally because the CENC boxes this parse
    /// yields (has_cenc / scheme / default_KID) have to be known BEFORE the
    /// sender is attached: the catalog's root contentProtections travel in
    /// the sender cfg.
    fn prepare_cmaf_track(&self, pad: &MoqSinkPad, init: Vec<u8>) -> bool {
        let kind = pad.kind();
        let init = match CmafInit::parse(init) {
            Ok(init) => init,
            Err(e) => {
                gst::element_imp_error!(
                    self,
                    gst::StreamError::Format,
                    (
                        "could not parse CMAF init segment on pad {} (rc={})",
                        pad.name(),
                        e.code()
                    )
                );
                return false;
            }
        };

        let codec_kind = init.info().codec_kind();
        let Some(codec) = codec::codec_string_from_init(init.info()) else {
            gst::element_imp_error!(
                self,
                gst::StreamError::CodecNotFound,
                (
                    "unsupported codec in CMAF init segment on pad {} (libmoq kind {}); this plugin publishes avc1, mp4a and opus",
                    pad.name(),
                    codec_kind.code()
                )
            );
            return false;
        };
        let audio_codec = matches!(codec_kind, CodecKind::Aac | CodecKind::Opus);
        if kind.is_audio() != audio_codec {
            gst::element_imp_error!(
                self,
                gst::StreamError::WrongType,
                (
                    "pad {} is {} but the init segment carries codec {}",
                    pad.name(),
                    if kind.is_audio() { "audio" } else { "video" },
                    codec
                )
            );
            return false;
        }

        let mut s = pad.state();
        s.init = Some(init);
        s.pend_codec = Some(codec);
        true
    }

    /// Register a prepared CMAF track. Caller holds the track lock.
    fn add_cmaf_track(&self, ts: &mut TrackState, pad: &MoqSinkPad) -> Flow {
        let kind = pad.kind();
        let track_name = pad.track_name();
        let bitrate = pad.bitrate();
        let sap_timeline = self.settings.lock().unwrap().sap_timeline;
        let s = pad.state();

        let (Some(codec), Some(init)) = (s.pend_codec.clone(), s.init.as_ref()) else {
            drop(s);
            gst::element_imp_error!(
                self,
                gst::StreamError::Failed,
                (
                    "CMAF track on pad {} was not prepared before the add",
                    pad.name()
                )
            );
            return Err(gst::FlowError::Error);
        };
        let info = init.info();

        let channels = if info.channel_count() > 0 {
            format!("{}", info.channel_count())
        } else {
            "2".to_string()
        };

        let mut tc = TrackCfg::new(&track_name, kind.media_type, Packaging::Cmaf, &codec)
            .bitrate(bitrate)
            .timescale(info.timescale())
            .init_data(init.data());
        if sap_timeline {
            tc = tc.sap_timeline(0);
        }
        // CMSF 4.1.2: declaring these ids tells a subscriber the track is
        // encrypted, and libmoq cross-checks that every id resolves to a root
        // entry AND that init_data really carries the 4.2 CENC boxes.
        // Resolved at the barrier, so an unprotected track leaves this empty.
        let refs = s.pend_cp_ref_ids.clone().unwrap_or_default();
        let n_refs = refs.len();
        if n_refs > 0 {
            tc = tc.content_protection_ref_ids(refs);
        }
        if kind.media_type == MediaType::Video {
            tc = tc.dimensions(info.width(), info.height());
            if s.fps_n > 0 && s.fps_d > 0 {
                tc = tc.framerate_millis((1000u64 * s.fps_n as u64) / s.fps_d as u64);
            }
        } else {
            tc = tc.samplerate(info.samplerate()).channel_config(&channels);
        }

        // register_track either adds the track now or defers the cfg to the
        // sender startup task, which owns everything the cfg points at.
        let rc = self.register_track(ts, pad, tc);
        let (width, height, samplerate, timescale, init_len) = (
            info.width(),
            info.height(),
            info.samplerate(),
            info.timescale(),
            init.data().len(),
        );
        drop(s);
        if rc != MOQ_OK {
            gst::element_imp_error!(
                self,
                gst::ResourceError::OpenWrite,
                (
                    "could not add CMAF track \"{}\" codec={} (rc={}){}",
                    track_name,
                    codec,
                    rc,
                    if n_refs > 0 {
                        " -- an encrypted track needs an init segment carrying the CMSF 4.2 CENC boxes"
                    } else {
                        ""
                    }
                )
            );
        } else if kind.media_type == MediaType::Video {
            gst::debug!(
                CAT,
                obj = pad,
                "prepared CMAF track {} codec={} {}x{} timescale={} init={} B",
                track_name,
                codec,
                width,
                height,
                timescale,
                init_len
            );
        } else {
            gst::debug!(
                CAT,
                obj = pad,
                "prepared CMAF track {} codec={} sr={} ch={} timescale={} init={} B",
                track_name,
                codec,
                samplerate,
                channels,
                timescale,
                init_len
            );
        }
        if rc == MOQ_OK {
            Ok(gst::FlowSuccess::Ok)
        } else {
            Err(gst::FlowError::Error)
        }
    }

    /// Send one moof+mdat fragment as a MoQ object.
    fn send_fragment(&self, pad: &MoqSinkPad, frag: Vec<u8>) -> Flow {
        // First sample flags decide whether this fragment opens a group. The
        // sample table is sized to the fragment (parse_fragment grows it once
        // when libmoq reports more samples than fit).
        let first_flags = match cmaf::parse_fragment(&frag) {
            Ok(fi) => fi.first_sample_flags(),
            Err(e) => {
                gst::element_imp_error!(
                    self,
                    gst::StreamError::Format,
                    (
                        "malformed CMAF fragment on pad {} (rc={})",
                        pad.name(),
                        e.code()
                    )
                );
                return Err(gst::FlowError::Error);
            }
        };
        // sample_is_non_sync_sample (bit 0x00010000): clear means this sample
        // IS a sync sample. Audio fragments are always treated as sync points.
        // Do not use cmaf::sap_from_sample_flags() here: it also reports
        // UNKNOWN (possible open-GOP SAP-3) for a non-sync-but-independent
        // sample, which is not what "opens a group" means for mp4mux's trun
        // first_sample_flags.
        let sync = pad.kind().is_audio() || first_flags & 0x0001_0000 == 0;

        let len = frag.len();
        // Zero-copy: libmoq drops the fragment when it releases the payload.
        let payload = match Buffer::from_vec(frag) {
            Ok(p) => p,
            Err(_) => {
                gst::element_imp_error!(self, gst::ResourceError::Write, ("rcbuf wrap failed"));
                return Err(gst::FlowError::Error);
            }
        };

        // CMAF timing lives in the fragment, so no properties.
        let o = SendObject {
            is_sync: sync,
            starts_group: sync,
            sap_type: sync.then_some(SapType::Type1),
            ..SendObject::new(payload)
        };
        self.write_object(pad, o, len)
    }

    fn chain_cmaf(&self, pad: &MoqSinkPad, buffer: gst::Buffer) -> Flow {
        // The sender is attached by the track barrier below, not by start, so
        // a null sender here just means no pad has produced a track yet.
        let (fatal, fatal_code) = match self.send_lock().sender.as_ref() {
            Some(sender) if sender.is_fatal() => (true, sender.fatal_code()),
            _ => (false, 0),
        };
        if fatal {
            gst::element_imp_error!(
                self,
                gst::ResourceError::Write,
                ("media sender failed (code={})", fatal_code)
            );
            return Err(gst::FlowError::Error);
        }

        self.sync(pad, buffer.dts_or_pts())?;

        let Ok(map) = buffer.map_readable() else {
            gst::element_imp_error!(self, gst::StreamError::Format, ("failed to map buffer"));
            return Err(gst::FlowError::Error);
        };
        let pushed = pad.state().splitter.push(map.as_slice());
        drop(map);
        drop(buffer);
        if let Err(err) = pushed {
            gst::element_imp_error!(
                self,
                gst::StreamError::Format,
                ("fragmented MP4 on pad {}: {}", pad.name(), err)
            );
            return Err(gst::FlowError::Error);
        }

        let mut ret = Ok(gst::FlowSuccess::Ok);
        while ret.is_ok() {
            // Pull one unit at a time: the pad lock must not be held across
            // the barrier or the write.
            let Some(u) = pad.state().splitter.pull() else {
                break;
            };
            match u.kind {
                fmp4::Kind::Init => {
                    if pad.track().is_some() {
                        // A new init segment mid-stream is a codec change; not
                        // supported.
                        gst::element_imp_error!(
                            self,
                            gst::StreamError::Format,
                            (
                                "pad {} received a second init segment; codec changes are not supported",
                                pad.name()
                            )
                        );
                        ret = Err(gst::FlowError::Error);
                    } else if !self.prepare_cmaf_track(pad, u.data) {
                        // Parse before the barrier: the catalog's
                        // contentProtections are built from these CENC boxes
                        // and travel in the sender cfg, so the parse has to
                        // precede the attach (build_content_protections).
                        ret = Err(gst::FlowError::Error);
                    } else if !self.await_tracks(pad) {
                        // Attaches the sender once every linked pad has its
                        // init segment, so the initial catalog carries them
                        // all.
                        ret = Err(if pad.flushing() || !self.started() {
                            gst::FlowError::Flushing
                        } else {
                            gst::FlowError::Error
                        });
                    }
                }
                fmp4::Kind::Fragment => {
                    if pad.track().is_none() {
                        gst::element_imp_error!(
                            self,
                            gst::StreamError::Format,
                            ("fragment before init segment on pad {}", pad.name())
                        );
                        ret = Err(gst::FlowError::Error);
                    } else {
                        ret = self.send_fragment(pad, u.data);
                    }
                }
            }
        }
        let mut s = pad.state();
        if !s.skip_logged && s.splitter.skipped() > 0 {
            gst::debug!(
                CAT,
                obj = pad,
                "skipping non-media top-level boxes (styp/sidx/...)"
            );
            s.skip_logged = true;
        }
        ret
    }

    // -- pad functions -------------------------------------------------------

    fn chain(&self, pad: &MoqSinkPad, buffer: gst::Buffer) -> Flow {
        if !self.started() {
            return Err(gst::FlowError::Flushing);
        }
        if pad.kind().is_cmaf() {
            self.chain_cmaf(pad, buffer)
        } else {
            self.chain_loc(pad, buffer)
        }
    }

    /// Keep accepted/deferred native obligations independently of Gst pads.
    /// Release serialization between retries so another track can keep
    /// writing.
    fn end_handle(&self, track: SendTrack, deadline: i64, generation: u64) -> moq_result_t {
        loop {
            let (rc, remaining) = {
                let mut guard = self.send_lock();
                let send = &mut *guard;
                let has_ep = self.obj_lock().ep.is_some();
                let sender = match send.sender.as_ref() {
                    Some(sender) if has_ep && send.sender_generation == generation => sender,
                    _ => return MOQ_ERR_CLOSED,
                };
                let idx = match send.end_requests.iter().position(|r| r.track == track) {
                    Some(i) => i,
                    None => {
                        send.end_requests.push(EndRequest {
                            track,
                            deadline,
                            accepted: false,
                        });
                        send.end_requests.len() - 1
                    }
                };
                let request = &mut send.end_requests[idx];
                let remaining = eos_remaining(request.deadline);
                let rc = if request.accepted {
                    MOQ_OK
                } else if sender.is_fatal() {
                    MOQ_ERR_CLOSED
                } else if remaining == 0 {
                    MOQ_DONE
                } else {
                    rc(&sender.end_track(track))
                };
                if rc == MOQ_OK {
                    request.accepted = true;
                }
                (rc, remaining)
            };
            if rc != MOQ_ERR_WOULD_BLOCK {
                return rc;
            }
            clock::sleep_us(remaining.min(5000));
        }
    }

    fn end_track(&self, pad: &MoqSinkPad, deadline: i64) -> moq_result_t {
        let (track, generation) = {
            let send = self.send_lock();
            let _st = self.obj_lock();
            (pad.track(), send.sender_generation)
        };
        match track {
            None => MOQ_OK,
            Some(track) => self.end_handle(track, deadline, generation),
        }
    }

    fn eos_error(&self, rc: moq_result_t) {
        self.send_lock().eos_failed = true;
        gst::element_imp_error!(
            self,
            gst::ResourceError::Write,
            ("publisher EOS failed or aborted (rc={})", rc),
            ["Local service/stream flush did not complete; teardown may discard the tail"]
        );
    }

    fn retry_ends(&self) {
        let (pending, generation) = {
            let send = self.send_lock();
            let pending: Vec<EndRequest> = send
                .end_requests
                .iter()
                .filter(|r| !r.accepted)
                .copied()
                .collect();
            (pending, send.sender_generation)
        };
        let mut rc = MOQ_OK;
        for r in &pending {
            if rc != MOQ_OK {
                break;
            }
            rc = self.end_handle(r.track, r.deadline, generation);
        }
        if rc != MOQ_OK && rc != MOQ_ERR_INTERRUPTED {
            self.eos_error(rc);
        }
    }

    /// The claim closes writer/add gates. The startup task has settled before
    /// a sender is published to writers, so no pending task can create output
    /// here.
    fn drain(&self, deadline: i64) -> bool {
        let rc = {
            let mut guard = self.send_lock();
            let send = &mut *guard;
            let ep = self.obj_lock().ep.clone();
            let mut rc = match (send.sender.as_ref(), ep.as_ref()) {
                _ if send.eos_failed => MOQ_ERR_CLOSED,
                (Some(_), Some(_)) => MOQ_OK,
                (None, None) if send.end_requests.is_empty() => MOQ_OK,
                _ => MOQ_ERR_CLOSED,
            };
            if let (Some(sender), Some(ep)) = (send.sender.as_ref(), ep.as_ref()) {
                for r in send.end_requests.iter_mut() {
                    if rc != MOQ_OK {
                        break;
                    }
                    if !r.accepted {
                        rc = eos_end(ep, sender, r.track, r.deadline.min(deadline));
                        r.accepted = rc == MOQ_OK;
                    }
                }
            }
            if rc == MOQ_OK {
                if let Some(ep) = ep.as_ref() {
                    rc = eos_drain(ep, deadline);
                }
            }
            rc
        };
        if rc != MOQ_OK {
            self.eos_error(rc);
            return false;
        }
        gst::info!(
            CAT,
            imp = self,
            "publisher EOS: local service and stream queues flushed"
        );
        let obj = self.obj();
        let _ = obj.post_message(gst::message::Eos::builder().src(&*obj).build());
        true
    }

    /// Claim once after all linked pads or pads with native tracks reach EOS
    /// and accept their end requests. An unused unlinked LOC pad is excluded.
    /// Taking the send lock before the object-state lock matches add/write
    /// ordering and prevents a concurrent EOS handler's unaccepted request
    /// from escaping the barrier.
    fn claim_eos(&self) -> bool {
        let pads = self.sink_pads();
        let send = self.send_lock();
        let mut st = self.obj_lock();
        if st.eos_posted || send.eos_failed {
            return false;
        }
        let (mut all, mut any_linked) = (true, false);
        for p in &pads {
            let track = p.track();
            if !p.is_linked() && track.is_none() {
                continue;
            }
            any_linked = true;
            if !p.eos() {
                all = false;
            }
            if let Some(track) = track {
                if !send
                    .end_requests
                    .iter()
                    .any(|r| r.track == track && r.accepted)
                {
                    all = false;
                }
            }
        }
        if all && any_linked {
            st.eos_posted = true;
            return true;
        }
        false
    }

    pub(crate) fn sink_event(&self, pad: &MoqSinkPad, event: gst::Event) -> bool {
        use gst::EventView;
        let kind = pad.kind();
        match event.view() {
            EventView::Caps(c) => {
                let caps = c.caps();
                let Some(st) = caps.structure(0) else {
                    return true;
                };
                let int = |f: &str| st.get::<i32>(f).unwrap_or(0);
                let buffer_field = |f: &str| {
                    st.get::<gst::Buffer>(f)
                        .ok()
                        .and_then(|b| b.map_readable().ok().map(|m| m.as_slice().to_vec()))
                };
                let mut s = pad.state();
                s.width = int("width");
                s.height = int("height");
                let (n, d) = st
                    .get::<gst::Fraction>("framerate")
                    .map(|f| (f.numer(), f.denom()))
                    .unwrap_or((0, 0));
                s.fps_n = n;
                s.fps_d = d;
                if kind.media_type == MediaType::Video && kind.packaging == Packaging::Raw {
                    s.avc = st.get::<&str>("stream-format").ok() == Some("avc");
                    s.codec_data = if s.avc {
                        buffer_field("codec_data")
                    } else {
                        None
                    };
                }
                if kind.is_audio() && kind.packaging == Packaging::Raw {
                    s.rate = int("rate");
                    s.channels = int("channels");
                    s.codec_data = buffer_field("codec_data");
                    // The catalog's samplerate/channelConfig are REQUIRED for
                    // an audio track (MSF-01 5.2.28/5.2.29) and libmoq rejects
                    // the track without them, so fall back to the ASC when the
                    // caps omit either.
                    if s.rate == 0 || s.channels == 0 {
                        if let Some((r, c)) =
                            s.codec_data.as_deref().and_then(codec::aac_asc_params)
                        {
                            if s.rate == 0 {
                                s.rate = r;
                            }
                            if s.channels == 0 {
                                s.channels = c;
                            }
                        }
                    }
                }
                drop(s);
                gst::debug!(CAT, obj = pad, "caps {:?}", caps);
            }
            EventView::Segment(seg) => {
                pad.state().segment = seg.segment().clone();
            }
            EventView::FlushStart(_) => self.set_pad_flush(pad, true),
            EventView::FlushStop(_) => {
                self.set_pad_flush(pad, false);
                pad.state().segment = gst::Segment::new();
                self.retry_ends();
            }
            // GST_EVENT_LATENCY never reaches here: it is flagged UPSTREAM,
            // so sending it to a sink pad is refused before this function
            // runs. It is intercepted in send_event instead.
            EventView::Eos(_) => {
                let deadline = clock::monotonic_us() + EOS_DRAIN_TIMEOUT_US;
                // EOS is serialized with this pad's chain. A buffered partial
                // box or unpaired moof must not disappear behind a successful
                // native drain.
                if kind.is_cmaf() && pad.state().splitter.has_partial() {
                    self.send_lock().eos_failed = true;
                    gst::element_imp_error!(
                        self,
                        gst::StreamError::Format,
                        ("incomplete fragmented MP4 at EOS on pad {}", pad.name()),
                        ["A partial box or moof without mdat remains buffered"]
                    );
                    return false;
                }
                {
                    let _st = self.obj_lock();
                    pad.set_eos(true);
                }
                // Drops this pad out of the track barrier (a stream that ends
                // without ever delivering a keyframe must not hold the others
                // up).
                self.notify_tracks();
                let rc = self.end_track(pad, deadline);
                if rc != MOQ_OK {
                    self.eos_error(rc);
                    return false;
                }
                if self.claim_eos() && !self.drain(deadline) {
                    return false;
                }
            }
            _ => {}
        }
        true
    }

    // -- lifecycle -----------------------------------------------------------

    fn start(&self) -> bool {
        let ns = self.settings.lock().unwrap().namespace.clone();
        match Namespace::parse(&ns) {
            Ok(n) => *self.namespace.lock().unwrap() = Some(n),
            Err(e) => {
                gst::error!(CAT, imp = self, "{}", e);
                return false;
            }
        }

        self.send_lock().eos_failed = false;
        let pads = self.sink_pads();
        {
            let mut st = self.obj_lock();
            st.eos_posted = false;
            st.loc_anchored = false;
            st.loc_video_group_pts = None;
            st.flush_count = 0;
            st.state_flushing = false;
            for p in &pads {
                p.reset();
            }
        }

        {
            let mut ts = self.track_lock();
            ts.tracks_committed = false;
            ts.tracks_failed = false;
        }

        self.started.store(true, Ordering::SeqCst);
        let s = self.settings();
        gst::info!(
            CAT,
            imp = self,
            "ready: ns={}, relay {}:{}{} (connecting once every linked pad has a track)",
            s.namespace,
            s.host,
            s.port,
            s.relay_path
        );
        true
    }

    /// Parse one system from the drm-systems spec: a bare systemID UUID, then
    /// optional key=value fields. None if the systemID is missing. Values are
    /// taken verbatim -- libmoq carries DRM metadata opaquely and checks only
    /// the UUID syntax.
    fn parse_drm_system(&self, spec: &str) -> Option<CpEntry> {
        let mut out = CpEntry::default();
        let mut ok = false;
        for (i, f) in spec.split(',').enumerate() {
            let f = f.trim();
            if f.is_empty() {
                continue;
            }
            if i == 0 {
                out.system_id = f.into();
                ok = true;
                continue;
            }
            // Split on the FIRST '=' only: a base64 PSSH is padded with '='.
            let (k, v) = match f.split_once('=') {
                Some((k, v)) => (k.trim(), Some(v.trim())),
                None => (f, None),
            };
            let Some(v) = v.filter(|v| !v.is_empty()) else {
                gst::warning!(
                    CAT,
                    imp = self,
                    "drm-systems: ignoring field \"{}\" with no value",
                    f
                );
                continue;
            };
            match k.to_ascii_lowercase().as_str() {
                "laurl" => out.la_url = Some(v.into()),
                "certurl" => out.cert_url = Some(v.into()),
                "authurl" => out.auth_url = Some(v.into()),
                "pssh" => out.pssh = Some(v.into()),
                "robustness" => out.robustness = Some(v.into()),
                _ => gst::warning!(CAT, imp = self, "drm-systems: unknown field \"{}\"", k),
            }
        }
        ok.then_some(out)
    }

    /// Build the session's root contentProtections: every protected CMAF
    /// pad's (scheme, defaultKID) crossed with every configured DRM system,
    /// and record on each pad the refIDs its track must reference.
    ///
    /// The scheme and the KID come from the init segment the pad is already
    /// forwarding -- parsing it yields the CENC scheme and default_KID,
    /// so nothing about the encryption itself has to be configured here.
    /// drm-systems supplies only what an fMP4 cannot carry: which DRM systems
    /// the stream is offered under, and their licence URLs and PSSH.
    ///
    /// Runs under the track lock, from commit_tracks, ahead of the pad walk
    /// that registers the tracks -- which is why the initial-catalog barrier
    /// is a precondition for this working at all: every pad's init segment
    /// must already be parsed when the refIDs are derived, and the sender cfg
    /// freezes the root entries at attach.
    fn build_content_protections(&self, ts: &mut TrackState) {
        ts.cp_entries.clear();

        // Parse drm-systems once per attach.
        let drm = self
            .settings
            .lock()
            .unwrap()
            .drm_systems
            .clone()
            .unwrap_or_default();
        let mut systems = Vec::new();
        for spec in drm.split(';') {
            if spec.trim().is_empty() {
                continue;
            }
            match self.parse_drm_system(spec.trim()) {
                Some(sys) => systems.push(sys),
                None => gst::warning!(
                    CAT,
                    imp = self,
                    "drm-systems: no systemID in \"{}\"; entry skipped",
                    spec.trim()
                ),
            }
        }

        let mut protected_pads = 0;
        for p in self.sink_pads() {
            let mut s = p.state();
            s.pend_cp_ref_ids = None;

            let cenc = if p.pend_ready() && p.kind().is_cmaf() {
                s.init.as_ref().and_then(|i| i.info().cenc()).map(|c| {
                    (
                        c.default_kid.to_vec(),
                        String::from_utf8_lossy(c.scheme).into_owned(),
                    )
                })
            } else {
                None
            };
            let Some((default_kid, scheme)) = cenc else {
                continue;
            };
            protected_pads += 1;

            let Some(kid) = kid_to_uuid(&default_kid) else {
                gst::warning!(
                    CAT,
                    obj = p,
                    "encrypted CMAF track with no usable tenc default_KID ({} bytes); publishing it without contentProtections",
                    default_kid.len()
                );
                continue;
            };

            if systems.is_empty() {
                gst::warning!(
                    CAT,
                    obj = p,
                    "CMAF track is {}-encrypted (KID {}) but no drm-systems are configured: the catalog will carry no contentProtections and a subscriber has no way to obtain a key",
                    scheme,
                    kid
                );
                continue;
            }

            let mut refs = Vec::new();
            for sys in &systems {
                // Reuse an entry when another pad shares this key and system.
                let found = ts.cp_entries.iter().position(|e| {
                    e.kid_uuid == kid && e.scheme == scheme && e.system_id == sys.system_id
                });
                let idx = found.unwrap_or_else(|| {
                    ts.cp_entries.push(CpEntry {
                        ref_id: format!("cp{}", ts.cp_entries.len()),
                        kid_uuid: kid.clone(),
                        scheme: scheme.clone(),
                        ..sys.clone()
                    });
                    ts.cp_entries.len() - 1
                });
                refs.push(ts.cp_entries[idx].ref_id.clone());
            }
            s.pend_cp_ref_ids = Some(refs);
            gst::info!(
                CAT,
                obj = p,
                "CMAF track is {}-encrypted, KID {}, offered under {} DRM system(s)",
                scheme,
                kid,
                systems.len()
            );
        }

        if protected_pads == 0 && !systems.is_empty() {
            gst::warning!(
                CAT,
                imp = self,
                "drm-systems is set but no track carries CENC boxes; the catalog will claim no content protection"
            );
        }
    }

    /// Connect the endpoint and attach the media sender. Deferred out of
    /// start so prepared tracks can be attached/added in one posted task
    /// before the first sender hook; see await_tracks. A relay that is down
    /// is therefore reported on the first buffer rather than on the state
    /// change. Caller holds the track lock.
    pub(crate) fn open_sender(&self, ts: &mut TrackState) -> bool {
        let s = self.settings();
        let Some(ns) = self.namespace.lock().unwrap().clone() else {
            return false;
        };

        let url = relay_url(&s.host, s.port, &s.relay_path);
        let ep = match Endpoint::connect(&endpoint_config(&url, s.insecure, s.draft)) {
            Ok(ep) => ep,
            Err(e) => {
                gst::element_imp_error!(
                    self,
                    gst::ResourceError::OpenWrite,
                    (
                        "could not open MoQ endpoint to relay {}:{} (rc={})",
                        s.host,
                        s.port,
                        e.code()
                    )
                );
                *self.namespace.lock().unwrap() = None;
                return false;
            }
        };

        // CMSF 4 root contentProtections, authored by commit_tracks before the
        // pads were registered (the track cfgs reference these entries by
        // refID). The sender cfg carries them, so they are frozen with the
        // rest of the initial catalog at attach. Each entry gets a
        // single-element defaultKID array -- CMSF allows several KIDs per
        // entry, but one init segment declares exactly one tenc default_KID.
        let cps: Vec<ContentProtection> = ts
            .cp_entries
            .iter()
            .map(|e| ContentProtection {
                ref_id: e.ref_id.clone(),
                default_kids: vec![e.kid_uuid.clone()],
                scheme: e.scheme.clone(),
                drm_system: DrmSystem {
                    system_id: e.system_id.clone(),
                    la_url: e.la_url.clone().map(CmsfUrl::new),
                    cert_url: e.cert_url.clone().map(CmsfUrl::new),
                    auth_url: e.auth_url.clone().map(CmsfUrl::new),
                    pssh: e.pssh.clone(),
                    robustness: e.robustness.clone(),
                },
            })
            .collect();
        let cp_count = cps.len();

        let cfg = SenderCfg::live(ns)
            // Push (contribution) mode: PUBLISH every track and write the
            // catalog live so a relay that expects a publisher-initiated
            // PUBLISH picks the stream up. Off by default -- announce-and-wait
            // is what red5-moq-relay serves today.
            .publish_tracks(s.publish_tracks)
            // Republish the catalog on a timer only when explicitly requested
            // for relay cache compatibility. Initial catalog completeness is
            // provided by the atomic startup task, not by periodic publication.
            //
            // Set it only for a relay that does not pull that retained group
            // on a late subscriber's behalf. MoQT permits a relay to answer a
            // FETCH from its own cache alone and to be "catalog-blind", and
            // libmoq documents the retained group as "NOT a relay-safe catalog
            // solution" for exactly that reason, so such a relay delivers no
            // catalog at all to a late joiner -- red5-moq-relay 1.6.40/1.7.8
            // answer the joining FETCH with REQUEST_ERROR 17. The periodic
            // republish sidesteps it by making the catalog ordinary live
            // content, which any relay forwards.
            .catalog_refresh_interval(Duration::from_millis(s.catalog_refresh_ms as u64))
            .content_protections(cps);

        // Attach, then add every prepared track before the namespace can be
        // accepted, so the initial catalog carries them all. libmoq publishes
        // the catalog on acceptance -- a network round trip away -- while
        // attach and add_track are local calls, so the tracks are in place
        // first in practice. It is no longer guaranteed by construction: the
        // posted endpoint task that made it atomic needs moq_endpoint_post,
        // which the moq5 crate no longer exposes. A track added after
        // readiness only republishes the catalog, so losing the race costs a
        // transient one-track catalog, not a wrong one.
        let (pads, tracks): (Vec<MoqSinkPad>, Vec<TrackCfg>) = ts
            .preparing_tracks
            .take()
            .unwrap_or_default()
            .into_iter()
            .unzip();
        // Dropping the sender on any early return destroys what was attached,
        // which is what the startup task's cancel used to settle.
        let started = (|| {
            let sender = Sender::attach(&ep, &cfg).map_err(|e| e.code())?;
            let mut handles = Vec::with_capacity(tracks.len());
            for track in &tracks {
                handles.push(sender.add_track(track).map_err(|e| e.code())?);
            }
            let deadline = clock::monotonic_us() + EOS_DRAIN_TIMEOUT_US;
            let ready_ns = s.namespace.clone();
            while !sender.is_ready() {
                let cancelled = {
                    let st = self.obj_lock();
                    st.state_flushing || st.flush_count > 0
                };
                if cancelled {
                    return Err(MOQ_ERR_INTERRUPTED);
                }
                let now = clock::monotonic_us();
                if now >= deadline {
                    return Err(MOQ_DONE);
                }
                let wait = (deadline - now).clamp(0, 5000) as u64;
                sender
                    .wait(Duration::from_micros(wait))
                    .map_err(|e| e.code())?;
            }
            // The readiness record operators look for. libmoq used to deliver
            // it as a callback; it is polled now, so it is logged once here,
            // where readiness is first observed.
            gst::info!(CAT, "native sender ready namespace={}", ready_ns);
            Ok((sender, handles))
        })();
        let sender = match started {
            Ok((sender, tracks)) => {
                for (pad, track) in pads.iter().zip(tracks) {
                    let _st = self.obj_lock();
                    pad.set_track(Some(track));
                }
                sender
            }
            Err(rc) => {
                // The last handle: stopping completes a queued startup task
                // with no session before the endpoint goes.
                drop(ep);
                gst::element_imp_error!(
                    self,
                    gst::ResourceError::OpenWrite,
                    (
                        "could not prepare MoQ sender (rc={}){}",
                        rc,
                        if cp_count > 0 {
                            " -- check drm-systems: a systemID must be a \"8-4-4-4-12\" UUID string"
                        } else {
                            ""
                        }
                    )
                );
                *self.namespace.lock().unwrap() = None;
                return false;
            }
        };
        if cp_count > 0 {
            gst::info!(
                CAT,
                imp = self,
                "catalog carries {} contentProtections entr{}",
                cp_count,
                if cp_count == 1 { "y" } else { "ies" }
            );
        }
        {
            let mut send = self.send_lock();
            send.sender = Some(sender);
            send.sender_generation += 1;
            let mut st = self.obj_lock();
            ep.set_interrupted(st.state_flushing || st.flush_count > 0);
            st.ep = Some(ep);
        }

        gst::info!(
            CAT,
            imp = self,
            "started: publishing ns={} via relay {}:{}{}",
            s.namespace,
            s.host,
            s.port,
            s.relay_path
        );
        true
    }

    fn stop(&self) {
        if !self.started() {
            return;
        }

        {
            let mut send = self.send_lock();
            let ep = {
                let mut st = self.obj_lock();
                let ep = st.ep.take();
                if let Some(ep) = &ep {
                    ep.set_interrupted(true);
                }
                ep
            };

            if let Some(sender) = send.sender.take() {
                // get_stats and destroying the sender both happen inside the
                // send lock, so a concurrent streaming-thread call (which only
                // ever reaches the sender under this same lock) can never see
                // a destroyed one. Destroying settles its callbacks first.
                let stats = sender.stats();
                drop(sender);
                if let Ok(stats) = stats {
                    gst::info!(
                        CAT,
                        imp = self,
                        "sender stats: written={} sent={} queued={} dropped={}",
                        stats.objects_written,
                        stats.objects_sent,
                        stats.objects_queued,
                        stats.objects_dropped
                    );
                }
            }
            send.end_requests.clear();
            // The last endpoint handle: stops and destroys it.
            drop(ep);
        }

        let pads = self.sink_pads();
        {
            let _st = self.obj_lock();
            for p in &pads {
                p.set_track(None);
            }
        }

        *self.namespace.lock().unwrap() = None;
        self.track_lock().cp_entries.clear();
        self.started.store(false, Ordering::SeqCst);
        gst::info!(CAT, imp = self, "stopped");
    }
}

#[glib::object_subclass]
impl ObjectSubclass for MoqSink {
    const NAME: &'static str = "GstMoqSink";
    type Type = super::MoqSink;
    type ParentType = gst::Element;
}

impl ObjectImpl for MoqSink {
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
                    .blurb("MoQ track name for the always (LOC) sink pad")
                    .default_value(Some(DEFAULT_TRACK_NAME))
                    .build(),
                glib::ParamSpecString::builder("codec")
                    .nick("Codec")
                    .blurb(
                        "Catalog codec string for the LOC track \
                         (default: derived from the stream's SPS, e.g. avc1.42c01e)",
                    )
                    .build(),
                glib::ParamSpecUInt64::builder("bitrate")
                    .nick("Bitrate")
                    .blurb("Advertised catalog max bitrate for the LOC track (bits/s)")
                    .minimum(1)
                    .maximum(u64::MAX)
                    .default_value(DEFAULT_BITRATE)
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
                glib::ParamSpecBoolean::builder("sync")
                    .nick("Sync")
                    .blurb("Pace buffers against the pipeline clock")
                    .default_value(true)
                    .build(),
                glib::ParamSpecBoolean::builder("sap-timeline")
                    .nick("SAP timeline")
                    .blurb("Publish a CMSF SAP event timeline track for every CMAF track")
                    .default_value(false)
                    .build(),
                glib::ParamSpecBoolean::builder("publish-tracks")
                    .nick("Publish tracks")
                    .blurb(
                        "Push mode: PUBLISH each track and write the catalog without waiting \
                         for a subscriber. Off means announce and wait for demand.",
                    )
                    .default_value(false)
                    .build(),
                glib::ParamSpecUInt::builder("catalog-refresh-ms")
                    .nick("Catalog refresh interval")
                    .blurb(
                        "Republish the catalog every N ms. 0 disables it, which is what \
                         MSF-01 5 asks for (republish on track-availability change): the \
                         element already holds the initial catalog until every linked pad \
                         has a track. Needed only for a relay that cannot serve the joining \
                         FETCH that fetches the retained catalog (red5-moq-relay 1.6.40 and \
                         1.7.8): there, keep it under their ~2000 ms park window.",
                    )
                    .minimum(0)
                    .maximum(u32::MAX)
                    .default_value(0)
                    .build(),
                glib::ParamSpecUInt::builder("catalog-wait-ms")
                    .nick("Initial catalog wait")
                    .blurb(
                        "How long the first pad to produce a track waits for the remaining \
                         linked pads before reporting an error without opening a connection. \
                         Bounds a linked pad that never delivers; a pad that reaches EOS or \
                         flushes drops out at once.",
                    )
                    .minimum(0)
                    .maximum(u32::MAX)
                    .default_value(DEFAULT_CATALOG_WAIT_MS)
                    .build(),
                glib::ParamSpecUInt::builder("loc-audio-group-ms")
                    .nick("LOC audio group length")
                    .blurb(
                        "How often the LOC audio track opens a new group (ms). A subscriber \
                         joins each track at its next group boundary, so this together with \
                         the video GOP decides which track's first object reaches a new \
                         subscriber first.",
                    )
                    .minimum(1)
                    .maximum(u32::MAX)
                    .default_value(LOC_AUDIO_GROUP_US / 1000)
                    .build(),
                glib::ParamSpecString::builder("drm-systems")
                    .nick("DRM systems")
                    .blurb(
                        "DRM systems to advertise for CENC-encrypted CMAF tracks, as the \
                         catalog's CMSF root contentProtections (CMSF 4.1.1). Semicolon \
                         separates systems; within one, the first field is the systemID UUID \
                         and the rest are optional key=value fields: laurl, certurl, authurl, \
                         pssh (base64), robustness. Values may not contain ';' or ','.\n\
                         The scheme and defaultKID are NOT configured here -- they are read \
                         from the init segment's own sinf/schm/schi(tenc) boxes, so this \
                         carries only what an fMP4 cannot: which systems the stream is \
                         offered under and how to reach their licences. One entry is authored \
                         per (scheme, KID, system), and a protected track references every \
                         entry for its key.\n\
                         Example: \
                         \"edef8ba9-79d6-4ace-a3c8-27dcd51d21ed,laurl=https://lic/wv,\
                         pssh=AAAA...;9a04f079-9840-4286-ab92-e65be0885f95,\
                         laurl=https://lic/pr\".\n\
                         LOC tracks are unaffected: LOC carries no CENC metadata. Leaving \
                         this unset while a track IS encrypted publishes ciphertext with no \
                         way to obtain a key, and is warned about.",
                    )
                    .build(),
                glib::ParamSpecUInt64::builder("max-fragment-size")
                    .nick("Max fragment size")
                    .blurb("Largest single ISO BMFF box accepted on a CMAF pad (bytes)")
                    .minimum(1024)
                    .maximum(u32::MAX as u64)
                    .default_value(DEFAULT_MAX_FRAGMENT)
                    .build(),
            ]
        });
        PROPS.as_ref()
    }

    fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
        let string = || value.get::<Option<String>>().unwrap();
        match pspec.name() {
            // forwarded to the always pad
            "track-name" | "bitrate" => self
                .locpad
                .get()
                .unwrap()
                .set_property_from_value(pspec.name(), value),
            name => {
                let mut s = self.settings.lock().unwrap();
                match name {
                    "host" => s.host = string().unwrap_or_default(),
                    "port" => s.port = value.get().unwrap(),
                    "relay-path" => s.relay_path = string().unwrap_or_default(),
                    "insecure" => s.insecure = value.get().unwrap(),
                    "namespace" => s.namespace = string().unwrap_or_default(),
                    "codec" => s.codec = string(),
                    "draft" => s.draft = value.get().unwrap(),
                    "sync" => s.sync = value.get().unwrap(),
                    "sap-timeline" => s.sap_timeline = value.get().unwrap(),
                    "publish-tracks" => s.publish_tracks = value.get().unwrap(),
                    "catalog-refresh-ms" => s.catalog_refresh_ms = value.get().unwrap(),
                    "catalog-wait-ms" => s.catalog_wait_ms = value.get().unwrap(),
                    "loc-audio-group-ms" => s.loc_audio_group_ms = value.get().unwrap(),
                    "drm-systems" => s.drm_systems = string(),
                    "max-fragment-size" => s.max_fragment_size = value.get().unwrap(),
                    _ => unimplemented!(),
                }
            }
        }
    }

    fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
        if let "track-name" | "bitrate" = pspec.name() {
            return self.locpad.get().unwrap().property_value(pspec.name());
        }
        let s = self.settings.lock().unwrap();
        match pspec.name() {
            "host" => s.host.to_value(),
            "port" => s.port.to_value(),
            "relay-path" => s.relay_path.to_value(),
            "insecure" => s.insecure.to_value(),
            "namespace" => s.namespace.to_value(),
            "codec" => s.codec.to_value(),
            "draft" => s.draft.to_value(),
            "sync" => s.sync.to_value(),
            "sap-timeline" => s.sap_timeline.to_value(),
            "publish-tracks" => s.publish_tracks.to_value(),
            "catalog-refresh-ms" => s.catalog_refresh_ms.to_value(),
            "catalog-wait-ms" => s.catalog_wait_ms.to_value(),
            "loc-audio-group-ms" => s.loc_audio_group_ms.to_value(),
            "drm-systems" => s.drm_systems.to_value(),
            "max-fragment-size" => s.max_fragment_size.to_value(),
            _ => unimplemented!(),
        }
    }

    fn constructed(&self) {
        self.parent_constructed();
        let obj = self.obj();
        let klass = obj.element_class();

        let templ = klass.pad_template("video").unwrap();
        let video = PadKind {
            media_type: MediaType::Video,
            packaging: Packaging::Raw,
        };
        let pad = self.new_pad(
            &templ,
            "video",
            video,
            DEFAULT_TRACK_NAME,
            DEFAULT_BITRATE,
            DEFAULT_MAX_FRAGMENT as usize,
        );
        obj.add_pad(&pad).unwrap();
        self.locpad.set(pad).unwrap();

        let templ = klass.pad_template("audio").unwrap();
        let audio = PadKind {
            media_type: MediaType::Audio,
            packaging: Packaging::Raw,
        };
        let pad = self.new_pad(
            &templ,
            "audio",
            audio,
            DEFAULT_AUDIO_TRACK_NAME,
            DEFAULT_AUDIO_BITRATE,
            DEFAULT_MAX_FRAGMENT as usize,
        );
        obj.add_pad(&pad).unwrap();
        self.locpad_audio.set(pad).unwrap();

        obj.set_element_flags(gst::ElementFlags::SINK);
    }
}

impl GstObjectImpl for MoqSink {}

impl ElementImpl for MoqSink {
    fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
        static META: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
            gst::subclass::ElementMetadata::new(
                "MoQ sink",
                "Sink/Network",
                "Publish LOC and CMAF tracks over Media-over-QUIC (libmoq service tier)",
                "Ray L <ray@raylucke.com>",
            )
        });
        Some(&*META)
    }

    fn pad_templates() -> &'static [gst::PadTemplate] {
        static TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
            let t = |name, presence, caps: &str| {
                gst::PadTemplate::with_gtype(
                    name,
                    gst::PadDirection::Sink,
                    presence,
                    &caps.parse::<gst::Caps>().unwrap(),
                    MoqSinkPad::static_type(),
                )
                .unwrap()
            };
            vec![
                t(
                    "video",
                    gst::PadPresence::Always,
                    "video/x-h264, stream-format = (string) { avc, byte-stream }, alignment = (string) au",
                ),
                // The second always pad: AAC access units, one LOC object per
                // frame. Raw AAC (no ADTS) because LOC carries the decoder
                // config out of band, in the catalog's initData, exactly as
                // the video pad carries avcC there.
                t(
                    "audio",
                    gst::PadPresence::Always,
                    "audio/mpeg, mpegversion = (int) 4, stream-format = (string) raw",
                ),
                t("video_%u", gst::PadPresence::Request, "video/quicktime"),
                t("audio_%u", gst::PadPresence::Request, "video/quicktime"),
            ]
        });
        TEMPLATES.as_ref()
    }

    fn change_state(
        &self,
        transition: gst::StateChange,
    ) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
        match transition {
            gst::StateChange::NullToReady => {
                if !self.start() {
                    return Err(gst::StateChangeError);
                }
            }
            gst::StateChange::ReadyToPaused => {
                if !self.started() && !self.start() {
                    return Err(gst::StateChangeError);
                }
            }
            gst::StateChange::PausedToReady => {
                let pads = self.sink_pads();
                {
                    let mut st = self.obj_lock();
                    st.state_flushing = true;
                    for p in &pads {
                        p.set_flushing(true);
                        if let Some(id) = p.clock_id().as_ref() {
                            id.unschedule();
                        }
                    }
                    if let Some(ep) = &st.ep {
                        ep.set_interrupted(true);
                    }
                }
                // Release any pad parked in the track barrier: going to READY
                // must not leave a streaming thread waiting for a track that
                // will never arrive. The track lock is taken OUTSIDE the
                // object-state lock, so this follows the unlock.
                self.notify_tracks();
            }
            _ => {}
        }

        let ret = self.parent_change_state(transition);

        match transition {
            gst::StateChange::ReadyToPaused => {
                let pads = self.sink_pads();
                let mut st = self.obj_lock();
                st.state_flushing = false;
                for p in &pads {
                    p.set_flushing(p.flush_pending());
                }
                st.eos_posted = false;
                if let Some(ep) = &st.ep {
                    ep.set_interrupted(st.flush_count > 0);
                }
            }
            gst::StateChange::PausedToReady => {
                if ret.is_ok() {
                    self.stop();
                }
            }
            gst::StateChange::ReadyToNull => self.stop(),
            _ => {}
        }
        ret
    }

    /// A plain GstElement has no source pads, so the base send_event() has
    /// nothing to route events sent to a sink element to. GST_EVENT_LATENCY
    /// is the one that matters here: it is flagged UPSTREAM, so sending it to
    /// any of our (sink-direction) pads would be refused ("wrong direction")
    /// without ever calling the pad's event function -- it must be handled
    /// directly, exactly like GstBaseSink does internally. Anything else is
    /// pushed onto every sink pad, which invokes that pad's event function
    /// synchronously (e.g. a flushing seek or a forced EOS sent straight to
    /// the element rather than flowing through the pipeline).
    fn send_event(&self, event: gst::Event) -> bool {
        if let gst::EventView::Latency(l) = event.view() {
            let latency = l.latency();
            self.obj_lock().latency = latency;
            gst::debug!(CAT, imp = self, "latency configured: {}", latency);
            return true;
        }

        let mut ret = false;
        for p in self.sink_pads() {
            if p.send_event(event.clone()) {
                ret = true;
            }
        }
        ret
    }

    fn request_new_pad(
        &self,
        templ: &gst::PadTemplate,
        name: Option<&str>,
        _caps: Option<&gst::Caps>,
    ) -> Option<gst::Pad> {
        let template = templ.name_template();
        let is_audio = template.starts_with("audio");
        let padname = match name {
            Some(n) => n.to_string(),
            None => {
                let serial = self.pad_serial.fetch_add(1, Ordering::SeqCst);
                template.replace("%u", &serial.to_string())
            }
        };
        let max_fragment = self.settings.lock().unwrap().max_fragment_size as usize;
        let kind = PadKind {
            media_type: if is_audio {
                MediaType::Audio
            } else {
                MediaType::Video
            },
            packaging: Packaging::Cmaf,
        };
        let pad = self.new_pad(
            templ,
            &padname,
            kind,
            if is_audio { "audio" } else { "video" },
            if is_audio {
                DEFAULT_AUDIO_BITRATE
            } else {
                DEFAULT_BITRATE
            },
            max_fragment,
        );

        let _ = pad.set_active(true);
        self.obj().add_pad(&pad).ok()?;
        gst::info!(
            CAT,
            obj = pad,
            "requested CMAF {} pad",
            if is_audio { "audio" } else { "video" }
        );
        Some(pad.upcast())
    }

    fn release_pad(&self, pad: &gst::Pad) {
        let pad = pad
            .downcast_ref::<MoqSinkPad>()
            .expect("moqsink pads are MoqSinkPads");
        self.unschedule(pad, true);
        let _ = pad.set_active(false);
        self.set_pad_flush(pad, false);
        let deadline = clock::monotonic_us() + EOS_DRAIN_TIMEOUT_US;
        let rc = self.end_track(pad, deadline);
        if rc != MOQ_OK && rc != MOQ_ERR_INTERRUPTED {
            self.eos_error(rc);
        }
        let _ = self.obj().remove_pad(pad);
        if self.claim_eos() {
            self.drain(deadline);
        }
    }
}

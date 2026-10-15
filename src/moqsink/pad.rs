//! The sink pad: per-track state for moqsink.
//!
//! Locking. Flags other threads read to make barrier/EOS decisions (`eos`,
//! `flushing`, `flush_pending`, `pend_ready`, `track`) are atomics; they are
//! only WRITTEN while holding the element's object-state lock, which is what
//! makes a multi-pad scan under that lock consistent. Everything else lives in
//! `state`, which is only ever held briefly -- never across a clock wait, the
//! track barrier, or another pad's lock.

use gst::glib;
use gst::prelude::*;
use gst::subclass::prelude::*;

use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard, OnceLock};

use crate::fmp4::Splitter;
use crate::moq::*;

glib::wrapper! {
    pub struct MoqSinkPad(ObjectSubclass<imp::MoqSinkPad>) @extends gst::Pad, gst::Object;
}

/// Fixed at request time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PadKind {
    pub media_type: moq_media_type_t,
    pub packaging: moq_media_packaging_t,
}

impl PadKind {
    pub fn is_audio(&self) -> bool {
        self.media_type == MOQ_MEDIA_TYPE_AUDIO
    }
    pub fn is_cmaf(&self) -> bool {
        self.packaging == MOQ_MEDIA_PACKAGING_CMAF
    }
}

/// A parsed init segment's info. Its spans borrow from `PadState::init`.
pub struct InitInfo(pub moq_cmaf_init_info_t);
// SAFETY: the spans point into the pad's own `init` bytes, which move with it.
unsafe impl Send for InitInfo {}

impl Default for InitInfo {
    fn default() -> Self {
        let mut i = moq_cmaf_init_info_t::default();
        unsafe { moq_cmaf_init_info_init(&mut i) };
        InitInfo(i)
    }
}

pub struct PadState {
    pub segment: gst::Segment,

    /// LOC (always pads)
    pub base_pts: Option<gst::ClockTime>,
    /// video caps
    pub width: i32,
    pub height: i32,
    pub fps_n: i32,
    pub fps_d: i32,
    /// audio caps
    pub rate: i32,
    pub channels: i32,
    /// video: length-prefixed, not Annex B
    pub avc: bool,
    /// video avcC / audio AudioSpecificConfig, from caps
    pub codec_data: Option<Vec<u8>>,
    /// audio: start of the open group (us)
    pub group_start: Option<u64>,
    /// audio: the video group-start PTS this pad has already aligned to, so
    /// each video group opens exactly one audio group (see chain_loc).
    pub aligned_to_video_pts: Option<gst::ClockTime>,

    /// Initial-catalog barrier. Everything add_track needs is derived at this
    /// pad's first keyframe and stashed here, so the sender can be attached
    /// only once EVERY linked pad has one and all tracks can be added before
    /// the session establishes -- otherwise a pad whose caps arrive late is
    /// absent from the initial catalog (see MoqSink::await_tracks).
    pub pend_codec: Option<String>,
    /// LOC video: avcC
    pub pend_avcc: Option<Vec<u8>>,
    /// LOC audio: channelConfig
    pub pend_chan: Option<String>,
    /// CMAF: this track's CMSF contentProtectionRefIDs (CMSF 4.1.2), resolved
    /// at the barrier from the init segment's CENC boxes and the element's
    /// drm-systems. None when the track is not encrypted, or when it is but no
    /// DRM system is configured to describe it.
    pub pend_cp_ref_ids: Option<Vec<String>>,

    /// CMAF (request pads)
    pub splitter: Splitter,
    /// ftyp + moov as sent to the catalog
    pub init: Option<Vec<u8>>,
    /// borrows from `init`
    pub init_info: InitInfo,
    pub has_init_info: bool,
    /// one-shot "skipping non-media boxes" log
    pub skip_logged: bool,
}

impl PadState {
    fn new(max_box: usize) -> Self {
        PadState {
            segment: gst::Segment::new(),
            base_pts: None,
            width: 0,
            height: 0,
            fps_n: 0,
            fps_d: 0,
            rate: 0,
            channels: 0,
            avc: false,
            codec_data: None,
            group_start: None,
            aligned_to_video_pts: None,
            pend_codec: None,
            pend_avcc: None,
            pend_chan: None,
            pend_cp_ref_ids: None,
            splitter: Splitter::new(max_box),
            init: None,
            init_info: InitInfo::default(),
            has_init_info: false,
            skip_logged: false,
        }
    }
}

impl MoqSinkPad {
    pub fn kind(&self) -> PadKind {
        *self.imp().kind.get().expect("pad kind is set at creation")
    }

    pub fn state(&self) -> MutexGuard<'_, PadState> {
        self.imp().state.lock().unwrap()
    }

    pub fn track_name(&self) -> String {
        self.imp().settings.lock().unwrap().track_name.clone()
    }

    pub fn bitrate(&self) -> u64 {
        self.imp().settings.lock().unwrap().bitrate
    }

    pub fn track(&self) -> Handle<moq_media_track_t> {
        Handle(self.imp().track.load(Ordering::SeqCst))
    }
    pub fn set_track(&self, t: Handle<moq_media_track_t>) {
        self.imp().track.store(t.get(), Ordering::SeqCst)
    }

    pub fn eos(&self) -> bool {
        self.imp().eos.load(Ordering::SeqCst)
    }
    pub fn set_eos(&self, v: bool) {
        self.imp().eos.store(v, Ordering::SeqCst)
    }
    pub fn flushing(&self) -> bool {
        self.imp().flushing.load(Ordering::SeqCst)
    }
    pub fn set_flushing(&self, v: bool) {
        self.imp().flushing.store(v, Ordering::SeqCst)
    }
    pub fn flush_pending(&self) -> bool {
        self.imp().flush_pending.load(Ordering::SeqCst)
    }
    pub fn set_flush_pending(&self, v: bool) {
        self.imp().flush_pending.store(v, Ordering::SeqCst)
    }
    pub fn pend_ready(&self) -> bool {
        self.imp().pend_ready.load(Ordering::SeqCst)
    }
    pub fn set_pend_ready(&self, v: bool) {
        self.imp().pend_ready.store(v, Ordering::SeqCst)
    }
    pub fn objects_sent(&self) -> u64 {
        self.imp().objects_sent.load(Ordering::SeqCst)
    }
    pub fn count_sent(&self) {
        self.imp().objects_sent.fetch_add(1, Ordering::SeqCst);
    }

    /// Pending sync wait. Accessed under the element's object-state lock.
    pub fn clock_id(&self) -> MutexGuard<'_, Option<gst::SingleShotClockId>> {
        self.imp().clock_id.lock().unwrap()
    }

    pub(crate) fn init_kind(
        &self,
        kind: PadKind,
        track_name: &str,
        bitrate: u64,
        max_fragment_size: usize,
    ) {
        let imp = self.imp();
        imp.kind.set(kind).expect("pad kind set twice");
        {
            let mut s = imp.settings.lock().unwrap();
            s.track_name = track_name.into();
            s.bitrate = bitrate;
        }
        *imp.state.lock().unwrap() = PadState::new(max_fragment_size);
    }

    /// Reset per-stream state (called for a fresh sender lifetime).
    pub fn reset(&self) {
        self.set_track(Handle::null());
        self.set_eos(false);
        self.set_flushing(false);
        self.set_flush_pending(false);
        self.set_pend_ready(false);
        self.imp().objects_sent.store(0, Ordering::SeqCst);
        let mut s = self.state();
        s.base_pts = None;
        s.group_start = None;
        s.aligned_to_video_pts = None;
        s.pend_codec = None;
        s.pend_avcc = None;
        s.pend_chan = None;
        s.pend_cp_ref_ids = None;
        let max_box = s.splitter.max_box();
        s.splitter = Splitter::new(max_box);
        s.init = None;
        s.has_init_info = false;
        s.skip_logged = false;
        s.segment = gst::Segment::new();
    }
}

pub mod imp {
    use super::*;

    pub struct Settings {
        pub track_name: String,
        pub bitrate: u64,
    }

    pub struct MoqSinkPad {
        pub(super) settings: Mutex<Settings>,
        pub(super) kind: OnceLock<PadKind>,
        pub(super) eos: AtomicBool,
        pub(super) flushing: AtomicBool,
        pub(super) flush_pending: AtomicBool,
        pub(super) pend_ready: AtomicBool,
        /// null until the track is added
        pub(super) track: AtomicPtr<moq_media_track_t>,
        pub(super) objects_sent: AtomicU64,
        pub(super) clock_id: Mutex<Option<gst::SingleShotClockId>>,
        pub(super) state: Mutex<PadState>,
    }

    impl Default for MoqSinkPad {
        fn default() -> Self {
            MoqSinkPad {
                settings: Mutex::new(Settings {
                    track_name: String::new(),
                    bitrate: 2_000_000,
                }),
                kind: OnceLock::new(),
                eos: AtomicBool::new(false),
                flushing: AtomicBool::new(false),
                flush_pending: AtomicBool::new(false),
                pend_ready: AtomicBool::new(false),
                track: AtomicPtr::new(std::ptr::null_mut()),
                objects_sent: AtomicU64::new(0),
                clock_id: Mutex::new(None),
                state: Mutex::new(PadState::new(16 << 20)),
            }
        }
    }

    #[glib::object_subclass]
    impl ObjectSubclass for MoqSinkPad {
        const NAME: &'static str = "GstMoqSinkPad";
        type Type = super::MoqSinkPad;
        type ParentType = gst::Pad;
    }

    impl ObjectImpl for MoqSinkPad {
        fn properties() -> &'static [glib::ParamSpec] {
            static PROPS: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| {
                vec![
                    glib::ParamSpecString::builder("track-name")
                        .nick("Track name")
                        .blurb("MoQ track name published for this pad")
                        .build(),
                    glib::ParamSpecUInt64::builder("bitrate")
                        .nick("Bitrate")
                        .blurb("Advertised catalog max bitrate for this pad (bits/s)")
                        .minimum(1)
                        .maximum(u64::MAX)
                        .default_value(2_000_000)
                        .build(),
                ]
            });
            PROPS.as_ref()
        }

        fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
            let mut s = self.settings.lock().unwrap();
            match pspec.name() {
                "track-name" => {
                    s.track_name = value.get::<Option<String>>().unwrap().unwrap_or_default()
                }
                "bitrate" => s.bitrate = value.get().unwrap(),
                _ => unimplemented!(),
            }
        }

        fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
            let s = self.settings.lock().unwrap();
            match pspec.name() {
                "track-name" => s.track_name.to_value(),
                "bitrate" => s.bitrate.to_value(),
                _ => unimplemented!(),
            }
        }
    }

    impl GstObjectImpl for MoqSinkPad {}
    impl PadImpl for MoqSinkPad {}
}

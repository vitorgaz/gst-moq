//! Exercise the source's actual create vmethod with deterministic public-API
//! mocks. No relay, network thread, sleeps, or production test hooks.
//!
//! Port of tests/test_src_delivery.c.

use gst::prelude::*;
use gst::subclass::prelude::*;

use super::MoqSrc;
use crate::service::fake::{control, suite, DescData, Event, ObjectData, Receiver};
use crate::service::Track;
use moq5::{Error, Packaging, TerminalReason, TrackEventKind};

/// Fixture track handles. The double indexes its own state by them, so any
/// distinct values do.
const VIDEO: Track = Track(0);
const AUDIO: Track = Track(1);
const VIDEO_PAYLOAD: [u8; 5] = [0, 0, 1, 0x65, 0x42];
const AUDIO_PAYLOAD: [u8; 2] = [0xf8, 0xff];

/// A source holding the double's receiver, past discovery and negotiation.
fn make_source() -> MoqSrc {
    let src = gst::glib::Object::new::<MoqSrc>();
    src.imp().settings.lock().unwrap().caps = Some(gst::Caps::new_any());
    let mut st = src.imp().lock_state();
    st.receiver = Some(Receiver);
    st.caps_pushed = true; // Negotiation is exercised separately by pipelines.
    st.discover_deadline = i64::MAX;
    drop(st);
    src
}

fn queue_object(track: Track, payload: &[u8]) {
    control().objects.push(ObjectData {
        track: Some(track),
        is_keyframe: true,
        payload: payload.to_vec(),
        ..Default::default()
    });
}

fn create(src: &MoqSrc) -> Result<gst::Buffer, gst::FlowError> {
    src.imp().create_buffer()
}

fn added(track: Track, desc: DescData) -> Event {
    Event {
        kind: TrackEventKind::Added,
        raw_kind: 0,
        track: Some(track),
        desc: Some(desc),
    }
}

fn terminal(kind: TrackEventKind, track: Track) -> Event {
    Event {
        kind,
        raw_kind: 0,
        track: Some(track),
        desc: None,
    }
}

fn desc(name: &str, codec: &str) -> DescData {
    DescData {
        name: Some(name.as_bytes().to_vec()),
        codec: codec.as_bytes().to_vec(),
        ..Default::default()
    }
}

fn contents(buf: &gst::Buffer) -> Vec<u8> {
    buf.map_readable().unwrap().as_slice().to_vec()
}

// -- cases -----------------------------------------------------------------------

#[test]
fn missing_track() {
    let _suite = suite();
    let src = make_source();
    queue_object(AUDIO, &AUDIO_PAYLOAD);
    control().empty_result = Some(Error::CLOSED);
    assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
    assert_eq!(control().cleaned, 1);
}

fn selected_end_drains_media(event_kind: TrackEventKind) {
    let _suite = suite();
    let src = make_source();
    let video = desc("video", "avc1.42c01e");
    let audio = DescData {
        name: Some(b"audio".to_vec()),
        ..Default::default()
    };
    control().events = vec![
        added(AUDIO, audio),
        added(VIDEO, video),
        terminal(event_kind, VIDEO),
    ];
    queue_object(AUDIO, &AUDIO_PAYLOAD);
    queue_object(VIDEO, &VIDEO_PAYLOAD);
    queue_object(VIDEO, &VIDEO_PAYLOAD);

    for _ in 0..2 {
        let out = create(&src).expect("a video buffer");
        assert_eq!(contents(&out), VIDEO_PAYLOAD);
    }
    assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
    let c = control();
    assert_eq!(c.cleaned, 3);
    assert_eq!(c.waits, 0);
    assert_eq!(c.subscriptions, 1);
    assert_eq!(c.subscribed_track, Some(VIDEO));
}

#[test]
fn selected_end_drains() {
    selected_end_drains_media(TrackEventKind::Ended);
}

#[test]
fn selected_removal_drains() {
    selected_end_drains_media(TrackEventKind::Removed);
}

#[test]
fn unrelated_end() {
    let _suite = suite();
    let src = make_source();
    src.imp().lock_state().want_track = Some(VIDEO);
    control().events = vec![terminal(TrackEventKind::Ended, AUDIO)];
    assert_eq!(create(&src).err(), Some(gst::FlowError::Flushing));
    assert_eq!(control().waits, 1);
}

#[test]
fn discovery_deadline() {
    let _suite = suite();
    let src = make_source();
    let bus = gst::Bus::new();
    src.set_bus(Some(&bus));
    src.imp().lock_state().discover_deadline = gst::glib::monotonic_time() - 1;
    assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
    let msg = bus
        .pop_filtered(&[gst::MessageType::Error])
        .expect("error message");
    let gst::MessageView::Error(err) = msg.view() else {
        unreachable!()
    };
    assert!(err.error().matches(gst::ResourceError::NotFound));
    src.set_bus(None);
}

#[test]
fn subscription_failure() {
    let _suite = suite();
    let src = make_source();
    control().subscribe_result = Some(Error::CLOSED);
    let video = desc("video", "avc1.42c01e");
    control().events = vec![added(VIDEO, video)];
    assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
    assert!(src.imp().lock_state().want_track.is_none());
    assert_eq!(control().waits, 0);
}

#[test]
fn interrupt_before_timeout() {
    let _suite = suite();
    let src = make_source();
    src.imp().lock_state().discover_deadline = gst::glib::monotonic_time() - 1;
    control().empty_result = Some(Error::INTERRUPTED);
    assert_eq!(create(&src).err(), Some(gst::FlowError::Flushing));
}

fn cmaf_tail(kind: TrackEventKind) {
    let _suite = suite();
    let src = make_source();
    static INIT: [u8; 8] = [0, 0, 0, 8, b'f', b't', b'y', b'p'];
    static FRAGMENT: [u8; 8] = [0, 0, 0, 8, b'm', b'o', b'o', b'f'];
    let d = DescData {
        name: Some(b"video".to_vec()),
        packaging: Some(Packaging::Cmaf),
        init_data: INIT.to_vec(),
        ..Default::default()
    };
    let add = added(VIDEO, d);
    // A duplicate announcement must not resubscribe.
    control().events = vec![add.clone(), add, terminal(kind, VIDEO)];
    for _ in 0..2 {
        control().objects.push(ObjectData {
            track: Some(VIDEO),
            is_keyframe: true,
            packaging: Some(Packaging::Cmaf),
            fragment: FRAGMENT.to_vec(),
            ..Default::default()
        });
    }
    for i in 0..3 {
        let out = create(&src).expect("a buffer");
        assert_eq!(contents(&out), if i == 0 { INIT } else { FRAGMENT });
        assert_eq!(out.flags().contains(gst::BufferFlags::HEADER), i == 0);
    }
    assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
    let c = control();
    assert_eq!(c.subscriptions, 1);
    assert_eq!(c.cleaned, 2);
    assert_eq!(c.waits, 0);
}

#[test]
fn cmaf_ended_tail() {
    cmaf_tail(TrackEventKind::Ended);
}

#[test]
fn cmaf_removed_tail() {
    cmaf_tail(TrackEventKind::Removed);
}

/// mode 0: classified terminal; 1: the getter fails; 2: unclassified.
fn startup_terminal(mode: u32) {
    let _suite = suite();
    {
        let mut c = control();
        // The receiver refuses to attach, which is what sends the element
        // down the terminal-diagnostic path.
        c.receiver_attach_result = Some(Error::CLOSED);
        c.terminal_getter_fails = mode == 1;
        c.terminal_reason = if mode == 2 {
            TerminalReason::None
        } else {
            TerminalReason::TlsCertificate
        };
        c.terminal_detail = if mode == 2 { 0 } else { 0x130 };
    }
    let src = gst::glib::Object::new::<MoqSrc>();
    let bus = gst::Bus::new();
    src.set_bus(Some(&bus));
    let cat = *super::imp::CAT;
    let previous = cat.threshold();
    let capture = gst::log::add_log_function(|cat, _level, _file, _fn, _line, _obj, msg| {
        let Some(text) = msg.get() else { return };
        if cat.name() == "moqsrc" && text.starts_with("endpoint terminal") {
            let mut c = control();
            assert_eq!(c.startup_calls, "connect;attach;get;");
            assert!(c.terminal_diagnostic.is_none());
            c.terminal_diagnostic = Some(text.to_string());
            c.startup_calls.push_str("log;");
        }
    });
    cat.set_threshold(gst::DebugLevel::Info);

    // Through the class vmethod, as GstBaseSrc calls it: a failed start posts
    // its error message from there.
    let started = unsafe {
        use gst::glib::translate::ToGlibPtr;
        let base: &gst_base::BaseSrc = src.upcast_ref();
        let klass = gst::glib::gobject_ffi::g_type_class_peek(
            gst::glib::translate::IntoGlib::into_glib(MoqSrc::static_type()),
        ) as *const gst_base::ffi::GstBaseSrcClass;
        (*klass).start.unwrap()(base.to_glib_none().0)
    };
    assert_eq!(started, gst::glib::ffi::GFALSE);

    gst::log::remove_log_function(capture);
    cat.set_threshold(previous);
    let expected = match mode {
        1 => "endpoint terminal unavailable getter_rc=-2",
        2 => "endpoint terminal unclassified reason=0 detail=0",
        _ => "endpoint terminal reason=3 detail=304",
    };
    {
        let c = control();
        assert_eq!(c.startup_calls, "connect;attach;get;log;stop;destroy;");
        assert_eq!(c.terminal_diagnostic.as_deref(), Some(expected));
        // The element dialled exactly the draft it was configured for, with
        // certificate verification left on.
        let dialled = c.dialled.as_ref().expect("the element dialled");
        assert!(!dialled.insecure);
        assert_eq!(dialled.draft, 16);
        // The receiver was asked for an explicit, non-empty namespace.
        let rcfg = c.receiver_cfg.as_ref().expect("a receiver config");
        assert!(!rcfg.auto_subscribe);
        assert!(rcfg.namespace.parts().count() > 0);
    }
    assert!(src.imp().ep.lock().unwrap().is_none());
    {
        let st = src.imp().lock_state();
        assert!(st.receiver.is_none());
        assert!(!st.started);
    }
    let message = bus.pop().expect("an error message");
    let gst::MessageView::Error(err) = message.view() else {
        panic!("expected an error, got {:?}", message.type_())
    };
    assert_eq!(
        err.error().message(),
        "could not attach media receiver (rc=-4)"
    );
    assert!(err.error().matches(gst::ResourceError::OpenRead));
    assert!(bus.pop().is_none()); // No successful EOS or second error.
    src.set_bus(None);
}

#[test]
fn startup_terminal_known() {
    startup_terminal(0);
}

#[test]
fn startup_terminal_getter_failure() {
    startup_terminal(1);
}

#[test]
fn startup_terminal_none() {
    startup_terminal(2);
}

fn loc_aac_caps(explicit_caps: bool) {
    let _suite = suite();
    let src = make_source();
    src.imp().settings.lock().unwrap().caps = if explicit_caps {
        Some(
            "audio/mpeg,mpegversion=4,stream-format=raw"
                .parse()
                .unwrap(),
        )
    } else {
        None
    };
    static ASC: [u8; 2] = [0x11, 0x90];
    let mut d = desc("video", "mp4a.40.2");
    d.packaging = Some(Packaging::Raw);
    d.samplerate = Some(48000);
    d.channel_config = b"2".to_vec();
    d.init_data = ASC.to_vec();
    control().events = vec![added(VIDEO, d)];
    {
        let mut st = src.imp().lock_state();
        src.imp().drain_track_events(&mut st);
        assert_eq!(st.want_init.len(), 0);
        if explicit_caps {
            assert!(st.derived_caps.is_none());
        } else {
            let caps = st.derived_caps.as_ref().expect("derived caps");
            let s = caps.structure(0).unwrap();
            assert_eq!(s.get::<i32>("mpegversion").unwrap(), 4);
            assert!(!s.has_field("mpeg-version"));
            assert_eq!(s.get::<i32>("rate").unwrap(), 48000);
            assert_eq!(s.get::<i32>("channels").unwrap(), 2);
            let cd = s.get::<gst::Buffer>("codec_data").unwrap();
            assert_eq!(contents(&cd), ASC);
            assert_eq!(st.want_init.len(), 0);
        }
    }
}

#[test]
fn loc_aac_caps_derived() {
    loc_aac_caps(false);
}

#[test]
fn loc_aac_explicit_caps() {
    loc_aac_caps(true);
}

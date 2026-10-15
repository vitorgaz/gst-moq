//! Exercise the source's actual create vmethod with deterministic public-API
//! mocks. No relay, network thread, sleeps, or production test hooks.
//!
//! Port of tests/test_src_delivery.c.

use gst::prelude::*;
use gst::subclass::prelude::*;

use std::sync::atomic::AtomicU8;
use std::sync::{Mutex, MutexGuard};

use super::MoqSrc;
use crate::moq::*;
use crate::test_support::{serial, Addr};

static RECEIVER_ID: AtomicU8 = AtomicU8::new(0);
static VIDEO_HANDLE: AtomicU8 = AtomicU8::new(0);
static AUDIO_HANDLE: AtomicU8 = AtomicU8::new(0);
static ENDPOINT_HANDLE: AtomicU8 = AtomicU8::new(0);
const VIDEO_PAYLOAD: [u8; 5] = [0, 0, 1, 0x65, 0x42];
const AUDIO_PAYLOAD: [u8; 2] = [0xf8, 0xff];

fn handle<T>(a: &'static AtomicU8) -> *mut T {
    a as *const AtomicU8 as *mut T
}

struct Receiver {
    events: Vec<moq_media_track_event_t>,
    event_index: usize,
    objects: Vec<moq_media_object_t>,
    object_index: usize,
    cleaned: u32,
    waits: u32,
    subscriptions: u32,
    subscribed_track: Addr,
    subscribe_result: moq_result_t,
    empty_result: moq_result_t,

    // Startup-failure mocks assert ownership and call/log ordering.
    startup_active: bool,
    startup_calls: String,
    terminal_diagnostic: Option<String>,
    terminal_getter_result: moq_result_t,
    terminal_reason: moq_endpoint_terminal_reason_t,
    terminal_detail: u64,
}

// SAFETY: the raw pointers inside are test fixtures the test keeps alive.
unsafe impl Send for Receiver {}

impl Receiver {
    const NEW: Receiver = Receiver {
        events: Vec::new(),
        event_index: 0,
        objects: Vec::new(),
        object_index: 0,
        cleaned: 0,
        waits: 0,
        subscriptions: 0,
        subscribed_track: Addr(0),
        subscribe_result: MOQ_OK,
        empty_result: MOQ_DONE,
        startup_active: false,
        startup_calls: String::new(),
        terminal_diagnostic: None,
        terminal_getter_result: MOQ_OK,
        terminal_reason: 0,
        terminal_detail: 0,
    };
}

static RECEIVER: Mutex<Receiver> = Mutex::new(Receiver::NEW);

fn receiver() -> MutexGuard<'static, Receiver> {
    RECEIVER.lock().unwrap_or_else(|e| e.into_inner())
}

fn is_mock_receiver(r: *const moq_media_receiver_t) -> bool {
    r == handle::<moq_media_receiver_t>(&RECEIVER_ID)
}

// -- receiver mocks ------------------------------------------------------------

unsafe extern "C" fn subscribe_track(
    r: *mut moq_media_receiver_t,
    track: *mut moq_media_track_t,
    cfg: *const moq_media_receiver_track_subscribe_cfg_t,
) -> moq_result_t {
    assert!(is_mock_receiver(r));
    assert!(cfg.is_null());
    let mut rc = receiver();
    rc.subscriptions += 1;
    rc.subscribed_track = Addr::of(track);
    rc.subscribe_result
}

unsafe extern "C" fn poll_track(
    r: *mut moq_media_receiver_t,
    ev: *mut moq_media_track_event_t,
    size: usize,
) -> moq_result_t {
    assert!(is_mock_receiver(r));
    assert_eq!(size, std::mem::size_of::<moq_media_track_event_t>());
    let mut rc = receiver();
    if rc.event_index == rc.events.len() {
        return MOQ_DONE;
    }
    *ev = rc.events[rc.event_index];
    rc.event_index += 1;
    (*ev).struct_size = size as u32;
    MOQ_OK
}

unsafe extern "C" fn poll_object(
    r: *mut moq_media_receiver_t,
    obj: *mut moq_media_object_t,
    size: usize,
) -> moq_result_t {
    assert!(is_mock_receiver(r));
    assert_eq!(size, std::mem::size_of::<moq_media_object_t>());
    let mut rc = receiver();
    if rc.object_index == rc.objects.len() {
        return rc.empty_result;
    }
    *obj = rc.objects[rc.object_index];
    rc.object_index += 1;
    (*obj).struct_size = size as u32;
    MOQ_OK
}

unsafe extern "C" fn object_cleanup(obj: *mut moq_media_object_t) {
    receiver().cleaned += 1;
    std::ptr::write_bytes(obj, 0, 1);
}

unsafe extern "C" fn receiver_wait(_r: *mut moq_media_receiver_t, _timeout: u64) -> moq_result_t {
    receiver().waits += 1;
    MOQ_ERR_INTERRUPTED // Turn unexpected waits into a quick failure.
}

unsafe extern "C" fn receiver_is_fatal(_r: *const moq_media_receiver_t) -> bool {
    false
}
unsafe extern "C" fn receiver_fatal_code(_r: *const moq_media_receiver_t) -> u64 {
    0
}
unsafe extern "C" fn negotiated_version(_ep: *const moq_endpoint_t) -> moq_version_t {
    16
}

// -- startup mocks -------------------------------------------------------------

fn startup_call(ep: *const moq_endpoint_t, call: &str) {
    let mut rc = receiver();
    assert!(rc.startup_active);
    assert_eq!(ep, handle::<moq_endpoint_t>(&ENDPOINT_HANDLE));
    rc.startup_calls.push_str(call);
}

unsafe extern "C" fn get_terminal(
    ep: *const moq_endpoint_t,
    out: *mut moq_endpoint_terminal_t,
    size: usize,
) -> moq_result_t {
    startup_call(ep, "get;");
    assert_eq!(size, std::mem::size_of::<moq_endpoint_terminal_t>());
    let rc = receiver();
    if rc.terminal_getter_result == MOQ_OK {
        (*out).struct_size = size as u32;
        (*out).reason = rc.terminal_reason;
        (*out).detail_code = rc.terminal_detail;
    }
    rc.terminal_getter_result
}
unsafe extern "C" fn endpoint_cfg_init(cfg: *mut moq_endpoint_cfg_t) {
    assert!(receiver().startup_active);
    std::ptr::write_bytes(cfg, 0, 1);
}
unsafe extern "C" fn endpoint_connect(
    cfg: *const moq_endpoint_cfg_t,
    out: *mut *mut moq_endpoint_t,
) -> moq_result_t {
    let cfg = &*cfg;
    let mut rc = receiver();
    assert!(rc.startup_active);
    assert!(!cfg.insecure_skip_verify);
    assert_eq!(cfg.versions.policy, MOQ_VERSION_POLICY_EXACT);
    assert_eq!(cfg.versions.version_count, 1);
    assert_eq!(*cfg.versions.versions, 16);
    rc.startup_calls.push_str("connect;");
    *out = handle(&ENDPOINT_HANDLE);
    MOQ_OK
}
unsafe extern "C" fn endpoint_stop(ep: *mut moq_endpoint_t) -> moq_result_t {
    startup_call(ep, "stop;");
    MOQ_OK
}
unsafe extern "C" fn endpoint_destroy(ep: *mut moq_endpoint_t) {
    startup_call(ep, "destroy;");
}
unsafe extern "C" fn endpoint_set_interrupted(_ep: *mut moq_endpoint_t, _interrupted: bool) {
    unreachable!("moq_endpoint_set_interrupted");
}
unsafe extern "C" fn receiver_cfg_init_live(cfg: *mut moq_media_receiver_cfg_t) {
    assert!(receiver().startup_active);
    std::ptr::write_bytes(cfg, 0, 1);
}
unsafe extern "C" fn receiver_attach(
    ep: *mut moq_endpoint_t,
    cfg: *const moq_media_receiver_cfg_t,
    out: *mut *mut moq_media_receiver_t,
) -> moq_result_t {
    startup_call(ep, "attach;");
    let cfg = &*cfg;
    assert!(cfg.endpoint.is_null());
    assert!(!cfg.auto_subscribe);
    assert!(cfg.namespace_.count > 0);
    *out = std::ptr::null_mut();
    MOQ_ERR_CLOSED
}
unsafe extern "C" fn receiver_destroy(_r: *mut moq_media_receiver_t) {
    unreachable!("moq_media_receiver_destroy");
}

const API: Api = Api {
    moq_media_receiver_subscribe_track: subscribe_track,
    moq_media_receiver_poll_track: poll_track,
    moq_media_receiver_poll_object: poll_object,
    moq_media_object_cleanup: object_cleanup,
    moq_media_receiver_wait: receiver_wait,
    moq_media_receiver_is_fatal: receiver_is_fatal,
    moq_media_receiver_fatal_code: receiver_fatal_code,
    moq_endpoint_negotiated_version: negotiated_version,
    moq_endpoint_get_terminal: get_terminal,
    moq_endpoint_cfg_init: endpoint_cfg_init,
    moq_endpoint_connect: endpoint_connect,
    moq_endpoint_stop: endpoint_stop,
    moq_endpoint_destroy: endpoint_destroy,
    moq_endpoint_set_interrupted: endpoint_set_interrupted,
    moq_media_receiver_cfg_init_live: receiver_cfg_init_live,
    moq_media_receiver_attach: receiver_attach,
    moq_media_receiver_destroy: receiver_destroy,
    ..REAL
};

// -- fixture -------------------------------------------------------------------

struct Suite {
    _api: ApiGuard,
    _serial: MutexGuard<'static, ()>,
}

fn suite() -> Suite {
    let serial = serial();
    *receiver() = Receiver::NEW;
    Suite {
        _api: install(API),
        _serial: serial,
    }
}

fn make_source() -> MoqSrc {
    *receiver() = Receiver::NEW;
    let src = gst::glib::Object::new::<MoqSrc>();
    src.imp().settings.lock().unwrap().caps = Some(gst::Caps::new_any());
    let mut st = src.imp().lock_state();
    st.receiver = Handle(handle(&RECEIVER_ID));
    st.caps_pushed = true; // Negotiation is exercised separately by pipelines.
    st.discover_deadline = i64::MAX;
    drop(st);
    src
}

/// Detach the mock receiver before the element goes away.
fn release(src: MoqSrc) {
    src.imp().lock_state().receiver = Handle::null();
}

fn queue_object(handle_of: &'static AtomicU8, payload: &'static [u8]) {
    receiver().objects.push(moq_media_object_t {
        track: handle(handle_of),
        payload: span(payload),
        keyframe: true,
        ..Default::default()
    });
}

fn create(src: &MoqSrc) -> Result<gst::Buffer, gst::FlowError> {
    src.imp().create_buffer()
}

fn added(track: &'static AtomicU8, desc: &moq_media_track_desc_t) -> moq_media_track_event_t {
    moq_media_track_event_t {
        kind: MOQ_MEDIA_TRACK_ADDED,
        track: handle(track),
        desc,
        ..Default::default()
    }
}

fn terminal(
    kind: moq_media_track_event_kind_t,
    track: &'static AtomicU8,
) -> moq_media_track_event_t {
    moq_media_track_event_t {
        kind,
        track: handle(track),
        ..Default::default()
    }
}

fn desc(name: &'static str, codec: &'static str) -> moq_media_track_desc_t {
    moq_media_track_desc_t {
        name: span(name.as_bytes()),
        codec: span(codec.as_bytes()),
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
    queue_object(&AUDIO_HANDLE, &AUDIO_PAYLOAD);
    receiver().empty_result = MOQ_ERR_CLOSED;
    assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
    assert_eq!(receiver().cleaned, 1);
    release(src);
}

fn selected_end_drains_media(event_kind: moq_media_track_event_kind_t) {
    let _suite = suite();
    let src = make_source();
    let video = desc("video", "avc1.42c01e");
    let audio = moq_media_track_desc_t {
        name: span(b"audio"),
        ..Default::default()
    };
    receiver().events = vec![
        added(&AUDIO_HANDLE, &audio),
        added(&VIDEO_HANDLE, &video),
        terminal(event_kind, &VIDEO_HANDLE),
    ];
    queue_object(&AUDIO_HANDLE, &AUDIO_PAYLOAD);
    queue_object(&VIDEO_HANDLE, &VIDEO_PAYLOAD);
    queue_object(&VIDEO_HANDLE, &VIDEO_PAYLOAD);

    for _ in 0..2 {
        let out = create(&src).expect("a video buffer");
        assert_eq!(contents(&out), VIDEO_PAYLOAD);
    }
    assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
    let rc = receiver();
    assert_eq!(rc.cleaned, 3);
    assert_eq!(rc.waits, 0);
    assert_eq!(rc.subscriptions, 1);
    assert_eq!(rc.subscribed_track, Addr::of(handle::<u8>(&VIDEO_HANDLE)));
    drop(rc);
    release(src);
}

#[test]
fn selected_end_drains() {
    selected_end_drains_media(MOQ_MEDIA_TRACK_ENDED);
}

#[test]
fn selected_removal_drains() {
    selected_end_drains_media(MOQ_MEDIA_TRACK_REMOVED);
}

#[test]
fn unrelated_end() {
    let _suite = suite();
    let src = make_source();
    src.imp().lock_state().want_track = Handle(handle(&VIDEO_HANDLE));
    receiver().events = vec![terminal(MOQ_MEDIA_TRACK_ENDED, &AUDIO_HANDLE)];
    assert_eq!(create(&src).err(), Some(gst::FlowError::Flushing));
    assert_eq!(receiver().waits, 1);
    release(src);
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
    release(src);
}

#[test]
fn subscription_failure() {
    let _suite = suite();
    let src = make_source();
    receiver().subscribe_result = MOQ_ERR_CLOSED;
    let video = desc("video", "avc1.42c01e");
    receiver().events = vec![added(&VIDEO_HANDLE, &video)];
    assert_eq!(create(&src).err(), Some(gst::FlowError::Error));
    assert!(src.imp().lock_state().want_track.is_null());
    assert_eq!(receiver().waits, 0);
    release(src);
}

#[test]
fn interrupt_before_timeout() {
    let _suite = suite();
    let src = make_source();
    src.imp().lock_state().discover_deadline = gst::glib::monotonic_time() - 1;
    receiver().empty_result = MOQ_ERR_INTERRUPTED;
    assert_eq!(create(&src).err(), Some(gst::FlowError::Flushing));
    release(src);
}

fn cmaf_tail(kind: moq_media_track_event_kind_t) {
    let _suite = suite();
    let src = make_source();
    static INIT: [u8; 8] = [0, 0, 0, 8, b'f', b't', b'y', b'p'];
    static FRAGMENT: [u8; 8] = [0, 0, 0, 8, b'm', b'o', b'o', b'f'];
    let mut d = moq_media_track_desc_t {
        name: span(b"video"),
        ..Default::default()
    };
    d.info.packaging = MOQ_MEDIA_PACKAGING_CMAF;
    d.init_data = span(&INIT);
    let add = added(&VIDEO_HANDLE, &d);
    // A duplicate announcement must not resubscribe.
    receiver().events = vec![add, add, terminal(kind, &VIDEO_HANDLE)];
    for _ in 0..2 {
        receiver().objects.push(moq_media_object_t {
            track: handle(&VIDEO_HANDLE),
            keyframe: true,
            packaging: MOQ_MEDIA_PACKAGING_CMAF,
            fragment: span(&FRAGMENT),
            ..Default::default()
        });
    }
    for i in 0..3 {
        let out = create(&src).expect("a buffer");
        assert_eq!(contents(&out), if i == 0 { INIT } else { FRAGMENT });
        assert_eq!(out.flags().contains(gst::BufferFlags::HEADER), i == 0);
    }
    assert_eq!(create(&src).err(), Some(gst::FlowError::Eos));
    let rc = receiver();
    assert_eq!(rc.subscriptions, 1);
    assert_eq!(rc.cleaned, 2);
    assert_eq!(rc.waits, 0);
    drop(rc);
    release(src);
}

#[test]
fn cmaf_ended_tail() {
    cmaf_tail(MOQ_MEDIA_TRACK_ENDED);
}

#[test]
fn cmaf_removed_tail() {
    cmaf_tail(MOQ_MEDIA_TRACK_REMOVED);
}

/// mode 0: classified terminal; 1: the getter fails; 2: unclassified.
fn startup_terminal(mode: u32) {
    let _suite = suite();
    {
        let mut rc = receiver();
        rc.startup_active = true;
        rc.terminal_getter_result = if mode == 1 { MOQ_ERR_INVAL } else { MOQ_OK };
        rc.terminal_reason = if mode == 2 {
            MOQ_ENDPOINT_TERMINAL_NONE
        } else {
            MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE
        };
        rc.terminal_detail = if mode == 2 { 0 } else { 0x130 };
    }
    let src = gst::glib::Object::new::<MoqSrc>();
    let bus = gst::Bus::new();
    src.set_bus(Some(&bus));
    let cat = *super::imp::CAT;
    let previous = cat.threshold();
    let capture = gst::log::add_log_function(|cat, _level, _file, _fn, _line, _obj, msg| {
        let Some(text) = msg.get() else { return };
        if cat.name() == "moqsrc" && text.starts_with("endpoint terminal") {
            let mut rc = receiver();
            assert!(rc.startup_active);
            assert_eq!(rc.startup_calls, "connect;attach;get;");
            assert!(rc.terminal_diagnostic.is_none());
            rc.terminal_diagnostic = Some(text.to_string());
            rc.startup_calls.push_str("log;");
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
        let rc = receiver();
        assert_eq!(rc.startup_calls, "connect;attach;get;log;stop;destroy;");
        assert_eq!(rc.terminal_diagnostic.as_deref(), Some(expected));
    }
    assert!(src
        .imp()
        .ep
        .load(std::sync::atomic::Ordering::SeqCst)
        .is_null());
    {
        let st = src.imp().lock_state();
        assert!(st.receiver.is_null());
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
    receiver().startup_active = false;
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
    d.info.packaging = MOQ_MEDIA_PACKAGING_RAW;
    d.has_samplerate = true;
    d.samplerate = 48000;
    d.channel_config = span(b"2");
    d.init_data = span(&ASC);
    receiver().events = vec![added(&VIDEO_HANDLE, &d)];
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
    release(src);
}

#[test]
fn loc_aac_caps_derived() {
    loc_aac_caps(false);
}

#[test]
fn loc_aac_explicit_caps() {
    loc_aac_caps(true);
}

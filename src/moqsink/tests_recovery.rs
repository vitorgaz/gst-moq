//! Sink EOS / flush / restart recovery against public-service mocks.
//!
//! Port of tests/test_sink_recovery.c. Like the C suite, it drives the real
//! element internals and replaces the libmoq service calls and the clock: the
//! monotonic time only advances when the element waits or retries, which makes
//! deadlines deterministic.

use gst::prelude::*;
use gst::subclass::prelude::*;

use std::ffi::c_void;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::sync::{Mutex, MutexGuard};

use super::imp::EOS_DRAIN_TIMEOUT_US;
use super::{MoqSink, MoqSinkPad};
use crate::test_support::{serial, zero_sized, Addr};
use moq5::sys::*;
use moq5::testing::{install, Api, ApiGuard, REAL};
use moq5::{MediaType, Packaging, RcBuf, SendObject, TrackConfig};

const USEC_PER_SEC: i64 = 1_000_000;

static NOW: AtomicI64 = AtomicI64::new(0);
static EP_ID: AtomicU8 = AtomicU8::new(0);
static SENDER_ID: AtomicU8 = AtomicU8::new(0);
static TRACK_IDS: [AtomicU8; 16] = [const { AtomicU8::new(0) }; 16];

fn track_id(i: usize) -> *mut moq_media_track_t {
    &TRACK_IDS[i] as *const AtomicU8 as *mut moq_media_track_t
}

struct Mock {
    tracks: usize,
    writes: u32,
    ends: u32,
    drains: u32,
    connects: u32,
    destroys: u32,
    refusals: u32,
    interrupted: bool,
    ended: [bool; 16],
    cancel_drain: bool,
    drain_result: moq_result_t,
    budget: u64,
    ready_ctx: Addr,
    ready_fn: Option<unsafe extern "C" fn(*mut c_void, *mut moq_media_sender_t)>,
    ready_during_attach: bool,
    ready_during_destroy: bool,
    ready_logs: u32,
}

impl Mock {
    const NEW: Mock = Mock {
        tracks: 0,
        writes: 0,
        ends: 0,
        drains: 0,
        connects: 0,
        destroys: 0,
        refusals: 0,
        interrupted: false,
        ended: [false; 16],
        cancel_drain: false,
        drain_result: MOQ_OK,
        budget: 0,
        ready_ctx: Addr(0),
        ready_fn: None,
        ready_during_attach: false,
        ready_during_destroy: false,
        ready_logs: 0,
    };
}

static MOCK: Mutex<Mock> = Mutex::new(Mock::NEW);
static ACTIVE: Mutex<Option<MoqSink>> = Mutex::new(None);
static CONTINUING: Mutex<Option<MoqSinkPad>> = Mutex::new(None);
static CANCELLER: Mutex<Option<MoqSinkPad>> = Mutex::new(None);

fn mock() -> MutexGuard<'static, Mock> {
    MOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn active() -> MoqSink {
    ACTIVE.lock().unwrap().clone().expect("active sink")
}

// -- libmoq service mocks ----------------------------------------------------

unsafe extern "C" fn endpoint_cfg_init(c: *mut moq_endpoint_cfg_t) {
    zero_sized(c, |c, n| c.struct_size = n);
}
unsafe extern "C" fn endpoint_connect(
    _c: *const moq_endpoint_cfg_t,
    out: *mut *mut moq_endpoint_t,
) -> moq_result_t {
    let mut m = mock();
    m.connects += 1;
    m.interrupted = false;
    *out = &EP_ID as *const AtomicU8 as *mut moq_endpoint_t;
    MOQ_OK
}
unsafe extern "C" fn endpoint_stop(_ep: *mut moq_endpoint_t) -> moq_result_t {
    MOQ_OK
}
unsafe extern "C" fn endpoint_post(
    ep: *mut moq_endpoint_t,
    f: moq_endpoint_task_fn,
    ctx: *mut c_void,
) -> moq_result_t {
    let _ = f.unwrap()(ep, ep as *mut moq_session_t, 0, ctx);
    MOQ_OK
}
unsafe extern "C" fn endpoint_destroy(_ep: *mut moq_endpoint_t) {
    mock().destroys += 1;
}
unsafe extern "C" fn endpoint_set_interrupted(_ep: *mut moq_endpoint_t, v: bool) {
    mock().interrupted = v;
}
unsafe extern "C" fn endpoint_wait(_ep: *mut moq_endpoint_t, _us: u64) -> moq_result_t {
    NOW.fetch_add(USEC_PER_SEC, Ordering::SeqCst);
    if mock().interrupted {
        MOQ_ERR_INTERRUPTED
    } else {
        MOQ_OK
    }
}
unsafe extern "C" fn endpoint_drain(_ep: *mut moq_endpoint_t, us: u64) -> moq_result_t {
    let cancel = {
        let mut m = mock();
        m.drains += 1;
        m.budget = us;
        m.cancel_drain
    };
    if cancel {
        let canceller = CANCELLER.lock().unwrap().clone().unwrap();
        active().imp().set_pad_flush(&canceller, true);
    }
    let m = mock();
    if m.interrupted {
        MOQ_ERR_INTERRUPTED
    } else {
        m.drain_result
    }
}
unsafe extern "C" fn sender_cfg_init_live_sized(c: *mut moq_media_sender_cfg_t, size: usize) {
    assert_eq!(size, std::mem::size_of::<moq_media_sender_cfg_t>());
    zero_sized(c, |c, n| c.struct_size = n);
}
unsafe extern "C" fn sender_callbacks_init_sized(
    c: *mut moq_media_sender_callbacks_t,
    size: usize,
) {
    assert_eq!(size, std::mem::size_of::<moq_media_sender_callbacks_t>());
    zero_sized(c, |c, n| c.struct_size = n);
}
unsafe extern "C" fn sender_attach(
    _ep: *mut moq_endpoint_t,
    c: *const moq_media_sender_cfg_t,
    out: *mut *mut moq_media_sender_t,
) -> moq_result_t {
    let c = &*c;
    assert_eq!(
        c.struct_size as usize,
        std::mem::size_of::<moq_media_sender_cfg_t>()
    );
    assert_eq!(
        c.callbacks.struct_size as usize,
        std::mem::size_of::<moq_media_sender_callbacks_t>()
    );
    assert!(c.callbacks.on_ready.is_some());
    let early = {
        let mut m = mock();
        m.ready_ctx = Addr::of(c.callbacks.ctx);
        m.ready_fn = c.callbacks.on_ready;
        m.ready_during_attach
    };
    let sender = &SENDER_ID as *const AtomicU8 as *mut moq_media_sender_t;
    if early {
        assert!(active().imp().send_lock().sender.is_none());
        c.callbacks.on_ready.unwrap()(c.callbacks.ctx, sender);
    }
    *out = sender;
    MOQ_OK
}
unsafe extern "C" fn sender_destroy(s: *mut moq_media_sender_t) {
    let (during, ctx, f) = {
        let m = mock();
        (m.ready_during_destroy, m.ready_ctx, m.ready_fn)
    };
    if during {
        f.unwrap()(ctx.ptr(), s);
    }
}
unsafe extern "C" fn track_cfg_init(c: *mut moq_media_track_cfg_t, n: usize) {
    assert_eq!(n, std::mem::size_of::<moq_media_track_cfg_t>());
    zero_sized(c, |c, n| c.struct_size = n);
}
unsafe extern "C" fn sender_add_track(
    _s: *mut moq_media_sender_t,
    _c: *const moq_media_track_cfg_t,
    out: *mut *mut moq_media_track_t,
) -> moq_result_t {
    let mut m = mock();
    assert!(m.tracks < 16);
    *out = track_id(m.tracks);
    m.tracks += 1;
    MOQ_OK
}
unsafe extern "C" fn sender_end_track(
    _s: *mut moq_media_sender_t,
    t: *mut moq_media_track_t,
) -> moq_result_t {
    let mut m = mock();
    m.ends += 1;
    if m.interrupted {
        return MOQ_ERR_INTERRUPTED;
    }
    if m.refusals > 0 {
        m.refusals -= 1;
        return MOQ_ERR_WOULD_BLOCK;
    }
    for i in 0..m.tracks {
        if t == track_id(i) {
            m.ended[i] = true;
            return MOQ_OK;
        }
    }
    MOQ_ERR_INVAL
}
unsafe extern "C" fn sender_write(
    _s: *mut moq_media_sender_t,
    t: *mut moq_media_track_t,
    o: *const moq_media_send_object_t,
) -> moq_result_t {
    let mut m = mock();
    if m.interrupted {
        return MOQ_ERR_INTERRUPTED;
    }
    for i in 0..m.tracks {
        if t == track_id(i) {
            if m.ended[i] {
                return MOQ_ERR_WRONG_STATE;
            }
            m.writes += 1;
            moq_rcbuf_decref((*o).payload);
            return MOQ_OK;
        }
    }
    MOQ_ERR_INVAL
}
unsafe extern "C" fn sender_get_stats(
    _s: *const moq_media_sender_t,
    out: *mut moq_media_sender_stats_t,
    n: usize,
) -> moq_result_t {
    assert_eq!(n, std::mem::size_of::<moq_media_sender_stats_t>());
    zero_sized(out, |o, n| o.struct_size = n);
    MOQ_OK
}
unsafe extern "C" fn sender_is_fatal(_s: *const moq_media_sender_t) -> bool {
    false
}
unsafe extern "C" fn sender_fatal_code(_s: *const moq_media_sender_t) -> u64 {
    0
}

const API: Api = Api {
    moq_endpoint_cfg_init: endpoint_cfg_init,
    moq_endpoint_connect: endpoint_connect,
    moq_endpoint_stop: endpoint_stop,
    moq_endpoint_destroy: endpoint_destroy,
    moq_endpoint_set_interrupted: endpoint_set_interrupted,
    moq_endpoint_wait: endpoint_wait,
    moq_endpoint_drain: endpoint_drain,
    moq_endpoint_post: endpoint_post,
    moq_media_sender_cfg_init_live_sized: sender_cfg_init_live_sized,
    moq_media_sender_callbacks_init_sized: sender_callbacks_init_sized,
    moq_media_sender_attach: sender_attach,
    moq_media_sender_destroy: sender_destroy,
    moq_media_track_cfg_init_sized: track_cfg_init,
    moq_media_sender_add_track: sender_add_track,
    moq_media_sender_write: sender_write,
    moq_media_sender_end_track: sender_end_track,
    moq_media_sender_is_fatal: sender_is_fatal,
    moq_media_sender_fatal_code: sender_fatal_code,
    moq_media_sender_get_stats: sender_get_stats,
    ..REAL
};

// -- clock: time only moves when the element waits or retries ----------------

fn now() -> i64 {
    NOW.load(Ordering::SeqCst)
}

fn write_other(p: &MoqSinkPad) {
    let payload = RcBuf::copy_from_slice(&[42]).unwrap();
    assert_eq!(
        active().imp().write_object(p, SendObject::new(payload), 1),
        Ok(gst::FlowSuccess::Ok)
    );
}

fn pause_retry(us: u64) {
    assert!(us <= 5000);
    // The retry pause must not hold sender serialization.
    assert!(active().imp().send.try_lock().is_ok());
    let continuing = CONTINUING.lock().unwrap().clone();
    if let Some(p) = continuing {
        write_other(&p);
    }
    NOW.fetch_add(USEC_PER_SEC, Ordering::SeqCst);
}

// -- fixture -----------------------------------------------------------------

struct Suite {
    _api: ApiGuard,
    _clock: crate::clock::ClockGuard,
    _serial: MutexGuard<'static, ()>,
}

impl Drop for Suite {
    fn drop(&mut self) {
        *ACTIVE.lock().unwrap() = None;
        *CONTINUING.lock().unwrap() = None;
        *CANCELLER.lock().unwrap() = None;
        *mock() = Mock::NEW;
    }
}

fn suite() -> Suite {
    let serial = serial();
    *mock() = Mock::NEW;
    Suite {
        _api: install(API),
        _clock: crate::clock::install(now, pause_retry),
        _serial: serial,
    }
}

fn make_sink() -> MoqSink {
    {
        let mut m = mock();
        let (ready_ctx, ready_fn, during_attach, during_destroy, logs, interrupted) = (
            m.ready_ctx,
            m.ready_fn,
            m.ready_during_attach,
            m.ready_during_destroy,
            m.ready_logs,
            m.interrupted,
        );
        *m = Mock {
            ready_ctx,
            ready_fn,
            ready_during_attach: during_attach,
            ready_during_destroy: during_destroy,
            ready_logs: logs,
            interrupted,
            ..Mock::NEW
        };
    }
    NOW.store(100, Ordering::SeqCst);
    *CONTINUING.lock().unwrap() = None;
    *CANCELLER.lock().unwrap() = None;
    let s = gst::glib::Object::builder::<MoqSink>()
        .property("sync", false)
        .build();
    *ACTIVE.lock().unwrap() = Some(s.clone());
    assert!(s.set_state(gst::State::Ready).is_ok());
    s
}

fn pad(s: &MoqSink, track: bool) -> MoqSinkPad {
    let p = s
        .request_pad_simple("audio_%u")
        .unwrap()
        .downcast::<MoqSinkPad>()
        .unwrap();
    if track {
        if s.imp().send_lock().sender.is_none() {
            let mut ts = s.imp().track_lock();
            ts.preparing_tracks = Some(Vec::new());
            assert!(s.imp().open_sender(&mut ts));
            ts.preparing_tracks = None;
        }
        let cfg = TrackConfig::new("", MediaType::Audio, Packaging::Cmaf, "");
        let t = s
            .imp()
            .send_lock()
            .sender
            .as_ref()
            .unwrap()
            .add_track(cfg)
            .unwrap();
        p.set_track(Some(t));
    }
    p
}

fn release(s: &MoqSink, p: MoqSinkPad) {
    s.release_request_pad(&p);
}

fn finish(s: MoqSink) {
    let _ = s.set_state(gst::State::Null);
}

fn eos(s: &MoqSink, p: &MoqSinkPad) -> bool {
    s.imp().sink_event(p, gst::event::Eos::new())
}

fn flush(s: &MoqSink, p: &MoqSinkPad, start: bool) {
    let ev = if start {
        gst::event::FlushStart::new()
    } else {
        gst::event::FlushStop::new(true)
    };
    assert!(s.imp().sink_event(p, ev));
}

// -- cases -------------------------------------------------------------------

fn incomplete_cmaf_eos(pending_moof: bool) {
    let _suite = suite();
    let s = make_sink();
    let p = pad(&s, true);
    let bus = gst::Bus::new();
    s.set_bus(Some(&bus));
    let partial_header: &[u8] = &[0, 0, 0];
    let missing_mdat: &[u8] = &[
        0, 0, 0, 8, b'f', b't', b'y', b'p', 0, 0, 0, 8, b'm', b'o', b'o', b'v', 0, 0, 0, 8, b'm',
        b'o', b'o', b'f',
    ];
    let bytes = if pending_moof {
        missing_mdat
    } else {
        partial_header
    };
    assert!(p.state().splitter.push(bytes).is_ok());
    assert!(!eos(&s, &p));
    assert_eq!(mock().ends, 0);
    assert_eq!(mock().drains, 0);
    assert!(s.imp().send_lock().eos_failed);
    let msg = bus
        .pop_filtered(&[gst::MessageType::Error])
        .expect("error message");
    let gst::MessageView::Error(err) = msg.view() else {
        unreachable!()
    };
    assert!(err.error().matches(gst::StreamError::Format));
    assert!(bus.pop_filtered(&[gst::MessageType::Eos]).is_none());
    s.set_bus(None);
    release(&s, p);
    finish(s);
}

#[test]
fn eos_partial_box() {
    incomplete_cmaf_eos(false);
}

#[test]
fn eos_missing_mdat() {
    incomplete_cmaf_eos(true);
}

#[test]
fn budget() {
    let _suite = suite();
    let s = make_sink();
    let p = pad(&s, true);
    mock().refusals = 2;
    assert!(eos(&s, &p));
    assert_eq!(mock().ends, 3);
    assert_eq!(mock().budget, USEC_PER_SEC as u64);
    release(&s, p);
    finish(s);
}

#[test]
fn independent() {
    let _suite = suite();
    let s = make_sink();
    let a = pad(&s, true);
    let b = pad(&s, true);
    *CONTINUING.lock().unwrap() = Some(b.clone());
    mock().refusals = 1;
    assert!(eos(&s, &a));
    assert_eq!(mock().writes, 1);
    assert!(mock().ended[0]);
    assert!(!mock().ended[1]);
    assert_eq!(mock().drains, 0);
    *CONTINUING.lock().unwrap() = None;
    release(&s, a);
    release(&s, b);
    finish(s);
}

#[test]
fn released() {
    let _suite = suite();
    let s = make_sink();
    let a = pad(&s, true);
    let b = pad(&s, true);
    flush(&s, &b, true);
    release(&s, a);
    assert!(!mock().ended[0]);
    assert!(mock().interrupted);
    flush(&s, &b, false);
    assert!(mock().ended[0]);
    assert!(!mock().ended[1]);
    release(&s, b);
    finish(s);
}

#[test]
fn flush_count() {
    let _suite = suite();
    // The C suite ran this after /sink/released, whose teardown leaves the
    // interrupt latch set; with no endpoint opened here nothing may clear it.
    mock().interrupted = true;
    let s = make_sink();
    let a = pad(&s, false);
    let b = pad(&s, false);
    flush(&s, &a, true);
    flush(&s, &a, true);
    flush(&s, &b, false);
    assert_eq!(s.imp().obj_lock().flush_count, 1);
    flush(&s, &b, true);
    release(&s, a);
    assert_eq!(s.imp().obj_lock().flush_count, 1);
    assert!(mock().interrupted);
    s.imp().obj_lock().state_flushing = true;
    flush(&s, &b, false);
    assert!(mock().interrupted);
    release(&s, b);
    finish(s);
}

#[test]
fn failed() {
    let _suite = suite();
    for result in [
        MOQ_DONE,
        MOQ_ERR_INTERRUPTED,
        MOQ_ERR_CLOSED,
        MOQ_ERR_UNSUPPORTED,
        MOQ_ERR_WRONG_STATE,
    ] {
        let s = make_sink();
        let p = pad(&s, true);
        let bus = gst::Bus::new();
        s.set_bus(Some(&bus));
        mock().drain_result = result;
        assert!(!eos(&s, &p));
        let m = bus.pop().expect("a message");
        assert_eq!(m.type_(), gst::MessageType::Error, "rc={result}");
        assert!(bus.pop().is_none());
        release(&s, p);
        s.set_bus(None);
        finish(s);
    }
}

#[test]
fn cancel() {
    let _suite = suite();
    let s = make_sink();
    let p = pad(&s, true);
    *CANCELLER.lock().unwrap() = Some(p.clone());
    mock().cancel_drain = true;
    assert!(!eos(&s, &p));
    assert!(mock().interrupted);
    mock().cancel_drain = false;
    release(&s, p);
    finish(s);
}

#[test]
fn aggregation() {
    let _suite = suite();
    let s = make_sink();
    let a = pad(&s, true);
    let b = pad(&s, true);
    a.set_eos(true);
    assert!(eos(&s, &b));
    assert!(!s.imp().obj_lock().eos_posted);
    assert_eq!(mock().drains, 0);
    release(&s, a);
    assert_eq!(mock().drains, 1);
    release(&s, b);
    finish(s);
}

#[test]
fn restart() {
    let _suite = suite();
    let s = make_sink();
    let _ = s.set_state(gst::State::Paused);
    let p = pad(&s, true);
    assert!(eos(&s, &p));
    p.state().splitter.buf.push(b'x');
    let _ = s.set_state(gst::State::Ready);
    assert_eq!(mock().destroys, 1);
    let _ = s.set_state(gst::State::Paused);
    assert!(p.track().is_none());
    assert!(!p.eos());
    assert!(p.state().splitter.buf.is_empty());
    assert_eq!(mock().connects, 1); // reconnect is deferred until media
    release(&s, p);
    finish(s);
}

#[test]
fn deadline() {
    let _suite = suite();
    let s = make_sink();
    let a = pad(&s, true);
    let b = pad(&s, true);
    flush(&s, &b, true);
    release(&s, a);
    NOW.fetch_add(EOS_DRAIN_TIMEOUT_US, Ordering::SeqCst);
    let before = mock().ends;
    flush(&s, &b, false);
    assert_eq!(mock().ends, before);
    assert!(s.imp().send_lock().eos_failed);
    assert!(!mock().ended[0]);
    release(&s, b);
    finish(s);
}

fn ready_callback(early: bool) {
    let _suite = suite();
    {
        let mut m = mock();
        m.ready_logs = 0;
        m.ready_during_attach = early;
    }
    let cat = *super::imp::CAT;
    let previous = cat.threshold();
    let capture = gst::log::add_log_function(|cat, _level, _file, _function, _line, _obj, msg| {
        let Some(text) = msg.get() else { return };
        if cat.name() == "moqsink" && text.starts_with("native sender ready") {
            assert_eq!(text.as_str(), "native sender ready namespace=example");
            mock().ready_logs += 1;
        }
    });
    cat.set_threshold(gst::DebugLevel::Info);

    let s = make_sink();
    let _ = s.set_state(gst::State::Paused);
    assert!(s.imp().started.load(Ordering::SeqCst));
    let p = pad(&s, true);
    assert_eq!(mock().ready_logs, if early { 1 } else { 0 });
    if !early {
        // Attach success is not readiness; only the callback emits the record.
        let (ctx, f) = {
            let m = mock();
            (m.ready_ctx, m.ready_fn)
        };
        let sender = s.imp().send_lock().sender.as_ref().unwrap().as_ptr();
        unsafe { f.unwrap()(ctx.ptr(), sender) };
        assert_eq!(mock().ready_logs, 1);
    }
    mock().ready_during_destroy = true; // Context must survive callback settlement.
    let _ = s.set_state(gst::State::Ready);
    assert_eq!(mock().ready_logs, 2);
    // The callback context went with the sender, after its destroy.
    assert!(s.imp().send_lock().sender.is_none());
    {
        let mut m = mock();
        m.ready_during_destroy = false;
        m.ready_during_attach = false;
    }
    gst::log::remove_log_function(capture);
    cat.set_threshold(previous);
    release(&s, p);
    finish(s);
}

#[test]
fn ready_after_attach() {
    ready_callback(false);
}

#[test]
fn ready_during_attach() {
    ready_callback(true);
}

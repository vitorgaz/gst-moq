//! Sink EOS / flush / restart recovery against the service double.
//!
//! Port of tests/test_sink_recovery.c. Like the C suite, it drives the real
//! element internals and replaces the libmoq service calls and the clock: the
//! monotonic time only advances when the element waits or retries, which makes
//! deadlines deterministic.

use gst::prelude::*;
use gst::subclass::prelude::*;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use super::imp::EOS_DRAIN_TIMEOUT_US;
use super::{MoqSink, MoqSinkPad};
use crate::service::fake::{control, suite as fake_suite, Suite as FakeSuite};
use crate::service::TrackCfg;
use moq5::{Buffer, Error, MediaType, Packaging, SendObject, WaitResult};

const USEC_PER_SEC: i64 = 1_000_000;

static NOW: AtomicI64 = AtomicI64::new(0);
static ACTIVE: Mutex<Option<MoqSink>> = Mutex::new(None);
static CONTINUING: Mutex<Option<MoqSinkPad>> = Mutex::new(None);

fn active() -> MoqSink {
    ACTIVE.lock().unwrap().clone().expect("active sink")
}

// -- clock: time only moves when the element waits or retries ----------------

fn now() -> i64 {
    NOW.load(Ordering::SeqCst)
}

fn write_other(p: &MoqSinkPad) {
    let payload = Buffer::copy_from(&[42]).unwrap();
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
    _fake: FakeSuite,
    _clock: crate::clock::ClockGuard,
}

impl Drop for Suite {
    fn drop(&mut self) {
        *ACTIVE.lock().unwrap() = None;
        *CONTINUING.lock().unwrap() = None;
    }
}

fn suite() -> Suite {
    // fake_suite takes the shared serial lock and resets the control block.
    let fake = fake_suite();
    // Waiting on the endpoint is what advances this suite's clock.
    control().on_wait = Some(Box::new(|| {
        NOW.fetch_add(USEC_PER_SEC, Ordering::SeqCst);
    }));
    Suite {
        _fake: fake,
        _clock: crate::clock::install(now, pause_retry),
    }
}

fn make_sink() -> MoqSink {
    NOW.store(100, Ordering::SeqCst);
    *CONTINUING.lock().unwrap() = None;
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
        let cfg = TrackCfg::new("", MediaType::Audio, Packaging::Cmaf, "");
        let t = s
            .imp()
            .send_lock()
            .sender
            .as_ref()
            .unwrap()
            .add_track(&cfg)
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
    assert_eq!(control().ends, 0);
    assert_eq!(control().drains, 0);
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
    control().refuse_ends = 2;
    assert!(eos(&s, &p));
    assert_eq!(control().ends, 3);
    assert_eq!(control().budget, USEC_PER_SEC as u64);
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
    control().refuse_ends = 1;
    assert!(eos(&s, &a));
    assert_eq!(control().writes, 1);
    assert!(control().ended[0]);
    assert!(!control().ended[1]);
    assert_eq!(control().drains, 0);
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
    assert!(!control().ended[0]);
    assert!(control().interrupted);
    flush(&s, &b, false);
    assert!(control().ended[0]);
    assert!(!control().ended[1]);
    release(&s, b);
    finish(s);
}

#[test]
fn flush_count() {
    let _suite = suite();
    // The C suite ran this after /sink/released, whose teardown leaves the
    // interrupt latch set; with no endpoint opened here nothing may clear it.
    control().interrupted = true;
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
    assert!(control().interrupted);
    s.imp().obj_lock().state_flushing = true;
    flush(&s, &b, false);
    assert!(control().interrupted);
    release(&s, b);
    finish(s);
}

#[test]
fn failed() {
    let _suite = suite();
    for result in [
        Ok(WaitResult::TimedOut),
        Err(Error::INTERRUPTED),
        Err(Error::CLOSED),
        Err(Error::UNSUPPORTED),
        Err(Error::WRONG_STATE),
    ] {
        let s = make_sink();
        let p = pad(&s, true);
        let bus = gst::Bus::new();
        s.set_bus(Some(&bus));
        control().drain_result = Some(result);
        assert!(!eos(&s, &p));
        let m = bus.pop().expect("a message");
        assert_eq!(m.type_(), gst::MessageType::Error, "drain={result:?}");
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
    // Flushing the draining pad from inside drain is what cancels the EOS.
    let canceller = p.clone();
    control().on_drain = Some(Box::new(move || {
        active().imp().set_pad_flush(&canceller, true);
    }));
    assert!(!eos(&s, &p));
    assert!(control().interrupted);
    control().on_drain = None;
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
    assert_eq!(control().drains, 0);
    release(&s, a);
    assert_eq!(control().drains, 1);
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
    assert_eq!(control().destroys, 1);
    let _ = s.set_state(gst::State::Paused);
    assert!(p.track().is_none());
    assert!(!p.eos());
    assert!(p.state().splitter.buf.is_empty());
    assert_eq!(control().connects, 1); // reconnect is deferred until media
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
    let before = control().ends;
    flush(&s, &b, false);
    assert_eq!(control().ends, before);
    assert!(s.imp().send_lock().eos_failed);
    assert!(!control().ended[0]);
    release(&s, b);
    finish(s);
}

/// Readiness is polled now, not delivered by a callback: the element logs the
/// record once, when `is_ready` first holds. `late` makes it hold only after
/// the element has waited, which is the case the old
/// `ready_after_attach`/`ready_during_attach` pair covered through libmoq's
/// deleted `on_ready` callback.
fn ready_record(late: bool) {
    let _suite = suite();
    if late {
        control().not_ready_for = 2;
    }
    let logs = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let seen = logs.clone();
    let cat = *super::imp::CAT;
    let previous = cat.threshold();
    let capture = gst::log::add_log_function(move |cat, _level, _file, _function, _line, _obj, msg| {
        let Some(text) = msg.get() else { return };
        if cat.name() == "moqsink" && text.starts_with("native sender ready") {
            assert_eq!(text.as_str(), "native sender ready namespace=example");
            seen.fetch_add(1, Ordering::SeqCst);
        }
    });
    cat.set_threshold(gst::DebugLevel::Info);

    let s = make_sink();
    let _ = s.set_state(gst::State::Paused);
    assert!(s.imp().started.load(Ordering::SeqCst));
    let p = pad(&s, true);
    // Exactly one record, whether readiness held at once or after waiting.
    assert_eq!(logs.load(Ordering::SeqCst), 1);
    if late {
        // It polled until readiness held rather than giving up.
        assert!(control().ready_calls >= 3);
    }

    gst::log::remove_log_function(capture);
    cat.set_threshold(previous);
    release(&s, p);
    finish(s);
}

#[test]
fn ready_at_attach() {
    ready_record(false);
}

#[test]
fn ready_after_waiting() {
    ready_record(true);
}

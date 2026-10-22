//! A programmable stand-in for the libmoq service, for the unit tests.
//!
//! It is a stub -- no relay, no network -- and a spy: every operation the
//! element performs is appended to an ordered log the tests assert on. The
//! element under test is the real one; only this boundary is replaced, which
//! is what lets `tests_barrier`, `tests_recovery` and `moqsrc::tests` check
//! the barrier, the sender state machine and track selection without a
//! server.
//!
//! The double is process-wide, like the clock seam in [`crate::clock`], so a
//! test takes [`suite`] first: it serialises against the other tests and
//! resets the control block.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use moq5::{
    ContentProtection, Error, MediaType, Packaging, Result, SendObject, SenderStats,
    SubscribeOptions, TerminalReason, TrackEventKind, WaitResult, WriteError,
};

use super::{EndpointCfg, ReceiverCfg, SenderCfg, TerminalInfo, TrackCfg};

// -- handles ----------------------------------------------------------------

/// A send-track handle: an index into the control block's per-track state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SendTrack(pub(crate) usize);

/// A receive-track handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Track(pub(crate) usize);

// -- the call log -----------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    /// A marker the test places to order its own steps against the element's.
    Mark,
    Connect,
    Attach,
    AddTrack,
    Write,
    EndTrack,
    Drain,
    Interrupt,
    Subscribe,
    DestroySender,
    DestroyReceiver,
}

static LOG: Mutex<Vec<(Op, Option<String>)>> = Mutex::new(Vec::new());

pub(crate) fn log_add(op: Op, detail: Option<&str>) {
    LOG.lock()
        .unwrap_or_else(|e| e.into_inner())
        .push((op, detail.map(str::to_owned)));
}

/// The index of the first `op` (with `detail`, when given), or -1.
pub(crate) fn log_find(op: Op, detail: Option<&str>) -> i32 {
    LOG.lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .position(|(o, d)| *o == op && detail.is_none_or(|want| d.as_deref() == Some(want)))
        .map_or(-1, |i| i as i32)
}

pub(crate) fn log_count(op: Op) -> usize {
    LOG.lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|(o, _)| *o == op)
        .count()
}

// -- what a test can arrange ------------------------------------------------

/// A track event queued for `poll_track`.
#[derive(Clone, Debug)]
pub(crate) struct Event {
    pub(crate) kind: TrackEventKind,
    /// The code `raw_kind` reports, for the kinds this crate only logs.
    pub(crate) raw_kind: u32,
    pub(crate) track: Option<Track>,
    pub(crate) desc: Option<DescData>,
}

/// A catalog description a queued event or query hands back.
#[derive(Clone, Debug, Default)]
pub(crate) struct DescData {
    pub(crate) name: Option<Vec<u8>>,
    pub(crate) codec: Vec<u8>,
    pub(crate) packaging: Option<Packaging>,
    pub(crate) init_data: Vec<u8>,
    pub(crate) codec_config: Vec<u8>,
    pub(crate) samplerate: Option<u32>,
    pub(crate) channel_config: Vec<u8>,
}

/// A media object queued for `poll_object`.
#[derive(Clone, Debug, Default)]
pub(crate) struct ObjectData {
    pub(crate) track: Option<Track>,
    pub(crate) packaging: Option<Packaging>,
    pub(crate) is_keyframe: bool,
    pub(crate) presentation_time_us: u64,
    pub(crate) payload: Vec<u8>,
    pub(crate) fragment: Vec<u8>,
}

/// Everything the double does that a test may want to steer.
pub(crate) struct Control {
    // -- sender
    /// Refuse `Sender::attach` with NOMEM.
    pub(crate) fail_attach: bool,
    /// Refuse the Nth (1-based) `add_track`; 0 refuses none.
    pub(crate) fail_add: u32,
    pub(crate) added_tracks: u32,
    /// Hold inside `attach` until released, so a test can flush mid-attach.
    pub(crate) hold_attach: bool,
    pub(crate) attach_entered: bool,
    /// `is_ready` answers false for this many calls, then true.
    pub(crate) not_ready_for: u32,
    pub(crate) ready_calls: u32,
    pub(crate) writes: u32,
    pub(crate) ends: u32,
    pub(crate) refusals: u32,
    /// Refuse the next `write` with WOULD_BLOCK this many times.
    pub(crate) refuse_writes: u32,
    /// Refuse the next `end_track` with WOULD_BLOCK this many times.
    pub(crate) refuse_ends: u32,
    pub(crate) ended: [bool; 16],
    pub(crate) stats: SenderStats,
    pub(crate) sender_fatal: Option<u64>,
    pub(crate) attached_cps: Vec<ContentProtection>,
    pub(crate) track_names: Vec<String>,
    pub(crate) added_cfgs: Vec<TrackCfg>,
    pub(crate) capture_video: u64,
    pub(crate) capture_audio: u64,
    pub(crate) audio_groups: u32,

    // -- endpoint
    pub(crate) connects: u32,
    /// What the element dialled, for the draft/verification assertions.
    pub(crate) dialled: Option<EndpointCfg>,
    pub(crate) connect_result: Option<Error>,
    pub(crate) receiver_cfg: Option<ReceiverCfg>,
    pub(crate) receiver_attach_result: Option<Error>,
    pub(crate) drains: u32,
    pub(crate) destroys: u32,
    pub(crate) interrupted: bool,
    /// What `drain` reports; `None` means it settled cleanly.
    pub(crate) drain_result: Option<std::result::Result<WaitResult, Error>>,
    /// The drain timeout the element asked for, in microseconds.
    pub(crate) budget: u64,
    /// Run when the element waits on the endpoint -- the recovery suite
    /// advances its clock here, so deadlines are deterministic.
    pub(crate) on_wait: Option<Box<dyn FnMut() + Send>>,
    /// Run when the element drains, for the cancel-during-drain case.
    pub(crate) on_drain: Option<Box<dyn FnMut() + Send>>,
    pub(crate) terminal_getter_fails: bool,
    pub(crate) terminal_reason: TerminalReason,
    pub(crate) terminal_detail: u64,

    // -- receiver
    pub(crate) events: Vec<Event>,
    pub(crate) event_index: usize,
    pub(crate) objects: Vec<ObjectData>,
    pub(crate) object_index: usize,
    pub(crate) waits: u32,
    /// An unexpected wait is a quick failure unless a test wants otherwise.
    pub(crate) wait_result: Option<Error>,
    pub(crate) cleaned: u32,
    pub(crate) subscriptions: u32,
    pub(crate) subscribed_track: Option<Track>,
    pub(crate) subscribe_result: Option<Error>,
    /// What `poll_object` reports once the queue is empty: `None` is simply
    /// "nothing queued now".
    pub(crate) empty_result: Option<Error>,
    pub(crate) receiver_fatal: Option<u64>,
    pub(crate) negotiated_draft: u32,
    pub(crate) startup_calls: String,
    /// The terminal diagnostic the element logged, captured by the test.
    pub(crate) terminal_diagnostic: Option<String>,
}

impl Control {
    fn new() -> Control {
        Control {
            fail_attach: false,
            fail_add: 0,
            added_tracks: 0,
            hold_attach: false,
            attach_entered: false,
            not_ready_for: 0,
            ready_calls: 0,
            writes: 0,
            ends: 0,
            refusals: 0,
            refuse_writes: 0,
            refuse_ends: 0,
            ended: [false; 16],
            stats: SenderStats::default(),
            sender_fatal: None,
            attached_cps: Vec::new(),
            track_names: Vec::new(),
            added_cfgs: Vec::new(),
            capture_video: 0,
            capture_audio: 0,
            audio_groups: 0,
            budget: 0,
            connects: 0,
            dialled: None,
            connect_result: None,
            receiver_cfg: None,
            receiver_attach_result: None,
            drains: 0,
            destroys: 0,
            interrupted: false,
            drain_result: None,
            on_wait: None,
            on_drain: None,
            terminal_getter_fails: false,
            terminal_reason: TerminalReason::None,
            terminal_detail: 0,
            events: Vec::new(),
            event_index: 0,
            objects: Vec::new(),
            object_index: 0,
            waits: 0,
            wait_result: Some(Error::INTERRUPTED),
            cleaned: 0,
            subscriptions: 0,
            subscribed_track: None,
            subscribe_result: None,
            empty_result: None,
            receiver_fatal: None,
            negotiated_draft: 0,
            startup_calls: String::new(),
            terminal_diagnostic: None,
        }
    }
}

static CONTROL: Mutex<Option<Control>> = Mutex::new(None);
static COND: Condvar = Condvar::new();
static RELEASED: AtomicBool = AtomicBool::new(false);

/// The control block. Panics outside a [`suite`], which is the point: a test
/// that forgot to take one would otherwise share state with its neighbour.
pub(crate) fn control() -> MappedControl {
    MappedControl(CONTROL.lock().unwrap_or_else(|e| e.into_inner()))
}

/// A guard that derefs to the control block.
pub(crate) struct MappedControl(MutexGuard<'static, Option<Control>>);

impl std::ops::Deref for MappedControl {
    type Target = Control;
    fn deref(&self) -> &Control {
        self.0.as_ref().expect("fake::suite() was not taken")
    }
}

impl std::ops::DerefMut for MappedControl {
    fn deref_mut(&mut self) -> &mut Control {
        self.0.as_mut().expect("fake::suite() was not taken")
    }
}

/// Serialises the test and installs a fresh control block, which is torn down
/// when the guard drops.
#[must_use = "the double is uninstalled when the guard drops"]
pub(crate) struct Suite(
    /// Held, not read: it serialises this test against the others.
    #[allow(dead_code)]
    MutexGuard<'static, ()>,
);

impl Drop for Suite {
    fn drop(&mut self) {
        *CONTROL.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

pub(crate) fn suite() -> Suite {
    // The shared serial lock: it also initialises GStreamer, and the clock
    // seam's users take the same one.
    let serial = crate::test_support::serial();
    LOG.lock().unwrap_or_else(|e| e.into_inner()).clear();
    RELEASED.store(false, Ordering::SeqCst);
    *CONTROL.lock().unwrap_or_else(|e| e.into_inner()) = Some(Control::new());
    Suite(serial)
}

/// Let an `attach` held by `hold_attach` proceed.
pub(crate) fn release_attach() {
    RELEASED.store(true, Ordering::SeqCst);
    COND.notify_all();
}

/// Block until the element is inside `attach`.
pub(crate) fn wait_attach_entered() {
    let mut c = CONTROL.lock().unwrap_or_else(|e| e.into_inner());
    while !c.as_ref().is_some_and(|c| c.attach_entered) {
        c = COND.wait(c).unwrap_or_else(|e| e.into_inner());
    }
}

// -- endpoint ---------------------------------------------------------------

/// A handle on the endpoint. `moq5::Endpoint` is refcounted, so cloning it
/// does not open a second one and only the last handle tears it down -- the
/// restart case counts exactly one teardown across the element's clones.
#[derive(Clone, Debug)]
pub(crate) struct Endpoint(Arc<()>);

impl Drop for Endpoint {
    /// `moq5::Endpoint`'s own drop stops and destroys the endpoint when the
    /// last handle goes; the startup tests assert that call order.
    fn drop(&mut self) {
        if Arc::strong_count(&self.0) > 1 {
            return;
        }
        if let Ok(mut guard) = CONTROL.lock() {
            if let Some(c) = guard.as_mut() {
                c.destroys += 1;
                c.startup_calls.push_str("stop;destroy;");
            }
        }
    }
}

impl Endpoint {
    pub(crate) fn connect(config: &EndpointCfg) -> Result<Endpoint> {
        let mut c = control();
        c.connects += 1;
        c.dialled = Some(config.clone());
        c.startup_calls.push_str("connect;");
        log_add(Op::Connect, None);
        match c.connect_result {
            Some(e) => Err(e),
            None => Ok(Endpoint(Arc::new(()))),
        }
    }

    pub(crate) fn drain(&self, timeout: Duration) -> Result<WaitResult> {
        let hook = {
            let mut c = control();
            c.drains += 1;
            c.budget = timeout.as_micros() as u64;
            log_add(Op::Drain, None);
            c.on_drain.take()
        };
        // Outside the lock: the hook reaches back into the element, which
        // may call the double again.
        if let Some(mut hook) = hook {
            hook();
            control().on_drain = Some(hook);
        }
        let c = control();
        if c.interrupted {
            return Err(Error::INTERRUPTED);
        }
        c.drain_result.unwrap_or(Ok(WaitResult::Ready))
    }

    pub(crate) fn set_interrupted(&self, interrupted: bool) {
        control().interrupted = interrupted;
        log_add(Op::Interrupt, None);
    }

    pub(crate) fn terminal(&self) -> Result<TerminalInfo> {
        let mut c = control();
        c.startup_calls.push_str("get;");
        if c.terminal_getter_fails {
            return Err(Error::INVAL);
        }
        Ok(TerminalInfo {
            reason: c.terminal_reason,
            detail_code: c.terminal_detail,
        })
    }

    pub(crate) fn wait(&self, _timeout: Duration) -> Result<WaitResult> {
        let hook = control().on_wait.take();
        if let Some(mut hook) = hook {
            hook();
            control().on_wait = Some(hook);
        }
        if control().interrupted {
            return Err(Error::INTERRUPTED);
        }
        Ok(WaitResult::Ready)
    }
}

// -- sender -----------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct Sender;

impl Drop for Sender {
    fn drop(&mut self) {
        log_add(Op::DestroySender, None);
    }
}

impl Sender {
    pub(crate) fn attach(_endpoint: &Endpoint, config: &SenderCfg) -> Result<Sender> {
        {
            let mut c = control();
            c.attach_entered = true;
            c.attached_cps = config.content_protections.clone();
            COND.notify_all();
            if c.hold_attach {
                drop(c);
                let mut guard = CONTROL.lock().unwrap_or_else(|e| e.into_inner());
                while !RELEASED.load(Ordering::SeqCst) {
                    guard = COND.wait(guard).unwrap_or_else(|e| e.into_inner());
                }
            }
        }
        log_add(Op::Attach, None);
        if control().fail_attach {
            return Err(Error::NOMEM);
        }
        Ok(Sender)
    }

    pub(crate) fn add_track(&self, config: &TrackCfg) -> Result<SendTrack> {
        let mut c = control();
        c.added_tracks += 1;
        let n = c.added_tracks;
        let name = config.name.clone();
        log_add(Op::AddTrack, Some(&name));
        if c.fail_add == n {
            return Err(Error::NOMEM);
        }
        let index = c.track_names.len();
        c.track_names.push(name);
        c.added_cfgs.push(config.clone());
        Ok(SendTrack(index))
    }

    pub(crate) fn write(
        &self,
        track: SendTrack,
        object: SendObject,
    ) -> std::result::Result<(), WriteError> {
        let mut c = control();
        if c.interrupted {
            return Err(WriteError {
                error: Error::INTERRUPTED,
                object,
            });
        }
        if c.refuse_writes > 0 {
            c.refuse_writes -= 1;
            c.refusals += 1;
            return Err(WriteError {
                error: Error::WOULD_BLOCK,
                object,
            });
        }
        // Writing an ended track is the state error libmoq reports.
        if c.ended.get(track.0).copied().unwrap_or(false) {
            return Err(WriteError {
                error: Error::WRONG_STATE,
                object,
            });
        }
        if track.0 >= c.track_names.len() {
            return Err(WriteError {
                error: Error::INVAL,
                object,
            });
        }
        c.writes += 1;
        // The LOC timing the barrier suite asserts: the capture timestamp per
        // media kind, and how often an audio object opened a group.
        let audio = c
            .added_cfgs
            .get(track.0)
            .is_some_and(|cfg| cfg.media_type == MediaType::Audio);
        if audio {
            c.capture_audio = object.capture_time_us.unwrap_or(0);
            if object.starts_group {
                c.audio_groups += 1;
            }
        } else {
            c.capture_video = object.capture_time_us.unwrap_or(0);
        }
        let name = c.track_names.get(track.0).cloned().unwrap_or_default();
        log_add(Op::Write, Some(&name));
        Ok(())
    }

    pub(crate) fn end_track(&self, track: SendTrack) -> Result<()> {
        let mut c = control();
        c.ends += 1;
        if c.interrupted {
            return Err(Error::INTERRUPTED);
        }
        if c.refuse_ends > 0 {
            c.refuse_ends -= 1;
            return Err(Error::WOULD_BLOCK);
        }
        if track.0 >= c.track_names.len() {
            return Err(Error::INVAL);
        }
        if let Some(slot) = c.ended.get_mut(track.0) {
            *slot = true;
        }
        let name = c.track_names.get(track.0).cloned().unwrap_or_default();
        log_add(Op::EndTrack, Some(&name));
        Ok(())
    }

    pub(crate) fn stats(&self) -> Result<SenderStats> {
        Ok(control().stats.clone())
    }

    pub(crate) fn is_ready(&self) -> bool {
        let mut c = control();
        c.ready_calls += 1;
        c.ready_calls > c.not_ready_for
    }

    pub(crate) fn is_fatal(&self) -> bool {
        control().sender_fatal.is_some()
    }

    pub(crate) fn fatal_code(&self) -> u64 {
        control().sender_fatal.unwrap_or(0)
    }

    pub(crate) fn wait(&self, _timeout: Duration) -> Result<WaitResult> {
        Ok(WaitResult::Ready)
    }
}

// -- receiver ---------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct Receiver;

impl Drop for Receiver {
    fn drop(&mut self) {
        log_add(Op::DestroyReceiver, None);
    }
}

impl Receiver {
    pub(crate) fn attach(_endpoint: &Endpoint, config: &ReceiverCfg) -> Result<Receiver> {
        let mut c = control();
        c.receiver_cfg = Some(config.clone());
        c.startup_calls.push_str("attach;");
        log_add(Op::Attach, None);
        match c.receiver_attach_result {
            Some(e) => Err(e),
            None => Ok(Receiver),
        }
    }

    pub(crate) fn subscribe_track(
        &self,
        track: Track,
        _options: Option<SubscribeOptions>,
    ) -> Result<()> {
        let mut c = control();
        c.subscriptions += 1;
        c.subscribed_track = Some(track);
        c.startup_calls.push_str("subscribe;");
        log_add(Op::Subscribe, None);
        match c.subscribe_result {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub(crate) fn poll_track(&self) -> Result<Option<TrackEvt<'_>>> {
        let mut c = control();
        if c.event_index >= c.events.len() {
            return Ok(None);
        }
        let ev = c.events[c.event_index].clone();
        c.event_index += 1;
        Ok(Some(TrackEvt {
            event: ev,
            _receiver: std::marker::PhantomData,
        }))
    }

    pub(crate) fn poll_object(&self) -> Result<Option<Object>> {
        let mut c = control();
        if c.object_index >= c.objects.len() {
            return match c.empty_result {
                Some(e) => Err(e),
                None => Ok(None),
            };
        }
        let o = c.objects[c.object_index].clone();
        c.object_index += 1;
        Ok(Some(Object(o)))
    }

    pub(crate) fn wait(&self, _timeout: Duration) -> Result<WaitResult> {
        let mut c = control();
        c.waits += 1;
        match c.wait_result {
            Some(e) => Err(e),
            None => Ok(WaitResult::Ready),
        }
    }

    pub(crate) fn is_fatal(&self) -> bool {
        control().receiver_fatal.is_some()
    }

    pub(crate) fn fatal_code(&self) -> u64 {
        control().receiver_fatal.unwrap_or(0)
    }

    pub(crate) fn negotiated_draft(&self) -> u32 {
        control().negotiated_draft
    }
}

// -- what the receiver hands back -------------------------------------------

pub(crate) struct TrackEvt<'r> {
    event: Event,
    _receiver: std::marker::PhantomData<&'r Receiver>,
}

impl<'r> TrackEvt<'r> {
    pub(crate) fn kind(&self) -> TrackEventKind {
        self.event.kind
    }
    pub(crate) fn track(&self) -> Option<Track> {
        self.event.track
    }
    pub(crate) fn description(&self) -> Result<Option<Desc<'r>>> {
        Ok(self.event.desc.clone().map(|d| Desc {
            data: d,
            _receiver: std::marker::PhantomData,
        }))
    }
    pub(crate) fn raw_kind(&self) -> u32 {
        self.event.raw_kind
    }
}

pub(crate) struct Desc<'r> {
    data: DescData,
    _receiver: std::marker::PhantomData<&'r Receiver>,
}

impl<'r> Desc<'r> {
    pub(crate) fn name(&self) -> &[u8] {
        self.data.name.as_deref().unwrap_or(&[])
    }
    pub(crate) fn codec(&self) -> &[u8] {
        &self.data.codec
    }
    pub(crate) fn packaging(&self) -> Option<Packaging> {
        self.data.packaging
    }
    pub(crate) fn init_data(&self) -> &[u8] {
        &self.data.init_data
    }
    pub(crate) fn codec_config(&self) -> &[u8] {
        &self.data.codec_config
    }
    pub(crate) fn samplerate(&self) -> Option<u32> {
        self.data.samplerate
    }
    pub(crate) fn channel_config(&self) -> &[u8] {
        &self.data.channel_config
    }
    pub(crate) fn has_name(&self) -> bool {
        self.data.name.is_some()
    }
}

pub(crate) struct Object(ObjectData);

impl Drop for Object {
    fn drop(&mut self) {
        if let Ok(mut guard) = CONTROL.lock() {
            if let Some(c) = guard.as_mut() {
                c.cleaned += 1;
            }
        }
    }
}

impl Object {
    pub(crate) fn track(&self) -> Option<Track> {
        self.0.track
    }
    pub(crate) fn packaging(&self) -> Option<Packaging> {
        self.0.packaging
    }
    pub(crate) fn is_keyframe(&self) -> bool {
        self.0.is_keyframe
    }
    pub(crate) fn presentation_time_us(&self) -> u64 {
        self.0.presentation_time_us
    }
    pub(crate) fn payload(&self) -> &[u8] {
        &self.0.payload
    }
    pub(crate) fn fragment(&self) -> &[u8] {
        &self.0.fragment
    }
}


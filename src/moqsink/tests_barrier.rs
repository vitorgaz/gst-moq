//! The track barrier: when `moqsink` opens a session, and with which tracks.
//!
//! Port of tests/test_sink_barrier.c. The element is the real one; the libmoq
//! service is the double in [`crate::service::fake`], which records an ordered
//! log of what the element asked for, so "connected too early" is an ordering
//! violation rather than a race.

use gst::prelude::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::MoqSink;
use crate::service::fake::{
    control, log_add, log_count, log_find, release_attach, suite as fake_suite, wait_attach_entered,
    Op, Suite as FakeSuite,
};
use moq5::sys::{
    moq_cmaf_fragment_info_init, moq_cmaf_init_info_init, moq_cmaf_init_info_t,
    moq_cmaf_parse_fragment, moq_cmaf_parse_init, moq_cmaf_sample_t, moq_cmaf_fragment_info_t,
    MOQ_OK,
};
use crate::test_support::{bytes, span};

// -- fixture -------------------------------------------------------------------

type Suite = FakeSuite;

fn suite() -> Suite {
    fake_suite()
}

/// The refIDs the element declared on the track called `name`, comma joined.
fn track_refs(name: &str) -> Option<String> {
    control()
        .added_cfgs
        .iter()
        .find(|cfg| cfg.name == name)
        .map(|cfg| cfg.cp_ref_ids.join(","))
}

/// Poll until `done` holds or a second passes.
fn wait_until(done: impl Fn() -> bool) -> bool {
    let limit = Instant::now() + Duration::from_secs(1);
    while !done() {
        if Instant::now() >= limit {
            return false;
        }
        std::thread::yield_now();
    }
    true
}

/// Set once the element logs that it is parked in the barrier.
static BARRIER_WAITING: AtomicBool = AtomicBool::new(false);

/// avcC: version 1, profile 0x42, compat 0xc0, level 0x1e. prepare_loc_track's
/// stream-format=avc path needs only these four bytes to be well-formed.
const AVCC: [u8; 6] = [1, 0x42, 0xc0, 0x1e, 0xff, 0xe1];
/// AudioSpecificConfig: AOT 2 (AAC-LC), 48 kHz, stereo.
const ASC: [u8; 2] = [0x11, 0x90];

fn video_caps() -> gst::Caps {
    gst::Caps::builder("video/x-h264")
        .field("stream-format", "avc")
        .field("alignment", "au")
        .field("width", 1280i32)
        .field("height", 720i32)
        .field("framerate", gst::Fraction::new(24, 1))
        .field("codec_data", gst::Buffer::from_slice(AVCC))
        .build()
}

fn audio_caps() -> gst::Caps {
    gst::Caps::builder("audio/mpeg")
        .field("mpegversion", 4i32)
        .field("stream-format", "raw")
        .field("rate", 48000i32)
        .field("channels", 2i32)
        .field("codec_data", gst::Buffer::from_slice(ASC))
        .build()
}

/// A src pad linked to `name` on `sink`, activated, with the sticky events a
/// chain function is entitled to assume.
fn open_src_pad(sink: &MoqSink, name: &str, stream_id: &str, caps: gst::Caps) -> gst::Pad {
    let src = gst::Pad::builder(gst::PadDirection::Src).build();
    let peer = sink.static_pad(name).expect("sink pad");
    src.link(&peer).unwrap();

    src.set_active(true).unwrap();
    assert!(src.push_event(gst::event::StreamStart::new(stream_id)));
    src.push_event(gst::event::Caps::new(&caps));
    // push_event returns TRUE for a caps event the peer REJECTED -- it only
    // marks the sticky event pending -- so check negotiation explicitly.
    // Without this a caps mismatch surfaces later as a confusing flow error
    // from the chain function instead of a failure here.
    assert!(peer.has_current_caps());

    let seg = gst::FormattedSegment::<gst::ClockTime>::new();
    assert!(src.push_event(gst::event::Segment::new(&seg)));
    src
}

fn close_source(src: gst::Pad) {
    let _ = src.set_active(false);
}

/// A keyframe (no DELTA_UNIT flag) at `pts`. Payload content is irrelevant on
/// the stream-format=avc path -- the avcC comes from the caps.
fn keyframe(pts: gst::ClockTime, len: usize) -> gst::Buffer {
    let mut b = gst::Buffer::from_slice(vec![0u8; len]);
    {
        let b = b.get_mut().unwrap();
        b.set_pts(pts);
        b.set_dts(pts);
    }
    b
}

fn new_sink() -> MoqSink {
    gst::glib::Object::builder::<MoqSink>()
        // Never dialled: moq_endpoint_connect is mocked.
        .property("host", "127.0.0.1")
        .property("port", 1u32)
        // No clock waits -- the barrier is what this test times, nothing else.
        .property("sync", false)
        .build()
}

// -- a push on its own streaming thread ----------------------------------------

#[derive(Default)]
struct JobState {
    entered: bool,
    done: bool,
    ret: Option<Result<gst::FlowSuccess, gst::FlowError>>,
}

struct PushJob {
    state: Mutex<JobState>,
    cond: Condvar,
}

impl PushJob {
    fn start(src: &gst::Pad, buffer: gst::Buffer) -> (Arc<PushJob>, JoinHandle<()>) {
        let job = Arc::new(PushJob {
            state: Mutex::new(JobState::default()),
            cond: Condvar::new(),
        });
        let (j, src) = (job.clone(), src.clone());
        let th = std::thread::spawn(move || {
            {
                j.state.lock().unwrap().entered = true;
                j.cond.notify_all();
            }
            let ret = src.push(buffer);
            let mut s = j.state.lock().unwrap();
            s.ret = Some(ret);
            s.done = true;
            j.cond.notify_all();
        });
        (job, th)
    }

    fn wait_entered(&self) {
        let mut s = self.state.lock().unwrap();
        while !s.entered {
            s = self.cond.wait(s).unwrap();
        }
    }

    /// Wait up to `limit` for the push to finish; true if it did.
    fn wait_done(&self, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        let mut s = self.state.lock().unwrap();
        while !s.done {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            s = self.cond.wait_timeout(s, deadline - now).unwrap().0;
        }
        s.done
    }

    fn join(self: Arc<Self>, th: JoinHandle<()>, expect: Result<gst::FlowSuccess, gst::FlowError>) {
        th.join().unwrap();
        assert_eq!(self.state.lock().unwrap().ret, Some(expect));
    }
}

const OK: Result<gst::FlowSuccess, gst::FlowError> = Ok(gst::FlowSuccess::Ok);

fn ms(v: u64) -> gst::ClockTime {
    gst::ClockTime::from_mseconds(v)
}

// -- tests -----------------------------------------------------------------------

/// Audio's first buffer arrives first and must NOT open the session: the
/// catalog cannot be published until the video pad has produced its track too.
#[test]
fn initial_catalog_complete() {
    let _suite = suite();
    let sink = new_sink();

    // PAUSED first: the element activates its sink pads on READY->PAUSED, and
    // an inactive pad rejects the sticky events open_src_pad pushes.
    assert!(sink.set_state(gst::State::Paused).is_ok());

    let asrc = open_src_pad(&sink, "audio", "audio-stream", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "video-stream", video_caps());

    // Audio first, on its own streaming thread: it must park in the barrier.
    let (job, th) = PushJob::start(&asrc, keyframe(gst::ClockTime::ZERO, 64));
    job.wait_entered();
    // SECONDARY, bounded: the push must still be in flight. A barrier that
    // released on one pad would have completed it by now.
    assert!(!job.wait_done(Duration::from_millis(250)));

    // PRIMARY: everything the element does from here on is logged after this
    // marker, so an early connect is an ordering violation, not a race.
    log_add(Op::Mark, Some("push-video"));
    assert_eq!(vsrc.push(keyframe(gst::ClockTime::ZERO, 64)), OK);

    job.join(th, OK);

    let mark = log_find(Op::Mark, Some("push-video"));
    let connect = log_find(Op::Connect, None);
    let attach = log_find(Op::Attach, None);
    assert!(mark >= 0);
    assert!(connect > mark);
    assert!(attach > connect);

    // One session, both tracks, and both added before anything is written:
    // the session cannot reach ESTABLISHED with a partial track set.
    assert_eq!(log_count(Op::Connect), 1);
    assert_eq!(log_count(Op::Attach), 1);
    assert_eq!(log_count(Op::AddTrack), 2);

    let audio_track = log_find(Op::AddTrack, Some("audio"));
    let video_track = log_find(Op::AddTrack, Some("video"));
    assert!(audio_track > attach);
    assert!(video_track > attach);

    let first_write = log_find(Op::Write, None);
    assert!(first_write > audio_track);
    assert!(first_write > video_track);

    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
}

/// The always pads exist even when a pipeline feeds only one of them. An
/// unlinked pad must not hold the barrier shut -- and this is what keeps the
/// ordering assertion above honest: here CONNECT *does* land on the first
/// buffer, so the oracle can observe an early connect when one is correct.
#[test]
fn unlinked_pad_excluded() {
    let _suite = suite();
    let sink = new_sink();

    assert!(sink.set_state(gst::State::Paused).is_ok());

    // "audio" deliberately left unlinked.
    let vsrc = open_src_pad(&sink, "video", "video-stream", video_caps());

    log_add(Op::Mark, Some("push-video"));
    assert_eq!(vsrc.push(keyframe(gst::ClockTime::ZERO, 64)), OK);

    let mark = log_find(Op::Mark, Some("push-video"));
    let connect = log_find(Op::Connect, None);
    assert!(connect > mark);
    assert_eq!(log_count(Op::Connect), 1);
    assert_eq!(log_count(Op::AddTrack), 1);
    assert!(log_find(Op::AddTrack, Some("video")) >= 0);
    assert_eq!(log_find(Op::AddTrack, Some("audio")), -1);

    let _ = sink.set_state(gst::State::Null);
    close_source(vsrc);
}

#[test]
fn timeout_no_partial() {
    let _suite = suite();
    let sink = new_sink();
    sink.set_property("catalog-wait-ms", 10u32);
    let bus = gst::Bus::new();
    sink.set_bus(Some(&bus));
    let _ = sink.set_state(gst::State::Paused);
    let asrc = open_src_pad(&sink, "audio", "a", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "v", video_caps());
    assert_eq!(
        asrc.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Error)
    );
    assert_eq!(log_count(Op::Connect), 0);
    assert_eq!(log_count(Op::Write), 0);
    assert!(bus.pop_filtered(&[gst::MessageType::Error]).is_some());
    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
    sink.set_bus(None);
}

#[test]
fn empty_eos() {
    let _suite = suite();
    let sink = new_sink();
    let bus = gst::Bus::new();
    sink.set_bus(Some(&bus));
    let _ = sink.set_state(gst::State::Paused);
    let src = open_src_pad(&sink, "video", "v", video_caps());
    assert!(src.push_event(gst::event::Eos::new()));
    assert_eq!(log_count(Op::Connect), 0);
    let m = bus
        .pop_filtered(&[gst::MessageType::Eos, gst::MessageType::Error])
        .expect("a message");
    assert_eq!(m.type_(), gst::MessageType::Eos);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
    sink.set_bus(None);
}

fn flush_cancels(state_change: bool) {
    let _suite = suite();
    BARRIER_WAITING.store(false, Ordering::SeqCst);
    let observe = gst::log::add_log_function(|_cat, _level, _file, _fn, _line, _obj, msg| {
        if msg
            .get()
            .is_some_and(|m| m.as_str() == "waiting for linked track set")
        {
            BARRIER_WAITING.store(true, Ordering::SeqCst);
        }
    });
    let cat = *super::imp::CAT;
    let previous = cat.threshold();
    cat.set_threshold(gst::DebugLevel::Debug);

    let sink = new_sink();
    sink.set_property("catalog-wait-ms", 5000u32);
    let _ = sink.set_state(gst::State::Paused);
    let asrc = open_src_pad(&sink, "audio", "a", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "v", video_caps());
    let (job, th) = PushJob::start(&asrc, keyframe(gst::ClockTime::ZERO, 64));
    assert!(wait_until(|| BARRIER_WAITING.load(Ordering::SeqCst)));
    if state_change {
        assert!(sink.set_state(gst::State::Ready).is_ok());
    } else {
        assert!(asrc.push_event(gst::event::FlushStart::new()));
    }
    assert!(job.wait_done(Duration::from_secs(1)));
    job.join(th, Err(gst::FlowError::Flushing));
    assert_eq!(log_count(Op::Connect), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
    gst::log::remove_log_function(observe);
    cat.set_threshold(previous);
}

#[test]
fn flush_cancels_barrier() {
    flush_cancels(false);
}

#[test]
fn state_cancels_barrier() {
    flush_cancels(true);
}

/// A startup that fails at the service: `attach` refused, or the first
/// `add_track` refused. Either way no media may be written.
///
/// The old suite had two more modes, for libmoq's posted startup task:
/// `queued_start_cancel` (flush a task still queued) and `post_rejected`
/// (`moq_endpoint_post` refusing). The moq5 crate no longer exposes
/// `moq_endpoint_post`, so the element attaches and adds its tracks directly
/// and neither case exists; cancelling while the barrier waits is covered by
/// `flush_cancels_barrier`, and cancelling mid-attach by
/// `running_start_cancel`.
fn start_failure(attach: bool) {
    let _suite = suite();
    {
        let mut c = control();
        c.fail_attach = attach;
        c.fail_add = if attach { 0 } else { 1 };
    }
    let sink = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let src = open_src_pad(&sink, "video", "v", video_caps());
    assert_eq!(
        src.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Error)
    );
    assert_eq!(log_count(Op::Write), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

#[test]
fn attach_failed() {
    start_failure(true);
}

#[test]
fn add_failed() {
    start_failure(false);
}

#[test]
fn partial_add_failed() {
    let _suite = suite();
    control().fail_add = 2;
    let sink = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let asrc = open_src_pad(&sink, "audio", "a", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "v", video_caps());
    let (job, th) = PushJob::start(&asrc, keyframe(gst::ClockTime::ZERO, 64));
    assert_eq!(
        vsrc.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Error)
    );
    job.join(th, Err(gst::FlowError::Error));
    assert_eq!(control().added_tracks, 2);
    assert_eq!(log_count(Op::Write), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
}

#[test]
fn running_start_cancel() {
    let _suite = suite();
    control().hold_attach = true;
    let sink = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let src = open_src_pad(&sink, "video", "v", video_caps());
    let (job, th) = PushJob::start(&src, keyframe(gst::ClockTime::ZERO, 64));
    wait_attach_entered();
    let sinkpad = sink.static_pad("video").unwrap();
    // The pad's public flushing flag is set by FLUSH_START before the handler
    // runs; release the held attach only once it is.
    let release = std::thread::spawn(move || {
        let limit = Instant::now() + Duration::from_secs(1);
        while !sinkpad.pad_flags().contains(gst::PadFlags::FLUSHING) && Instant::now() < limit {
            std::thread::yield_now();
        }
        assert!(sinkpad.pad_flags().contains(gst::PadFlags::FLUSHING));
        release_attach();
    });
    assert!(src.push_event(gst::event::FlushStart::new()));
    release.join().unwrap();
    job.join(th, Err(gst::FlowError::Flushing));
    assert_eq!(log_count(Op::Write), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

/// mode 0: truncated ASC; 1: HE-AAC ASC; 2: caps rate disagrees with the ASC.
fn bad_audio_config(mode: u32) {
    let _suite = suite();
    let sink = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let mut caps = audio_caps();
    {
        let config = [if mode == 1 { 0x2a } else { 0x11 }, 0x90];
        let len = if mode == 0 { 1 } else { 2 };
        let caps = caps.make_mut();
        caps.set(
            "codec_data",
            gst::Buffer::from_slice(config[..len].to_vec()),
        );
        if mode == 2 {
            caps.set("rate", 44100i32);
        }
    }
    let src = open_src_pad(&sink, "audio", "a", caps);
    assert_eq!(
        src.push(keyframe(gst::ClockTime::ZERO, 64)),
        Err(gst::FlowError::Error)
    );
    assert_eq!(log_count(Op::Connect), 0);
    let _ = sink.set_state(gst::State::Null);
    close_source(src);
}

#[test]
fn audio_truncated() {
    bad_audio_config(0);
}

#[test]
fn audio_he_rejected() {
    bad_audio_config(1);
}

#[test]
fn audio_caps_mismatch() {
    bad_audio_config(2);
}

fn push_segment(src: &gst::Pad, offset: gst::ClockTime) {
    let mut seg = gst::FormattedSegment::<gst::ClockTime>::new();
    seg.set_start(offset);
    seg.set_position(offset);
    assert!(src.push_event(gst::event::Segment::new(&seg)));
}

#[test]
fn capture_and_groups() {
    let _suite = suite();
    let sink = new_sink();
    let _ = sink.set_state(gst::State::Paused);
    let asrc = open_src_pad(&sink, "audio", "a", audio_caps());
    let vsrc = open_src_pad(&sink, "video", "v", video_caps());
    // Common encoder offset: video PTS is large but running time is zero.
    let offset = gst::ClockTime::from_seconds(3_600_000);
    push_segment(&vsrc, offset);
    let (job, th) = PushJob::start(&asrc, keyframe(gst::ClockTime::ZERO, 64));
    assert_eq!(vsrc.push(keyframe(offset, 64)), OK);
    job.join(th, OK);
    {
        let c = control();
        assert_eq!(c.capture_video, c.capture_audio);
    }
    let before = control().audio_groups;
    assert_eq!(asrc.push(keyframe(ms(20), 64)), OK);
    let after = control().audio_groups;
    assert_eq!(asrc.push(keyframe(ms(40), 64)), OK);
    assert_eq!(control().audio_groups, after); // no group for every AAC AU
    assert!(after <= before + 1);
    assert_eq!(vsrc.push(keyframe(offset + ms(100), 64)), OK);
    assert_eq!(asrc.push(keyframe(ms(120), 64)), OK);
    assert_eq!(control().audio_groups, after + 1);
    let _ = sink.set_state(gst::State::Null);
    close_source(asrc);
    close_source(vsrc);
}

// -- CMSF contentProtections (CMSF 4.1.1) ----------------------------------------

/// A CMAF init segment, optionally CENC-protected:
///   ftyp + moov(trak(tkhd mdia(mdhd minf(stbl(stsd(
///       <avc1|encv>(VisualSampleEntry avcC [sinf(frma schm schi(tenc))]))))))
/// Layout mirrors libmoq's own cmaf parser expectations; the first assertion
/// of every test below is that libmoq parses this back, so a wrong fixture
/// fails loudly rather than quietly weakening the test.
const FIXTURE_AVCC: [u8; 4] = [0x01, 0x42, 0xe0, 0x1e];

fn box_hdr(out: &mut Vec<u8>, size: usize, ty: &[u8; 4]) {
    out.extend_from_slice(&(size as u32).to_be_bytes());
    out.extend_from_slice(ty);
}

fn be32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn build_cmaf_init(
    timescale: u32,
    w: u16,
    h: u16,
    protected: bool,
    scheme: &[u8; 4],
    kid: &[u8; 16],
) -> Vec<u8> {
    let avcc_box = 8 + FIXTURE_AVCC.len();
    // ISO/IEC 23001-7 8.2.2 TrackEncryptionBox body: version/flags(4) +
    // reserved(1) + reserved-or-pattern(1) + default_isProtected(1) +
    // default_Per_Sample_IV_Size(1) + default_KID(16) = 24. TWO bytes precede
    // isProtected, not one -- a fixture that writes 23 shifts the KID by a byte
    // and silently agrees with a parser that reads it one byte early.
    let tenc_box = if protected { 8 + 24 } else { 0 };
    let schi_box = if protected { 8 + tenc_box } else { 0 };
    let schm_box = if protected { 20 } else { 0 }; // hdr + v/f + 4cc + version
    let frma_box = if protected { 12 } else { 0 }; // hdr + 4cc
    let sinf_box = if protected {
        8 + frma_box + schm_box + schi_box
    } else {
        0
    };
    let entry_box = 8 + 78 + avcc_box + sinf_box;
    let stsd_box = 8 + 8 + entry_box;
    let stbl_box = 8 + stsd_box;
    let minf_box = 8 + stbl_box;
    let mdhd_box = 28;
    let mdia_box = 8 + mdhd_box + minf_box;
    let tkhd_box = 24;
    let trak_box = 8 + tkhd_box + mdia_box;
    let moov_box = 8 + trak_box;
    let mut o = Vec::new();

    box_hdr(&mut o, 20, b"ftyp");
    o.extend_from_slice(b"isom");
    be32(&mut o, 0);
    o.extend_from_slice(b"isom");

    box_hdr(&mut o, moov_box, b"moov");
    box_hdr(&mut o, trak_box, b"trak");
    box_hdr(&mut o, tkhd_box, b"tkhd");
    o.extend_from_slice(&[0; 12]); // v/f + creation + modification
    be32(&mut o, 1); // track_ID

    box_hdr(&mut o, mdia_box, b"mdia");
    box_hdr(&mut o, mdhd_box, b"mdhd");
    o.extend_from_slice(&[0; 12]); // v/f + creation + modification
    be32(&mut o, timescale);
    be32(&mut o, 0); // duration

    box_hdr(&mut o, minf_box, b"minf");
    box_hdr(&mut o, stbl_box, b"stbl");
    box_hdr(&mut o, stsd_box, b"stsd");
    be32(&mut o, 0); // v/f
    be32(&mut o, 1); // entry_count

    box_hdr(&mut o, entry_box, if protected { b"encv" } else { b"avc1" });
    let mut entry = [0u8; 78];
    entry[24..26].copy_from_slice(&w.to_be_bytes());
    entry[26..28].copy_from_slice(&h.to_be_bytes());
    o.extend_from_slice(&entry);

    box_hdr(&mut o, avcc_box, b"avcC");
    o.extend_from_slice(&FIXTURE_AVCC);

    if protected {
        box_hdr(&mut o, sinf_box, b"sinf");
        box_hdr(&mut o, frma_box, b"frma");
        o.extend_from_slice(b"avc1"); // original format
        box_hdr(&mut o, schm_box, b"schm");
        be32(&mut o, 0); // v/f
        o.extend_from_slice(scheme); // scheme_type
        be32(&mut o, 0); // scheme_version
        box_hdr(&mut o, schi_box, b"schi");
        box_hdr(&mut o, tenc_box, b"tenc");
        be32(&mut o, 0); // v/f (version 0)
        o.push(0); // reserved
        o.push(0); // reserved (pattern in v1)
        o.push(1); // default_isProtected
        o.push(8); // default_Per_Sample_IV_Size
        o.extend_from_slice(kid); // default_KID
    }
    o
}

/// moof(mfhd traf(tfhd tfdt trun)) + mdat. The splitter only emits the INIT
/// unit once a moof arrives (an init segment alone is still accumulating), so
/// a CMAF pad needs one chunk before the barrier can see its track.
fn build_cmaf_chunk(track_id: u32, first_sample_flags: u32) -> Vec<u8> {
    // FullBox with a single payload word, as the CMAF fragment parser expects.
    let mut inner = Vec::new();
    box_hdr(&mut inner, 16, b"tfhd");
    be32(&mut inner, 0); // version + flags
    be32(&mut inner, track_id);
    box_hdr(&mut inner, 16, b"tfdt");
    be32(&mut inner, 0);
    be32(&mut inner, 0); // baseMediaDecodeTime
    box_hdr(&mut inner, 20, b"trun");
    be32(&mut inner, 0x0000_0400); // first-sample-flags
    be32(&mut inner, 1); // sample_count
    be32(&mut inner, first_sample_flags);

    let moof_body = 16 /* mfhd */ + 8 + inner.len();
    let mut o = Vec::new();
    box_hdr(&mut o, 8 + moof_body, b"moof");
    box_hdr(&mut o, 16, b"mfhd");
    be32(&mut o, 0);
    be32(&mut o, 1); // sequence_number
    box_hdr(&mut o, 8 + inner.len(), b"traf");
    o.extend_from_slice(&inner);
    box_hdr(&mut o, 9, b"mdat");
    o.push(0x00);
    o
}

const FIXTURE_KID: [u8; 16] = [
    0x10, 0x77, 0xef, 0xec, 0xc0, 0xb2, 0x4d, 0x02, 0xac, 0xe3, 0x3c, 0x1e, 0x52, 0xe2, 0xfb, 0x4b,
];
const FIXTURE_KID_UUID: &str = "1077efec-c0b2-4d02-ace3-3c1e52e2fb4b";
const WIDEVINE_UUID: &str = "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed";
const PLAYREADY_UUID: &str = "9a04f079-9840-4286-ab92-e65be0885f95";

/// Drive one CMAF request pad with an init segment and let the barrier commit.
fn push_cmaf_init(sink: &MoqSink, protected: bool, scheme: &[u8; 4]) {
    let init = build_cmaf_init(90000, 1920, 1080, protected, scheme, &FIXTURE_KID);

    // The fixture must be what libmoq thinks it is, or nothing below means
    // anything.
    let mut probe = moq_cmaf_init_info_t::default();
    unsafe {
        moq_cmaf_init_info_init(&mut probe);
        assert_eq!(moq_cmaf_parse_init(span(&init), &mut probe), MOQ_OK);
    }
    assert_eq!(probe.has_cenc, protected);
    if protected {
        assert_eq!(probe.default_kid.len, 16);
        assert_eq!(unsafe { bytes(probe.default_kid) }, FIXTURE_KID);
        assert_eq!(&unsafe { bytes(probe.scheme) }[..4], scheme);
    }

    assert!(sink.set_state(gst::State::Paused).is_ok());

    // A video_%u request pad defaults its track-name to "video".
    let peer = sink.request_pad_simple("video_%u").expect("request pad");

    let src = gst::Pad::builder(gst::PadDirection::Src).build();
    src.link(&peer).unwrap();
    src.set_active(true).unwrap();
    assert!(src.push_event(gst::event::StreamStart::new("cmaf-stream")));
    src.push_event(gst::event::Caps::new(&gst::Caps::new_empty_simple(
        "video/quicktime",
    )));
    assert!(peer.has_current_caps());
    let seg = gst::FormattedSegment::<gst::ClockTime>::new();
    assert!(src.push_event(gst::event::Segment::new(&seg)));

    let mut b = gst::Buffer::from_slice(init);
    b.get_mut().unwrap().set_pts(gst::ClockTime::ZERO);
    assert_eq!(src.push(b), OK);

    // One sync chunk: this is what makes the splitter release the init, which
    // runs prepare -> barrier -> attach -> add_track.
    let chunk = build_cmaf_chunk(1, 0 /* sync: non-sync bit clear */);
    let mut samples = [moq_cmaf_sample_t::default(); 8];
    let mut fi = moq_cmaf_fragment_info_t::default();
    unsafe {
        moq_cmaf_fragment_info_init(&mut fi, samples.as_mut_ptr(), samples.len());
        assert_eq!(moq_cmaf_parse_fragment(span(&chunk), &mut fi), MOQ_OK);
    }

    let mut cb = gst::Buffer::from_slice(chunk);
    cb.get_mut().unwrap().set_pts(gst::ClockTime::ZERO);
    assert_eq!(src.push(cb), OK);

    let _ = sink.set_state(gst::State::Null);
    let _ = src.set_active(false);
}

/// The contentProtections the element authored on the sender.
fn cps() -> Vec<moq5::ContentProtection> {
    control().attached_cps.clone()
}

/// The scheme and KID are never configured -- they come from the init
/// segment's own sinf/schm/schi(tenc). drm-systems supplies only what an fMP4
/// cannot.
#[test]
fn cp_authored_from_init() {
    let _suite = suite();
    let sink = new_sink();
    sink.set_property(
        "drm-systems",
        format!("{WIDEVINE_UUID},laurl=https://lic.example/wv,pssh=AAAAQ3Bzc2g="),
    );

    push_cmaf_init(&sink, true, b"cenc");

    let cps = cps();
    assert_eq!(cps.len(), 1);
    assert_eq!(cps[0].ref_id, "cp0");
    // Derived from the tenc default_KID: raw 16 bytes -> CMSF UUID string.
    assert_eq!(cps[0].default_kids, [FIXTURE_KID_UUID]);
    assert_eq!(cps[0].scheme, "cenc");
    assert_eq!(cps[0].drm_system.system_id, WIDEVINE_UUID);
    assert_eq!(
        cps[0].drm_system.la_url.as_ref().map(|u| u.url.as_str()),
        Some("https://lic.example/wv")
    );
    // The '=' padding of a base64 PSSH must survive the key=value split.
    assert_eq!(cps[0].drm_system.pssh.as_deref(), Some("AAAAQ3Bzc2g="));

    assert_eq!(track_refs("video").as_deref(), Some("cp0"));
}

/// CMSF carries one drmSystem per entry, so two systems over one key means two
/// entries sharing the KID, and the track references both.
#[test]
fn cp_two_systems_one_key() {
    let _suite = suite();
    let sink = new_sink();
    sink.set_property(
        "drm-systems",
        format!(
            "{WIDEVINE_UUID},laurl=https://lic.example/wv;{PLAYREADY_UUID},laurl=https://lic.example/pr"
        ),
    );

    push_cmaf_init(&sink, true, b"cbcs");

    let cps = cps();
    assert_eq!(cps.len(), 2);
    assert_eq!(cps[0].ref_id, "cp0");
    assert_eq!(cps[1].ref_id, "cp1");
    assert_eq!(cps[0].drm_system.system_id, WIDEVINE_UUID);
    assert_eq!(cps[1].drm_system.system_id, PLAYREADY_UUID);
    // One key, carried by both, and the scheme comes from schm, not config.
    assert_eq!(cps[0].default_kids, [FIXTURE_KID_UUID]);
    assert_eq!(cps[1].default_kids, [FIXTURE_KID_UUID]);
    assert_eq!(cps[0].scheme, "cbcs");
    assert_eq!(cps[1].scheme, "cbcs");

    assert_eq!(track_refs("video").as_deref(), Some("cp0,cp1"));
}

/// An encrypted track with no DRM system configured: libmoq would reject a
/// contentProtection with no systemID, so the catalog carries none and the
/// element warns. Pins the documented fallback rather than a crash or a bogus
/// entry.
#[test]
fn cp_encrypted_without_systems() {
    let _suite = suite();
    let sink = new_sink();

    push_cmaf_init(&sink, true, b"cenc");

    assert!(cps().is_empty());
    assert_eq!(track_refs("video").as_deref(), Some(""));
}

/// A clear track must never be described as protected, even with drm-systems
/// set: declaring refIDs on an unencrypted track would make libmoq reject the
/// add, and would lie to subscribers.
#[test]
fn cp_clear_track_declares_nothing() {
    let _suite = suite();
    let sink = new_sink();
    sink.set_property("drm-systems", WIDEVINE_UUID);

    push_cmaf_init(&sink, false, b"cenc");

    assert!(cps().is_empty());
    assert_eq!(track_refs("video").as_deref(), Some(""));
}

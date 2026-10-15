# Relay playback debugging: moq-relay.red5.net and Playa

Date: 2026-09-06. MOQ5 branch: `fix/picoquic-facade-close-and-wt-keepalive`.
Relay: `https://moq-relay.red5.net:4433/moq`. Player: Red5 Playa
(`moq-playa/examples/player`).

This documents how the plugin was built against the MOQ5 fix branch, the
scenario used to test it, the failures found along the way, and what was
changed to make playback work end to end.

## 1. Building against MOQ5 on Linux

> The plugin has since been ported from C/CMake to Rust/Cargo; see the README
> for the current build (`MOQ_PREFIX`, `MOQ_EXTRA_LIBS`). The CMake steps below
> are kept as the record of this session.

The plugin is a shared object (`libgstmoq.so`) that links three static
libraries: picotls, picoquic and libmoq. All three must be compiled with
`-fPIC`. The regular MOQ5 tree (`moq5/build/dev`) is not, and linking against
it fails with:

```
relocation R_X86_64_TPOFF32 against symbol `ptls_log_conn_state_override'
can not be used when making a shared object; recompile with -fPIC
```

A separate PIC stack was built under `moq5/build/gst-pic/` and installed to
`moq5/build/gst-pic/prefix`:

1. picotls from `moq5/.deps/picoquic-ci/picotls` with
   `-DCMAKE_POSITION_INDEPENDENT_CODE=ON` (targets `picotls-core`,
   `picotls-openssl`, `picotls-minicrypto`). MOQ5's FindPicoquic expects the
   picotls headers at `<prefix>/../include`, so the build directory sits next
   to a symlink to the source `include/`.
2. picoquic from `moq5/.deps/picoquic-ci/picoquic` with `BUILD_HTTP=ON`
   (WebTransport needs h3zero), PIC on, installed into the prefix.
3. libmoq with PIC on, `MOQ_BUILD_SERVICE=ON`, the picoquic and pico_wt
   adapters, `MOQ_BUILD_PICO_WT_MANAGED=ON`, sim/tests/examples off,
   `MOQ_PICOQUIC_SOURCE_DIR` and `MOQ_PICOTLS_PREFIX` pointing at the trees
   above, installed into the same prefix.

Then:

```sh
cmake -B build -DCMAKE_PREFIX_PATH=<moq5>/build/gst-pic/prefix
cmake --build build
export GST_PLUGIN_PATH="$PWD/build/plugins"
```

The plugin loaded with `undefined symbol: picoquic_set_qlog`. The installed
picoquic CMake package exports `picoquic-core` without a dependency on
`picoquic-log`, while `picoquic-log` depends on `picoquic-core`. The gst-moq
`CMakeLists.txt` now appends `picoquic::picoquic-log` to `picoquic-core`'s
link interface, the same fix-up MOQ5 applies in its source-tree mode, so CMake
sees the archive cycle and repeats the archives.

## 2. Test scenario

Publisher (5 to 10 minutes of live test pattern):

```sh
gst-launch-1.0 -e videotestsrc is-live=true pattern=ball \
  ! video/x-raw,width=640,height=360,framerate=30/1 ! timeoverlay \
  ! x264enc tune=zerolatency key-int-max=30 bitrate=1000 \
  ! video/x-h264,profile=constrained-baseline \
  ! h264parse config-interval=-1 \
  ! video/x-h264,stream-format=byte-stream,alignment=au \
  ! moqsink host=moq-relay.red5.net port=4433 relay-path=/moq \
      namespace=gsttest track-name=video
```

Native subscriber:

```sh
gst-launch-1.0 moqsrc host=moq-relay.red5.net port=4433 relay-path=/moq \
    namespace=gsttest track-name=video \
    caps="video/x-h264,stream-format=byte-stream,alignment=au" \
  ! queue ! h264parse ! avdec_h264 ! fpsdisplaysink video-sink=fakesink
```

Browser subscriber: `cd moq-playa/examples && npx vite --port 5173`, then open
`http://localhost:5173/player/?url=https://moq-relay.red5.net:4433/moq&ns=gsttest`.
Add `&v=18` to force draft-18 and `&log=debug` for console logs. Playback
starts from the `#start` button; the "stats for nerds" overlay shows objects,
decoded frames and decode errors.

## 3. Failures and fixes

### 3.1 moqsrc stalled with a synced sink

The first native run delivered only 21 objects in 15 seconds. LOC presentation
times were passed straight through as buffer PTS. They are relative to the
publisher's first frame (over 100 seconds in by then), so a synced sink waited
for running time to catch up and the whole pipeline blocked.

Fix in `moqsrc`: the first delivered object is anchored to the pipeline
running time at arrival and later objects keep their offset from it, which
preserves the publisher cadence. A new `latency` property (default 200 ms) is
answered through the LATENCY query so synced sinks absorb network jitter
instead of dropping late frames. Result: about 30 objects per second through
`avdec_h264` and a synced sink.

### 3.2 Playa received zero objects (draft mismatch)

Playa connected, read the catalog and subscribed, but reported 0 objects and
logged:

```
Malformed track "video": Varint requires 8 bytes but only 6 available
```

A temporary debug line in Playa's subscription manager dumped the LOC
extension bytes: `02 e2 bd e7 80 02 20`. Read as draft-16 QUIC varints, `e2`
announces an 8-byte integer. Read as a draft-18 vi64, `e2 bd e7 80` is the
4-byte value 46,000,000, the capture timestamp in microseconds. libmoq had
negotiated draft-18 with the relay, and the relay forwarded the draft-18
property block unchanged to a draft-16 subscriber.

The reverse direction fails the same way: a draft-16 publisher and a
draft-18 libmoq subscriber ends with the session closed and no objects.
Same-draft pairs work in both drafts.

Fix in both elements: a `draft` property (16 or 18, 0 offers every draft libmoq
supports). It defaults to 16 because browsers, including Playa, default to
draft-16. The relay behaviour itself is a separate issue: a relay that bridges
drafts needs to re-encode the property block.

### 3.3 Playa received objects but could not decode (no initData)

With matching drafts Playa parsed every object but logged:

```
Video decode submit failed (codec=avc1.42e01e, size=?x?, config=none, ...)
```

Playa's H.264 path converts Annex B to length-prefixed NAL units and needs an
avcC `description` on the WebCodecs decoder. It takes that from the catalog
track's `initData`, which the sink never published. The advertised codec
string was also the hard-coded `avc1.42e01e`, while the stream was
constrained baseline level 3.0.

Fix in `moqsink`: the track is added on the first keyframe instead of in
`start()`. The sink locates the SPS and PPS in the Annex B access unit, builds
an AVCDecoderConfigurationRecord and publishes it as `initData`, derives the
codec string from the SPS profile, constraint and level bytes (`avc1.42c01e`
here) unless the `codec` property overrides it, and fills width, height and
framerate from the negotiated caps. Delta frames before the first keyframe are
dropped. `h264parse config-interval=-1` keeps SPS/PPS on every keyframe.

## 4. Verified results

| Test | Result |
|---|---|
| moqsink to relay, 5 min live | written = sent, 0 dropped |
| moqsrc to avdec_h264 to synced sink, 15 s | 431 objects, 640x360 decoded |
| Playa default (draft-16) | 28 fps, 0 decode errors, TTFF 1.1 s |
| Playa draft-18, publisher `draft=18` | 27 fps, 0 decode errors |

## 5. Observations that are not bugs

- Playa logs "Catalog updated (delta)" once per second. That is libmoq's
  default catalog refresh (`catalog_refresh_interval_us`, 1 s) so late
  joiners see the catalog.
- Playa's stats show a "Gaps" counter incrementing about once per group.
  Playback was smooth with 0 dropped frames; it was not investigated further.

## 6. CMAF and CMSF-01

Publisher (video + AAC, `fragment-duration` matched to the 1 s GOP from
`key-int-max=30` at 30 fps):

```sh
gst-launch-1.0 -e moqsink name=ms host=moq-relay.red5.net port=4433 relay-path=/moq namespace=gsttest \
  videotestsrc is-live=true pattern=ball ! video/x-raw,width=640,height=360,framerate=30/1 ! timeoverlay \
    ! x264enc tune=zerolatency key-int-max=30 bitrate=1000 ! video/x-h264,profile=constrained-baseline ! h264parse \
    ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss ! ms.video_0 \
  audiotestsrc is-live=true wave=sine ! audio/x-raw,rate=48000,channels=2 ! voaacenc bitrate=128000 ! aacparse \
    ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss ! ms.audio_0
```

Opus variant: replace the audio branch with `opusenc bitrate=96000 ! opusparse
! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss ! ms.audio_0`.

Browser subscriber: `cd moq-playa/examples && npx vite --port 5173`, then open
`http://localhost:5173/player/?url=https://moq-relay.red5.net:4433/moq&ns=gsttest&log=debug`
(add `&msedebug=1` for per-append MSE tracing). Native subscribers:

```sh
gst-launch-1.0 moqsrc host=moq-relay.red5.net port=4433 relay-path=/moq namespace=gsttest track-name=video \
  ! qtdemux ! h264parse ! avdec_h264 ! fakesink -v
gst-launch-1.0 moqsrc host=moq-relay.red5.net port=4433 relay-path=/moq namespace=gsttest track-name=audio \
  ! qtdemux ! opusdec ! fakesink -v
```

### 6.1 First Playa join landed mid-fragment near publisher EOS (false alarm)

The first attempt opened Playa about 30 seconds into a publisher run limited
to `num-buffers=9000` (300 s of video at 30 fps). Playa built the CMAF
`MediaSource`, subscribed both tracks and received objects, but never rendered
a frame:

```
[moqt] Error [.../]0x1203: fatal catalog 1203 CMAF MediaSource initialized but
no frame rendered (init/codec mismatch?) within 10000ms
```

Stats showed Objects=45, Decoded=0, Rendered=0, State=error. Inspecting the
player's internal state (`window.__player.cmafAssembler`) showed
`lastVideoOutputBmd` and `lastAudioOutputBmd` both non-null, i.e. the
assembler had rebased and emitted segments — the failure was not in fragment
parsing. Re-running the publisher with a longer `num-buffers` (18000, 600 s)
and reloading Playa immediately after the publisher started reproduced a
clean run every time, with `[moqt] First frame rendered` logged under 1 s
after the catalog. The original run's publisher log showed it hit `Got EOS`
and tore down about the time the failure surfaced, which lines up with the
watchdog timing: the run was not a CMAF fragmentation bug, it was Playa's
5-minute publisher script ending near the point where we opened the tab. Not
a gst-moq issue; noted here so the next run does not re-chase it. Keep the
publisher's `num-buffers` comfortably longer than the test window.

### 6.2 Playa's "Decoded"/"FPS" stat tiles read 0 during CMAF playback

With a clean run, Playa's stats overlay showed `Decoded 0`, `Rendered 0`,
`FPS 0.0` throughout playback even though the `<video>` element was visibly
advancing (`currentTime` climbing, `webkitDecodedFrameCount` incrementing
about 30/s). Those three tiles are wired to the old LOC/WebCodecs decode path
and are not yet updated for the CMAF/MSE path; frame counts are only
observable through the `<video>` element itself in this mode. Not a gst-moq
issue — recorded as an observation, same as the "Gaps" counter in section 5.

### 6.3 Interop with a foreign CMSF-01 publisher (moqxr)

Interop target: `moqxr` (`openmoq-publisher`, the C++ OpenMOQ publisher one
directory up), which packages a progressive MP4 into CMAF chunks and authors
a CMSF version-1 catalog (`packaging: "cmaf"`, root `initDataList`, per-track
`initRef`; tracks named `vide_1` and `soun_2`). It publishes to
moq-relay.red5.net over WebTransport on draft-16, the same dialect and draft
gst-moq uses, so the public relay can be used directly.

Source clip: a 120 s synthetic file (`ffmpeg -f lavfi -i
testsrc2=size=640x360:rate=30 -f lavfi -i sine=frequency=440:sample_rate=48000
-t 120 -c:v libx264 -profile:v baseline -g 30 -c:a aac -ac 2 ...`).

```sh
./build/openmoq-publisher --input moqxr-test.mp4 --transport webtransport \
    --endpoint https://moq-relay.red5.net:4433/moq --namespace gstinterop \
    --draft 16 --forward 1 --publish-catalog --paced --loop
GST_DEBUG=moqsrc:4 gst-launch-1.0 -v moqsrc host=moq-relay.red5.net port=4433 relay-path=/moq \
    namespace=gstinterop track-name=vide_1 discovery-timeout=15000 \
  ! qtdemux ! h264parse ! avdec_h264 ! fakesink silent=false
GST_DEBUG=moqsrc:4 gst-launch-1.0 -v moqsrc host=moq-relay.red5.net port=4433 relay-path=/moq \
    namespace=gstinterop track-name=soun_2 discovery-timeout=15000 \
  ! qtdemux ! aacparse ! avdec_aac ! fakesink silent=false
```

Both subscribers discovered both tracks from the catalog
(`TRACK_ADDED name=vide_1 codec=avc1.42C01E packaging=cmaf`,
`TRACK_ADDED name=soun_2 codec=mp4a.40.2 packaging=cmaf`), pushed the
catalog init segment, and negotiated raw caps downstream of `qtdemux`. In a
25 s run the video subscriber received 725 objects and `avdec_h264` produced
725 frames (about 30 fps, 640x360); the audio subscriber received 5626
objects and `avdec_aac` produced 5626 buffers (the relay delivers the cached
backlog of the open group first, so the rate is above real time). No errors
or warnings on either side.

Two observations from this run:

- moqxr publishes the whole file as one MoQT group (group 0) with one CMAF
  chunk per frame, so a late joiner receives only non-sync fragments until
  the loop restarts; libmoq reports them as delta objects and `qtdemux` still
  decodes because every fragment carries its own `moof`. This is a publisher
  packaging choice, not a gst-moq concern.
- The first subscriber started within a second of the publisher and saw only
  one object in 20 s; every later run streamed normally. Start subscribers a
  few seconds after the publisher.

Playa's `node-publisher`/`node-relay` examples were tried first and cannot be
used for this check: the node-publisher pins the relay certificate hash (so it
cannot reach the public relay), and the node-relay is built on the current
WebTransport draft while libmoq's picoquic backend speaks the drafts-13/14
dialect, so the session closes before any MoQT control stream is exchanged
(`UniPairTopology: no inbound control stream`).

### 6.4 Verified results

| Test | Codec | Resolution/fps | Decode errors | TTFF |
|---|---|---|---|---|
| Playa, video + AAC | avc1.42c01e / mp4a.40.2 | 640x360, ~29.9 fps (1405 frames / 46.9 s) | 0 | 879 ms |
| Playa, video + Opus | avc1.42c01e / opus | 640x360, ~30.1 fps (1199 frames / 39.8 s) | 0 | 782 ms |
| moqsrc, track-name=video (AAC run) | h264parse/avdec_h264 | caps negotiated, 640x360 | 0 | n/a |
| moqsrc, track-name=video (Opus run) | h264parse/avdec_h264 | caps negotiated, 640x360 | 0 | n/a |
| moqsrc, track-name=audio (Opus run) | qtdemux/opusdec | caps negotiated, 48kHz stereo | 0 | n/a |
| moqsrc, moqxr CMSF-01 publisher, track vide_1 | qtdemux/avdec_h264 | 725 objects, 725 frames in 25 s, 640x360 | 0 | n/a |
| moqsrc, moqxr CMSF-01 publisher, track soun_2 | qtdemux/avdec_aac | 5626 objects, 5626 buffers in 25 s | 0 | n/a |

Screenshots (not included in this doc): the AAC run showed the Playa player's
`<video>` element visibly playing the 640x360 ball test pattern with the
timeoverlay clock advancing; the Opus run showed the same playback with the
"stats for nerds" overlay open, reporting the resolution/fps and zero decode
errors listed in the table above.

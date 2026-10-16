# gst-moq

GStreamer elements for [Media over QUIC Transport](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/),
backed by [MOQ5](https://github.com/openmoq/moq5) through its high-level media
service tier.

- **`moqsink`** — publish a GStreamer stream to a MoQ relay.
- **`moqsrc`** — subscribe to a MoQ track and inject it into a pipeline.

## Design

- Thin adapters over the MOQ5 **service tier** (`moq_endpoint_t` +
  `moq_media_sender_t` / `moq_media_receiver_t`). MOQ5 owns the network thread,
  version negotiation, TLS, and the MSF catalog; the elements only translate
  GstBuffers ⇄ MoQ media objects.
- WebTransport client to a relay (the service tier is client-only — there is no
  peer-to-peer mode).
- H.264 byte-stream carried as LOC-01 media objects: a keyframe opens a MoQ
  group, delta frames extend it, timestamps are stream-relative.

## Build

The plugin is written in Rust on [gstreamer-rs](https://gitlab.freedesktop.org/gstreamer/gstreamer-rs).
It requires Rust 1.92 or newer, the GStreamer 1.x development packages
(`gstreamer-1.0`, `gstreamer-base-1.0`, 1.22 or newer), libclang (the
`moq5-sys` crate generates its libmoq bindings with bindgen), and an installed
MOQ5 with the service component (`libmoq-service`).

The plugin talks to libmoq through the `moq5` crate, a safe Rust API over
libmoq's service tier, which sits on the raw `moq5-sys` bindings. Both live
in the MOQ5 repository under `bindings/rust`; `Cargo.toml` pins the commit.
That commit binds the libmoq API of upstream MOQ5 `main` (draft 22), so the
installed MOQ5 must be built from a matching checkout.

`moq5-sys` links the static libmoq archives from `$MOQ_PREFIX/lib` and reads
the headers from `$MOQ_PREFIX/include`. `.cargo/config.toml` sets
`MOQ_PREFIX` to `../local-prefix` next to this repository -- where MOQ5's
`install` preset puts it -- unless the environment already sets it. On Linux
the plugin is a shared object, so every static library it links must be
built position-independent:

```sh
cd ../moq5
cmake --preset install -DCMAKE_POSITION_INDEPENDENT_CODE=ON
cmake --build build/install && cmake --install build/install
```

If your libmoq build references further static libraries (picoquic, picotls,
OpenSSL), name them in `MOQ_EXTRA_LIBS` (space-separated
`cargo:rustc-link-lib` values, e.g.
`"static=picoquic-core static=picotls-core ssl crypto"`); they are searched in
the same `lib` directory.

```sh
cargo build --release                 # MOQ_PREFIX=<moq5-prefix> for another install
export GST_PLUGIN_PATH="$PWD/target/release"
gst-inspect-1.0 moqsink
gst-inspect-1.0 moqsrc
```

## Test

```sh
cargo test
```

runs, with no relay and no sockets (the `moq5` crate's own tests run in the
MOQ5 repository):

- unit tests for the fMP4 splitter and the codec strings;
- the element suites (`moqsink` track barrier and content protection,
  `moqsink` EOS/flush recovery, `moqsrc` delivery), which drive the real
  element code with the libmoq service calls replaced by mocks (installed
  through `moq5`'s `testing` feature);
- `tests/smoke.sh` against a freshly built plugin (inspection, a CMAF publish
  pipeline against a refused relay, a `moqsink` state cycle), plus its
  socket-free guard tests in `tests/test_smoke_guards.py`.

`cargo run --bin inspect_cmaf_fixture -- file.mp4` prints the init/fragment
inventory of a fragmented MP4 as the plugin splits and libmoq parses it.

## Usage

Publish a stream to a relay:

```sh
gst-launch-1.0 -e \
  videotestsrc ! x264enc tune=zerolatency key-int-max=30 ! h264parse \
  ! video/x-h264,stream-format=byte-stream,alignment=au \
  ! moqsink host=RELAY port=4433 relay-path=/moq-relay namespace=example track-name=video
```

Subscribe, decode, and display:

```sh
gst-launch-1.0 \
  moqsrc host=RELAY port=4433 relay-path=/moq-relay namespace=example track-name=video \
    caps="video/x-h264,stream-format=byte-stream,alignment=au" \
  ! h264parse ! avdec_h264 ! autovideosink
```

Use `h264parse config-interval=-1` so every keyframe carries SPS/PPS: `moqsink`
adds the track on the first keyframe and publishes the SPS/PPS as an avcC
`initData` in the catalog (WebCodecs players need the decoder description),
together with a `codec` string derived from the SPS profile/level and the
width, height and framerate from the caps.

Common properties: `host`, `port`, `relay-path`, `namespace`, `track-name`,
`insecure` (skip TLS verification — test only), `draft` (MoQ Transport draft to
negotiate, default 16; 0 offers every draft libmoq supports). `moqsink` adds
`codec` (override the derived catalog codec string), `bitrate`, `sync`,
`sap-timeline` and `max-fragment-size`; `moqsrc` adds `discovery-timeout`,
`latency` (ms reported through the LATENCY query so synced sinks absorb
network jitter instead of dropping late frames; default 200) and `caps`,
which is optional for CMAF, AAC-LC LOC and H.264 LOC — it is derived from the catalog
when omitted. Automatic H.264 LOC caps describe Annex B access units;
length-prefixed payloads and catalog-only decoder configuration are not yet
handled automatically; for length-prefixed AVC, supply explicit `video/x-h264,
stream-format=avc,alignment=au` caps including the matching `codec_data`.
Other LOC codecs require explicit compatible caps.

`moqsink` is a plain `GstElement`, not a `GstBaseSink`: it does not preroll,
so `READY` to `PAUSED` returns immediately instead of waiting for the first
buffer, and `PAUSED` does not pause publishing — buffers already queued keep
going out to the relay until the pipeline reaches `NULL` or the source stops
delivering them. Only `sync` is exposed; the other `GstBaseSink` properties
(`async`, `qos`, `max-lateness`, `ts-offset`, `blocksize`,
`enable-last-sample`, `render-delay`) do not exist on this element.

`moqsrc` maps LOC presentation times onto the pipeline clock: the first object
is anchored at the running time of its arrival and later objects keep their
offset from it, so synced sinks play at the publisher's cadence.

Keep `draft` the same on the publisher and on every subscriber. Relays that
bridge draft-16 and draft-18 sessions have been seen to forward the LOC
property block verbatim, and the two drafts encode its integers differently
(QUIC varint vs vi64), so a cross-draft subscriber rejects the objects.

### LOC with audio

The always pads are `sink` (H.264 byte-stream) and `audio` (raw AAC access
units). Each publishes a LOC-01 track into the same catalog, so a LOC broadcast
can carry video and audio:

```sh
gst-launch-1.0 -e moqsink name=ms host=RELAY port=4433 relay-path=/moq namespace=example \
  videotestsrc is-live=true ! x264enc tune=zerolatency key-int-max=30 ! h264parse config-interval=-1 \
    ! video/x-h264,stream-format=byte-stream,alignment=au ! ms.sink \
  audiotestsrc is-live=true ! voaacenc ! aacparse \
    ! audio/mpeg,mpegversion=4,stream-format=raw ! ms.audio
```

The audio pad wants **raw** AAC, not ADTS: the decoder config travels in the
catalog's `initData`, taken from the caps' `codec_data`, exactly as the video
pad publishes avcC there. `rate` and `channels` become the catalog's
`samplerate` and `channelConfig`, which MSF-01 §5.2.28/§5.2.29 make mandatory
for an audio track — the element reads them from the caps and falls back to the
AudioSpecificConfig. Every AAC frame is a sync point, so objects are never
dropped waiting for a keyframe; groups are cut on a one-second budget rather
than one per frame, matching the video GOP cadence. The element's `track-name`
and `bitrate` properties apply to the video pad; the audio pad carries its own
(`ms.audio::track-name=...`), defaulting to `audio`.

Play it back with `moqsrc`, which derives AAC caps from the catalog:

```sh
gst-launch-1.0 moqsrc host=RELAY port=4433 relay-path=/moq namespace=example track-name=audio \
  ! aacparse ! avdec_aac ! autoaudiosink
```

### CMAF (fragmented MP4) with audio

`moqsink` also takes fragmented MP4 on request pads `video_%u` and `audio_%u`
and publishes each as a CMAF track under one CMSF-01 catalog (`packaging:
"cmaf"`, root `initDataList`, per-track `initRef`). Feed each pad from its own
fragmented muxer; one moof+mdat fragment becomes one MoQ object, and a
fragment starting on a sync sample opens a new group, so keep
`fragment-duration` a multiple of the GOP length.

```sh
gst-launch-1.0 -e moqsink name=ms host=RELAY port=4433 relay-path=/moq namespace=example \
  videotestsrc is-live=true ! x264enc tune=zerolatency key-int-max=30 ! h264parse \
    ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss ! ms.video_0 \
  audiotestsrc is-live=true ! voaacenc ! aacparse \
    ! mp4mux fragment-duration=1000 fragment-mode=dash-or-mss ! ms.audio_0
```

Opus works the same way (`opusenc ! opusparse ! mp4mux ...`). Pad properties
`track-name` and `bitrate` set the catalog entry per pad (`ms.video_0::track-name=hd`).
Element property `sap-timeline` adds a CMSF SAP event timeline track per CMAF track.

Play a CMAF track back with `qtdemux`; `caps` is derived from the catalog:

```sh
gst-launch-1.0 moqsrc host=RELAY port=4433 relay-path=/moq namespace=example track-name=video \
  ! qtdemux ! h264parse ! avdec_h264 ! autovideosink
gst-launch-1.0 moqsrc host=RELAY port=4433 relay-path=/moq namespace=example track-name=audio \
  ! qtdemux ! aacparse ! avdec_aac ! autoaudiosink
```

## Status

The plugin supports H.264 Annex B LOC publishing, LOC AAC audio on a second
always pad, and CMAF video/audio under a CMSF-01 catalog. CMAF codec strings
cover AVC, AAC and Opus; automatic LOC receiver caps cover H.264 and AAC.
Finite draft-16 LOC plus CMAF/AAC delivery, independent track EOS and publisher
drain passed through the Chicago and London moqx relays with the exact private
SDK described below. Separately captured CMAF AVC/AAC from Playa decoded
offline. LOC video-plus-audio publishing, and `moqsrc` decoding the audio track
back, were validated against `moq-relay.red5.net` on draft-16 only. These
results do not qualify arbitrary LOC framing, Opus playback, draft-18 finite
EOS or every relay implementation. A raw-QUIC (`moqt://`) transport option is a
follow-up.

## Author

Raymond Lucke and the Red5 Team

## License

Apache-2.0. See [LICENSE](LICENSE).

Publisher EOS uses one three-second acceptance/drain budget and requires MOQ5's
service-aware endpoint drain. Failure or cancellation posts an error rather than
successful EOS; local flush does not prove peer receipt or decode. Released pads
retain their native end obligations until sender teardown. FLUSH_STOP does not
reopen an ended track. Returning through READY creates a fresh sender lifetime.

The local fixture in `tests/local_fixture.py` generates independent LOC access
units and CMAF init/fragments, pins their SHA256 inventory, and validates output
through an offline appsrc/appsink replay. Its `plan` command constructs one
publisher and both receivers at NULL without connecting. `publish`/`receive`
require explicit traffic authorization and accept `--draft 16` or `--draft 18`,
with draft16 as the default,
TLS verification and a fresh per-run readiness directory. Priming media is part
of the inventory; the counted suffix waits for real receiver priming receipts.
Public TRACK_ENDED evidence distinguishes service end from local timeout/close,
but the native event also covers rejection and is not wire-marker provenance.

The EOS contract requires MOQ5's service-aware endpoint drain. The tested SDK is
a private delivery based on f9e20ebf with additional native changes, not a
released MOQ5 package; older transport-only drain implementations are insufficient.
Consumers must supply a MOQ5 SDK with these service-drain guarantees; a public
minimum revision has not yet been established. Relay catalog bootstrap support
is a separate requirement for playback against a given relay.


LOC audio publishing accepts AAC-LC with a supported two-byte
AudioSpecificConfig, an indexed sample rate and an explicit channel configuration.
The config must agree with any rate/channels in caps. HE-AAC, explicit-frequency
core dependencies, extension/trailing config and program-config-element forms are rejected rather than guessed. This
restriction applies to LOC publishing, not the existing CMAF codec-string support.

Publishing connects after every linked pad has prepared its first track. Put
queues before pads fed by a common upstream streaming task. `catalog-wait-ms`
(default 3000) bounds that wait: an incomplete track set reports an error and
opens no connection. Initial attach/add runs in one MOQ5 endpoint task before
the first sender pump, so it cannot publish a partial initial catalog. Link all
publishing pads before the first buffer; restart through READY to change the
initial stream set. Empty streams finish without opening a connection.
`catalog-refresh-ms` defaults to zero; opt into refresh only for relay cache
compatibility. Capture timestamps use each pad's segment running time so encoder
PTS offsets do not shift audio against video. Audio group boundaries follow
observed video keyframe boundaries, with `loc-audio-group-ms` as an upper bound;
this does not guarantee simultaneous delivery by the relay.

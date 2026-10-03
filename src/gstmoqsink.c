/*
 * moqsink — publish GStreamer streams over Media-over-QUIC using the
 * libmoq service tier (moq_endpoint_t + moq_media_sender_t).
 *
 * One element owns one endpoint and one media sender, so every pad's track
 * lands in a single MSF/CMSF catalog. Pads:
 *
 *   sink       (always)  H.264 Annex B access units -> LOC objects. The
 *                        track is added on the first keyframe so the catalog
 *                        carries an avcC initData and an SPS-derived codec.
 *   video_%u   (request) fragmented MP4 -> CMAF objects (one moof+mdat each);
 *   audio_%u   (request) ftyp+moov becomes the catalog init segment.
 *
 * The service tier owns the network thread, version negotiation, the sticky
 * catalog and the bounded per-track send queues; chain functions only
 * translate buffers into media objects. moq_media_sender_write() takes the
 * payload rcbuf on MOQ_OK and its release callback (on the network thread)
 * drops our GstBuffer/GBytes reference.
 *
 * Sync: with sync=true (default) each chain waits on the pipeline clock for
 * the buffer's running time plus the configured latency, so non-live
 * sources are paced like a normal sink. Live sources already arrive at
 * rate. GST_EVENT_LATENCY is flagged GST_EVENT_TYPE_UPSTREAM (see
 * gstevent.h), so gst_pad_send_event() on one of our (sink-direction) pads
 * refuses it outright ("sending latency event in wrong direction") without
 * ever calling the pad's event function — the same reason GstBaseSink
 * intercepts GST_EVENT_LATENCY in its own send_event override instead of
 * routing it through a pad. element_class->send_event does the same here:
 * it stores the latency directly, and only forwards other (non-latency)
 * events onto the sink pads via gst_pad_send_event.
 *
 * GstElement, not GstBaseSink: this is a plain GstElement, so it never
 * preroll waits on the first buffer -- GST_STATE_CHANGE_READY_TO_PAUSED
 * returns immediately -- and reaching PAUSED does not pause publishing;
 * only GstBaseSink's "sync" property has an equivalent here ("async",
 * "qos", "max-lateness", "ts-offset", "blocksize", "enable-last-sample"
 * and "render-delay" do not apply).
 *
 * Thread safety: libmoq's media sender expects every call
 * (add_track/write/end_track/get_stats/is_fatal/fatal_code) to come from a
 * single application thread. With request pads several GStreamer streaming
 * threads share one sender, so all sender calls are serialized by
 * self->send_lock; it is never held while waiting on the pipeline clock.
 * gst_moq_sink_stop() destroys the sender and clears self->sender to NULL
 * inside that same lock (one critical section together with the final
 * get_stats), and every streaming-thread caller re-checks self->sender
 * under the lock before touching it, so a late call can never dereference a
 * freed sender.
 */
#include "gstmoqsink.h"
#include "gstmoqeos.h"
#include "gstmoqsinkpad.h"
#include "gstmoqcodec.h"

#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

#include <string.h>

GST_DEBUG_CATEGORY_STATIC (gst_moq_sink_debug);
#define GST_CAT_DEFAULT gst_moq_sink_debug

#define DEFAULT_HOST       "localhost"
#define DEFAULT_PORT       4443
#define DEFAULT_RELAY_PATH "/moq-relay"
#define DEFAULT_NAMESPACE  "example"
#define DEFAULT_TRACK_NAME "video"
#define DEFAULT_DRAFT      16            /* browsers (playa) default to draft-16; 0 = offer all */
#define DEFAULT_BITRATE    2000000       /* catalog max bitrate (MSF-01 §5.2.22) */
#define DEFAULT_AUDIO_BITRATE 128000
#define DEFAULT_AUDIO_TRACK_NAME "audio"
/* LOC audio group cadence. Every AAC frame is independently decodable, so any
 * object could open a group; one group per ~21 ms frame would give a relay ~47
 * groups/s per track. A group per second matches both the video path's usual
 * GOP cadence and what the CMAF path produces (one fragment, one group). */
#define LOC_AUDIO_GROUP_US 1000000
#define DEFAULT_MAX_FRAGMENT (16u << 20)

typedef struct {
  moq_media_track_t *track;
  gint64 deadline;
  gboolean accepted;
} EndRequest;

/* How long the first pad to produce a track waits for the remaining LINKED pads
 * to produce theirs before the sender is attached anyway. It bounds the case
 * where a pad is linked but never delivers (a silent or absent elementary
 * stream that also never sends EOS); a pad that EOSes or flushes drops out of
 * the barrier immediately, so the normal cost is the gap between the pads'
 * first keyframes, not this timeout. */
#define DEFAULT_CATALOG_WAIT_MS 3000

enum
{
  PROP_0,
  PROP_HOST,
  PROP_PORT,
  PROP_RELAY_PATH,
  PROP_INSECURE,
  PROP_NAMESPACE,
  PROP_TRACK_NAME,
  PROP_CODEC,
  PROP_BITRATE,
  PROP_DRAFT,
  PROP_SYNC,
  PROP_SAP_TIMELINE,
  PROP_MAX_FRAGMENT_SIZE,
  PROP_PUBLISH_TRACKS,
  PROP_CATALOG_REFRESH_MS,
  PROP_CATALOG_WAIT_MS,
  PROP_LOC_AUDIO_GROUP_MS,
  PROP_DRM_SYSTEMS,
};

/* Immutable diagnostic snapshot, owned until sender callbacks are settled. */
typedef struct { gchar *namespace_str; } GstMoqReadyDiagnostic;

typedef struct {
  moq_media_track_cfg_t cfg;
  GstMoqSinkPad *pad;
  GBytes *name, *codec, *channels, *init;
  /* contentProtectionRefIDs: the spans are copied because the caller's array is
   * a temporary, and the strings they point at because the pad clears
   * pend_cp_ref_ids on reset (gst_moq_sink_pad_reset) while this snapshot may
   * still be queued. */
  gchar **cp_ref_strv;
  moq_bytes_t *cp_refs;
  moq_media_track_t *track;
} PreparedTrack;

typedef struct {
  GMutex lock;
  GCond cond;
  gboolean done, running, cancelled;
  moq_result_t result;
  moq_media_sender_cfg_t cfg;
  moq_media_sender_t *sender;
  GPtrArray *tracks;
} SenderStartup;

/* One authored CMSF 4.1.1 contentProtections entry: a (scheme, defaultKID, DRM
 * system) triple. CMSF carries ONE drmSystem per entry, so a stream offered
 * under several systems produces one entry per system, all sharing the KID, and
 * the track references them all. The strings own the bytes that back the
 * moq_bytes_t spans handed to libmoq. */
typedef struct
{
  gchar *ref_id;        /* refID, generated: "cp0", "cp1", ... */
  gchar *kid_uuid;      /* defaultKID as the CMSF "8-4-4-4-12" UUID string */
  gchar *scheme;        /* "cenc" or "cbcs", from the init segment's schm */
  gchar *system_id;     /* drmSystem.systemID, a UUID string */
  gchar *la_url;        /* NULL when absent */
  gchar *cert_url;
  gchar *auth_url;
  gchar *pssh;          /* base64, opaque to us and to libmoq */
  gchar *robustness;
} MoqCpEntry;

struct _GstMoqSink
{
  GstElement parent;

  /* properties */
  gchar   *host;
  guint    port;
  gchar   *relay_path;
  gboolean insecure;
  gchar   *namespace_str;
  gchar   *codec;          /* LOC only; NULL = derive from the SPS */
  guint    draft;          /* MoQT draft to negotiate, 0 = auto */
  gboolean sync;
  gboolean sap_timeline;
  guint64  max_fragment_size;
  gchar   *drm_systems;    /* DRM systems spec; see the property docs */

  /* runtime */
  moq_endpoint_t     *ep;
  moq_media_sender_t *sender;
  GstMoqReadyDiagnostic *ready_diagnostic;
  GMutex              send_lock;  /* serializes every moq_media_sender_* call */
  gboolean            started;
  gboolean            eos_posted;
  gboolean            eos_failed;
  gboolean            state_flushing;
  guint64             sender_generation;
  GList              *end_requests; /* sender-owned; survives pad release */
  GstClockTime        latency;    /* from the last GST_EVENT_LATENCY, under OBJECT_LOCK */
  gint                flush_count; /* concurrent FLUSH_START/STOP pads, under OBJECT_LOCK */

  gchar      **ns_tokens;
  moq_bytes_t *ns_bytes;
  guint        ns_count;

  /* Wall-clock anchor for the LOC Capture Timestamp (LOC-01 2.3.1.1 is epoch
   * microseconds, not stream-relative). Taken once, from whichever LOC pad sees
   * a timestamped buffer first, and shared by both: anchoring each pad
   * separately would offset audio from video by the gap between their first
   * buffers. Guarded by OBJECT_LOCK. */
  gboolean     publish_tracks;   /* push (PUBLISH) instead of announce-and-wait */
  guint        catalog_refresh_ms; /* 0 = never republish the catalog */
  guint        catalog_wait_ms;  /* how long to wait for every linked pad's track */
  guint        loc_audio_group_ms; /* LOC audio group length */

  /* Initial-catalog barrier. The sender is attached, and every track added, in
   * one step once all linked pads have prepared their track -- see
   * gst_moq_sink_await_tracks for why. Guarded by track_lock, which is taken
   * OUTSIDE both OBJECT_LOCK and send_lock. */
  GMutex       track_lock;
  GCond        track_cond;
  GPtrArray   *preparing_tracks; /* app-thread snapshot, under track_lock */
  gboolean     tracks_committed;
  gboolean     tracks_failed;

  /* CMSF 4.1.1 root contentProtections authored for this session, built at the
   * barrier (gst_moq_sink_build_content_protections) from every protected CMAF
   * pad's init segment crossed with drm-systems. Owned; the entries' strings
   * back the moq_bytes_t spans handed to libmoq, which deep-copies them at
   * attach and at add_track, so this only has to outlive commit_tracks -- it is
   * kept until stop so the values stay inspectable. Guarded by track_lock. */
  GArray      *cp_entries;        /* MoqCpEntry */

  /* Raw PTS of the most recent VIDEO group start, published so the audio pad
   * can open its group at the same instant (see gst_moq_sink_chain_loc).
   * Compared by identity only -- the two pads rebase presentation time
   * separately, so magnitudes are not comparable across them. Guarded by
   * OBJECT_LOCK. */
  GstClockTime loc_video_group_pts;

  gboolean     loc_anchored;
  gint64       loc_anchor_epoch_us;
  GstClockTime loc_anchor_pts;

  GstMoqSinkPad *locpad;        /* the always video (H.264) pad */
  GstMoqSinkPad *locpad_audio;  /* the always audio (AAC) pad */
  guint          pad_serial;
};

G_DEFINE_TYPE (GstMoqSink, gst_moq_sink, GST_TYPE_ELEMENT)

static GstStaticPadTemplate sink_template =
GST_STATIC_PAD_TEMPLATE ("sink", GST_PAD_SINK, GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/x-h264, "
        "stream-format = (string) { avc, byte-stream }, "
        "alignment = (string) au"));

/* The second always pad: AAC access units, one LOC object per frame. Raw AAC
 * (no ADTS) because LOC carries the decoder config out of band, in the
 * catalog's initData, exactly as the video pad carries avcC there. */
static GstStaticPadTemplate audio_loc_template =
GST_STATIC_PAD_TEMPLATE ("audio", GST_PAD_SINK, GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("audio/mpeg, "
        "mpegversion = (int) 4, "
        "stream-format = (string) raw"));

static GstStaticPadTemplate video_template =
GST_STATIC_PAD_TEMPLATE ("video_%u", GST_PAD_SINK, GST_PAD_REQUEST,
    GST_STATIC_CAPS ("video/quicktime"));

static GstStaticPadTemplate audio_template =
GST_STATIC_PAD_TEMPLATE ("audio_%u", GST_PAD_SINK, GST_PAD_REQUEST,
    GST_STATIC_CAPS ("video/quicktime"));

/* -- payload release ------------------------------------------------------ */

/* A mapped GstBuffer kept alive by the in-flight payload rcbuf. */
typedef struct
{
  GstBuffer *buffer;
  GstMapInfo map;
} SinkFrame;

static void
sink_frame_free (SinkFrame *f)
{
  if (!f)
    return;
  gst_buffer_unmap (f->buffer, &f->map);
  gst_buffer_unref (f->buffer);
  g_free (f);
}

/* rcbuf release callback: runs on the service network thread. */
static void
sink_frame_release (void *ctx, const uint8_t *data, size_t len)
{
  (void) data;
  (void) len;
  sink_frame_free (ctx);
}

/* Same for a GBytes payload (CMAF fragments). */
static void
sink_bytes_release (void *ctx, const uint8_t *data, size_t len)
{
  (void) data;
  (void) len;
  g_bytes_unref (ctx);
}

/* -- namespace (verbatim from the previous version) ---------------------- */

static gboolean
gst_moq_sink_build_namespace (GstMoqSink *self)
{
  g_clear_pointer (&self->ns_tokens, g_strfreev);
  g_clear_pointer (&self->ns_bytes, g_free);
  self->ns_count = 0;

  gchar **toks = g_strsplit (self->namespace_str, "/", -1);
  guint n = 0;
  for (gchar **p = toks; *p; p++) {
    if (**p == '\0') {
      GST_ERROR_OBJECT (self, "namespace has an empty part: '%s'",
          self->namespace_str);
      g_strfreev (toks);
      return FALSE;
    }
    n++;
  }
  if (n == 0) {
    GST_ERROR_OBJECT (self, "namespace is empty");
    g_strfreev (toks);
    return FALSE;
  }

  self->ns_tokens = toks;
  self->ns_bytes = g_new0 (moq_bytes_t, n);
  for (guint i = 0; i < n; i++) {
    self->ns_bytes[i].data = (const uint8_t *) toks[i];
    self->ns_bytes[i].len = strlen (toks[i]);
  }
  self->ns_count = n;
  return TRUE;
}

/* -- H.264 parameter sets (verbatim from the previous version) ----------- */

static gboolean
find_sps_pps (const guint8 *data, gsize len, const guint8 **sps, gsize *sps_len,
    const guint8 **pps, gsize *pps_len)
{
  *sps = *pps = NULL;
  *sps_len = *pps_len = 0;

  gsize i = 0;
  gsize nal_start = 0;
  gboolean in_nal = FALSE;

  while (i + 3 <= len) {
    if (data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1) {
      if (in_nal) {
        gsize end = i;
        while (end > nal_start && data[end - 1] == 0)
          end--;
        guint8 type = data[nal_start] & 0x1f;
        if (type == 7 && !*sps) {
          *sps = data + nal_start;
          *sps_len = end - nal_start;
        } else if (type == 8 && !*pps) {
          *pps = data + nal_start;
          *pps_len = end - nal_start;
        }
      }
      i += 3;
      nal_start = i;
      in_nal = TRUE;
      continue;
    }
    i++;
  }
  if (in_nal && nal_start < len) {
    guint8 type = data[nal_start] & 0x1f;
    if (type == 7 && !*sps) {
      *sps = data + nal_start;
      *sps_len = len - nal_start;
    } else if (type == 8 && !*pps) {
      *pps = data + nal_start;
      *pps_len = len - nal_start;
    }
  }
  return *sps && *sps_len >= 4 && *pps && *pps_len >= 1;
}

static GBytes *
build_avcc (const guint8 *sps, gsize sps_len, const guint8 *pps, gsize pps_len)
{
  GByteArray *a = g_byte_array_sized_new (11 + sps_len + pps_len);
  guint8 hdr[6] = { 1, sps[1], sps[2], sps[3], 0xfc | 3, 0xe0 | 1 };
  guint8 be16[2];

  g_byte_array_append (a, hdr, sizeof hdr);
  be16[0] = sps_len >> 8; be16[1] = sps_len & 0xff;
  g_byte_array_append (a, be16, 2);
  g_byte_array_append (a, sps, sps_len);
  hdr[0] = 1;
  g_byte_array_append (a, hdr, 1);
  be16[0] = pps_len >> 8; be16[1] = pps_len & 0xff;
  g_byte_array_append (a, be16, 2);
  g_byte_array_append (a, pps, pps_len);
  return g_byte_array_free_to_bytes (a);
}

/* -- shared write path ---------------------------------------------------- */

/* Submit one object and map the libmoq result onto a flow return. On any
 * non-OK result the payload is released here. */
static GstFlowReturn
gst_moq_sink_write_object (GstMoqSink *self, GstMoqSinkPad *pad,
    moq_rcbuf_t *payload, moq_media_send_object_t *o, gsize len)
{
  g_mutex_lock (&self->send_lock);
  if (!self->sender) {
    g_mutex_unlock (&self->send_lock);
    moq_rcbuf_decref (payload);
    return GST_FLOW_FLUSHING;
  }
  GST_OBJECT_LOCK (self);
  gboolean flushing = pad->flushing || self->state_flushing;
  gboolean ended = pad->eos || self->eos_posted;
  GST_OBJECT_UNLOCK (self);
  if (flushing) {
    g_mutex_unlock (&self->send_lock);
    moq_rcbuf_decref (payload);
    return GST_FLOW_FLUSHING;
  }
  if (ended) {
    g_mutex_unlock (&self->send_lock);
    moq_rcbuf_decref (payload);
    return GST_FLOW_EOS;
  }
  moq_result_t rc = moq_media_sender_write (self->sender, pad->track, o);
  g_mutex_unlock (&self->send_lock);
  switch (rc) {
    case MOQ_OK:
      pad->objects_sent++;
      GST_LOG_OBJECT (pad, "wrote %s object (%" G_GSIZE_FORMAT " B)",
          o->is_sync ? "sync" : "delta", len);
      return GST_FLOW_OK;
    case MOQ_ERR_WOULD_BLOCK:
      moq_rcbuf_decref (payload);
      GST_LOG_OBJECT (pad, "dropped object under backpressure");
      return GST_FLOW_OK;
    case MOQ_ERR_INTERRUPTED:
      moq_rcbuf_decref (payload);
      return GST_FLOW_FLUSHING;
    case MOQ_ERR_CLOSED:
      moq_rcbuf_decref (payload);
      GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
          ("endpoint closed before publisher EOS"), (NULL));
      return GST_FLOW_ERROR;
    case MOQ_ERR_INVAL:
      moq_rcbuf_decref (payload);
      GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
          ("libmoq rejected object %" G_GUINT64_FORMAT " on track \"%s\" "
              "(rc=%d: invalid object, e.g. CMAF validation failed or a "
              "group start that is not a SAP)", pad->objects_sent,
              pad->track_name, (int) rc), (NULL));
      return GST_FLOW_ERROR;
    default:
      moq_rcbuf_decref (payload);
      GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
          ("media write failed (rc=%d)", (int) rc), (NULL));
      return GST_FLOW_ERROR;
  }
}

/* -- clock sync ----------------------------------------------------------- */

/* Wait until the buffer's running time when sync is on. Returns FLUSHING if
 * the wait was unscheduled by a flush or state change. */
static GstFlowReturn
gst_moq_sink_sync (GstMoqSink *self, GstMoqSinkPad *pad, GstBuffer *buf)
{
  if (!self->sync || pad->segment.format != GST_FORMAT_TIME)
    return GST_FLOW_OK;
  GstClockTime ts = GST_BUFFER_DTS_OR_PTS (buf);
  if (!GST_CLOCK_TIME_IS_VALID (ts))
    return GST_FLOW_OK;
  GstClockTime rt = gst_segment_to_running_time (&pad->segment,
      GST_FORMAT_TIME, ts);
  if (!GST_CLOCK_TIME_IS_VALID (rt))
    return GST_FLOW_OK;

  GstClock *clock = gst_element_get_clock (GST_ELEMENT (self));
  if (!clock)
    return GST_FLOW_OK;
  GstClockTime base = gst_element_get_base_time (GST_ELEMENT (self));
  if (!GST_CLOCK_TIME_IS_VALID (base)) {
    gst_object_unref (clock);
    return GST_FLOW_OK;
  }

  GST_OBJECT_LOCK (self);
  if (pad->flushing) {
    GST_OBJECT_UNLOCK (self);
    gst_object_unref (clock);
    return GST_FLOW_FLUSHING;
  }
  GstClockTime latency = self->latency;
  pad->clock_id = gst_clock_new_single_shot_id (clock, base + rt + latency);
  GST_OBJECT_UNLOCK (self);

  GstClockReturn cr = gst_clock_id_wait (pad->clock_id, NULL);

  GST_OBJECT_LOCK (self);
  gst_clock_id_unref (pad->clock_id);
  pad->clock_id = NULL;
  GST_OBJECT_UNLOCK (self);
  gst_object_unref (clock);

  return cr == GST_CLOCK_UNSCHEDULED ? GST_FLOW_FLUSHING : GST_FLOW_OK;
}

static void
gst_moq_sink_unschedule (GstMoqSink *self, GstMoqSinkPad *pad, gboolean flushing)
{
  GST_OBJECT_LOCK (self);
  pad->flushing = flushing;
  if (pad->clock_id)
    gst_clock_id_unschedule (pad->clock_id);
  GST_OBJECT_UNLOCK (self);
  /* A flushing pad will not produce a track, so let the barrier re-evaluate
   * instead of waiting out its timeout for this pad. */
  if (flushing) {
    g_mutex_lock (&self->track_lock);
    g_cond_broadcast (&self->track_cond);
    g_mutex_unlock (&self->track_lock);
  }
}

/* Cancellation owns OBJECT_LOCK independently of sender serialization, so
 * flush can interrupt an in-progress final drain. Endpoint publication and
 * removal share this lock; no callback can touch a freed endpoint. */
static void
gst_moq_sink_set_pad_flush (GstMoqSink *self, GstMoqSinkPad *pad, gboolean pending)
{
  GST_OBJECT_LOCK (self);
  if (pad->flush_pending != pending) {
    self->flush_count += pending ? 1 : -1;
    pad->flush_pending = pending;
  }
  pad->flushing = pending || self->state_flushing;
  if (pad->flushing && pad->clock_id)
    gst_clock_id_unschedule (pad->clock_id);
  if (self->ep)
    moq_endpoint_set_interrupted (self->ep, self->state_flushing || self->flush_count > 0);
  GST_OBJECT_UNLOCK (self);
  g_mutex_lock (&self->track_lock);
  g_cond_broadcast (&self->track_cond);
  g_mutex_unlock (&self->track_lock);
}

/* -- LOC path (always pads) ---------------------------------------------- */

/* Byte offset of the first NAL that is not an Access Unit Delimiter.
 *
 * h264parse emits an AUD at the head of every access unit when it outputs
 * byte-stream/au, whatever the encoder was asked for. In LOC the object
 * boundary already delimits the access unit, so the AUD carries nothing -- and
 * it costs: a consumer that walks the payload as length-prefixed NAL units
 * (the form the avc1 codec string and the avcC initData imply) reads the
 * 4-byte start code as a length, and the AUD's own bytes as the next one, so
 * the walk desynchronises and the keyframe fails to validate. Dropping it puts
 * the SPS first, where every parser expects it. */
static gsize
loc_skip_leading_aud (const guint8 *d, gsize len)
{
  gsize sc = 0;
  if (len >= 5 && d[0] == 0 && d[1] == 0 && d[2] == 0 && d[3] == 1)
    sc = 4;
  else if (len >= 4 && d[0] == 0 && d[1] == 0 && d[2] == 1)
    sc = 3;
  if (sc == 0 || (d[sc] & 0x1f) != 9)
    return 0;
  /* Skip to the next start code; without one the AU is only an AUD, so keep
   * the buffer as it is rather than publishing an empty object. */
  for (gsize i = sc; i + 3 < len; i++) {
    if (d[i] == 0 && d[i + 1] == 0 &&
        (d[i + 2] == 1 || (d[i + 2] == 0 && d[i + 3] == 1)))
      return i;
  }
  return 0;
}

/* Derive the LOC audio track's catalog fields and stash them on the pad. Unlike
 * video there is nothing to scavenge from the payload: AAC access units carry
 * no in-band config, so everything comes from the caps (codec_data =
 * AudioSpecificConfig), which GStreamer guarantees before the first buffer.
 * Deriving is split from adding so the sender can be attached only once every
 * linked pad has its track (gst_moq_sink_await_tracks). */
static gboolean
gst_moq_sink_prepare_loc_audio_track (GstMoqSink *self, GstMoqSinkPad *pad)
{
  gsize asc_len = 0;
  const guint8 *asc = pad->codec_data
      ? g_bytes_get_data (pad->codec_data, &asc_len) : NULL;
  gint rate = 0, channels = 0;
  if (!gst_moq_codec_aac_asc_params (asc, asc_len, &rate, &channels) ||
      (pad->rate > 0 && pad->rate != rate) ||
      (pad->channels > 0 && pad->channels != channels)) {
    GST_ELEMENT_ERROR (self, STREAM, FORMAT,
        ("LOC audio requires a two-byte AAC-LC AudioSpecificConfig with matching rate/channels"),
        ("Explicit-frequency, HE-AAC and program-config-element forms are not supported"));
    return FALSE;
  }
  pad->rate = rate;
  pad->channels = channels;
  gchar *codec = gst_moq_codec_string_from_aac_asc (asc, asc_len);
  g_free (pad->pend_codec);
  pad->pend_codec = codec;
  g_free (pad->pend_chan);
  pad->pend_chan = g_strdup_printf ("%d", pad->channels);
  return TRUE;
}

static moq_bytes_t
snapshot_span (moq_bytes_t span, GBytes **owned)
{
  *owned = g_bytes_new (span.data, span.len);
  return (moq_bytes_t) { g_bytes_get_data (*owned, NULL), span.len };
}

static void
prepared_track_free (gpointer data)
{
  PreparedTrack *t = data;
  gst_object_unref (t->pad);
  g_bytes_unref (t->name);
  g_bytes_unref (t->codec);
  g_bytes_unref (t->channels);
  g_bytes_unref (t->init);
  g_strfreev (t->cp_ref_strv);
  g_free (t->cp_refs);
  g_free (t);
}

static moq_result_t
register_track (GstMoqSink *self, GstMoqSinkPad *pad,
    const moq_media_track_cfg_t *cfg)
{
  if (self->preparing_tracks) {
    PreparedTrack *t = g_new0 (PreparedTrack, 1);
    t->pad = gst_object_ref (pad);
    t->cfg = *cfg;
    t->cfg.name = snapshot_span (cfg->name, &t->name);
    t->cfg.codec = snapshot_span (cfg->codec, &t->codec);
    t->cfg.channel_config = snapshot_span (cfg->channel_config, &t->channels);
    t->cfg.init_data = snapshot_span (cfg->init_data, &t->init);
    if (cfg->content_protection_ref_id_count > 0) {
      size_t n = cfg->content_protection_ref_id_count;
      t->cp_ref_strv = g_new0 (gchar *, n + 1);
      t->cp_refs = g_new0 (moq_bytes_t, n);
      for (size_t i = 0; i < n; i++) {
        const moq_bytes_t *r = &cfg->content_protection_ref_ids[i];
        t->cp_ref_strv[i] = g_strndup ((const gchar *) r->data, r->len);
        t->cp_refs[i].data = (const uint8_t *) t->cp_ref_strv[i];
        t->cp_refs[i].len = r->len;
      }
      t->cfg.content_protection_ref_ids = t->cp_refs;
    }
    g_ptr_array_add (self->preparing_tracks, t);
    return MOQ_OK;
  }
  g_mutex_lock (&self->send_lock);
  GST_OBJECT_LOCK (self);
  moq_result_t rc = !self->sender ? MOQ_ERR_CLOSED
      : pad->eos || pad->flushing || self->eos_posted ? MOQ_ERR_WRONG_STATE
      : moq_media_sender_add_track (self->sender, cfg, &pad->track);
  GST_OBJECT_UNLOCK (self);
  g_mutex_unlock (&self->send_lock);
  return rc;
}

/* No element locks, waits, writes or teardown in this task. The endpoint runs
 * posted tasks before sender hooks, so its first pump sees the whole set. */
static moq_result_t
start_sender_task (moq_endpoint_t *ep, moq_session_t *session,
    uint64_t now_us, void *ctx)
{
  SenderStartup *task = ctx;
  (void) now_us;
  g_mutex_lock (&task->lock);
  gboolean cancelled = task->cancelled || !session;
  task->running = TRUE;
  g_mutex_unlock (&task->lock);
  moq_result_t rc = cancelled ? MOQ_ERR_INTERRUPTED
      : moq_media_sender_attach (ep, &task->cfg, &task->sender);
  for (guint i = 0; rc == MOQ_OK && i < task->tracks->len; i++) {
    PreparedTrack *t = g_ptr_array_index (task->tracks, i);
    rc = moq_media_sender_add_track (task->sender, &t->cfg, &t->track);
  }
  g_mutex_lock (&task->lock);
  task->result = rc;
  task->done = TRUE;
  g_cond_broadcast (&task->cond);
  g_mutex_unlock (&task->lock);
  return rc;
}

/* Add the prepared LOC audio track. Caller holds track_lock and has already
 * attached the sender. */
static gboolean
gst_moq_sink_add_loc_audio_track (GstMoqSink *self, GstMoqSinkPad *pad)
{
  gsize asc_len = 0;
  const guint8 *asc = pad->codec_data
      ? g_bytes_get_data (pad->codec_data, &asc_len) : NULL;
  const gchar *codec = pad->pend_codec;
  const gchar *chan_cfg = pad->pend_chan;

  moq_media_track_cfg_t tc;
  moq_media_track_cfg_init (&tc);
  tc.name.data = (const uint8_t *) pad->track_name;
  tc.name.len = strlen (pad->track_name);
  tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
  tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
  tc.codec.data = (const uint8_t *) codec;
  tc.codec.len = strlen (codec);
  tc.is_live = TRUE;
  tc.bitrate = pad->bitrate;
  tc.samplerate = (uint32_t) pad->rate;
  tc.channel_config.data = (const uint8_t *) chan_cfg;
  tc.channel_config.len = strlen (chan_cfg);
  /* The ASC is this track's decoder config, so it belongs in initData for the
   * same reason avcC does on the video pad: a subscriber cannot configure a
   * decoder from the access units alone. */
  tc.init_data.data = asc;
  tc.init_data.len = asc_len;

  moq_result_t rc = register_track (self, pad, &tc);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not add LOC audio track \"%s\" (rc=%d)", pad->track_name,
            (int) rc), (NULL));
  } else {
    GST_DEBUG_OBJECT (pad, "prepared LOC track %s codec=%s sr=%d ch=%d "
        "initData=%" G_GSIZE_FORMAT " B", pad->track_name, codec, pad->rate,
        pad->channels, asc_len);
  }
  return rc == MOQ_OK;
}

/* Derive the LOC video track's catalog fields (avcC + codec string) from the
 * caps or the first keyframe and stash them on the pad. Split from the add for
 * the same reason as the audio pad: see gst_moq_sink_await_tracks. */
static gboolean
gst_moq_sink_prepare_loc_track (GstMoqSink *self, GstMoqSinkPad *pad,
    const guint8 *au, gsize au_len)
{
  const guint8 *sps, *pps;
  gsize sps_len, pps_len;
  GBytes *avcc = NULL;
  gchar *codec = NULL;

  /* stream-format=avc: the AVCDecoderConfigurationRecord is already in the
   * caps, and the access units carry no in-band parameter sets to scavenge. */
  if (pad->avc) {
    gsize n = 0;
    const guint8 *cd = pad->codec_data
        ? g_bytes_get_data (pad->codec_data, &n) : NULL;
    if (!cd || n < 4 || cd[0] != 1) {
      GST_ELEMENT_ERROR (self, STREAM, FORMAT,
          ("stream-format=avc on pad %s without a usable codec_data (avcC)",
              GST_PAD_NAME (pad)), (NULL));
      return FALSE;
    }
    avcc = g_bytes_ref (pad->codec_data);
    if (!self->codec)
      codec = g_strdup_printf ("avc1.%02x%02x%02x", cd[1], cd[2], cd[3]);
  } else if (find_sps_pps (au, au_len, &sps, &sps_len, &pps, &pps_len)) {
    avcc = build_avcc (sps, sps_len, pps, pps_len);
    if (!self->codec)
      codec = g_strdup_printf ("avc1.%02x%02x%02x", sps[1], sps[2], sps[3]);
  } else {
    GST_WARNING_OBJECT (pad, "keyframe carries no SPS/PPS; publishing "
        "without initData (use h264parse config-interval=-1)");
  }
  if (!codec)
    codec = g_strdup (self->codec ? self->codec : "avc1.42e01e");

  g_free (pad->pend_codec);
  pad->pend_codec = codec;
  g_clear_pointer (&pad->pend_avcc, g_bytes_unref);
  pad->pend_avcc = avcc;      /* may be NULL: no initData */
  return TRUE;
}

/* Add the prepared LOC video track. Caller holds track_lock and has already
 * attached the sender. */
static gboolean
gst_moq_sink_add_loc_track (GstMoqSink *self, GstMoqSinkPad *pad)
{
  GBytes *avcc = pad->pend_avcc;
  const gchar *codec = pad->pend_codec;

  moq_media_track_cfg_t tc;
  moq_media_track_cfg_init (&tc);
  tc.name.data = (const uint8_t *) pad->track_name;
  tc.name.len = strlen (pad->track_name);
  tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
  tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
  tc.codec.data = (const uint8_t *) codec;
  tc.codec.len = strlen (codec);
  tc.is_live = TRUE;
  tc.bitrate = pad->bitrate;
  if (avcc) {
    gsize n;
    tc.init_data.data = g_bytes_get_data (avcc, &n);
    tc.init_data.len = n;
  }
  if (pad->width > 0 && pad->height > 0) {
    tc.width = pad->width;
    tc.height = pad->height;
  }
  if (pad->fps_n > 0 && pad->fps_d > 0)
    tc.framerate_millis = gst_util_uint64_scale_int (1000, pad->fps_n, pad->fps_d);

  moq_result_t rc = register_track (self, pad, &tc);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not add track \"%s\" (rc=%d)", pad->track_name, (int) rc),
        (NULL));
  } else {
    GST_DEBUG_OBJECT (pad, "prepared LOC track %s codec=%s %dx%d initData=%zu B",
        pad->track_name, codec, pad->width, pad->height,
        (gsize) tc.init_data.len);
  }
  return rc == MOQ_OK;
}

static GstFlowReturn gst_moq_sink_add_cmaf_track (GstMoqSink *self,
    GstMoqSinkPad *pad, GBytes *init);
static gboolean gst_moq_sink_open_sender (GstMoqSink *self);
static void gst_moq_sink_build_content_protections (GstMoqSink *self);
static void gst_moq_sink_clear_content_protections (GstMoqSink *self);

static gboolean
pad_cancelled (GstMoqSink *self, GstMoqSinkPad *pad)
{
  GST_OBJECT_LOCK (self);
  gboolean cancelled = pad->flushing || pad->eos || !self->started;
  GST_OBJECT_UNLOCK (self);
  return cancelled;
}

/* TRUE when every LINKED pad has either prepared its track or dropped out
 * (EOS/flushing means it will never deliver one), and at least one did prepare.
 * Unlinked pads are excluded exactly as in gst_moq_sink_claim_eos: the always
 * "sink"/"audio" pads exist even in a CMAF-only pipeline that uses only the
 * request pads. Caller holds track_lock (taken outside OBJECT_LOCK). */
static gboolean
gst_moq_sink_tracks_ready (GstMoqSink *self)
{
  gboolean all = TRUE, any = FALSE;
  GST_OBJECT_LOCK (self);
  for (GList *l = GST_ELEMENT (self)->sinkpads; l; l = l->next) {
    GstMoqSinkPad *p = GST_MOQ_SINK_PAD (l->data);
    if (!gst_pad_is_linked (GST_PAD (p)))
      continue;
    if (p->pend_ready) {
      any = TRUE;
    } else if (!p->eos && !p->flushing) {
      all = FALSE;
    }
  }
  GST_OBJECT_UNLOCK (self);
  return all && any;
}

/* Attach the sender and add every prepared track to it. Caller holds
 * track_lock, which keeps this to one pass and keeps the add_track calls
 * together. */
static void
gst_moq_sink_commit_tracks (GstMoqSink *self)
{
  if (self->tracks_committed || self->tracks_failed)
    return;

  self->preparing_tracks = g_ptr_array_new_with_free_func (prepared_track_free);

  /* Author the root contentProtections BEFORE the pad walk, not at attach time:
   * it is what fills each protected pad's pend_cp_ref_ids, and the walk below
   * reads them into every track cfg. Authoring it later (where the attach
   * happens) leaves every track declaring no refIDs while the root entries are
   * published -- a catalog that advertises DRM systems no track references, so
   * a subscriber reads the tracks as clear while the objects are encrypted.
   * Safe this early: it needs only each pad's parsed init_info, which
   * gst_moq_sink_prepare_cmaf_track filled before the barrier released. */
  gst_moq_sink_build_content_protections (self);

  /* Snapshot the pad list so the walk does not hold OBJECT_LOCK across
   * add_track (which takes send_lock). */
  GList *pads = NULL;
  GST_OBJECT_LOCK (self);
  for (GList *l = GST_ELEMENT (self)->sinkpads; l; l = l->next)
    pads = g_list_prepend (pads, gst_object_ref (l->data));
  GST_OBJECT_UNLOCK (self);
  pads = g_list_reverse (pads);

  gboolean any = FALSE, failed = FALSE;
  for (GList *l = pads; l && !failed; l = l->next) {
    GstMoqSinkPad *p = GST_MOQ_SINK_PAD (l->data);
    if (!p->pend_ready || p->track || pad_cancelled (self, p))
      continue;
    gboolean ok;
    if (p->packaging == MOQ_MEDIA_PACKAGING_CMAF)
      ok = gst_moq_sink_add_cmaf_track (self, p, p->init) == GST_FLOW_OK;
    else if (p->media_type == MOQ_MEDIA_TYPE_AUDIO)
      ok = gst_moq_sink_add_loc_audio_track (self, p);
    else
      ok = gst_moq_sink_add_loc_track (self, p);
    if (ok)
      any = TRUE;
    else
      failed = TRUE;
  }
  g_list_free_full (pads, gst_object_unref);

  if (failed || !any || !gst_moq_sink_open_sender (self))
    self->tracks_failed = TRUE;
  else
    self->tracks_committed = TRUE;
  g_clear_pointer (&self->preparing_tracks, g_ptr_array_unref);
  g_cond_broadcast (&self->track_cond);
}

/* Wait for all linked pads, then create one immutable track snapshot. The
 * public endpoint task attaches/adds that snapshot in one managed turn before
 * sender hooks run. Failure to prepare a linked stream is an error, never a
 * successful partial catalog. Put queues on branches sharing an upstream task. */
static gboolean
gst_moq_sink_await_tracks (GstMoqSink *self, GstMoqSinkPad *pad)
{
  g_mutex_lock (&self->track_lock);
  pad->pend_ready = TRUE;
  g_cond_broadcast (&self->track_cond);

  gint64 deadline = g_get_monotonic_time ()
      + (gint64) self->catalog_wait_ms * G_TIME_SPAN_MILLISECOND;
  while (!self->tracks_committed && !self->tracks_failed) {
    if (pad_cancelled (self, pad))
      break;
    if (gst_moq_sink_tracks_ready (self)) {
      gst_moq_sink_commit_tracks (self);
      break;
    }
    if (pad_cancelled (self, pad))
      break;
    GST_DEBUG_OBJECT (self, "waiting for linked track set");
    if (!g_cond_wait_until (&self->track_cond, &self->track_lock, deadline)) {
      if (pad_cancelled (self, pad))
        break;
      self->tracks_failed = TRUE;
      GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
          ("linked pads did not prepare all tracks within %u ms", self->catalog_wait_ms),
          ("Add queues before moqsink pads; every linked stream must supply its first buffer"));
      g_cond_broadcast (&self->track_cond);
      break;
    }
  }
  if (self->tracks_committed && !pad->track && pad->pend_ready &&
      !pad_cancelled (self, pad)) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("pad %s was not in the initial track set", GST_PAD_NAME (pad)),
        ("Link all publishing pads before the first buffer; restart through READY to add streams"));
  }

  gboolean ok = self->tracks_committed && pad->track != NULL;
  g_mutex_unlock (&self->track_lock);
  return ok;
}

static GstFlowReturn
gst_moq_sink_chain_loc (GstMoqSink *self, GstMoqSinkPad *pad, GstBuffer *buffer)
{
  /* Map before the barrier: preparing the video track needs the access unit
   * when the caps carry no avcC (Annex B), and the barrier can block. */
  SinkFrame *f = g_new0 (SinkFrame, 1);
  f->buffer = buffer;          /* takes the chain's reference */
  if (!gst_buffer_map (f->buffer, &f->map, GST_MAP_READ)) {
    gst_buffer_unref (f->buffer);
    g_free (f);
    GST_ELEMENT_ERROR (self, STREAM, FORMAT, ("failed to map buffer"), (NULL));
    return GST_FLOW_ERROR;
  }

  gboolean audio = pad->media_type == MOQ_MEDIA_TYPE_AUDIO;
  /* Every AAC access unit is independently decodable, so an audio object is
   * always a sync point; video waits for a real keyframe. */
  gboolean keyframe = audio ||
      !GST_BUFFER_FLAG_IS_SET (buffer, GST_BUFFER_FLAG_DELTA_UNIT);
  if (!pad->track) {
    if (!keyframe) {
      GST_LOG_OBJECT (pad, "dropping delta frame before the first keyframe");
      sink_frame_free (f);
      return GST_FLOW_OK;
    }
    gboolean prepared = audio
        ? gst_moq_sink_prepare_loc_audio_track (self, pad)
        : gst_moq_sink_prepare_loc_track (self, pad, f->map.data, f->map.size);
    if (!prepared) {
      sink_frame_free (f);
      return GST_FLOW_ERROR;
    }
    /* Attaches the sender once every linked pad has a track. */
    if (!gst_moq_sink_await_tracks (self, pad)) {
      sink_frame_free (f);
      return pad_cancelled (self, pad)
          ? GST_FLOW_FLUSHING : GST_FLOW_ERROR;
    }
  }

  g_mutex_lock (&self->send_lock);
  gboolean fatal = !self->sender || moq_media_sender_is_fatal (self->sender);
  guint64 fatal_code = (self->sender && fatal)
      ? moq_media_sender_fatal_code (self->sender) : 0;
  g_mutex_unlock (&self->send_lock);
  if (fatal) {
    GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
        ("media sender failed (code=%" G_GUINT64_FORMAT ")", fatal_code),
        (NULL));
    sink_frame_free (f);
    return GST_FLOW_ERROR;
  }

  GstFlowReturn sr = gst_moq_sink_sync (self, pad, buffer);
  if (sr != GST_FLOW_OK) {
    sink_frame_free (f);
    return sr;
  }

  /* Rebase to the first PTS so LOC carries small, stream-relative times. */
  GstClockTime pts = GST_BUFFER_PTS (buffer);
  GstClockTime dts = GST_BUFFER_DTS (buffer);
  if (GST_CLOCK_TIME_IS_VALID (pts) && !GST_CLOCK_TIME_IS_VALID (pad->base_pts))
    pad->base_pts = pts;
  guint64 pts_us = (GST_CLOCK_TIME_IS_VALID (pts) &&
      GST_CLOCK_TIME_IS_VALID (pad->base_pts) && pts >= pad->base_pts)
      ? (pts - pad->base_pts) / GST_USECOND : 0;
  guint64 dts_us = (GST_CLOCK_TIME_IS_VALID (dts) &&
      GST_CLOCK_TIME_IS_VALID (pad->base_pts) && dts >= pad->base_pts)
      ? (dts - pad->base_pts) / GST_USECOND : pts_us;

  /* Video only: audio access units have no AUD to strip. */
  gsize skip = (audio || pad->avc)
      ? 0 : loc_skip_leading_aud (f->map.data, f->map.size);
  moq_rcbuf_t *payload = NULL;
  if (moq_rcbuf_wrap (moq_alloc_default (), f->map.data + skip,
          f->map.size - skip, sink_frame_release, f, &payload) != MOQ_OK) {
    sink_frame_free (f);
    GST_ELEMENT_ERROR (self, RESOURCE, WRITE, ("rcbuf wrap failed"), (NULL));
    return GST_FLOW_ERROR;
  }

  /* LOC-01 2.3.1.1: the Capture Timestamp is wall-clock microseconds since the
   * Unix epoch. presentation_time_us above is rebased to the start of the
   * stream, and libmoq would emit that verbatim if has_capture_time were false,
   * so a subscriber computing `now - capture` would see the whole epoch as
   * latency and treat every frame as ancient. Map this buffer's PTS onto the
   * shared anchor instead. */
  gboolean have_capture = FALSE;
  guint64 capture_us = 0;
  GstClockTime running = pad->segment.format == GST_FORMAT_TIME
      ? gst_segment_to_running_time (&pad->segment, GST_FORMAT_TIME, pts)
      : GST_CLOCK_TIME_NONE;
  if (GST_CLOCK_TIME_IS_VALID (running)) {
    GST_OBJECT_LOCK (self);
    if (!self->loc_anchored) {
      self->loc_anchor_epoch_us = g_get_real_time ();
      self->loc_anchor_pts = running;
      self->loc_anchored = TRUE;
      GST_INFO_OBJECT (pad, "LOC capture-timestamp anchor: pts %" GST_TIME_FORMAT
          " = %" G_GINT64_FORMAT " us since the epoch",
          GST_TIME_ARGS (pts), self->loc_anchor_epoch_us);
    }
    GstClockTimeDiff d = GST_CLOCK_DIFF (self->loc_anchor_pts, running);
    gint64 c = self->loc_anchor_epoch_us + d / GST_USECOND;
    GST_OBJECT_UNLOCK (self);
    if (c > 0) {
      capture_us = (guint64) c;
      have_capture = TRUE;
    }
  }

  gboolean starts_group = keyframe;
  if (audio) {
    /* Open an audio group at every VIDEO group start (keyframe), so both
     * tracks are joinable at the same instant. A subscriber starts each track
     * at that track's next group boundary, and the two cadences are otherwise
     * unrelated (video = GOP, audio = a fixed period), so their join points
     * drift against each other: measured on this pipeline, audio arrived up to
     * a full audio-period after video, and a player that cannot start video
     * before it has an audio reference then renders nothing at all. Aligning
     * collapses that gap to network jitter.
     *
     * loc-audio-group-ms stays as an UPPER bound so audio still opens groups
     * when video stalls or is absent (audio-only). An extra boundary between
     * keyframes is harmless here: it only makes audio available EARLIER, which
     * is the safe direction. */
    GST_OBJECT_LOCK (self);
    GstClockTime vgrp = self->loc_video_group_pts;
    GST_OBJECT_UNLOCK (self);

    if (GST_CLOCK_TIME_IS_VALID (vgrp) && vgrp != pad->aligned_to_video_pts) {
      pad->aligned_to_video_pts = vgrp;   /* one audio group per video group */
      pad->group_start = pts_us;
    } else if (!GST_CLOCK_TIME_IS_VALID (pad->group_start) ||
               pts_us >= pad->group_start + (guint64) self->loc_audio_group_ms * 1000) {
      pad->group_start = pts_us;
    } else {
      starts_group = FALSE;
    }
  } else if (starts_group && GST_CLOCK_TIME_IS_VALID (pts)) {
    /* Publish this video group start for the audio pad to align to. */
    GST_OBJECT_LOCK (self);
    self->loc_video_group_pts = pts;
    GST_OBJECT_UNLOCK (self);
  }

  moq_media_send_object_t o;
  memset (&o, 0, sizeof o);
  o.struct_size = sizeof o;
  o.payload = payload;
  o.is_sync = keyframe;
  o.starts_group = starts_group;
  o.decode_time_us = dts_us;
  o.presentation_time_us = pts_us;
  o.has_capture_time = have_capture;
  o.capture_time_us = capture_us;

  return gst_moq_sink_write_object (self, pad, payload, &o, f->map.size - skip);
}

/* -- CMAF path (request pads) ------------------------------------------- */

/* Parse a CMAF pad's init segment and stash what the track needs on the pad.
 * Split from the add for the same reason as the LOC pads -- see
 * gst_moq_sink_await_tracks -- and additionally because the CENC boxes this
 * parse yields (has_cenc / scheme / default_KID) have to be known BEFORE the
 * sender is attached: the catalog's root contentProtections travel in the
 * sender cfg. */
static gboolean
gst_moq_sink_prepare_cmaf_track (GstMoqSink *self, GstMoqSinkPad *pad,
    GBytes *init)
{
  gsize init_len;
  const guint8 *init_data = g_bytes_get_data (init, &init_len);

  moq_cmaf_init_info_init (&pad->init_info);
  moq_result_t rc = moq_cmaf_parse_init (
      (moq_bytes_t) { init_data, init_len }, &pad->init_info);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, STREAM, FORMAT,
        ("could not parse CMAF init segment on pad %s (rc=%d)",
            GST_PAD_NAME (pad), (int) rc), (NULL));
    return FALSE;
  }
  pad->has_init_info = TRUE;

  gchar *codec = gst_moq_codec_string_from_init (&pad->init_info);
  if (!codec) {
    GST_ELEMENT_ERROR (self, STREAM, CODEC_NOT_FOUND,
        ("unsupported codec in CMAF init segment on pad %s (libmoq kind %d); "
            "this plugin publishes avc1, mp4a and opus", GST_PAD_NAME (pad),
            (int) pad->init_info.codec_kind), (NULL));
    return FALSE;
  }
  if ((pad->media_type == MOQ_MEDIA_TYPE_AUDIO) !=
      (pad->init_info.codec_kind == MOQ_CMAF_CODEC_AAC ||
          pad->init_info.codec_kind == MOQ_CMAF_CODEC_OPUS)) {
    GST_ELEMENT_ERROR (self, STREAM, WRONG_TYPE,
        ("pad %s is %s but the init segment carries codec %s",
            GST_PAD_NAME (pad),
            pad->media_type == MOQ_MEDIA_TYPE_AUDIO ? "audio" : "video",
            codec), (NULL));
    g_free (codec);
    return FALSE;
  }

  g_free (pad->pend_codec);
  pad->pend_codec = codec;
  return TRUE;
}

/* Register a prepared CMAF track. Caller holds track_lock and has already
 * attached the sender. */
static GstFlowReturn
gst_moq_sink_add_cmaf_track (GstMoqSink *self, GstMoqSinkPad *pad, GBytes *init)
{
  gsize init_len;
  const guint8 *init_data = g_bytes_get_data (init, &init_len);
  const gchar *codec = pad->pend_codec;
  moq_result_t rc;

  if (!pad->has_init_info || !codec) {
    GST_ELEMENT_ERROR (self, STREAM, FAILED,
        ("CMAF track on pad %s was not prepared before the add",
            GST_PAD_NAME (pad)), (NULL));
    return GST_FLOW_ERROR;
  }

  gchar channels[16] = "2";
  if (pad->init_info.channel_count > 0)
    g_snprintf (channels, sizeof channels, "%u", pad->init_info.channel_count);

  moq_media_track_cfg_t tc;
  moq_media_track_cfg_init (&tc);
  tc.name.data = (const uint8_t *) pad->track_name;
  tc.name.len = strlen (pad->track_name);
  tc.media_type = pad->media_type;
  tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
  tc.codec.data = (const uint8_t *) codec;
  tc.codec.len = strlen (codec);
  tc.is_live = TRUE;
  tc.bitrate = pad->bitrate;
  tc.timescale = pad->init_info.timescale;
  tc.init_data.data = init_data;
  tc.init_data.len = init_len;
  tc.emit_sap_timeline = self->sap_timeline;
  /* CMSF 4.1.2: declaring these ids tells a subscriber the track is encrypted,
   * and libmoq cross-checks that every id resolves to a root entry AND that
   * init_data really carries the 4.2 CENC boxes. Resolved at the barrier, so an
   * unprotected track leaves this empty. */
  guint n_refs = pad->pend_cp_ref_ids
      ? g_strv_length (pad->pend_cp_ref_ids) : 0;
  moq_bytes_t *refs = NULL;
  if (n_refs > 0) {
    refs = g_new0 (moq_bytes_t, n_refs);
    for (guint i = 0; i < n_refs; i++) {
      refs[i].data = (const uint8_t *) pad->pend_cp_ref_ids[i];
      refs[i].len = strlen (pad->pend_cp_ref_ids[i]);
    }
    tc.content_protection_ref_ids = refs;
    tc.content_protection_ref_id_count = n_refs;
  }
  if (pad->media_type == MOQ_MEDIA_TYPE_VIDEO) {
    tc.width = pad->init_info.width;
    tc.height = pad->init_info.height;
    if (pad->fps_n > 0 && pad->fps_d > 0)
      tc.framerate_millis = gst_util_uint64_scale_int (1000, pad->fps_n, pad->fps_d);
  } else {
    tc.samplerate = pad->init_info.samplerate;
    tc.channel_config.data = (const uint8_t *) channels;
    tc.channel_config.len = strlen (channels);
  }

  /* register_track either adds the track now or defers the cfg to the sender
   * startup task; it snapshots every span it keeps, refs included, so the
   * array is ours to free either way. */
  rc = register_track (self, pad, &tc);
  g_free (refs);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not add CMAF track \"%s\" codec=%s (rc=%d)%s",
            pad->track_name, codec, (int) rc,
            n_refs ? " -- an encrypted track needs an init segment carrying "
            "the CMSF 4.2 CENC boxes" : ""), (NULL));
  } else if (pad->media_type == MOQ_MEDIA_TYPE_VIDEO) {
    GST_DEBUG_OBJECT (pad, "prepared CMAF track %s codec=%s %ux%u timescale=%u "
        "init=%" G_GSIZE_FORMAT " B", pad->track_name, codec,
        pad->init_info.width, pad->init_info.height,
        pad->init_info.timescale, init_len);
  } else {
    GST_DEBUG_OBJECT (pad, "prepared CMAF track %s codec=%s sr=%u ch=%s "
        "timescale=%u init=%" G_GSIZE_FORMAT " B", pad->track_name, codec,
        pad->init_info.samplerate, channels, pad->init_info.timescale,
        init_len);
  }
  return rc == MOQ_OK ? GST_FLOW_OK : GST_FLOW_ERROR;
}

/* Send one moof+mdat fragment as a MoQ object. Takes the GBytes reference. */
static GstFlowReturn
gst_moq_sink_send_fragment (GstMoqSink *self, GstMoqSinkPad *pad, GBytes *frag)
{
  gsize len;
  const guint8 *data = g_bytes_get_data (frag, &len);

  /* First sample flags decide whether this fragment opens a group. Start
   * with a stack array; if the fragment holds more samples than that,
   * MOQ_ERR_BUFFER reports the required count (trusted, fragment-bounded)
   * without filling any sample, so grow and reparse once. Only MOQ_OK means
   * the sample table (and samples[0].flags) is actually populated. */
  moq_cmaf_sample_t stack_samples[64];
  moq_cmaf_sample_t *heap_samples = NULL;
  moq_cmaf_fragment_info_t fi;
  moq_cmaf_fragment_info_init (&fi, stack_samples, G_N_ELEMENTS (stack_samples));
  moq_result_t rc = moq_cmaf_parse_fragment ((moq_bytes_t) { data, len }, &fi);
  if (rc == MOQ_ERR_BUFFER) {
    heap_samples = g_new (moq_cmaf_sample_t, fi.sample_count);
    moq_cmaf_fragment_info_init (&fi, heap_samples, fi.sample_count);
    rc = moq_cmaf_parse_fragment ((moq_bytes_t) { data, len }, &fi);
  }
  if (rc != MOQ_OK) {
    g_clear_pointer (&heap_samples, g_free);
    g_bytes_unref (frag);
    GST_ELEMENT_ERROR (self, STREAM, FORMAT,
        ("malformed CMAF fragment on pad %s (rc=%d)", GST_PAD_NAME (pad),
            (int) rc), (NULL));
    return GST_FLOW_ERROR;
  }
  /* sample_is_non_sync_sample (bit 0x00010000): clear means this sample IS
   * a sync sample. Audio fragments are always treated as sync points. Do
   * not use moq_cmaf_sap_from_sample_flags() here: it also reports UNKNOWN
   * (possible open-GOP SAP-3) for a non-sync-but-independent sample, which
   * is not what "opens a group" means for mp4mux's trun first_sample_flags. */
  guint32 first_flags = fi.sample_count > 0 ? fi.samples[0].flags : fi.default_sample_flags;
  gboolean sync = pad->media_type == MOQ_MEDIA_TYPE_AUDIO ||
      !(first_flags & 0x00010000);
  g_clear_pointer (&heap_samples, g_free);

  moq_rcbuf_t *payload = NULL;
  if (moq_rcbuf_wrap (moq_alloc_default (), data, len, sink_bytes_release,
          frag, &payload) != MOQ_OK) {
    g_bytes_unref (frag);
    GST_ELEMENT_ERROR (self, RESOURCE, WRITE, ("rcbuf wrap failed"), (NULL));
    return GST_FLOW_ERROR;
  }

  moq_media_send_object_t o;
  memset (&o, 0, sizeof o);
  o.struct_size = sizeof o;
  o.payload = payload;
  o.properties = NULL;          /* CMAF timing lives in the fragment */
  o.is_sync = sync;
  o.starts_group = sync;
  if (sync) {
    o.has_sap_type = TRUE;
    o.sap_type = MOQ_SAP_TYPE_1;
  }
  return gst_moq_sink_write_object (self, pad, payload, &o, len);
}

static GstFlowReturn
gst_moq_sink_chain_cmaf (GstMoqSink *self, GstMoqSinkPad *pad, GstBuffer *buffer)
{
  /* The sender is attached by the track barrier below, not by _start, so a
   * NULL sender here just means no pad has produced a track yet. */
  g_mutex_lock (&self->send_lock);
  gboolean fatal = self->sender && moq_media_sender_is_fatal (self->sender);
  guint64 fatal_code = fatal ? moq_media_sender_fatal_code (self->sender) : 0;
  g_mutex_unlock (&self->send_lock);
  if (fatal) {
    GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
        ("media sender failed (code=%" G_GUINT64_FORMAT ")", fatal_code),
        (NULL));
    gst_buffer_unref (buffer);
    return GST_FLOW_ERROR;
  }

  GstFlowReturn ret = gst_moq_sink_sync (self, pad, buffer);
  if (ret != GST_FLOW_OK) {
    gst_buffer_unref (buffer);
    return ret;
  }

  GstMapInfo map;
  if (!gst_buffer_map (buffer, &map, GST_MAP_READ)) {
    gst_buffer_unref (buffer);
    GST_ELEMENT_ERROR (self, STREAM, FORMAT, ("failed to map buffer"), (NULL));
    return GST_FLOW_ERROR;
  }
  GError *err = NULL;
  gboolean ok = gst_moq_fmp4_splitter_push (&pad->splitter, map.data, map.size, &err);
  gst_buffer_unmap (buffer, &map);
  gst_buffer_unref (buffer);
  if (!ok) {
    GST_ELEMENT_ERROR (self, STREAM, FORMAT,
        ("fragmented MP4 on pad %s: %s", GST_PAD_NAME (pad), err->message), (NULL));
    g_error_free (err);
    return GST_FLOW_ERROR;
  }

  GstMoqFmp4Unit *u;
  while (ret == GST_FLOW_OK && (u = gst_moq_fmp4_splitter_pull (&pad->splitter))) {
    if (u->kind == GST_MOQ_FMP4_INIT) {
      if (pad->track) {
        /* A new init segment mid-stream is a codec change; not supported. */
        GST_ELEMENT_ERROR (self, STREAM, FORMAT,
            ("pad %s received a second init segment; codec changes are not "
                "supported", GST_PAD_NAME (pad)), (NULL));
        ret = GST_FLOW_ERROR;
      } else {
        g_clear_pointer (&pad->init, g_bytes_unref);
        pad->init = g_bytes_ref (u->data);
        /* Parse before the barrier: the catalog's contentProtections are built
         * from these CENC boxes and travel in the sender cfg, so the parse has
         * to precede the attach (gst_moq_sink_build_content_protections). */
        if (!gst_moq_sink_prepare_cmaf_track (self, pad, pad->init)) {
          ret = GST_FLOW_ERROR;
        } else if (!gst_moq_sink_await_tracks (self, pad)) {
          /* Attaches the sender once every linked pad has its init segment, so
           * the initial catalog carries them all. */
          ret = (pad->flushing || !self->started)
              ? GST_FLOW_FLUSHING : GST_FLOW_ERROR;
        }
      }
    } else {
      if (!pad->track) {
        GST_ELEMENT_ERROR (self, STREAM, FORMAT,
            ("fragment before init segment on pad %s", GST_PAD_NAME (pad)), (NULL));
        ret = GST_FLOW_ERROR;
      } else {
        ret = gst_moq_sink_send_fragment (self, pad, g_bytes_ref (u->data));
      }
    }
    gst_moq_fmp4_unit_free (u);
  }
  if (!pad->skip_logged && gst_moq_fmp4_splitter_skipped (&pad->splitter)) {
    GST_DEBUG_OBJECT (pad, "skipping non-media top-level boxes (styp/sidx/...)");
    pad->skip_logged = TRUE;
  }
  return ret;
}

/* -- pad functions -------------------------------------------------------- */

static GstFlowReturn
gst_moq_sink_chain (GstPad *gpad, GstObject *parent, GstBuffer *buffer)
{
  GstMoqSink *self = GST_MOQ_SINK (parent);
  GstMoqSinkPad *pad = GST_MOQ_SINK_PAD (gpad);

  if (!self->started) {
    gst_buffer_unref (buffer);
    return GST_FLOW_FLUSHING;
  }
  if (pad->packaging == MOQ_MEDIA_PACKAGING_CMAF)
    return gst_moq_sink_chain_cmaf (self, pad, buffer);
  return gst_moq_sink_chain_loc (self, pad, buffer);
}

/* Keep accepted/deferred native obligations independently of Gst pads.
 * Release serialization between retries so another track can keep writing. */
static moq_result_t
gst_moq_sink_end_handle (GstMoqSink *self, moq_media_track_t *track,
    gint64 deadline, guint64 generation)
{
  for (;;) {
    g_mutex_lock (&self->send_lock);
    if (!self->sender || !self->ep || self->sender_generation != generation) {
      g_mutex_unlock (&self->send_lock);
      return MOQ_ERR_CLOSED;
    }
    EndRequest *request = NULL;
    for (GList *l = self->end_requests; l; l = l->next) {
      EndRequest *r = l->data;
      if (r->track == track) { request = r; break; }
    }
    if (!request) {
      request = g_new0 (EndRequest, 1);
      request->track = track;
      request->deadline = deadline;
      self->end_requests = g_list_append (self->end_requests, request);
    }
    guint64 remaining = gst_moq_eos_remaining (request->deadline);
    moq_result_t rc = request->accepted ? MOQ_OK
        : moq_media_sender_is_fatal (self->sender) ? MOQ_ERR_CLOSED
        : !remaining ? MOQ_DONE
        : moq_media_sender_end_track (self->sender, track);
    if (rc == MOQ_OK)
      request->accepted = TRUE;
    g_mutex_unlock (&self->send_lock);
    if (rc != MOQ_ERR_WOULD_BLOCK)
      return rc;
    g_usleep (MIN (remaining, 5000));
  }
}

static moq_result_t
gst_moq_sink_end_track (GstMoqSink *self, GstMoqSinkPad *pad, gint64 deadline)
{
  g_mutex_lock (&self->send_lock);
  GST_OBJECT_LOCK (self);
  moq_media_track_t *track = pad->track;
  guint64 generation = self->sender_generation;
  GST_OBJECT_UNLOCK (self);
  g_mutex_unlock (&self->send_lock);
  return track ? gst_moq_sink_end_handle (self, track, deadline, generation) : MOQ_OK;
}

static void
gst_moq_sink_eos_error (GstMoqSink *self, moq_result_t rc)
{
  g_mutex_lock (&self->send_lock);
  self->eos_failed = TRUE;
  g_mutex_unlock (&self->send_lock);
  GST_ELEMENT_ERROR (self, RESOURCE, WRITE,
      ("publisher EOS failed or aborted (rc=%d)", (int) rc),
      ("Local service/stream flush did not complete; teardown may discard the tail"));
}

static void
gst_moq_sink_retry_ends (GstMoqSink *self)
{
  GArray *pending = g_array_new (FALSE, FALSE, sizeof (EndRequest));
  g_mutex_lock (&self->send_lock);
  guint64 generation = self->sender_generation;
  for (GList *l = self->end_requests; l; l = l->next) {
    EndRequest *request = l->data;
    if (!request->accepted)
      g_array_append_val (pending, *request);
  }
  g_mutex_unlock (&self->send_lock);
  moq_result_t rc = MOQ_OK;
  for (guint i = 0; i < pending->len && rc == MOQ_OK; i++) {
    EndRequest *r = &g_array_index (pending, EndRequest, i);
    rc = gst_moq_sink_end_handle (self, r->track, r->deadline, generation);
  }
  g_array_unref (pending);
  if (rc != MOQ_OK && rc != MOQ_ERR_INTERRUPTED)
    gst_moq_sink_eos_error (self, rc);
}

/* The claim closes writer/add gates. The startup task has settled before
 * a sender is published to writers, so no pending task can create output here. */
static gboolean
gst_moq_sink_drain (GstMoqSink *self, gint64 deadline)
{
  g_mutex_lock (&self->send_lock);
  moq_result_t rc = !self->eos_failed &&
      ((self->sender && self->ep) || (!self->sender && !self->ep && !self->end_requests))
      ? MOQ_OK : MOQ_ERR_CLOSED;
  for (GList *l = self->end_requests; l && rc == MOQ_OK; l = l->next) {
    EndRequest *r = l->data;
    if (!r->accepted) {
      rc = gst_moq_eos_end (self->ep, self->sender, r->track, MIN (r->deadline, deadline));
      r->accepted = rc == MOQ_OK;
    }
  }
  if (rc == MOQ_OK && self->ep)
    rc = gst_moq_eos_drain (self->ep, deadline);
  g_mutex_unlock (&self->send_lock);
  if (rc != MOQ_OK) {
    gst_moq_sink_eos_error (self, rc);
    return FALSE;
  }
  GST_INFO_OBJECT (self, "publisher EOS: local service and stream queues flushed");
  gst_element_post_message (GST_ELEMENT (self), gst_message_new_eos (GST_OBJECT (self)));
  return TRUE;
}

/* Claim once after all linked pads or pads with native tracks reach EOS and
 * accept their end requests. An unused unlinked LOC pad is excluded. Taking
 * send_lock before OBJECT_LOCK matches add/write ordering and prevents a
 * concurrent EOS handler's unaccepted request from escaping the barrier. */
static gboolean
gst_moq_sink_claim_eos (GstMoqSink *self)
{
  gboolean claim = FALSE;
  g_mutex_lock (&self->send_lock);
  GST_OBJECT_LOCK (self);
  if (!self->eos_posted && !self->eos_failed) {
    gboolean all = TRUE;
    gboolean any_linked = FALSE;
    for (GList *l = GST_ELEMENT (self)->sinkpads; l; l = l->next) {
      GstMoqSinkPad *p = GST_MOQ_SINK_PAD (l->data);
      if (!gst_pad_is_linked (GST_PAD (p)) && !p->track)
        continue;
      any_linked = TRUE;
      if (!p->eos)
        all = FALSE;
      if (p->track) {
        gboolean accepted = FALSE;
        for (GList *l = self->end_requests; l; l = l->next) {
          EndRequest *r = l->data;
          if (r->track == p->track && r->accepted)
            accepted = TRUE;
        }
        if (!accepted) all = FALSE;
      }
    }
    if (all && any_linked) {
      self->eos_posted = TRUE;
      claim = TRUE;
    }
  }
  GST_OBJECT_UNLOCK (self);
  g_mutex_unlock (&self->send_lock);
  return claim;
}

static gboolean
gst_moq_sink_event (GstPad *gpad, GstObject *parent, GstEvent *event)
{
  GstMoqSink *self = GST_MOQ_SINK (parent);
  GstMoqSinkPad *pad = GST_MOQ_SINK_PAD (gpad);

  switch (GST_EVENT_TYPE (event)) {
    case GST_EVENT_CAPS: {
      GstCaps *caps;
      gst_event_parse_caps (event, &caps);
      GstStructure *st = gst_caps_get_structure (caps, 0);
      pad->width = pad->height = 0;
      pad->fps_n = pad->fps_d = 0;
      gst_structure_get_int (st, "width", &pad->width);
      gst_structure_get_int (st, "height", &pad->height);
      gst_structure_get_fraction (st, "framerate", &pad->fps_n, &pad->fps_d);
      if (pad->media_type == MOQ_MEDIA_TYPE_VIDEO &&
          pad->packaging == MOQ_MEDIA_PACKAGING_RAW) {
        const gchar *sf = gst_structure_get_string (st, "stream-format");
        pad->avc = sf && !strcmp (sf, "avc");
        g_clear_pointer (&pad->codec_data, g_bytes_unref);
        const GValue *cd = gst_structure_get_value (st, "codec_data");
        if (pad->avc && cd && GST_VALUE_HOLDS_BUFFER (cd)) {
          GstBuffer *cdbuf = gst_value_get_buffer (cd);
          GstMapInfo m;
          if (gst_buffer_map (cdbuf, &m, GST_MAP_READ)) {
            pad->codec_data = g_bytes_new (m.data, m.size);
            gst_buffer_unmap (cdbuf, &m);
          }
        }
      }
      if (pad->media_type == MOQ_MEDIA_TYPE_AUDIO &&
          pad->packaging == MOQ_MEDIA_PACKAGING_RAW) {
        pad->rate = pad->channels = 0;
        gst_structure_get_int (st, "rate", &pad->rate);
        gst_structure_get_int (st, "channels", &pad->channels);
        g_clear_pointer (&pad->codec_data, g_bytes_unref);
        const GValue *cd = gst_structure_get_value (st, "codec_data");
        if (cd && GST_VALUE_HOLDS_BUFFER (cd)) {
          GstBuffer *cdbuf = gst_value_get_buffer (cd);
          GstMapInfo m;
          if (gst_buffer_map (cdbuf, &m, GST_MAP_READ)) {
            pad->codec_data = g_bytes_new (m.data, m.size);
            gst_buffer_unmap (cdbuf, &m);
          }
        }
        /* The catalog's samplerate/channelConfig are REQUIRED for an audio
         * track (MSF-01 5.2.28/5.2.29) and libmoq rejects the track without
         * them, so fall back to the ASC when the caps omit either. */
        if ((pad->rate == 0 || pad->channels == 0) && pad->codec_data) {
          gsize n;
          const guint8 *asc = g_bytes_get_data (pad->codec_data, &n);
          gint r = 0, c = 0;
          if (gst_moq_codec_aac_asc_params (asc, n, &r, &c)) {
            if (pad->rate == 0) pad->rate = r;
            if (pad->channels == 0) pad->channels = c;
          }
        }
      }
      GST_DEBUG_OBJECT (pad, "caps %" GST_PTR_FORMAT, caps);
      break;
    }
    case GST_EVENT_SEGMENT:
      gst_event_copy_segment (event, &pad->segment);
      break;
    case GST_EVENT_FLUSH_START:
      gst_moq_sink_set_pad_flush (self, pad, TRUE);
      break;
    case GST_EVENT_FLUSH_STOP:
      gst_moq_sink_set_pad_flush (self, pad, FALSE);
      gst_segment_init (&pad->segment, GST_FORMAT_UNDEFINED);
      gst_moq_sink_retry_ends (self);
      break;
    /* GST_EVENT_LATENCY never reaches here: it is flagged UPSTREAM, so
     * gst_pad_send_event() on a sink pad refuses it before invoking this
     * function. It is intercepted in gst_moq_sink_send_event() instead. */
    case GST_EVENT_EOS: {
      gint64 deadline = g_get_monotonic_time () + EOS_DRAIN_TIMEOUT_US;
      /* EOS is serialized with this pad's chain. A buffered partial box or
       * unpaired moof must not disappear behind a successful native drain. */
      if (pad->packaging == MOQ_MEDIA_PACKAGING_CMAF &&
          (pad->splitter.buf->len != 0 || pad->splitter.moof != NULL)) {
        g_mutex_lock (&self->send_lock);
        self->eos_failed = TRUE;
        g_mutex_unlock (&self->send_lock);
        GST_ELEMENT_ERROR (self, STREAM, FORMAT,
            ("incomplete fragmented MP4 at EOS on pad %s", GST_PAD_NAME (pad)),
            ("A partial box or moof without mdat remains buffered"));
        gst_event_unref (event);
        return FALSE;
      }
      GST_OBJECT_LOCK (self);
      pad->eos = TRUE;
      GST_OBJECT_UNLOCK (self);
      /* Drops this pad out of the track barrier (a stream that ends without
       * ever delivering a keyframe must not hold the others up). */
      g_mutex_lock (&self->track_lock);
      g_cond_broadcast (&self->track_cond);
      g_mutex_unlock (&self->track_lock);
      moq_result_t rc = gst_moq_sink_end_track (self, pad, deadline);
      if (rc != MOQ_OK) {
        gst_moq_sink_eos_error (self, rc);
        gst_event_unref (event);
        return FALSE;
      }
      if (gst_moq_sink_claim_eos (self) && !gst_moq_sink_drain (self, deadline)) {
        gst_event_unref (event);
        return FALSE;
      }
      break;
    }
    default:
      break;
  }
  gst_event_unref (event);
  return TRUE;
}

static GstPad *
gst_moq_sink_request_new_pad (GstElement *element, GstPadTemplate *templ,
    const gchar *name, const GstCaps *caps)
{
  GstMoqSink *self = GST_MOQ_SINK (element);
  (void) caps;
  gboolean is_audio = g_str_has_prefix (templ->name_template, "audio");
  gchar *padname = name ? g_strdup (name)
      : g_strdup_printf (templ->name_template, self->pad_serial++);

  GstMoqSinkPad *pad = gst_moq_sink_pad_new (templ, padname,
      is_audio ? MOQ_MEDIA_TYPE_AUDIO : MOQ_MEDIA_TYPE_VIDEO,
      MOQ_MEDIA_PACKAGING_CMAF,
      is_audio ? "audio" : "video",
      is_audio ? DEFAULT_AUDIO_BITRATE : DEFAULT_BITRATE,
      self->max_fragment_size);
  g_free (padname);

  gst_pad_set_chain_function (GST_PAD (pad), gst_moq_sink_chain);
  gst_pad_set_event_function (GST_PAD (pad), gst_moq_sink_event);
  gst_pad_set_active (GST_PAD (pad), TRUE);
  gst_element_add_pad (element, GST_PAD (pad));
  GST_INFO_OBJECT (pad, "requested CMAF %s pad", is_audio ? "audio" : "video");
  return GST_PAD (pad);
}

/* A plain GstElement has no source pads, so the base gst_element_send_event()
 * has nothing to route events sent to a sink element to. GST_EVENT_LATENCY
 * is the one that matters here: it is flagged GST_EVENT_TYPE_UPSTREAM, so
 * gst_pad_send_event() on any of our (sink-direction) pads would refuse it
 * ("wrong direction") without ever calling the pad's event function -- it
 * must be handled directly, exactly like GstBaseSink does internally.
 * Anything else is pushed onto every sink pad via gst_pad_send_event(),
 * which invokes that pad's event function synchronously (e.g. a flushing
 * seek or a forced EOS sent straight to the element rather than flowing
 * through the pipeline). */
static gboolean
gst_moq_sink_send_event (GstElement *element, GstEvent *event)
{
  GstMoqSink *self = GST_MOQ_SINK (element);

  if (GST_EVENT_TYPE (event) == GST_EVENT_LATENCY) {
    GstClockTime latency;
    gst_event_parse_latency (event, &latency);
    GST_OBJECT_LOCK (self);
    self->latency = latency;
    GST_OBJECT_UNLOCK (self);
    GST_DEBUG_OBJECT (self, "latency configured: %" GST_TIME_FORMAT,
        GST_TIME_ARGS (latency));
    gst_event_unref (event);
    return TRUE;
  }

  GList *pads = NULL, *l;
  gboolean ret = FALSE;

  GST_OBJECT_LOCK (self);
  for (l = element->sinkpads; l; l = l->next)
    pads = g_list_prepend (pads, gst_object_ref (GST_PAD (l->data)));
  GST_OBJECT_UNLOCK (self);

  for (l = pads; l; l = l->next) {
    if (gst_pad_send_event (GST_PAD (l->data), gst_event_ref (event)))
      ret = TRUE;
  }
  g_list_free_full (pads, gst_object_unref);
  gst_event_unref (event);
  return ret;
}

static void
gst_moq_sink_release_pad (GstElement *element, GstPad *gpad)
{
  GstMoqSink *self = GST_MOQ_SINK (element);
  GstMoqSinkPad *pad = GST_MOQ_SINK_PAD (gpad);
  gst_moq_sink_unschedule (self, pad, TRUE);
  gst_pad_set_active (gpad, FALSE);
  gst_moq_sink_set_pad_flush (self, pad, FALSE);
  gint64 deadline = g_get_monotonic_time () + EOS_DRAIN_TIMEOUT_US;
  moq_result_t rc = gst_moq_sink_end_track (self, pad, deadline);
  if (rc != MOQ_OK && rc != MOQ_ERR_INTERRUPTED)
    gst_moq_sink_eos_error (self, rc);
  gst_element_remove_pad (element, gpad);
  if (gst_moq_sink_claim_eos (self))
    gst_moq_sink_drain (self, deadline);
}

/* -- lifecycle ------------------------------------------------------------ */

/* Network callback: no waiting, locks or dependency on self->sender. */
static void
gst_moq_sink_on_ready (void *ctx, moq_media_sender_t *sender)
{
  GstMoqReadyDiagnostic *diagnostic = ctx;
  (void) sender;
  GST_INFO ("native sender ready namespace=%s", diagnostic->namespace_str);
}

static void
gst_moq_sink_ready_diagnostic_free (GstMoqReadyDiagnostic *diagnostic)
{
  if (diagnostic) {
    g_free (diagnostic->namespace_str);
    g_free (diagnostic);
  }
}

static gboolean
gst_moq_sink_start (GstMoqSink *self)
{
  if (!gst_moq_sink_build_namespace (self))
    return FALSE;

  self->eos_posted = FALSE;
  self->eos_failed = FALSE;
  self->loc_anchored = FALSE;
  self->loc_video_group_pts = GST_CLOCK_TIME_NONE;
  GST_OBJECT_LOCK (self);
  self->flush_count = 0;
  self->state_flushing = FALSE;
  for (GList *l = GST_ELEMENT (self)->sinkpads; l; l = l->next)
    gst_moq_sink_pad_reset (GST_MOQ_SINK_PAD (l->data));
  GST_OBJECT_UNLOCK (self);

  g_mutex_lock (&self->track_lock);
  self->tracks_committed = FALSE;
  self->tracks_failed = FALSE;
  g_mutex_unlock (&self->track_lock);

  self->started = TRUE;
  GST_INFO_OBJECT (self, "ready: ns=%s, relay %s:%u%s (connecting once every "
      "linked pad has a track)", self->namespace_str, self->host, self->port,
      self->relay_path);
  return TRUE;
}

/* -- CMSF content protection (CMSF 4) ------------------------------------- */

/* tenc default_KID is 16 raw bytes; CMSF 4.1.1.2 wants the UUID STRING form,
 * and libmoq validates that syntax on encode (msf.c cp_is_uuid), so the hyphens
 * are mandatory. Returns NULL for anything that is not a 16-byte KID. */
static gchar *
moq_kid_to_uuid (const guint8 *kid, gsize len)
{
  if (!kid || len != 16)
    return NULL;
  return g_strdup_printf (
      "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-"
      "%02x%02x%02x%02x%02x%02x",
      kid[0], kid[1], kid[2], kid[3], kid[4], kid[5], kid[6], kid[7],
      kid[8], kid[9], kid[10], kid[11], kid[12], kid[13], kid[14], kid[15]);
}

static void
moq_cp_entry_clear (MoqCpEntry *e)
{
  g_clear_pointer (&e->ref_id, g_free);
  g_clear_pointer (&e->kid_uuid, g_free);
  g_clear_pointer (&e->scheme, g_free);
  g_clear_pointer (&e->system_id, g_free);
  g_clear_pointer (&e->la_url, g_free);
  g_clear_pointer (&e->cert_url, g_free);
  g_clear_pointer (&e->auth_url, g_free);
  g_clear_pointer (&e->pssh, g_free);
  g_clear_pointer (&e->robustness, g_free);
}

static void
gst_moq_sink_clear_content_protections (GstMoqSink *self)
{
  if (!self->cp_entries)
    return;
  for (guint i = 0; i < self->cp_entries->len; i++)
    moq_cp_entry_clear (&g_array_index (self->cp_entries, MoqCpEntry, i));
  g_array_set_size (self->cp_entries, 0);
}

/* One system from the drm-systems spec: a bare systemID UUID, then optional
 * key=value fields. Appends nothing and returns FALSE if the systemID is
 * missing. Values are taken verbatim -- libmoq carries DRM metadata opaquely
 * and checks only the UUID syntax. */
static gboolean
moq_parse_drm_system (GstMoqSink *self, const gchar *spec, MoqCpEntry *out)
{
  gchar **fields = g_strsplit (spec, ",", -1);
  gboolean ok = FALSE;

  for (guint i = 0; fields[i]; i++) {
    gchar *f = g_strstrip (fields[i]);
    if (*f == '\0')
      continue;
    if (i == 0) {
      out->system_id = g_strdup (f);
      ok = TRUE;
      continue;
    }
    /* Split on the FIRST '=' only: a base64 PSSH is padded with '='. */
    gchar **kv = g_strsplit (f, "=", 2);
    const gchar *k = kv[0] ? g_strstrip (kv[0]) : NULL;
    const gchar *v = kv[1] ? g_strstrip (kv[1]) : NULL;
    if (!k || !v || *v == '\0') {
      GST_WARNING_OBJECT (self, "drm-systems: ignoring field \"%s\" with no "
          "value", f);
    } else if (!g_ascii_strcasecmp (k, "laurl")) {
      g_free (out->la_url); out->la_url = g_strdup (v);
    } else if (!g_ascii_strcasecmp (k, "certurl")) {
      g_free (out->cert_url); out->cert_url = g_strdup (v);
    } else if (!g_ascii_strcasecmp (k, "authurl")) {
      g_free (out->auth_url); out->auth_url = g_strdup (v);
    } else if (!g_ascii_strcasecmp (k, "pssh")) {
      g_free (out->pssh); out->pssh = g_strdup (v);
    } else if (!g_ascii_strcasecmp (k, "robustness")) {
      g_free (out->robustness); out->robustness = g_strdup (v);
    } else {
      GST_WARNING_OBJECT (self, "drm-systems: unknown field \"%s\"", k);
    }
    g_strfreev (kv);
  }
  g_strfreev (fields);
  if (!ok)
    moq_cp_entry_clear (out);
  return ok;
}

/* Build the session's root contentProtections: every protected CMAF pad's
 * (scheme, defaultKID) crossed with every configured DRM system, and record on
 * each pad the refIDs its track must reference.
 *
 * The scheme and the KID come from the init segment the pad is already
 * forwarding -- moq_cmaf_parse_init fills has_cenc/scheme/default_kid, so
 * nothing about the encryption itself has to be configured here. drm-systems
 * supplies only what an fMP4 cannot carry: which DRM systems the stream is
 * offered under, and their licence URLs and PSSH.
 *
 * Runs under track_lock, from gst_moq_sink_commit_tracks, ahead of the pad walk
 * that registers the tracks -- which is why the initial-catalog barrier is a
 * precondition for this working at all: every pad's init segment must already
 * be parsed when the refIDs are derived, and the sender cfg freezes the root
 * entries at attach. Caller holds track_lock. */
static void
gst_moq_sink_build_content_protections (GstMoqSink *self)
{
  gst_moq_sink_clear_content_protections (self);
  if (!self->cp_entries)
    self->cp_entries = g_array_new (FALSE, TRUE, sizeof (MoqCpEntry));

  /* Parse drm-systems once per attach. */
  GArray *systems = g_array_new (FALSE, TRUE, sizeof (MoqCpEntry));
  if (self->drm_systems && *self->drm_systems) {
    gchar **specs = g_strsplit (self->drm_systems, ";", -1);
    for (guint i = 0; specs[i]; i++) {
      if (*g_strstrip (specs[i]) == '\0')
        continue;
      MoqCpEntry sys;
      memset (&sys, 0, sizeof sys);
      if (moq_parse_drm_system (self, specs[i], &sys))
        g_array_append_val (systems, sys);
      else
        GST_WARNING_OBJECT (self, "drm-systems: no systemID in \"%s\"; entry "
            "skipped", specs[i]);
    }
    g_strfreev (specs);
  }

  GList *pads = NULL;
  GST_OBJECT_LOCK (self);
  for (GList *l = GST_ELEMENT (self)->sinkpads; l; l = l->next)
    pads = g_list_prepend (pads, gst_object_ref (l->data));
  GST_OBJECT_UNLOCK (self);
  pads = g_list_reverse (pads);

  guint protected_pads = 0;
  for (GList *l = pads; l; l = l->next) {
    GstMoqSinkPad *p = GST_MOQ_SINK_PAD (l->data);
    g_clear_pointer (&p->pend_cp_ref_ids, g_strfreev);

    if (!p->pend_ready || p->packaging != MOQ_MEDIA_PACKAGING_CMAF ||
        !p->has_init_info || !p->init_info.has_cenc)
      continue;
    protected_pads++;

    gchar *kid = moq_kid_to_uuid (p->init_info.default_kid.data,
        p->init_info.default_kid.len);
    if (!kid) {
      GST_WARNING_OBJECT (p, "encrypted CMAF track with no usable tenc "
          "default_KID (%" G_GSIZE_FORMAT " bytes); publishing it without "
          "contentProtections", (gsize) p->init_info.default_kid.len);
      continue;
    }
    gchar *scheme = g_strndup ((const gchar *) p->init_info.scheme.data,
        p->init_info.scheme.len);

    if (systems->len == 0) {
      GST_WARNING_OBJECT (p, "CMAF track is %s-encrypted (KID %s) but no "
          "drm-systems are configured: the catalog will carry no "
          "contentProtections and a subscriber has no way to obtain a key",
          scheme, kid);
      g_free (kid);
      g_free (scheme);
      continue;
    }

    GPtrArray *refs = g_ptr_array_new ();
    for (guint i = 0; i < systems->len; i++) {
      MoqCpEntry *sys = &g_array_index (systems, MoqCpEntry, i);
      /* Reuse an entry when another pad shares this key and system. */
      MoqCpEntry *found = NULL;
      for (guint j = 0; j < self->cp_entries->len && !found; j++) {
        MoqCpEntry *e = &g_array_index (self->cp_entries, MoqCpEntry, j);
        if (!g_strcmp0 (e->kid_uuid, kid) && !g_strcmp0 (e->scheme, scheme) &&
            !g_strcmp0 (e->system_id, sys->system_id))
          found = e;
      }
      if (!found) {
        MoqCpEntry e;
        memset (&e, 0, sizeof e);
        e.ref_id = g_strdup_printf ("cp%u", self->cp_entries->len);
        e.kid_uuid = g_strdup (kid);
        e.scheme = g_strdup (scheme);
        e.system_id = g_strdup (sys->system_id);
        e.la_url = g_strdup (sys->la_url);
        e.cert_url = g_strdup (sys->cert_url);
        e.auth_url = g_strdup (sys->auth_url);
        e.pssh = g_strdup (sys->pssh);
        e.robustness = g_strdup (sys->robustness);
        g_array_append_val (self->cp_entries, e);
        found = &g_array_index (self->cp_entries, MoqCpEntry,
            self->cp_entries->len - 1);
      }
      g_ptr_array_add (refs, g_strdup (found->ref_id));
    }
    g_ptr_array_add (refs, NULL);
    p->pend_cp_ref_ids = (gchar **) g_ptr_array_free (refs, FALSE);
    GST_INFO_OBJECT (p, "CMAF track is %s-encrypted, KID %s, offered under %u "
        "DRM system(s)", scheme, kid, systems->len);
    g_free (kid);
    g_free (scheme);
  }
  g_list_free_full (pads, gst_object_unref);

  if (protected_pads == 0 && systems->len > 0)
    GST_WARNING_OBJECT (self, "drm-systems is set but no track carries CENC "
        "boxes; the catalog will claim no content protection");

  for (guint i = 0; i < systems->len; i++)
    moq_cp_entry_clear (&g_array_index (systems, MoqCpEntry, i));
  g_array_free (systems, TRUE);
}

/* Flatten cp_entries into the array moq_media_sender_cfg_t wants. libmoq
 * deep-copies every field at attach, so both blocks are freed by the caller
 * straight after. *kids_out holds one single-element defaultKID array per
 * entry -- CMSF allows several KIDs per entry, but one init segment declares
 * exactly one tenc default_KID. */
static moq_cmsf_content_protection_t *
gst_moq_sink_cp_array (GstMoqSink *self, moq_bytes_t **kids_out, size_t *n_out)
{
  guint n = self->cp_entries ? self->cp_entries->len : 0;
  *n_out = n;
  *kids_out = NULL;
  if (n == 0)
    return NULL;

  moq_cmsf_content_protection_t *cps =
      g_new0 (moq_cmsf_content_protection_t, n);
  moq_bytes_t *kids = g_new0 (moq_bytes_t, n);

  for (guint i = 0; i < n; i++) {
    MoqCpEntry *e = &g_array_index (self->cp_entries, MoqCpEntry, i);
    kids[i].data = (const uint8_t *) e->kid_uuid;
    kids[i].len = strlen (e->kid_uuid);

    cps[i].ref_id.data = (const uint8_t *) e->ref_id;
    cps[i].ref_id.len = strlen (e->ref_id);
    cps[i].default_kids = &kids[i];
    cps[i].default_kid_count = 1;
    cps[i].scheme.data = (const uint8_t *) e->scheme;
    cps[i].scheme.len = strlen (e->scheme);
    cps[i].drm_system.system_id.data = (const uint8_t *) e->system_id;
    cps[i].drm_system.system_id.len = strlen (e->system_id);
    if (e->la_url) {
      cps[i].drm_system.la_url.present = true;
      cps[i].drm_system.la_url.url.data = (const uint8_t *) e->la_url;
      cps[i].drm_system.la_url.url.len = strlen (e->la_url);
    }
    if (e->cert_url) {
      cps[i].drm_system.cert_url.present = true;
      cps[i].drm_system.cert_url.url.data = (const uint8_t *) e->cert_url;
      cps[i].drm_system.cert_url.url.len = strlen (e->cert_url);
    }
    if (e->auth_url) {
      cps[i].drm_system.auth_url.present = true;
      cps[i].drm_system.auth_url.url.data = (const uint8_t *) e->auth_url;
      cps[i].drm_system.auth_url.url.len = strlen (e->auth_url);
    }
    if (e->pssh) {
      cps[i].drm_system.has_pssh = true;
      cps[i].drm_system.pssh.data = (const uint8_t *) e->pssh;
      cps[i].drm_system.pssh.len = strlen (e->pssh);
    }
    if (e->robustness) {
      cps[i].drm_system.has_robustness = true;
      cps[i].drm_system.robustness.data = (const uint8_t *) e->robustness;
      cps[i].drm_system.robustness.len = strlen (e->robustness);
    }
  }
  *kids_out = kids;
  return cps;
}

/* Connect the endpoint and attach the media sender. Deferred out of _start so
 * prepared tracks can be attached/added in one posted task before the first
 * sender hook; see gst_moq_sink_await_tracks. A relay that
 * is down is therefore reported on the first buffer rather than on the state
 * change. Caller holds track_lock. */
static gboolean
gst_moq_sink_open_sender (GstMoqSink *self)
{
  gchar *url = g_strdup_printf ("https://%s:%u%s", self->host, self->port,
      self->relay_path);

  moq_endpoint_cfg_t ec;
  moq_endpoint_cfg_init (&ec);
  ec.url.data = (const uint8_t *) url;
  ec.url.len = strlen (url);
  ec.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
  ec.insecure_skip_verify = self->insecure;
  moq_version_t pinned = (moq_version_t) self->draft;
  if (self->draft != 0) {
    ec.versions.struct_size = sizeof ec.versions;
    ec.versions.policy = MOQ_VERSION_POLICY_EXACT;
    ec.versions.versions = &pinned;
    ec.versions.version_count = 1;
  }

  moq_endpoint_t *ep = NULL;
  moq_result_t rc = moq_endpoint_connect (&ec, &ep);
  g_free (url);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not open MoQ endpoint to relay %s:%u (rc=%d)",
            self->host, self->port, (int) rc), (NULL));
    goto fail_ns;
  }

  moq_media_sender_cfg_t sc;
  moq_media_sender_cfg_init_live_sized (&sc, sizeof sc);
  moq_media_sender_callbacks_init_sized (&sc.callbacks, sizeof sc.callbacks);
  GstMoqReadyDiagnostic *diagnostic = g_new0 (GstMoqReadyDiagnostic, 1);
  diagnostic->namespace_str = g_strdup (self->namespace_str);
  sc.callbacks.ctx = diagnostic;
  sc.callbacks.on_ready = gst_moq_sink_on_ready;
  /* Push (contribution) mode: PUBLISH every track and write the catalog live so
   * a relay that expects a publisher-initiated PUBLISH picks the stream up.
   * Off by default -- announce-and-wait is what red5-moq-relay serves today. */
  sc.publish_tracks = self->publish_tracks;
  /* Republish the catalog on a timer only when explicitly requested for
   * relay cache compatibility. Initial catalog completeness is provided by
   * the atomic startup task, not by periodic publication.
   *
   * Set it only for a relay that does not pull that retained group on a late
   * subscriber's behalf. MoQT permits a relay to answer a FETCH from its own
   * cache alone and to be "catalog-blind", and libmoq documents the retained
   * group as "NOT a relay-safe catalog solution" for exactly that reason, so
   * such a relay delivers no catalog at all to a late joiner -- red5-moq-relay
   * 1.6.40/1.7.8 answer the joining FETCH with REQUEST_ERROR 17. The periodic
   * republish sidesteps it by making the catalog ordinary live content, which
   * any relay forwards. */
  sc.catalog_refresh_interval_us =
      self->catalog_refresh_ms ? (guint64) self->catalog_refresh_ms * 1000 : 0;
  sc.endpoint = NULL;
  sc.namespace_.parts = self->ns_bytes;
  sc.namespace_.count = self->ns_count;

  /* CMSF 4 root contentProtections, authored by commit_tracks before the pads
   * were registered (the track cfgs reference these entries by refID). The
   * sender cfg carries them, so they are frozen with the rest of the initial
   * catalog at attach. */
  moq_bytes_t *cp_kids = NULL;
  size_t cp_count = 0;
  moq_cmsf_content_protection_t *cps =
      gst_moq_sink_cp_array (self, &cp_kids, &cp_count);
  sc.content_protections = cps;
  sc.content_protection_count = cp_count;

  SenderStartup task = { 0 };
  g_mutex_init (&task.lock);
  g_cond_init (&task.cond);
  task.cfg = sc;
  task.tracks = self->preparing_tracks;
  rc = moq_endpoint_post (ep, start_sender_task, &task);
  gboolean timed_out = FALSE;
  if (rc == MOQ_OK) {
    gint64 deadline = g_get_monotonic_time () + EOS_DRAIN_TIMEOUT_US;
    g_mutex_lock (&task.lock);
    while (!task.done) {
      GST_OBJECT_LOCK (self);
      gboolean cancelled = self->state_flushing || self->flush_count > 0;
      GST_OBJECT_UNLOCK (self);
      if (cancelled || g_get_monotonic_time () >= deadline) {
        timed_out = !cancelled;
        task.cancelled = TRUE;
        /* A running task only performs finite attach/add calls. Settle it
         * before app-thread destruction; an unstarted task runs closed in stop. */
        while (task.running && !task.done)
          g_cond_wait (&task.cond, &task.lock);
        break;
      }
      g_cond_wait_until (&task.cond, &task.lock,
          MIN (deadline, g_get_monotonic_time () + 5000));
    }
    rc = task.cancelled ? (timed_out ? MOQ_DONE : MOQ_ERR_INTERRUPTED) : task.result;
    g_mutex_unlock (&task.lock);
  }
  moq_media_sender_t *sender = task.sender;
  if (rc != MOQ_OK) {
    if (sender)
      moq_media_sender_destroy (sender);
    /* Completes a queued callback with session==NULL before its ctx expires. */
    moq_endpoint_stop (ep);
    moq_endpoint_destroy (ep);
  } else {
    for (guint i = 0; i < task.tracks->len; i++) {
      PreparedTrack *t = g_ptr_array_index (task.tracks, i);
      GST_OBJECT_LOCK (self);
      t->pad->track = t->track;
      GST_OBJECT_UNLOCK (self);
    }
  }
  g_cond_clear (&task.cond);
  g_mutex_clear (&task.lock);
  /* libmoq deep-copies every contentProtections field at attach, and the wait
   * above has settled the task, so the attach has either happened or been
   * abandoned. Safe to drop the authored arrays either way. */
  g_free (cps);
  g_free (cp_kids);
  if (rc != MOQ_OK) {
    GST_ELEMENT_ERROR (self, RESOURCE, OPEN_WRITE,
        ("could not prepare MoQ sender (rc=%d)%s", (int) rc,
            cp_count ? " -- check drm-systems: a systemID must be a "
            "\"8-4-4-4-12\" UUID string" : ""), (NULL));
    gst_moq_sink_ready_diagnostic_free (diagnostic);
    goto fail_ns;
  }
  if (cp_count)
    GST_INFO_OBJECT (self, "catalog carries %" G_GSIZE_FORMAT
        " contentProtections entr%s", (gsize) cp_count,
        cp_count == 1 ? "y" : "ies");
  g_mutex_lock (&self->send_lock);
  self->sender = sender;
  self->ready_diagnostic = diagnostic;
  self->sender_generation++;
  GST_OBJECT_LOCK (self);
  self->ep = ep;
  moq_endpoint_set_interrupted (ep, self->state_flushing || self->flush_count > 0);
  GST_OBJECT_UNLOCK (self);
  g_mutex_unlock (&self->send_lock);

  GST_INFO_OBJECT (self, "started: publishing ns=%s via relay %s:%u%s",
      self->namespace_str, self->host, self->port, self->relay_path);
  return TRUE;

fail_ns:
  g_clear_pointer (&self->ns_tokens, g_strfreev);
  g_clear_pointer (&self->ns_bytes, g_free);
  self->ns_count = 0;
  return FALSE;
}

static void
gst_moq_sink_stop (GstMoqSink *self)
{
  if (!self->started)
    return;

  g_mutex_lock (&self->send_lock);
  GST_OBJECT_LOCK (self);
  moq_endpoint_t *ep = self->ep;
  self->ep = NULL;
  if (ep)
    moq_endpoint_set_interrupted (ep, TRUE);
  GST_OBJECT_UNLOCK (self);

  if (self->sender) {
    moq_media_sender_stats_t st;
    /* get_stats, destroy and clearing self->sender all happen under one
     * critical section so a concurrent streaming-thread call (which only
     * ever checks self->sender under this same lock) can never observe a
     * freed sender. */
    moq_result_t stats_rc = moq_media_sender_get_stats (self->sender, &st, sizeof st);
    moq_media_sender_destroy (self->sender);
    self->sender = NULL;
    /* detach/destruction settles callbacks before the diagnostic is freed. */
    gst_moq_sink_ready_diagnostic_free (self->ready_diagnostic);
    self->ready_diagnostic = NULL;
    if (stats_rc == MOQ_OK)
      GST_INFO_OBJECT (self, "sender stats: written=%" G_GUINT64_FORMAT
          " sent=%" G_GUINT64_FORMAT " queued=%" G_GUINT64_FORMAT
          " dropped=%" G_GUINT64_FORMAT, st.objects_written, st.objects_sent,
          st.objects_queued, st.objects_dropped);
  }
  g_list_free_full (self->end_requests, g_free);
  self->end_requests = NULL;
  if (ep) {
    moq_endpoint_stop (ep);
    moq_endpoint_destroy (ep);
  }
  g_mutex_unlock (&self->send_lock);

  GST_OBJECT_LOCK (self);
  for (GList *l = GST_ELEMENT (self)->sinkpads; l; l = l->next)
    GST_MOQ_SINK_PAD (l->data)->track = NULL;
  GST_OBJECT_UNLOCK (self);

  g_clear_pointer (&self->ns_tokens, g_strfreev);
  g_clear_pointer (&self->ns_bytes, g_free);
  self->ns_count = 0;
  g_mutex_lock (&self->track_lock);
  gst_moq_sink_clear_content_protections (self);
  g_mutex_unlock (&self->track_lock);
  self->started = FALSE;
  GST_INFO_OBJECT (self, "stopped");
}

static GstStateChangeReturn
gst_moq_sink_change_state (GstElement *element, GstStateChange transition)
{
  GstMoqSink *self = GST_MOQ_SINK (element);

  switch (transition) {
    case GST_STATE_CHANGE_NULL_TO_READY:
      if (!gst_moq_sink_start (self))
        return GST_STATE_CHANGE_FAILURE;
      break;
    case GST_STATE_CHANGE_READY_TO_PAUSED:
      if (!self->started && !gst_moq_sink_start (self))
        return GST_STATE_CHANGE_FAILURE;
      break;
    case GST_STATE_CHANGE_PAUSED_TO_READY:
      GST_OBJECT_LOCK (self);
      self->state_flushing = TRUE;
      for (GList *l = element->sinkpads; l; l = l->next) {
        GstMoqSinkPad *p = GST_MOQ_SINK_PAD (l->data);
        p->flushing = TRUE;
        if (p->clock_id)
          gst_clock_id_unschedule (p->clock_id);
      }
      if (self->ep)
        moq_endpoint_set_interrupted (self->ep, TRUE);
      GST_OBJECT_UNLOCK (self);
      /* Release any pad parked in the track barrier: going to READY must not
       * leave a streaming thread waiting for a track that will never arrive.
       * track_lock is taken OUTSIDE OBJECT_LOCK, so this follows the unlock. */
      g_mutex_lock (&self->track_lock);
      g_cond_broadcast (&self->track_cond);
      g_mutex_unlock (&self->track_lock);
      break;
    default:
      break;
  }

  GstStateChangeReturn ret =
      GST_ELEMENT_CLASS (gst_moq_sink_parent_class)->change_state (element,
      transition);

  switch (transition) {
    case GST_STATE_CHANGE_READY_TO_PAUSED:
      GST_OBJECT_LOCK (self);
      self->state_flushing = FALSE;
      for (GList *l = element->sinkpads; l; l = l->next)
        GST_MOQ_SINK_PAD (l->data)->flushing = GST_MOQ_SINK_PAD (l->data)->flush_pending;
      self->eos_posted = FALSE;
      if (self->ep)
        moq_endpoint_set_interrupted (self->ep, self->flush_count > 0);
      GST_OBJECT_UNLOCK (self);
      break;
    case GST_STATE_CHANGE_PAUSED_TO_READY:
      if (ret != GST_STATE_CHANGE_FAILURE)
        gst_moq_sink_stop (self);
      break;
    case GST_STATE_CHANGE_READY_TO_NULL:
      gst_moq_sink_stop (self);
      break;
    default:
      break;
  }
  return ret;
}

/* -- GObject -------------------------------------------------------------- */

static void
gst_moq_sink_set_property (GObject *object, guint prop_id,
    const GValue *value, GParamSpec *pspec)
{
  GstMoqSink *self = GST_MOQ_SINK (object);
  switch (prop_id) {
    case PROP_HOST:
      g_free (self->host);
      self->host = g_value_dup_string (value);
      break;
    case PROP_PORT:
      self->port = g_value_get_uint (value);
      break;
    case PROP_RELAY_PATH:
      g_free (self->relay_path);
      self->relay_path = g_value_dup_string (value);
      break;
    case PROP_INSECURE:
      self->insecure = g_value_get_boolean (value);
      break;
    case PROP_NAMESPACE:
      g_free (self->namespace_str);
      self->namespace_str = g_value_dup_string (value);
      break;
    case PROP_TRACK_NAME:          /* forwarded to the always pad */
      g_object_set_property (G_OBJECT (self->locpad), "track-name", value);
      break;
    case PROP_CODEC:
      g_free (self->codec);
      self->codec = g_value_dup_string (value);
      break;
    case PROP_BITRATE:             /* forwarded to the always pad */
      g_object_set_property (G_OBJECT (self->locpad), "bitrate", value);
      break;
    case PROP_DRAFT:
      self->draft = g_value_get_uint (value);
      break;
    case PROP_SYNC:
      self->sync = g_value_get_boolean (value);
      break;
    case PROP_SAP_TIMELINE:
      self->sap_timeline = g_value_get_boolean (value);
      break;
    case PROP_PUBLISH_TRACKS:
      self->publish_tracks = g_value_get_boolean (value);
      break;
    case PROP_CATALOG_REFRESH_MS:
      self->catalog_refresh_ms = g_value_get_uint (value);
      break;
    case PROP_CATALOG_WAIT_MS:
      self->catalog_wait_ms = g_value_get_uint (value);
      break;
    case PROP_LOC_AUDIO_GROUP_MS:
      self->loc_audio_group_ms = g_value_get_uint (value);
      break;
    case PROP_DRM_SYSTEMS:
      g_free (self->drm_systems);
      self->drm_systems = g_value_dup_string (value);
      break;
    case PROP_MAX_FRAGMENT_SIZE:
      self->max_fragment_size = g_value_get_uint64 (value);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
  }
}

static void
gst_moq_sink_get_property (GObject *object, guint prop_id,
    GValue *value, GParamSpec *pspec)
{
  GstMoqSink *self = GST_MOQ_SINK (object);
  switch (prop_id) {
    case PROP_HOST:
      g_value_set_string (value, self->host);
      break;
    case PROP_PORT:
      g_value_set_uint (value, self->port);
      break;
    case PROP_RELAY_PATH:
      g_value_set_string (value, self->relay_path);
      break;
    case PROP_INSECURE:
      g_value_set_boolean (value, self->insecure);
      break;
    case PROP_NAMESPACE:
      g_value_set_string (value, self->namespace_str);
      break;
    case PROP_TRACK_NAME:
      g_object_get_property (G_OBJECT (self->locpad), "track-name", value);
      break;
    case PROP_CODEC:
      g_value_set_string (value, self->codec);
      break;
    case PROP_BITRATE:
      g_object_get_property (G_OBJECT (self->locpad), "bitrate", value);
      break;
    case PROP_DRAFT:
      g_value_set_uint (value, self->draft);
      break;
    case PROP_SYNC:
      g_value_set_boolean (value, self->sync);
      break;
    case PROP_SAP_TIMELINE:
      g_value_set_boolean (value, self->sap_timeline);
      break;
    case PROP_PUBLISH_TRACKS:
      g_value_set_boolean (value, self->publish_tracks);
      break;
    case PROP_CATALOG_REFRESH_MS:
      g_value_set_uint (value, self->catalog_refresh_ms);
      break;
    case PROP_CATALOG_WAIT_MS:
      g_value_set_uint (value, self->catalog_wait_ms);
      break;
    case PROP_LOC_AUDIO_GROUP_MS:
      g_value_set_uint (value, self->loc_audio_group_ms);
      break;
    case PROP_DRM_SYSTEMS:
      g_value_set_string (value, self->drm_systems);
      break;
    case PROP_MAX_FRAGMENT_SIZE:
      g_value_set_uint64 (value, self->max_fragment_size);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
  }
}

static void
gst_moq_sink_finalize (GObject *object)
{
  GstMoqSink *self = GST_MOQ_SINK (object);
  g_free (self->host);
  g_free (self->relay_path);
  g_free (self->namespace_str);
  g_free (self->codec);
  g_free (self->drm_systems);
  gst_moq_sink_clear_content_protections (self);
  if (self->cp_entries)
    g_array_free (self->cp_entries, TRUE);
  g_mutex_clear (&self->send_lock);
  g_mutex_clear (&self->track_lock);
  g_cond_clear (&self->track_cond);
  G_OBJECT_CLASS (gst_moq_sink_parent_class)->finalize (object);
}

static void
gst_moq_sink_class_init (GstMoqSinkClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  GstElementClass *element_class = GST_ELEMENT_CLASS (klass);

  GST_DEBUG_CATEGORY_INIT (gst_moq_sink_debug, "moqsink", 0, "MoQ sink");

  gobject_class->set_property = gst_moq_sink_set_property;
  gobject_class->get_property = gst_moq_sink_get_property;
  gobject_class->finalize = gst_moq_sink_finalize;

  g_object_class_install_property (gobject_class, PROP_HOST,
      g_param_spec_string ("host", "Host", "MoQ relay host to connect to",
          DEFAULT_HOST, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_PORT,
      g_param_spec_uint ("port", "Port", "MoQ relay UDP/QUIC port", 1, 65535,
          DEFAULT_PORT, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_RELAY_PATH,
      g_param_spec_string ("relay-path", "Relay path",
          "WebTransport endpoint path on the relay", DEFAULT_RELAY_PATH,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_INSECURE,
      g_param_spec_boolean ("insecure", "Insecure",
          "Skip TLS verification (demo/test only)", FALSE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_NAMESPACE,
      g_param_spec_string ("namespace", "Track namespace",
          "Slash-separated MoQ track namespace", DEFAULT_NAMESPACE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_TRACK_NAME,
      g_param_spec_string ("track-name", "Track name",
          "MoQ track name for the always (LOC) sink pad", DEFAULT_TRACK_NAME,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_CODEC,
      g_param_spec_string ("codec", "Codec",
          "Catalog codec string for the LOC track "
          "(default: derived from the stream's SPS, e.g. avc1.42c01e)", NULL,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_BITRATE,
      g_param_spec_uint64 ("bitrate", "Bitrate",
          "Advertised catalog max bitrate for the LOC track (bits/s)", 1,
          G_MAXUINT64, DEFAULT_BITRATE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_DRAFT,
      g_param_spec_uint ("draft", "MoQT draft",
          "MoQ Transport draft version to negotiate (16 or 18); "
          "0 offers every draft libmoq supports", 0, 18, DEFAULT_DRAFT,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_SYNC,
      g_param_spec_boolean ("sync", "Sync",
          "Pace buffers against the pipeline clock", TRUE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_SAP_TIMELINE,
      g_param_spec_boolean ("sap-timeline", "SAP timeline",
          "Publish a CMSF SAP event timeline track for every CMAF track",
          FALSE, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_PUBLISH_TRACKS,
      g_param_spec_boolean ("publish-tracks", "Publish tracks",
          "Push mode: PUBLISH each track and write the catalog without waiting "
          "for a subscriber. Off means announce and wait for demand.", FALSE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_CATALOG_REFRESH_MS,
      g_param_spec_uint ("catalog-refresh-ms", "Catalog refresh interval",
          "Republish the catalog every N ms. 0 disables it, which is what "
          "MSF-01 5 asks for (republish on track-availability change): the "
          "element already holds the initial catalog until every linked pad "
          "has a track. Needed only for a relay that cannot serve the joining "
          "FETCH that fetches the retained catalog (red5-moq-relay 1.6.40 and "
          "1.7.8): there, keep it under their ~2000 ms park window.",
          0, G_MAXUINT, 0,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_CATALOG_WAIT_MS,
      g_param_spec_uint ("catalog-wait-ms", "Initial catalog wait",
          "How long the first pad to produce a track waits for the remaining "
          "linked pads before reporting an error without opening a connection. Bounds a "
          "linked pad that never delivers; a pad that reaches EOS or flushes "
          "drops out at once.",
          0, G_MAXUINT, DEFAULT_CATALOG_WAIT_MS,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_LOC_AUDIO_GROUP_MS,
      g_param_spec_uint ("loc-audio-group-ms", "LOC audio group length",
          "How often the LOC audio track opens a new group (ms). A subscriber "
          "joins each track at its next group boundary, so this together with "
          "the video GOP decides which track's first object reaches a new "
          "subscriber first.",
          1, G_MAXUINT, LOC_AUDIO_GROUP_US / 1000,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_DRM_SYSTEMS,
      g_param_spec_string ("drm-systems", "DRM systems",
          "DRM systems to advertise for CENC-encrypted CMAF tracks, as the "
          "catalog's CMSF root contentProtections (CMSF 4.1.1). Semicolon "
          "separates systems; within one, the first field is the systemID UUID "
          "and the rest are optional key=value fields: laurl, certurl, authurl, "
          "pssh (base64), robustness. Values may not contain ';' or ','.\n"
          "The scheme and defaultKID are NOT configured here -- they are read "
          "from the init segment's own sinf/schm/schi(tenc) boxes, so this "
          "carries only what an fMP4 cannot: which systems the stream is "
          "offered under and how to reach their licences. One entry is authored "
          "per (scheme, KID, system), and a protected track references every "
          "entry for its key.\n"
          "Example: "
          "\"edef8ba9-79d6-4ace-a3c8-27dcd51d21ed,laurl=https://lic/wv,"
          "pssh=AAAA...;9a04f079-9840-4286-ab92-e65be0885f95,"
          "laurl=https://lic/pr\".\n"
          "LOC tracks are unaffected: LOC carries no CENC metadata. Leaving "
          "this unset while a track IS encrypted publishes ciphertext with no "
          "way to obtain a key, and is warned about.",
          NULL, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_MAX_FRAGMENT_SIZE,
      g_param_spec_uint64 ("max-fragment-size", "Max fragment size",
          "Largest single ISO BMFF box accepted on a CMAF pad (bytes)",
          1024, G_MAXUINT32, DEFAULT_MAX_FRAGMENT,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  gst_element_class_add_static_pad_template_with_gtype (element_class,
      &sink_template, GST_TYPE_MOQ_SINK_PAD);
  gst_element_class_add_static_pad_template_with_gtype (element_class,
      &audio_loc_template, GST_TYPE_MOQ_SINK_PAD);
  gst_element_class_add_static_pad_template_with_gtype (element_class,
      &video_template, GST_TYPE_MOQ_SINK_PAD);
  gst_element_class_add_static_pad_template_with_gtype (element_class,
      &audio_template, GST_TYPE_MOQ_SINK_PAD);
  gst_element_class_set_static_metadata (element_class,
      "MoQ sink", "Sink/Network",
      "Publish LOC and CMAF tracks over Media-over-QUIC (libmoq service tier)",
      "Ray L <ray@raylucke.com>");

  element_class->change_state = GST_DEBUG_FUNCPTR (gst_moq_sink_change_state);
  element_class->send_event = GST_DEBUG_FUNCPTR (gst_moq_sink_send_event);
  element_class->request_new_pad = GST_DEBUG_FUNCPTR (gst_moq_sink_request_new_pad);
  element_class->release_pad = GST_DEBUG_FUNCPTR (gst_moq_sink_release_pad);
}

static void
gst_moq_sink_init (GstMoqSink *self)
{
  self->host = g_strdup (DEFAULT_HOST);
  self->port = DEFAULT_PORT;
  self->relay_path = g_strdup (DEFAULT_RELAY_PATH);
  self->namespace_str = g_strdup (DEFAULT_NAMESPACE);
  self->codec = NULL;
  self->draft = DEFAULT_DRAFT;
  self->sync = TRUE;
  self->max_fragment_size = DEFAULT_MAX_FRAGMENT;
  self->publish_tracks = FALSE;
  self->catalog_refresh_ms = 0;
  self->catalog_wait_ms = DEFAULT_CATALOG_WAIT_MS;
  self->loc_audio_group_ms = LOC_AUDIO_GROUP_US / 1000;
  self->loc_video_group_pts = GST_CLOCK_TIME_NONE;
  self->latency = 0;
  g_mutex_init (&self->send_lock);
  g_mutex_init (&self->track_lock);
  g_cond_init (&self->track_cond);

  GstPadTemplate *templ = gst_static_pad_template_get (&sink_template);
  self->locpad = gst_moq_sink_pad_new (templ, "sink", MOQ_MEDIA_TYPE_VIDEO,
      MOQ_MEDIA_PACKAGING_RAW, DEFAULT_TRACK_NAME, DEFAULT_BITRATE,
      DEFAULT_MAX_FRAGMENT);
  gst_object_unref (templ);
  gst_pad_set_chain_function (GST_PAD (self->locpad), gst_moq_sink_chain);
  gst_pad_set_event_function (GST_PAD (self->locpad), gst_moq_sink_event);
  gst_element_add_pad (GST_ELEMENT (self), GST_PAD (self->locpad));

  templ = gst_static_pad_template_get (&audio_loc_template);
  self->locpad_audio = gst_moq_sink_pad_new (templ, "audio",
      MOQ_MEDIA_TYPE_AUDIO, MOQ_MEDIA_PACKAGING_RAW, DEFAULT_AUDIO_TRACK_NAME,
      DEFAULT_AUDIO_BITRATE, DEFAULT_MAX_FRAGMENT);
  gst_object_unref (templ);
  gst_pad_set_chain_function (GST_PAD (self->locpad_audio), gst_moq_sink_chain);
  gst_pad_set_event_function (GST_PAD (self->locpad_audio), gst_moq_sink_event);
  gst_element_add_pad (GST_ELEMENT (self), GST_PAD (self->locpad_audio));

  GST_OBJECT_FLAG_SET (self, GST_ELEMENT_FLAG_SINK);
}

/* Real element and pad code with portable public-service mocks. The executor
 * mock runs asynchronously; its first simulated sender pump snapshots the
 * track inventory after the posted startup task. No relay or socket is used. */
#include "gstmoqsink.h"

#include <gst/gst.h>
#include <moq/cmaf.h>
#include <moq/endpoint.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

#include <string.h>

/* Portable public-service mocks, as in the lifecycle suite. */
void moq_endpoint_cfg_init (moq_endpoint_cfg_t *c)
{ memset (c, 0, sizeof *c); c->struct_size = sizeof *c; }
void moq_media_track_cfg_init (moq_media_track_cfg_t *c)
{ memset (c, 0, sizeof *c); c->struct_size = sizeof *c; }
void moq_media_sender_cfg_init_live_sized (moq_media_sender_cfg_t *c, size_t n)
{ g_assert_cmpuint (n, ==, sizeof *c); memset (c, 0, n); c->struct_size = n; }
void moq_media_sender_callbacks_init_sized (moq_media_sender_callbacks_t *c, size_t n)
{ g_assert_cmpuint (n, ==, sizeof *c); memset (c, 0, n); c->struct_size = n; }

moq_result_t moq_endpoint_wait (moq_endpoint_t *ep, uint64_t us)
{ (void) ep; (void) us; return MOQ_OK; }
/* -- call log ------------------------------------------------------------- */

typedef enum
{
  OP_MARK,
  OP_CONNECT,
  OP_ATTACH,
  OP_ADD_TRACK,
  OP_WRITE,
} LogOp;

typedef struct
{
  LogOp op;
  gchar *detail;                /* track name, or marker label */
} LogEntry;

static GMutex log_lock;
static GArray *call_log;        /* LogEntry */

/* What the element handed libmoq at attach: the root contentProtections array
 * (CMSF 4.1.1), flattened to strings. */
#define MAX_CP 8
typedef struct
{
  guint  count;
  gchar *ref_id[MAX_CP];
  gchar *kid[MAX_CP];
  gchar *scheme[MAX_CP];
  gchar *system_id[MAX_CP];
  gchar *la_url[MAX_CP];
  gchar *pssh[MAX_CP];
} CpCapture;
static CpCapture attached_cps;

/* track name -> comma-joined contentProtectionRefIDs declared at add_track. */
static GHashTable *track_refs;

static gchar *
span_dup (moq_bytes_t b)
{
  return b.data ? g_strndup ((const gchar *) b.data, b.len) : NULL;
}

static void
capture_reset (void)
{
  for (guint i = 0; i < attached_cps.count; i++) {
    g_clear_pointer (&attached_cps.ref_id[i], g_free);
    g_clear_pointer (&attached_cps.kid[i], g_free);
    g_clear_pointer (&attached_cps.scheme[i], g_free);
    g_clear_pointer (&attached_cps.system_id[i], g_free);
    g_clear_pointer (&attached_cps.la_url[i], g_free);
    g_clear_pointer (&attached_cps.pssh[i], g_free);
  }
  attached_cps.count = 0;
  if (track_refs)
    g_hash_table_remove_all (track_refs);
  else
    track_refs = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);
}

static void
log_reset (void)
{
  g_mutex_lock (&log_lock);
  if (call_log) {
    for (guint i = 0; i < call_log->len; i++)
      g_free (g_array_index (call_log, LogEntry, i).detail);
    g_array_set_size (call_log, 0);
  } else {
    call_log = g_array_new (FALSE, FALSE, sizeof (LogEntry));
  }
  g_mutex_unlock (&log_lock);
}

static void
log_add (LogOp op, const gchar *detail)
{
  LogEntry e = { op, g_strdup (detail) };
  g_mutex_lock (&log_lock);
  g_array_append_val (call_log, e);
  g_mutex_unlock (&log_lock);
}

/* Index of the first entry matching op (and detail, when non-NULL), or -1. */
static gint
log_find (LogOp op, const gchar *detail)
{
  gint found = -1;
  g_mutex_lock (&log_lock);
  for (guint i = 0; i < call_log->len && found < 0; i++) {
    LogEntry *e = &g_array_index (call_log, LogEntry, i);
    if (e->op == op && (!detail || g_strcmp0 (e->detail, detail) == 0))
      found = (gint) i;
  }
  g_mutex_unlock (&log_lock);
  return found;
}

static guint
log_count (LogOp op)
{
  guint n = 0;
  g_mutex_lock (&log_lock);
  for (guint i = 0; i < call_log->len; i++)
    if (g_array_index (call_log, LogEntry, i).op == op)
      n++;
  g_mutex_unlock (&log_lock);
  return n;
}

/* -- libmoq seams (-Wl,--wrap) -------------------------------------------- *
 * Opaque handles the element only ever passes back to us. */
static int fake_endpoint;
static int fake_sender;
static int fake_track_video;
static int fake_track_audio;

static GMutex task_lock;
static GCond task_cond;
static moq_endpoint_task_fn pending_fn;
static void *pending_ctx;
static moq_endpoint_t *pending_ep;
static GThread *task_thread;
static gboolean task_queued, hold_task, hold_running, running_entered;
static gboolean inside_task;
static guint expected_tracks, pump_snapshots, task_closed, added_tracks;
static guint fail_add;
static gboolean fail_attach, fail_post;
static guint64 capture_video, capture_audio;
static guint audio_groups;
static gboolean barrier_waiting;

static void
observe_wait (GstDebugCategory *category, GstDebugLevel level,
    const gchar *file, const gchar *function, gint line, GObject *object,
    GstDebugMessage *message, gpointer data)
{
  (void) category; (void) level; (void) file; (void) function;
  (void) line; (void) object; (void) data;
  if (g_strcmp0 (gst_debug_message_get (message), "waiting for linked track set") == 0) {
    g_mutex_lock (&task_lock);
    barrier_waiting = TRUE;
    g_cond_broadcast (&task_cond);
    g_mutex_unlock (&task_lock);
  }
}

static gpointer
run_posted (gpointer unused)
{
  (void) unused;
  g_mutex_lock (&task_lock);
  moq_endpoint_task_fn fn = pending_fn;
  void *ctx = pending_ctx;
  moq_endpoint_t *ep = pending_ep;
  pending_fn = NULL;
  inside_task = TRUE;
  g_mutex_unlock (&task_lock);
  moq_result_t rc = fn (ep, (moq_session_t *) ep, 0, ctx);
  if (rc == MOQ_OK) {
    g_assert_cmpuint (added_tracks, ==, expected_tracks);
    pump_snapshots++;
  }
  inside_task = FALSE;
  return NULL;
}

moq_result_t
moq_endpoint_post (moq_endpoint_t *ep, moq_endpoint_task_fn fn, void *ctx)
{
  if (fail_post)
    return MOQ_ERR_NOMEM;
  g_mutex_lock (&task_lock);
  pending_fn = fn; pending_ctx = ctx; pending_ep = ep;
  task_queued = TRUE;
  g_cond_broadcast (&task_cond);
  if (!hold_task)
    task_thread = g_thread_new ("mock-network", run_posted, NULL);
  g_mutex_unlock (&task_lock);
  return MOQ_OK; /* Scheduling success is independent of the task result. */
}

moq_result_t moq_endpoint_connect (const moq_endpoint_cfg_t *cfg,
    moq_endpoint_t **out);
moq_result_t
moq_endpoint_connect (const moq_endpoint_cfg_t *cfg,
    moq_endpoint_t **out)
{
  (void) cfg;
  log_add (OP_CONNECT, NULL);
  *out = (moq_endpoint_t *) &fake_endpoint;
  return MOQ_OK;
}

moq_result_t moq_endpoint_stop (moq_endpoint_t *ep);
moq_result_t
moq_endpoint_stop (moq_endpoint_t *ep)
{
  if (task_thread) {
    g_thread_join (task_thread);
    task_thread = NULL;
  }
  if (pending_fn) {
    inside_task = TRUE;
    g_assert_cmpint (pending_fn (ep, NULL, 0, pending_ctx), ==, MOQ_ERR_INTERRUPTED);
    inside_task = FALSE;
    pending_fn = NULL;
    task_closed++;
  }
  return MOQ_OK;
}

void moq_endpoint_destroy (moq_endpoint_t *ep);
void
moq_endpoint_destroy (moq_endpoint_t *ep)
{
  (void) ep;
}

void moq_endpoint_set_interrupted (moq_endpoint_t *ep, bool interrupted);
void
moq_endpoint_set_interrupted (moq_endpoint_t *ep, bool interrupted)
{
  (void) ep;
  (void) interrupted;
}

moq_result_t moq_endpoint_drain (moq_endpoint_t *ep, uint64_t timeout_us);
moq_result_t
moq_endpoint_drain (moq_endpoint_t *ep, uint64_t timeout_us)
{
  (void) ep;
  (void) timeout_us;
  return MOQ_OK;
}

moq_result_t moq_media_sender_attach (moq_endpoint_t *ep,
    const moq_media_sender_cfg_t *cfg, moq_media_sender_t **out);
moq_result_t
moq_media_sender_attach (moq_endpoint_t *ep,
    const moq_media_sender_cfg_t *cfg, moq_media_sender_t **out)
{
  (void) ep;
  g_assert_true (inside_task); /* Fails if attach escapes the managed turn. */
  log_add (OP_ATTACH, NULL);
  /* Capture the authored contentProtections for the /cp/ cases. */
  attached_cps.count = 0;
  for (size_t i = 0; i < cfg->content_protection_count && i < MAX_CP; i++) {
    const moq_cmsf_content_protection_t *cp = &cfg->content_protections[i];
    guint k = attached_cps.count++;
    attached_cps.ref_id[k] = span_dup (cp->ref_id);
    attached_cps.scheme[k] = span_dup (cp->scheme);
    attached_cps.kid[k] = cp->default_kid_count > 0
        ? span_dup (cp->default_kids[0]) : NULL;
    attached_cps.system_id[k] = span_dup (cp->drm_system.system_id);
    attached_cps.la_url[k] = cp->drm_system.la_url.present
        ? span_dup (cp->drm_system.la_url.url) : NULL;
    attached_cps.pssh[k] = cp->drm_system.has_pssh
        ? span_dup (cp->drm_system.pssh) : NULL;
  }
  g_mutex_lock (&task_lock);
  running_entered = TRUE;
  g_cond_broadcast (&task_cond);
  while (hold_running)
    g_cond_wait (&task_cond, &task_lock);
  g_mutex_unlock (&task_lock);
  if (fail_attach)
    return MOQ_ERR_NOMEM;
  *out = (moq_media_sender_t *) &fake_sender;
  return MOQ_OK;
}

moq_result_t moq_media_sender_add_track (moq_media_sender_t *s,
    const moq_media_track_cfg_t *cfg, moq_media_track_t **out);
moq_result_t
moq_media_sender_add_track (moq_media_sender_t *s,
    const moq_media_track_cfg_t *cfg, moq_media_track_t **out)
{
  (void) s;
  added_tracks++;
  if (fail_add == added_tracks)
    return MOQ_ERR_NOMEM;
  gchar *name = g_strndup ((const gchar *) cfg->name.data, cfg->name.len);
  log_add (OP_ADD_TRACK, name);

  GString *refs = g_string_new (NULL);
  for (size_t i = 0; i < cfg->content_protection_ref_id_count; i++) {
    if (i)
      g_string_append_c (refs, ',');
    gchar *r = span_dup (cfg->content_protection_ref_ids[i]);
    g_string_append (refs, r ? r : "");
    g_free (r);
  }
  g_hash_table_replace (track_refs, g_strdup (name),
      g_string_free (refs, FALSE));

  *out = (moq_media_track_t *) (cfg->media_type == MOQ_MEDIA_TYPE_AUDIO
      ? &fake_track_audio : &fake_track_video);
  g_free (name);
  return MOQ_OK;
}

moq_result_t moq_media_sender_write (moq_media_sender_t *s,
    moq_media_track_t *track, const moq_media_send_object_t *obj);
moq_result_t
moq_media_sender_write (moq_media_sender_t *s,
    moq_media_track_t *track, const moq_media_send_object_t *obj)
{
  (void) s;
  if (track == (moq_media_track_t *) &fake_track_audio) {
    capture_audio = obj->capture_time_us;
    if (obj->starts_group) audio_groups++;
  } else {
    capture_video = obj->capture_time_us;
  }
  moq_rcbuf_decref (obj->payload);
  log_add (OP_WRITE, track == (moq_media_track_t *) &fake_track_audio
      ? "audio" : "video");
  return MOQ_OK;
}

moq_result_t moq_media_sender_end_track (moq_media_sender_t *s,
    moq_media_track_t *track);
moq_result_t
moq_media_sender_end_track (moq_media_sender_t *s,
    moq_media_track_t *track)
{
  (void) s;
  (void) track;
  return MOQ_OK;
}

bool moq_media_sender_is_fatal (const moq_media_sender_t *s);
bool
moq_media_sender_is_fatal (const moq_media_sender_t *s)
{
  (void) s;
  return false;
}

uint64_t moq_media_sender_fatal_code (const moq_media_sender_t *s);
uint64_t
moq_media_sender_fatal_code (const moq_media_sender_t *s)
{
  (void) s;
  return 0;
}

moq_result_t moq_media_sender_get_stats (const moq_media_sender_t *s,
    moq_media_sender_stats_t *out, size_t out_size);
moq_result_t
moq_media_sender_get_stats (const moq_media_sender_t *s,
    moq_media_sender_stats_t *out, size_t out_size)
{
  (void) s;
  memset (out, 0, out_size);
  return MOQ_OK;
}

void moq_media_sender_destroy (moq_media_sender_t *s);
void
moq_media_sender_destroy (moq_media_sender_t *s)
{
  (void) s;
}

/* -- fixture -------------------------------------------------------------- */

/* avcC: version 1, profile 0x42, compat 0xc0, level 0x1e. prepare_loc_track's
 * stream-format=avc path needs only these four bytes to be well-formed. */
static const guint8 avcc[] = { 1, 0x42, 0xc0, 0x1e, 0xff, 0xe1 };
/* AudioSpecificConfig: AOT 2 (AAC-LC), 48 kHz, stereo. */
static const guint8 asc[] = { 0x11, 0x90 };

static GstCaps *
video_caps (void)
{
  GstBuffer *cd = gst_buffer_new_memdup (avcc, sizeof avcc);
  GstCaps *caps = gst_caps_new_simple ("video/x-h264",
      "stream-format", G_TYPE_STRING, "avc",
      "alignment", G_TYPE_STRING, "au",
      "width", G_TYPE_INT, 1280,
      "height", G_TYPE_INT, 720,
      "framerate", GST_TYPE_FRACTION, 24, 1,
      "codec_data", GST_TYPE_BUFFER, cd, NULL);
  gst_buffer_unref (cd);
  return caps;
}

static GstCaps *
audio_caps (void)
{
  GstBuffer *cd = gst_buffer_new_memdup (asc, sizeof asc);
  GstCaps *caps = gst_caps_new_simple ("audio/mpeg",
      "mpegversion", G_TYPE_INT, 4,
      "stream-format", G_TYPE_STRING, "raw",
      "rate", G_TYPE_INT, 48000,
      "channels", G_TYPE_INT, 2,
      "codec_data", GST_TYPE_BUFFER, cd, NULL);
  gst_buffer_unref (cd);
  return caps;
}

/* A src pad linked to `name` on `sink`, activated, with the sticky events a
 * chain function is entitled to assume. */
static GstPad *
open_src_pad (GstElement *sink, const gchar *name, const gchar *stream_id,
    GstCaps *caps)
{
  GstPad *src = gst_pad_new (NULL, GST_PAD_SRC);
  GstPad *peer = gst_element_get_static_pad (sink, name);
  g_assert_nonnull (peer);
  g_assert_cmpint (gst_pad_link (src, peer), ==, GST_PAD_LINK_OK);

  gst_pad_set_active (src, TRUE);
  g_assert_true (gst_pad_push_event (src, gst_event_new_stream_start (stream_id)));
  gst_pad_push_event (src, gst_event_new_caps (caps));
  gst_caps_unref (caps);
  /* gst_pad_push_event returns TRUE for a caps event the peer REJECTED -- it
   * only marks the sticky event pending -- so check negotiation explicitly.
   * Without this a caps mismatch surfaces later as a confusing flow error from
   * the chain function instead of a failure here. */
  g_assert_true (gst_pad_has_current_caps (peer));

  GstSegment seg;
  gst_segment_init (&seg, GST_FORMAT_TIME);
  g_assert_true (gst_pad_push_event (src, gst_event_new_segment (&seg)));
  gst_object_unref (peer);
  return src;
}

/* A keyframe (no DELTA_UNIT flag) at `pts`. Payload content is irrelevant on
 * the stream-format=avc path -- the avcC comes from the caps. */
static GstBuffer *
keyframe (GstClockTime pts, gsize len)
{
  GstBuffer *b = gst_buffer_new_allocate (NULL, len, NULL);
  gst_buffer_memset (b, 0, 0, len);
  GST_BUFFER_PTS (b) = pts;
  GST_BUFFER_DTS (b) = pts;
  return b;
}

static GstElement *
new_sink (void)
{
  GstElement *sink = g_object_new (GST_TYPE_MOQ_SINK,
      /* Never dialled: moq_endpoint_connect is wrapped. */
      "host", "127.0.0.1", "port", 1,
      /* No clock waits -- the barrier is what this test times, nothing else. */
      "sync", FALSE,
      NULL);
  g_assert_nonnull (sink);
  return sink;
}

/* -- the blocked audio push ---------------------------------------------- */

typedef struct
{
  GstPad *src;
  GstBuffer *buffer;
  GstFlowReturn ret;
  GMutex lock;
  GCond cond;
  gboolean entered;
  gboolean done;
} PushJob;

static gpointer
push_thread (gpointer data)
{
  PushJob *j = data;
  g_mutex_lock (&j->lock);
  j->entered = TRUE;
  g_cond_broadcast (&j->cond);
  g_mutex_unlock (&j->lock);

  GstFlowReturn ret = gst_pad_push (j->src, j->buffer);

  g_mutex_lock (&j->lock);
  j->ret = ret;
  j->done = TRUE;
  g_cond_broadcast (&j->cond);
  g_mutex_unlock (&j->lock);
  return NULL;
}

/* -- tests ---------------------------------------------------------------- */

/* Audio's first buffer arrives first and must NOT open the session: the
 * catalog cannot be published until the video pad has produced its track too.
 */
static void
test_initial_catalog_complete (void)
{
  log_reset ();
  expected_tracks = 2;
  added_tracks = 0;
  GstElement *sink = new_sink ();

  /* PAUSED first: the element activates its sink pads on READY->PAUSED, and an
   * inactive pad rejects the sticky events open_src_pad pushes. */
  g_assert_cmpint (gst_element_set_state (sink, GST_STATE_PAUSED), !=,
      GST_STATE_CHANGE_FAILURE);

  GstPad *asrc = open_src_pad (sink, "audio", "audio-stream", audio_caps ());
  GstPad *vsrc = open_src_pad (sink, "sink", "video-stream", video_caps ());

  /* Audio first, on its own streaming thread: it must park in the barrier. */
  PushJob job;
  memset (&job, 0, sizeof job);
  job.src = asrc;
  job.buffer = keyframe (0, 64);
  job.ret = GST_FLOW_OK;
  g_mutex_init (&job.lock);
  g_cond_init (&job.cond);
  GThread *t = g_thread_new ("audio-push", push_thread, &job);

  g_mutex_lock (&job.lock);
  while (!job.entered)
    g_cond_wait (&job.cond, &job.lock);
  /* SECONDARY, bounded: the push must still be in flight. A barrier that
   * released on one pad would have completed it by now. */
  gint64 deadline = g_get_monotonic_time () + 250 * G_TIME_SPAN_MILLISECOND;
  while (!job.done && g_cond_wait_until (&job.cond, &job.lock, deadline))
    ;
  gboolean released_early = job.done;
  g_mutex_unlock (&job.lock);
  g_assert_false (released_early);

  /* PRIMARY: everything the element does from here on is logged after this
   * marker, so an early connect is an ordering violation, not a race. */
  log_add (OP_MARK, "push-video");
  g_assert_cmpint (gst_pad_push (vsrc, keyframe (0, 64)), ==, GST_FLOW_OK);

  g_thread_join (t);
  g_assert_cmpint (job.ret, ==, GST_FLOW_OK);

  gint mark = log_find (OP_MARK, "push-video");
  gint connect = log_find (OP_CONNECT, NULL);
  gint attach = log_find (OP_ATTACH, NULL);
  g_assert_cmpint (mark, >=, 0);
  g_assert_cmpint (connect, >, mark);
  g_assert_cmpint (attach, >, connect);

  /* One session, both tracks, and both added before anything is written: the
   * session cannot reach ESTABLISHED with a partial track set. */
  g_assert_cmpuint (log_count (OP_CONNECT), ==, 1);
  g_assert_cmpuint (log_count (OP_ATTACH), ==, 1);
  g_assert_cmpuint (log_count (OP_ADD_TRACK), ==, 2);

  gint audio_track = log_find (OP_ADD_TRACK, "audio");
  gint video_track = log_find (OP_ADD_TRACK, "video");
  g_assert_cmpint (audio_track, >, attach);
  g_assert_cmpint (video_track, >, attach);

  gint first_write = log_find (OP_WRITE, NULL);
  g_assert_cmpint (first_write, >, audio_track);
  g_assert_cmpint (first_write, >, video_track);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_pad_set_active (asrc, FALSE);
  gst_pad_set_active (vsrc, FALSE);
  gst_object_unref (asrc);
  gst_object_unref (vsrc);
  gst_object_unref (sink);
  g_mutex_clear (&job.lock);
  g_cond_clear (&job.cond);
}

/* The always pads exist even when a pipeline feeds only one of them. An
 * unlinked pad must not hold the barrier shut -- and this is what keeps the
 * ordering assertion above honest: here CONNECT *does* land on the first
 * buffer, so the oracle can observe an early connect when one is correct.
 */
static void
test_unlinked_pad_excluded (void)
{
  log_reset ();
  expected_tracks = 1;
  added_tracks = 0;
  GstElement *sink = new_sink ();

  g_assert_cmpint (gst_element_set_state (sink, GST_STATE_PAUSED), !=,
      GST_STATE_CHANGE_FAILURE);

  /* "audio" deliberately left unlinked. */
  GstPad *vsrc = open_src_pad (sink, "sink", "video-stream", video_caps ());

  log_add (OP_MARK, "push-video");
  g_assert_cmpint (gst_pad_push (vsrc, keyframe (0, 64)), ==, GST_FLOW_OK);

  gint mark = log_find (OP_MARK, "push-video");
  gint connect = log_find (OP_CONNECT, NULL);
  g_assert_cmpint (connect, >, mark);
  g_assert_cmpuint (log_count (OP_CONNECT), ==, 1);
  g_assert_cmpuint (log_count (OP_ADD_TRACK), ==, 1);
  g_assert_cmpint (log_find (OP_ADD_TRACK, "video"), >=, 0);
  g_assert_cmpint (log_find (OP_ADD_TRACK, "audio"), ==, -1);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_pad_set_active (vsrc, FALSE);
  gst_object_unref (vsrc);
  gst_object_unref (sink);
}

static void
close_source (GstPad *src)
{
  gst_pad_set_active (src, FALSE);
  gst_object_unref (src);
}

static void
test_timeout (void)
{
  log_reset (); added_tracks = 0;
  GstElement *sink = new_sink ();
  g_object_set (sink, "catalog-wait-ms", 10, NULL);
  GstBus *bus = gst_bus_new ();
  gst_element_set_bus (sink, bus);
  gst_element_set_state (sink, GST_STATE_PAUSED);
  GstPad *asrc = open_src_pad (sink, "audio", "a", audio_caps ());
  GstPad *vsrc = open_src_pad (sink, "sink", "v", video_caps ());
  g_assert_cmpint (gst_pad_push (asrc, keyframe (0, 64)), ==, GST_FLOW_ERROR);
  g_assert_cmpuint (log_count (OP_CONNECT), ==, 0);
  g_assert_cmpuint (log_count (OP_WRITE), ==, 0);
  GstMessage *m = gst_bus_pop_filtered (bus, GST_MESSAGE_ERROR);
  g_assert_nonnull (m); gst_message_unref (m);
  gst_element_set_state (sink, GST_STATE_NULL);
  close_source (asrc); close_source (vsrc);
  gst_element_set_bus (sink, NULL); gst_object_unref (bus); gst_object_unref (sink);
}

static void
test_empty (void)
{
  log_reset ();
  GstElement *sink = new_sink ();
  GstBus *bus = gst_bus_new (); gst_element_set_bus (sink, bus);
  gst_element_set_state (sink, GST_STATE_PAUSED);
  GstPad *src = open_src_pad (sink, "sink", "v", video_caps ());
  g_assert_true (gst_pad_push_event (src, gst_event_new_eos ()));
  g_assert_cmpuint (log_count (OP_CONNECT), ==, 0);
  GstMessage *m = gst_bus_pop_filtered (bus, GST_MESSAGE_EOS | GST_MESSAGE_ERROR);
  g_assert_nonnull (m); g_assert_cmpint (GST_MESSAGE_TYPE (m), ==, GST_MESSAGE_EOS);
  gst_message_unref (m);
  gst_element_set_state (sink, GST_STATE_NULL); close_source (src);
  gst_element_set_bus (sink, NULL); gst_object_unref (bus); gst_object_unref (sink);
}

static void
init_job (PushJob *job, GstPad *src)
{
  memset (job, 0, sizeof *job);
  job->src = src; job->buffer = keyframe (0, 64);
  g_mutex_init (&job->lock); g_cond_init (&job->cond);
}

static void
join_job (PushJob *job, GThread *thread, GstFlowReturn ret)
{
  g_thread_join (thread);
  g_assert_cmpint (job->ret, ==, ret);
  g_mutex_clear (&job->lock); g_cond_clear (&job->cond);
}

static void
test_flush (gconstpointer state_change)
{
  barrier_waiting = FALSE;
  gst_debug_add_log_function (observe_wait, NULL, NULL);
  gst_debug_set_threshold_for_name ("moqsink", GST_LEVEL_DEBUG);
  log_reset (); task_queued = FALSE; added_tracks = 0;
  GstElement *sink = new_sink ();
  g_object_set (sink, "catalog-wait-ms", 5000, NULL);
  gst_element_set_state (sink, GST_STATE_PAUSED);
  GstPad *asrc = open_src_pad (sink, "audio", "a", audio_caps ());
  GstPad *vsrc = open_src_pad (sink, "sink", "v", video_caps ());
  PushJob job; init_job (&job, asrc);
  GThread *thread = g_thread_new ("push", push_thread, &job);
  g_mutex_lock (&task_lock);
  gint64 wait_limit = g_get_monotonic_time () + G_TIME_SPAN_SECOND;
  while (!barrier_waiting && g_cond_wait_until (&task_cond, &task_lock, wait_limit)) ;
  g_assert_true (barrier_waiting);
  g_mutex_unlock (&task_lock);
  if (GPOINTER_TO_INT (state_change))
    g_assert_cmpint (gst_element_set_state (sink, GST_STATE_READY), !=, GST_STATE_CHANGE_FAILURE);
  else
    g_assert_true (gst_pad_push_event (asrc, gst_event_new_flush_start ()));
  g_mutex_lock (&job.lock);
  gint64 limit = g_get_monotonic_time () + G_TIME_SPAN_SECOND;
  while (!job.done && g_cond_wait_until (&job.cond, &job.lock, limit)) ;
  g_assert_true (job.done);
  g_mutex_unlock (&job.lock);
  join_job (&job, thread, GST_FLOW_FLUSHING);
  g_assert_cmpuint (log_count (OP_CONNECT), ==, 0);
  gst_element_set_state (sink, GST_STATE_NULL);
  close_source (asrc); close_source (vsrc); gst_object_unref (sink);
  gst_debug_remove_log_function (observe_wait);
  gst_debug_unset_threshold_for_name ("moqsink");
}

static void
test_start_failure (gconstpointer mode_ptr)
{
  guint mode = GPOINTER_TO_UINT (mode_ptr);
  log_reset (); task_queued = FALSE; added_tracks = 0; expected_tracks = 1;
  hold_task = mode == 0; fail_post = mode == 1; fail_attach = mode == 2;
  fail_add = mode == 3 ? 1 : 0;
  GstElement *sink = new_sink (); gst_element_set_state (sink, GST_STATE_PAUSED);
  GstPad *src = open_src_pad (sink, "sink", "v", video_caps ());
  if (hold_task) {
    PushJob job; init_job (&job, src);
    GThread *thread = g_thread_new ("push", push_thread, &job);
    g_mutex_lock (&task_lock);
    gint64 limit = g_get_monotonic_time () + G_TIME_SPAN_SECOND;
    while (!task_queued && g_cond_wait_until (&task_cond, &task_lock, limit)) ;
    g_assert_true (task_queued);
    g_mutex_unlock (&task_lock);
    g_assert_true (gst_pad_push_event (src, gst_event_new_flush_start ()));
    join_job (&job, thread, GST_FLOW_FLUSHING);
    g_assert_cmpuint (task_closed, ==, 1);
    g_assert_cmpuint (log_count (OP_ATTACH), ==, 0);
  } else {
    g_assert_cmpint (gst_pad_push (src, keyframe (0, 64)), ==, GST_FLOW_ERROR);
  }
  g_assert_cmpuint (log_count (OP_WRITE), ==, 0);
  gst_element_set_state (sink, GST_STATE_NULL); close_source (src); gst_object_unref (sink);
  hold_task = fail_post = fail_attach = FALSE; fail_add = task_closed = 0;
}

static gpointer
release_running_on_flush (gpointer data)
{
  GstPad *sinkpad = data;
  /* Public flushing flag is set by FLUSH_START before invoking the handler. */
  gint64 limit = g_get_monotonic_time () + G_TIME_SPAN_SECOND;
  while (!GST_PAD_IS_FLUSHING (sinkpad) && g_get_monotonic_time () < limit)
    g_thread_yield ();
  g_assert_true (GST_PAD_IS_FLUSHING (sinkpad));
  g_mutex_lock (&task_lock); hold_running = FALSE;
  g_cond_broadcast (&task_cond); g_mutex_unlock (&task_lock);
  return NULL;
}

static void
test_running_cancel (void)
{
  log_reset (); added_tracks = 0; expected_tracks = 1; running_entered = FALSE;
  hold_running = TRUE;
  GstElement *sink = new_sink (); gst_element_set_state (sink, GST_STATE_PAUSED);
  GstPad *src = open_src_pad (sink, "sink", "v", video_caps ());
  PushJob job; init_job (&job, src);
  GThread *thread = g_thread_new ("push", push_thread, &job);
  g_mutex_lock (&task_lock);
  gint64 limit = g_get_monotonic_time () + G_TIME_SPAN_SECOND;
  while (!running_entered && g_cond_wait_until (&task_cond, &task_lock, limit)) ;
  g_assert_true (running_entered); g_mutex_unlock (&task_lock);
  GstPad *sinkpad = gst_element_get_static_pad (sink, "sink");
  GThread *release = g_thread_new ("release-task", release_running_on_flush, sinkpad);
  g_assert_true (gst_pad_push_event (src, gst_event_new_flush_start ()));
  g_thread_join (release); gst_object_unref (sinkpad);
  join_job (&job, thread, GST_FLOW_FLUSHING);
  g_assert_cmpuint (log_count (OP_WRITE), ==, 0);
  gst_element_set_state (sink, GST_STATE_NULL); close_source (src); gst_object_unref (sink);
}

static void
test_partial_add_failure (void)
{
  log_reset (); added_tracks = 0; expected_tracks = 2; fail_add = 2;
  GstElement *sink = new_sink (); gst_element_set_state (sink, GST_STATE_PAUSED);
  GstPad *asrc = open_src_pad (sink, "audio", "a", audio_caps ());
  GstPad *vsrc = open_src_pad (sink, "sink", "v", video_caps ());
  PushJob job; init_job (&job, asrc);
  GThread *thread = g_thread_new ("push", push_thread, &job);
  g_assert_cmpint (gst_pad_push (vsrc, keyframe (0, 64)), ==, GST_FLOW_ERROR);
  join_job (&job, thread, GST_FLOW_ERROR);
  g_assert_cmpuint (added_tracks, ==, 2);
  g_assert_cmpuint (log_count (OP_WRITE), ==, 0);
  gst_element_set_state (sink, GST_STATE_NULL);
  close_source (asrc); close_source (vsrc); gst_object_unref (sink); fail_add = 0;
}

static void
test_bad_audio_config (gconstpointer mode_ptr)
{
  guint mode = GPOINTER_TO_UINT (mode_ptr);
  log_reset ();
  GstElement *sink = new_sink (); gst_element_set_state (sink, GST_STATE_PAUSED);
  GstCaps *caps = audio_caps ();
  guint8 config[] = {mode == 1 ? 0x2a : 0x11, 0x90};
  GstBuffer *cd = gst_buffer_new_memdup (config, mode == 0 ? 1 : 2);
  gst_caps_set_simple (caps, "codec_data", GST_TYPE_BUFFER, cd, NULL);
  if (mode == 2) gst_caps_set_simple (caps, "rate", G_TYPE_INT, 44100, NULL);
  gst_buffer_unref (cd);
  GstPad *src = open_src_pad (sink, "audio", "a", caps);
  g_assert_cmpint (gst_pad_push (src, keyframe (0, 64)), ==, GST_FLOW_ERROR);
  g_assert_cmpuint (log_count (OP_CONNECT), ==, 0);
  gst_element_set_state (sink, GST_STATE_NULL); close_source (src); gst_object_unref (sink);
}

static void
push_segment (GstPad *src, GstClockTime offset)
{
  GstSegment seg; gst_segment_init (&seg, GST_FORMAT_TIME);
  seg.start = seg.position = offset;
  g_assert_true (gst_pad_push_event (src, gst_event_new_segment (&seg)));
}

static void
test_timestamps_and_groups (void)
{
  log_reset (); added_tracks = 0; expected_tracks = 2; audio_groups = 0;
  GstElement *sink = new_sink (); gst_element_set_state (sink, GST_STATE_PAUSED);
  GstPad *asrc = open_src_pad (sink, "audio", "a", audio_caps ());
  GstPad *vsrc = open_src_pad (sink, "sink", "v", video_caps ());
  /* Common encoder offset: video PTS is large but running time is zero. */
  GstClockTime offset = 3600000 * GST_SECOND;
  push_segment (vsrc, offset);
  PushJob job; init_job (&job, asrc);
  GThread *thread = g_thread_new ("push", push_thread, &job);
  g_assert_cmpint (gst_pad_push (vsrc, keyframe (offset, 64)), ==, GST_FLOW_OK);
  join_job (&job, thread, GST_FLOW_OK);
  g_assert_cmpuint (capture_video, ==, capture_audio);
  guint before = audio_groups;
  g_assert_cmpint (gst_pad_push (asrc, keyframe (20 * GST_MSECOND, 64)), ==, GST_FLOW_OK);
  guint after = audio_groups;
  g_assert_cmpint (gst_pad_push (asrc, keyframe (40 * GST_MSECOND, 64)), ==, GST_FLOW_OK);
  g_assert_cmpuint (audio_groups, ==, after); /* no group for every AAC AU */
  g_assert_cmpuint (after, <=, before + 1);
  g_assert_cmpint (gst_pad_push (vsrc, keyframe (offset + 100 * GST_MSECOND, 64)), ==, GST_FLOW_OK);
  g_assert_cmpint (gst_pad_push (asrc, keyframe (120 * GST_MSECOND, 64)), ==, GST_FLOW_OK);
  g_assert_cmpuint (audio_groups, ==, after + 1);
  gst_element_set_state (sink, GST_STATE_NULL);
  close_source (asrc); close_source (vsrc); gst_object_unref (sink);
}

/* -- CMSF contentProtections (CMSF 4.1.1) --------------------------------- */

/* A CMAF init segment, optionally CENC-protected:
 *   ftyp + moov(trak(tkhd mdia(mdhd minf(stbl(stsd(
 *       <avc1|encv>(VisualSampleEntry avcC [sinf(frma schm schi(tenc))]))))))
 * Layout mirrors libmoq's own cmaf parser expectations; the first assertion of
 * every test below is that libmoq parses this back, so a wrong fixture fails
 * loudly rather than quietly weakening the test. */
static const guint8 fixture_avcc[] = { 0x01, 0x42, 0xe0, 0x1e };

static gsize
box_hdr (guint8 *o, guint32 size, const gchar type[4])
{
  GST_WRITE_UINT32_BE (o, size);
  memcpy (o + 4, type, 4);
  return 8;
}

static gsize
build_cmaf_init (guint8 *out, guint32 timescale, guint16 w, guint16 h,
    gboolean protected_, const gchar scheme[4], const guint8 kid[16])
{
  gsize avcc_box = 8 + sizeof fixture_avcc;
  /* ISO/IEC 23001-7 8.2.2 TrackEncryptionBox body: version/flags(4) +
   * reserved(1) + reserved-or-pattern(1) + default_isProtected(1) +
   * default_Per_Sample_IV_Size(1) + default_KID(16) = 24. TWO bytes precede
   * isProtected, not one -- a fixture that writes 23 shifts the KID by a byte
   * and silently agrees with a parser that reads it one byte early. */
  gsize tenc_box = protected_ ? 8 + 24 : 0;
  gsize schi_box = protected_ ? 8 + tenc_box : 0;
  gsize schm_box = protected_ ? 20 : 0;          /* hdr + v/f + 4cc + version */
  gsize frma_box = protected_ ? 12 : 0;          /* hdr + 4cc */
  gsize sinf_box = protected_ ? 8 + frma_box + schm_box + schi_box : 0;
  gsize entry_box = 8 + 78 + avcc_box + sinf_box;
  gsize stsd_box = 8 + 8 + entry_box;
  gsize stbl_box = 8 + stsd_box;
  gsize minf_box = 8 + stbl_box;
  gsize mdhd_box = 28;
  gsize mdia_box = 8 + mdhd_box + minf_box;
  gsize tkhd_box = 24;
  gsize trak_box = 8 + tkhd_box + mdia_box;
  gsize moov_box = 8 + trak_box;
  gsize p = 0;

  p += box_hdr (out + p, 20, "ftyp");
  memcpy (out + p, "isom", 4); p += 4;
  GST_WRITE_UINT32_BE (out + p, 0); p += 4;
  memcpy (out + p, "isom", 4); p += 4;

  p += box_hdr (out + p, moov_box, "moov");
  p += box_hdr (out + p, trak_box, "trak");
  p += box_hdr (out + p, tkhd_box, "tkhd");
  memset (out + p, 0, 12); p += 12;              /* v/f + creation + modification */
  GST_WRITE_UINT32_BE (out + p, 1); p += 4;      /* track_ID */

  p += box_hdr (out + p, mdia_box, "mdia");
  p += box_hdr (out + p, mdhd_box, "mdhd");
  memset (out + p, 0, 12); p += 12;              /* v/f + creation + modification */
  GST_WRITE_UINT32_BE (out + p, timescale); p += 4;
  GST_WRITE_UINT32_BE (out + p, 0); p += 4;      /* duration */

  p += box_hdr (out + p, minf_box, "minf");
  p += box_hdr (out + p, stbl_box, "stbl");
  p += box_hdr (out + p, stsd_box, "stsd");
  GST_WRITE_UINT32_BE (out + p, 0); p += 4;      /* v/f */
  GST_WRITE_UINT32_BE (out + p, 1); p += 4;      /* entry_count */

  p += box_hdr (out + p, entry_box, protected_ ? "encv" : "avc1");
  memset (out + p, 0, 78);
  GST_WRITE_UINT16_BE (out + p + 24, w);
  GST_WRITE_UINT16_BE (out + p + 26, h);
  p += 78;

  p += box_hdr (out + p, avcc_box, "avcC");
  memcpy (out + p, fixture_avcc, sizeof fixture_avcc);
  p += sizeof fixture_avcc;

  if (protected_) {
    p += box_hdr (out + p, sinf_box, "sinf");
    p += box_hdr (out + p, frma_box, "frma");
    memcpy (out + p, "avc1", 4); p += 4;         /* original format */
    p += box_hdr (out + p, schm_box, "schm");
    GST_WRITE_UINT32_BE (out + p, 0); p += 4;    /* v/f */
    memcpy (out + p, scheme, 4); p += 4;         /* scheme_type */
    GST_WRITE_UINT32_BE (out + p, 0); p += 4;    /* scheme_version */
    p += box_hdr (out + p, schi_box, "schi");
    p += box_hdr (out + p, tenc_box, "tenc");
    GST_WRITE_UINT32_BE (out + p, 0); p += 4;    /* v/f (version 0) */
    out[p++] = 0;                                /* reserved */
    out[p++] = 0;                                /* reserved (pattern in v1) */
    out[p++] = 1;                                /* default_isProtected */
    out[p++] = 8;                                /* default_Per_Sample_IV_Size */
    memcpy (out + p, kid, 16); p += 16;          /* default_KID */
  }
  return p;
}

/* moof(mfhd traf(tfhd tfdt trun)) + mdat. The splitter only emits the INIT
 * unit once a moof arrives (an init segment alone is still accumulating), so a
 * CMAF pad needs one chunk before the barrier can see its track. */
static gsize
build_cmaf_chunk (guint8 *out, guint32 track_id, guint32 first_sample_flags)
{
  /* FullBox with a single payload word, as the CMAF fragment parser expects. */
  gsize p = 0, traf_inner = 0;
  guint8 inner[64];
  gsize i = 0;
  i += box_hdr (inner + i, 16, "tfhd");
  GST_WRITE_UINT32_BE (inner + i, 0); i += 4;        /* version + flags */
  GST_WRITE_UINT32_BE (inner + i, track_id); i += 4;
  i += box_hdr (inner + i, 16, "tfdt");
  GST_WRITE_UINT32_BE (inner + i, 0); i += 4;
  GST_WRITE_UINT32_BE (inner + i, 0); i += 4;        /* baseMediaDecodeTime */
  i += box_hdr (inner + i, 20, "trun");
  GST_WRITE_UINT32_BE (inner + i, 0x00000400); i += 4;  /* first-sample-flags */
  GST_WRITE_UINT32_BE (inner + i, 1); i += 4;           /* sample_count */
  GST_WRITE_UINT32_BE (inner + i, first_sample_flags); i += 4;
  traf_inner = i;

  gsize moof_body = 16 /* mfhd */ + 8 + traf_inner;
  p += box_hdr (out + p, 8 + moof_body, "moof");
  p += box_hdr (out + p, 16, "mfhd");
  GST_WRITE_UINT32_BE (out + p, 0); p += 4;
  GST_WRITE_UINT32_BE (out + p, 1); p += 4;          /* sequence_number */
  p += box_hdr (out + p, 8 + traf_inner, "traf");
  memcpy (out + p, inner, traf_inner); p += traf_inner;
  p += box_hdr (out + p, 9, "mdat");
  out[p++] = 0x00;
  return p;
}

static const guint8 fixture_kid[16] = {
  0x10, 0x77, 0xef, 0xec, 0xc0, 0xb2, 0x4d, 0x02,
  0xac, 0xe3, 0x3c, 0x1e, 0x52, 0xe2, 0xfb, 0x4b
};
#define FIXTURE_KID_UUID "1077efec-c0b2-4d02-ace3-3c1e52e2fb4b"
#define WIDEVINE_UUID    "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
#define PLAYREADY_UUID   "9a04f079-9840-4286-ab92-e65be0885f95"

/* Drive one CMAF request pad with an init segment and let the barrier commit.
 * Returns the init length actually built. */
static void
push_cmaf_init (GstElement *sink, gboolean protected_, const gchar scheme[4])
{
  guint8 init[1024];
  gsize len = build_cmaf_init (init, 90000, 1920, 1080, protected_, scheme,
      fixture_kid);
  g_assert_cmpuint (len, <, sizeof init);

  /* The fixture must be what libmoq thinks it is, or nothing below means
   * anything. */
  moq_cmaf_init_info_t probe;
  moq_cmaf_init_info_init (&probe);
  g_assert_cmpint (moq_cmaf_parse_init ((moq_bytes_t) { init, len }, &probe),
      ==, MOQ_OK);
  g_assert_true (probe.has_cenc == (protected_ ? true : false));
  if (protected_) {
    g_assert_cmpuint (probe.default_kid.len, ==, 16);
    g_assert_cmpint (memcmp (probe.default_kid.data, fixture_kid, 16), ==, 0);
    g_assert_cmpint (memcmp (probe.scheme.data, scheme, 4), ==, 0);
  }

  /* One video pad, so one track reaches the startup task; run_posted asserts
   * this count, and the barrier cases above leave their own totals behind. */
  added_tracks = 0;
  expected_tracks = 1;

  g_assert_cmpint (gst_element_set_state (sink, GST_STATE_PAUSED), !=,
      GST_STATE_CHANGE_FAILURE);

  /* A video_%u request pad defaults its track-name to "video". */
  GstPad *peer = gst_element_request_pad_simple (sink, "video_%u");
  g_assert_nonnull (peer);

  GstPad *src = gst_pad_new (NULL, GST_PAD_SRC);
  g_assert_cmpint (gst_pad_link (src, peer), ==, GST_PAD_LINK_OK);
  gst_pad_set_active (src, TRUE);
  g_assert_true (gst_pad_push_event (src,
          gst_event_new_stream_start ("cmaf-stream")));
  gst_pad_push_event (src,
      gst_event_new_caps (gst_caps_new_empty_simple ("video/quicktime")));
  g_assert_true (gst_pad_has_current_caps (peer));
  GstSegment seg;
  gst_segment_init (&seg, GST_FORMAT_TIME);
  g_assert_true (gst_pad_push_event (src, gst_event_new_segment (&seg)));

  GstBuffer *b = gst_buffer_new_memdup (init, len);
  GST_BUFFER_PTS (b) = 0;
  g_assert_cmpint (gst_pad_push (src, b), ==, GST_FLOW_OK);

  /* One sync chunk: this is what makes the splitter release the init, which
   * runs prepare -> barrier -> attach -> add_track. */
  guint8 chunk[256];
  gsize clen = build_cmaf_chunk (chunk, 1, 0 /* sync: non-sync bit clear */);
  moq_cmaf_sample_t samples[8];
  moq_cmaf_fragment_info_t fi;
  moq_cmaf_fragment_info_init (&fi, samples, G_N_ELEMENTS (samples));
  g_assert_cmpint (moq_cmaf_parse_fragment ((moq_bytes_t) { chunk, clen }, &fi),
      ==, MOQ_OK);

  GstBuffer *cb = gst_buffer_new_memdup (chunk, clen);
  GST_BUFFER_PTS (cb) = 0;
  g_assert_cmpint (gst_pad_push (src, cb), ==, GST_FLOW_OK);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_pad_set_active (src, FALSE);
  gst_object_unref (src);
  gst_object_unref (peer);
}

/* The scheme and KID are never configured -- they come from the init segment's
 * own sinf/schm/schi(tenc). drm-systems supplies only what an fMP4 cannot. */
static void
test_cp_authored_from_init (void)
{
  log_reset ();
  capture_reset ();
  GstElement *sink = new_sink ();
  g_object_set (sink, "drm-systems",
      WIDEVINE_UUID ",laurl=https://lic.example/wv,pssh=AAAAQ3Bzc2g=", NULL);

  push_cmaf_init (sink, TRUE, "cenc");

  g_assert_cmpuint (attached_cps.count, ==, 1);
  g_assert_cmpstr (attached_cps.ref_id[0], ==, "cp0");
  /* Derived from the tenc default_KID: raw 16 bytes -> CMSF UUID string. */
  g_assert_cmpstr (attached_cps.kid[0], ==, FIXTURE_KID_UUID);
  g_assert_cmpstr (attached_cps.scheme[0], ==, "cenc");
  g_assert_cmpstr (attached_cps.system_id[0], ==, WIDEVINE_UUID);
  g_assert_cmpstr (attached_cps.la_url[0], ==, "https://lic.example/wv");
  /* The '=' padding of a base64 PSSH must survive the key=value split. */
  g_assert_cmpstr (attached_cps.pssh[0], ==, "AAAAQ3Bzc2g=");

  g_assert_cmpstr (g_hash_table_lookup (track_refs, "video"), ==, "cp0");

  gst_object_unref (sink);
}

/* CMSF carries one drmSystem per entry, so two systems over one key means two
 * entries sharing the KID, and the track references both. */
static void
test_cp_two_systems_one_key (void)
{
  log_reset ();
  capture_reset ();
  GstElement *sink = new_sink ();
  g_object_set (sink, "drm-systems",
      WIDEVINE_UUID ",laurl=https://lic.example/wv;"
      PLAYREADY_UUID ",laurl=https://lic.example/pr", NULL);

  push_cmaf_init (sink, TRUE, "cbcs");

  g_assert_cmpuint (attached_cps.count, ==, 2);
  g_assert_cmpstr (attached_cps.ref_id[0], ==, "cp0");
  g_assert_cmpstr (attached_cps.ref_id[1], ==, "cp1");
  g_assert_cmpstr (attached_cps.system_id[0], ==, WIDEVINE_UUID);
  g_assert_cmpstr (attached_cps.system_id[1], ==, PLAYREADY_UUID);
  /* One key, carried by both, and the scheme comes from schm, not config. */
  g_assert_cmpstr (attached_cps.kid[0], ==, FIXTURE_KID_UUID);
  g_assert_cmpstr (attached_cps.kid[1], ==, FIXTURE_KID_UUID);
  g_assert_cmpstr (attached_cps.scheme[0], ==, "cbcs");
  g_assert_cmpstr (attached_cps.scheme[1], ==, "cbcs");

  g_assert_cmpstr (g_hash_table_lookup (track_refs, "video"), ==, "cp0,cp1");

  gst_object_unref (sink);
}

/* An encrypted track with no DRM system configured: libmoq would reject a
 * contentProtection with no systemID, so the catalog carries none and the
 * element warns. Pins the documented fallback rather than a crash or a bogus
 * entry. */
static void
test_cp_encrypted_without_systems (void)
{
  log_reset ();
  capture_reset ();
  GstElement *sink = new_sink ();

  push_cmaf_init (sink, TRUE, "cenc");

  g_assert_cmpuint (attached_cps.count, ==, 0);
  g_assert_cmpstr (g_hash_table_lookup (track_refs, "video"), ==, "");

  gst_object_unref (sink);
}

/* A clear track must never be described as protected, even with drm-systems
 * set: declaring refIDs on an unencrypted track would make libmoq reject the
 * add, and would lie to subscribers. */
static void
test_cp_clear_track_declares_nothing (void)
{
  log_reset ();
  capture_reset ();
  GstElement *sink = new_sink ();
  g_object_set (sink, "drm-systems", WIDEVINE_UUID, NULL);

  push_cmaf_init (sink, FALSE, "cenc");

  g_assert_cmpuint (attached_cps.count, ==, 0);
  g_assert_cmpstr (g_hash_table_lookup (track_refs, "video"), ==, "");

  gst_object_unref (sink);
}

int
main (int argc, char **argv)
{
  gst_init (&argc, &argv);
  g_test_init (&argc, &argv, NULL);
  g_mutex_init (&log_lock);
  g_mutex_init (&task_lock); g_cond_init (&task_cond);
  log_reset ();
  capture_reset ();
  g_test_add_func ("/barrier/initial-catalog-complete",
      test_initial_catalog_complete);
  g_test_add_func ("/barrier/unlinked-pad-excluded", test_unlinked_pad_excluded);
  g_test_add_func ("/barrier/timeout-no-partial", test_timeout);
  g_test_add_func ("/barrier/empty-eos", test_empty);
  g_test_add_data_func ("/barrier/flush-cancels", GINT_TO_POINTER (0), test_flush);
  g_test_add_data_func ("/barrier/state-cancels", GINT_TO_POINTER (1), test_flush);
  g_test_add_data_func ("/barrier/queued-start-cancel", GUINT_TO_POINTER (0), test_start_failure);
  g_test_add_data_func ("/barrier/post-rejected", GUINT_TO_POINTER (1), test_start_failure);
  g_test_add_data_func ("/barrier/attach-failed", GUINT_TO_POINTER (2), test_start_failure);
  g_test_add_data_func ("/barrier/add-failed", GUINT_TO_POINTER (3), test_start_failure);
  g_test_add_func ("/barrier/partial-add-failed", test_partial_add_failure);
  g_test_add_func ("/barrier/running-start-cancel", test_running_cancel);
  g_test_add_data_func ("/barrier/audio-truncated", GUINT_TO_POINTER (0), test_bad_audio_config);
  g_test_add_data_func ("/barrier/audio-he-rejected", GUINT_TO_POINTER (1), test_bad_audio_config);
  g_test_add_data_func ("/barrier/audio-caps-mismatch", GUINT_TO_POINTER (2), test_bad_audio_config);
  g_test_add_func ("/barrier/capture-and-groups", test_timestamps_and_groups);
  g_test_add_func ("/cp/authored-from-init", test_cp_authored_from_init);
  g_test_add_func ("/cp/two-systems-one-key", test_cp_two_systems_one_key);
  g_test_add_func ("/cp/encrypted-without-systems",
      test_cp_encrypted_without_systems);
  g_test_add_func ("/cp/clear-track-declares-nothing",
      test_cp_clear_track_declares_nothing);
  int result = g_test_run ();
  log_reset (); g_array_unref (call_log);
  return result;
}

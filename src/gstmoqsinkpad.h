/* src/gstmoqsinkpad.h */
#ifndef GST_MOQ_SINK_PAD_H
#define GST_MOQ_SINK_PAD_H

#include <gst/gst.h>
#include <moq/cmaf.h>
#include <moq/media_sender.h>

#include "gstmoqfmp4.h"

G_BEGIN_DECLS

#define GST_TYPE_MOQ_SINK_PAD (gst_moq_sink_pad_get_type ())
G_DECLARE_FINAL_TYPE (GstMoqSinkPad, gst_moq_sink_pad, GST, MOQ_SINK_PAD, GstPad)

struct _GstMoqSinkPad
{
  GstPad parent;

  /* properties */
  gchar   *track_name;
  guint64  bitrate;

  /* fixed at request time */
  moq_media_type_t      media_type;
  moq_media_packaging_t packaging;

  /* runtime */
  moq_media_track_t *track;      /* NULL until the track is added */
  gboolean           eos;
  gboolean           flushing;
  gboolean           flush_pending;
  guint64            objects_sent;
  GstSegment         segment;
  GstClockID         clock_id;   /* pending sync wait, under OBJECT_LOCK */

  /* LOC (always pads) */
  GstClockTime base_pts;
  gint width, height, fps_n, fps_d;   /* video */
  gint rate, channels;                /* audio */
  gboolean avc;                       /* video: length-prefixed, not Annex B */
  GBytes *codec_data;                 /* audio: AudioSpecificConfig from caps */
  GstClockTime group_start;           /* audio: start of the open group */
  /* audio: the video group-start PTS this pad has already aligned to, so each
   * video group opens exactly one audio group (see gst_moq_sink_chain_loc). */
  GstClockTime aligned_to_video_pts;

  /* Initial-catalog barrier. Everything moq_media_sender_add_track needs is
   * derived at this pad's first keyframe and stashed here, so the sender can be
   * attached only once EVERY linked pad has one and all tracks can be added
   * before the session establishes -- otherwise a pad whose caps arrive late is
   * absent from the initial catalog (see gst_moq_sink_await_tracks). */
  gboolean pend_ready;
  gchar   *pend_codec;                /* codec string (owned) */
  GBytes  *pend_avcc;                 /* LOC video: avcC (owned) */
  gchar   *pend_chan;                 /* LOC audio: channelConfig (owned) */
  /* CMAF: this track's CMSF contentProtectionRefIDs (CMSF 4.1.2), resolved at
   * the barrier from the init segment's CENC boxes and the element's
   * drm-systems. NULL-terminated and owned; NULL when the track is not
   * encrypted, or when it is but no DRM system is configured to describe it. */
  gchar  **pend_cp_ref_ids;

  /* CMAF (request pads) */
  GstMoqFmp4Splitter   splitter;
  GBytes              *init;         /* ftyp + moov as sent to the catalog */
  moq_cmaf_init_info_t init_info;    /* borrows from `init` */
  gboolean             has_init_info;
  gboolean             skip_logged;  /* one-shot "skipping non-media boxes" log */
};

GstMoqSinkPad *gst_moq_sink_pad_new (GstPadTemplate *templ, const gchar *name,
    moq_media_type_t media_type, moq_media_packaging_t packaging,
    const gchar *default_track_name, guint64 default_bitrate,
    gsize max_fragment_size);

/* Reset per-stream state (called for a fresh sender lifetime). */
void gst_moq_sink_pad_reset (GstMoqSinkPad *pad);

G_END_DECLS

#endif

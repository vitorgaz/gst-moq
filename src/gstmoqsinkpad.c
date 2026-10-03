/* src/gstmoqsinkpad.c */
#include "gstmoqsinkpad.h"

enum
{
  PAD_PROP_0,
  PAD_PROP_TRACK_NAME,
  PAD_PROP_BITRATE,
};

G_DEFINE_TYPE (GstMoqSinkPad, gst_moq_sink_pad, GST_TYPE_PAD)

static void
gst_moq_sink_pad_set_property (GObject *object, guint prop_id,
    const GValue *value, GParamSpec *pspec)
{
  GstMoqSinkPad *pad = GST_MOQ_SINK_PAD (object);
  switch (prop_id) {
    case PAD_PROP_TRACK_NAME:
      g_free (pad->track_name);
      pad->track_name = g_value_dup_string (value);
      break;
    case PAD_PROP_BITRATE:
      pad->bitrate = g_value_get_uint64 (value);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
  }
}

static void
gst_moq_sink_pad_get_property (GObject *object, guint prop_id,
    GValue *value, GParamSpec *pspec)
{
  GstMoqSinkPad *pad = GST_MOQ_SINK_PAD (object);
  switch (prop_id) {
    case PAD_PROP_TRACK_NAME:
      g_value_set_string (value, pad->track_name);
      break;
    case PAD_PROP_BITRATE:
      g_value_set_uint64 (value, pad->bitrate);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
  }
}

static void
gst_moq_sink_pad_finalize (GObject *object)
{
  GstMoqSinkPad *pad = GST_MOQ_SINK_PAD (object);
  g_free (pad->track_name);
  gst_moq_fmp4_splitter_clear (&pad->splitter);
  g_clear_pointer (&pad->init, g_bytes_unref);
  g_clear_pointer (&pad->codec_data, g_bytes_unref);
  g_clear_pointer (&pad->pend_codec, g_free);
  g_clear_pointer (&pad->pend_avcc, g_bytes_unref);
  g_clear_pointer (&pad->pend_chan, g_free);
  g_clear_pointer (&pad->pend_cp_ref_ids, g_strfreev);
  G_OBJECT_CLASS (gst_moq_sink_pad_parent_class)->finalize (object);
}

static void
gst_moq_sink_pad_class_init (GstMoqSinkPadClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  gobject_class->set_property = gst_moq_sink_pad_set_property;
  gobject_class->get_property = gst_moq_sink_pad_get_property;
  gobject_class->finalize = gst_moq_sink_pad_finalize;

  g_object_class_install_property (gobject_class, PAD_PROP_TRACK_NAME,
      g_param_spec_string ("track-name", "Track name",
          "MoQ track name published for this pad", NULL,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PAD_PROP_BITRATE,
      g_param_spec_uint64 ("bitrate", "Bitrate",
          "Advertised catalog max bitrate for this pad (bits/s)", 1,
          G_MAXUINT64, 2000000,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
}

static void
gst_moq_sink_pad_init (GstMoqSinkPad *pad)
{
  gst_segment_init (&pad->segment, GST_FORMAT_UNDEFINED);
  gst_moq_fmp4_splitter_init (&pad->splitter, 16 << 20);
  pad->base_pts = GST_CLOCK_TIME_NONE;
}

void
gst_moq_sink_pad_reset (GstMoqSinkPad *pad)
{
  pad->track = NULL;
  pad->eos = FALSE;
  pad->flushing = FALSE;
  pad->flush_pending = FALSE;
  pad->objects_sent = 0;
  pad->base_pts = GST_CLOCK_TIME_NONE;
  pad->group_start = GST_CLOCK_TIME_NONE;
  pad->aligned_to_video_pts = GST_CLOCK_TIME_NONE;
  pad->pend_ready = FALSE;
  g_clear_pointer (&pad->pend_codec, g_free);
  g_clear_pointer (&pad->pend_avcc, g_bytes_unref);
  g_clear_pointer (&pad->pend_chan, g_free);
  g_clear_pointer (&pad->pend_cp_ref_ids, g_strfreev);
  gsize max_box = pad->splitter.max_box;
  gst_moq_fmp4_splitter_clear (&pad->splitter);
  gst_moq_fmp4_splitter_init (&pad->splitter, max_box);
  g_clear_pointer (&pad->init, g_bytes_unref);
  pad->has_init_info = FALSE;
  pad->skip_logged = FALSE;
  gst_segment_init (&pad->segment, GST_FORMAT_UNDEFINED);
}

GstMoqSinkPad *
gst_moq_sink_pad_new (GstPadTemplate *templ, const gchar *name,
    moq_media_type_t media_type, moq_media_packaging_t packaging,
    const gchar *default_track_name, guint64 default_bitrate,
    gsize max_fragment_size)
{
  GstMoqSinkPad *pad = g_object_new (GST_TYPE_MOQ_SINK_PAD,
      "name", name, "direction", GST_PAD_SINK, "template", templ, NULL);
  pad->media_type = media_type;
  pad->packaging = packaging;
  pad->track_name = g_strdup (default_track_name);
  pad->bitrate = default_bitrate;
  gst_moq_fmp4_splitter_clear (&pad->splitter);
  gst_moq_fmp4_splitter_init (&pad->splitter, max_fragment_size);
  return pad;
}

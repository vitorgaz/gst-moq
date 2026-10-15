//! moqsrc -- subscribe to a Media-over-QUIC track and push buffers (libmoq).

use gst::glib;
use gst::prelude::*;

pub(crate) mod imp;

#[cfg(test)]
mod tests;

glib::wrapper! {
    pub struct MoqSrc(ObjectSubclass<imp::MoqSrc>)
        @extends gst_base::PushSrc, gst_base::BaseSrc, gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "moqsrc",
        gst::Rank::NONE,
        MoqSrc::static_type(),
    )
}

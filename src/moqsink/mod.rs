//! moqsink -- publish a GStreamer stream over Media-over-QUIC (libmoq).

use gst::glib;
use gst::prelude::*;

pub(crate) mod imp;
pub mod pad;

#[cfg(test)]
mod tests_barrier;
#[cfg(test)]
mod tests_recovery;

pub use pad::MoqSinkPad;

glib::wrapper! {
    pub struct MoqSink(ObjectSubclass<imp::MoqSink>) @extends gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "moqsink",
        gst::Rank::NONE,
        MoqSink::static_type(),
    )
}

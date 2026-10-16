//! gst-moq plugin entry point.
//!
//! Registers the moqsrc and moqsink elements. Media-over-QUIC transport is
//! provided by libmoq ("MOQ5") over WebTransport.

use gst::glib;

mod clock;
pub mod codec;
pub mod fmp4;
pub mod moq;
pub mod moqsink;
pub mod moqsrc;

#[cfg(test)]
mod test_support;

fn plugin_init(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    // Touch a libmoq symbol so the link chain is exercised at load time and
    // surfaces in diagnostics.
    gst::info!(
        gst::CAT_DEFAULT,
        obj = plugin,
        "gst-moq backed by libmoq {}",
        moq5::libmoq_version()
    );

    moqsink::register(plugin)?;
    moqsrc::register(plugin)?;
    Ok(())
}

gst::plugin_define!(
    moq,
    env!("CARGO_PKG_DESCRIPTION"),
    plugin_init,
    env!("CARGO_PKG_VERSION"),
    "Apache-2.0",
    "gst-moq",
    "gst-moq",
    "https://github.com/openmoq/moq5"
);

//! What both elements share on top of the moq5 crate.

use moq5::{EndpointConfig, Protocol, Version, VersionOffer};

/// The `https://host:port/path` URL both elements dial.
pub fn relay_url(host: &str, port: u32, relay_path: &str) -> String {
    format!("https://{host}:{port}{relay_path}")
}

/// A WebTransport endpoint config for `url`, pinned to `draft` unless it is 0.
pub fn endpoint_config(url: &str, insecure: bool, draft: u32) -> EndpointConfig {
    let cfg = EndpointConfig::new(url)
        .protocol(Protocol::WebTransport)
        .insecure_skip_verify(insecure);
    if draft == 0 {
        cfg
    } else {
        cfg.versions(VersionOffer::Exact(Version::from_draft(draft)))
    }
}

/// The libmoq result code of a call whose only success is `MOQ_OK`.
pub fn rc<T>(r: &moq5::Result<T>) -> i32 {
    match r {
        Ok(_) => moq5::sys::MOQ_OK,
        Err(e) => e.code(),
    }
}

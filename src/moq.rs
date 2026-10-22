//! What both elements share on top of the moq5 crate.

use crate::service::EndpointCfg;

/// The `https://host:port/path` URL both elements dial.
pub fn relay_url(host: &str, port: u32, relay_path: &str) -> String {
    format!("https://{host}:{port}{relay_path}")
}

/// A WebTransport endpoint config for `url`, pinned to `draft` unless it is 0.
/// The transport choice lives in the service adapter, which replays this onto
/// libmoq's own builder.
pub(crate) fn endpoint_config(url: &str, insecure: bool, draft: u32) -> EndpointCfg {
    EndpointCfg {
        url: url.to_owned(),
        insecure,
        draft,
    }
}

/// The libmoq result code of a call whose only success is `MOQ_OK`.
pub fn rc<T>(r: &moq5::Result<T>) -> i32 {
    match r {
        Ok(_) => moq5::sys::MOQ_OK,
        Err(e) => e.code(),
    }
}

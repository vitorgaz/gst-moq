//! The libmoq seam.
//!
//! Every libmoq service-tier call the elements make goes through [`api`]. A
//! release build resolves it to [`REAL`], a constant table of the libmoq
//! functions themselves, so the call is direct. Unit tests install their own
//! table instead -- the Rust counterpart of the C suites replacing these
//! symbols at link time -- and keep the real CMAF parser and rcbuf code.

pub use moq_sys::*;

use std::ffi::c_void;

/// An opaque libmoq handle (endpoint, sender, receiver, track). libmoq owns
/// the pointee; this only carries the address between threads, which the
/// service tier permits for handles under the locking each element documents.
#[derive(Debug)]
pub struct Handle<T>(pub *mut T);

impl<T> Handle<T> {
    pub const fn null() -> Self {
        Handle(std::ptr::null_mut())
    }
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }
    pub fn get(&self) -> *mut T {
        self.0
    }
}

impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Handle<T> {}
impl<T> PartialEq for Handle<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl<T> Eq for Handle<T> {}
impl<T> Default for Handle<T> {
    fn default() -> Self {
        Self::null()
    }
}
// SAFETY: see the type docs -- these are opaque addresses, never dereferenced
// on the Rust side.
unsafe impl<T> Send for Handle<T> {}
unsafe impl<T> Sync for Handle<T> {}

/// A byte span borrowed from libmoq-owned memory (a catalog descriptor),
/// valid while the owning receiver lives.
#[derive(Clone, Copy, Debug, Default)]
pub struct ForeignSpan(pub moq_bytes_t);

// SAFETY: a read-only view whose lifetime the owning element manages.
unsafe impl Send for ForeignSpan {}
unsafe impl Sync for ForeignSpan {}

impl ForeignSpan {
    pub fn len(&self) -> usize {
        if self.0.data.is_null() {
            0
        } else {
            self.0.len
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// # Safety
    /// The owning receiver must still be alive.
    pub unsafe fn as_slice(&self) -> &[u8] {
        bytes(self.0)
    }
}

/// A slash-separated MoQ namespace split into the parts libmoq takes. The
/// spans point into `tokens`, which this owns for as long as libmoq needs them.
pub struct Namespace {
    tokens: Vec<Vec<u8>>,
    parts: Vec<moq_bytes_t>,
}

// SAFETY: `parts` only points into `tokens`, which moves with it.
unsafe impl Send for Namespace {}
unsafe impl Sync for Namespace {}

impl Namespace {
    /// Split `s` on '/'. Every part must be non-empty, and there must be one.
    pub fn parse(s: &str) -> Result<Self, String> {
        if s.is_empty() {
            return Err("namespace is empty".into());
        }
        let tokens: Vec<Vec<u8>> = s.split('/').map(|t| t.as_bytes().to_vec()).collect();
        if tokens.iter().any(|t| t.is_empty()) {
            return Err(format!("namespace has an empty part: '{s}'"));
        }
        let parts = tokens.iter().map(|t| span(t)).collect();
        Ok(Namespace { tokens, parts })
    }

    pub fn parts(&self) -> *const moq_bytes_t {
        self.parts.as_ptr()
    }

    pub fn count(&self) -> usize {
        debug_assert_eq!(self.parts.len(), self.tokens.len());
        self.parts.len()
    }
}

/// The `https://host:port/path` URL both elements dial.
pub fn relay_url(host: &str, port: u32, relay_path: &str) -> String {
    format!("https://{host}:{port}{relay_path}")
}

/// Fill an endpoint cfg for WebTransport to `url`, pinned to `draft` unless it
/// is 0.
///
/// # Safety
/// The cfg borrows `url` and `pinned` by raw pointer: both must outlive every
/// use of it (the connect call).
pub unsafe fn endpoint_cfg(
    url: &str,
    insecure: bool,
    draft: u32,
    pinned: &moq_version_t,
) -> moq_endpoint_cfg_t {
    let mut ec = moq_endpoint_cfg_t::default();
    moq_endpoint_cfg_init(&mut ec);
    ec.url = span(url.as_bytes());
    ec.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
    ec.insecure_skip_verify = insecure;
    if draft != 0 {
        ec.versions.struct_size = std::mem::size_of_val(&ec.versions) as u32;
        ec.versions.policy = MOQ_VERSION_POLICY_EXACT;
        ec.versions.versions = pinned;
        ec.versions.version_count = 1;
    }
    ec
}

/// Borrow `s` as a libmoq byte span. An empty slice becomes the null span.
pub fn span(s: &[u8]) -> moq_bytes_t {
    if s.is_empty() {
        moq_bytes_t::default()
    } else {
        moq_bytes_t {
            data: s.as_ptr(),
            len: s.len(),
        }
    }
}

/// View a libmoq byte span as a slice (empty for a null span).
///
/// # Safety
/// `b` must describe memory valid for `'a`.
pub unsafe fn bytes<'a>(b: moq_bytes_t) -> &'a [u8] {
    if b.data.is_null() || b.len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(b.data, b.len)
    }
}

/// Wrap `len` bytes at `data` in a libmoq rcbuf whose release callback hands
/// `ctx` back to `release` (on whichever thread drops the last reference).
///
/// # Safety
/// `data..data+len` must stay valid until `release` runs, and `release` must
/// be safe to call with `ctx` from any thread.
pub unsafe fn rcbuf_wrap(
    data: *const u8,
    len: usize,
    release: moq_rcbuf_release_fn,
    ctx: *mut c_void,
) -> Result<*mut moq_rcbuf_t, moq_result_t> {
    let mut out = std::ptr::null_mut();
    match moq_rcbuf_wrap(moq_alloc_default(), data, len, release, ctx, &mut out) {
        MOQ_OK => Ok(out),
        rc => Err(rc),
    }
}

macro_rules! api_table {
    ($($name:ident : fn($($arg:ident : $ty:ty),*) $(-> $ret:ty)?;)*) => {
        /// The libmoq service calls the elements make, as replaceable entries.
        #[derive(Clone, Copy)]
        pub struct Api {
            $(pub $name: unsafe extern "C" fn($($ty),*) $(-> $ret)?,)*
        }

        /// The real libmoq.
        pub const REAL: Api = Api { $($name: moq_sys::$name,)* };

        // Call wrappers with the C names, routed through `api()`. Defined in
        // this module, they shadow the raw functions of the glob re-export
        // above, so `use crate::moq::*` gets the routed versions.
        $(
            #[inline]
            #[allow(clippy::missing_safety_doc)]
            pub unsafe fn $name($($arg: $ty),*) $(-> $ret)? {
                (api().$name)($($arg),*)
            }
        )*
    };
}

api_table! {
    moq_endpoint_cfg_init: fn(cfg: *mut moq_endpoint_cfg_t);
    moq_endpoint_connect: fn(cfg: *const moq_endpoint_cfg_t, out: *mut *mut moq_endpoint_t) -> moq_result_t;
    moq_endpoint_stop: fn(ep: *mut moq_endpoint_t) -> moq_result_t;
    moq_endpoint_destroy: fn(ep: *mut moq_endpoint_t);
    moq_endpoint_set_interrupted: fn(ep: *mut moq_endpoint_t, interrupted: bool);
    moq_endpoint_wait: fn(ep: *mut moq_endpoint_t, timeout_us: u64) -> moq_result_t;
    moq_endpoint_drain: fn(ep: *mut moq_endpoint_t, timeout_us: u64) -> moq_result_t;
    moq_endpoint_post: fn(ep: *mut moq_endpoint_t, fn_: moq_endpoint_task_fn, ctx: *mut c_void) -> moq_result_t;
    moq_endpoint_get_terminal: fn(ep: *const moq_endpoint_t, out: *mut moq_endpoint_terminal_t, out_size: usize) -> moq_result_t;
    moq_endpoint_negotiated_version: fn(ep: *const moq_endpoint_t) -> moq_version_t;

    moq_media_sender_cfg_init_live_sized: fn(cfg: *mut moq_media_sender_cfg_t, cfg_size: usize);
    moq_media_sender_callbacks_init_sized: fn(cb: *mut moq_media_sender_callbacks_t, cb_size: usize);
    moq_media_sender_attach: fn(ep: *mut moq_endpoint_t, cfg: *const moq_media_sender_cfg_t, out: *mut *mut moq_media_sender_t) -> moq_result_t;
    moq_media_sender_destroy: fn(s: *mut moq_media_sender_t);
    moq_media_track_cfg_init: fn(cfg: *mut moq_media_track_cfg_t);
    moq_media_sender_add_track: fn(s: *mut moq_media_sender_t, cfg: *const moq_media_track_cfg_t, out: *mut *mut moq_media_track_t) -> moq_result_t;
    moq_media_sender_write: fn(s: *mut moq_media_sender_t, track: *mut moq_media_track_t, obj: *const moq_media_send_object_t) -> moq_result_t;
    moq_media_sender_end_track: fn(s: *mut moq_media_sender_t, track: *mut moq_media_track_t) -> moq_result_t;
    moq_media_sender_is_fatal: fn(s: *const moq_media_sender_t) -> bool;
    moq_media_sender_fatal_code: fn(s: *const moq_media_sender_t) -> u64;
    moq_media_sender_get_stats: fn(s: *const moq_media_sender_t, out: *mut moq_media_sender_stats_t, out_size: usize) -> moq_result_t;

    moq_media_receiver_cfg_init_live: fn(cfg: *mut moq_media_receiver_cfg_t);
    moq_media_receiver_attach: fn(ep: *mut moq_endpoint_t, cfg: *const moq_media_receiver_cfg_t, out: *mut *mut moq_media_receiver_t) -> moq_result_t;
    moq_media_receiver_destroy: fn(r: *mut moq_media_receiver_t);
    moq_media_receiver_subscribe_track: fn(r: *mut moq_media_receiver_t, track: *mut moq_media_track_t, cfg: *const moq_media_receiver_track_subscribe_cfg_t) -> moq_result_t;
    moq_media_receiver_poll_track: fn(r: *mut moq_media_receiver_t, ev: *mut moq_media_track_event_t, ev_size: usize) -> moq_result_t;
    moq_media_receiver_poll_object: fn(r: *mut moq_media_receiver_t, obj: *mut moq_media_object_t, obj_size: usize) -> moq_result_t;
    moq_media_receiver_wait: fn(r: *mut moq_media_receiver_t, timeout_us: u64) -> moq_result_t;
    moq_media_receiver_is_fatal: fn(r: *const moq_media_receiver_t) -> bool;
    moq_media_receiver_fatal_code: fn(r: *const moq_media_receiver_t) -> u64;
    moq_media_object_cleanup: fn(obj: *mut moq_media_object_t);
}

#[cfg(not(test))]
#[inline(always)]
pub fn api() -> Api {
    REAL
}

#[cfg(test)]
pub use self::test_seam::{api, install, ApiGuard};

#[cfg(test)]
mod test_seam {
    use super::{Api, REAL};
    use std::sync::RwLock;

    static ACTIVE: RwLock<Api> = RwLock::new(REAL);

    pub fn api() -> Api {
        *ACTIVE.read().unwrap()
    }

    /// Restores the previous table when dropped.
    pub struct ApiGuard(Api);

    impl Drop for ApiGuard {
        fn drop(&mut self) {
            *ACTIVE.write().unwrap() = self.0;
        }
    }

    /// Replace the table until the returned guard drops.
    pub fn install(table: Api) -> ApiGuard {
        ApiGuard(std::mem::replace(&mut *ACTIVE.write().unwrap(), table))
    }
}

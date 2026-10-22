//! Media payloads handed to libmoq without copying.
//!
//! libmoq takes a payload as a refcounted buffer (`moq_rcbuf_t`) and releases
//! it when the last reference goes. A GStreamer buffer is already mapped
//! readable by the time we send it, so the bytes can be lent to libmoq as they
//! are and the mapping dropped from libmoq's release callback. That keeps one
//! access unit per object off the memcpy path, which is what `moqsink` relied
//! on when `moq5` still offered an owner-generic constructor
//! (`RcBuf::from_owner_range`).
//!
//! `moq5::Buffer` now adopts only `Vec` and `Box<[u8]>`, so an owner of any
//! other type goes through its `wrap_raw` escape hatch. This module is the
//! only place in the plugin that uses `unsafe`, and the whole of it is the two
//! obligations `wrap_raw` documents: the bytes stay valid and unmodified until
//! the release callback runs, and the callback runs exactly once.

use std::ffi::c_void;
use std::ops::Range;

use moq5::{Buffer, Error, Result};

/// Drops the owner `wrap` boxed. libmoq calls this once, with the context
/// `wrap` passed, when the last reference to the buffer goes.
///
/// # Safety
/// `context` must be the `Box<T>` raw pointer `wrap` installed, unreleased.
unsafe extern "C" fn release<T>(context: *mut c_void, _data: *const u8, _len: usize) {
    // SAFETY: the caller's contract: our own Box, released exactly once.
    drop(unsafe { Box::from_raw(context as *mut T) });
}

/// Lend `range` of `owner`'s bytes to libmoq, keeping `owner` alive until the
/// buffer is released. The bytes are not copied.
///
/// `owner` must deref to the same bytes for its whole life -- true of a mapped
/// GStreamer buffer, which is read-only while mapped. It must be `Send`
/// because libmoq releases the buffer from whichever thread drops the last
/// reference, which is not the one that sent it.
pub(crate) fn wrap_owner<T>(owner: T, range: Range<usize>) -> Result<Buffer>
where
    T: AsRef<[u8]> + Send + 'static,
{
    let owner = Box::new(owner);
    let bytes = owner.as_ref().as_ref();
    if range.start > range.end || range.end > bytes.len() {
        return Err(Error::INVAL);
    }
    let len = range.end - range.start;
    let data = bytes[range].as_ptr();
    let context = Box::into_raw(owner);
    // SAFETY: `data`/`len` address `context`'s own bytes, which live until the
    // release callback drops it; the box is leaked to libmoq until then. The
    // callback is ours, runs once per libmoq's contract, and frees exactly
    // that box. On failure libmoq has not taken the context, so we drop it.
    let wrapped = unsafe { Buffer::wrap_raw(data, len, Some(release::<T>), context as *mut c_void) };
    if wrapped.is_err() {
        // SAFETY: the pointer Box::into_raw gave us, never released.
        drop(unsafe { Box::from_raw(context) });
    }
    wrapped
}

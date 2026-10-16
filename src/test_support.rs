//! Shared plumbing for the element suites.
//!
//! Each C suite ran as its own process with global mock state. Here the
//! suites share one test binary, so every test that installs a libmoq table,
//! the clock hooks or a GStreamer log function holds [`serial`] for its whole
//! run.

use std::sync::{Mutex, MutexGuard, Once};

static SERIAL: Mutex<()> = Mutex::new(());

/// Serialize a mock-driven test and make sure GStreamer is initialized.
pub fn serial() -> MutexGuard<'static, ()> {
    static INIT: Once = Once::new();
    INIT.call_once(|| gst::init().unwrap());
    // A failed test poisons the lock; the next one still runs on fresh mocks.
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// A raw address that can sit in a static (mock handles, captured contexts).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Addr(pub usize);

impl Addr {
    pub fn of<T>(p: *const T) -> Self {
        Addr(p as usize)
    }
    pub fn ptr<T>(self) -> *mut T {
        self.0 as *mut T
    }
}

/// Copy a libmoq span out as a string (None for a null span).
/// Borrow `s` as a libmoq byte span (null when empty), for fake objects.
pub fn span(s: &[u8]) -> moq5::sys::moq_bytes_t {
    if s.is_empty() {
        moq5::sys::moq_bytes_t::default()
    } else {
        moq5::sys::moq_bytes_t {
            data: s.as_ptr(),
            len: s.len(),
        }
    }
}

/// View a libmoq byte span.
///
/// # Safety
/// `b` must describe memory valid for `'a`.
pub unsafe fn bytes<'a>(b: moq5::sys::moq_bytes_t) -> &'a [u8] {
    if b.data.is_null() || b.len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(b.data, b.len)
    }
}

pub fn span_dup(b: moq5::sys::moq_bytes_t) -> Option<String> {
    if b.data.is_null() {
        None
    } else {
        Some(String::from_utf8_lossy(unsafe { bytes(b) }).into_owned())
    }
}

/// Zero a libmoq cfg struct and stamp its size, as the mocked `*_init`s do.
pub unsafe fn zero_sized<T>(cfg: *mut T, set_size: impl FnOnce(&mut T, u32)) {
    std::ptr::write_bytes(cfg, 0, 1);
    set_size(&mut *cfg, std::mem::size_of::<T>() as u32);
}

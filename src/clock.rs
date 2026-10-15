//! Monotonic time and short sleeps, behind a test seam.
//!
//! The sink's EOS retry loop is driven by `monotonic_us()`/`sleep_us()`.
//! Release builds use GLib's monotonic clock and a real sleep; unit tests can
//! substitute both (the C suite did the same with macro overrides of
//! `g_get_monotonic_time` and `g_usleep`).

/// Microseconds on the monotonic clock (`g_get_monotonic_time`).
#[cfg(not(test))]
#[inline]
pub fn monotonic_us() -> i64 {
    gst::glib::monotonic_time()
}

/// Sleep for `us` microseconds (`g_usleep`).
#[cfg(not(test))]
#[inline]
pub fn sleep_us(us: u64) {
    std::thread::sleep(std::time::Duration::from_micros(us));
}

#[cfg(test)]
pub use self::test_seam::{install, monotonic_us, sleep_us, ClockGuard};

#[cfg(test)]
mod test_seam {
    use std::sync::RwLock;

    #[derive(Clone, Copy)]
    pub struct Hooks {
        pub monotonic_us: fn() -> i64,
        pub sleep_us: fn(u64),
    }

    fn real_monotonic() -> i64 {
        gst::glib::monotonic_time()
    }
    fn real_sleep(us: u64) {
        std::thread::sleep(std::time::Duration::from_micros(us));
    }

    static HOOKS: RwLock<Hooks> = RwLock::new(Hooks {
        monotonic_us: real_monotonic,
        sleep_us: real_sleep,
    });

    pub fn monotonic_us() -> i64 {
        let f = HOOKS.read().unwrap().monotonic_us;
        f()
    }

    pub fn sleep_us(us: u64) {
        let f = HOOKS.read().unwrap().sleep_us;
        f(us)
    }

    /// Restores the previous hooks when dropped.
    pub struct ClockGuard(Hooks);

    impl Drop for ClockGuard {
        fn drop(&mut self) {
            *HOOKS.write().unwrap() = self.0;
        }
    }

    /// Replace both hooks until the returned guard drops.
    pub fn install(monotonic_us: fn() -> i64, sleep_us: fn(u64)) -> ClockGuard {
        ClockGuard(std::mem::replace(
            &mut *HOOKS.write().unwrap(),
            Hooks {
                monotonic_us,
                sleep_us,
            },
        ))
    }
}

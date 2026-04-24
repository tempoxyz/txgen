//! Shared benchmark clock.
//!
//! [`RunClock`] provides two time axes from a single start point:
//! - **offset** (monotonic) — for correlating metrics within a run.
//! - **unix** (wall-clock) — for TSDB export and debugging.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

/// Inner state for [`RunClock`].
#[derive(Debug)]
struct Inner {
    /// Monotonic instant captured at benchmark start.
    start_instant: Instant,
    /// Unix milliseconds captured at benchmark start.
    start_unix_ms: u64,
}

/// A shared, cheaply cloneable benchmark clock.
///
/// Created once when the benchmark starts and shared with all components
/// that need timestamps: metrics collector, Prometheus scraper, block
/// observer, and reporters.
#[derive(Clone, Debug)]
pub struct RunClock {
    inner: Arc<Inner>,
}

impl RunClock {
    /// Create a new clock, capturing the current time as the start.
    pub fn new() -> Self {
        // SAFETY: `SystemTime::now()` is always after `UNIX_EPOCH`.
        let start_unix_ms =
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;

        Self { inner: Arc::new(Inner { start_instant: Instant::now(), start_unix_ms }) }
    }

    /// Monotonic offset in milliseconds since benchmark start.
    pub fn offset_ms(&self) -> u64 {
        self.inner.start_instant.elapsed().as_millis() as u64
    }

    /// Monotonic elapsed [`Duration`] since benchmark start.
    pub fn elapsed(&self) -> Duration {
        self.inner.start_instant.elapsed()
    }

    /// Current wall-clock time in Unix milliseconds.
    pub fn unix_ms(&self) -> u64 {
        self.inner.start_unix_ms + self.offset_ms()
    }

    /// Unix milliseconds captured at benchmark start.
    pub fn start_unix_ms(&self) -> u64 {
        self.inner.start_unix_ms
    }
}

impl Default for RunClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn offset_increases() {
        let clock = RunClock::new();
        let a = clock.offset_ms();
        thread::sleep(Duration::from_millis(10));
        let b = clock.offset_ms();
        assert!(b > a);
    }

    #[test]
    fn unix_ms_is_reasonable() {
        let clock = RunClock::new();
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            // SAFETY: `SystemTime::now()` is always after `UNIX_EPOCH`.
            .unwrap()
            .as_millis() as u64;
        // Should be within 100ms of current time.
        assert!((clock.unix_ms() as i64 - now_ms as i64).unsigned_abs() < 100);
    }

    #[test]
    fn clone_shares_start() {
        let clock = RunClock::new();
        let clone = clock.clone();
        thread::sleep(Duration::from_millis(10));
        let diff = (clock.offset_ms() as i64 - clone.offset_ms() as i64).unsigned_abs();
        assert!(diff < 5);
    }
}

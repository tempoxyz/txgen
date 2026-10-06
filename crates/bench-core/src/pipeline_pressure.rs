//! Opt-in, bounded wall-time diagnostics shared by the generator and sender.
//!
//! No background sampler is started. Long waits delay periodic output until the
//! caller resumes; the terminal record includes completed waits' accumulated time.
//! Canceling an await before its accounting point excludes that unfinished wait.

use serde::Serialize;
use std::{
    ffi::OsStr,
    io::Write,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const PERIOD_NS: u64 = 1_000_000_000;
const MAX_PERIODIC_RECORDS: u32 = 120;
const MAX_RECORD_BYTES: usize = 8192;

fn enabled(value: Option<&OsStr>) -> bool {
    value == Some(OsStr::new("1"))
}

/// Saturating nanoseconds for diagnostic counters only.
pub fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Default)]
struct EmissionBudget {
    last_ns: u64,
    periodic: u32,
    terminal: bool,
}

impl EmissionBudget {
    fn admit(&mut self, elapsed_ns: u64, terminal: bool) -> bool {
        if self.terminal {
            return false;
        }
        if terminal {
            self.terminal = true;
            return true;
        }
        if self.periodic == MAX_PERIODIC_RECORDS ||
            elapsed_ns.saturating_sub(self.last_ns) < PERIOD_NS
        {
            return false;
        }
        self.last_ns = elapsed_ns;
        self.periodic += 1;
        true
    }
}

/// One fixed-size diagnostic stream; inactive unless the shared opt-in is `1`.
pub struct PressureLog {
    start: Instant,
    start_unix_ms: u64,
    role: &'static str,
    label: String,
    budget: EmissionBudget,
}

impl PressureLog {
    /// Enable a diagnostic instance without mutating process-global environment.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn for_test(role: &'static str) -> Self {
        Self {
            start: Instant::now(),
            start_unix_ms: 0,
            role,
            label: "semantic-test".to_owned(),
            budget: EmissionBudget::default(),
        }
    }

    /// An optional caller-supplied label is truncated to 96 printable ASCII bytes.
    pub fn from_env(role: &'static str) -> Option<Self> {
        if !enabled(std::env::var_os("TXGEN_PIPELINE_PRESSURE").as_deref()) {
            return None;
        }
        let label = std::env::var("TXGEN_PIPELINE_PRESSURE_LABEL")
            .unwrap_or_default()
            .chars()
            .filter(char::is_ascii_graphic)
            .take(96)
            .collect();
        Some(Self {
            start: Instant::now(),
            start_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            role,
            label,
            budget: EmissionBudget::default(),
        })
    }

    /// Monotonic origin for concurrent request counters in the same process.
    pub fn start(&self) -> Instant {
        self.start
    }

    /// Relative monotonic time; unrelated processes need their wall-clock anchor.
    pub fn offset_ns(&self) -> u64 {
        nanos(self.start.elapsed())
    }

    /// Emit at most 120 periodic records, at least one second apart, plus a final.
    /// Only fixed-size aggregate structs should be passed; never transaction data.
    pub fn emit<T: Serialize>(&mut self, data: &T, terminal: bool) {
        let elapsed_ns = self.offset_ns();
        if !self.budget.admit(elapsed_ns, terminal) {
            return;
        }
        #[derive(Serialize)]
        struct Record<'a, T> {
            version: u8,
            role: &'static str,
            pid: u32,
            label: &'a str,
            start_unix_ms: u64,
            elapsed_ns: u64,
            terminal: bool,
            periodic_records: u32,
            periodic_limit_reached: bool,
            data: &'a T,
        }
        let record = Record {
            version: 1,
            role: self.role,
            pid: std::process::id(),
            label: &self.label,
            start_unix_ms: self.start_unix_ms,
            elapsed_ns,
            terminal,
            periodic_records: self.budget.periodic,
            periodic_limit_reached: self.budget.periodic == MAX_PERIODIC_RECORDS,
            data,
        };
        // Logging failure never changes transaction execution or its error path.
        if let Ok(line) = serde_json::to_string(&record) &&
            line.len() <= MAX_RECORD_BYTES
        {
            let _ = writeln!(std::io::stderr().lock(), "txgen_pipeline_pressure {line}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opt_in_is_exact_and_default_off() {
        for value in [None, Some(OsStr::new("")), Some(OsStr::new("true")), Some(OsStr::new("0"))] {
            assert!(!enabled(value));
        }
        assert!(enabled(Some(OsStr::new("1"))));
    }

    #[test]
    fn output_is_rate_limited_and_terminal_is_unique() {
        let mut budget = EmissionBudget::default();
        assert!(!budget.admit(PERIOD_NS - 1, false));
        assert!(budget.admit(PERIOD_NS, false));
        assert!(!budget.admit(PERIOD_NS, false));
        for second in 2..=MAX_PERIODIC_RECORDS {
            assert!(budget.admit(u64::from(second) * PERIOD_NS, false));
        }
        assert!(!budget.admit(u64::MAX, false));
        assert!(budget.admit(u64::MAX, true));
        assert!(!budget.admit(u64::MAX, true));
        assert!(!budget.admit(u64::MAX, false));
    }

    #[test]
    fn long_gap_emits_once_without_catchup_and_duration_saturates() {
        let mut budget = EmissionBudget::default();
        assert!(budget.admit(10 * PERIOD_NS, false));
        assert!(!budget.admit(10 * PERIOD_NS + 1, false));
        assert!(budget.admit(10 * PERIOD_NS + 2, true));
        assert_eq!(nanos(Duration::MAX), u64::MAX);
    }
}

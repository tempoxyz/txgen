//! Fixed-size, optional sender diagnostics. Concurrent request counters are not
//! an atomic snapshot; only a completed flush permits exact count closure.

use crate::pipeline_pressure::{nanos, PressureLog};
use serde::Serialize;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

#[derive(Clone, Copy)]
pub(crate) enum Gate {
    Ready,
    Empty,
    Pending,
    KeysOrSetup,
    RpcPermit,
    RateSleep,
}

#[derive(Default, Serialize)]
pub(crate) struct SenderState {
    pub observed_ns: u64,
    pub queue_len: usize,
    pub max_buffered: usize,
    pub in_flight_pending: usize,
    pub max_pending: Option<usize>,
    pub available_rpc_permits: usize,
    pub active_keys: usize,
}

#[derive(Serialize)]
pub(crate) struct SenderTotals {
    pub interval: &'static str,
    pub source_wait_ns: u64,
    pub source_reads: u64,
    pub source_eof_ns: Option<u64>,
    pub source_end_ns: Option<u64>,
    pub source_end_reason: &'static str,
    pub flush_start_ns: Option<u64>,
    pub flush_end_ns: Option<u64>,
    pub flush_ok: Option<bool>,
    pub rpc_permit_capacity: usize,
    pub state: SenderState,
    // These are source-priority observations, not independently active gates.
    // Ready counts dispatches; empty and blocked gates count pump exits.
    pub gate_order: [&'static str; 6],
    pub last_observed_gate: &'static str,
    pub pump_observations: [u64; 6],
    pub completion_wait_at_last_gate_ns: [u64; 6],
    pub rate_sleep_ns: u64,
}

pub(crate) struct SenderPressure {
    pub log: PressureLog,
    pub totals: SenderTotals,
    pub http: Arc<HttpPressure>,
    last_gate: Gate,
}

impl SenderPressure {
    pub fn from_env(rpc_permit_capacity: usize) -> Option<Self> {
        let log = PressureLog::from_env("sender")?;
        Some(Self::with_log(log, rpc_permit_capacity))
    }

    fn with_log(log: PressureLog, rpc_permit_capacity: usize) -> Self {
        let http = Arc::new(HttpPressure::new(log.start()));
        Self {
            log,
            http,
            last_gate: Gate::Ready,
            totals: SenderTotals {
                interval: "measurement",
                source_wait_ns: 0,
                source_reads: 0,
                source_eof_ns: None,
                source_end_ns: None,
                source_end_reason: "unfinished",
                flush_start_ns: None,
                flush_end_ns: None,
                flush_ok: None,
                rpc_permit_capacity,
                state: SenderState::default(),
                gate_order: [
                    "ready",
                    "empty",
                    "pending",
                    "keys_or_setup",
                    "rpc_permit",
                    "rate_sleep",
                ],
                last_observed_gate: "ready",
                pump_observations: [0; 6],
                completion_wait_at_last_gate_ns: [0; 6],
                rate_sleep_ns: 0,
            },
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn for_test(rpc_permit_capacity: usize) -> Self {
        Self::with_log(PressureLog::for_test("sender"), rpc_permit_capacity)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn snapshot_for_test(&self) -> serde_json::Value {
        serde_json::json!({ "sender": &self.totals, "http": self.http.snapshot() })
    }

    pub fn gate(&mut self, gate: Gate) {
        self.last_gate = gate;
        self.totals.last_observed_gate = self.totals.gate_order[gate as usize];
        let count = &mut self.totals.pump_observations[gate as usize];
        *count = count.saturating_add(1);
    }

    pub fn completion_wait(&mut self, start: Instant) {
        let elapsed = &mut self.totals.completion_wait_at_last_gate_ns[self.last_gate as usize];
        *elapsed = elapsed.saturating_add(nanos(start.elapsed()));
    }

    pub fn emit(&mut self, terminal: bool) {
        #[derive(Serialize)]
        struct Data<'a> {
            sender: &'a SenderTotals,
            http: HttpSnapshot,
        }
        self.log.emit(&Data { sender: &self.totals, http: self.http.snapshot() }, terminal);
    }
}

impl Drop for SenderPressure {
    fn drop(&mut self) {
        // Error/unwinding paths are intentionally visible as unfinished streams.
        self.emit(true);
    }
}

pub(crate) struct HttpPressure {
    start: Instant,
    started: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    canceled: AtomicU64,
    active: AtomicU64,
    first_begin_ns: AtomicU64,
    last_begin_ns: AtomicU64,
}

#[derive(Serialize)]
struct HttpSnapshot {
    started: u64,
    succeeded: u64,
    failed: u64,
    canceled: u64,
    active: u64,
    first_begin_ns: Option<u64>,
    last_begin_ns: Option<u64>,
}

impl HttpPressure {
    fn new(start: Instant) -> Self {
        Self {
            start,
            started: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            canceled: AtomicU64::new(0),
            active: AtomicU64::new(0),
            first_begin_ns: AtomicU64::new(u64::MAX),
            last_begin_ns: AtomicU64::new(0),
        }
    }

    pub fn begin(self: &Arc<Self>) -> HttpRequest {
        let offset = nanos(self.start.elapsed());
        self.first_begin_ns.fetch_min(offset, Ordering::Relaxed);
        self.last_begin_ns.fetch_max(offset, Ordering::Relaxed);
        self.started.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::Relaxed);
        HttpRequest { counters: self.clone(), finished: false }
    }

    fn snapshot(&self) -> HttpSnapshot {
        let first = self.first_begin_ns.load(Ordering::Relaxed);
        HttpSnapshot {
            started: self.started.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            canceled: self.canceled.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            first_begin_ns: (first != u64::MAX).then_some(first),
            last_begin_ns: (first != u64::MAX).then(|| self.last_begin_ns.load(Ordering::Relaxed)),
        }
    }
}

pub(crate) struct HttpRequest {
    counters: Arc<HttpPressure>,
    finished: bool,
}

impl HttpRequest {
    pub fn finish(mut self, success: bool) {
        let counter = if success { &self.counters.succeeded } else { &self.counters.failed };
        counter.fetch_add(1, Ordering::Relaxed);
        self.finished = true;
    }
}

impl Drop for HttpRequest {
    fn drop(&mut self) {
        if !self.finished {
            self.counters.canceled.fetch_add(1, Ordering::Relaxed);
        }
        self.counters.active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_counts_close_after_success_failure_and_cancellation() {
        let counters = Arc::new(HttpPressure::new(Instant::now()));
        assert_eq!(counters.snapshot().first_begin_ns, None);
        let canceled = counters.begin();
        counters.begin().finish(true);
        counters.begin().finish(false);
        assert_eq!(counters.snapshot().active, 1);
        drop(canceled);
        let snapshot = counters.snapshot();
        assert_eq!(
            (
                snapshot.started,
                snapshot.succeeded,
                snapshot.failed,
                snapshot.canceled,
                snapshot.active
            ),
            (3, 1, 1, 1, 0)
        );
        assert!(snapshot.first_begin_ns <= snapshot.last_begin_ns);
    }
}

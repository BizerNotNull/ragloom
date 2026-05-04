//! Minimal ingest metrics shared by runtime, worker, and operator endpoints.
//!
//! # Why
//! Ragloom keeps observability dependency-free. These counters provide a stable
//! machine-readable view without introducing a metrics registry.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IngestionMetricsSnapshot {
    pub discovered_files_total: u64,
    pub indexed_files_total: u64,
    pub failed_files_total: u64,
    pub emitted_points_total: u64,
    pub pending_files: u64,
    pub retry_attempts_total: u64,
    pub retry_exhausted_total: u64,
    pub retry_queue_depth: u64,
    pub work_queue_depth: u64,
}

#[derive(Debug, Default)]
struct IngestionMetricsState {
    discovered_files_total: AtomicU64,
    indexed_files_total: AtomicU64,
    failed_files_total: AtomicU64,
    emitted_points_total: AtomicU64,
    pending_files: AtomicU64,
    retry_attempts_total: AtomicU64,
    retry_exhausted_total: AtomicU64,
    retry_queue_depth: AtomicU64,
    work_queue_depth: AtomicU64,
}

/// Monotonic ingest counters plus current queue/pending gauges.
#[derive(Debug, Clone, Default)]
pub struct IngestionMetrics {
    inner: Arc<IngestionMetricsState>,
}

impl IngestionMetrics {
    pub fn record_discovered(&self, count: usize) {
        if count == 0 {
            return;
        }

        let count = count as u64;
        self.inner
            .discovered_files_total
            .fetch_add(count, Ordering::Relaxed);
        self.inner.pending_files.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_work_queued(&self) {
        self.inner.work_queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_work_dequeued(&self) {
        self.inner
            .work_queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
    }

    pub fn record_success(&self, point_count: usize) {
        self.inner
            .indexed_files_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .emitted_points_total
            .fetch_add(point_count as u64, Ordering::Relaxed);
        self.decrement_pending();
    }

    pub fn record_failure(&self) {
        self.inner
            .failed_files_total
            .fetch_add(1, Ordering::Relaxed);
        self.decrement_pending();
    }

    pub fn record_retry_scheduled(&self, queue_depth: usize) {
        self.inner
            .retry_attempts_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .retry_queue_depth
            .store(queue_depth as u64, Ordering::Relaxed);
    }

    pub fn record_retry_exhausted(&self, queue_depth: usize) {
        self.inner
            .retry_exhausted_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .retry_queue_depth
            .store(queue_depth as u64, Ordering::Relaxed);
    }

    pub fn record_retry_dequeued(&self, queue_depth: usize) {
        self.inner
            .retry_queue_depth
            .store(queue_depth as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> IngestionMetricsSnapshot {
        IngestionMetricsSnapshot {
            discovered_files_total: self.inner.discovered_files_total.load(Ordering::Relaxed),
            indexed_files_total: self.inner.indexed_files_total.load(Ordering::Relaxed),
            failed_files_total: self.inner.failed_files_total.load(Ordering::Relaxed),
            emitted_points_total: self.inner.emitted_points_total.load(Ordering::Relaxed),
            pending_files: self.inner.pending_files.load(Ordering::Relaxed),
            retry_attempts_total: self.inner.retry_attempts_total.load(Ordering::Relaxed),
            retry_exhausted_total: self.inner.retry_exhausted_total.load(Ordering::Relaxed),
            retry_queue_depth: self.inner.retry_queue_depth.load(Ordering::Relaxed),
            work_queue_depth: self.inner.work_queue_depth.load(Ordering::Relaxed),
        }
    }

    fn decrement_pending(&self) {
        self.inner
            .pending_files
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_track_ingest_and_reliability_counters() {
        let metrics = IngestionMetrics::default();

        metrics.record_discovered(2);
        metrics.record_work_queued();
        metrics.record_work_queued();
        metrics.record_work_dequeued();
        metrics.record_retry_scheduled(1);
        metrics.record_success(3);
        metrics.record_retry_exhausted(0);
        metrics.record_failure();

        assert_eq!(
            metrics.snapshot(),
            IngestionMetricsSnapshot {
                discovered_files_total: 2,
                indexed_files_total: 1,
                failed_files_total: 1,
                emitted_points_total: 3,
                pending_files: 0,
                retry_attempts_total: 1,
                retry_exhausted_total: 1,
                retry_queue_depth: 0,
                work_queue_depth: 1,
            }
        );
    }
}

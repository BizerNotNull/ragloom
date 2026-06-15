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
    pub wal_bytes: u64,
    pub failed_work_bytes: u64,
    pub wal_pending_work: u64,
    pub failed_work_pending: u64,
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
    wal_bytes: AtomicU64,
    failed_work_bytes: AtomicU64,
    wal_pending_work: AtomicU64,
    failed_work_pending: AtomicU64,
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

    pub fn seed_durable_state(
        &self,
        wal_bytes: u64,
        failed_work_bytes: u64,
        wal_pending_work: usize,
        failed_work_pending: usize,
    ) {
        self.replace_durable_state(
            wal_bytes,
            failed_work_bytes,
            wal_pending_work,
            failed_work_pending,
        );
    }

    pub fn replace_durable_state(
        &self,
        wal_bytes: u64,
        failed_work_bytes: u64,
        wal_pending_work: usize,
        failed_work_pending: usize,
    ) {
        self.inner.wal_bytes.store(wal_bytes, Ordering::Relaxed);
        self.inner
            .failed_work_bytes
            .store(failed_work_bytes, Ordering::Relaxed);
        self.inner
            .wal_pending_work
            .store(wal_pending_work as u64, Ordering::Relaxed);
        self.inner
            .failed_work_pending
            .store(failed_work_pending as u64, Ordering::Relaxed);
    }

    pub fn record_wal_appended_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.inner
            .wal_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_failed_work_appended_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.inner
            .failed_work_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_wal_pending_increase(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.inner
            .wal_pending_work
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn record_wal_pending_decrease(&self, count: usize) {
        self.saturating_sub(&self.inner.wal_pending_work, count);
    }

    pub fn record_failed_work_pending_increase(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.inner
            .failed_work_pending
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn record_failed_work_pending_decrease(&self, count: usize) {
        self.saturating_sub(&self.inner.failed_work_pending, count);
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
            wal_bytes: self.inner.wal_bytes.load(Ordering::Relaxed),
            failed_work_bytes: self.inner.failed_work_bytes.load(Ordering::Relaxed),
            wal_pending_work: self.inner.wal_pending_work.load(Ordering::Relaxed),
            failed_work_pending: self.inner.failed_work_pending.load(Ordering::Relaxed),
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

    fn saturating_sub(&self, gauge: &AtomicU64, count: usize) {
        gauge
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(count as u64))
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
                wal_bytes: 0,
                failed_work_bytes: 0,
                wal_pending_work: 0,
                failed_work_pending: 0,
            }
        );
    }

    #[test]
    fn metrics_track_durable_state_gauges() {
        let metrics = IngestionMetrics::default();

        metrics.seed_durable_state(17, 9, 3, 2);
        metrics.record_wal_appended_bytes(11);
        metrics.record_failed_work_appended_bytes(7);
        metrics.record_wal_pending_increase(2);
        metrics.record_wal_pending_decrease(4);
        metrics.record_failed_work_pending_increase(3);
        metrics.record_failed_work_pending_decrease(4);

        assert_eq!(
            metrics.snapshot(),
            IngestionMetricsSnapshot {
                discovered_files_total: 0,
                indexed_files_total: 0,
                failed_files_total: 0,
                emitted_points_total: 0,
                pending_files: 0,
                retry_attempts_total: 0,
                retry_exhausted_total: 0,
                retry_queue_depth: 0,
                work_queue_depth: 0,
                wal_bytes: 28,
                failed_work_bytes: 16,
                wal_pending_work: 1,
                failed_work_pending: 1,
            }
        );
    }

    #[test]
    fn metrics_replace_durable_state_after_compaction() {
        let metrics = IngestionMetrics::default();

        metrics.seed_durable_state(30, 12, 5, 4);
        metrics.record_wal_appended_bytes(8);
        metrics.record_failed_work_appended_bytes(6);
        metrics.replace_durable_state(14, 5, 2, 1);

        assert_eq!(
            metrics.snapshot(),
            IngestionMetricsSnapshot {
                discovered_files_total: 0,
                indexed_files_total: 0,
                failed_files_total: 0,
                emitted_points_total: 0,
                pending_files: 0,
                retry_attempts_total: 0,
                retry_exhausted_total: 0,
                retry_queue_depth: 0,
                work_queue_depth: 0,
                wal_bytes: 14,
                failed_work_bytes: 5,
                wal_pending_work: 2,
                failed_work_pending: 1,
            }
        );
    }
}

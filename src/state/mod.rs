//! Persistent state primitives.
//!
//! # Why
//! Ragloom needs crash recovery and idempotency tracking. The core pipeline should
//! not be tied to a specific storage engine, so we define a small trait surface
//! that keeps the system open for extension.

pub mod compact;
pub mod failed;
pub mod wal;

use std::io::{BufRead, ErrorKind};
use std::path::Path;

use crate::error::{RagloomError, RagloomErrorKind};

/// Persistent state store abstraction.
///
/// # Why
/// The ingestion runtime must be able to record durable progress and query
/// outstanding work after restart. This trait defines that minimal contract
/// without locking the core into a concrete database.
pub trait StateStore: Send + Sync {}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DurableStateSnapshot {
    pub wal_bytes: u64,
    pub failed_work_bytes: u64,
    pub wal_pending_work: usize,
    pub failed_work_pending: usize,
}

pub fn durable_state_snapshot_from_paths(
    wal_path: &Path,
) -> Result<DurableStateSnapshot, RagloomError> {
    let failed_path = failed::failed_work_path_from_state_path(wal_path);
    let wal_records = read_state_records::<wal::WalRecord>(wal_path, "WAL")?;
    let failed_records =
        read_state_records::<failed::FailedWorkRecord>(&failed_path, "failed-work")?;

    Ok(DurableStateSnapshot {
        wal_bytes: file_len_or_zero(wal_path)?,
        failed_work_bytes: file_len_or_zero(&failed_path)?,
        wal_pending_work: wal::unacked_work_items(&wal_records).len(),
        failed_work_pending: failed::pending_failed_work(&failed_records).len(),
    })
}

fn file_len_or_zero(path: &Path) -> Result<u64, RagloomError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(0),
        Err(err) => Err(RagloomError::new(RagloomErrorKind::State, err)
            .with_context(format!("failed to stat state file: {}", path.display()))),
    }
}

fn read_state_records<T>(path: &Path, label: &str) -> Result<Vec<T>, RagloomError>
where
    T: serde::de::DeserializeOwned,
{
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(RagloomError::new(RagloomErrorKind::State, err)
                .with_context(format!("failed to read {label} file: {}", path.display())));
        }
    };

    let reader = std::io::BufReader::new(file);
    let mut records = Vec::new();
    for (idx, line_result) in reader.lines().enumerate() {
        let line = line_result.map_err(|e| {
            RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
                "failed to read {label} line {} in {}",
                idx + 1,
                path.display()
            ))
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<T>(&line).map_err(|e| {
            RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
                "failed to parse {label} record at line {} in {}",
                idx + 1,
                path.display()
            ))
        })?;
        records.push(record);
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::durable_state_snapshot_from_paths;
    use crate::ids::FileFingerprint;
    use crate::state::failed::{
        FailedWorkFailureKind, FailedWorkRecord, FailedWorkTerminalReason, FileFailedWorkStore,
        failed_work_path_from_state_path,
    };
    use crate::state::wal::{FileWal, WalRecord};

    #[test]
    fn durable_state_snapshot_reports_zero_for_missing_journals() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal_path = dir.path().join("state").join("wal.ndjson");

        let snapshot = durable_state_snapshot_from_paths(&wal_path).expect("state snapshot");

        assert_eq!(snapshot.wal_bytes, 0);
        assert_eq!(snapshot.failed_work_bytes, 0);
        assert_eq!(snapshot.wal_pending_work, 0);
        assert_eq!(snapshot.failed_work_pending, 0);
    }

    #[test]
    fn durable_state_snapshot_reports_sizes_and_pending_counts() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal_path = dir.path().join("wal.ndjson");
        let failed_path = failed_work_path_from_state_path(&wal_path);
        let fingerprint = FileFingerprint {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: None,
        };

        let mut wal = FileWal::open(&wal_path).expect("open wal");
        wal.append(WalRecord::WorkItemV2 {
            fingerprint: fingerprint.clone(),
        })
        .expect("append work");
        wal.append(WalRecord::DeleteDocument {
            canonical_path: "/x/b.txt".to_string(),
        })
        .expect("append delete");
        wal.append(WalRecord::SinkAckV2 {
            fingerprint: fingerprint.clone(),
        })
        .expect("append ack");

        let mut failed = FileFailedWorkStore::open(&failed_path).expect("open failed");
        failed
            .append(FailedWorkRecord::Exhausted {
                id: 1,
                work: WalRecord::DeleteDocument {
                    canonical_path: "/x/b.txt".to_string(),
                },
                failure_kind: FailedWorkFailureKind::Sink,
                terminal_reason: FailedWorkTerminalReason::RetryExhausted,
                attempts: 2,
            })
            .expect("append failed");

        let snapshot = durable_state_snapshot_from_paths(&wal_path).expect("state snapshot");

        assert_eq!(
            snapshot.wal_bytes,
            std::fs::metadata(&wal_path).expect("wal metadata").len()
        );
        assert_eq!(
            snapshot.failed_work_bytes,
            std::fs::metadata(&failed_path)
                .expect("failed metadata")
                .len()
        );
        assert_eq!(snapshot.wal_pending_work, 1);
        assert_eq!(snapshot.failed_work_pending, 1);
    }
}

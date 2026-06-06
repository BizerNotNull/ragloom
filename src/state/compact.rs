//! Crash-safe state compaction for append-only journals.
//!
//! # Why
//! Ragloom's WAL and failed-work journal intentionally grow by durable appends.
//! Explicit compaction lets operators reclaim space while preserving the
//! replay, acknowledgement, and delete-sync behavior derived from those logs.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{RagloomError, RagloomErrorKind};
use crate::state::failed::{FailedWorkRecord, next_failed_work_id, pending_failed_work};
use crate::state::wal::WalRecord;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalCompactionSummary {
    pub records_before: usize,
    pub records_after: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateCompactionSummary {
    pub wal: JournalCompactionSummary,
    pub failed_work: JournalCompactionSummary,
}

pub fn compact_wal_records(records: &[WalRecord]) -> Vec<WalRecord> {
    let mut legacy = std::collections::BTreeMap::<[u8; 32], (usize, WalRecord)>::new();
    let mut live_files = std::collections::BTreeMap::<String, (usize, WalRecord)>::new();
    let mut pending_deletes = std::collections::BTreeMap::<String, (usize, WalRecord)>::new();

    for (idx, record) in records.iter().enumerate() {
        match record {
            WalRecord::WorkItem { chunk_id } | WalRecord::SinkAck { chunk_id } => {
                legacy.insert(*chunk_id, (idx, record.clone()));
            }
            WalRecord::WorkItemV2 { fingerprint } | WalRecord::SinkAckV2 { fingerprint } => {
                pending_deletes.remove(&fingerprint.canonical_path);
                live_files.insert(fingerprint.canonical_path.clone(), (idx, record.clone()));
            }
            WalRecord::DeleteDocument { canonical_path } => {
                live_files.remove(canonical_path);
                pending_deletes.insert(canonical_path.clone(), (idx, record.clone()));
            }
            WalRecord::DeleteAck { canonical_path } => {
                pending_deletes.remove(canonical_path);
            }
        }
    }

    let mut compacted = legacy
        .into_values()
        .chain(live_files.into_values())
        .chain(pending_deletes.into_values())
        .collect::<Vec<_>>();
    compacted.sort_by_key(|(idx, _)| *idx);
    compacted.into_iter().map(|(_, record)| record).collect()
}

pub fn compact_failed_work_records(records: &[FailedWorkRecord]) -> Vec<FailedWorkRecord> {
    let mut exhausted_by_id = std::collections::BTreeMap::<u64, (usize, FailedWorkRecord)>::new();
    let mut requeued_by_id = std::collections::BTreeMap::<u64, (usize, FailedWorkRecord)>::new();

    for (idx, record) in records.iter().enumerate() {
        match record {
            FailedWorkRecord::Exhausted { id, .. } => {
                exhausted_by_id.insert(*id, (idx, record.clone()));
            }
            FailedWorkRecord::Requeued { exhausted_id } => {
                requeued_by_id.insert(*exhausted_id, (idx, record.clone()));
            }
        }
    }

    let mut keep = pending_failed_work(records)
        .into_iter()
        .filter_map(|pending| exhausted_by_id.get(&pending.id).cloned())
        .collect::<Vec<_>>();

    let highest_id = next_failed_work_id(records).saturating_sub(1);
    if highest_id != 0
        && !keep.iter().any(|(_, record)| {
            matches!(record, FailedWorkRecord::Exhausted { id, .. } if *id == highest_id)
        })
        && let (Some(exhausted), Some(requeued)) = (
            exhausted_by_id.get(&highest_id),
            requeued_by_id.get(&highest_id),
        )
    {
        keep.push(exhausted.clone());
        keep.push(requeued.clone());
    }

    keep.sort_by_key(|(idx, _)| *idx);
    keep.into_iter().map(|(_, record)| record).collect()
}

pub fn compact_state_files(
    wal_path: &Path,
    failed_work_path: &Path,
) -> Result<StateCompactionSummary, RagloomError> {
    let wal_records = crate::state::wal::FileWal::open(wal_path)
        .map_err(|e| e.with_context("failed to initialize persistent WAL"))?
        .read_all()
        .map_err(|e| e.with_context("failed to read WAL before compaction"))?;
    let failed_records = crate::state::failed::FileFailedWorkStore::open(failed_work_path)
        .map_err(|e| e.with_context("failed to initialize failed-work store"))?
        .read_all()
        .map_err(|e| e.with_context("failed to read failed-work store before compaction"))?;

    let compacted_wal = compact_wal_records(&wal_records);
    let compacted_failed = compact_failed_work_records(&failed_records);

    let wal_summary = rewrite_journal_file(
        wal_path,
        &wal_records,
        &compacted_wal,
        serde_json::to_string,
        "WAL",
    )?;
    let failed_work_summary = rewrite_journal_file(
        failed_work_path,
        &failed_records,
        &compacted_failed,
        serde_json::to_string,
        "failed-work",
    )?;

    Ok(StateCompactionSummary {
        wal: wal_summary,
        failed_work: failed_work_summary,
    })
}

fn rewrite_journal_file<T>(
    path: &Path,
    before: &[T],
    after: &[T],
    encode: impl Fn(&T) -> Result<String, serde_json::Error>,
    label: &str,
) -> Result<JournalCompactionSummary, RagloomError> {
    let bytes_before = file_len_or_zero(path)?;
    let temp_path = temp_compaction_path(path);
    rewrite_journal_file_with_temp_path(path, &temp_path, after, &encode, label)?;
    let bytes_after = file_len_or_zero(path)?;

    Ok(JournalCompactionSummary {
        records_before: before.len(),
        records_after: after.len(),
        bytes_before,
        bytes_after,
    })
}

fn rewrite_journal_file_with_temp_path<T>(
    path: &Path,
    temp_path: &Path,
    records: &[T],
    encode: &impl Fn(&T) -> Result<String, serde_json::Error>,
    label: &str,
) -> Result<(), RagloomError> {
    let mut temp_file = create_temp_compaction_file(temp_path, label)?;
    write_ndjson_records(&mut temp_file, records, encode, path, label)?;
    temp_file.sync_data().map_err(|e| {
        RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
            "failed to sync compacted {label} temp file: {}",
            temp_path.display()
        ))
    })?;
    drop(temp_file);

    replace_file_with_temp(path, temp_path, label).inspect_err(|_| {
        let _ = std::fs::remove_file(temp_path);
    })?;
    sync_parent_directory(path, label)?;
    Ok(())
}

fn write_ndjson_records<T>(
    file: &mut File,
    records: &[T],
    encode: &impl Fn(&T) -> Result<String, serde_json::Error>,
    path: &Path,
    label: &str,
) -> Result<(), RagloomError> {
    for record in records {
        let encoded = encode(record).map_err(|e| {
            RagloomError::new(RagloomErrorKind::State, e)
                .with_context(format!("failed to encode compacted {label} record"))
        })?;
        writeln!(file, "{encoded}").map_err(|e| {
            RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
                "failed to write compacted {label} temp file for {}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

fn file_len_or_zero(path: &Path) -> Result<u64, RagloomError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(RagloomError::new(RagloomErrorKind::State, err)
            .with_context(format!("failed to stat state file: {}", path.display()))),
    }
}

fn temp_compaction_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.ndjson");
    path.with_file_name(format!("{file_name}.compact.tmp"))
}

fn create_temp_compaction_file(path: &Path, label: &str) -> Result<File, RagloomError> {
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|e| {
            RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
                "failed to create compacted {label} temp file: {}",
                path.display()
            ))
        })
}

fn replace_file_with_temp(path: &Path, temp_path: &Path, label: &str) -> Result<(), RagloomError> {
    #[cfg(unix)]
    {
        std::fs::rename(temp_path, path).map_err(|e| {
            RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
                "failed to replace {label} file with compacted temp file: {}",
                path.display()
            ))
        })?;
        return Ok(());
    }

    #[cfg(windows)]
    {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;

        fn wide(s: &OsStr) -> Vec<u16> {
            s.encode_wide().chain(std::iter::once(0)).collect()
        }

        unsafe extern "system" {
            fn ReplaceFileW(
                lpReplacedFileName: *const u16,
                lpReplacementFileName: *const u16,
                lpBackupFileName: *const u16,
                dwReplaceFlags: u32,
                lpExclude: *mut core::ffi::c_void,
                lpReserved: *mut core::ffi::c_void,
            ) -> i32;
        }

        const REPLACEFILE_WRITE_THROUGH: u32 = 0x0000_0001;
        const REPLACEFILE_IGNORE_MERGE_ERRORS: u32 = 0x0000_0002;

        let target = wide(path.as_os_str());
        let replacement = wide(temp_path.as_os_str());

        // Use the native replace primitive on Windows so compaction can
        // replace an existing journal without first deleting the original.
        let replaced = unsafe {
            ReplaceFileW(
                target.as_ptr(),
                replacement.as_ptr(),
                std::ptr::null(),
                REPLACEFILE_WRITE_THROUGH | REPLACEFILE_IGNORE_MERGE_ERRORS,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if replaced == 0 {
            return Err(RagloomError::new(
                RagloomErrorKind::State,
                std::io::Error::last_os_error(),
            )
            .with_context(format!(
                "failed to replace {label} file with compacted temp file: {}",
                path.display()
            )));
        }
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, temp_path, label);
        Err(RagloomError::from_kind(RagloomErrorKind::State)
            .with_context("state compaction file replacement is unsupported on this platform"))
    }
}

fn sync_parent_directory(path: &Path, label: &str) -> Result<(), RagloomError> {
    #[cfg(unix)]
    {
        let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        else {
            return Ok(());
        };
        File::open(parent)
            .map_err(|e| {
                RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
                    "failed to open {label} parent directory: {}",
                    parent.display()
                ))
            })?
            .sync_all()
            .map_err(|e| {
                RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
                    "failed to sync {label} parent directory: {}",
                    parent.display()
                ))
            })?;
    }

    #[cfg(not(unix))]
    {
        let _ = (path, label);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::FileFingerprint;
    use crate::pipeline::planner::Planner;
    use crate::source::FileVersionDiscovered;
    use crate::state::failed::{
        FailedWorkFailureKind, FailedWorkTerminalReason, pending_failed_work,
    };
    use crate::state::wal::{known_live_document_paths, unacked_work_items};
    use tempfile::tempdir;

    fn work(path: &str, mtime: i64) -> WalRecord {
        WalRecord::WorkItemV2 {
            fingerprint: FileFingerprint {
                canonical_path: path.to_string(),
                size_bytes: 10,
                mtime_unix_secs: mtime,
                etag: None,
            },
        }
    }

    fn ack(path: &str, mtime: i64) -> WalRecord {
        match work(path, mtime) {
            WalRecord::WorkItemV2 { fingerprint } => WalRecord::SinkAckV2 { fingerprint },
            _ => unreachable!(),
        }
    }

    #[test]
    fn compact_wal_records_preserves_replay_and_live_document_observables() {
        let records = vec![
            work("/x/a.txt", 100),
            ack("/x/a.txt", 100),
            work("/x/b.txt", 200),
            WalRecord::DeleteDocument {
                canonical_path: "/x/a.txt".to_string(),
            },
            WalRecord::DeleteAck {
                canonical_path: "/x/a.txt".to_string(),
            },
            work("/x/c.txt", 300),
            ack("/x/c.txt", 300),
            WalRecord::DeleteDocument {
                canonical_path: "/x/d.txt".to_string(),
            },
            WalRecord::WorkItem {
                chunk_id: [7u8; 32],
            },
            WalRecord::SinkAck {
                chunk_id: [7u8; 32],
            },
        ];

        let compacted = compact_wal_records(&records);

        assert_eq!(unacked_work_items(&compacted), unacked_work_items(&records));
        assert_eq!(
            known_live_document_paths(&compacted),
            known_live_document_paths(&records)
        );

        let discovered = FileVersionDiscovered {
            fingerprint: FileFingerprint {
                canonical_path: "/x/c.txt".to_string(),
                size_bytes: 10,
                mtime_unix_secs: 300,
                etag: None,
            },
            file_version_id: crate::ids::file_version_id(&FileFingerprint {
                canonical_path: "/x/c.txt".to_string(),
                size_bytes: 10,
                mtime_unix_secs: 300,
                etag: None,
            }),
        };

        let original_wal = std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::state::wal::InMemoryWal::new(),
        ));
        let compacted_wal = std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::state::wal::InMemoryWal::new(),
        ));
        let mut original_planner = Planner::from_wal_records(&records);
        let mut compacted_planner = Planner::from_wal_records(&compacted);
        original_planner
            .plan_file_version(&discovered, &original_wal)
            .expect("plan from original");
        compacted_planner
            .plan_file_version(&discovered, &compacted_wal)
            .expect("plan from compacted");
        assert_eq!(
            original_wal
                .try_lock()
                .expect("original wal")
                .read_all()
                .expect("read original wal"),
            compacted_wal
                .try_lock()
                .expect("compacted wal")
                .read_all()
                .expect("read compacted wal")
        );
    }

    #[test]
    fn compact_wal_records_is_idempotent() {
        let records = vec![
            work("/x/a.txt", 100),
            ack("/x/a.txt", 100),
            WalRecord::DeleteDocument {
                canonical_path: "/x/b.txt".to_string(),
            },
        ];

        let once = compact_wal_records(&records);
        let twice = compact_wal_records(&once);
        assert_eq!(once, twice);
    }

    #[test]
    fn compact_failed_work_records_preserves_pending_and_next_id() {
        let records = vec![
            FailedWorkRecord::Exhausted {
                id: 1,
                work: work("/x/a.txt", 100),
                failure_kind: FailedWorkFailureKind::Embed,
                terminal_reason: FailedWorkTerminalReason::RetryExhausted,
                attempts: 3,
            },
            FailedWorkRecord::Requeued { exhausted_id: 1 },
            FailedWorkRecord::Exhausted {
                id: 2,
                work: work("/x/b.txt", 200),
                failure_kind: FailedWorkFailureKind::Sink,
                terminal_reason: FailedWorkTerminalReason::NonRetryable,
                attempts: 1,
            },
        ];

        let compacted = compact_failed_work_records(&records);

        assert_eq!(
            pending_failed_work(&compacted),
            pending_failed_work(&records)
        );
        assert_eq!(
            next_failed_work_id(&compacted),
            next_failed_work_id(&records)
        );
    }

    #[test]
    fn compact_failed_work_records_is_idempotent() {
        let records = vec![
            FailedWorkRecord::Exhausted {
                id: 9,
                work: work("/x/a.txt", 100),
                failure_kind: FailedWorkFailureKind::Io,
                terminal_reason: FailedWorkTerminalReason::RetryExhausted,
                attempts: 2,
            },
            FailedWorkRecord::Requeued { exhausted_id: 9 },
        ];

        let once = compact_failed_work_records(&records);
        let twice = compact_failed_work_records(&once);
        assert_eq!(once, twice);
    }

    #[test]
    fn rewrite_journal_file_with_temp_path_keeps_original_when_temp_creation_fails() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("wal.ndjson");
        std::fs::write(&path, "{\"type\":\"work_item\",\"chunk_id\":[1]}\n").expect("write");
        let temp_path = dir.path().join("wal.ndjson.compact.tmp");
        std::fs::write(&temp_path, "busy").expect("precreate temp");

        let err = rewrite_journal_file_with_temp_path(
            &path,
            &temp_path,
            &[WalRecord::DeleteDocument {
                canonical_path: "/x/a.txt".to_string(),
            }],
            &|record| serde_json::to_string(record),
            "WAL",
        )
        .expect_err("temp creation should fail");

        assert!(
            err.to_string()
                .contains("failed to create compacted WAL temp file")
        );
        assert_eq!(
            std::fs::read_to_string(&path).expect("read original"),
            "{\"type\":\"work_item\",\"chunk_id\":[1]}\n"
        );
    }
}

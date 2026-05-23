//! Local filesystem discovery and change detection.
//!
//! # Why
//! The daemon needs a reliable way to discover new/changed files without
//! coupling discovery to downstream processing. The file tailer emits
//! file-version events using the MVP fingerprint strategy.

use std::collections::{HashMap, HashSet};

use crate::ids::{FileFingerprint, file_version_id};
use crate::source::{FileVersionDiscovered, SourceEvent};

/// Internal representation of file metadata used for deterministic tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedFileMeta {
    pub canonical_path: String,
    pub size_bytes: u64,
    pub mtime_unix_secs: i64,
    pub etag: Option<String>,
}

impl ObservedFileMeta {
    fn to_fingerprint(&self) -> FileFingerprint {
        FileFingerprint {
            canonical_path: self.canonical_path.clone(),
            size_bytes: self.size_bytes,
            mtime_unix_secs: self.mtime_unix_secs,
            etag: self.etag.clone(),
        }
    }
}

/// A minimal file tailer state machine.
///
/// # Why
/// For TDD, we separate the pure state machine (tested via injected observations)
/// from any real filesystem scanning/watching implementation.
#[derive(Debug, Default)]
pub struct FileTailer {
    last_seen_version: HashMap<String, Option<[u8; 32]>>,
    pending: Vec<SourceEvent>,
}

impl FileTailer {
    /// Constructs a new tailer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs a tailer seeded with previously observed canonical paths.
    ///
    /// # Why
    /// Restart-time delete detection only needs path membership. We seed
    /// placeholder versions so a first completed scan can emit delete events
    /// for files removed while the process was offline.
    pub fn with_previously_observed_paths(
        canonical_paths: impl IntoIterator<Item = String>,
    ) -> Self {
        let last_seen_version = canonical_paths
            .into_iter()
            .map(|canonical_path| (canonical_path, None))
            .collect();
        Self {
            last_seen_version,
            pending: Vec::new(),
        }
    }

    /// Feeds an observation into the tailer.
    ///
    /// # Why
    /// This method enables deterministic unit tests without touching the OS.
    pub fn observe(&mut self, meta: ObservedFileMeta) {
        let fingerprint = meta.to_fingerprint();
        let version_id = file_version_id(&fingerprint);

        let should_emit = self
            .last_seen_version
            .get(&fingerprint.canonical_path)
            .map(|existing| existing.as_ref() != Some(&version_id))
            .unwrap_or(true);

        if should_emit {
            self.last_seen_version
                .insert(fingerprint.canonical_path.clone(), Some(version_id));
            self.pending
                .push(SourceEvent::FileVersionDiscovered(FileVersionDiscovered {
                    fingerprint,
                    file_version_id: version_id,
                }));
        }
    }

    /// Marks a completed scan and emits deletes for previously seen paths that
    /// were absent from that scan.
    pub fn complete_scan(&mut self, observed_paths: &HashSet<String>) {
        let pending = &mut self.pending;
        self.last_seen_version.retain(|canonical_path, _| {
            if observed_paths.contains(canonical_path) {
                true
            } else {
                pending.push(SourceEvent::FileDeleted {
                    canonical_path: canonical_path.clone(),
                });
                false
            }
        });
    }

    /// Drains pending discovery events.
    pub fn drain(&mut self) -> Vec<SourceEvent> {
        std::mem::take(&mut self.pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_fingerprint_does_not_emit_new_version_event() {
        let mut tailer = FileTailer::new();

        tailer.observe(ObservedFileMeta {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: None,
        });
        assert_eq!(tailer.drain().len(), 1);

        tailer.observe(ObservedFileMeta {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: None,
        });
        assert_eq!(tailer.drain().len(), 0);
    }

    #[test]
    fn changed_mtime_emits_new_version_event() {
        let mut tailer = FileTailer::new();

        tailer.observe(ObservedFileMeta {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: None,
        });
        let first = tailer.drain();
        assert_eq!(first.len(), 1);

        tailer.observe(ObservedFileMeta {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 101,
            etag: None,
        });
        let second = tailer.drain();
        assert_eq!(second.len(), 1);
        let first_version = match &first[0] {
            SourceEvent::FileVersionDiscovered(discovered) => discovered.file_version_id,
            SourceEvent::FileDeleted { .. } => panic!("expected discovery"),
        };
        let second_version = match &second[0] {
            SourceEvent::FileVersionDiscovered(discovered) => discovered.file_version_id,
            SourceEvent::FileDeleted { .. } => panic!("expected discovery"),
        };
        assert_ne!(first_version, second_version);
    }

    #[test]
    fn completed_scan_emits_delete_for_previously_seen_missing_path_once() {
        let mut tailer = FileTailer::new();
        tailer.observe(ObservedFileMeta {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: None,
        });
        tailer.drain();

        tailer.complete_scan(&HashSet::new());
        assert_eq!(
            tailer.drain(),
            vec![SourceEvent::FileDeleted {
                canonical_path: "/x/a.txt".to_string()
            }]
        );

        tailer.complete_scan(&HashSet::new());
        assert!(tailer.drain().is_empty());
    }

    #[test]
    fn seeded_missing_path_emits_delete_on_first_completed_scan() {
        let mut tailer = FileTailer::with_previously_observed_paths(["/x/a.txt".to_string()]);

        tailer.complete_scan(&HashSet::new());

        assert_eq!(
            tailer.drain(),
            vec![SourceEvent::FileDeleted {
                canonical_path: "/x/a.txt".to_string()
            }]
        );
    }

    #[test]
    fn seeded_existing_path_does_not_emit_spurious_delete() {
        let mut tailer = FileTailer::with_previously_observed_paths(["/x/a.txt".to_string()]);
        let observed = HashSet::from(["/x/a.txt".to_string()]);

        tailer.complete_scan(&observed);

        assert!(tailer.drain().is_empty());
    }

    #[test]
    fn seeded_existing_path_emits_discovery_once_on_first_observe() {
        let mut tailer = FileTailer::with_previously_observed_paths(["/x/a.txt".to_string()]);

        tailer.observe(ObservedFileMeta {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: None,
        });
        assert_eq!(tailer.drain().len(), 1);

        tailer.observe(ObservedFileMeta {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: None,
        });
        assert!(tailer.drain().is_empty());
    }

    #[test]
    fn s3_etag_change_emits_new_version_event() {
        let mut tailer = FileTailer::new();

        tailer.observe(ObservedFileMeta {
            canonical_path: "s3://docs-bucket/kb/a.md".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: Some("\"etag-a\"".to_string()),
        });
        assert_eq!(tailer.drain().len(), 1);

        tailer.observe(ObservedFileMeta {
            canonical_path: "s3://docs-bucket/kb/a.md".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: Some("\"etag-b\"".to_string()),
        });
        assert_eq!(tailer.drain().len(), 1);
    }

    #[test]
    fn s3_rename_behaves_as_delete_plus_new_document() {
        let mut tailer = FileTailer::new();

        let old_path = "s3://docs-bucket/kb/old.md".to_string();
        let new_path = "s3://docs-bucket/kb/new.md".to_string();

        tailer.observe(ObservedFileMeta {
            canonical_path: old_path.clone(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: Some("\"etag-a\"".to_string()),
        });
        tailer.drain();

        let mut observed = HashSet::new();
        observed.insert(new_path.clone());
        tailer.observe(ObservedFileMeta {
            canonical_path: new_path.clone(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: Some("\"etag-a\"".to_string()),
        });
        tailer.complete_scan(&observed);

        assert_eq!(
            tailer.drain(),
            vec![
                SourceEvent::FileVersionDiscovered(FileVersionDiscovered {
                    fingerprint: FileFingerprint {
                        canonical_path: new_path,
                        size_bytes: 10,
                        mtime_unix_secs: 100,
                        etag: Some("\"etag-a\"".to_string()),
                    },
                    file_version_id: crate::ids::file_version_id(&FileFingerprint {
                        canonical_path: "s3://docs-bucket/kb/new.md".to_string(),
                        size_bytes: 10,
                        mtime_unix_secs: 100,
                        etag: Some("\"etag-a\"".to_string()),
                    }),
                }),
                SourceEvent::FileDeleted {
                    canonical_path: old_path
                }
            ]
        );
    }
}

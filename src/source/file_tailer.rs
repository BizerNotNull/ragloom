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
}

impl ObservedFileMeta {
    fn to_fingerprint(&self) -> FileFingerprint {
        FileFingerprint {
            canonical_path: self.canonical_path.clone(),
            size_bytes: self.size_bytes,
            mtime_unix_secs: self.mtime_unix_secs,
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
    last_seen_version: HashMap<String, [u8; 32]>,
    pending: Vec<SourceEvent>,
}

impl FileTailer {
    /// Constructs a new tailer.
    pub fn new() -> Self {
        Self::default()
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
            .map(|existing| existing != &version_id)
            .unwrap_or(true);

        if should_emit {
            self.last_seen_version
                .insert(fingerprint.canonical_path.clone(), version_id);
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
        let mut deleted_paths: Vec<String> = self
            .last_seen_version
            .keys()
            .filter(|path| !observed_paths.contains(*path))
            .cloned()
            .collect();
        deleted_paths.sort();

        for canonical_path in deleted_paths {
            self.last_seen_version.remove(&canonical_path);
            self.pending
                .push(SourceEvent::FileDeleted { canonical_path });
        }
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
        });
        assert_eq!(tailer.drain().len(), 1);

        tailer.observe(ObservedFileMeta {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
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
        });
        let first = tailer.drain();
        assert_eq!(first.len(), 1);

        tailer.observe(ObservedFileMeta {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 101,
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
}

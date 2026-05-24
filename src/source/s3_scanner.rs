use std::collections::HashSet;
use std::sync::Arc;

use crate::RagloomError;
use crate::s3::{S3Client, canonical_s3_path};
use crate::source::file_tailer::{FileTailer, ObservedFileMeta};
use crate::source::{Source, SourceEvent};

#[derive(Debug)]
pub struct S3PollingSource {
    bucket: String,
    prefix: Option<String>,
    client: Arc<dyn S3Client>,
    tailer: FileTailer,
}

impl S3PollingSource {
    pub fn with_previously_observed_paths(
        bucket: impl Into<String>,
        prefix: Option<String>,
        client: Arc<dyn S3Client>,
        canonical_paths: HashSet<String>,
    ) -> Result<Self, RagloomError> {
        let bucket = bucket.into();
        if client.bucket_name() != bucket {
            return Err(
                crate::RagloomError::from_kind(crate::RagloomErrorKind::Config).with_context(
                    format!(
                        "configured S3 bucket {bucket} does not match S3 client bucket {}",
                        client.bucket_name()
                    ),
                ),
            );
        }
        Ok(Self {
            bucket,
            prefix,
            client,
            tailer: FileTailer::with_previously_observed_paths(canonical_paths),
        })
    }

    fn observe_bucket_once(&mut self) -> Result<(), RagloomError> {
        let mut objects = self
            .client
            .list_objects(self.prefix.as_deref())
            .map_err(|e| {
                crate::RagloomError::new(e.kind, e).with_context(format!(
                    "failed to poll S3 source s3://{}/{}",
                    self.bucket,
                    self.prefix.as_deref().unwrap_or("")
                ))
            })?;
        objects.sort_by(|left, right| left.key.cmp(&right.key));

        let mut observed_paths = HashSet::new();
        for object in objects {
            let canonical_path = canonical_s3_path(&self.bucket, &object.key);
            observed_paths.insert(canonical_path.clone());
            self.tailer.observe(ObservedFileMeta {
                canonical_path,
                size_bytes: object.size_bytes,
                mtime_unix_secs: object.mtime_unix_secs,
                etag: object.etag,
            });
        }
        self.tailer.complete_scan(&observed_paths);
        Ok(())
    }
}

impl Source for S3PollingSource {
    fn poll(&mut self) -> Vec<SourceEvent> {
        if let Err(err) = self.observe_bucket_once() {
            tracing::warn!(
                event.name = "ragloom.source.s3.poll_failed",
                error.kind = %err.kind,
                error.message = %err,
                bucket = %self.bucket,
                prefix = self.prefix.as_deref().unwrap_or(""),
                "ragloom.source.s3.poll_failed"
            );
        }
        self.tailer.drain()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use crate::s3::S3ObjectMeta;
    use crate::source::FileVersionDiscovered;

    #[derive(Debug)]
    struct FakeS3Client {
        listings: Mutex<VecDeque<Result<Vec<S3ObjectMeta>, RagloomError>>>,
    }

    impl FakeS3Client {
        fn new(
            listings: impl IntoIterator<Item = Result<Vec<S3ObjectMeta>, RagloomError>>,
        ) -> Self {
            Self {
                listings: Mutex::new(listings.into_iter().collect()),
            }
        }
    }

    impl S3Client for FakeS3Client {
        fn bucket_name(&self) -> &str {
            "docs-bucket"
        }

        fn list_objects(&self, _prefix: Option<&str>) -> Result<Vec<S3ObjectMeta>, RagloomError> {
            self.listings
                .lock()
                .expect("lock")
                .pop_front()
                .unwrap_or_else(|| Ok(Vec::new()))
        }

        fn get_object(&self, _key: &str) -> Result<Vec<u8>, RagloomError> {
            unreachable!("loader only")
        }
    }

    fn object(
        key: &str,
        size_bytes: u64,
        mtime_unix_secs: i64,
        etag: Option<&str>,
    ) -> S3ObjectMeta {
        S3ObjectMeta {
            key: key.to_string(),
            size_bytes,
            mtime_unix_secs,
            etag: etag.map(str::to_string),
        }
    }

    #[test]
    fn first_poll_emits_discoveries_in_sorted_key_order() {
        let client = Arc::new(FakeS3Client::new([Ok(vec![
            object("kb/z.md", 1, 20, Some("\"z\"")),
            object("kb/a.md", 1, 10, Some("\"a\"")),
        ])]));
        let mut source = S3PollingSource::with_previously_observed_paths(
            "docs-bucket",
            Some("kb/".to_string()),
            client,
            HashSet::new(),
        )
        .expect("create source");

        let events = source.poll();
        let paths: Vec<String> = events
            .into_iter()
            .map(|event| match event {
                SourceEvent::FileVersionDiscovered(FileVersionDiscovered {
                    fingerprint, ..
                }) => fingerprint.canonical_path,
                SourceEvent::FileDeleted { canonical_path } => canonical_path,
            })
            .collect();

        assert_eq!(
            paths,
            vec![
                "s3://docs-bucket/kb/a.md".to_string(),
                "s3://docs-bucket/kb/z.md".to_string()
            ]
        );
    }

    #[test]
    fn later_poll_with_missing_key_emits_delete() {
        let client = Arc::new(FakeS3Client::new([
            Ok(vec![
                object("kb/a.md", 1, 10, Some("\"a\"")),
                object("kb/b.md", 1, 10, Some("\"b\"")),
            ]),
            Ok(vec![object("kb/a.md", 1, 10, Some("\"a\""))]),
        ]));
        let mut source = S3PollingSource::with_previously_observed_paths(
            "docs-bucket",
            Some("kb/".to_string()),
            client,
            HashSet::new(),
        )
        .expect("create source");

        assert_eq!(source.poll().len(), 2);
        assert_eq!(
            source.poll(),
            vec![SourceEvent::FileDeleted {
                canonical_path: "s3://docs-bucket/kb/b.md".to_string()
            }]
        );
    }

    #[test]
    fn later_poll_with_changed_etag_emits_new_version() {
        let client = Arc::new(FakeS3Client::new([
            Ok(vec![object("kb/a.md", 1, 10, Some("\"etag-a\""))]),
            Ok(vec![object("kb/a.md", 1, 10, Some("\"etag-b\""))]),
        ]));
        let mut source = S3PollingSource::with_previously_observed_paths(
            "docs-bucket",
            Some("kb/".to_string()),
            client,
            HashSet::new(),
        )
        .expect("create source");

        let first = source.poll();
        let second = source.poll();

        let first_id = match &first[0] {
            SourceEvent::FileVersionDiscovered(FileVersionDiscovered {
                file_version_id, ..
            }) => *file_version_id,
            SourceEvent::FileDeleted { .. } => panic!("expected discovery"),
        };
        let second_id = match &second[0] {
            SourceEvent::FileVersionDiscovered(FileVersionDiscovered {
                file_version_id, ..
            }) => *file_version_id,
            SourceEvent::FileDeleted { .. } => panic!("expected discovery"),
        };

        assert_ne!(first_id, second_id);
    }

    #[test]
    fn constructor_rejects_bucket_mismatch_with_client() {
        let client = Arc::new(FakeS3Client::new([Ok(Vec::new())]));

        let err = S3PollingSource::with_previously_observed_paths(
            "other-bucket",
            Some("kb/".to_string()),
            client,
            HashSet::new(),
        )
        .expect_err("expected bucket mismatch");

        assert_eq!(err.kind, crate::RagloomErrorKind::Config);
        assert!(err.to_string().contains("does not match S3 client bucket"));
    }
}

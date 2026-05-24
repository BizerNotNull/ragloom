use std::sync::Arc;

use async_trait::async_trait;

use crate::doc::{DocumentLoader, LoadedDocument};
use crate::s3::{S3Client, parse_s3_uri};
use crate::{RagloomError, RagloomErrorKind};

#[derive(Debug, Clone)]
pub struct S3Utf8Loader {
    client: Arc<dyn S3Client>,
}

impl S3Utf8Loader {
    pub fn new(client: Arc<dyn S3Client>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DocumentLoader for S3Utf8Loader {
    async fn load(&self, path: &str) -> Result<LoadedDocument, RagloomError> {
        let location = parse_s3_uri(path)?;
        if location.bucket != self.client.bucket_name() {
            return Err(
                RagloomError::from_kind(RagloomErrorKind::InvalidInput).with_context(format!(
                    "S3 canonical path bucket {} does not match configured bucket {}",
                    location.bucket,
                    self.client.bucket_name()
                )),
            );
        }
        let client = Arc::clone(&self.client);
        let key = location.key.to_string();
        let bytes = tokio::task::spawn_blocking(move || client.get_object(&key))
            .await
            .map_err(|e| {
                RagloomError::new(RagloomErrorKind::Internal, e)
                    .with_context("failed to join S3 object read task")
            })?
            .map_err(|e| {
                RagloomError::new(e.kind, e).with_context("failed to load document bytes")
            })?;

        let text = String::from_utf8(bytes).map_err(|e| {
            RagloomError::new(RagloomErrorKind::InvalidInput, e)
                .with_context("failed to extract UTF-8 text from document bytes")
        })?;

        Ok(LoadedDocument { text })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::s3::S3ObjectMeta;

    #[derive(Debug)]
    struct FakeS3Client {
        bytes: Vec<u8>,
        fail_get_object: bool,
    }

    impl S3Client for FakeS3Client {
        fn bucket_name(&self) -> &str {
            "docs-bucket"
        }

        fn list_objects(&self, _prefix: Option<&str>) -> Result<Vec<S3ObjectMeta>, RagloomError> {
            unreachable!("source only")
        }

        fn get_object(&self, _key: &str) -> Result<Vec<u8>, RagloomError> {
            if self.fail_get_object {
                return Err(RagloomError::from_kind(RagloomErrorKind::Io)
                    .with_context("failed to get S3 object s3://docs-bucket/kb/hello.txt"));
            }

            Ok(self.bytes.clone())
        }
    }

    #[tokio::test]
    async fn rejects_non_s3_canonical_path() {
        let loader = S3Utf8Loader::new(Arc::new(FakeS3Client {
            bytes: b"hello".to_vec(),
            fail_get_object: false,
        }));

        let err = loader
            .load("/tmp/hello.txt")
            .await
            .expect_err("expected invalid input");

        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn reads_utf8_text_from_s3() {
        let loader = S3Utf8Loader::new(Arc::new(FakeS3Client {
            bytes: b"hello from s3".to_vec(),
            fail_get_object: false,
        }));

        let text = loader
            .load("s3://docs-bucket/kb/hello.txt")
            .await
            .expect("load text");

        assert_eq!(text.text, "hello from s3");
    }

    #[tokio::test]
    async fn rejects_bucket_mismatch() {
        let loader = S3Utf8Loader::new(Arc::new(FakeS3Client {
            bytes: b"hello from s3".to_vec(),
            fail_get_object: false,
        }));

        let err = loader
            .load("s3://other-bucket/kb/hello.txt")
            .await
            .expect_err("expected invalid input");

        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(err.to_string().contains("does not match configured bucket"));
    }

    #[tokio::test]
    async fn surfaces_utf8_extraction_context_for_s3_bytes() {
        let loader = S3Utf8Loader::new(Arc::new(FakeS3Client {
            bytes: vec![0xff, 0xfe, 0xfd],
            fail_get_object: false,
        }));

        let err = loader
            .load("s3://docs-bucket/kb/hello.txt")
            .await
            .expect_err("expected invalid utf-8");

        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("failed to extract UTF-8 text from document bytes")
        );
    }

    #[tokio::test]
    async fn surfaces_load_context_for_s3_read_failures() {
        let loader = S3Utf8Loader::new(Arc::new(FakeS3Client {
            bytes: Vec::new(),
            fail_get_object: true,
        }));

        let err = loader
            .load("s3://docs-bucket/kb/hello.txt")
            .await
            .expect_err("expected read failure");

        assert_eq!(err.kind, RagloomErrorKind::Io);
        assert!(err.to_string().contains("failed to load document bytes"));
    }
}

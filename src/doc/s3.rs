use std::sync::Arc;

use async_trait::async_trait;

use crate::doc::DocumentLoader;
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
    async fn load_utf8(&self, path: &str) -> Result<String, RagloomError> {
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
            .map_err(|e| RagloomError::new(e.kind, e).with_context("failed to read utf-8 file"))?;

        String::from_utf8(bytes).map_err(|e| {
            RagloomError::new(RagloomErrorKind::InvalidInput, e)
                .with_context("failed to read utf-8 file")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::s3::S3ObjectMeta;

    #[derive(Debug)]
    struct FakeS3Client {
        bytes: Vec<u8>,
    }

    impl S3Client for FakeS3Client {
        fn bucket_name(&self) -> &str {
            "docs-bucket"
        }

        fn list_objects(&self, _prefix: Option<&str>) -> Result<Vec<S3ObjectMeta>, RagloomError> {
            unreachable!("source only")
        }

        fn get_object(&self, _key: &str) -> Result<Vec<u8>, RagloomError> {
            Ok(self.bytes.clone())
        }
    }

    #[tokio::test]
    async fn rejects_non_s3_canonical_path() {
        let loader = S3Utf8Loader::new(Arc::new(FakeS3Client {
            bytes: b"hello".to_vec(),
        }));

        let err = loader
            .load_utf8("/tmp/hello.txt")
            .await
            .expect_err("expected invalid input");

        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn reads_utf8_text_from_s3() {
        let loader = S3Utf8Loader::new(Arc::new(FakeS3Client {
            bytes: b"hello from s3".to_vec(),
        }));

        let text = loader
            .load_utf8("s3://docs-bucket/kb/hello.txt")
            .await
            .expect("load text");

        assert_eq!(text, "hello from s3");
    }

    #[tokio::test]
    async fn rejects_bucket_mismatch() {
        let loader = S3Utf8Loader::new(Arc::new(FakeS3Client {
            bytes: b"hello from s3".to_vec(),
        }));

        let err = loader
            .load_utf8("s3://other-bucket/kb/hello.txt")
            .await
            .expect_err("expected invalid input");

        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(err.to_string().contains("does not match configured bucket"));
    }
}

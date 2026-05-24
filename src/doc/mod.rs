//! Document loading abstractions.

use async_trait::async_trait;

use crate::{RagloomError, RagloomErrorKind};

pub mod s3;
pub use s3::S3Utf8Loader;

/// Loads document bytes/text from a backing store.
///
/// # Why
/// Ingestion pipelines should depend on a small abstraction rather than hard-coding
/// filesystem, HTTP, or object-store logic. This trait provides a stable surface
/// for the pipeline while enabling alternate loaders in tests or other runtimes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedDocument {
    pub text: String,
}

#[async_trait]
pub trait DocumentLoader: Send + Sync {
    /// Loads a document and returns extracted text.
    async fn load(&self, path: &str) -> Result<LoadedDocument, RagloomError>;
}

/// Loads UTF-8 documents from the local filesystem.
///
/// # Why
/// The MVP ingest path often starts from local files. This loader keeps the
/// filesystem concerns encapsulated and returns crate-level errors with context.
#[derive(Debug, Default, Clone, Copy)]
pub struct FsUtf8Loader;

#[async_trait]
impl DocumentLoader for FsUtf8Loader {
    async fn load(&self, path: &str) -> Result<LoadedDocument, RagloomError> {
        let bytes = tokio::fs::read(path).await.map_err(|e| {
            RagloomError::new(RagloomErrorKind::Io, e).with_context("failed to load document bytes")
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

    #[tokio::test]
    async fn fs_utf8_loader_reads_text_file() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("hello.txt");
        tokio::fs::write(&path, "hello\nworld")
            .await
            .expect("write file");

        let loader = FsUtf8Loader;
        let loaded = loader
            .load(path.to_str().expect("utf-8 path"))
            .await
            .expect("load file");

        assert_eq!(loaded.text, "hello\nworld");
    }

    #[tokio::test]
    async fn fs_utf8_loader_returns_error_with_context_on_missing_file() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("missing.txt");

        let loader = FsUtf8Loader;
        let err = loader
            .load(path.to_str().expect("utf-8 path"))
            .await
            .expect_err("expected error");

        assert_eq!(err.kind, RagloomErrorKind::Io);
        assert!(err.to_string().contains("failed to load document bytes"));
    }

    #[tokio::test]
    async fn fs_utf8_loader_surfaces_utf8_extraction_context() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("invalid.bin");
        tokio::fs::write(&path, [0xff, 0xfe, 0xfd])
            .await
            .expect("write file");

        let loader = FsUtf8Loader;
        let err = loader
            .load(path.to_str().expect("utf-8 path"))
            .await
            .expect_err("expected invalid utf-8");

        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("failed to extract UTF-8 text from document bytes")
        );
    }
}

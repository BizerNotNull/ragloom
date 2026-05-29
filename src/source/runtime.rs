use std::collections::HashSet;
use std::sync::Arc;

use crate::config::{SourceConfig, SourceKind as ConfigSourceKind};
use crate::doc::{DocumentLoader, FsUtf8Loader, S3Utf8Loader};
use crate::error::{RagloomError, RagloomErrorKind};
use crate::s3::{RustS3Client, S3Client};
use crate::source::{DirectoryScannerSource, S3PollingSource, Source};

pub const USAGE: &str = "usage: ragloom [check|dry-run] [--config <path>] [--source-kind <filesystem|s3>] [--dir <path>] [--s3-bucket <name>] [--s3-prefix <prefix>] --qdrant-url <url> --collection <name> [--state-path <path>] [--health-addr <host:port>] [--retry-max-attempts <n>] [--embed-backend <openai|http>] (omit command to run ingestion)";

/// Source selection constructed from CLI arguments and config.
///
/// # Why
/// Keeping source selection explicit lets the CLI stay as a small composition
/// layer while leaving room for future source kinds without pushing that logic
/// into the runtime.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RunSource {
    Filesystem {
        root: String,
    },
    S3 {
        bucket: String,
        prefix: Option<String>,
    },
}

impl RunSource {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Filesystem { .. } => "filesystem",
            Self::S3 { .. } => "s3",
        }
    }

    pub fn log_target(&self) -> String {
        match self {
            Self::Filesystem { root } => root.clone(),
            Self::S3 { bucket, prefix } => match prefix {
                Some(prefix) => format!("s3://{bucket}/{prefix}"),
                None => format!("s3://{bucket}"),
            },
        }
    }
}

pub struct PreparedSourceRuntime {
    source: Box<dyn Source + Send>,
    loader: Arc<dyn DocumentLoader + Send + Sync>,
}

impl PreparedSourceRuntime {
    pub fn into_parts(
        self,
    ) -> (
        Box<dyn Source + Send>,
        Arc<dyn DocumentLoader + Send + Sync>,
    ) {
        (self.source, self.loader)
    }
}

pub fn resolve_run_source(
    cli_source_kind: Option<&str>,
    dir: Option<String>,
    s3_bucket: Option<String>,
    s3_prefix: Option<String>,
    file_source: Option<&SourceConfig>,
) -> Result<RunSource, RagloomError> {
    let source_kind_overridden = cli_source_kind.is_some();
    let cli_dir_overrides_s3 = dir.is_some()
        && !source_kind_overridden
        && file_source
            .map(|source| source.kind == ConfigSourceKind::S3)
            .unwrap_or(false);
    let source_kind = match cli_source_kind {
        Some(kind) => parse_source_kind(kind)?,
        None if dir.is_some() => ConfigSourceKind::Filesystem,
        None => file_source
            .map(|source| source.kind)
            .unwrap_or(ConfigSourceKind::Filesystem),
    };

    match source_kind {
        ConfigSourceKind::Filesystem => {
            if s3_bucket.is_some() || s3_prefix.is_some() {
                return Err(cli_config_error(
                    "--s3-bucket and --s3-prefix require --source-kind s3",
                ));
            }
            if !cli_dir_overrides_s3
                && !source_kind_overridden
                && file_source
                    .and_then(|source| source.bucket.as_deref())
                    .is_some()
            {
                return Err(cli_config_error(
                    "source.bucket is only valid when source.kind=s3",
                ));
            }
            if !cli_dir_overrides_s3
                && !source_kind_overridden
                && file_source
                    .and_then(|source| source.prefix.as_deref())
                    .is_some()
            {
                return Err(cli_config_error(
                    "source.prefix is only valid when source.kind=s3",
                ));
            }

            let root = dir
                .or_else(|| file_source.and_then(|source| source.root.clone()))
                .ok_or_else(|| {
                    cli_config_error("missing required value: --dir or source.root in --config")
                })?;
            if root.trim().is_empty() {
                return Err(cli_config_error("--dir or source.root is empty"));
            }

            Ok(RunSource::Filesystem { root })
        }
        ConfigSourceKind::S3 => {
            if dir.is_some() {
                return Err(cli_config_error(
                    "--dir is only valid when source.kind=filesystem",
                ));
            }
            if !source_kind_overridden
                && file_source
                    .and_then(|source| source.root.as_deref())
                    .is_some()
            {
                return Err(cli_config_error(
                    "source.root is only valid when source.kind=filesystem",
                ));
            }

            let bucket = s3_bucket
                .or_else(|| file_source.and_then(|source| source.bucket.clone()))
                .ok_or_else(|| {
                    cli_config_error(
                        "missing required value for s3 source: --s3-bucket or source.bucket in --config",
                    )
                })?;
            if bucket.trim().is_empty() {
                return Err(cli_config_error("--s3-bucket or source.bucket is empty"));
            }

            let prefix = s3_prefix.or_else(|| file_source.and_then(|source| source.prefix.clone()));
            if prefix
                .as_deref()
                .is_some_and(|prefix| prefix.trim().is_empty())
            {
                return Err(cli_config_error("--s3-prefix or source.prefix is empty"));
            }

            Ok(RunSource::S3 { bucket, prefix })
        }
    }
}

pub fn prepare_source_runtime(
    source: &RunSource,
    previously_observed_paths: HashSet<String>,
) -> Result<PreparedSourceRuntime, RagloomError> {
    prepare_source_runtime_with_s3_client(source, previously_observed_paths, None)
}

fn prepare_source_runtime_with_s3_client(
    source: &RunSource,
    previously_observed_paths: HashSet<String>,
    s3_client: Option<Arc<dyn S3Client>>,
) -> Result<PreparedSourceRuntime, RagloomError> {
    match source {
        RunSource::Filesystem { root } => {
            let source = DirectoryScannerSource::with_previously_observed_paths(
                root,
                previously_observed_paths,
            )
            .map_err(|e| {
                RagloomError::new(RagloomErrorKind::Io, e)
                    .with_context("failed to create directory scanner source")
            })?;

            Ok(PreparedSourceRuntime {
                source: Box::new(source),
                loader: Arc::new(FsUtf8Loader),
            })
        }
        RunSource::S3 { bucket, prefix } => {
            let client = match s3_client {
                Some(client) => client,
                None => Arc::new(RustS3Client::from_default_env(bucket)?),
            };
            let source = S3PollingSource::with_previously_observed_paths(
                bucket.clone(),
                prefix.clone(),
                Arc::clone(&client),
                previously_observed_paths,
            )
            .map_err(|e| {
                RagloomError::new(e.kind, e).with_context("failed to create S3 polling source")
            })?;

            Ok(PreparedSourceRuntime {
                source: Box::new(source),
                loader: Arc::new(S3Utf8Loader::new(client)),
            })
        }
    }
}

fn parse_source_kind(kind: &str) -> Result<ConfigSourceKind, RagloomError> {
    match kind {
        "filesystem" => Ok(ConfigSourceKind::Filesystem),
        "s3" => Ok(ConfigSourceKind::S3),
        other => Err(cli_invalid_input(format!(
            "invalid value for --source-kind: {other} (expected: filesystem|s3)"
        ))),
    }
}

fn cli_invalid_input(message: impl Into<String>) -> RagloomError {
    let message = message.into();
    RagloomError::from_kind(RagloomErrorKind::InvalidInput)
        .with_context(format!("{message}\n{USAGE}"))
}

fn cli_config_error(message: impl Into<String>) -> RagloomError {
    let message = message.into();
    RagloomError::from_kind(RagloomErrorKind::Config).with_context(format!("{message}\n{USAGE}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filesystem_source(root: &str) -> RunSource {
        RunSource::Filesystem {
            root: root.to_string(),
        }
    }

    #[test]
    fn resolve_run_source_allows_cli_dir_to_override_configured_s3_source() {
        let file_source = SourceConfig {
            kind: ConfigSourceKind::S3,
            root: None,
            bucket: Some("docs-bucket".to_string()),
            prefix: Some("knowledge/".to_string()),
        };

        let source = resolve_run_source(
            None,
            Some("/tmp/from-cli".to_string()),
            None,
            None,
            Some(&file_source),
        )
        .expect("source");

        assert_eq!(source, filesystem_source("/tmp/from-cli"));
    }

    #[test]
    fn resolve_run_source_rejects_invalid_filesystem_shape_without_override() {
        let file_source = SourceConfig {
            kind: ConfigSourceKind::Filesystem,
            root: Some("/tmp/from-config".to_string()),
            bucket: Some("docs-bucket".to_string()),
            prefix: None,
        };

        let err = resolve_run_source(None, None, None, None, Some(&file_source))
            .expect_err("should fail source validation");

        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("source.bucket"));
        assert!(err.to_string().contains(USAGE));
    }

    #[test]
    fn resolve_run_source_rejects_s3_flags_without_s3_kind() {
        let err = resolve_run_source(
            None,
            Some("/tmp/docs".to_string()),
            Some("docs-bucket".to_string()),
            None,
            None,
        )
        .expect_err("expected config error");

        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("--s3-bucket"));
        assert!(err.to_string().contains(USAGE));
    }

    #[test]
    fn resolve_run_source_rejects_invalid_source_kind_with_usage() {
        let err = resolve_run_source(Some("blob"), None, None, None, None)
            .expect_err("expected invalid source-kind");

        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(err.to_string().contains("invalid value for --source-kind"));
        assert!(err.to_string().contains(USAGE));
    }

    #[test]
    fn prepare_source_runtime_builds_filesystem_source_and_loader() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("hello.txt");
        std::fs::write(&path, b"hello filesystem").expect("write file");

        let source = RunSource::Filesystem {
            root: dir.path().display().to_string(),
        };

        let prepared =
            prepare_source_runtime(&source, HashSet::new()).expect("prepare filesystem source");
        let (mut source, loader) = prepared.into_parts();

        assert_eq!(source.poll().len(), 1);
        let text = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(loader.load(path.to_str().expect("utf-8 path")))
            .expect("load text");
        assert_eq!(text.text, "hello filesystem");
    }

    #[test]
    fn prepare_source_runtime_builds_s3_source_and_loader() {
        #[derive(Debug)]
        struct FakeS3Client;

        impl crate::s3::S3Client for FakeS3Client {
            fn bucket_name(&self) -> &str {
                "docs-bucket"
            }

            fn list_objects(
                &self,
                _prefix: Option<&str>,
            ) -> Result<Vec<crate::s3::S3ObjectMeta>, RagloomError> {
                Ok(Vec::new())
            }

            fn get_object(&self, key: &str) -> Result<Vec<u8>, RagloomError> {
                Ok(format!("loaded {key}").into_bytes())
            }
        }

        let source = RunSource::S3 {
            bucket: "docs-bucket".to_string(),
            prefix: Some("knowledge/".to_string()),
        };

        let prepared = prepare_source_runtime_with_s3_client(
            &source,
            HashSet::new(),
            Some(Arc::new(FakeS3Client)),
        )
        .expect("prepare S3 source");
        let (mut source, loader) = prepared.into_parts();

        assert!(source.poll().is_empty());
        let text = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(loader.load("s3://docs-bucket/knowledge/hello.txt"))
            .expect("load text");
        assert_eq!(text.text, "loaded knowledge/hello.txt");
    }
}

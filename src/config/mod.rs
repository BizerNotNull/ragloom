//! Pipeline configuration.
//!
//! # Why
//! Ragloom is designed to be operated as a single binary configured via a file.
//! A typed config model makes validation explicit and enables safe hot reload
//! by validating changes before applying them.

pub mod reload;

use serde::Deserialize;

use crate::error::{RagloomError, RagloomErrorKind};

pub const DEFAULT_STATE_PATH: &str = ".ragloom/wal.ndjson";

/// Top-level pipeline configuration.
///
/// # Why
/// We keep operational settings (endpoints, limits, paths) in one tree so
/// validation and reload can be performed atomically.
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    pub source: SourceConfig,
    pub embed: EmbedConfig,
    pub sink: SinkConfig,
    #[serde(default)]
    pub state: StateConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub health: HealthConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    #[serde(default = "default_source_kind")]
    pub kind: SourceKind,
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Filesystem,
    S3,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmbedConfig {
    pub endpoint: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    pub qdrant_url: String,
    pub collection: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StateConfig {
    #[serde(default = "default_state_path")]
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_retry_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_retry_max_queued")]
    pub max_queued: usize,
    #[serde(default = "default_retry_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_retry_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct HealthConfig {
    #[serde(default)]
    pub addr: Option<String>,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            path: default_state_path(),
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_retry_max_attempts(),
            max_queued: default_retry_max_queued(),
            initial_backoff_ms: default_retry_initial_backoff_ms(),
            max_backoff_ms: default_retry_max_backoff_ms(),
        }
    }
}

fn default_state_path() -> String {
    DEFAULT_STATE_PATH.to_string()
}

fn default_source_kind() -> SourceKind {
    SourceKind::Filesystem
}

fn default_retry_max_attempts() -> u32 {
    3
}

fn default_retry_max_queued() -> usize {
    128
}

fn default_retry_initial_backoff_ms() -> u64 {
    100
}

fn default_retry_max_backoff_ms() -> u64 {
    2_000
}

impl PipelineConfig {
    /// Parses a YAML document into a typed config.
    ///
    /// # Why
    /// Parsing is a boundary operation; failures must be reported with enough
    /// context for operators to fix the configuration quickly.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, RagloomError> {
        serde_yaml::from_str(yaml).map_err(|e| {
            RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context(format!("invalid yaml: {e}"))
        })
    }

    /// Validates invariants that are required for a safe runtime.
    ///
    /// # Why
    /// Reload applies configs at runtime. Validation prevents partial/unsafe
    /// configs from being activated.
    pub fn validate(&self) -> Result<(), RagloomError> {
        self.source.validate()?;
        if self.embed.endpoint.trim().is_empty() {
            return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("embed.endpoint is empty"));
        }
        if self.sink.qdrant_url.trim().is_empty() {
            return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("sink.qdrant_url is empty"));
        }
        if self.sink.collection.trim().is_empty() {
            return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("sink.collection is empty"));
        }
        if self.state.path.trim().is_empty() {
            return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("state.path is empty"));
        }
        if self.retry.max_attempts == 0 {
            return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("retry.max_attempts must be at least 1"));
        }
        if self.retry.max_attempts > 1 && self.retry.max_queued == 0 {
            return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("retry.max_queued must be at least 1 when retries are enabled"));
        }
        if self.retry.max_backoff_ms < self.retry.initial_backoff_ms {
            return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("retry.max_backoff_ms must be >= retry.initial_backoff_ms"));
        }
        if self
            .health
            .addr
            .as_deref()
            .is_some_and(|addr| addr.trim().is_empty())
        {
            return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("health.addr is empty"));
        }
        Ok(())
    }
}

impl SourceConfig {
    fn validate(&self) -> Result<(), RagloomError> {
        match self.kind {
            SourceKind::Filesystem => {
                let root = self.root.as_deref().ok_or_else(|| {
                    RagloomError::from_kind(RagloomErrorKind::Config)
                        .with_context("source.root is required when source.kind=filesystem")
                })?;
                if root.trim().is_empty() {
                    return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                        .with_context("source.root is empty"));
                }
                if self.bucket.is_some() {
                    return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                        .with_context("source.bucket is only valid when source.kind=s3"));
                }
                if self.prefix.is_some() {
                    return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                        .with_context("source.prefix is only valid when source.kind=s3"));
                }
            }
            SourceKind::S3 => {
                let bucket = self.bucket.as_deref().ok_or_else(|| {
                    RagloomError::from_kind(RagloomErrorKind::Config)
                        .with_context("source.bucket is required when source.kind=s3")
                })?;
                if bucket.trim().is_empty() {
                    return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                        .with_context("source.bucket is empty"));
                }
                if self
                    .prefix
                    .as_deref()
                    .is_some_and(|prefix| prefix.trim().is_empty())
                {
                    return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                        .with_context("source.prefix is empty"));
                }
                if self.root.is_some() {
                    return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                        .with_context("source.root is only valid when source.kind=filesystem"));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pipeline_yaml_and_validates_required_fields() {
        let yaml = r#"
source:
  root: "/data"
embed:
  endpoint: "http://localhost:8080/embed"
sink:
  qdrant_url: "http://localhost:6333"
  collection: "docs"
state:
  path: ".ragloom/wal.ndjson"
retry:
  max_attempts: 3
  max_queued: 128
  initial_backoff_ms: 100
  max_backoff_ms: 2000
health:
  addr: "127.0.0.1:0"
"#;
        let cfg = PipelineConfig::from_yaml_str(yaml).expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.state.path, ".ragloom/wal.ndjson");
        assert_eq!(cfg.retry.max_attempts, 3);
        assert_eq!(cfg.health.addr.as_deref(), Some("127.0.0.1:0"));
        assert_eq!(cfg.source.kind, SourceKind::Filesystem);
        assert_eq!(cfg.source.root.as_deref(), Some("/data"));
    }

    #[test]
    fn rejects_invalid_retry_config() {
        let yaml = r#"
source:
  root: "/data"
embed:
  endpoint: "http://localhost:8080/embed"
sink:
  qdrant_url: "http://localhost:6333"
  collection: "docs"
retry:
  max_attempts: 0
"#;
        let cfg = PipelineConfig::from_yaml_str(yaml).expect("parse");
        let err = cfg.validate().expect_err("validate");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("retry.max_attempts"));
    }

    #[test]
    fn rejects_empty_health_addr() {
        let yaml = r#"
source:
  root: "/data"
embed:
  endpoint: "http://localhost:8080/embed"
sink:
  qdrant_url: "http://localhost:6333"
  collection: "docs"
health:
  addr: " "
"#;
        let cfg = PipelineConfig::from_yaml_str(yaml).expect("parse");
        let err = cfg.validate().expect_err("validate");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("health.addr"));
    }

    #[test]
    fn validates_explicit_filesystem_source_kind() {
        let yaml = r#"
source:
  kind: filesystem
  root: "/data"
embed:
  endpoint: "http://localhost:8080/embed"
sink:
  qdrant_url: "http://localhost:6333"
  collection: "docs"
"#;
        let cfg = PipelineConfig::from_yaml_str(yaml).expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.source.kind, SourceKind::Filesystem);
        assert_eq!(cfg.source.root.as_deref(), Some("/data"));
    }

    #[test]
    fn rejects_filesystem_source_with_s3_fields() {
        let yaml = r#"
source:
  kind: filesystem
  root: "/data"
  bucket: "docs"
embed:
  endpoint: "http://localhost:8080/embed"
sink:
  qdrant_url: "http://localhost:6333"
  collection: "docs"
"#;
        let cfg = PipelineConfig::from_yaml_str(yaml).expect("parse");
        let err = cfg.validate().expect_err("validate");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("source.bucket"));
    }

    #[test]
    fn rejects_s3_source_without_bucket() {
        let yaml = r#"
source:
  kind: s3
embed:
  endpoint: "http://localhost:8080/embed"
sink:
  qdrant_url: "http://localhost:6333"
  collection: "docs"
"#;
        let cfg = PipelineConfig::from_yaml_str(yaml).expect("parse");
        let err = cfg.validate().expect_err("validate");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("source.bucket"));
    }

    #[test]
    fn validates_s3_source_shape() {
        let yaml = r#"
source:
  kind: s3
  bucket: "docs"
  prefix: "kb/"
embed:
  endpoint: "http://localhost:8080/embed"
sink:
  qdrant_url: "http://localhost:6333"
  collection: "docs"
"#;
        let cfg = PipelineConfig::from_yaml_str(yaml).expect("parse");
        cfg.validate().expect("validate");
        assert_eq!(cfg.source.kind, SourceKind::S3);
        assert_eq!(cfg.source.bucket.as_deref(), Some("docs"));
        assert_eq!(cfg.source.prefix.as_deref(), Some("kb/"));
    }
}

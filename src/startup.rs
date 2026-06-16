//! Reusable startup wiring for Ragloom runtimes.
//!
//! # Why
//! The CLI binary should stay focused on argument parsing and top-level process
//! orchestration. Reusable startup, validation, and replay behavior belongs in
//! the library crate so it can be tested in its owning module.

use std::collections::HashSet;
use std::fmt;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{RagloomError, RagloomErrorKind};
use crate::sink::qdrant::{QdrantConfig, QdrantSink};
use crate::source::runtime::{RunSource, prepare_source_runtime};
use crate::state::compact::{StateCompactionSummary, compact_state_files};
use crate::state::failed::{
    FailedWorkRecord, FailedWorkStore, FileFailedWorkStore, failed_work_path_from_state_path,
    pending_failed_work,
};
use crate::state::wal::WalStore;

#[derive(Clone, Eq, PartialEq)]
pub enum EmbedBackend {
    OpenAi {
        endpoint: String,
        api_key: String,
        model: String,
    },
    Http {
        url: String,
        model: String,
    },
}

impl fmt::Debug for EmbedBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenAi {
                endpoint, model, ..
            } => f
                .debug_struct("OpenAi")
                .field("endpoint", endpoint)
                .field("api_key", &"<redacted>")
                .field("model", model)
                .finish(),
            Self::Http { url, model } => f
                .debug_struct("Http")
                .field("url", url)
                .field("model", model)
                .finish(),
        }
    }
}

impl EmbedBackend {
    pub fn name(&self) -> &'static str {
        match self {
            Self::OpenAi { .. } => "openai",
            Self::Http { .. } => "http",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RunConfig {
    pub source: RunSource,
    pub embed_backend: EmbedBackend,
    pub qdrant_url: String,
    pub collection: String,
    pub state_path: String,
    pub health_addr: Option<String>,
    pub create_collection_if_missing: bool,
    pub collection_vector_size: Option<usize>,
    pub chunker_strategy: String,
    pub size_metric: String,
    pub size_max: usize,
    pub size_min: usize,
    pub size_overlap: usize,
    pub tokenizer: String,
    pub chunker_mode: String,
    pub chunker_single: Option<String>,
    pub enable_semantic: bool,
    pub semantic_provider: String,
    pub semantic_percentile: u8,
    pub retry_max_attempts: u32,
    pub retry_max_queued: usize,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReplayFailedConfig {
    pub state_path: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ReplayFailedSummary {
    pub pending: usize,
    pub requeued: usize,
    pub skipped: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompactStateConfig {
    pub state_path: String,
}

const OPENAI_EMBEDDING_VECTOR_SIZES: &[(&str, usize)] = &[
    ("text-embedding-3-small", 1536),
    ("text-embedding-3-large", 3072),
    ("text-embedding-ada-002", 1536),
];

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BootstrapPlan {
    Disabled,
    WouldEnsureCollection { vector_size: usize },
}

impl BootstrapPlan {
    fn render(&self) -> String {
        match self {
            Self::Disabled => "disabled".to_string(),
            Self::WouldEnsureCollection { vector_size } => {
                format!("would ensure collection exists (vector_size={vector_size})")
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StartupValidationSummary {
    pub source_kind: String,
    pub source_target: String,
    pub embed_backend: String,
    pub chunker_selection: String,
    pub bootstrap: BootstrapPlan,
    pub state_path: String,
    pub wal_status: String,
    pub failed_work_status: String,
}

impl StartupValidationSummary {
    pub fn render(&self) -> String {
        [
            "ragloom dry-run",
            &format!("source_kind={}", self.source_kind),
            &format!("source_target={}", self.source_target),
            &format!("embed_backend={}", self.embed_backend),
            &format!("chunker={}", self.chunker_selection),
            &format!("bootstrap={}", self.bootstrap.render()),
            &format!("state_path={}", self.state_path),
            &format!("wal={}", self.wal_status),
            &format!("failed_work={}", self.failed_work_status),
        ]
        .join("\n")
    }
}

pub struct PreparedStartup {
    pub embedding: std::sync::Arc<dyn crate::embed::EmbeddingProvider + Send + Sync>,
    pub sink: QdrantSink,
    pub chunker: std::sync::Arc<dyn crate::transform::chunker::Chunker>,
}

fn parse_code_lang(s: &str) -> Result<crate::transform::chunker::code::Language, RagloomError> {
    use crate::transform::chunker::code::Language;

    match s {
        "rust" => Ok(Language::Rust),
        "python" => Ok(Language::Python),
        "javascript" => Ok(Language::JavaScript),
        "typescript" => Ok(Language::TypeScript),
        "tsx" => Ok(Language::Tsx),
        "go" => Ok(Language::Go),
        "java" => Ok(Language::Java),
        "c" => Ok(Language::C),
        "cpp" => Ok(Language::Cpp),
        "ruby" => Ok(Language::Ruby),
        "bash" => Ok(Language::Bash),
        other => Err(RagloomError::from_kind(RagloomErrorKind::InvalidInput)
            .with_context(format!("unsupported language: {other}"))),
    }
}

fn embedding_fingerprint(cfg: &RunConfig) -> String {
    match &cfg.embed_backend {
        EmbedBackend::OpenAi { model, .. } => format!("openai:{model}"),
        EmbedBackend::Http { model, .. } => format!("http:{model}"),
    }
}

fn openai_embedding_vector_size(model: &str) -> Option<usize> {
    OPENAI_EMBEDDING_VECTOR_SIZES
        .iter()
        .find_map(|(known_model, size)| (*known_model == model).then_some(*size))
}

fn resolve_collection_vector_size(cfg: &RunConfig) -> Result<usize, RagloomError> {
    if let Some(size) = cfg.collection_vector_size {
        return Ok(size);
    }

    match &cfg.embed_backend {
        EmbedBackend::OpenAi { model, .. } => openai_embedding_vector_size(model).ok_or_else(|| {
            RagloomError::from_kind(RagloomErrorKind::Config).with_context(format!(
                "unknown OpenAI model for collection vector size: {model}; pass --collection-vector-size"
            ))
        }),
        EmbedBackend::Http { .. } => Err(
            RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("http backend requires --collection-vector-size"),
        ),
    }
}

async fn bootstrap_collection_if_needed(
    cfg: &RunConfig,
    sink: &QdrantSink,
) -> Result<(), RagloomError> {
    if !cfg.create_collection_if_missing {
        return Ok(());
    }

    let vector_size = resolve_collection_vector_size(cfg).map_err(|e| {
        RagloomError::new(e.kind, e).with_context("failed to bootstrap Qdrant collection")
    })?;
    sink.ensure_collection_exists(vector_size)
        .await
        .map_err(|e| {
            RagloomError::new(e.kind, e).with_context("failed to bootstrap Qdrant collection")
        })
}

fn chunker_selection(cfg: &RunConfig) -> Result<String, RagloomError> {
    match cfg.chunker_mode.as_str() {
        "router"
            if semantic_chunking_active(cfg.chunker_mode.as_str(), None, cfg.enable_semantic) =>
        {
            Ok(format!(
                "router+semantic(provider={})",
                cfg.semantic_provider
            ))
        }
        "single"
            if semantic_chunking_active(
                cfg.chunker_mode.as_str(),
                cfg.chunker_single.as_deref(),
                cfg.enable_semantic,
            ) =>
        {
            Ok(format!(
                "single:semantic(provider={})",
                cfg.semantic_provider
            ))
        }
        "router" => Ok("router".to_string()),
        "single" => Ok(format!(
            "single:{}",
            required_chunker_single(cfg.chunker_single.as_deref())?
        )),
        other => Err(RagloomError::from_kind(RagloomErrorKind::Config)
            .with_context(format!("invalid chunker_mode: {other}"))),
    }
}

fn semantic_chunking_active(
    chunker_mode: &str,
    chunker_single: Option<&str>,
    enable_semantic: bool,
) -> bool {
    match chunker_mode {
        "router" => enable_semantic,
        "single" => chunker_single == Some("semantic"),
        _ => false,
    }
}

fn build_semantic_signal_provider(
    cfg: &RunConfig,
    embedding: &std::sync::Arc<dyn crate::embed::EmbeddingProvider + Send + Sync>,
    embed_fingerprint: &str,
) -> Result<std::sync::Arc<dyn crate::transform::chunker::SemanticSignalProvider>, RagloomError> {
    use crate::transform::chunker::{EmbeddingProviderAdapter, SemanticSignalProvider};

    let signal: std::sync::Arc<dyn SemanticSignalProvider> = match cfg.semantic_provider.as_str() {
        "adapter" => std::sync::Arc::new(EmbeddingProviderAdapter::new(
            std::sync::Arc::clone(embedding),
            embed_fingerprint.to_string(),
        )),
        #[cfg(feature = "fastembed")]
        "fastembed" => std::sync::Arc::new(
            crate::transform::chunker::FastembedSignalProvider::new().map_err(|e| {
                RagloomError::new(RagloomErrorKind::Config, e).with_context("fastembed init")
            })?,
        ),
        other => {
            return Err(RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                .with_context(format!("unsupported --semantic-provider: {other}")));
        }
    };

    Ok(signal)
}

fn bootstrap_plan(cfg: &RunConfig) -> Result<BootstrapPlan, RagloomError> {
    if !cfg.create_collection_if_missing {
        return Ok(BootstrapPlan::Disabled);
    }

    let vector_size = resolve_collection_vector_size(cfg).map_err(|e| {
        RagloomError::new(e.kind, e).with_context("failed to bootstrap Qdrant collection")
    })?;
    Ok(BootstrapPlan::WouldEnsureCollection { vector_size })
}

fn size_metric_from_config(
    size_metric: &str,
) -> Result<crate::transform::chunker::size::SizeMetric, RagloomError> {
    match size_metric {
        "chars" => Ok(crate::transform::chunker::size::SizeMetric::Chars),
        "tokens" => Ok(crate::transform::chunker::size::SizeMetric::Tokens),
        other => Err(RagloomError::from_kind(RagloomErrorKind::Config)
            .with_context(format!("invalid size_metric: {other}"))),
    }
}

fn required_chunker_single(chunker_single: Option<&str>) -> Result<&str, RagloomError> {
    chunker_single.ok_or_else(|| {
        RagloomError::from_kind(RagloomErrorKind::Config)
            .with_context("chunker_mode=single requires chunker_single")
    })
}

pub async fn prepare_startup(
    cfg: &RunConfig,
    perform_bootstrap_writes: bool,
) -> Result<PreparedStartup, RagloomError> {
    let embed_fingerprint = embedding_fingerprint(cfg);

    let embedding: std::sync::Arc<dyn crate::embed::EmbeddingProvider + Send + Sync> =
        match &cfg.embed_backend {
            EmbedBackend::OpenAi {
                endpoint,
                api_key,
                model,
            } => {
                let client = crate::embed::openai_client::OpenAiEmbeddingClient::new(
                    crate::embed::openai_client::OpenAiEmbeddingConfig {
                        endpoint: endpoint.clone(),
                        api_key: api_key.clone(),
                        model: model.clone(),
                        timeout: Duration::from_secs(30),
                    },
                )
                .map_err(|e| e.with_context("failed to build OpenAI embedding client"))?;
                std::sync::Arc::new(client)
            }
            EmbedBackend::Http { url, model } => {
                let client = crate::embed::http_client::HttpEmbeddingClient::new(
                    crate::embed::http_client::HttpEmbeddingConfig {
                        endpoint: url.clone(),
                        model: model.clone(),
                        timeout: Duration::from_secs(30),
                    },
                )
                .map_err(|e| e.with_context("failed to build HTTP embedding client"))?;
                std::sync::Arc::new(client)
            }
        };

    let sink = QdrantSink::new(QdrantConfig {
        base_url: cfg.qdrant_url.clone(),
        collection: cfg.collection.clone(),
        timeout: Duration::from_secs(30),
    })
    .map_err(|e| e.with_context("failed to build Qdrant sink"))?;

    if cfg.tokenizer != "tiktoken-cl100k" {
        return Err(
            RagloomError::from_kind(RagloomErrorKind::Config).with_context(format!(
                "unsupported --tokenizer: {} (phase 1 supports only: tiktoken-cl100k)",
                cfg.tokenizer
            )),
        );
    }
    tracing::info!(
        event.name = "ragloom.chunker.tokenizer_selected",
        tokenizer = %cfg.tokenizer,
        "ragloom.chunker.tokenizer_selected"
    );

    let metric = size_metric_from_config(&cfg.size_metric)?;

    let rec_cfg = crate::transform::chunker::recursive::RecursiveConfig {
        metric,
        max_size: cfg.size_max,
        min_size: cfg.size_min,
        overlap: cfg.size_overlap,
    };

    if cfg.chunker_strategy == "legacy" {
        tracing::warn!(
            event.name = "ragloom.chunker.legacy_alias",
            "--chunker-strategy=legacy currently routes through the recursive chunker; \
             retained as a rollback seam for future phases"
        );
    }

    use crate::transform::chunker::{
        Chunker, MarkdownChunker, SemanticChunker, default_router, recursive::RecursiveChunker,
        semantic_router,
    };

    let chunker: std::sync::Arc<dyn Chunker> = if semantic_chunking_active(
        cfg.chunker_mode.as_str(),
        cfg.chunker_single.as_deref(),
        cfg.enable_semantic,
    ) && cfg.chunker_mode == "router"
    {
        let signal = build_semantic_signal_provider(cfg, &embedding, &embed_fingerprint)?;
        let semantic_chunker: std::sync::Arc<dyn Chunker> = std::sync::Arc::new(
            SemanticChunker::new(signal, rec_cfg, cfg.semantic_percentile).map_err(|e| {
                RagloomError::new(RagloomErrorKind::Config, e)
                    .with_context("invalid semantic config")
            })?,
        );
        std::sync::Arc::new(semantic_router(rec_cfg, semantic_chunker).map_err(|e| {
            RagloomError::new(RagloomErrorKind::Config, e)
                .with_context("invalid semantic router config")
        })?)
    } else {
        match cfg.chunker_mode.as_str() {
            "router" => std::sync::Arc::new(default_router(rec_cfg).map_err(|e| {
                RagloomError::new(RagloomErrorKind::Config, e).with_context("invalid router config")
            })?),
            "single" => {
                let kind = required_chunker_single(cfg.chunker_single.as_deref())?;
                match kind {
                    "semantic" => {
                        let signal =
                            build_semantic_signal_provider(cfg, &embedding, &embed_fingerprint)?;
                        std::sync::Arc::new(
                            SemanticChunker::new(signal, rec_cfg, cfg.semantic_percentile)
                                .map_err(|e| {
                                    RagloomError::new(RagloomErrorKind::Config, e)
                                        .with_context("invalid semantic config")
                                })?,
                        )
                    }
                    "recursive" => {
                        std::sync::Arc::new(RecursiveChunker::new(rec_cfg).map_err(|e| {
                            RagloomError::new(RagloomErrorKind::Config, e)
                                .with_context("invalid chunker config")
                        })?)
                    }
                    "markdown" => {
                        std::sync::Arc::new(MarkdownChunker::new(rec_cfg).map_err(|e| {
                            RagloomError::new(RagloomErrorKind::Config, e)
                                .with_context("invalid markdown config")
                        })?)
                    }
                    s if s.starts_with("code:") => {
                        let lang = parse_code_lang(&s[5..])?;
                        std::sync::Arc::new(
                            crate::transform::chunker::CodeChunker::new(lang, rec_cfg).map_err(
                                |e| {
                                    RagloomError::new(RagloomErrorKind::Config, e)
                                        .with_context("invalid code config")
                                },
                            )?,
                        )
                    }
                    other => {
                        return Err(RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                            .with_context(format!("invalid --chunker-single: {other}")));
                    }
                }
            }
            other => {
                return Err(RagloomError::from_kind(RagloomErrorKind::Config)
                    .with_context(format!("invalid chunker_mode: {other}")));
            }
        }
    };

    if perform_bootstrap_writes {
        bootstrap_collection_if_needed(cfg, &sink).await?;
    } else {
        let _ = bootstrap_plan(cfg)?;
    }

    Ok(PreparedStartup {
        embedding,
        sink,
        chunker,
    })
}

pub async fn validate_startup(cfg: &RunConfig) -> Result<StartupValidationSummary, RagloomError> {
    let _ = prepare_startup(cfg, false).await?;
    let _ = prepare_source_runtime(&cfg.source, HashSet::new())?;
    let state = validate_state_preflight(Path::new(&cfg.state_path))?;

    Ok(StartupValidationSummary {
        source_kind: cfg.source.kind().to_string(),
        source_target: cfg.source.log_target(),
        embed_backend: cfg.embed_backend.name().to_string(),
        chunker_selection: chunker_selection(cfg)?,
        bootstrap: bootstrap_plan(cfg)?,
        state_path: cfg.state_path.clone(),
        wal_status: state.wal_status,
        failed_work_status: state.failed_work_status,
    })
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct StatePreflightSummary {
    wal_status: String,
    failed_work_status: String,
}

fn validate_state_preflight(state_path: &Path) -> Result<StatePreflightSummary, RagloomError> {
    let failed_path = failed_work_path_from_state_path(state_path);
    let wal_status = validate_state_journal::<crate::state::wal::WalRecord>(state_path, "WAL")?;
    let failed_work_status =
        validate_state_journal::<FailedWorkRecord>(&failed_path, "failed-work")?;
    Ok(StatePreflightSummary {
        wal_status,
        failed_work_status,
    })
}

fn validate_state_journal<T>(path: &Path, journal_name: &str) -> Result<String, RagloomError>
where
    T: serde::de::DeserializeOwned,
{
    match std::fs::File::open(path) {
        Ok(file) => {
            ensure_writable_path(path, journal_name)?;
            let mut records = 0usize;
            for (idx, line) in std::io::BufReader::new(file).lines().enumerate() {
                let line = line.map_err(|e| {
                    RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
                        "failed to read {journal_name} record at line {} in {}",
                        idx + 1,
                        path.display()
                    ))
                })?;
                let line = line.strip_suffix('\r').unwrap_or(&line);
                if line.trim().is_empty() {
                    continue;
                }
                serde_json::from_str::<T>(line).map_err(|e| {
                    RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
                        "failed to parse {journal_name} record at line {} in {}",
                        idx + 1,
                        path.display()
                    ))
                })?;
                records += 1;
            }
            Ok(format!("readable,writable,records={records}"))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let parent = nearest_existing_parent(path)?;
            ensure_writable_path(&parent, journal_name)?;
            Ok("missing,creatable".to_string())
        }
        Err(err) => Err(
            RagloomError::new(RagloomErrorKind::State, err).with_context(format!(
                "failed to read {journal_name} file: {}",
                path.display()
            )),
        ),
    }
}

fn nearest_existing_parent(path: &Path) -> Result<PathBuf, RagloomError> {
    let mut candidate = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    loop {
        match std::fs::metadata(candidate) {
            Ok(metadata) if metadata.is_dir() => return Ok(candidate.to_path_buf()),
            Ok(_) => {
                return Err(
                    RagloomError::from_kind(RagloomErrorKind::State).with_context(format!(
                        "state parent is not a directory: {}",
                        candidate.display()
                    )),
                );
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                candidate = candidate.parent().unwrap_or_else(|| Path::new("."));
            }
            Err(err) => {
                return Err(
                    RagloomError::new(RagloomErrorKind::State, err).with_context(format!(
                        "failed to inspect state parent: {}",
                        candidate.display()
                    )),
                );
            }
        }
    }
}

fn ensure_writable_path(path: &Path, journal_name: &str) -> Result<(), RagloomError> {
    let metadata = std::fs::metadata(path).map_err(|e| {
        RagloomError::new(RagloomErrorKind::State, e).with_context(format!(
            "failed to inspect {journal_name} path: {}",
            path.display()
        ))
    })?;
    let writable = if metadata.is_dir() {
        has_directory_create_mode(&metadata)
    } else {
        has_write_mode(&metadata)
    };
    if metadata.permissions().readonly() || !writable {
        return Err(
            RagloomError::from_kind(RagloomErrorKind::State).with_context(format!(
                "{journal_name} path is not writable: {}",
                path.display()
            )),
        );
    }
    Ok(())
}

#[cfg(unix)]
fn has_write_mode(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o222 != 0
}

#[cfg(unix)]
fn has_directory_create_mode(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    let mode = metadata.permissions().mode();
    (mode & 0o222 != 0) && (mode & 0o111 != 0)
}

#[cfg(not(unix))]
fn has_write_mode(_metadata: &std::fs::Metadata) -> bool {
    true
}

#[cfg(not(unix))]
fn has_directory_create_mode(_metadata: &std::fs::Metadata) -> bool {
    true
}

pub fn validate_reloadable_changes(
    current: &RunConfig,
    next: &RunConfig,
) -> Result<bool, RagloomError> {
    if current == next {
        return Ok(false);
    }

    let mut rejected = Vec::new();
    if current.source != next.source {
        rejected.push("source");
    }
    if current.embed_backend != next.embed_backend {
        rejected.push("embed");
    }
    if current.qdrant_url != next.qdrant_url {
        rejected.push("sink.qdrant_url");
    }
    if current.collection != next.collection {
        rejected.push("sink.collection");
    }
    if current.state_path != next.state_path {
        rejected.push("state.path");
    }
    if current.health_addr != next.health_addr {
        rejected.push("health.addr");
    }
    if current.create_collection_if_missing != next.create_collection_if_missing {
        rejected.push("create_collection_if_missing");
    }
    if current.collection_vector_size != next.collection_vector_size {
        rejected.push("collection_vector_size");
    }
    if current.chunker_strategy != next.chunker_strategy {
        rejected.push("chunker_strategy");
    }
    if current.size_metric != next.size_metric {
        rejected.push("size_metric");
    }
    if current.size_max != next.size_max {
        rejected.push("size_max");
    }
    if current.size_min != next.size_min {
        rejected.push("size_min");
    }
    if current.size_overlap != next.size_overlap {
        rejected.push("size_overlap");
    }
    if current.tokenizer != next.tokenizer {
        rejected.push("tokenizer");
    }
    if current.chunker_mode != next.chunker_mode {
        rejected.push("chunker_mode");
    }
    if current.chunker_single != next.chunker_single {
        rejected.push("chunker_single");
    }
    if current.enable_semantic != next.enable_semantic {
        rejected.push("enable_semantic");
    }
    if current.semantic_provider != next.semantic_provider {
        rejected.push("semantic_provider");
    }
    if current.semantic_percentile != next.semantic_percentile {
        rejected.push("semantic_percentile");
    }

    if !rejected.is_empty() {
        return Err(
            RagloomError::from_kind(RagloomErrorKind::Config).with_context(format!(
                "config reload rejected: changed non-reloadable fields: {}",
                rejected.join(", ")
            )),
        );
    }

    Ok(current.retry_max_attempts != next.retry_max_attempts
        || current.retry_max_queued != next.retry_max_queued
        || current.retry_initial_backoff_ms != next.retry_initial_backoff_ms
        || current.retry_max_backoff_ms != next.retry_max_backoff_ms)
}

pub fn replay_failed_into_wal(
    wal: &mut impl WalStore,
    failed_store: &mut impl FailedWorkStore,
) -> Result<ReplayFailedSummary, RagloomError> {
    let records = failed_store
        .read_all()
        .map_err(|e| e.with_context("failed to read failed-work store"))?;

    let pending = pending_failed_work(&records);
    let pending_count = pending.len();
    let exhausted_count = records
        .iter()
        .filter(|record| matches!(record, FailedWorkRecord::Exhausted { .. }))
        .count();
    let skipped = exhausted_count.saturating_sub(pending_count);
    let mut requeued = 0usize;
    for item in pending {
        if let Err(err) = wal.append(item.work.clone()) {
            return Err(err.with_context(format!(
                "failed to append replayed work into WAL; replay summary: pending={pending_count} requeued={requeued} skipped={skipped} failed=1"
            )));
        }
        if let Err(err) = failed_store.append(FailedWorkRecord::Requeued {
            exhausted_id: item.id,
        }) {
            return Err(err.with_context(format!(
                "failed to mark failed work as requeued; replay summary: pending={pending_count} requeued={requeued} skipped={skipped} failed=1"
            )));
        }
        requeued += 1;

        tracing::info!(
            event.name = "ragloom.failed_work.requeued",
            exhausted_id = item.id,
            terminal_reason = ?item.terminal_reason,
            attempts = item.attempts,
            "ragloom.failed_work.requeued"
        );
    }

    Ok(ReplayFailedSummary {
        pending: pending_count,
        requeued,
        skipped,
        failed: 0,
    })
}

pub async fn replay_failed_command(
    cfg: &ReplayFailedConfig,
) -> Result<ReplayFailedSummary, RagloomError> {
    let failed_path = failed_work_path_from_state_path(&cfg.state_path);
    let mut wal = crate::state::wal::FileWal::open(&cfg.state_path)
        .map_err(|e| e.with_context("failed to initialize persistent WAL"))?;
    let mut failed_store = FileFailedWorkStore::open(&failed_path)
        .map_err(|e| e.with_context("failed to initialize failed-work store"))?;

    replay_failed_into_wal(&mut wal, &mut failed_store)
}

pub async fn compact_state_command(
    cfg: &CompactStateConfig,
) -> Result<StateCompactionSummary, RagloomError> {
    let failed_path = failed_work_path_from_state_path(&cfg.state_path);
    compact_state_files(std::path::Path::new(&cfg.state_path), &failed_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::sync::mpsc::Sender;

    use tempfile::NamedTempFile;

    use crate::config::DEFAULT_STATE_PATH;
    use crate::test_support::{TestHttpResponse, spawn_scripted_http_server};

    fn filesystem_source(root: &str) -> RunSource {
        RunSource::Filesystem {
            root: root.to_string(),
        }
    }

    fn sample_run_config() -> RunConfig {
        RunConfig {
            source: filesystem_source("/tmp/docs"),
            embed_backend: EmbedBackend::Http {
                url: "http://embed".to_string(),
                model: "default".to_string(),
            },
            qdrant_url: "http://qdrant".to_string(),
            collection: "docs".to_string(),
            state_path: ".ragloom/wal.ndjson".to_string(),
            health_addr: None,
            create_collection_if_missing: false,
            collection_vector_size: Some(3),
            chunker_strategy: "recursive".to_string(),
            size_metric: "chars".to_string(),
            size_max: 2000,
            size_min: 0,
            size_overlap: 0,
            tokenizer: "tiktoken-cl100k".to_string(),
            chunker_mode: "router".to_string(),
            chunker_single: None,
            enable_semantic: false,
            semantic_provider: "adapter".to_string(),
            semantic_percentile: 95,
            retry_max_attempts: 3,
            retry_max_queued: 128,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 2_000,
        }
    }

    struct RequestCounterServer {
        stop: Sender<()>,
        handle: std::thread::JoinHandle<usize>,
    }

    fn spawn_qdrant_request_counter_server() -> (String, RequestCounterServer) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");
        let (stop_tx, stop_rx) = mpsc::channel();

        let handle = std::thread::spawn(move || {
            let mut requests = 0;

            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buf = [0_u8; 8192];
                        let _ = std::io::Read::read(&mut stream, &mut buf);
                        requests += 1;
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Length: 15\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{{\"status\":\"ok\"}}"
                        )
                        .expect("write response");
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        if stop_rx.try_recv().is_ok() {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(err) => panic!("accept failed: {err}"),
                }
            }

            requests
        });

        (
            format!("http://{}", addr),
            RequestCounterServer {
                stop: stop_tx,
                handle,
            },
        )
    }

    #[test]
    fn validate_reloadable_changes_accepts_retry_only_change() {
        let current = sample_run_config();
        let mut next = sample_run_config();
        next.retry_max_attempts = 5;
        next.retry_max_queued = 32;

        assert!(validate_reloadable_changes(&current, &next).expect("reloadable"));
    }

    #[test]
    fn validate_reloadable_changes_returns_false_for_noop_reload() {
        let current = sample_run_config();
        let next = sample_run_config();

        assert!(!validate_reloadable_changes(&current, &next).expect("noop"));
    }

    #[test]
    fn validate_reloadable_changes_rejects_source_change() {
        let current = sample_run_config();
        let mut next = sample_run_config();
        next.source = filesystem_source("/tmp/other");

        let err = validate_reloadable_changes(&current, &next).expect_err("should reject");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("source"));
    }

    #[test]
    fn validate_reloadable_changes_rejects_health_addr_change() {
        let current = sample_run_config();
        let mut next = sample_run_config();
        next.health_addr = Some("127.0.0.1:8080".to_string());

        let err = validate_reloadable_changes(&current, &next).expect_err("should reject");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("health.addr"));
    }

    #[test]
    fn embed_backend_debug_redacts_openai_api_key() {
        let backend = EmbedBackend::OpenAi {
            endpoint: "https://api.openai.com/v1/embeddings".to_string(),
            api_key: "super-secret".to_string(),
            model: "text-embedding-3-small".to_string(),
        };

        let rendered = format!("{backend:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("super-secret"));
    }

    #[test]
    fn resolve_collection_vector_size_prefers_explicit_override() {
        let cfg = RunConfig {
            source: filesystem_source("/tmp/docs"),
            embed_backend: EmbedBackend::OpenAi {
                endpoint: "https://api.openai.com/v1/embeddings".to_string(),
                api_key: "test-key".to_string(),
                model: "text-embedding-3-small".to_string(),
            },
            qdrant_url: "http://qdrant".to_string(),
            collection: "docs".to_string(),
            state_path: DEFAULT_STATE_PATH.to_string(),
            health_addr: None,
            create_collection_if_missing: true,
            collection_vector_size: Some(768),
            chunker_strategy: "recursive".to_string(),
            size_metric: "chars".to_string(),
            size_max: 2000,
            size_min: 0,
            size_overlap: 0,
            tokenizer: "tiktoken-cl100k".to_string(),
            chunker_mode: "router".to_string(),
            chunker_single: None,
            enable_semantic: false,
            semantic_provider: "adapter".to_string(),
            semantic_percentile: 95,
            retry_max_attempts: 3,
            retry_max_queued: 128,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 2_000,
        };

        let size = resolve_collection_vector_size(&cfg).expect("vector size");
        assert_eq!(size, 768);
    }

    #[test]
    fn resolve_collection_vector_size_infers_known_openai_model_size() {
        let mut cfg = RunConfig {
            source: filesystem_source("/tmp/docs"),
            embed_backend: EmbedBackend::OpenAi {
                endpoint: "https://api.openai.com/v1/embeddings".to_string(),
                api_key: "test-key".to_string(),
                model: "text-embedding-3-small".to_string(),
            },
            qdrant_url: "http://qdrant".to_string(),
            collection: "docs".to_string(),
            state_path: DEFAULT_STATE_PATH.to_string(),
            health_addr: None,
            create_collection_if_missing: true,
            collection_vector_size: None,
            chunker_strategy: "recursive".to_string(),
            size_metric: "chars".to_string(),
            size_max: 2000,
            size_min: 0,
            size_overlap: 0,
            tokenizer: "tiktoken-cl100k".to_string(),
            chunker_mode: "router".to_string(),
            chunker_single: None,
            enable_semantic: false,
            semantic_provider: "adapter".to_string(),
            semantic_percentile: 95,
            retry_max_attempts: 3,
            retry_max_queued: 128,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 2_000,
        };

        for (model, expected_size) in OPENAI_EMBEDDING_VECTOR_SIZES {
            cfg.embed_backend = EmbedBackend::OpenAi {
                endpoint: "https://api.openai.com/v1/embeddings".to_string(),
                api_key: "test-key".to_string(),
                model: model.to_string(),
            };

            let size = resolve_collection_vector_size(&cfg).expect("vector size");
            assert_eq!(size, *expected_size);
        }
    }

    #[test]
    fn resolve_collection_vector_size_rejects_http_backend_without_override() {
        let cfg = RunConfig {
            source: filesystem_source("/tmp/docs"),
            embed_backend: EmbedBackend::Http {
                url: "http://embed".to_string(),
                model: "default".to_string(),
            },
            qdrant_url: "http://qdrant".to_string(),
            collection: "docs".to_string(),
            state_path: DEFAULT_STATE_PATH.to_string(),
            health_addr: None,
            create_collection_if_missing: true,
            collection_vector_size: None,
            chunker_strategy: "recursive".to_string(),
            size_metric: "chars".to_string(),
            size_max: 2000,
            size_min: 0,
            size_overlap: 0,
            tokenizer: "tiktoken-cl100k".to_string(),
            chunker_mode: "router".to_string(),
            chunker_single: None,
            enable_semantic: false,
            semantic_provider: "adapter".to_string(),
            semantic_percentile: 95,
            retry_max_attempts: 3,
            retry_max_queued: 128,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 2_000,
        };

        let err = resolve_collection_vector_size(&cfg).expect_err("expected config error");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("requires --collection-vector-size")
        );
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn prepare_startup_skips_bootstrap_when_embedding_client_construction_fails() {
        let (base_url, server) = spawn_qdrant_request_counter_server();
        let cfg = RunConfig {
            source: filesystem_source("."),
            embed_backend: EmbedBackend::OpenAi {
                endpoint: "https://api.openai.com/v1/embeddings".to_string(),
                api_key: "bad\nkey".to_string(),
                model: "text-embedding-3-small".to_string(),
            },
            qdrant_url: base_url,
            collection: "docs".to_string(),
            state_path: DEFAULT_STATE_PATH.to_string(),
            health_addr: None,
            create_collection_if_missing: true,
            collection_vector_size: None,
            chunker_strategy: "recursive".to_string(),
            size_metric: "chars".to_string(),
            size_max: 2000,
            size_min: 0,
            size_overlap: 0,
            tokenizer: "tiktoken-cl100k".to_string(),
            chunker_mode: "router".to_string(),
            chunker_single: None,
            enable_semantic: false,
            semantic_provider: "adapter".to_string(),
            semantic_percentile: 95,
            retry_max_attempts: 3,
            retry_max_queued: 128,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 2_000,
        };

        let err = match prepare_startup(&cfg, true).await {
            Ok(_) => panic!("expected embedding client construction error"),
            Err(err) => err,
        };
        server.stop.send(()).expect("stop");
        let request_count = server.handle.join().expect("join");

        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("failed to build OpenAI embedding client")
        );
        assert_eq!(request_count, 0);
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn bootstrap_collection_if_needed_infers_known_openai_model_size_in_startup_path() {
        let server = spawn_scripted_http_server(vec![
            TestHttpResponse::json(404, r#"{"status":"not_found"}"#),
            TestHttpResponse::json(200, r#"{"status":"ok"}"#),
        ]);
        let base_url = server.base_url();

        let cfg = RunConfig {
            source: filesystem_source("/tmp/docs"),
            embed_backend: EmbedBackend::OpenAi {
                endpoint: "https://api.openai.com/v1/embeddings".to_string(),
                api_key: "test-key".to_string(),
                model: "text-embedding-3-small".to_string(),
            },
            qdrant_url: base_url.clone(),
            collection: "docs".to_string(),
            state_path: DEFAULT_STATE_PATH.to_string(),
            health_addr: None,
            create_collection_if_missing: true,
            collection_vector_size: None,
            chunker_strategy: "recursive".to_string(),
            size_metric: "chars".to_string(),
            size_max: 2000,
            size_min: 0,
            size_overlap: 0,
            tokenizer: "tiktoken-cl100k".to_string(),
            chunker_mode: "router".to_string(),
            chunker_single: None,
            enable_semantic: false,
            semantic_provider: "adapter".to_string(),
            semantic_percentile: 95,
            retry_max_attempts: 3,
            retry_max_queued: 128,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 2_000,
        };

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        bootstrap_collection_if_needed(&cfg, &sink)
            .await
            .expect("bootstrap");

        let requests = server.join();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("PUT /collections/docs HTTP/1.1"));
        assert!(requests[1].contains(r#""size":1536"#));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn bootstrap_collection_if_needed_uses_explicit_http_vector_size_in_startup_path() {
        let server = spawn_scripted_http_server(vec![
            TestHttpResponse::json(404, r#"{"status":"not_found"}"#),
            TestHttpResponse::json(200, r#"{"status":"ok"}"#),
        ]);
        let base_url = server.base_url();

        let cfg = RunConfig {
            source: filesystem_source("/tmp/docs"),
            embed_backend: EmbedBackend::Http {
                url: "http://embed".to_string(),
                model: "default".to_string(),
            },
            qdrant_url: base_url.clone(),
            collection: "docs".to_string(),
            state_path: DEFAULT_STATE_PATH.to_string(),
            health_addr: None,
            create_collection_if_missing: true,
            collection_vector_size: Some(768),
            chunker_strategy: "recursive".to_string(),
            size_metric: "chars".to_string(),
            size_max: 2000,
            size_min: 0,
            size_overlap: 0,
            tokenizer: "tiktoken-cl100k".to_string(),
            chunker_mode: "router".to_string(),
            chunker_single: None,
            enable_semantic: false,
            semantic_provider: "adapter".to_string(),
            semantic_percentile: 95,
            retry_max_attempts: 3,
            retry_max_queued: 128,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 2_000,
        };

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        bootstrap_collection_if_needed(&cfg, &sink)
            .await
            .expect("bootstrap");

        let requests = server.join();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("PUT /collections/docs HTTP/1.1"));
        assert!(requests[1].contains(r#""size":768"#));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn bootstrap_collection_if_needed_surfaces_unknown_openai_model_with_config_context() {
        let cfg = RunConfig {
            source: filesystem_source("/tmp/docs"),
            embed_backend: EmbedBackend::OpenAi {
                endpoint: "https://api.openai.com/v1/embeddings".to_string(),
                api_key: "test-key".to_string(),
                model: "text-embedding-unknown".to_string(),
            },
            qdrant_url: "http://127.0.0.1:1".to_string(),
            collection: "docs".to_string(),
            state_path: DEFAULT_STATE_PATH.to_string(),
            health_addr: None,
            create_collection_if_missing: true,
            collection_vector_size: None,
            chunker_strategy: "recursive".to_string(),
            size_metric: "chars".to_string(),
            size_max: 2000,
            size_min: 0,
            size_overlap: 0,
            tokenizer: "tiktoken-cl100k".to_string(),
            chunker_mode: "router".to_string(),
            chunker_single: None,
            enable_semantic: false,
            semantic_provider: "adapter".to_string(),
            semantic_percentile: 95,
            retry_max_attempts: 3,
            retry_max_queued: 128,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 2_000,
        };

        let sink = QdrantSink::new(QdrantConfig {
            base_url: cfg.qdrant_url.clone(),
            collection: cfg.collection.clone(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        let err = bootstrap_collection_if_needed(&cfg, &sink)
            .await
            .expect_err("expected config error");

        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("failed to bootstrap Qdrant collection")
        );
        let source = std::error::Error::source(&err).expect("source");
        assert!(
            source.to_string().contains(
                "unknown OpenAI model for collection vector size: text-embedding-unknown"
            )
        );
    }

    #[test]
    fn validate_startup_summary_reports_router_defaults() {
        let cfg = sample_run_config();

        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let summary = runtime.block_on(validate_startup(&cfg)).expect("summary");

        assert_eq!(summary.source_kind, "filesystem");
        assert_eq!(summary.source_target, "/tmp/docs");
        assert_eq!(summary.embed_backend, "http");
        assert_eq!(summary.chunker_selection, "router");
        assert_eq!(summary.bootstrap, BootstrapPlan::Disabled);
        assert_eq!(summary.state_path, ".ragloom/wal.ndjson");
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn validate_startup_render_reports_effective_startup_choices() {
        let cfg = RunConfig {
            create_collection_if_missing: true,
            collection_vector_size: Some(384),
            ..sample_run_config()
        };

        let summary = validate_startup(&cfg).await.expect("summary");
        let rendered = summary.render();

        assert!(rendered.contains("ragloom dry-run"));
        assert!(rendered.contains("source_kind=filesystem"));
        assert!(rendered.contains("source_target=/tmp/docs"));
        assert!(rendered.contains("embed_backend=http"));
        assert!(rendered.contains("chunker=router"));
        assert!(rendered.contains("bootstrap=would ensure collection exists (vector_size=384)"));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn validate_startup_dry_run_skips_qdrant_writes() {
        let (qdrant_url, server) = spawn_qdrant_request_counter_server();
        let cfg = RunConfig {
            qdrant_url,
            create_collection_if_missing: true,
            collection_vector_size: Some(384),
            ..sample_run_config()
        };

        let summary = validate_startup(&cfg).await.expect("summary");
        server.stop.send(()).expect("stop");
        let requests = server.handle.join().expect("join");

        assert_eq!(
            summary.bootstrap,
            BootstrapPlan::WouldEnsureCollection { vector_size: 384 }
        );
        assert_eq!(requests, 0);
    }

    #[tokio::test]
    async fn validate_startup_surfaces_bootstrap_prerequisite_errors_without_io() {
        let cfg = RunConfig {
            create_collection_if_missing: true,
            collection_vector_size: None,
            ..sample_run_config()
        };

        let err = validate_startup(&cfg)
            .await
            .expect_err("expected config error");

        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("failed to bootstrap Qdrant collection")
        );
        let source = std::error::Error::source(&err).expect("source");
        assert!(
            source
                .to_string()
                .contains("http backend requires --collection-vector-size")
        );
    }

    #[test]
    fn single_semantic_chunker_selection_includes_provider() {
        let mut cfg = sample_run_config();
        cfg.chunker_mode = "single".to_string();
        cfg.chunker_single = Some("semantic".to_string());
        cfg.semantic_provider = "adapter".to_string();

        assert_eq!(
            chunker_selection(&cfg).expect("chunker selection"),
            "single:semantic(provider=adapter)"
        );
    }

    #[test]
    fn router_semantic_chunker_selection_includes_provider() {
        let mut cfg = sample_run_config();
        cfg.enable_semantic = true;
        cfg.semantic_provider = "adapter".to_string();

        assert_eq!(
            chunker_selection(&cfg).expect("chunker selection"),
            "router+semantic(provider=adapter)"
        );
    }

    #[test]
    fn chunker_selection_rejects_single_mode_without_kind() {
        let mut cfg = sample_run_config();
        cfg.chunker_mode = "single".to_string();
        cfg.chunker_single = None;

        let err = chunker_selection(&cfg).expect_err("expected invalid config");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("chunker_mode=single requires chunker_single")
        );
    }

    #[tokio::test]
    async fn prepare_startup_rejects_invalid_size_metric_with_config_error() {
        let mut cfg = sample_run_config();
        cfg.size_metric = "bytes".to_string();

        let err = match prepare_startup(&cfg, false).await {
            Ok(_) => panic!("expected invalid config"),
            Err(err) => err,
        };
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("invalid size_metric: bytes"));
    }

    #[tokio::test]
    async fn prepare_startup_rejects_single_mode_without_kind() {
        let mut cfg = sample_run_config();
        cfg.chunker_mode = "single".to_string();
        cfg.chunker_single = None;

        let err = match prepare_startup(&cfg, false).await {
            Ok(_) => panic!("expected invalid config"),
            Err(err) => err,
        };
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("chunker_mode=single requires chunker_single")
        );
    }

    #[test]
    fn replay_failed_into_wal_appends_original_work_and_marks_requeued() {
        let mut wal = crate::state::wal::InMemoryWal::new();
        let mut failed = crate::state::failed::InMemoryFailedWorkStore::new();
        let work = crate::state::wal::WalRecord::WorkItemV2 {
            fingerprint: crate::ids::FileFingerprint {
                canonical_path: "/x/a.txt".to_string(),
                size_bytes: 10,
                mtime_unix_secs: 100,
                etag: None,
            },
        };

        failed
            .append(FailedWorkRecord::Exhausted {
                id: 1,
                work: work.clone(),
                failure_kind: crate::state::failed::FailedWorkFailureKind::Embed,
                terminal_reason: crate::state::failed::FailedWorkTerminalReason::RetryExhausted,
                attempts: 2,
            })
            .expect("append exhausted");

        let summary = replay_failed_into_wal(&mut wal, &mut failed).expect("replay");
        assert_eq!(
            summary,
            ReplayFailedSummary {
                pending: 1,
                requeued: 1,
                skipped: 0,
                failed: 0,
            }
        );
        assert_eq!(wal.read_all().expect("read wal"), vec![work]);
        assert_eq!(
            failed.read_all().expect("read failed"),
            vec![
                FailedWorkRecord::Exhausted {
                    id: 1,
                    work: crate::state::wal::WalRecord::WorkItemV2 {
                        fingerprint: crate::ids::FileFingerprint {
                            canonical_path: "/x/a.txt".to_string(),
                            size_bytes: 10,
                            mtime_unix_secs: 100,
                            etag: None,
                        },
                    },
                    failure_kind: crate::state::failed::FailedWorkFailureKind::Embed,
                    terminal_reason: crate::state::failed::FailedWorkTerminalReason::RetryExhausted,
                    attempts: 2,
                },
                FailedWorkRecord::Requeued { exhausted_id: 1 },
            ]
        );
    }

    #[test]
    fn replay_failed_into_wal_skips_already_requeued_entries() {
        let mut wal = crate::state::wal::InMemoryWal::new();
        let mut failed = crate::state::failed::InMemoryFailedWorkStore::new();

        failed
            .append(FailedWorkRecord::Exhausted {
                id: 1,
                work: crate::state::wal::WalRecord::DeleteDocument {
                    canonical_path: "/x/a.txt".to_string(),
                },
                failure_kind: crate::state::failed::FailedWorkFailureKind::InvalidInput,
                terminal_reason: crate::state::failed::FailedWorkTerminalReason::NonRetryable,
                attempts: 1,
            })
            .expect("append exhausted");
        failed
            .append(FailedWorkRecord::Requeued { exhausted_id: 1 })
            .expect("append requeued");

        let summary = replay_failed_into_wal(&mut wal, &mut failed).expect("replay");
        assert_eq!(
            summary,
            ReplayFailedSummary {
                pending: 0,
                requeued: 0,
                skipped: 1,
                failed: 0,
            }
        );
        assert!(wal.read_all().expect("read wal").is_empty());
    }

    #[test]
    fn replay_failed_into_wal_is_at_least_once_after_partial_prior_replay() {
        let mut wal = crate::state::wal::InMemoryWal::new();
        let mut failed = crate::state::failed::InMemoryFailedWorkStore::new();
        let work = crate::state::wal::WalRecord::WorkItemV2 {
            fingerprint: crate::ids::FileFingerprint {
                canonical_path: "/x/a.txt".to_string(),
                size_bytes: 10,
                mtime_unix_secs: 100,
                etag: None,
            },
        };

        wal.append(work.clone()).expect("append prior replay");
        failed
            .append(FailedWorkRecord::Exhausted {
                id: 1,
                work: work.clone(),
                failure_kind: crate::state::failed::FailedWorkFailureKind::Embed,
                terminal_reason: crate::state::failed::FailedWorkTerminalReason::RetryExhausted,
                attempts: 2,
            })
            .expect("append exhausted");

        let summary = replay_failed_into_wal(&mut wal, &mut failed).expect("replay");
        assert_eq!(summary.requeued, 1);
        assert_eq!(wal.read_all().expect("read wal"), vec![work.clone(), work]);
    }

    #[test]
    fn replay_failed_into_wal_reports_partial_summary_on_failure() {
        struct FailingWal;

        impl crate::state::wal::WalStore for FailingWal {
            fn append(
                &mut self,
                _record: crate::state::wal::WalRecord,
            ) -> Result<(), RagloomError> {
                Err(RagloomError::from_kind(RagloomErrorKind::State)
                    .with_context("simulated WAL failure"))
            }

            fn read_all(&self) -> Result<Vec<crate::state::wal::WalRecord>, RagloomError> {
                Ok(Vec::new())
            }

            fn is_empty(&self) -> bool {
                true
            }
        }

        let mut failed = crate::state::failed::InMemoryFailedWorkStore::new();
        failed
            .append(FailedWorkRecord::Exhausted {
                id: 1,
                work: crate::state::wal::WalRecord::DeleteDocument {
                    canonical_path: "/x/a.txt".to_string(),
                },
                failure_kind: crate::state::failed::FailedWorkFailureKind::State,
                terminal_reason: crate::state::failed::FailedWorkTerminalReason::RetryExhausted,
                attempts: 2,
            })
            .expect("append exhausted");

        let err =
            replay_failed_into_wal(&mut FailingWal, &mut failed).expect_err("replay should fail");

        assert!(
            err.to_string()
                .contains("replay summary: pending=1 requeued=0 skipped=0 failed=1")
        );
    }

    #[tokio::test]
    async fn replay_failed_command_changes_durable_state_metrics_snapshot() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal_path = dir.path().join("wal.ndjson");
        let failed_path = failed_work_path_from_state_path(&wal_path);
        let work = crate::state::wal::WalRecord::WorkItemV2 {
            fingerprint: crate::ids::FileFingerprint {
                canonical_path: "/x/a.txt".to_string(),
                size_bytes: 10,
                mtime_unix_secs: 100,
                etag: None,
            },
        };

        FileFailedWorkStore::open(&failed_path)
            .expect("open failed")
            .append(FailedWorkRecord::Exhausted {
                id: 1,
                work: work.clone(),
                failure_kind: crate::state::failed::FailedWorkFailureKind::Embed,
                terminal_reason: crate::state::failed::FailedWorkTerminalReason::RetryExhausted,
                attempts: 2,
            })
            .expect("append failed");

        let metrics = crate::observability::metrics::IngestionMetrics::default();
        let before = crate::state::durable_state_snapshot_from_paths(&wal_path)
            .expect("state snapshot before replay");
        metrics.replace_durable_state(
            before.wal_bytes,
            before.failed_work_bytes,
            before.wal_pending_work,
            before.failed_work_pending,
        );

        replay_failed_command(&ReplayFailedConfig {
            state_path: wal_path.to_string_lossy().to_string(),
        })
        .await
        .expect("replay failed");

        let after = crate::state::durable_state_snapshot_from_paths(&wal_path)
            .expect("state snapshot after replay");
        metrics.replace_durable_state(
            after.wal_bytes,
            after.failed_work_bytes,
            after.wal_pending_work,
            after.failed_work_pending,
        );

        let snapshot = metrics.snapshot();
        assert_eq!(before.wal_pending_work, 0);
        assert_eq!(before.failed_work_pending, 1);
        assert_eq!(snapshot.wal_pending_work, 1);
        assert_eq!(snapshot.failed_work_pending, 0);
        assert!(snapshot.wal_bytes > before.wal_bytes);
        assert!(snapshot.failed_work_bytes > before.failed_work_bytes);
    }

    #[tokio::test]
    async fn validate_startup_accepts_missing_state_directory_without_creating_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let state_dir = dir.path().join("missing").join("state");
        let mut cfg = sample_run_config();
        cfg.state_path = state_dir.join("wal.ndjson").to_string_lossy().to_string();

        let summary = validate_startup(&cfg).await.expect("validate startup");

        assert_eq!(summary.wal_status, "missing,creatable");
        assert_eq!(summary.failed_work_status, "missing,creatable");
        assert!(
            !state_dir.exists(),
            "check must not create the state directory"
        );
    }

    #[tokio::test]
    async fn validate_startup_rejects_malformed_existing_wal_without_mutating_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal_path = dir.path().join("wal.ndjson");
        std::fs::write(&wal_path, "{not json}\r\n").expect("write malformed wal");
        let before = std::fs::read(&wal_path).expect("read before");
        let mut cfg = sample_run_config();
        cfg.state_path = wal_path.to_string_lossy().to_string();

        let err = validate_startup(&cfg)
            .await
            .expect_err("malformed WAL should fail check");

        assert_eq!(err.kind, RagloomErrorKind::State);
        assert!(err.to_string().contains("failed to parse WAL record"));
        assert_eq!(std::fs::read(&wal_path).expect("read after"), before);
        assert!(!dir.path().join("failed.ndjson").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn validate_startup_rejects_missing_state_when_parent_lacks_execute_bit() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("temp dir");
        let state_dir = dir.path().join("state");
        std::fs::create_dir(&state_dir).expect("create state dir");
        let original_permissions = std::fs::metadata(&state_dir)
            .expect("state dir metadata")
            .permissions();
        std::fs::set_permissions(&state_dir, std::fs::Permissions::from_mode(0o222))
            .expect("remove execute bit");
        let mut cfg = sample_run_config();
        cfg.state_path = state_dir.join("wal.ndjson").to_string_lossy().to_string();

        let err = validate_startup(&cfg)
            .await
            .expect_err("missing WAL parent without execute bit should fail check");

        std::fs::set_permissions(&state_dir, original_permissions)
            .expect("restore state dir permissions");
        assert_eq!(err.kind, RagloomErrorKind::State);
        let message = err.to_string();
        assert!(
            message.contains("failed to read WAL file")
                || message.contains("WAL path is not writable")
        );
    }

    #[tokio::test]
    async fn validate_startup_rejects_read_only_existing_wal() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal_path = dir.path().join("wal.ndjson");
        std::fs::write(&wal_path, "").expect("write WAL");
        let original_permissions = std::fs::metadata(&wal_path)
            .expect("WAL metadata")
            .permissions();
        let mut permissions = original_permissions.clone();
        permissions.set_readonly(true);
        std::fs::set_permissions(&wal_path, permissions).expect("make WAL read-only");
        let mut cfg = sample_run_config();
        cfg.state_path = wal_path.to_string_lossy().to_string();

        let err = validate_startup(&cfg)
            .await
            .expect_err("read-only WAL should fail check");

        assert_eq!(err.kind, RagloomErrorKind::State);
        assert!(err.to_string().contains("WAL path is not writable"));

        std::fs::set_permissions(&wal_path, original_permissions).expect("restore WAL permissions");
    }

    #[tokio::test]
    async fn compact_state_command_preserves_observable_replay_state() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal_path = dir.path().join("wal.ndjson");
        let failed_path = failed_work_path_from_state_path(&wal_path);

        let mut wal = crate::state::wal::FileWal::open(&wal_path).expect("open wal");
        let a_v1 = crate::ids::FileFingerprint {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: None,
        };
        let a_v2 = crate::ids::FileFingerprint {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 11,
            mtime_unix_secs: 101,
            etag: None,
        };
        let b_v1 = crate::ids::FileFingerprint {
            canonical_path: "/x/b.txt".to_string(),
            size_bytes: 20,
            mtime_unix_secs: 200,
            etag: None,
        };
        wal.append(crate::state::wal::WalRecord::WorkItemV2 {
            fingerprint: a_v1.clone(),
        })
        .expect("append work");
        wal.append(crate::state::wal::WalRecord::SinkAckV2 {
            fingerprint: a_v1.clone(),
        })
        .expect("append ack");
        wal.append(crate::state::wal::WalRecord::WorkItemV2 {
            fingerprint: a_v2.clone(),
        })
        .expect("append pending work");
        wal.append(crate::state::wal::WalRecord::WorkItemV2 {
            fingerprint: b_v1.clone(),
        })
        .expect("append b work");
        wal.append(crate::state::wal::WalRecord::SinkAckV2 {
            fingerprint: b_v1.clone(),
        })
        .expect("append b ack");
        wal.append(crate::state::wal::WalRecord::DeleteDocument {
            canonical_path: "/x/c.txt".to_string(),
        })
        .expect("append delete");

        let mut failed = FileFailedWorkStore::open(&failed_path).expect("open failed");
        failed
            .append(FailedWorkRecord::Exhausted {
                id: 1,
                work: crate::state::wal::WalRecord::DeleteDocument {
                    canonical_path: "/x/c.txt".to_string(),
                },
                failure_kind: crate::state::failed::FailedWorkFailureKind::Sink,
                terminal_reason: crate::state::failed::FailedWorkTerminalReason::RetryExhausted,
                attempts: 3,
            })
            .expect("append failed exhausted");
        failed
            .append(FailedWorkRecord::Requeued { exhausted_id: 1 })
            .expect("append requeued");
        failed
            .append(FailedWorkRecord::Exhausted {
                id: 2,
                work: crate::state::wal::WalRecord::WorkItemV2 {
                    fingerprint: a_v2.clone(),
                },
                failure_kind: crate::state::failed::FailedWorkFailureKind::Embed,
                terminal_reason: crate::state::failed::FailedWorkTerminalReason::NonRetryable,
                attempts: 1,
            })
            .expect("append pending failed exhausted");

        let wal_before = crate::state::wal::FileWal::open(&wal_path)
            .expect("reopen wal")
            .read_all()
            .expect("read wal before");
        let failed_before = FileFailedWorkStore::open(&failed_path)
            .expect("reopen failed")
            .read_all()
            .expect("read failed before");

        let summary = compact_state_command(&CompactStateConfig {
            state_path: wal_path.to_string_lossy().to_string(),
        })
        .await
        .expect("compact state");

        assert!(summary.wal.records_after <= summary.wal.records_before);
        assert!(summary.failed_work.records_after <= summary.failed_work.records_before);

        let wal_after = crate::state::wal::FileWal::open(&wal_path)
            .expect("reopen wal after")
            .read_all()
            .expect("read wal after");
        let failed_after = FileFailedWorkStore::open(&failed_path)
            .expect("reopen failed after")
            .read_all()
            .expect("read failed after");

        assert_eq!(
            crate::state::wal::unacked_work_items(&wal_after),
            crate::state::wal::unacked_work_items(&wal_before)
        );
        assert_eq!(
            crate::state::wal::known_live_document_paths(&wal_after),
            crate::state::wal::known_live_document_paths(&wal_before)
        );
        assert_eq!(
            crate::state::failed::pending_failed_work(&failed_after),
            crate::state::failed::pending_failed_work(&failed_before)
        );
        assert_eq!(
            crate::state::failed::next_failed_work_id(&failed_after),
            crate::state::failed::next_failed_work_id(&failed_before)
        );
    }

    #[tokio::test]
    async fn compact_state_command_changes_durable_state_metric_sizes_without_changing_backlog() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal_path = dir.path().join("wal.ndjson");
        let failed_path = failed_work_path_from_state_path(&wal_path);

        let mut wal = crate::state::wal::FileWal::open(&wal_path).expect("open wal");
        let fingerprint = crate::ids::FileFingerprint {
            canonical_path: "/x/a.txt".to_string(),
            size_bytes: 10,
            mtime_unix_secs: 100,
            etag: None,
        };
        wal.append(crate::state::wal::WalRecord::WorkItemV2 {
            fingerprint: fingerprint.clone(),
        })
        .expect("append work");
        wal.append(crate::state::wal::WalRecord::SinkAckV2 {
            fingerprint: fingerprint.clone(),
        })
        .expect("append ack");
        wal.append(crate::state::wal::WalRecord::DeleteDocument {
            canonical_path: "/x/b.txt".to_string(),
        })
        .expect("append delete");

        let mut failed = FileFailedWorkStore::open(&failed_path).expect("open failed");
        failed
            .append(FailedWorkRecord::Exhausted {
                id: 1,
                work: crate::state::wal::WalRecord::DeleteDocument {
                    canonical_path: "/x/b.txt".to_string(),
                },
                failure_kind: crate::state::failed::FailedWorkFailureKind::Sink,
                terminal_reason: crate::state::failed::FailedWorkTerminalReason::RetryExhausted,
                attempts: 2,
            })
            .expect("append failed");
        failed
            .append(FailedWorkRecord::Requeued { exhausted_id: 1 })
            .expect("append requeued");

        let metrics = crate::observability::metrics::IngestionMetrics::default();
        let before = crate::state::durable_state_snapshot_from_paths(&wal_path)
            .expect("state snapshot before compaction");
        metrics.replace_durable_state(
            before.wal_bytes,
            before.failed_work_bytes,
            before.wal_pending_work,
            before.failed_work_pending,
        );

        compact_state_command(&CompactStateConfig {
            state_path: wal_path.to_string_lossy().to_string(),
        })
        .await
        .expect("compact state");

        let after = crate::state::durable_state_snapshot_from_paths(&wal_path)
            .expect("state snapshot after compaction");
        metrics.replace_durable_state(
            after.wal_bytes,
            after.failed_work_bytes,
            after.wal_pending_work,
            after.failed_work_pending,
        );

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.wal_pending_work, before.wal_pending_work as u64);
        assert_eq!(
            snapshot.failed_work_pending,
            before.failed_work_pending as u64
        );
        assert!(snapshot.wal_bytes <= before.wal_bytes);
        assert!(snapshot.failed_work_bytes <= before.failed_work_bytes);
    }

    #[tokio::test]
    async fn compact_state_command_fails_on_malformed_wal_without_replacing_original() {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal_path = dir.path().join("wal.ndjson");
        std::fs::write(&wal_path, "{not json}\n").expect("write malformed wal");

        let err = compact_state_command(&CompactStateConfig {
            state_path: wal_path.to_string_lossy().to_string(),
        })
        .await
        .expect_err("malformed wal should fail");

        assert!(
            err.to_string()
                .contains("failed to initialize persistent WAL")
        );
        assert_eq!(
            std::fs::read_to_string(&wal_path).expect("read original wal"),
            "{not json}\n"
        );
    }

    #[test]
    fn validate_startup_module_tests_compile_before_main_migration() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(b"placeholder").expect("write placeholder");
        assert!(file.path().exists());
    }
}

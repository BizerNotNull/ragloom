//! Reusable startup wiring for Ragloom runtimes.
//!
//! # Why
//! The CLI binary should stay focused on argument parsing and top-level process
//! orchestration. Reusable startup, validation, and replay behavior belongs in
//! the library crate so it can be tested in its owning module.

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use crate::error::{RagloomError, RagloomErrorKind};
use crate::sink::qdrant::{QdrantConfig, QdrantSink};
use crate::source::runtime::{RunSource, prepare_source_runtime};
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

    Ok(StartupValidationSummary {
        source_kind: cfg.source.kind().to_string(),
        source_target: cfg.source.log_target(),
        embed_backend: cfg.embed_backend.name().to_string(),
        chunker_selection: chunker_selection(cfg)?,
        bootstrap: bootstrap_plan(cfg)?,
    })
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
) -> Result<usize, RagloomError> {
    let records = failed_store
        .read_all()
        .map_err(|e| e.with_context("failed to read failed-work store"))?;

    let pending = pending_failed_work(&records);
    let replayed = pending.len();
    for item in pending {
        wal.append(item.work.clone())
            .map_err(|e| e.with_context("failed to append replayed work into WAL"))?;
        failed_store
            .append(FailedWorkRecord::Requeued {
                exhausted_id: item.id,
            })
            .map_err(|e| e.with_context("failed to mark failed work as requeued"))?;

        tracing::info!(
            event.name = "ragloom.failed_work.requeued",
            exhausted_id = item.id,
            terminal_reason = ?item.terminal_reason,
            attempts = item.attempts,
            "ragloom.failed_work.requeued"
        );
    }

    Ok(replayed)
}

pub async fn replay_failed_command(cfg: &ReplayFailedConfig) -> Result<usize, RagloomError> {
    let failed_path = failed_work_path_from_state_path(&cfg.state_path);
    let mut wal = crate::state::wal::FileWal::open(&cfg.state_path)
        .map_err(|e| e.with_context("failed to initialize persistent WAL"))?;
    let mut failed_store = FileFailedWorkStore::open(&failed_path)
        .map_err(|e| e.with_context("failed to initialize failed-work store"))?;

    replay_failed_into_wal(&mut wal, &mut failed_store)
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

        assert_eq!(
            summary,
            StartupValidationSummary {
                source_kind: "filesystem".to_string(),
                source_target: "/tmp/docs".to_string(),
                embed_backend: "http".to_string(),
                chunker_selection: "router".to_string(),
                bootstrap: BootstrapPlan::Disabled,
            }
        );
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

        let replayed = replay_failed_into_wal(&mut wal, &mut failed).expect("replay");
        assert_eq!(replayed, 1);
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

        let replayed = replay_failed_into_wal(&mut wal, &mut failed).expect("replay");
        assert_eq!(replayed, 0);
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

        let replayed = replay_failed_into_wal(&mut wal, &mut failed).expect("replay");
        assert_eq!(replayed, 1);
        assert_eq!(wal.read_all().expect("read wal"), vec![work.clone(), work]);
    }

    #[test]
    fn validate_startup_module_tests_compile_before_main_migration() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(b"placeholder").expect("write placeholder");
        assert!(file.path().exists());
    }
}

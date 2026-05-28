//! Ragloom CLI runner.
//!
//! # Why
//! The library crate contains most logic and is reusable by other programs.
//! This binary provides the minimum wiring to run the real I/O pipeline in a
//! single daemon-style process.

use std::time::Duration;

use ragloom::config::reload::{FileReloadSource, ReloadSource};
use ragloom::config::{DEFAULT_STATE_PATH, PipelineConfig};
use ragloom::embed::http_client::{HttpEmbeddingClient, HttpEmbeddingConfig};
use ragloom::error::{RagloomError, RagloomErrorKind};
use ragloom::observability::health::{HealthServer, HealthState};
use ragloom::observability::metrics::IngestionMetrics;
use ragloom::pipeline::runtime::{
    AckingExecutor, AsyncRuntime, IngestionSummary, LiveRetryPolicy, PipelineExecutor, RetryPolicy,
    Runtime, RuntimeExitReason, run_worker_with_live_retry_and_metrics,
};
use ragloom::sink::qdrant::{QdrantConfig, QdrantSink};
use ragloom::source::runtime::{RunSource, USAGE, prepare_source_runtime, resolve_run_source};

#[cfg(test)]
mod test_support;

const CONFIG_RELOAD_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Runtime configuration constructed from CLI arguments.
///
/// # Why
/// Keeping configuration in a struct makes the CLI parsing testable and keeps
/// `main()` focused on wiring.
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

/// Top-level CLI command selected by argument parsing.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ParsedCommand {
    // Box the run config to keep this enum small enough for clippy's
    // `large_enum_variant` lint while still modeling early-exit commands cleanly.
    Run(Box<RunConfig>),
    Help,
    Version,
}

#[derive(Debug, Clone, Default)]
struct RawCliArgs {
    config_path: Option<String>,
    source_kind: Option<String>,
    dir: Option<String>,
    s3_bucket: Option<String>,
    s3_prefix: Option<String>,
    embed_backend: Option<String>,
    embed_url: Option<String>,
    embed_model: Option<String>,
    openai_endpoint: Option<String>,
    openai_api_key: Option<String>,
    openai_model: Option<String>,
    qdrant_url: Option<String>,
    collection: Option<String>,
    state_path: Option<String>,
    health_addr: Option<String>,
    create_collection_if_missing: bool,
    collection_vector_size: Option<String>,
    chunker_strategy: Option<String>,
    size_metric: Option<String>,
    size_max: Option<String>,
    size_min: Option<String>,
    size_overlap: Option<String>,
    tokenizer: Option<String>,
    chunker_mode: Option<String>,
    chunker_single: Option<String>,
    enable_semantic: bool,
    semantic_provider: Option<String>,
    semantic_percentile: Option<String>,
    retry_max_attempts: Option<String>,
    retry_max_queued: Option<String>,
    retry_initial_backoff_ms: Option<String>,
    retry_max_backoff_ms: Option<String>,
}

enum RawParsedCommand {
    Run(Box<RawCliArgs>),
    Help,
    Version,
}

/// Embedding backend selection.
///
/// # Why
/// Keeping selection as an enum makes backend-specific required flags explicit
/// and prevents invalid combinations from reaching wiring.
#[derive(Debug, Clone, Eq, PartialEq)]
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

/// Parse CLI arguments into a top-level command.
///
/// # Why
/// Using `std::env::args` keeps the binary dependency-free while still allowing
/// deterministic unit tests for argument handling.
pub fn parse_args(args: &[String]) -> Result<ParsedCommand, RagloomError> {
    match parse_raw_cli_args(args)? {
        RawParsedCommand::Help => Ok(ParsedCommand::Help),
        RawParsedCommand::Version => Ok(ParsedCommand::Version),
        RawParsedCommand::Run(raw) => {
            let file_config = raw
                .config_path
                .as_deref()
                .map(load_pipeline_config)
                .transpose()?;
            Ok(ParsedCommand::Run(Box::new(build_run_config(
                *raw,
                file_config.as_ref(),
            )?)))
        }
    }
}

fn parse_reload_run_config_from_contents(
    args: &[String],
    yaml: &str,
    path: &std::path::Path,
) -> Result<RunConfig, RagloomError> {
    let raw = match parse_raw_cli_args(args)? {
        RawParsedCommand::Run(raw) => *raw,
        RawParsedCommand::Help | RawParsedCommand::Version => {
            return Err(
                RagloomError::from_kind(RagloomErrorKind::Config).with_context(format!(
                    "config reload expected runtime command for {}",
                    path.display()
                )),
            );
        }
    };
    let file_config = PipelineConfig::from_yaml_str(yaml)
        .map_err(|e| e.with_context(format!("failed to parse config file: {}", path.display())))?;
    build_run_config(raw, Some(&file_config))
}

fn parse_raw_cli_args(args: &[String]) -> Result<RawParsedCommand, RagloomError> {
    let mut raw = RawCliArgs::default();
    let mut iter = args.iter().skip(1).peekable();
    while let Some(arg) = iter.next() {
        let (flag, inline_value) = match arg.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (arg.as_str(), None),
        };

        match flag {
            "--config" => raw.config_path = next_arg_value(inline_value, &mut iter),
            "--source-kind" => raw.source_kind = next_arg_value(inline_value, &mut iter),
            "--dir" => raw.dir = next_arg_value(inline_value, &mut iter),
            "--s3-bucket" => raw.s3_bucket = next_arg_value(inline_value, &mut iter),
            "--s3-prefix" => raw.s3_prefix = next_arg_value(inline_value, &mut iter),
            "--embed-backend" => raw.embed_backend = next_arg_value(inline_value, &mut iter),
            "--embed-url" => raw.embed_url = next_arg_value(inline_value, &mut iter),
            "--embed-model" => raw.embed_model = next_arg_value(inline_value, &mut iter),
            "--openai-endpoint" => raw.openai_endpoint = next_arg_value(inline_value, &mut iter),
            "--openai-api-key" => raw.openai_api_key = next_arg_value(inline_value, &mut iter),
            "--openai-model" => raw.openai_model = next_arg_value(inline_value, &mut iter),
            "--qdrant-url" => raw.qdrant_url = next_arg_value(inline_value, &mut iter),
            "--collection" => raw.collection = next_arg_value(inline_value, &mut iter),
            "--state-path" => raw.state_path = next_arg_value(inline_value, &mut iter),
            "--health-addr" => raw.health_addr = next_arg_value(inline_value, &mut iter),
            "--create-collection-if-missing" => {
                validate_boolean_flag(
                    flag,
                    inline_value,
                    iter.peek().map(|next_arg| next_arg.as_str()),
                )?;
                raw.create_collection_if_missing = true;
            }
            "--collection-vector-size" => {
                raw.collection_vector_size =
                    Some(next_arg_value(inline_value, &mut iter).ok_or_else(|| {
                        cli_invalid_input("missing required value: --collection-vector-size")
                    })?);
            }
            "--chunker-strategy" => raw.chunker_strategy = next_arg_value(inline_value, &mut iter),
            "--size-metric" => raw.size_metric = next_arg_value(inline_value, &mut iter),
            "--size-max" => raw.size_max = next_arg_value(inline_value, &mut iter),
            "--size-min" => raw.size_min = next_arg_value(inline_value, &mut iter),
            "--size-overlap" => raw.size_overlap = next_arg_value(inline_value, &mut iter),
            "--tokenizer" => raw.tokenizer = next_arg_value(inline_value, &mut iter),
            "--chunker-mode" => raw.chunker_mode = next_arg_value(inline_value, &mut iter),
            "--chunker-single" => raw.chunker_single = next_arg_value(inline_value, &mut iter),
            "--enable-semantic" => {
                validate_boolean_flag(
                    flag,
                    inline_value,
                    iter.peek().map(|next_arg| next_arg.as_str()),
                )?;
                raw.enable_semantic = true;
            }
            "--semantic-provider" => {
                raw.semantic_provider = next_arg_value(inline_value, &mut iter)
            }
            "--semantic-percentile" => {
                raw.semantic_percentile = next_arg_value(inline_value, &mut iter)
            }
            "--retry-max-attempts" => {
                raw.retry_max_attempts = next_arg_value(inline_value, &mut iter)
            }
            "--retry-max-queued" => raw.retry_max_queued = next_arg_value(inline_value, &mut iter),
            "--retry-initial-backoff-ms" => {
                raw.retry_initial_backoff_ms = next_arg_value(inline_value, &mut iter)
            }
            "--retry-max-backoff-ms" => {
                raw.retry_max_backoff_ms = next_arg_value(inline_value, &mut iter)
            }
            "--help" | "-h" => return Ok(RawParsedCommand::Help),
            "--version" | "-V" => return Ok(RawParsedCommand::Version),
            unknown => return Err(cli_invalid_input(format!("unknown flag: {unknown}"))),
        }
    }
    Ok(RawParsedCommand::Run(Box::new(raw)))
}

fn build_run_config(
    raw: RawCliArgs,
    file_config: Option<&PipelineConfig>,
) -> Result<RunConfig, RagloomError> {
    let source = resolve_run_source(
        raw.source_kind.as_deref(),
        raw.dir,
        raw.s3_bucket,
        raw.s3_prefix,
        file_config.map(|cfg| &cfg.source),
    )?;

    let qdrant_url = raw
        .qdrant_url
        .or_else(|| file_config.map(|c| c.sink.qdrant_url.clone()))
        .ok_or_else(|| {
            cli_config_error("missing required value: --qdrant-url or sink.qdrant_url in --config")
        })?;
    if qdrant_url.trim().is_empty() {
        return Err(cli_config_error("--qdrant-url or sink.qdrant_url is empty"));
    }
    let collection = raw
        .collection
        .or_else(|| file_config.map(|c| c.sink.collection.clone()))
        .ok_or_else(|| {
            cli_config_error("missing required value: --collection or sink.collection in --config")
        })?;
    if collection.trim().is_empty() {
        return Err(cli_config_error("--collection or sink.collection is empty"));
    }
    let state_path = raw
        .state_path
        .or_else(|| file_config.map(|c| c.state.path.clone()))
        .unwrap_or_else(|| DEFAULT_STATE_PATH.to_string());
    if state_path.trim().is_empty() {
        return Err(cli_config_error("--state-path or state.path is empty"));
    }
    let health_addr = raw
        .health_addr
        .or_else(|| file_config.and_then(|c| c.health.addr.clone()));
    if health_addr
        .as_deref()
        .is_some_and(|addr| addr.trim().is_empty())
    {
        return Err(cli_config_error("--health-addr or health.addr is empty"));
    }
    let collection_vector_size = raw
        .collection_vector_size
        .map(|s| {
            s.parse::<usize>().map_err(|e| {
                RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                    .with_context(format!("--collection-vector-size must be integer: {e}"))
            })
        })
        .transpose()?;
    if collection_vector_size == Some(0) {
        return Err(cli_invalid_input(
            "--collection-vector-size must be positive",
        ));
    }

    let backend = raw.embed_backend.unwrap_or_else(|| "openai".to_string());

    tracing::info!(
        event.name = "ragloom.start",
        source_kind = %source.kind(),
        source_target = %source.log_target(),
        embed_backend = %backend,
        qdrant_collection = %collection,
        "ragloom.start"
    );

    let embed_backend = match backend.as_str() {
        "openai" => {
            let endpoint = raw
                .openai_endpoint
                .or_else(|| file_config.map(|c| c.embed.endpoint.clone()))
                .unwrap_or_else(|| "https://api.openai.com/v1/embeddings".to_string());
            if endpoint.trim().is_empty() {
                return Err(cli_config_error(
                    "--openai-endpoint or embed.endpoint is empty",
                ));
            }
            let api_key = raw.openai_api_key.ok_or_else(|| {
                cli_config_error("missing required flag for openai backend: --openai-api-key")
            })?;
            let model = raw
                .openai_model
                .unwrap_or_else(|| "text-embedding-3-small".to_string());
            EmbedBackend::OpenAi {
                endpoint,
                api_key,
                model,
            }
        }
        "http" => {
            let url = raw
                .embed_url
                .or_else(|| file_config.map(|c| c.embed.endpoint.clone()))
                .ok_or_else(|| {
                    cli_config_error(
                        "missing required value for http backend: --embed-url or embed.endpoint in --config",
                    )
                })?;
            if url.trim().is_empty() {
                return Err(cli_config_error("--embed-url or embed.endpoint is empty"));
            }
            let model = raw.embed_model.unwrap_or_else(|| "default".to_string());
            EmbedBackend::Http { url, model }
        }
        other => {
            return Err(cli_invalid_input(format!(
                "invalid value for --embed-backend: {other} (expected: openai|http)"
            )));
        }
    };

    let chunker_strategy = raw
        .chunker_strategy
        .unwrap_or_else(|| "recursive".to_string());
    match chunker_strategy.as_str() {
        "recursive" | "legacy" => {}
        other => {
            return Err(cli_invalid_input(format!(
                "invalid --chunker-strategy: {other} (expected: recursive|legacy)"
            )));
        }
    }

    let size_metric = raw.size_metric.unwrap_or_else(|| "chars".to_string());
    match size_metric.as_str() {
        "chars" | "tokens" => {}
        other => {
            return Err(cli_invalid_input(format!(
                "invalid --size-metric: {other} (expected: chars|tokens)"
            )));
        }
    }

    let size_max = raw
        .size_max
        .map(|s| {
            s.parse::<usize>().map_err(|e| {
                RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                    .with_context(format!("--size-max must be integer: {e}"))
            })
        })
        .transpose()?
        .unwrap_or(if size_metric == "tokens" { 512 } else { 2000 });

    let size_min = raw
        .size_min
        .map(|s| {
            s.parse::<usize>().map_err(|e| {
                RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                    .with_context(format!("--size-min must be integer: {e}"))
            })
        })
        .transpose()?
        .unwrap_or(0);

    let size_overlap = raw
        .size_overlap
        .map(|s| {
            s.parse::<usize>().map_err(|e| {
                RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                    .with_context(format!("--size-overlap must be integer: {e}"))
            })
        })
        .transpose()?
        .unwrap_or(0);

    let tokenizer = raw
        .tokenizer
        .unwrap_or_else(|| "tiktoken-cl100k".to_string());
    match tokenizer.as_str() {
        "tiktoken-cl100k" => {}
        other => {
            return Err(cli_invalid_input(format!(
                "invalid --tokenizer: {other} (expected: tiktoken-cl100k)"
            )));
        }
    }

    let chunker_mode = raw.chunker_mode.unwrap_or_else(|| "router".to_string());
    match chunker_mode.as_str() {
        "router" | "single" => {}
        other => {
            return Err(cli_invalid_input(format!(
                "invalid --chunker-mode: {other} (expected: router|single)"
            )));
        }
    }
    if chunker_mode == "single" && raw.chunker_single.is_none() {
        return Err(cli_config_error(
            "--chunker-mode=single requires --chunker-single",
        ));
    }

    if raw.enable_semantic
        && chunker_mode == "single"
        && raw.chunker_single.as_deref() != Some("semantic")
    {
        return Err(
            RagloomError::from_kind(RagloomErrorKind::InvalidInput).with_context(
                "--enable-semantic is only honored with --chunker-mode=router or \
             --chunker-mode=single with --chunker-single=semantic",
            ),
        );
    }

    let semantic_provider = raw
        .semantic_provider
        .unwrap_or_else(|| "adapter".to_string());
    match semantic_provider.as_str() {
        "adapter" => {}
        "fastembed" => {
            #[cfg(not(feature = "fastembed"))]
            {
                return Err(
                    RagloomError::from_kind(RagloomErrorKind::InvalidInput).with_context(
                        "--semantic-provider=fastembed requires the \"fastembed\" Cargo feature",
                    ),
                );
            }
        }
        other => {
            return Err(
                RagloomError::from_kind(RagloomErrorKind::InvalidInput).with_context(format!(
                    "invalid --semantic-provider: {other} (expected: adapter|fastembed)"
                )),
            );
        }
    }

    let semantic_percentile = raw
        .semantic_percentile
        .map(|s| {
            s.parse::<u8>().map_err(|e| {
                RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                    .with_context(format!("--semantic-percentile must be 1..=99: {e}"))
            })
        })
        .transpose()?
        .unwrap_or(95);
    if !(1..=99).contains(&semantic_percentile) {
        return Err(
            RagloomError::from_kind(RagloomErrorKind::InvalidInput).with_context(format!(
                "--semantic-percentile must be in 1..=99, got {semantic_percentile}"
            )),
        );
    }

    let retry_max_attempts = raw
        .retry_max_attempts
        .map(|s| {
            s.parse::<u32>().map_err(|e| {
                RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                    .with_context(format!("--retry-max-attempts must be integer: {e}"))
            })
        })
        .transpose()?
        .or_else(|| file_config.map(|c| c.retry.max_attempts))
        .unwrap_or(3);

    let retry_max_queued = raw
        .retry_max_queued
        .map(|s| {
            s.parse::<usize>().map_err(|e| {
                RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                    .with_context(format!("--retry-max-queued must be integer: {e}"))
            })
        })
        .transpose()?
        .or_else(|| file_config.map(|c| c.retry.max_queued))
        .unwrap_or(128);

    let retry_initial_backoff_ms = raw
        .retry_initial_backoff_ms
        .map(|s| {
            s.parse::<u64>().map_err(|e| {
                RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                    .with_context(format!("--retry-initial-backoff-ms must be integer: {e}"))
            })
        })
        .transpose()?
        .or_else(|| file_config.map(|c| c.retry.initial_backoff_ms))
        .unwrap_or(100);

    let retry_max_backoff_ms = raw
        .retry_max_backoff_ms
        .map(|s| {
            s.parse::<u64>().map_err(|e| {
                RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                    .with_context(format!("--retry-max-backoff-ms must be integer: {e}"))
            })
        })
        .transpose()?
        .or_else(|| file_config.map(|c| c.retry.max_backoff_ms))
        .unwrap_or(2_000);

    let retry_policy = RetryPolicy {
        max_attempts: retry_max_attempts,
        max_queued_retries: retry_max_queued,
        initial_backoff: Duration::from_millis(retry_initial_backoff_ms),
        max_backoff: Duration::from_millis(retry_max_backoff_ms),
    };
    retry_policy.validate()?;

    Ok(RunConfig {
        source,
        embed_backend,
        qdrant_url,
        collection,
        state_path,
        health_addr,
        create_collection_if_missing: raw.create_collection_if_missing,
        collection_vector_size,
        chunker_strategy,
        size_metric,
        size_max,
        size_min,
        size_overlap,
        tokenizer,
        chunker_mode,
        chunker_single: raw.chunker_single,
        enable_semantic: raw.enable_semantic,
        semantic_provider,
        semantic_percentile,
        retry_max_attempts,
        retry_max_queued,
        retry_initial_backoff_ms,
        retry_max_backoff_ms,
    })
}

fn load_pipeline_config(path: &str) -> Result<PipelineConfig, RagloomError> {
    let yaml = std::fs::read_to_string(path).map_err(|e| {
        RagloomError::new(RagloomErrorKind::Io, e)
            .with_context(format!("failed to read config file: {path}"))
    })?;

    // CLI flags are merged after file load, so parse the raw shape here and let
    // the merged runtime configuration enforce the effective invariants.
    PipelineConfig::from_yaml_str(&yaml)
        .map_err(|e| e.with_context(format!("failed to parse config file: {path}")))
}

fn cli_invalid_input(message: impl Into<String>) -> RagloomError {
    let message = message.into();
    RagloomError::from_kind(RagloomErrorKind::InvalidInput)
        .with_context(format!("{message}\n{USAGE}"))
}

fn validate_boolean_flag(
    flag: &str,
    inline_value: Option<&str>,
    next_arg: Option<&str>,
) -> Result<(), RagloomError> {
    if inline_value.is_some() || next_arg.is_some_and(|arg| !arg.starts_with('-')) {
        return Err(cli_invalid_input(format!("{flag} does not accept a value")));
    }

    Ok(())
}

fn next_arg_value<'a, I>(inline_value: Option<&str>, iter: &mut I) -> Option<String>
where
    I: Iterator<Item = &'a String>,
{
    inline_value
        .map(str::to_string)
        .or_else(|| iter.next().cloned())
}

fn cli_config_error(message: impl Into<String>) -> RagloomError {
    let message = message.into();
    RagloomError::from_kind(RagloomErrorKind::Config).with_context(format!("{message}\n{USAGE}"))
}

fn parse_code_lang(s: &str) -> Result<ragloom::transform::chunker::code::Language, RagloomError> {
    use ragloom::transform::chunker::code::Language;
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
        EmbedBackend::OpenAi { model, .. } => format!("openai:{}", model),
        EmbedBackend::Http { model, .. } => format!("http:{}", model),
    }
}

const OPENAI_EMBEDDING_VECTOR_SIZES: &[(&str, usize)] = &[
    ("text-embedding-3-small", 1536),
    ("text-embedding-3-large", 3072),
    ("text-embedding-ada-002", 1536),
];

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
        EmbedBackend::OpenAi { model, .. } => {
            openai_embedding_vector_size(model).ok_or_else(|| {
                let context = format!(
                    "unknown OpenAI model for collection vector size: {model}; pass --collection-vector-size"
                );
                RagloomError::from_kind(RagloomErrorKind::Config).with_context(context)
            })
        }
        EmbedBackend::Http { .. } => Err(RagloomError::from_kind(RagloomErrorKind::Config)
            .with_context("http backend requires --collection-vector-size")),
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

struct PreparedStartup {
    embedding: std::sync::Arc<dyn ragloom::embed::EmbeddingProvider + Send + Sync>,
    sink: QdrantSink,
    chunker: std::sync::Arc<dyn ragloom::transform::chunker::Chunker>,
}

struct RunningSystem {
    health_state: HealthState,
    health_server: Option<HealthServer>,
    shutdown: ragloom::pipeline::runtime::ShutdownHandle,
    worker: tokio::task::JoinHandle<()>,
    health_monitor: tokio::task::JoinHandle<()>,
    summary: IngestionSummary,
    retry_policy: LiveRetryPolicy,
}

impl RunningSystem {
    async fn shutdown(self, trigger: &'static str) {
        self.health_state.mark_shutting_down();
        self.shutdown.shutdown();
        if let Some(server) = self.health_server {
            server.shutdown().await;
        }
        let _ = self.worker.await;
        self.health_monitor.abort();
        let _ = self.health_monitor.await;
        self.summary.emit_if_dirty(trigger);
    }
}

async fn prepare_startup(cfg: &RunConfig) -> Result<PreparedStartup, RagloomError> {
    let embed_fingerprint = embedding_fingerprint(cfg);

    let embedding: std::sync::Arc<dyn ragloom::embed::EmbeddingProvider + Send + Sync> =
        match &cfg.embed_backend {
            EmbedBackend::OpenAi {
                endpoint,
                api_key,
                model,
            } => {
                let client = ragloom::embed::openai_client::OpenAiEmbeddingClient::new(
                    ragloom::embed::openai_client::OpenAiEmbeddingConfig {
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
                let client = HttpEmbeddingClient::new(HttpEmbeddingConfig {
                    endpoint: url.clone(),
                    model: model.clone(),
                    timeout: Duration::from_secs(30),
                })
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

    let metric = match cfg.size_metric.as_str() {
        "chars" => ragloom::transform::chunker::size::SizeMetric::Chars,
        "tokens" => ragloom::transform::chunker::size::SizeMetric::Tokens,
        _ => unreachable!("validated in parse_args"),
    };

    let rec_cfg = ragloom::transform::chunker::recursive::RecursiveConfig {
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

    use ragloom::transform::chunker::{
        Chunker, EmbeddingProviderAdapter, MarkdownChunker, SemanticChunker,
        SemanticSignalProvider, default_router, recursive::RecursiveChunker, semantic_router,
    };

    let chunker: std::sync::Arc<dyn Chunker> = if cfg.chunker_mode == "router"
        && cfg.enable_semantic
    {
        let signal: std::sync::Arc<dyn SemanticSignalProvider> =
            match cfg.semantic_provider.as_str() {
                "adapter" => std::sync::Arc::new(EmbeddingProviderAdapter::new(
                    std::sync::Arc::clone(&embedding),
                    embed_fingerprint.clone(),
                )),
                #[cfg(feature = "fastembed")]
                "fastembed" => std::sync::Arc::new(
                    ragloom::transform::chunker::FastembedSignalProvider::new().map_err(|e| {
                        RagloomError::new(RagloomErrorKind::Config, e)
                            .with_context("fastembed init")
                    })?,
                ),
                other => {
                    return Err(RagloomError::from_kind(RagloomErrorKind::InvalidInput)
                        .with_context(format!("unsupported --semantic-provider: {other}")));
                }
            };
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
                let kind = cfg.chunker_single.as_deref().unwrap();
                match kind {
                    "semantic" => {
                        let signal: std::sync::Arc<dyn SemanticSignalProvider> =
                            std::sync::Arc::new(EmbeddingProviderAdapter::new(
                                std::sync::Arc::clone(&embedding),
                                embed_fingerprint.clone(),
                            ));
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
                            ragloom::transform::chunker::CodeChunker::new(lang, rec_cfg).map_err(
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
            _ => unreachable!("validated in parse_args"),
        }
    };

    bootstrap_collection_if_needed(cfg, &sink).await?;

    Ok(PreparedStartup {
        embedding,
        sink,
        chunker,
    })
}

#[tokio::main]
async fn main() {
    if let Err(err) = try_main().await {
        tracing::error!(
            error.message = %err,
            error.kind = %err.kind,
            "ragloom.fatal"
        );
        std::process::exit(1);
    }
}

async fn try_main() -> Result<(), RagloomError> {
    let obs_cfg = ragloom::observability::load_from_process_env()?;
    let dispatch = ragloom::observability::init_subscriber(&obs_cfg)?;
    tracing::dispatcher::set_global_default(dispatch).map_err(|e| {
        RagloomError::new(RagloomErrorKind::Internal, e)
            .with_context("failed to install tracing subscriber")
    })?;

    tracing::info!(
        event.name = "ragloom.log_config",
        log_format = ?obs_cfg.format,
        log_filter = %obs_cfg.filter_directives,
        "ragloom.log_config"
    );

    let args: Vec<String> = std::env::args().collect();
    let cfg = match parse_args(&args)? {
        ParsedCommand::Help => {
            println!("{USAGE}");
            return Ok(());
        }
        ParsedCommand::Version => {
            println!("ragloom {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        ParsedCommand::Run(cfg) => *cfg,
    };
    let mut reload_source = extract_config_path(&args)
        .map(FileReloadSource::new)
        .transpose()?;
    let mut active_cfg = cfg.clone();
    let running = start_running_system(&cfg).await?;
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        tokio::select! {
            result = &mut ctrl_c => {
                result.map_err(|e| {
                    RagloomError::new(RagloomErrorKind::Internal, e)
                        .with_context("failed to install Ctrl-C handler")
                })?;
                running.shutdown("shutdown").await;
                return Ok(());
            }
            _ = tokio::time::sleep(CONFIG_RELOAD_POLL_INTERVAL), if reload_source.is_some() => {
                let Some(source) = reload_source.as_mut() else {
                    continue;
                };
                match source.poll_changed_contents() {
                    Ok(Some(contents)) => {
                        match parse_reload_run_config_from_contents(&args, &contents, &source.config_path()) {
                            Ok(next_cfg) => {
                                match validate_reloadable_changes(&active_cfg, &next_cfg) {
                                    Ok(false) => {
                                        tracing::info!(
                                            event.name = "ragloom.config.reload.noop",
                                            path = %source.config_path().display(),
                                            "ragloom.config.reload.noop"
                                        );
                                    }
                                    Ok(true) => {
                                        let next_policy = retry_policy_from_cfg(&next_cfg)?;
                                        tracing::info!(
                                            event.name = "ragloom.config.reload.applying",
                                            path = %source.config_path().display(),
                                            "ragloom.config.reload.applying"
                                        );
                                        running.retry_policy.replace(next_policy)?;
                                        active_cfg = next_cfg;
                                        tracing::info!(
                                            event.name = "ragloom.config.reload.applied",
                                            path = %source.config_path().display(),
                                            retry_max_attempts = active_cfg.retry_max_attempts,
                                            retry_max_queued = active_cfg.retry_max_queued,
                                            retry_initial_backoff_ms = active_cfg.retry_initial_backoff_ms,
                                            retry_max_backoff_ms = active_cfg.retry_max_backoff_ms,
                                            "ragloom.config.reload.applied"
                                        );
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            event.name = "ragloom.config.reload.rejected",
                                            path = %source.config_path().display(),
                                            error.kind = %err.kind,
                                            error.message = %err,
                                            "ragloom.config.reload.rejected"
                                        );
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    event.name = "ragloom.config.reload.rejected",
                                    path = %source.config_path().display(),
                                    error.kind = %err.kind,
                                    error.message = %err,
                                    "ragloom.config.reload.rejected"
                                );
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(
                            event.name = "ragloom.config.reload.poll_failed",
                            path = %source.config_path().display(),
                            error.kind = %err.kind,
                            error.message = %err,
                            "ragloom.config.reload.poll_failed"
                        );
                    }
                }
            }
        }
    }
}

async fn start_running_system(cfg: &RunConfig) -> Result<RunningSystem, RagloomError> {
    let health_state = HealthState::starting();
    let metrics = IngestionMetrics::default();
    let health_server = match cfg.health_addr.as_deref() {
        Some(addr) => {
            match HealthServer::bind_with_metrics(addr, health_state.clone(), Some(metrics.clone()))
                .await
            {
                Ok(server) => {
                    tracing::info!(
                        event.name = "ragloom.health.started",
                        addr = %addr,
                        "ragloom.health.started"
                    );
                    Some(server)
                }
                Err(err) => {
                    health_state.mark_startup_failed();
                    return Err(err);
                }
            }
        }
        None => None,
    };

    let wal = match ragloom::state::wal::FileWal::open(&cfg.state_path)
        .map_err(|e| e.with_context("failed to initialize persistent WAL"))
    {
        Ok(wal) => std::sync::Arc::new(tokio::sync::Mutex::new(wal)),
        Err(err) => {
            health_state.mark_startup_failed();
            return Err(err);
        }
    };

    let previously_observed_paths = {
        let guard = wal.lock().await;
        let records = guard
            .read_all()
            .map_err(|e| e.with_context("failed to read persistent WAL for source recovery"))?;
        ragloom::state::wal::known_live_document_paths(&records)
    };

    let PreparedStartup {
        embedding,
        sink,
        chunker,
    } = match prepare_startup(cfg).await {
        Ok(startup) => startup,
        Err(err) => {
            health_state.mark_startup_failed();
            return Err(err);
        }
    };

    let (source, loader) = match prepare_source_runtime(&cfg.source, previously_observed_paths) {
        Ok(prepared) => prepared.into_parts(),
        Err(err) => {
            health_state.mark_startup_failed();
            return Err(err);
        }
    };

    let runtime = Runtime::with_shared_wal(source, std::sync::Arc::clone(&wal));
    let summary = IngestionSummary::default();
    let (queue, shutdown) = AsyncRuntime::new(runtime, 128)
        .with_summary(summary.clone())
        .with_metrics(metrics.clone())
        .start();
    let mut shutdown_for_monitor = shutdown.clone();

    let pipeline =
        PipelineExecutor::with_chunker(embedding, std::sync::Arc::new(sink), loader, chunker)
            .with_summary(summary.clone())
            .with_metrics(metrics.clone());

    let executor = AckingExecutor {
        inner: pipeline,
        wal: std::sync::Arc::clone(&wal),
    };
    let retry_policy = RetryPolicy {
        max_attempts: cfg.retry_max_attempts,
        max_queued_retries: cfg.retry_max_queued,
        initial_backoff: Duration::from_millis(cfg.retry_initial_backoff_ms),
        max_backoff: Duration::from_millis(cfg.retry_max_backoff_ms),
    };
    let live_retry_policy = LiveRetryPolicy::new(retry_policy)?;
    let summary_for_worker = summary.clone();
    let metrics_for_worker = metrics.clone();
    let live_retry_policy_for_worker = live_retry_policy.clone();

    let worker = tokio::spawn(async move {
        run_worker_with_live_retry_and_metrics(
            queue,
            executor,
            live_retry_policy_for_worker,
            Some(summary_for_worker),
            Some(metrics_for_worker),
        )
        .await;
    });

    health_state.mark_ready();
    let health_for_monitor = health_state.clone();
    let health_monitor = tokio::spawn(async move {
        if let Some(reason) = shutdown_for_monitor.wait_for_exit().await {
            mark_health_from_runtime_exit(&health_for_monitor, reason);
        }
    });

    Ok(RunningSystem {
        health_state,
        health_server,
        shutdown,
        worker,
        health_monitor,
        summary,
        retry_policy: live_retry_policy,
    })
}

fn mark_health_from_runtime_exit(health: &HealthState, reason: RuntimeExitReason) {
    match reason {
        RuntimeExitReason::StartupFailed => health.mark_startup_failed(),
        RuntimeExitReason::RuntimeFailed => health.mark_runtime_failed(),
    }
}

fn extract_config_path(args: &[String]) -> Option<String> {
    let mut config_path = None;
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        let (flag, inline_value) = match arg.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (arg.as_str(), None),
        };
        if flag == "--config" {
            config_path = next_arg_value(inline_value, &mut iter);
        }
    }
    config_path
}

fn retry_policy_from_cfg(cfg: &RunConfig) -> Result<RetryPolicy, RagloomError> {
    let retry_policy = RetryPolicy {
        max_attempts: cfg.retry_max_attempts,
        max_queued_retries: cfg.retry_max_queued,
        initial_backoff: Duration::from_millis(cfg.retry_initial_backoff_ms),
        max_backoff: Duration::from_millis(cfg.retry_max_backoff_ms),
    };
    retry_policy.validate()?;
    Ok(retry_policy)
}

fn validate_reloadable_changes(
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::sync::mpsc::Sender;
    use tempfile::NamedTempFile;

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
    fn parse_args_returns_error_when_required_flags_missing() {
        let args = vec!["ragloom".to_string()];
        let err = parse_args(&args).expect_err("expected error");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("missing required value"));
    }

    #[test]
    fn extract_config_path_supports_inline_and_separate_forms() {
        let separate = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            "./ragloom.yaml".to_string(),
        ];
        let inline = vec!["ragloom".to_string(), "--config=./ragloom.yaml".to_string()];

        assert_eq!(
            extract_config_path(&separate).as_deref(),
            Some("./ragloom.yaml")
        );
        assert_eq!(
            extract_config_path(&inline).as_deref(),
            Some("./ragloom.yaml")
        );
    }

    #[test]
    fn extract_config_path_uses_last_config_flag() {
        let args = vec![
            "ragloom".to_string(),
            "--config=./first.yaml".to_string(),
            "--config".to_string(),
            "./second.yaml".to_string(),
        ];

        assert_eq!(extract_config_path(&args).as_deref(), Some("./second.yaml"));
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
    fn parse_reload_run_config_from_contents_keeps_cli_pinned_retry_fields() {
        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            "./ragloom.yaml".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--retry-max-attempts".to_string(),
            "9".to_string(),
        ];
        let yaml = r#"
source:
  root: "/tmp/from-config"
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
retry:
  max_attempts: 2
  max_queued: 32
  initial_backoff_ms: 10
  max_backoff_ms: 40
"#;

        let cfg = parse_reload_run_config_from_contents(
            &args,
            yaml,
            std::path::Path::new("./ragloom.yaml"),
        )
        .expect("reload config");

        assert_eq!(cfg.retry_max_attempts, 9);
        assert_eq!(cfg.retry_max_queued, 32);
        assert_eq!(cfg.retry_initial_backoff_ms, 10);
        assert_eq!(cfg.retry_max_backoff_ms, 40);
    }

    #[test]
    fn parse_args_defaults_to_openai_backend_and_requires_api_key() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
        ];

        let err = parse_args(&args).expect_err("expected error");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("missing required flag for openai backend")
        );
    }

    #[test]
    fn parse_args_returns_config_when_all_flags_are_present() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        assert_eq!(
            cfg,
            ParsedCommand::Run(Box::new(RunConfig {
                source: filesystem_source("/tmp/docs"),
                embed_backend: EmbedBackend::Http {
                    url: "http://embed".to_string(),
                    model: "default".to_string(),
                },
                qdrant_url: "http://qdrant".to_string(),
                collection: "docs".to_string(),
                state_path: DEFAULT_STATE_PATH.to_string(),
                health_addr: None,
                create_collection_if_missing: false,
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
            }))
        );
    }

    #[test]
    fn parse_args_defaults_state_path() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.state_path, DEFAULT_STATE_PATH);
    }

    #[test]
    fn parse_args_accepts_state_path_inline_value() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--state-path=.state/ragloom.ndjson".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.state_path, ".state/ragloom.ndjson");
    }

    #[test]
    fn parse_args_disables_health_endpoint_by_default() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.health_addr, None);
    }

    #[test]
    fn parse_args_accepts_health_addr_separate_and_inline_values() {
        for health_flag in [
            vec!["--health-addr".to_string(), "127.0.0.1:0".to_string()],
            vec!["--health-addr=127.0.0.1:0".to_string()],
        ] {
            let mut args = vec![
                "ragloom".to_string(),
                "--dir".to_string(),
                "/tmp/docs".to_string(),
                "--embed-backend".to_string(),
                "http".to_string(),
                "--embed-url".to_string(),
                "http://embed".to_string(),
                "--embed-model".to_string(),
                "default".to_string(),
                "--qdrant-url".to_string(),
                "http://qdrant".to_string(),
                "--collection".to_string(),
                "docs".to_string(),
            ];
            args.extend(health_flag);

            let cfg = parse_args(&args).expect("config");
            let ParsedCommand::Run(cfg) = cfg else {
                panic!("expected run config");
            };
            assert_eq!(cfg.health_addr.as_deref(), Some("127.0.0.1:0"));
        }
    }

    #[test]
    fn parse_args_accepts_explicit_filesystem_source_kind() {
        let args = vec![
            "ragloom".to_string(),
            "--source-kind".to_string(),
            "filesystem".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.source, filesystem_source("/tmp/docs"));
    }

    #[test]
    fn parse_args_rejects_s3_flags_without_s3_source_kind() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--s3-bucket".to_string(),
            "docs-bucket".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
        ];

        let err = parse_args(&args).expect_err("expected config error");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("--s3-bucket"));
    }

    #[test]
    fn parse_args_loads_s3_source_from_yaml_config() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  kind: s3
  bucket: "docs-bucket"
  prefix: "knowledge/"
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(
            cfg.source,
            RunSource::S3 {
                bucket: "docs-bucket".to_string(),
                prefix: Some("knowledge/".to_string()),
            }
        );
    }

    #[test]
    fn parse_args_allows_cli_dir_to_complete_filesystem_source_config() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  kind: filesystem
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--dir".to_string(),
            "/tmp/from-cli".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.source, filesystem_source("/tmp/from-cli"));
    }

    #[test]
    fn parse_args_allows_cli_bucket_to_complete_s3_source_config() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  kind: s3
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--s3-bucket".to_string(),
            "docs-bucket".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(
            cfg.source,
            RunSource::S3 {
                bucket: "docs-bucket".to_string(),
                prefix: None,
            }
        );
    }

    #[test]
    fn parse_args_dir_overrides_configured_s3_source_kind() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  kind: s3
  bucket: "docs-bucket"
  prefix: "knowledge/"
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--dir".to_string(),
            "/tmp/from-cli".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.source, filesystem_source("/tmp/from-cli"));
    }

    #[test]
    fn parse_args_rejects_dir_with_s3_source_kind() {
        let args = vec![
            "ragloom".to_string(),
            "--source-kind".to_string(),
            "s3".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--s3-bucket".to_string(),
            "docs-bucket".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
        ];

        let err = parse_args(&args).expect_err("expected config error");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("--dir"));
    }

    #[test]
    fn parse_args_rejects_invalid_filesystem_source_shape_without_override() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  kind: filesystem
  bucket: "docs-bucket"
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--dir".to_string(),
            "/tmp/from-cli".to_string(),
        ];

        let err = parse_args(&args).expect_err("should fail source validation");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("source.bucket"));
    }

    #[test]
    fn parse_args_returns_version_command_for_long_flag() {
        let args = vec!["ragloom".to_string(), "--version".to_string()];

        let cmd = parse_args(&args).expect("version command");
        assert_eq!(cmd, ParsedCommand::Version);
    }

    #[test]
    fn parse_args_returns_version_command_for_short_flag() {
        let args = vec!["ragloom".to_string(), "-V".to_string()];

        let cmd = parse_args(&args).expect("version command");
        assert_eq!(cmd, ParsedCommand::Version);
    }

    #[test]
    fn parse_args_returns_version_command_before_required_flag_validation() {
        let args = vec![
            "ragloom".to_string(),
            "--version".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
        ];

        let cmd = parse_args(&args).expect("version command");
        assert_eq!(cmd, ParsedCommand::Version);
    }

    #[test]
    fn parse_args_returns_help_command_for_help_flag() {
        let args = vec!["ragloom".to_string(), "--help".to_string()];

        let cmd = parse_args(&args).expect("help command");
        assert_eq!(cmd, ParsedCommand::Help);
    }

    #[test]
    fn parse_args_supports_inline_version_flag_before_required_validation() {
        let args = vec![
            "ragloom".to_string(),
            "--version".to_string(),
            "--qdrant-url=http://qdrant".to_string(),
        ];

        let cmd = parse_args(&args).expect("version command");
        assert_eq!(cmd, ParsedCommand::Version);
    }

    #[test]
    fn parse_args_defaults_bootstrap_flags_to_disabled_and_none() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert!(!cfg.create_collection_if_missing);
        assert_eq!(cfg.collection_vector_size, None);
    }

    #[test]
    fn parse_args_accepts_collection_bootstrap_flags() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--create-collection-if-missing".to_string(),
            "--collection-vector-size".to_string(),
            "768".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert!(cfg.create_collection_if_missing);
        assert_eq!(cfg.collection_vector_size, Some(768));
    }

    #[test]
    fn parse_args_rejects_inline_value_for_create_collection_if_missing() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--create-collection-if-missing=false".to_string(),
        ];

        let err = parse_args(&args).expect_err("expected invalid boolean flag usage");
        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("--create-collection-if-missing does not accept a value")
        );
    }

    #[test]
    fn parse_args_rejects_inline_value_for_enable_semantic() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--enable-semantic=false".to_string(),
        ];

        let err = parse_args(&args).expect_err("expected invalid boolean flag usage");
        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("--enable-semantic does not accept a value")
        );
    }

    #[test]
    fn parse_args_rejects_positional_value_for_enable_semantic() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--enable-semantic".to_string(),
            "false".to_string(),
        ];

        let err = parse_args(&args).expect_err("expected invalid boolean flag usage");
        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("--enable-semantic does not accept a value")
        );
    }

    #[test]
    fn parse_args_accepts_collection_vector_size_inline_value() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--collection-vector-size=768".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.collection_vector_size, Some(768));
    }

    #[test]
    fn parse_args_accepts_retry_policy_flags() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--retry-max-attempts=4".to_string(),
            "--retry-max-queued".to_string(),
            "16".to_string(),
            "--retry-initial-backoff-ms".to_string(),
            "25".to_string(),
            "--retry-max-backoff-ms=100".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.retry_max_attempts, 4);
        assert_eq!(cfg.retry_max_queued, 16);
        assert_eq!(cfg.retry_initial_backoff_ms, 25);
        assert_eq!(cfg.retry_max_backoff_ms, 100);
    }

    #[test]
    fn parse_args_rejects_invalid_retry_policy() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--retry-max-attempts".to_string(),
            "0".to_string(),
        ];

        let err = parse_args(&args).expect_err("expected invalid retry policy");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("retry.max_attempts"));
    }

    #[test]
    fn parse_args_rejects_missing_collection_vector_size_value() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--collection-vector-size".to_string(),
        ];

        let err = parse_args(&args).expect_err("expected missing vector size value");
        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("missing required value: --collection-vector-size")
        );
    }

    #[test]
    fn parse_args_rejects_invalid_collection_vector_size() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--collection-vector-size".to_string(),
            "0".to_string(),
        ];

        let err = parse_args(&args).expect_err("expected invalid vector size");
        assert_eq!(err.kind, RagloomErrorKind::InvalidInput);
        assert!(
            err.to_string()
                .contains("--collection-vector-size must be positive")
        );
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

        let err = match prepare_startup(&cfg).await {
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
    fn enable_semantic_errors_in_single_mode_without_semantic() {
        let args = vec![
            "ragloom".to_string(),
            "--dir".to_string(),
            "/tmp/docs".to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--embed-url".to_string(),
            "http://embed".to_string(),
            "--embed-model".to_string(),
            "default".to_string(),
            "--qdrant-url".to_string(),
            "http://qdrant".to_string(),
            "--collection".to_string(),
            "docs".to_string(),
            "--chunker-mode".to_string(),
            "single".to_string(),
            "--chunker-single".to_string(),
            "recursive".to_string(),
            "--enable-semantic".to_string(),
        ];
        let err = parse_args(&args).expect_err("must reject");
        assert!(err.to_string().contains("--enable-semantic"));
    }

    #[test]
    fn parse_args_loads_required_values_from_yaml_config() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  root: "/tmp/from-config"
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
state:
  path: ".state/from-config.ndjson"
retry:
  max_attempts: 5
  max_queued: 32
  initial_backoff_ms: 10
  max_backoff_ms: 80
health:
  addr: "127.0.0.1:9000"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.source, filesystem_source("/tmp/from-config"));
        assert_eq!(cfg.qdrant_url, "http://qdrant-from-config");
        assert_eq!(cfg.collection, "from-config");
        assert_eq!(cfg.state_path, ".state/from-config.ndjson");
        assert_eq!(cfg.retry_max_attempts, 5);
        assert_eq!(cfg.retry_max_queued, 32);
        assert_eq!(cfg.retry_initial_backoff_ms, 10);
        assert_eq!(cfg.retry_max_backoff_ms, 80);
        assert_eq!(cfg.health_addr.as_deref(), Some("127.0.0.1:9000"));
        assert_eq!(
            cfg.embed_backend,
            EmbedBackend::Http {
                url: "http://embed-from-config".to_string(),
                model: "default".to_string(),
            }
        );
    }

    #[test]
    fn parse_args_health_addr_cli_overrides_yaml_config() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  root: "/tmp/from-config"
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
health:
  addr: "127.0.0.1:9000"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
            "--health-addr".to_string(),
            "127.0.0.1:9001".to_string(),
        ];

        let cfg = parse_args(&args).expect("config");
        let ParsedCommand::Run(cfg) = cfg else {
            panic!("expected run config");
        };
        assert_eq!(cfg.health_addr.as_deref(), Some("127.0.0.1:9001"));
    }

    #[test]
    fn parse_args_rejects_empty_yaml_health_addr() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  root: "/tmp/from-config"
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
health:
  addr: ""
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let err = parse_args(&args).expect_err("should fail validation");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("--health-addr or health.addr is empty")
        );
    }

    #[test]
    fn parse_args_rejects_empty_yaml_embed_endpoint() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  root: "/tmp/from-config"
embed:
  endpoint: ""
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: "from-config"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let err = parse_args(&args).expect_err("should fail validation");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("--embed-url or embed.endpoint is empty")
        );
    }

    #[test]
    fn parse_args_rejects_empty_yaml_qdrant_url() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  root: "/tmp/from-config"
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: ""
  collection: "from-config"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let err = parse_args(&args).expect_err("should fail validation");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("--qdrant-url or sink.qdrant_url is empty")
        );
    }

    #[test]
    fn parse_args_rejects_empty_yaml_collection() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  root: "/tmp/from-config"
embed:
  endpoint: "http://embed-from-config"
sink:
  qdrant_url: "http://qdrant-from-config"
  collection: ""
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let err = parse_args(&args).expect_err("should fail validation");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(
            err.to_string()
                .contains("--collection or sink.collection is empty")
        );
    }

    #[test]
    fn runtime_exit_reason_updates_health_readiness() {
        let health = HealthState::starting();
        health.mark_ready();

        mark_health_from_runtime_exit(&health, RuntimeExitReason::RuntimeFailed);

        assert_eq!(
            health.status(),
            ragloom::observability::health::HealthStatus::NotReady
        );
        assert_eq!(
            health.reason(),
            Some(ragloom::observability::health::HealthFailureReason::RuntimeFailed)
        );

        let startup_health = HealthState::starting();
        mark_health_from_runtime_exit(&startup_health, RuntimeExitReason::StartupFailed);
        assert_eq!(
            startup_health.reason(),
            Some(ragloom::observability::health::HealthFailureReason::StartupFailed)
        );
    }

    #[test]
    fn parse_args_surfaces_yaml_validation_context() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  root: ""
embed:
  endpoint: "http://embed"
sink:
  qdrant_url: "http://qdrant"
  collection: "docs"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let err = parse_args(&args).expect_err("should fail validation");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("--dir or source.root is empty"));
    }

    #[test]
    fn parse_args_surfaces_yaml_parse_context() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(
            br#"
source:
  root: "/tmp/docs"
embed:
  endpoint "missing-colon"
sink:
  qdrant_url: "http://qdrant"
  collection: "docs"
"#,
        )
        .expect("write config");

        let args = vec![
            "ragloom".to_string(),
            "--config".to_string(),
            file.path().to_string_lossy().to_string(),
            "--embed-backend".to_string(),
            "http".to_string(),
        ];

        let err = parse_args(&args).expect_err("should fail parse");
        assert_eq!(err.kind, RagloomErrorKind::Config);
        assert!(err.to_string().contains("failed to parse config file"));
    }
}

//! Proves the default Router dispatches to the correct chunker based on file
//! extension and that the selected strategy is reflected in the point
//! `strategy_fingerprint` payload.

use std::sync::Arc;

use async_trait::async_trait;

use ragloom::RagloomError;
use ragloom::doc::DocumentLoader;
use ragloom::embed::EmbeddingProvider;
use ragloom::ids::FileFingerprint;
use ragloom::sink::{Sink, VectorPoint};
use ragloom::transform::chunker::{
    Chunker, default_router, recursive::RecursiveConfig, size::SizeMetric,
};

#[derive(Default)]
struct FakeEmbedding;
#[async_trait]
impl EmbeddingProvider for FakeEmbedding {
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, RagloomError> {
        Ok(inputs.iter().map(|_| vec![0.0_f32; 4]).collect())
    }
}

#[derive(Default)]
struct FakeSink;
#[async_trait]
impl Sink for FakeSink {
    async fn upsert_points(&self, _points: Vec<VectorPoint>) -> Result<(), RagloomError> {
        Ok(())
    }
}

#[derive(Default)]
struct FakeLoader;
#[async_trait]
impl DocumentLoader for FakeLoader {
    async fn load_utf8(&self, _path: &str) -> Result<String, RagloomError> {
        Ok(String::new())
    }
}

fn cfg() -> RecursiveConfig {
    RecursiveConfig {
        metric: SizeMetric::Chars,
        max_size: 512,
        min_size: 0,
        overlap: 0,
    }
}

fn router() -> Arc<dyn Chunker> {
    Arc::new(default_router(cfg()).expect("router"))
}

fn exec() -> ragloom::pipeline::runtime::PipelineExecutor {
    ragloom::pipeline::runtime::PipelineExecutor::with_chunker(
        Arc::new(FakeEmbedding),
        Arc::new(FakeSink),
        Arc::new(FakeLoader),
        router(),
    )
}

#[tokio::test]
async fn txt_gets_recursive_fingerprint() {
    let fp = FileFingerprint {
        canonical_path: "/tmp/notes.txt".into(),
        size_bytes: 1,
        mtime_unix_secs: 1,
    };
    let points = exec()
        .build_points_from_text(&fp, "plain text content here\n")
        .await
        .unwrap();
    let strategy = points[0].payload["strategy_fingerprint"].as_str().unwrap();
    assert!(strategy.starts_with("recursive:v1"), "got {}", strategy);
}

#[tokio::test]
async fn md_gets_markdown_fingerprint() {
    let fp = FileFingerprint {
        canonical_path: "/tmp/notes.md".into(),
        size_bytes: 1,
        mtime_unix_secs: 1,
    };
    let points = exec()
        .build_points_from_text(&fp, "# Title\n\nSome body.\n")
        .await
        .unwrap();
    let strategy = points[0].payload["strategy_fingerprint"].as_str().unwrap();
    assert!(strategy.starts_with("markdown:v1"), "got {}", strategy);
}

#[tokio::test]
async fn rs_gets_code_rust_fingerprint() {
    let fp = FileFingerprint {
        canonical_path: "/tmp/x.rs".into(),
        size_bytes: 1,
        mtime_unix_secs: 1,
    };
    let points = exec()
        .build_points_from_text(&fp, "fn a(){}\nfn b(){}\n")
        .await
        .unwrap();
    let strategy = points[0].payload["strategy_fingerprint"].as_str().unwrap();
    assert!(strategy.contains("lang=rust"), "got {}", strategy);
}

//! md and rs paths through the Router produce disjoint point-ID sets.

use std::collections::HashSet;
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
        Ok(inputs.iter().map(|_| vec![0.0; 4]).collect())
    }
}
#[derive(Default)]
struct FakeSink;
#[async_trait]
impl Sink for FakeSink {
    async fn upsert_points(&self, _: Vec<VectorPoint>) -> Result<(), RagloomError> {
        Ok(())
    }
}
#[derive(Default)]
struct FakeLoader;
#[async_trait]
impl DocumentLoader for FakeLoader {
    async fn load_utf8(&self, _: &str) -> Result<String, RagloomError> {
        Ok(String::new())
    }
}

fn exec(router: Arc<dyn Chunker>) -> ragloom::pipeline::runtime::PipelineExecutor {
    ragloom::pipeline::runtime::PipelineExecutor::with_chunker(
        Arc::new(FakeEmbedding),
        Arc::new(FakeSink),
        Arc::new(FakeLoader),
        router,
    )
}

#[tokio::test]
async fn md_and_rs_produce_disjoint_point_ids_from_same_text() {
    let rec_cfg = RecursiveConfig {
        metric: SizeMetric::Chars,
        max_size: 32,
        min_size: 0,
        overlap: 0,
    };
    let router: Arc<dyn Chunker> = Arc::new(default_router(rec_cfg).unwrap());

    let text = "# hello\n\nworld of content\n\n## section\n\nmore\n";

    let md_fp = FileFingerprint {
        canonical_path: "/tmp/n.md".into(),
        size_bytes: 1,
        mtime_unix_secs: 1,
    };
    let rs_fp = FileFingerprint {
        canonical_path: "/tmp/n.rs".into(),
        size_bytes: 1,
        mtime_unix_secs: 1,
    };

    let md_pts = exec(Arc::clone(&router))
        .build_points_from_text(&md_fp, text)
        .await
        .unwrap();
    let rs_pts = exec(router)
        .build_points_from_text(&rs_fp, text)
        .await
        .unwrap();

    let md_ids: HashSet<String> = md_pts.iter().map(|p| format!("{:?}", p.id)).collect();
    let rs_ids: HashSet<String> = rs_pts.iter().map(|p| format!("{:?}", p.id)).collect();

    assert!(!md_ids.is_empty() && !rs_ids.is_empty());
    assert!(
        md_ids.is_disjoint(&rs_ids),
        "expected disjoint: md={:?} rs={:?}",
        md_ids,
        rs_ids
    );
}

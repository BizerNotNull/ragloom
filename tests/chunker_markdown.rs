//! Markdown chunker integration — verifies fingerprint and boundary behaviour
//! on a realistic sample.

use ragloom::transform::chunker::{
    ChunkHint, Chunker, MarkdownChunker, recursive::RecursiveConfig, size::SizeMetric,
};

#[test]
fn sample_markdown_produces_markdown_fingerprint() {
    let text =
        std::fs::read_to_string("tests/fixtures/markdown/sample.md").expect("fixture readable");
    let c = MarkdownChunker::new(RecursiveConfig {
        metric: SizeMetric::Chars,
        max_size: 200,
        min_size: 0,
        overlap: 0,
    })
    .unwrap();
    let doc = c.chunk(&text, &ChunkHint::none()).unwrap();
    assert!(!doc.chunks.is_empty());
    assert!(doc.strategy_fingerprint.as_str().starts_with("markdown:v1"));
    assert!(doc.chunks.len() >= 3, "got {} chunks", doc.chunks.len());
}

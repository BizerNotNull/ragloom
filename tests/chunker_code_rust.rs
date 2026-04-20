use ragloom::transform::chunker::{
    code::Language, recursive::RecursiveConfig, size::SizeMetric,
    ChunkHint, Chunker, CodeChunker,
};

#[test]
fn rust_fixture_splits_into_declarations() {
    let text = std::fs::read_to_string("tests/fixtures/code/hello.rs").unwrap();
    let c = CodeChunker::new(Language::Rust, RecursiveConfig {
        metric: SizeMetric::Chars,
        max_size: 1000, min_size: 0, overlap: 0,
    }).unwrap();
    let doc = c.chunk(&text, &ChunkHint::none()).unwrap();
    assert!(doc.chunks.len() >= 2);
    assert!(doc.strategy_fingerprint.as_str().contains("lang=rust"));
}

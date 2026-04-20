use ragloom::transform::chunker::{
    ChunkHint, Chunker, CodeChunker, code::Language, recursive::RecursiveConfig, size::SizeMetric,
};

#[test]
fn python_fixture_splits_into_declarations() {
    let text = std::fs::read_to_string("tests/fixtures/code/hello.py").unwrap();
    let c = CodeChunker::new(
        Language::Python,
        RecursiveConfig {
            metric: SizeMetric::Chars,
            max_size: 1000,
            min_size: 0,
            overlap: 0,
        },
    )
    .unwrap();
    let doc = c.chunk(&text, &ChunkHint::none()).unwrap();
    assert!(doc.chunks.len() >= 2);
    assert!(doc.strategy_fingerprint.as_str().contains("lang=python"));
}

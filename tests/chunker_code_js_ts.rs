use ragloom::transform::chunker::{
    ChunkHint, Chunker, CodeChunker, code::Language, recursive::RecursiveConfig, size::SizeMetric,
};

fn cfg() -> RecursiveConfig {
    RecursiveConfig {
        metric: SizeMetric::Chars,
        max_size: 1000,
        min_size: 0,
        overlap: 0,
    }
}

#[test]
fn js_fixture_splits_into_declarations() {
    let text = std::fs::read_to_string("tests/fixtures/code/hello.js").unwrap();
    let c = CodeChunker::new(Language::JavaScript, cfg()).unwrap();
    let doc = c.chunk(&text, &ChunkHint::none()).unwrap();
    assert!(doc.chunks.len() >= 2);
    assert!(
        doc.strategy_fingerprint
            .as_str()
            .contains("lang=javascript")
    );
}

#[test]
fn ts_fixture_splits_into_declarations() {
    let text = std::fs::read_to_string("tests/fixtures/code/hello.ts").unwrap();
    let c = CodeChunker::new(Language::TypeScript, cfg()).unwrap();
    let doc = c.chunk(&text, &ChunkHint::none()).unwrap();
    assert!(doc.chunks.len() >= 2);
    assert!(
        doc.strategy_fingerprint
            .as_str()
            .contains("lang=typescript")
    );
}

#[test]
fn tsx_fixture_splits_into_declarations() {
    let text = std::fs::read_to_string("tests/fixtures/code/hello.tsx").unwrap();
    let c = CodeChunker::new(Language::Tsx, cfg()).unwrap();
    let doc = c.chunk(&text, &ChunkHint::none()).unwrap();
    assert!(doc.chunks.len() >= 2);
    assert!(doc.strategy_fingerprint.as_str().contains("lang=tsx"));
}

use ragloom::transform::chunker::{
    code::Language, recursive::RecursiveConfig, size::SizeMetric,
    ChunkHint, Chunker, CodeChunker,
};

fn cfg() -> RecursiveConfig {
    RecursiveConfig { metric: SizeMetric::Chars, max_size: 1000, min_size: 0, overlap: 0 }
}

fn check(lang: Language, path: &str, expected_lang: &str) {
    let text = std::fs::read_to_string(path).unwrap();
    let c = CodeChunker::new(lang, cfg()).unwrap();
    let doc = c.chunk(&text, &ChunkHint::none()).unwrap();
    assert!(
        doc.chunks.len() >= 2,
        "expected >=2 chunks for {}, got {}",
        path,
        doc.chunks.len()
    );
    assert!(
        doc.strategy_fingerprint.as_str().contains(expected_lang),
        "fingerprint missing lang marker: {}",
        doc.strategy_fingerprint.as_str()
    );
}

#[test] fn go_fixture()    { check(Language::Go,    "tests/fixtures/code/hello.go", "lang=go"); }
#[test] fn java_fixture()  { check(Language::Java,  "tests/fixtures/code/Hello.java", "lang=java"); }
#[test] fn c_fixture()     { check(Language::C,     "tests/fixtures/code/hello.c", "lang=c"); }
#[test] fn cpp_fixture()   { check(Language::Cpp,   "tests/fixtures/code/hello.cpp", "lang=cpp"); }
#[test] fn ruby_fixture()  { check(Language::Ruby,  "tests/fixtures/code/hello.rb", "lang=ruby"); }
#[test] fn bash_fixture()  { check(Language::Bash,  "tests/fixtures/code/hello.sh", "lang=bash"); }

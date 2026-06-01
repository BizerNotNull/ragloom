use std::fs;
use std::path::Path;

fn read_repo_file(path: &str) -> String {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    fs::read_to_string(repo_root.join(path)).expect("read repository file")
}

#[test]
fn readme_describes_v0_4_sources_formats_and_s3_example() {
    let readme = read_repo_file("README.md");

    assert!(
        readme.contains("v0.4"),
        "expected README to frame the current support story around v0.4"
    );
    assert!(
        readme.contains("local filesystem source")
            && readme.contains("kind: \"filesystem\"")
            && readme.contains("root: \"./docs\"")
            && readme.contains("source.kind: s3"),
        "expected README to document both filesystem and S3 source shapes with concrete examples"
    );
    assert!(
        readme.contains("PDF extraction is text-only")
            && readme.contains("DOCX extraction is also text-only"),
        "expected README to keep the PDF and DOCX loader limits explicit"
    );
    assert!(
        readme.contains("bucket: \"docs-bucket\"") && readme.contains("prefix: \"kb/\""),
        "expected README to include a concrete S3 configuration example"
    );
}

#[test]
fn support_policy_describes_current_v0_4_support_boundary() {
    let support = read_repo_file("SUPPORT.md");

    assert!(
        support.contains("v0.4"),
        "expected SUPPORT.md to describe the current support boundary in v0.4 terms"
    );
    assert!(
        support.contains("local filesystem ingestion under one configured directory, plus polling S3 ingestion under one configured bucket/prefix"),
        "expected SUPPORT.md to describe both supported source shapes"
    );
    assert!(
        support.contains("deterministic PDF text loading")
            && support.contains("deterministic DOCX text extraction"),
        "expected SUPPORT.md to keep PDF and DOCX support boundaries explicit"
    );
    assert!(
        support.contains("Current PDF support is limited to embedded text extraction")
            && support.contains("Current DOCX support is limited to deterministic extracted text"),
        "expected SUPPORT.md to preserve the parser limitation details"
    );
}

#[test]
fn changelog_records_v0_4_release_alignment() {
    let changelog = read_repo_file("CHANGELOG.md");

    assert!(
        changelog.contains("## [0.4.0] - 2026-05-26")
            && changelog.contains("### Docs")
            && changelog.contains("v0.4")
            && changelog.contains("support matrix"),
        "expected the changelog to record the v0.4 release docs alignment work"
    );
}

#[test]
fn semantic_support_boundary_is_consistent_across_docs() {
    let readme = read_repo_file("README.md");
    let support = read_repo_file("SUPPORT.md");
    let changelog = read_repo_file("CHANGELOG.md");

    let support_phrase = "semantic chunking remains experimental and opt-in";
    let fastembed_phrase = "`fastembed` remains a feature-gated semantic provider";

    assert!(
        readme.contains(support_phrase) && readme.contains(fastembed_phrase),
        "expected README to describe the experimental semantic support boundary"
    );
    assert!(
        support.contains(support_phrase) && support.contains(fastembed_phrase),
        "expected SUPPORT.md to describe the experimental semantic support boundary"
    );
    assert!(
        changelog.contains(support_phrase) && changelog.contains(fastembed_phrase),
        "expected CHANGELOG.md to record the semantic support-boundary decision"
    );
}

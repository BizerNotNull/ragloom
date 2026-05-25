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
fn changelog_unreleased_mentions_v0_4_docs_alignment() {
    let changelog = read_repo_file("CHANGELOG.md");

    assert!(
        changelog.contains("## [Unreleased]")
            && changelog.contains("### Docs")
            && changelog.contains("v0.4")
            && changelog.contains("support matrix"),
        "expected the unreleased changelog to record the v0.4 docs alignment work"
    );
}

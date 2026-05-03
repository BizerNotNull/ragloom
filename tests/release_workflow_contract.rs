use std::fs;
use std::path::Path;

fn read_repo_file(path: &str) -> String {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    fs::read_to_string(repo_root.join(path)).expect("read repository file")
}

#[test]
fn release_workflow_supports_version_dispatch_and_release_notes() {
    let workflow_yaml = read_repo_file(".github/workflows/release.yml");
    let workflow: serde_yaml::Value =
        serde_yaml::from_str(&workflow_yaml).expect("release workflow is valid YAML");

    let on = workflow
        .get("on")
        .and_then(serde_yaml::Value::as_mapping)
        .expect("expected release workflow to define an `on` mapping");

    let workflow_dispatch = on
        .get(serde_yaml::Value::String("workflow_dispatch".to_string()))
        .and_then(serde_yaml::Value::as_mapping)
        .expect("expected release workflow to support manual `workflow_dispatch`");

    let inputs = workflow_dispatch
        .get(serde_yaml::Value::String("inputs".to_string()))
        .and_then(serde_yaml::Value::as_mapping)
        .expect("expected `workflow_dispatch` to define `inputs`");

    let version_input = inputs
        .get(serde_yaml::Value::String("version".to_string()))
        .and_then(serde_yaml::Value::as_mapping)
        .expect("expected `workflow_dispatch.inputs` to define a `version` input");

    assert!(
        version_input
            .get(serde_yaml::Value::String("required".to_string()))
            .and_then(serde_yaml::Value::as_bool)
            .unwrap_or(false),
        "expected `workflow_dispatch.inputs.version.required` to be true"
    );
    assert!(
        matches!(
            version_input.get(serde_yaml::Value::String("type".to_string())),
            Some(serde_yaml::Value::String(kind)) if kind == "string"
        ),
        "expected `workflow_dispatch.inputs.version.type` to be `string`"
    );
    assert!(
        workflow_yaml.contains("generate_release_notes: true"),
        "expected release workflow to generate release notes deterministically"
    );
}

#[test]
fn release_workflows_verify_tag_and_crate_version_consistency_and_pin_python() {
    let release_workflow = read_repo_file(".github/workflows/release.yml");
    let publish_workflow = read_repo_file(".github/workflows/publish-crate.yml");
    let quality_workflow = read_repo_file(".github/workflows/quality-deep.yml");
    let codeql_workflow = read_repo_file(".github/workflows/codeql.yml");

    assert!(
        release_workflow.contains("verify-release-version"),
        "expected release workflow to verify crate and tag versions before publishing"
    );
    assert!(
        publish_workflow.contains("verify-release-version"),
        "expected publish workflow to verify crate and tag versions before cargo publish"
    );
    assert!(
        release_workflow.contains("actions/setup-python@v5"),
        "expected release workflow to pin Python for the verification script"
    );
    assert!(
        release_workflow.contains("python-version: \"3.11\""),
        "expected release workflow to require Python 3.11 for tomllib"
    );
    assert!(
        publish_workflow.contains("actions/setup-python@v5"),
        "expected publish workflow to pin Python for the verification script"
    );
    assert!(
        publish_workflow.contains("python-version: \"3.11\""),
        "expected publish workflow to require Python 3.11 for tomllib"
    );
    assert!(
        release_workflow.contains("security-events: write"),
        "expected release workflow permissions to include security-events: write for reusable workflow calls"
    );
    assert!(
        !quality_workflow.contains("cargo +nightly miri"),
        "expected deep quality workflow to keep Miri out of the release-critical CI path"
    );
    assert!(
        codeql_workflow.contains("github/codeql-action/init@v4"),
        "expected repository code scanning to run through CodeQL"
    );
    assert!(
        codeql_workflow.contains("languages: rust"),
        "expected CodeQL workflow to analyze Rust code"
    );
    assert!(
        codeql_workflow.contains("languages: actions"),
        "expected CodeQL workflow to analyze GitHub Actions workflows"
    );
}

#[test]
fn release_workflow_keeps_macos_best_effort_and_uses_portable_checksums() {
    let workflow_yaml = read_repo_file(".github/workflows/release.yml");
    let workflow: serde_yaml::Value =
        serde_yaml::from_str(&workflow_yaml).expect("release workflow is valid YAML");

    let jobs = workflow
        .get("jobs")
        .and_then(serde_yaml::Value::as_mapping)
        .expect("expected release workflow to define jobs");

    let release_binaries = jobs
        .get(serde_yaml::Value::String("release-binaries".to_string()))
        .and_then(serde_yaml::Value::as_mapping)
        .expect("expected release workflow to define supported release binaries");
    let best_effort = jobs
        .get(serde_yaml::Value::String(
            "release-binaries-best-effort".to_string(),
        ))
        .and_then(serde_yaml::Value::as_mapping)
        .expect("expected release workflow to define best-effort macOS binaries");
    let publish_release = jobs
        .get(serde_yaml::Value::String("publish-release".to_string()))
        .and_then(serde_yaml::Value::as_mapping)
        .expect("expected release workflow to define publish-release");

    let supported_job_yaml =
        serde_yaml::to_string(release_binaries).expect("serialize supported release job");
    let best_effort_yaml =
        serde_yaml::to_string(best_effort).expect("serialize best-effort release job");
    let publish_release_yaml =
        serde_yaml::to_string(publish_release).expect("serialize publish-release job");

    assert!(
        workflow_yaml.contains("command -v sha256sum")
            && workflow_yaml.contains("shasum -a 256"),
        "expected release workflow to use a portable checksum command across Linux and macOS"
    );
    assert!(
        supported_job_yaml.contains("x86_64-unknown-linux-gnu")
            && supported_job_yaml.contains("aarch64-unknown-linux-gnu")
            && supported_job_yaml.contains("x86_64-pc-windows-msvc"),
        "expected supported release job to gate Linux and Windows artifacts"
    );
    assert!(
        !supported_job_yaml.contains("apple-darwin"),
        "expected supported release job to exclude macOS targets"
    );
    assert!(
        best_effort_yaml.contains("x86_64-apple-darwin")
            && best_effort_yaml.contains("aarch64-apple-darwin")
            && best_effort_yaml.contains("continue-on-error: true"),
        "expected macOS artifacts to remain best-effort and non-blocking"
    );
    assert!(
        !publish_release_yaml.contains("release-binaries-best-effort"),
        "expected publish-release to depend only on release-blocking targets"
    );
}

#[test]
fn support_docs_describe_release_dispatch_runbook() {
    let support = read_repo_file("SUPPORT.md");

    assert!(
        support.contains("workflow_dispatch"),
        "expected support docs to describe the manual release workflow entrypoint"
    );
    assert!(
        support.contains("Cargo.toml"),
        "expected support docs to document crate-version verification"
    );
    assert!(
        support.contains("Best-effort") || support.contains("best-effort"),
        "expected support docs to describe macOS release artifacts as best-effort"
    );
}

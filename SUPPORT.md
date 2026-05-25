# Support Policy

## Supported Platforms

Officially supported release targets:

- Linux x86_64
- Linux aarch64
- Windows x86_64

Best-effort release artifacts:

- macOS x86_64
- macOS aarch64

## Release Channels

Ragloom publishes artifacts through:

- GitHub Release binaries
- crates.io crate releases
- GHCR container images for Linux targets only

## Release Runbook

Maintainers should start releases from `.github/workflows/release.yml` using
`workflow_dispatch` with an explicit crate version from `Cargo.toml`
(for example `0.3.0`).

The release workflow verifies that:

- the requested version matches `Cargo.toml` `package.version`
- any pushed tag matches the same crate version
- the crate publish workflow re-checks that version/tag pair before `cargo publish`
- Linux and Windows release targets complete before GitHub Release and crates.io publication continue

Manual `push` of a `v*` tag is still supported, but it goes through the same
`Cargo.toml` consistency guard before release artifacts or crates.io publish
steps run.

GitHub Release notes are generated automatically by the release workflow so the
published notes come from the repository event history rather than ad hoc local
release text.

GitHub Release archives are published with explicit target names such as
`ragloom-v0.3.0-x86_64-unknown-linux-gnu.tar.gz` and
`ragloom-v0.3.0-x86_64-pc-windows-msvc.zip`.

Each published archive also includes a matching `.sha256.txt` checksum file and
`.spdx.json` SBOM. Release checksums are generated with a platform-aware
command so Linux targets use `sha256sum` while macOS targets use `shasum -a 256`.

## v0.4 Support Readiness

For the current `v0.4` support surface, maintainers should treat the following
as release-blocking for the commit being released:

- `ci` is green for the release commit or release branch tip
- `quality-deep` is green for the same commit, or maintainers have run the equivalent local checks from `cargo maintainer-qa`
- the release workflow version and tag consistency guards pass
- supported Linux and Windows artifacts build successfully before GitHub Release and crates.io publication continue
- any release-tracked security or dependency finding has been resolved or explicitly deferred with a documented rationale in the release notes or tracking issue

The following are not release-blocking by default:

- macOS artifact failures, unless maintainers explicitly promote macOS to a supported release target
- experimental semantic chunking behavior, as long as the default non-semantic ingest path remains healthy

## Support Scope

The project treats Linux and Windows release targets as the formal support boundary for CI, release verification, and issue triage priority.

macOS binaries are provided as convenience artifacts. They should compile and publish when practical, but breakage on macOS does not block release unless maintainers explicitly promote it to a supported target.

When a macOS build succeeds, its archive is appended to the existing GitHub
Release after the supported Linux and Windows assets have already published.

## Feature Boundaries

Core support boundary maintainers are hardening for the current `v0.4`
support boundary:

- local filesystem ingestion under one configured directory, plus polling S3 ingestion under one configured bucket/prefix
- UTF-8 text, Markdown, source code, deterministic PDF text loading, and deterministic DOCX text extraction
- recursive, Markdown-aware, and code-aware chunking
- OpenAI and generic HTTP embedding backends
- Qdrant sink behavior, deterministic point IDs, local WAL replay, bounded retry, and the loopback-only health and metrics endpoint

Feature-gated paths:

- `fastembed` support is opt-in at build time and must keep passing its dedicated checks, but it is not the default shipped runtime path

Experimental or best-effort paths:

- semantic chunking remains experimental and opt-in even when the required provider path is available
- macOS release artifacts remain convenience builds rather than part of the formal support contract

Out of scope for the current support contract:

- broader parser guarantees beyond built-in DOCX text extraction
- non-local operator surfaces
- collection lifecycle management beyond optional first-run bootstrap of the configured target collection

Current PDF support is limited to embedded text extraction. OCR, rich layout
reconstruction, and guarantees for image-only, scan-only, encrypted, or
otherwise unsupported PDFs remain outside the current support contract.

Current DOCX support is limited to deterministic extracted text. Ragloom
linearizes paragraphs and tables into chunkable text, but does not preserve
rich formatting, embedded assets, or full page layout fidelity.

## Getting Help

### Check the documentation first

- Review `README.md` for quickstart and configuration
- Review `CONTRIBUTING.md` for development guidelines
- Review `AGENTS.md` for AI coding agent guidance

### Open an issue

When opening an issue, please include:

- Ragloom version (`ragloom --version` or `ragloom -V`; source checkouts may also check `Cargo.toml`)
- Rust version (`rustc --version`)
- Operating system and architecture
- whether the missing files are nested under subdirectories or behind symbolic links
- Qdrant version (if applicable)
- Embedding backend and model used
- Steps to reproduce the issue
- Relevant log output (without API keys or secrets)

### Community resources

- GitHub Issues: https://github.com/ragloom/ragloom/issues
- GitHub Discussions: https://github.com/ragloom/ragloom/discussions

### Security issues

For security vulnerabilities, please follow `SECURITY.md` and do **not** open a public issue.

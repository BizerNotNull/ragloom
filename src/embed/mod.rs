//! Embedding providers.
//!
//! # Why
//! Embedding is an external dependency boundary. We keep it behind a trait so
//! the pipeline can remain open to new providers (HTTP API, local inference)
//! without changing call sites.

pub mod http_client;
pub mod openai_client;

use async_trait::async_trait;
use reqwest::Response;

use crate::error::RagloomError;

const MAX_UPSTREAM_ERROR_BODY_CHARS: usize = 256;
const MAX_UPSTREAM_ERROR_BODY_BYTES: usize = 1024;
const TRUNCATION_SUFFIX: &str = "...(truncated)";

/// Produces embedding vectors for input texts.
///
/// # Why
/// The pipeline needs a stable interface for embedding so execution and retry
/// policy can be tested independently of any specific vendor API.
#[async_trait]
pub trait EmbeddingProvider {
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, RagloomError>;
}

async fn read_error_body(mut response: Response) -> String {
    let mut body = Vec::new();
    let mut truncated = false;

    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(_) => {
                if body.is_empty() {
                    return "<failed to read body>".to_string();
                }
                truncated = true;
                break;
            }
        };

        let remaining = MAX_UPSTREAM_ERROR_BODY_BYTES.saturating_sub(body.len());
        if remaining == 0 {
            truncated = true;
            break;
        }

        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }

        body.extend_from_slice(&chunk);
    }

    let body = String::from_utf8_lossy(&body);
    format_error_body(&body, truncated)
}

fn format_error_body(body: &str, truncated: bool) -> String {
    let normalized = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return "<empty body>".to_string();
    }

    let mut bounded = normalized.chars();
    let preview: String = bounded
        .by_ref()
        .take(MAX_UPSTREAM_ERROR_BODY_CHARS)
        .collect();
    if truncated || bounded.next().is_some() {
        format!("{preview}{TRUNCATION_SUFFIX}")
    } else {
        preview
    }
}

#[cfg(test)]
mod tests {
    use super::format_error_body;

    #[test]
    fn upstream_error_body_is_normalized_and_bounded() {
        let body = format!(
            "  error:\n{}\n  ",
            "x".repeat(super::MAX_UPSTREAM_ERROR_BODY_CHARS + 32)
        );

        let formatted = format_error_body(&body, false);

        assert!(formatted.starts_with("error:"));
        assert!(formatted.ends_with(super::TRUNCATION_SUFFIX));
        assert!(!formatted.contains('\n'));
    }

    #[test]
    fn empty_upstream_error_body_has_placeholder() {
        assert_eq!(format_error_body(" \n\t ", false), "<empty body>");
    }

    #[test]
    fn byte_truncated_upstream_error_body_is_marked_truncated() {
        let formatted = format_error_body("temporary upstream outage", true);

        assert_eq!(
            formatted,
            format!("temporary upstream outage{}", super::TRUNCATION_SUFFIX)
        );
    }
}

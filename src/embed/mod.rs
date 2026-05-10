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
use serde_json::Value;

use crate::error::RagloomError;

const MAX_UPSTREAM_ERROR_BODY_CHARS: usize = 256;
const MAX_UPSTREAM_ERROR_BODY_BYTES: usize = 1024;
const TRUNCATION_SUFFIX: &str = "...(truncated)";
const REDACTED_EMAIL: &str = "[redacted-email]";
const REDACTED_TOKEN: &str = "[redacted-token]";
const REDACTED_JSON_BODY: &str = "<redacted json body>";
const SAFE_JSON_FIELDS: &[&str] = &["message", "error", "code", "type"];

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
    let sanitized = sanitize_error_body(body);
    let normalized = sanitized.split_whitespace().collect::<Vec<_>>().join(" ");
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

fn sanitize_error_body(body: &str) -> String {
    match serde_json::from_str::<Value>(body) {
        Ok(value) => sanitize_json_error_body(&value),
        Err(_) => redact_plain_text(body),
    }
}

fn sanitize_json_error_body(value: &Value) -> String {
    let mut snippets = Vec::new();
    collect_safe_json_fields(value, None, &mut snippets);

    if snippets.is_empty() {
        REDACTED_JSON_BODY.to_string()
    } else {
        snippets.join("; ")
    }
}

fn collect_safe_json_fields(value: &Value, prefix: Option<&str>, snippets: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if SAFE_JSON_FIELDS.contains(&key.as_str()) {
                    let path = join_json_path(prefix, key);
                    match child {
                        Value::Object(_) | Value::Array(_) => {
                            collect_safe_json_fields(child, Some(path.as_str()), snippets);
                        }
                        _ => {
                            snippets.push(format!(
                                "{}: {}",
                                path,
                                redact_plain_text(&json_scalar_to_text(child))
                            ));
                        }
                    }
                } else {
                    collect_safe_json_fields(child, prefix, snippets);
                }
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_safe_json_fields(child, prefix, snippets);
            }
        }
        _ => {}
    }
}

fn join_json_path(prefix: Option<&str>, key: &str) -> String {
    match prefix {
        Some(prefix) => format!("{prefix}.{key}"),
        None => key.to_string(),
    }
}

fn json_scalar_to_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => String::new(),
    }
}

fn redact_plain_text(text: &str) -> String {
    let mut redacted_words = Vec::new();
    let mut redact_next = false;

    for word in text.split_whitespace() {
        if redact_next {
            redacted_words.push(REDACTED_TOKEN.to_string());
            redact_next = false;
            continue;
        }

        let lower = word.to_ascii_lowercase();
        if lower == "bearer" || lower == "token" {
            redacted_words.push(word.to_string());
            redact_next = true;
            continue;
        }

        let punctuation_end = word
            .find(|ch: char| ch.is_ascii_alphanumeric())
            .unwrap_or(word.len());
        let suffix_start = word
            .rfind(|ch: char| ch.is_ascii_alphanumeric())
            .map(|index| index + 1)
            .unwrap_or(0);

        if punctuation_end >= suffix_start {
            redacted_words.push(word.to_string());
            continue;
        }

        let prefix = &word[..punctuation_end];
        let core = &word[punctuation_end..suffix_start];
        let suffix = &word[suffix_start..];

        let redacted_core = if is_email_like(core) {
            REDACTED_EMAIL.to_string()
        } else if is_secret_like(core) {
            REDACTED_TOKEN.to_string()
        } else {
            core.to_string()
        };

        redacted_words.push(format!("{prefix}{redacted_core}{suffix}"));
    }

    redacted_words.join(" ")
}

fn is_email_like(token: &str) -> bool {
    token
        .split_once('@')
        .is_some_and(|(local, domain)| !local.is_empty() && domain.contains('.'))
}

fn is_secret_like(token: &str) -> bool {
    let token = token.trim_matches(|ch: char| !is_secret_char(ch));
    if token.is_empty() {
        return false;
    }

    let lower = token.to_ascii_lowercase();
    if lower.starts_with("sk-") && token.len() >= 10 {
        return true;
    }

    if lower.starts_with("authorization:") || lower.starts_with("api-key:") {
        return true;
    }

    if token.len() < 24 {
        return false;
    }

    let has_alpha = token.chars().any(|ch| ch.is_ascii_alphabetic());
    let has_digit = token.chars().any(|ch| ch.is_ascii_digit());
    let all_secret_chars = token.chars().all(is_secret_char);

    all_secret_chars && has_alpha && has_digit
}

fn is_secret_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '=')
}

#[cfg(test)]
mod tests {
    use super::{format_error_body, redact_plain_text, sanitize_error_body};

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

    #[test]
    fn json_error_body_only_keeps_safe_fields() {
        let formatted = sanitize_error_body(
            r#"{"error":{"message":"quota exceeded","type":"insufficient_quota","details":"secret doc text"},"request_id":"abc"}"#,
        );

        assert!(formatted.contains("error.message: quota exceeded"));
        assert!(formatted.contains("error.type: insufficient_quota"));
        assert!(!formatted.contains("details"));
        assert!(!formatted.contains("request_id"));
    }

    #[test]
    fn plain_text_redaction_masks_emails_and_tokens() {
        let formatted = redact_plain_text(
            "contact ops@example.com Authorization: Bearer sk-1234567890abcdefghijklmnop",
        );

        assert!(formatted.contains(super::REDACTED_EMAIL));
        assert!(formatted.contains("Bearer [redacted-token]"));
        assert!(!formatted.contains("ops@example.com"));
        assert!(!formatted.contains("sk-1234567890abcdefghijklmnop"));
    }
}

//! Size metrics for chunking budgets.
//!
//! # Why
//! Embedding models measure context in tokens; a char-based budget is a rough
//! and often wrong proxy. We expose both metrics as first-class options with a
//! pluggable [`TokenCounter`] trait so future tokenizers can be swapped in.

use std::sync::Arc;
use std::sync::OnceLock;

use super::error::{ChunkError, ChunkResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeMetric {
    Chars,
    Tokens,
}

pub trait TokenCounter: Send + Sync {
    fn count(&self, text: &str) -> usize;
    /// Short stable identifier used inside the strategy fingerprint.
    fn fingerprint(&self) -> &str;
}

/// Char counter — uses Unicode scalar values (`chars().count()`).
#[derive(Debug, Default)]
pub struct CharCounter;

impl TokenCounter for CharCounter {
    fn count(&self, text: &str) -> usize {
        text.chars().count()
    }
    fn fingerprint(&self) -> &str {
        "chars"
    }
}

/// Tiktoken cl100k_base counter.
///
/// # Why
/// Ragloom's OpenAI embedding backend uses cl100k_base family encodings.
/// We cache the BPE in a process-global `OnceLock` to avoid repeated setup.
pub struct TiktokenCounter {
    bpe: Arc<tiktoken_rs::CoreBPE>,
}

static CL100K_BPE: OnceLock<Arc<tiktoken_rs::CoreBPE>> = OnceLock::new();

impl TiktokenCounter {
    pub fn cl100k_base() -> ChunkResult<Self> {
        let bpe = CL100K_BPE
            .get_or_init(|| {
                Arc::new(tiktoken_rs::cl100k_base().expect("cl100k_base is built in"))
            })
            .clone();
        Ok(Self { bpe })
    }
}

impl TokenCounter for TiktokenCounter {
    fn count(&self, text: &str) -> usize {
        // `encode_with_special_tokens` matches what real clients see.
        self.bpe.encode_with_special_tokens(text).len()
    }
    fn fingerprint(&self) -> &str {
        "tiktoken:cl100k_base"
    }
}

/// Helper used by chunker constructors to turn a metric into a concrete counter.
pub(crate) fn counter_for(metric: SizeMetric) -> ChunkResult<Arc<dyn TokenCounter>> {
    match metric {
        SizeMetric::Chars => Ok(Arc::new(CharCounter)),
        SizeMetric::Tokens => {
            let tc = TiktokenCounter::cl100k_base()
                .map_err(|e| ChunkError::Tokenizer(format!("{e}")))?;
            Ok(Arc::new(tc))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn char_counter_counts_unicode_scalars_not_bytes() {
        let c = CharCounter;
        assert_eq!(c.count(""), 0);
        assert_eq!(c.count("abc"), 3);
        assert_eq!(c.count("你好"), 2);
        assert_eq!(c.count("🙂"), 1);
        assert_eq!(c.fingerprint(), "chars");
    }

    #[test]
    fn tiktoken_counter_produces_nonzero_counts_for_ascii_text() {
        let t = TiktokenCounter::cl100k_base().expect("tiktoken cl100k_base");
        assert!(t.count("hello world") >= 2);
        assert_eq!(t.count(""), 0);
        assert_eq!(t.fingerprint(), "tiktoken:cl100k_base");
    }

    #[test]
    fn counter_for_returns_expected_concrete_counter() {
        let c = counter_for(SizeMetric::Chars).expect("chars counter");
        assert_eq!(c.fingerprint(), "chars");
        let t = counter_for(SizeMetric::Tokens).expect("tokens counter");
        assert_eq!(t.fingerprint(), "tiktoken:cl100k_base");
    }
}

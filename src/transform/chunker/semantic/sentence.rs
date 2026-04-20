//! UAX #29 sentence segmentation for [`super::SemanticChunker`].
//!
//! # Why
//! Semantic chunking hinges on high-quality sentence boundaries. Rust's
//! `unicode-segmentation` crate implements Unicode Standard Annex #29 and
//! handles CJK / Arabic / abbreviations consistently without runtime deps.

use unicode_segmentation::UnicodeSegmentation;

/// A sentence slice with its byte offsets into the original string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sentence<'a> {
    pub start_byte: usize,
    pub end_byte: usize,
    pub text: &'a str,
}

/// Segment `text` into sentences. Empty or whitespace-only segments are
/// skipped. Returned offsets refer to the original string.
pub fn sentences(text: &str) -> Vec<Sentence<'_>> {
    text.split_sentence_bound_indices()
        .filter_map(|(start, s)| {
            if s.trim().is_empty() {
                return None;
            }
            Some(Sentence {
                start_byte: start,
                end_byte: start + s.len(),
                text: s,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_plain_english() {
        let text = "One sentence. Two sentences! Three?";
        let got = sentences(text);
        assert_eq!(got.len(), 3, "got: {:?}", got);
        assert!(got[0].text.starts_with("One"));
        assert!(got[1].text.starts_with("Two"));
        assert!(got[2].text.starts_with("Three"));
    }

    #[test]
    fn handles_chinese() {
        let text = "你好。今天天气不错。我们走吧！";
        let got = sentences(text);
        assert!(got.len() >= 3, "got: {:?}", got);
    }

    #[test]
    fn handles_empty_and_whitespace() {
        assert!(sentences("").is_empty());
        assert!(sentences("   \n  \t  ").is_empty());
    }

    #[test]
    fn single_sentence_without_terminator() {
        let got = sentences("no final period");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].text.trim_end(), "no final period");
    }

    #[test]
    fn byte_offsets_align_with_original_text() {
        let text = "A. 你好。C.";
        let got = sentences(text);
        for s in &got {
            assert_eq!(&text[s.start_byte..s.end_byte], s.text);
        }
    }
}

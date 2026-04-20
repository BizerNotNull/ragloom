//! UTF-8 safe byte-level boundary scanner.
//!
//! # Why
//! Ragloom's chunker needs to find paragraph, line, sentence, and whitespace
//! boundaries in UTF-8 text without ever returning a split point that lies
//! inside a multi-byte code point. The scanner operates in byte-space for
//! speed (delegating to the SIMD-accelerated `chunk` crate for the hottest
//! single-byte delimiter, `\n`) but only emits offsets that satisfy
//! [`str::is_char_boundary`].

use std::borrow::Cow;

/// A candidate split point, measured in *byte* offsets into the input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Boundary {
    pub end_byte: usize,
    pub kind: BoundaryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BoundaryKind {
    Paragraph,
    Line,
    Sentence,
    Whitespace,
    Forced,
}

/// Scans `text` from `start_byte` up to `start_byte + max_window_bytes` and
/// returns every candidate boundary that lies strictly inside that window,
/// annotated with its matching kind. The result is sorted by ascending
/// `end_byte`. Every offset is guaranteed to lie on a UTF-8 character
/// boundary.
pub fn scan_boundaries(
    text: &str,
    start_byte: usize,
    max_window_bytes: usize,
) -> Vec<Boundary> {
    let bytes = text.as_bytes();
    debug_assert!(start_byte <= bytes.len());
    let window_end = (start_byte + max_window_bytes).min(bytes.len());
    let window: &[u8] = &bytes[start_byte..window_end];

    let mut out: Vec<Boundary> = Vec::new();

    // Paragraph: "\n\n"
    let mut i = 0;
    while i + 1 < window.len() {
        if window[i] == b'\n' && window[i + 1] == b'\n' {
            let abs = start_byte + i;
            out.push(Boundary {
                end_byte: abs,
                kind: BoundaryKind::Paragraph,
            });
        }
        i += 1;
    }

    // Line: single "\n" — delegate enumeration to the SIMD-accelerated
    // `chunk` crate via `split_at_delimiters`. Each returned `(start, end)`
    // pair describes a segment; with `IncludeDelim::Prev` the delimiter is
    // attached to the previous segment, so a segment whose last byte is
    // `\n` marks a line boundary at `end`.
    if !window.is_empty() {
        let offsets = chunk::split_at_delimiters(window, b"\n", chunk::IncludeDelim::Prev, 0);
        for (_, end) in offsets {
            // Only segments terminated by '\n' count as line boundaries
            // (the final segment may lack a trailing newline).
            if end == 0 || window.get(end - 1) != Some(&b'\n') {
                continue;
            }
            let abs = start_byte + end;
            out.push(Boundary {
                end_byte: abs,
                kind: BoundaryKind::Line,
            });
        }
    }

    // Sentence: '.' / '?' / '!' followed by ' ' or '\n'.
    let mut j = 0;
    while j + 1 < window.len() {
        let b = window[j];
        let next = window[j + 1];
        if (b == b'.' || b == b'?' || b == b'!') && (next == b' ' || next == b'\n') {
            let abs = start_byte + j + 1;
            out.push(Boundary {
                end_byte: abs,
                kind: BoundaryKind::Sentence,
            });
        }
        j += 1;
    }

    // Whitespace: ' ' or '\t' (newlines were handled above).
    for (rel, &b) in window.iter().enumerate() {
        if b == b' ' || b == b'\t' {
            let abs = start_byte + rel + 1;
            out.push(Boundary {
                end_byte: abs,
                kind: BoundaryKind::Whitespace,
            });
        }
    }

    // Clamp and keep only UTF-8-safe offsets. The `chunk` crate operates on
    // raw bytes and may in principle emit offsets that don't lie on a char
    // boundary; the filter below makes that safe.
    out.retain(|b| b.end_byte <= window_end && text.is_char_boundary(b.end_byte));
    out.sort_by_key(|b| b.end_byte);
    out
}

/// Build a "forced" boundary at the last UTF-8 char boundary within the window.
pub fn forced_boundary(start_byte: usize, max_window_bytes: usize, text: &str) -> Boundary {
    let raw = (start_byte + max_window_bytes).min(text.len());
    let mut end = raw;
    while end > start_byte && !text.is_char_boundary(end) {
        end -= 1;
    }
    Boundary {
        end_byte: end,
        kind: BoundaryKind::Forced,
    }
}

/// Normalise CRLF / CR line endings to LF.
///
/// Returns `Cow::Borrowed` when the input is already LF-only.
pub fn normalize_newlines(text: &str) -> Cow<'_, str> {
    if !text.contains('\r') {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                out.push('\n');
            }
            _ => out.push(ch),
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_produces_paragraph_line_sentence_and_whitespace_candidates() {
        let text = "hello world.\n\nnext line. more";
        let boundaries = scan_boundaries(text, 0, text.len());
        let kinds: Vec<BoundaryKind> = boundaries.iter().map(|b| b.kind).collect();
        assert!(kinds.contains(&BoundaryKind::Paragraph));
        assert!(kinds.contains(&BoundaryKind::Line));
        assert!(kinds.contains(&BoundaryKind::Sentence));
        assert!(kinds.contains(&BoundaryKind::Whitespace));
    }

    #[test]
    fn scan_never_returns_mid_codepoint_boundaries() {
        let text = "a你好b c";
        let boundaries = scan_boundaries(text, 0, text.len());
        for b in boundaries {
            assert!(
                text.is_char_boundary(b.end_byte),
                "offset {} not on char boundary",
                b.end_byte
            );
        }
    }

    #[test]
    fn forced_boundary_rounds_back_to_char_boundary() {
        let text = "a你b";
        let fb = forced_boundary(0, 2, text);
        assert_eq!(fb.kind, BoundaryKind::Forced);
        assert!(text.is_char_boundary(fb.end_byte));
        assert_eq!(fb.end_byte, 1);
    }

    #[test]
    fn normalize_newlines_converts_crlf_and_cr_to_lf() {
        assert_eq!(&*normalize_newlines("a\r\nb\rc\nd"), "a\nb\nc\nd");
    }

    #[test]
    fn normalize_newlines_returns_borrowed_when_no_cr() {
        let n = normalize_newlines("all good");
        assert!(matches!(n, std::borrow::Cow::Borrowed(_)));
    }
}

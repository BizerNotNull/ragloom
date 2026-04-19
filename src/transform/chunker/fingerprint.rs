//! Stable strategy fingerprint used to salt deterministic point IDs.
//!
//! # Why
//! Once chunkers can vary in strategy, parameters, and model version, the
//! deterministic point-ID scheme must include those inputs or silent
//! collisions will happen after upgrades.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StrategyFingerprint(String);

impl StrategyFingerprint {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for StrategyFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_bytes_and_str() {
        let fp = StrategyFingerprint::new("recursive:v1|metric=chars|max=1000");
        assert_eq!(fp.as_str(), "recursive:v1|metric=chars|max=1000");
        assert_eq!(fp.as_bytes().len(), fp.as_str().len());
    }

    #[test]
    fn equal_strings_produce_equal_fingerprints() {
        let a = StrategyFingerprint::new("x");
        let b = StrategyFingerprint::new(String::from("x"));
        assert_eq!(a, b);
    }
}

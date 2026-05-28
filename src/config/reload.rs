//! Configuration reload sources.
//!
//! # Why
//! Hot reload must be testable and composable. By abstracting the trigger
//! mechanism (SIGHUP vs file watch), the pipeline runtime remains open for
//! extension without modification.

use std::path::PathBuf;

use blake3::Hash;

use crate::error::{RagloomError, RagloomErrorKind};

/// A source of reload signals.
///
/// # Why
/// The runtime should not couple itself to an OS mechanism. This trait provides
/// the minimum surface needed to locate the config file and later attach a
/// trigger implementation.
pub trait ReloadSource: Send + Sync {
    fn config_path(&self) -> PathBuf;
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ReloadObservation {
    Contents(Hash),
    ReadError(String),
}

/// Polls a file-backed config path for content changes.
///
/// # Why
/// Ragloom already uses polling for source discovery. Reusing a small polling
/// trigger for config reload keeps the implementation dependency-free and
/// deterministic while still letting the daemon react to file edits.
#[derive(Debug, Clone)]
pub struct FileReloadSource {
    path: PathBuf,
    last_observation: ReloadObservation,
}

impl FileReloadSource {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, RagloomError> {
        let path = path.into();
        let last_observation = read_observation(&path)?;
        Ok(Self {
            path,
            last_observation,
        })
    }

    pub fn poll_changed_contents(&mut self) -> Result<Option<String>, RagloomError> {
        match read_observation_with_contents(&self.path) {
            Ok((next_observation, contents)) => {
                if next_observation == self.last_observation {
                    return Ok(None);
                }
                self.last_observation = next_observation;
                Ok(contents)
            }
            Err(err) => {
                let next_observation = ReloadObservation::ReadError(err.to_string());
                if next_observation == self.last_observation {
                    return Ok(None);
                }
                self.last_observation = next_observation;
                Err(err)
            }
        }
    }
}

impl ReloadSource for FileReloadSource {
    fn config_path(&self) -> PathBuf {
        self.path.clone()
    }
}

fn read_observation(path: &PathBuf) -> Result<ReloadObservation, RagloomError> {
    let (observation, _contents) = read_observation_with_contents(path)?;
    Ok(observation)
}

fn read_observation_with_contents(
    path: &PathBuf,
) -> Result<(ReloadObservation, Option<String>), RagloomError> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let hash = blake3::hash(contents.as_bytes());
            Ok((ReloadObservation::Contents(hash), Some(contents)))
        }
        Err(err) => Err(
            RagloomError::new(RagloomErrorKind::Io, err).with_context(format!(
                "failed to read config file for reload: {}",
                path.display()
            )),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;

    use tempfile::NamedTempFile;

    #[test]
    fn file_reload_source_emits_once_per_content_change() {
        let mut file = NamedTempFile::new().expect("temp file");
        write!(file, "retry:\n  max_attempts: 3\n").expect("write initial");

        let mut reload = FileReloadSource::new(file.path()).expect("reload source");
        assert_eq!(reload.poll_changed_contents().expect("poll"), None);

        std::fs::write(file.path(), "retry:\n  max_attempts: 4\n").expect("rewrite");
        let changed = reload
            .poll_changed_contents()
            .expect("poll changed")
            .expect("changed contents");
        assert!(changed.contains("max_attempts: 4"));

        assert_eq!(reload.poll_changed_contents().expect("poll"), None);
    }
}

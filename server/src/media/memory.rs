//! **Where the last play session on a torrent file ended**, kept across
//! restarts: what a resumed film's pre-want asks for first
//! (`super::prewant`, [`crate::ServerHandle::note_media_position`]).
//!
//! One small JSON file beside `settings.json` (`read-positions.json`),
//! holding the last [`MEMORY_CAP`] files by when they were left, the oldest
//! let go first. Keyed by info hash and file index, never by anything the
//! app named: a media id does not outlive a restart, and the bytes of a
//! torrent file are where they are whatever id reads them. A file that
//! will not parse is a memory of nothing, never an error: the worst it
//! costs is an estimate where a memory would have been.

use super::prewant::Remembered;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;

/// How many files are remembered: far more than anybody leaves half
/// watched, and a file of a few kilobytes.
pub const MEMORY_CAP: usize = 64;

/// The file's name, beside `settings.json` in the config directory.
pub const MEMORY_FILE: &str = "read-positions.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Kept {
    info_hash: String,
    file_idx: usize,
    #[serde(flatten)]
    at: Remembered,
}

/// The memory: in memory, and written through to its file when it has one.
pub struct ReadMemory {
    path: Option<PathBuf>,
    kept: std::sync::Mutex<VecDeque<Kept>>,
    /// One write at a time, serialised from the snapshot to the rename, so
    /// an older snapshot never lands last.
    writer: tokio::sync::Mutex<()>,
}

impl Default for ReadMemory {
    /// A memory with no file: kept for this process only.
    fn default() -> Self {
        Self {
            path: None,
            kept: std::sync::Mutex::default(),
            writer: tokio::sync::Mutex::const_new(()),
        }
    }
}

impl ReadMemory {
    /// The memory kept at `path`, empty when there is none or it will not
    /// parse.
    pub fn load(path: PathBuf) -> Self {
        let kept = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<VecDeque<Kept>>(&bytes).ok())
            .unwrap_or_default();
        Self {
            path: Some(path),
            kept: std::sync::Mutex::new(kept),
            writer: tokio::sync::Mutex::const_new(()),
        }
    }

    fn kept(&self) -> std::sync::MutexGuard<'_, VecDeque<Kept>> {
        self.kept
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Where the last session on `file_idx` of `info_hash` ended, if it is
    /// remembered.
    pub fn recall(&self, info_hash: &str, file_idx: usize) -> Option<Remembered> {
        self.kept()
            .iter()
            .find(|kept| kept.info_hash == info_hash && kept.file_idx == file_idx)
            .map(|kept| kept.at)
    }

    /// Remember where a session on `file_idx` of `info_hash` ended, the
    /// newest entry, and write the memory to its file. A write that fails is
    /// logged and the memory stands for this process.
    pub async fn remember(&self, info_hash: &str, file_idx: usize, at: Remembered) {
        let _writer = self.writer.lock().await;
        let snapshot = {
            let mut kept = self.kept();
            kept.retain(|kept| !(kept.info_hash == info_hash && kept.file_idx == file_idx));
            kept.push_back(Kept {
                info_hash: info_hash.to_string(),
                file_idx,
                at,
            });
            while kept.len() > MEMORY_CAP {
                kept.pop_front();
            }
            kept.clone()
        };
        let Some(path) = &self.path else {
            return;
        };
        let written = match serde_json::to_vec(&snapshot) {
            Ok(bytes) => crate::state::write_whole(path, &bytes).await,
            Err(error) => Err(error.into()),
        };
        if let Err(error) = written {
            tracing::warn!(
                error = %format!("{error:#}"),
                "could not write where the last play sessions ended; kept for this run only"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(offset: u64) -> Remembered {
        Remembered {
            offset,
            at_ms: offset * 10,
        }
    }

    /// **Remembered across a restart**, the newest session on a file
    /// replacing the last, and the oldest files let go past the cap.
    #[tokio::test]
    async fn the_memory_outlives_a_restart_and_keeps_the_newest_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(MEMORY_FILE);
        let memory = ReadMemory::load(path.clone());
        assert_eq!(memory.recall("aa", 0), None);
        memory.remember("aa", 0, at(1)).await;
        memory.remember("aa", 0, at(2)).await;
        memory.remember("aa", 1, at(3)).await;
        let again = ReadMemory::load(path.clone());
        assert_eq!(again.recall("aa", 0), Some(at(2)), "the newest session");
        assert_eq!(again.recall("aa", 1), Some(at(3)));
        assert_eq!(again.recall("bb", 0), None);

        for n in 0..MEMORY_CAP as u64 {
            again.remember(&format!("h{n}"), 0, at(n)).await;
        }
        let last = ReadMemory::load(path.clone());
        assert_eq!(last.recall("aa", 0), None, "the oldest went past the cap");
        assert_eq!(last.recall("h0", 0), Some(at(0)));

        std::fs::write(&path, b"{not json").unwrap();
        assert_eq!(
            ReadMemory::load(path).recall("h0", 0),
            None,
            "a memory of nothing"
        );
    }
}

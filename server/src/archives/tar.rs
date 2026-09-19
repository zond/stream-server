use super::{ArchiveEntry, ArchiveReader, OpenedMember, window::MemberWindow};
use anyhow::{Result, anyhow};
use std::path::PathBuf;
use tokio::fs::File;

pub struct TarHandler {
    path: PathBuf,
}

impl TarHandler {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait::async_trait]
impl ArchiveReader for TarHandler {
    async fn list_files(&self) -> Result<Vec<ArchiveEntry>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(&path)?;
            let mut archive = tar::Archive::new(file);
            let mut entries = Vec::new();

            for file in archive.entries()? {
                let file = file?;
                entries.push(ArchiveEntry {
                    path: file.path()?.to_string_lossy().to_string(),
                    size: file.size(),
                    is_dir: file.header().entry_type().is_dir(),
                });
            }
            Ok(entries)
        })
        .await?
    }

    async fn open_file(&self, path: &str) -> Result<OpenedMember> {
        // For TAR, we need to find the offset and size.
        // We can't jump directly without index.
        // Linear scan is needed (unfortunately, typical for TAR).
        // Optimization: Cache index? For now, scan.

        let path_clone = self.path.clone();
        let target_path = path.to_string();

        // Use spawn_blocking to scan because `tar` crate is sync
        let (offset, size) = tokio::task::spawn_blocking(move || -> Result<(u64, u64)> {
            let file = std::fs::File::open(&path_clone)?;
            let mut archive = tar::Archive::new(file);
            for file in archive.entries()? {
                let file = file?;
                if file.path()?.to_string_lossy() == target_path {
                    return Ok((file.raw_file_position(), file.size()));
                }
            }
            Err(anyhow!("File not found in TAR archive"))
        })
        .await??;

        // A stored TAR member is a byte range of the archive: no decoding,
        // no second copy on disk, and the reader is the shared window (see
        // `archives::window`).
        let member = MemberWindow::new(File::open(&self.path).await?, offset, size).await?;
        Ok(OpenedMember::Direct(Box::new(member)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn content() -> Vec<u8> {
        (0..40 * 1024u32)
            .map(|i| (i.wrapping_mul(31) % 251) as u8)
            .collect()
    }

    fn write_tar(dir: &std::path::Path) -> PathBuf {
        let path = dir.join("fixture.tar");
        let mut builder = tar::Builder::new(std::fs::File::create(&path).unwrap());
        for (name, data) in [
            ("first.txt", b"first".to_vec()),
            ("videos/second.bin", content()),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, name, data.as_slice())
                .unwrap();
        }
        builder.finish().unwrap();
        path
    }

    /// A TAR member is served as a range of the archive -- no extraction,
    /// nothing on disk -- and it is served *whole*: the slice reader this
    /// replaced filled a `ReadBuf::take` sub-buffer and never advanced the
    /// caller's, so every read reported zero bytes and the member went out
    /// as an empty body.
    #[tokio::test]
    async fn a_member_is_read_whole_from_the_archive_itself() {
        let root = tempfile::tempdir().unwrap();
        let handler = TarHandler::new(write_tar(root.path()));

        let OpenedMember::Direct(mut reader) =
            handler.open_file("videos/second.bin").await.unwrap()
        else {
            panic!("a stored TAR member needs no extraction");
        };
        assert_eq!(reader.seek(std::io::SeekFrom::End(0)).await.unwrap(), 40960);
        reader.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, content());

        // And a range out of the middle of it is that range, not the
        // archive's bytes at that offset.
        let OpenedMember::Direct(mut reader) =
            handler.open_file("videos/second.bin").await.unwrap()
        else {
            panic!("a stored TAR member needs no extraction");
        };
        reader.seek(std::io::SeekFrom::Start(1024)).await.unwrap();
        let mut middle = [0u8; 512];
        reader.read_exact(&mut middle).await.unwrap();
        assert_eq!(middle.as_slice(), &content()[1024..1536]);
    }
}

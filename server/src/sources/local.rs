//! **A file on this device as a [`ByteSource`]**
//! (`docs/design/media-pipeline.md` §2.3, step B): a path, or on Android an
//! fd `ParcelFileDescriptor.detachFd()` handed over.
//!
//! Nothing fetches it and nothing retains it: the bytes are the file's own
//! and stay where they are, so [`ByteSource::is_live`] keeps its `false`
//! default -- no retention cell could ever name one.
//!
//! **Random access or nothing.** A cloud provider behind Android's storage
//! framework may hand out a pipe rather than a file: its bytes can be read
//! once, forwards, and a player seeks. [`LocalSource::open_file`] asks the
//! fd to seek (`lseek`) and refuses one that cannot, with
//! [`LocalError::NotSeekable`]'s sentence, rather than streaming it forward
//! only and failing at the viewer's first seek.
//!
//! **Every read is positional** (`pread`, `seek_read` on Windows), on the
//! blocking pool. An fd handed over is one open file description, and a
//! `dup` of it shares its offset: two readers of one id that each seeked
//! and read would move each other's position. A positional read has no
//! position to share, so a reader here is a `pos` of its own over one
//! shared [`std::fs::File`].

use super::{ByteSource, ReadHint, SeekableReader};
use std::io::{self, Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

/// How much one read of a reader asks the disk for at most: one blocking
/// call's worth, the buffer a player reads through being far smaller.
const READ_CHUNK: usize = 256 * 1024;

/// A file on this device, as the app names it to
/// [`crate::ServerHandle::register`] -- the only way one is ever named: no
/// HTTP route resolves an id, so no URL can reach a file on this device.
#[derive(Debug)]
pub enum LocalFile {
    /// A path the app can open.
    Path(PathBuf),
    /// A descriptor already open for reading, owned from here on: on
    /// Android, `ParcelFileDescriptor.detachFd()` of a document the viewer
    /// picked. Closed when its id is let go.
    #[cfg(unix)]
    Fd(std::os::fd::OwnedFd),
}

impl LocalFile {
    /// What to call it when the app said nothing: a path's file name, and
    /// `None` for an fd, which has no name.
    pub(crate) fn file_name(&self) -> Option<String> {
        match self {
            Self::Path(path) => path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned()),
            #[cfg(unix)]
            Self::Fd(_) => None,
        }
    }
}

/// Why a local file cannot be a source.
#[derive(Debug)]
pub enum LocalError {
    /// The file reads only forwards: a pipe, which is what some cloud
    /// providers hand out for a document.
    NotSeekable,
    /// Not a regular file: a directory, a device.
    NotAFile,
    /// It could not be opened or asked about.
    Open(io::Error),
}

impl std::fmt::Display for LocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSeekable => f.write_str(
                "this file can only be read once from start to end (its provider streams it \
                 rather than handing over the file), and playing it needs to seek: save it \
                 to this device first",
            ),
            Self::NotAFile => f.write_str("that is not a file this device can play"),
            Self::Open(error) => write!(f, "the file on this device could not be opened: {error}"),
        }
    }
}

impl std::error::Error for LocalError {}

/// A file on this device, open, its length known and its seekability
/// proved.
pub struct LocalSource {
    file: Arc<std::fs::File>,
    len: u64,
    name: String,
}

impl LocalSource {
    /// Open `file` and prove it a source: seekable, regular, its length
    /// read. Blocking -- call it from the blocking pool ([`Self::open`]).
    /// An fd is duplicated rather than consumed, so a refusal leaves the
    /// caller's as it was.
    pub fn open_file(file: &LocalFile, name: String) -> Result<Self, LocalError> {
        let mut opened = match file {
            LocalFile::Path(path) => std::fs::File::open(path).map_err(LocalError::Open)?,
            #[cfg(unix)]
            LocalFile::Fd(fd) => std::fs::File::from(fd.try_clone().map_err(LocalError::Open)?),
        };
        // The pipe question first: a pipe is not a regular file either, and
        // the sentence that says why it cannot play is this one.
        // `stream_position` is `lseek(fd, 0, SEEK_CUR)`, which a pipe
        // answers with `ESPIPE`.
        opened
            .stream_position()
            .map_err(|_| LocalError::NotSeekable)?;
        let metadata = opened.metadata().map_err(LocalError::Open)?;
        if !metadata.is_file() {
            return Err(LocalError::NotAFile);
        }
        Ok(Self {
            file: Arc::new(opened),
            len: metadata.len(),
            name,
        })
    }

    /// [`Self::open_file`] on the blocking pool.
    pub async fn open(file: &LocalFile, name: String) -> Result<Self, LocalError> {
        let file = match file {
            LocalFile::Path(path) => LocalFile::Path(path.clone()),
            #[cfg(unix)]
            LocalFile::Fd(fd) => LocalFile::Fd(fd.try_clone().map_err(LocalError::Open)?),
        };
        tokio::task::spawn_blocking(move || Self::open_file(&file, name))
            .await
            .map_err(|error| LocalError::Open(io::Error::other(error)))?
    }

    /// What to call it.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Its type, from its name's extension, as the stream route answers a
    /// torrent file's.
    pub fn content_type(&self) -> &'static str {
        crate::routes::stream::content_type_for_name(&self.name)
    }
}

/// `want` bytes at `offset`, positionally, filled until the end of the
/// file: what `read_at` promises. Blocking.
fn pread(file: &std::fs::File, offset: u64, want: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; want];
    let mut filled = 0;
    while filled < want {
        #[cfg(unix)]
        let read =
            std::os::unix::fs::FileExt::read_at(file, &mut buf[filled..], offset + filled as u64);
        #[cfg(windows)]
        let read = std::os::windows::fs::FileExt::seek_read(
            file,
            &mut buf[filled..],
            offset + filled as u64,
        );
        match read {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    buf.truncate(filled);
    Ok(buf)
}

/// [`pread`] on the blocking pool.
async fn pread_blocking(file: Arc<std::fs::File>, offset: u64, want: usize) -> io::Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || pread(&file, offset, want))
        .await
        .map_err(io::Error::other)?
}

#[async_trait::async_trait]
impl ByteSource for LocalSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn describe(&self) -> String {
        "a file on this device".to_string()
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if offset >= self.len || buf.is_empty() {
            return Ok(0);
        }
        let read = pread_blocking(self.file.clone(), offset, buf.len()).await?;
        buf[..read.len()].copy_from_slice(&read);
        Ok(read.len())
    }

    async fn open(&self, offset: u64, _hint: ReadHint) -> io::Result<Box<dyn SeekableReader>> {
        Ok(Box::new(LocalReader {
            file: self.file.clone(),
            len: self.len,
            pos: offset,
            reading: None,
        }))
    }
}

type Pending = Pin<Box<dyn std::future::Future<Output = io::Result<Vec<u8>>> + Send>>;

/// A reader of a local file: a position of its own and one positional read
/// in flight at a time.
struct LocalReader {
    file: Arc<std::fs::File>,
    len: u64,
    pos: u64,
    reading: Option<Pending>,
}

impl AsyncRead for LocalReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if this.pos >= this.len || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let reading = this.reading.get_or_insert_with(|| {
            let want = buf
                .remaining()
                .min(READ_CHUNK)
                .min(usize::try_from(this.len - this.pos).unwrap_or(usize::MAX));
            Box::pin(pread_blocking(this.file.clone(), this.pos, want))
        });
        let read = ready!(reading.as_mut().poll(cx));
        this.reading = None;
        let read = read?;
        // The buffer may be smaller than the read begun for an earlier,
        // larger one; what does not fit is read again.
        let taken = read.len().min(buf.remaining());
        buf.put_slice(&read[..taken]);
        this.pos += taken as u64;
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for LocalReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let to = match position {
            SeekFrom::Start(to) => Some(to),
            SeekFrom::Current(by) => self.pos.checked_add_signed(by),
            SeekFrom::End(by) => self.len.checked_add_signed(by),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "a seek before the start"))?;
        if to != self.pos {
            self.pos = to;
            self.reading = None;
        }
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn film(len: usize) -> Vec<u8> {
        (0..len).map(|at| (at % 251) as u8).collect()
    }

    fn written(bytes: &[u8]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("The Film.mkv");
        std::fs::write(&path, bytes).expect("the film written");
        (dir, path)
    }

    /// `read_at` answers the bytes at the offset, short at the end and
    /// nothing past it; a reader reads from where it was opened and from
    /// where a seek put it.
    #[tokio::test]
    async fn reads_at_an_offset_and_through_a_reader() {
        let bytes = film(3 * READ_CHUNK + 17);
        let (_dir, path) = written(&bytes);
        let source = LocalSource::open(&LocalFile::Path(path), "The Film.mkv".to_string())
            .await
            .expect("a source");
        assert_eq!(source.len(), bytes.len() as u64);
        assert_eq!(source.content_type(), "video/x-matroska");

        let mut buf = [0u8; 10];
        assert_eq!(source.read_at(1000, &mut buf).await.expect("read"), 10);
        assert_eq!(buf, bytes[1000..1010]);
        let end = bytes.len() as u64 - 4;
        assert_eq!(source.read_at(end, &mut buf).await.expect("read"), 4);
        assert_eq!(buf[..4], bytes[bytes.len() - 4..]);
        assert_eq!(
            source
                .read_at(bytes.len() as u64, &mut buf)
                .await
                .expect("read"),
            0
        );

        let mut reader = source.open(7, ReadHint::REST).await.expect("a reader");
        let mut all = Vec::new();
        reader.read_to_end(&mut all).await.expect("read");
        assert_eq!(all, bytes[7..]);
        reader
            .seek(SeekFrom::Start(READ_CHUNK as u64 + 3))
            .await
            .expect("seek");
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.expect("read");
        assert_eq!(tail, bytes[READ_CHUNK + 3..]);
    }

    /// Two readers over one fd do not move each other: a positional read
    /// has no shared offset to move.
    #[cfg(unix)]
    #[tokio::test]
    async fn two_readers_of_one_fd_keep_their_own_places() {
        let bytes = film(2 * READ_CHUNK);
        let (_dir, path) = written(&bytes);
        let fd: std::os::fd::OwnedFd = std::fs::File::open(&path).expect("open").into();
        let source = LocalSource::open(&LocalFile::Fd(fd), "x.mkv".to_string())
            .await
            .expect("a source");
        let mut first = source.open(0, ReadHint::REST).await.expect("a reader");
        let mut second = source
            .open(READ_CHUNK as u64, ReadHint::REST)
            .await
            .expect("a reader");
        let mut a = [0u8; 100];
        let mut b = [0u8; 100];
        first.read_exact(&mut a).await.expect("read");
        second.read_exact(&mut b).await.expect("read");
        first.read_exact(&mut a).await.expect("read");
        assert_eq!(a, bytes[100..200]);
        assert_eq!(b, bytes[READ_CHUNK..READ_CHUNK + 100]);
    }

    /// A directory is not a file to play. (Windows will not open one at
    /// all, which is a refusal of its own.)
    #[cfg(unix)]
    #[test]
    fn a_directory_is_refused() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let refused = LocalSource::open_file(&LocalFile::Path(dir.path().into()), "d".into());
        assert!(
            matches!(refused, Err(LocalError::NotAFile)),
            "{:?}",
            refused.err()
        );
    }
}

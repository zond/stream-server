//! **A file whose bytes somebody else fetches and keeps.**
//!
//! The principle this module is the seam for: *only the thing that
//! downloads a file may store it* -- the torrent piece store, the proxy
//! cache. Everything between a fetched file and the player is a
//! translation of byte ranges, and a translation stores nothing. So the
//! one thing an archive, a disc image or a nested container is ever given
//! is a [`ByteSource`]: something it can ask for bytes `a..b` of, which
//! answers out of whatever store already holds them and fetches exactly the
//! rest. A format that cannot say which bytes of the underlying file a
//! member is made of is refused rather than extracted; see
//! `docs/translated-sources.md`.
//!
//! What is here is the seam itself and the two sources a fetcher exists
//! for:
//!
//! * [`ByteSource`], with [`ReadHint`] -- the two ways bytes are asked for,
//!   an index read and a body read, which are different enough that a
//!   source does different things for them;
//! * [`MemberView`] -- several extents of several sources as one file, which
//!   is what a member of a container is. It is a `ByteSource` too, so a
//!   translator can sit on another translator's member;
//! * [`torrent::TorrentFileSource`] over a torrent's file and
//!   [`proxy::ProxySource`] over an HTTP entity through `/proxy`'s cache;
//! * [`testing`] -- a source over a `Vec<u8>` and a wrapper that counts what
//!   was asked of the one underneath, which is how a test proves that
//!   reading an index read no more than the index.

use async_trait::async_trait;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};

pub mod proxy;
pub mod testing;
pub mod torrent;
pub mod view;

pub use proxy::ProxySource;
pub use torrent::TorrentFileSource;
pub use view::{MemberReader, MemberView};

/// A reader over one span of a source's bytes, for one sequential pass.
///
/// **Read-only and forward-only on purpose.** The doc's trait said
/// `AsyncSeekableReader` here, and for a torrent that would be honest --
/// librqbit's reader seeks, and cheaply. For an HTTP entity it would not
/// be: the only thing behind a seek is another ranged request, and a
/// `poll_seek` that quietly refetches is how a translator ends up
/// downloading a file it meant to read a kilobyte of. Seeking a source is
/// therefore spelled [`ByteSource::open`] again, at the new offset, where
/// the cost is visible. [`MemberView`], which *is* seekable because the
/// route above it frames ranges against it, implements its own seek as
/// exactly that reopen.
pub trait SourceReader: AsyncRead + Send + Unpin {}
impl<T: AsyncRead + Send + Unpin> SourceReader for T {}

/// A reader that can also seek: what a torrent's file handle is, and what
/// [`MemberView`] is, so the stream route's range framing can sit on one.
pub trait SeekableReader: AsyncRead + AsyncSeek + Send + Unpin {}
impl<T: AsyncRead + AsyncSeek + Send + Unpin> SeekableReader for T {}

/// How far from the offset the caller expects to read, handed to
/// [`ByteSource::open`].
///
/// It exists because the two fetchers spend it differently and neither can
/// work it out for itself: a torrent turns it into the reader's lookahead
/// -- how far ahead of the playhead the swarm is asked for -- and an HTTP
/// source turns it into the `Range`'s last byte, which is one request
/// instead of one per chunk.
///
/// **It is a bound and not a wish.** A reader may end at it, because for an
/// HTTP source it is literally where the response stops; a caller that
/// wants more opens again at the new offset. That is the honest shape: a
/// hint that only sped things up would let a translator read a whole file
/// through a reader it opened for a header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadHint {
    bytes: u64,
}

impl ReadHint {
    /// Everything from the offset to the end of the source: what a body
    /// read of a whole member asks for.
    pub const REST: Self = Self { bytes: u64::MAX };

    /// `bytes` from the offset.
    pub fn of(bytes: u64) -> Self {
        Self { bytes }
    }

    /// How many bytes were hinted at.
    pub fn bytes(self) -> u64 {
        self.bytes
    }

    /// The last byte this hint covers in a source of `len` bytes, or `None`
    /// when `offset` is at or past the end -- which is the inclusive end an
    /// HTTP `Range` is written with.
    pub fn last_byte(self, offset: u64, len: u64) -> Option<u64> {
        if offset >= len {
            return None;
        }
        Some(
            offset
                .saturating_add(self.bytes.saturating_sub(1))
                .min(len - 1),
        )
    }
}

/// A file whose bytes are fetched and kept by something else. Random access
/// by construction: a source that cannot answer [`read_at`] for an
/// arbitrary offset is not a `ByteSource`, it is a refusal.
///
/// Two ways in, because the two uses differ. An index is a handful of reads
/// at offsets the format dictates -- the end of a ZIP, the start of a RAR
/// volume, sector 16 of an ISO -- and a body is one long read the fetcher
/// should treat as a stream, with a lookahead on a torrent and one ranged
/// request on HTTP, not thousands of [`read_at`]s.
///
/// [`read_at`]: ByteSource::read_at
#[async_trait]
pub trait ByteSource: Send + Sync {
    /// The file's length. Known up front for every source there is: a
    /// torrent's file list states it, an HTTP entity's `Content-Range`
    /// does, and a member view's is the sum of its extents.
    fn len(&self) -> u64;

    /// Whether it has no bytes at all. A real case rather than clippy's
    /// idea of one: a container may hold a member of zero bytes, and a
    /// view of it is a source of no bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A short name for logs and errors: **never a URL and never a
    /// credential**. A `d=` target carries the viewer's signed query and an
    /// `h=` header carries their token, and the log files this process
    /// keeps for the last ten launches are the one place neither may turn
    /// up -- see `routes::util::log_origin`, which is what an HTTP source
    /// describes itself through.
    fn describe(&self) -> String;

    /// `buf.len()` bytes at `offset`, or fewer at the end of the file --
    /// and `0` for an offset at or past it. Short reads below that are
    /// filled in, so a caller reading a header gets the header or the
    /// reason it could not.
    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// A reader positioned at `offset` for a long sequential read: the body
    /// of a response. See [`ReadHint`] for what `hint` is spent on and why
    /// the reader may end at it.
    async fn open(&self, offset: u64, hint: ReadHint) -> io::Result<Box<dyn SourceReader>>;
}

/// One run of one source's bytes: where a member's bytes physically are.
///
/// A member is a list of these -- one for a stored ZIP or TAR member, one
/// per volume for a RAR set, one per directory record for an ISO file over
/// 4 GiB -- and [`MemberView`] is that list read as a file. `source` is an
/// index into the view's own source list, which for a multi-volume set is
/// the volume number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    /// Which of the view's sources these bytes are in.
    pub source: usize,
    /// Where in that source the run starts.
    pub offset: u64,
    /// How long it is.
    pub len: u64,
}

/// [`ByteSource::open`] with the source owned rather than borrowed, so the
/// future it returns can be held in a `poll_read` state machine (see
/// [`MemberView`]).
pub(crate) async fn open_owned(
    source: Arc<dyn ByteSource>,
    offset: u64,
    hint: ReadHint,
) -> io::Result<Box<dyn SourceReader>> {
    source.open(offset, hint).await
}

/// Fill `buf` from `reader`, stopping only at its end: what a `read_at`
/// built on a reader has to do, since one `poll_read` of a network stream
/// returns whatever arrived rather than what was asked for.
pub(crate) async fn read_filling<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]).await? {
            0 => break,
            read => filled += read,
        }
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hint an HTTP source writes into a `Range`: inclusive, clamped
    /// into the entity, and nothing at all past its end.
    #[test]
    fn a_hint_names_the_last_byte_of_the_span_it_covers() {
        assert_eq!(ReadHint::of(10).last_byte(0, 100), Some(9));
        assert_eq!(ReadHint::of(1).last_byte(7, 100), Some(7));
        // Clamped into the entity: a hint reaching past the end asks for
        // the end.
        assert_eq!(ReadHint::of(1000).last_byte(50, 100), Some(99));
        assert_eq!(ReadHint::REST.last_byte(0, 100), Some(99));
        // And at or past the end there is no satisfiable range at all.
        assert_eq!(ReadHint::of(10).last_byte(100, 100), None);
        assert_eq!(ReadHint::of(10).last_byte(0, 0), None);
    }
}

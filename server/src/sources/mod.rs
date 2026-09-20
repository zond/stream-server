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

/// A reader over a source's bytes: what [`ByteSource::open`] hands out.
///
/// **Seekable, like every other handle on a fetched file.** A fetcher
/// already assumes a consumer it does not understand -- mpv seeks
/// anywhere, the piece store and the proxy cache serve what they hold and
/// fetch the rest, the stream detector and the cache's own bound cope with
/// whatever that consumer does. A translator is one more such consumer,
/// and a forward-only handle capped at its hint would be a second, weaker
/// copy of a protection the fetcher already gives -- one no other consumer
/// is held to, and one the format crates fight, since their parsers take
/// `Read + Seek`. So a seek is a seek: cheap in the piece store, and one
/// new ranged request through the proxy cache, which is exactly what a
/// player's seek through `/proxy` already is.
///
/// What keeps an index read small is therefore not the handle. It is the
/// translator's own read budget, counted and asserted
/// (`crate::translators::Budget`, and `crate::images` before it).
pub trait SeekableReader: AsyncRead + AsyncSeek + Send + Unpin {}
impl<T: AsyncRead + AsyncSeek + Send + Unpin> SeekableReader for T {}

/// How far from the offset the caller expects to read, handed to
/// [`ByteSource::open`].
///
/// **Advisory.** It exists because an HTTP source can spend it -- one
/// ranged request for the whole span instead of one per chunk -- and a
/// torrent cannot: the engine works the lookahead out from the intent, the
/// film's measured bitrate and what the retention budget can keep, which
/// is a better answer than any caller has. A reader may read past its
/// hint, and the one that does simply asks its source for the next span.
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

    /// A reader positioned at `offset`, for a long read: the body of a
    /// response, or a format parser that wants to seek about. See
    /// [`ReadHint`] for what `hint` is spent on, and [`SeekableReader`] for
    /// why a seek on one is a seek rather than a refusal.
    async fn open(&self, offset: u64, hint: ReadHint) -> io::Result<Box<dyn SeekableReader>>;

    /// Whether the entity `reading` names -- the one stream this server is
    /// playing (`enginefs::retention::live`) -- is this source's own file.
    ///
    /// What it is for: a translated container's session is kept while its
    /// container is what is playing and goes when the viewer opens
    /// something else, and the container is a list of these
    /// (`crate::translators::session`). A source is the only thing that
    /// knows which retained entity its bytes are, so it is the thing that
    /// answers.
    ///
    /// **The default is `false`, and it is the honest answer for a source
    /// whose bytes this server does not retain** -- a view over other
    /// sources, a fixture in memory: nothing in the retention cell could
    /// ever name one, so nothing could keep it alive and nothing should
    /// pretend to. A source added later over bytes this server *does* keep
    /// must answer for itself; a `false` from one of those would read as
    /// "the viewer has moved on" every time the cell was consulted.
    ///
    /// Cheap and synchronous by contract: it is asked of every session in
    /// the map at the instant the live entity moves, so it may compute a
    /// key but it may not go to the disk or the network.
    fn is_live(&self, reading: &enginefs::retention::live::Reading) -> bool {
        let _ = reading;
        false
    }
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
) -> io::Result<Box<dyn SeekableReader>> {
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

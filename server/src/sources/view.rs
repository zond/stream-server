//! A member of a container as a file: several extents of several sources,
//! read in order.
//!
//! A member that is *stored* -- written into its container uncompressed --
//! is a list of byte ranges of that container, so serving it needs no
//! decoding and no second copy on disk. A [`MemberView`] over the
//! container's sources answers every read and every seek in the member's
//! own coordinates, and the bytes come from wherever the container's bytes
//! come from: a torrent, where a player's seek becomes a seek in the piece
//! store, or the proxy cache, where it becomes one ranged request.
//!
//! **Several sources, not one**, because a RAR set is one member across
//! `.part1.rar`, `.part2.rar` and `.part3.rar`, and an ISO file over 4 GiB
//! is several directory records under one name. The single-extent case --
//! a stored ZIP or TAR member, which is what `archives::window::
//! MemberWindow` used to be on its own -- is this with one extent.
//!
//! The member and a read of it are two types. [`MemberView`] is the member
//! -- what it is made of, with no read in progress -- and is the
//! [`ByteSource`]; [`MemberReader`] is one read of it, and is what is
//! `AsyncRead + AsyncSeek`. They are apart because a `ByteSource` is
//! `Sync` (an `Arc<dyn ByteSource>` is shared between tasks, and a
//! translator holds one while it awaits) and a value with a reader open
//! inside it cannot be: the reader a body is being served through is not
//! `Sync` and neither is the `open` future in flight before it. A view is
//! cheap to clone and every reader taken from one is independent, which is
//! also exactly what a player's second range request wants.
//!
//! The `assume_init`/`advance` discipline in [`MemberReader::poll_read`] is
//! not an optimisation: `ReadBuf::take` hands out a buffer over the
//! parent's *unfilled* region and filling it does not move the parent
//! along, so a reader that does not say what it read reports zero bytes.
//! That bug served every `.tar` member as an empty body.

use super::{ByteSource, Extent, ReadHint, SeekableReader, open_owned};
use std::future::Future;
use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

/// A source's `open` in flight, as a reader's state machine holds it.
type Opening = Pin<Box<dyn Future<Output = io::Result<Box<dyn SeekableReader>>> + Send>>;

/// What a member is made of. Shared by every reader of it.
struct Member {
    name: String,
    sources: Vec<Arc<dyn ByteSource>>,
    extents: Vec<Extent>,
    /// Where each extent begins in the member's own coordinates, with the
    /// member's length as a final entry -- so the extent a position is in
    /// is one `partition_point` away.
    starts: Vec<u64>,
    len: u64,
}

/// One member of a container as a file: the [`ByteSource`] a translator
/// indexes and a route serves ranges of.
#[derive(Clone)]
pub struct MemberView {
    member: Arc<Member>,
}

impl MemberView {
    /// The member `name` made of `extents` over `sources`.
    ///
    /// Refuses an extent naming a source the list does not have, or one
    /// reaching past its source's end: a translator that computed an
    /// offset wrongly is a bug to be told about at the index, not a body
    /// that ends in a read error halfway through a film. Zero-length
    /// extents are dropped -- they are bytes nobody reads, and keeping
    /// them would put a position in two extents at once.
    pub fn new(
        name: impl Into<String>,
        sources: Vec<Arc<dyn ByteSource>>,
        extents: Vec<Extent>,
    ) -> io::Result<Self> {
        let mut kept = Vec::with_capacity(extents.len());
        let mut starts = Vec::with_capacity(extents.len() + 1);
        let mut len = 0u64;
        for extent in extents {
            let source = sources.get(extent.source).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("extent names source {} of {}", extent.source, sources.len()),
                )
            })?;
            if extent.offset.saturating_add(extent.len) > source.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "extent {}..{} reaches past {} ({} bytes)",
                        extent.offset,
                        extent.offset.saturating_add(extent.len),
                        source.describe(),
                        source.len(),
                    ),
                ));
            }
            if extent.len == 0 {
                continue;
            }
            starts.push(len);
            len += extent.len;
            kept.push(extent);
        }
        starts.push(len);
        Ok(Self {
            member: Arc::new(Member {
                name: name.into(),
                sources,
                extents: kept,
                starts,
                len,
            }),
        })
    }

    /// The whole of one source as one member: the single-extent case,
    /// which is what a container that *is* the file looks like.
    pub fn whole(name: impl Into<String>, source: Arc<dyn ByteSource>) -> io::Result<Self> {
        let len = source.len();
        Self::new(
            name,
            vec![source],
            vec![Extent {
                source: 0,
                offset: 0,
                len,
            }],
        )
    }

    /// A reader over the member from its first byte.
    pub fn reader(&self) -> MemberReader {
        self.reader_at(0)
    }

    /// A reader over the member from `pos`. Readers are independent: a
    /// seek in one is invisible to another, which is what two range
    /// requests on one member are.
    pub fn reader_at(&self, pos: u64) -> MemberReader {
        MemberReader {
            member: self.member.clone(),
            pos,
            state: State::Idle,
            seeking: None,
        }
    }

    /// What the member is called, as its container states it.
    pub fn name(&self) -> &str {
        &self.member.name
    }
}

impl Member {
    /// Which extent `pos` is in, and how far into it, or `None` at or past
    /// the member's end.
    fn extent_at(&self, pos: u64) -> Option<(usize, Extent, u64)> {
        if pos >= self.len {
            return None;
        }
        // Every kept extent is at least one byte long, so the runs are
        // strictly increasing and the position lands in exactly one.
        let index = self.starts.partition_point(|start| *start <= pos) - 1;
        Some((index, self.extents[index], pos - self.starts[index]))
    }
}

#[async_trait::async_trait]
impl ByteSource for MemberView {
    fn len(&self) -> u64 {
        self.member.len
    }

    fn describe(&self) -> String {
        self.member.name.clone()
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // Straight through to the sources rather than through a reader: an
        // index read is a handful of scattered reads and must not disturb
        // -- or wait behind -- the body read of the same member.
        let mut filled = 0;
        let mut pos = offset;
        while filled < buf.len() {
            let Some((_, extent, within)) = self.member.extent_at(pos) else {
                break;
            };
            // Compared as `u64` and cast after, never before: `usize` is
            // 32 bits on the Android ABI this ships to (armeabi-v7a), and
            // an extent longer than 4 GiB -- an ISO's, a film's -- casts
            // there to something shorter than itself or to nothing at all.
            let want = (extent.len - within).min((buf.len() - filled) as u64) as usize;
            let read = self.member.sources[extent.source]
                .read_at(extent.offset + within, &mut buf[filled..filled + want])
                .await?;
            if read == 0 {
                break;
            }
            filled += read;
            pos += read as u64;
        }
        Ok(filled)
    }

    async fn open(&self, offset: u64, _hint: ReadHint) -> io::Result<Box<dyn SeekableReader>> {
        // The hint is spent by the extents the reader opens as it reaches
        // them -- each one asks its own source for its own span -- so
        // there is nothing here to narrow it against.
        Ok(Box::new(self.reader_at(offset)))
    }
}

/// What a reader is doing right now.
enum State {
    /// Nothing open: the next read opens the extent its position is in.
    /// Where every reader starts, and where a seek out of the open extent
    /// puts one.
    Idle,
    /// A source's `open` is in flight for extent `index`.
    Opening { index: usize, open: Opening },
    /// A seek of the open reader is in flight, to `left` bytes before the
    /// end of extent `index`: a seek **inside** the extent a reader is
    /// already open on is that reader's own seek, not another `open`.
    Seeking {
        index: usize,
        reader: Box<dyn SeekableReader>,
        left: u64,
    },
    /// A reader is open with `left` bytes of its extent still to come.
    /// `left` is what keeps the read inside the extent: the source has the
    /// container's other bytes after it, and they are not the member's.
    Reading {
        index: usize,
        reader: Box<dyn SeekableReader>,
        left: u64,
    },
}

/// One read of one member: `AsyncRead + AsyncSeek` in the member's own
/// coordinates, whatever the container's are.
pub struct MemberReader {
    member: Arc<Member>,
    /// Position inside the member, moved by every read and by a completed
    /// seek.
    pos: u64,
    state: State,
    /// Where `start_seek` said to go, until `poll_complete` takes it.
    seeking: Option<u64>,
}

impl MemberReader {
    /// Where a `SeekFrom` lands in the member's own coordinates. The inner
    /// sources know nothing of the member, and their `End` is the
    /// container's end rather than this member's.
    fn seek_target(&self, position: SeekFrom) -> io::Result<u64> {
        Ok(match position {
            SeekFrom::Start(from_start) => from_start,
            SeekFrom::End(from_end) => {
                if from_end < 0 {
                    self.member.len.saturating_sub(from_end.unsigned_abs())
                } else {
                    self.member.len.saturating_add(from_end as u64)
                }
            }
            SeekFrom::Current(from_here) => {
                let there = self.pos as i64 + from_here;
                if there < 0 {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "Negative seek"));
                }
                there as u64
            }
        })
    }
}

impl AsyncRead for MemberReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            match &mut this.state {
                State::Idle => {
                    // Past the member's end is the member's end, whatever
                    // the container still has after it.
                    let Some((index, extent, within)) = this.member.extent_at(this.pos) else {
                        return Poll::Ready(Ok(()));
                    };
                    let source = this.member.sources[extent.source].clone();
                    let left = extent.len - within;
                    this.state = State::Opening {
                        index,
                        open: Box::pin(open_owned(
                            source,
                            extent.offset + within,
                            // This extent and no further: the hint is what
                            // a torrent reads ahead by and what an HTTP
                            // source puts in its `Range`, and the bytes
                            // after this extent are not this member's.
                            ReadHint::of(left),
                        )),
                    };
                }
                State::Opening { index, open } => {
                    let index = *index;
                    match open.as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            this.state = State::Idle;
                            return Poll::Ready(Err(error));
                        }
                        Poll::Ready(Ok(reader)) => {
                            let extent = this.member.extents[index];
                            let within = this.pos - this.member.starts[index];
                            this.state = State::Reading {
                                index,
                                reader,
                                left: extent.len - within,
                            };
                        }
                    }
                }
                State::Seeking {
                    index,
                    reader,
                    left,
                } => match Pin::new(reader).poll_complete(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => {
                        this.state = State::Idle;
                        return Poll::Ready(Err(error));
                    }
                    Poll::Ready(Ok(_)) => {
                        let (index, left) = (*index, *left);
                        let State::Seeking { reader, .. } =
                            std::mem::replace(&mut this.state, State::Idle)
                        else {
                            unreachable!("just matched")
                        };
                        this.state = State::Reading {
                            index,
                            reader,
                            left,
                        };
                    }
                },
                State::Reading {
                    index: _,
                    reader,
                    left,
                } => {
                    // As above: the comparison is in `u64` because
                    // `*left` does not fit a 32-bit `usize`, and the
                    // result does by construction.
                    let want = (*left).min(buf.remaining() as u64) as usize;
                    let mut window = buf.take(want);
                    let before = window.filled().len();
                    match Pin::new(reader).poll_read(cx, &mut window) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            this.state = State::Idle;
                            return Poll::Ready(Err(error));
                        }
                        Poll::Ready(Ok(())) => {
                            let read = window.filled().len() - before;
                            if read == 0 {
                                // The extent promised bytes its source
                                // stopped short of. Said as an error
                                // rather than as the member's end: a short
                                // body under a stated `Content-Length` is
                                // a player quietly playing half a film.
                                let short = *left;
                                let name = this.member.name.clone();
                                this.state = State::Idle;
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::UnexpectedEof,
                                    format!("{name} ended {short} bytes before its extent did"),
                                )));
                            }
                            // `take` hands out a buffer over the parent's
                            // *unfilled* region, and filling it does not
                            // move the parent along -- so say so, or the
                            // caller is told the read produced nothing and
                            // reads it as the end of the member.
                            // SAFETY: the read initialised `read` bytes of
                            // the parent's unfilled region through
                            // `window`.
                            unsafe { buf.assume_init(read) };
                            buf.advance(read);
                            this.pos += read as u64;
                            *left -= read as u64;
                            if *left == 0 {
                                // The next read opens the next extent.
                                this.state = State::Idle;
                            }
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
            }
        }
    }
}

impl AsyncSeek for MemberReader {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        this.seeking = Some(this.seek_target(position)?);
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        if let Some(target) = this.seeking.take()
            && target != this.pos
        {
            this.pos = target;
            // **A seek inside the open extent is that reader's own seek.**
            // A source's reader is a handle on the file it came from (see
            // `sources::SeekableReader`), so the member's coordinates are
            // translated to the source's and the handle moves; a target
            // in another extent -- or past the member's end -- is a new
            // reader, because it is a different source's span.
            let open = std::mem::replace(&mut this.state, State::Idle);
            if let State::Reading { index, reader, .. } | State::Seeking { index, reader, .. } =
                open
                && let Some((at, extent, within)) = this.member.extent_at(target)
                && at == index
            {
                let mut reader = reader;
                match Pin::new(&mut reader).start_seek(SeekFrom::Start(extent.offset + within)) {
                    Ok(()) => {
                        this.state = State::Seeking {
                            index,
                            reader,
                            left: extent.len - within,
                        };
                    }
                    // A reader that will not take the seek is simply
                    // replaced: the position is the member's, and the next
                    // read opens the extent it is in.
                    Err(_) => this.state = State::Idle,
                }
            }
        }
        // A seek of the inner reader started above -- or left over from a
        // read that was polled while one was in flight -- is driven here,
        // so the position this returns is one the next read can start at.
        if let State::Seeking { reader, .. } = &mut this.state {
            match Pin::new(reader).poll_complete(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    this.state = State::Idle;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(_)) => {
                    let State::Seeking {
                        index,
                        reader,
                        left,
                    } = std::mem::replace(&mut this.state, State::Idle)
                    else {
                        unreachable!("just matched")
                    };
                    this.state = State::Reading {
                        index,
                        reader,
                        left,
                    };
                }
            }
        }
        Poll::Ready(Ok(this.pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::testing::{CountingSource, MemorySource};
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    /// The member's own bytes: `i * 7 % 251` at `i`, so a byte says where
    /// in the member it came from.
    fn member() -> Vec<u8> {
        (0..96u32)
            .map(|i| (i.wrapping_mul(7) % 251) as u8)
            .collect()
    }

    /// Two containers with the member split across them, and container
    /// bytes on either side of every extent so a read that runs too far or
    /// starts too early is caught: `first[6..70]` then `second[3..35]`.
    fn split_view() -> (MemberView, Arc<CountingSource>, Arc<CountingSource>) {
        let member = member();
        let mut first = b"HEADER".to_vec();
        first.extend(&member[..64]);
        first.extend(b"TRAILER");
        let mut second = b"HDR".to_vec();
        second.extend(&member[64..]);
        second.extend(b"TAIL");

        let first = Arc::new(CountingSource::new(Arc::new(MemorySource::new(
            "volume 1", first,
        ))));
        let second = Arc::new(CountingSource::new(Arc::new(MemorySource::new(
            "volume 2", second,
        ))));
        let view = MemberView::new(
            "film.mkv",
            vec![
                first.clone() as Arc<dyn ByteSource>,
                second.clone() as Arc<dyn ByteSource>,
            ],
            vec![
                Extent {
                    source: 0,
                    offset: 6,
                    len: 64,
                },
                Extent {
                    source: 1,
                    offset: 3,
                    len: 32,
                },
            ],
        )
        .expect("the extents are inside their sources");
        (view, first, second)
    }

    /// A member read whole is its own bytes and nothing of the containers
    /// around it -- across the join, where the reader for one extent is
    /// closed and the next opened.
    #[tokio::test]
    async fn a_view_reads_its_extents_in_order_and_nothing_between_them() {
        let (view, first, second) = split_view();
        let mut reader = view.reader();
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, member());
        // One body read per extent, and no index read at all: the reads
        // went through `open`, which is what a body read is for.
        assert_eq!(first.counts().opens(), 1);
        assert_eq!(second.counts().opens(), 1);
        assert_eq!(first.counts().read_at_calls(), 0);
        // And no byte of either container outside the extents.
        assert_eq!(first.counts().opened_bytes(), 64);
        assert_eq!(second.counts().opened_bytes(), 32);
    }

    /// **A member ends where its extents end, whatever its sources hand
    /// out.** A [`ReadHint`] is advice about how much is coming and not a
    /// cap -- a source's reader is a handle on the whole file, and a
    /// torrent's runs to the end of it -- so what keeps a read inside the
    /// member is the run of the extent it is in. Checked here with the
    /// container's own bytes on either side of every extent, which a
    /// reader that trusted its source to stop would hand to the player.
    #[tokio::test]
    async fn a_source_that_reads_past_its_hint_still_reads_only_the_member() {
        let member = member();
        let mut first = b"HEADER".to_vec();
        first.extend(&member[..64]);
        first.extend(b"TRAILER");
        let mut second = b"HDR".to_vec();
        second.extend(&member[64..]);
        second.extend(b"TAIL");
        let view = MemberView::new(
            "film.mkv",
            vec![
                Arc::new(MemorySource::new("volume 1", first)) as Arc<dyn ByteSource>,
                Arc::new(MemorySource::new("volume 2", second)) as Arc<dyn ByteSource>,
            ],
            vec![
                Extent {
                    source: 0,
                    offset: 6,
                    len: 64,
                },
                Extent {
                    source: 1,
                    offset: 3,
                    len: 32,
                },
            ],
        )
        .unwrap();
        let mut reader = view.reader();
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, member);
    }

    /// **A seek inside the extent a reader is already open on moves that
    /// reader**, rather than opening another at the new offset.
    ///
    /// That is what a source's reader being a handle on its file buys
    /// (`sources::SeekableReader`): the member's position is translated
    /// into the source's and the handle goes there -- a seek of the piece
    /// store, or one ranged request through the proxy cache. Crossing into
    /// another extent is still an `open`, because that is a different
    /// source's span.
    #[tokio::test]
    async fn a_seek_inside_the_open_extent_moves_the_reader_it_has() {
        let (view, first, second) = split_view();
        let member = member();
        let mut reader = view.reader();
        let mut read = [0u8; 8];
        reader.read_exact(&mut read).await.unwrap();
        assert_eq!(read.to_vec(), member[..8]);
        assert_eq!(first.counts().opens(), 1);

        // Forwards and then backwards, both inside the first extent
        // (which is the member's first 64 bytes).
        for at in [40usize, 8] {
            reader.seek(SeekFrom::Start(at as u64)).await.unwrap();
            reader.read_exact(&mut read).await.unwrap();
            assert_eq!(read.to_vec(), member[at..at + 8]);
        }
        assert_eq!(
            first.counts().opens(),
            1,
            "a seek inside the open extent opened the source again"
        );

        // And into the second extent, which is another source: that one
        // has to be opened.
        reader.seek(SeekFrom::Start(70)).await.unwrap();
        reader.read_exact(&mut read).await.unwrap();
        assert_eq!(read.to_vec(), member[70..78]);
        assert_eq!(second.counts().opens(), 1);
        assert_eq!(first.counts().opens(), 1);
    }

    /// Seeks are in the member's coordinates -- `Start` from its first
    /// byte, `End` from its last, `Current` from where the reader is --
    /// and never the container's.
    #[tokio::test]
    async fn seeks_are_in_the_members_own_coordinates() {
        let (view, _, _) = split_view();
        let mut reader = view.reader();
        let member = member();

        assert_eq!(reader.seek(SeekFrom::End(0)).await.unwrap(), 96);
        assert_eq!(reader.seek(SeekFrom::Start(0)).await.unwrap(), 0);

        // The range a player asks for after its seek, landing in the first
        // extent and reading across the join.
        assert_eq!(reader.seek(SeekFrom::Start(32)).await.unwrap(), 32);
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, member[32..]);

        // A seek to the last minutes of the film: the second extent, which
        // is a different source.
        assert_eq!(reader.seek(SeekFrom::End(-4)).await.unwrap(), 92);
        let mut tail = [0u8; 4];
        reader.read_exact(&mut tail).await.unwrap();
        assert_eq!(tail, member[92..]);

        assert_eq!(reader.seek(SeekFrom::Current(-96)).await.unwrap(), 0);
        assert!(reader.seek(SeekFrom::Current(-1)).await.is_err());
    }

    /// **A seek while a reader is open is still a seek.** The reader that
    /// was open is positioned where the last read left it, and nothing but
    /// opening its source again at the new offset can move it -- which is
    /// the only thing an HTTP source could do anyway, and is what a player
    /// seeking mid-file is. Every seek here is made from a reader that is
    /// open and elsewhere: backwards, forwards inside the same extent, and
    /// across the join.
    #[tokio::test]
    async fn a_seek_in_the_middle_of_a_read_lands_where_it_was_told() {
        let (view, _, _) = split_view();
        let member = member();
        let mut reader = view.reader();
        let mut head = [0u8; 8];
        reader.read_exact(&mut head).await.unwrap();
        assert_eq!(head, member[..8]);
        for at in [0usize, 20, 63, 70] {
            assert_eq!(
                reader.seek(SeekFrom::Start(at as u64)).await.unwrap(),
                at as u64
            );
            let mut got = [0u8; 4];
            reader.read_exact(&mut got).await.unwrap();
            assert_eq!(got, member[at..at + 4], "reading from {at}");
        }
    }

    /// A seek exactly onto an extent boundary reads the next extent from
    /// its first byte, and one just before it reads across -- the two ways
    /// a `partition_point` off by one shows up.
    #[tokio::test]
    async fn a_seek_lands_on_the_extent_that_holds_the_byte() {
        let (view, first, second) = split_view();
        let mut reader = view.reader();
        let member = member();

        assert_eq!(reader.seek(SeekFrom::Start(64)).await.unwrap(), 64);
        let mut from_the_join = Vec::new();
        reader.read_to_end(&mut from_the_join).await.unwrap();
        assert_eq!(from_the_join, member[64..]);
        // The first container was never opened: the byte at 64 is the
        // second one's.
        assert_eq!(first.counts().opens(), 0);
        assert_eq!(second.counts().opened_bytes(), 32);

        assert_eq!(reader.seek(SeekFrom::Start(63)).await.unwrap(), 63);
        let mut across = [0u8; 2];
        reader.read_exact(&mut across).await.unwrap();
        assert_eq!(across, member[63..65]);
    }

    /// A seek past the end is not an error -- a player probing a file does
    /// it -- and what follows it is the end of the member.
    #[tokio::test]
    async fn a_seek_past_the_end_reads_nothing() {
        let (view, first, second) = split_view();
        let mut reader = view.reader();
        assert_eq!(reader.seek(SeekFrom::Start(1000)).await.unwrap(), 1000);
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.unwrap();
        assert!(read.is_empty());
        assert_eq!(reader.seek(SeekFrom::End(8)).await.unwrap(), 104);
        assert_eq!(reader.read(&mut [0u8; 8]).await.unwrap(), 0);
        // Nothing was asked of either container for bytes that are not
        // there.
        assert_eq!(first.counts().opens(), 0);
        assert_eq!(second.counts().opens(), 0);
    }

    /// A member of no bytes is a member: it reads empty, it seeks, and it
    /// opens nothing.
    #[tokio::test]
    async fn a_zero_length_member_is_an_empty_file() {
        let source = Arc::new(CountingSource::new(Arc::new(MemorySource::new(
            "archive.zip",
            b"PK\x03\x04nothing".to_vec(),
        ))));
        let view = MemberView::new(
            "empty.txt",
            vec![source.clone() as Arc<dyn ByteSource>],
            vec![Extent {
                source: 0,
                offset: 4,
                len: 0,
            }],
        )
        .unwrap();
        assert_eq!(view.len(), 0);
        assert!(view.is_empty());
        let mut reader = view.reader();
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.unwrap();
        assert!(read.is_empty());
        assert_eq!(reader.seek(SeekFrom::End(0)).await.unwrap(), 0);
        assert_eq!(source.counts().opens(), 0);
    }

    /// The view is a `ByteSource` too, so a translator can sit on another
    /// translator's member -- and its `read_at` crosses extents without
    /// moving what a body read of the same member is doing.
    #[tokio::test]
    async fn a_view_is_itself_a_source_whose_index_reads_cross_extents() {
        let (view, first, second) = split_view();
        let member = member();
        let mut reader = view.reader();

        // A body read in progress.
        let mut head = [0u8; 8];
        reader.read_exact(&mut head).await.unwrap();
        assert_eq!(head, member[..8]);

        // An index read across the join, which is the whole point of the
        // second method: small, scattered, and never through the reader
        // the body is being served from.
        let mut across = [0u8; 16];
        assert_eq!(view.read_at(56, &mut across).await.unwrap(), 16);
        assert_eq!(across, member[56..72]);
        assert_eq!(first.counts().read_at_calls(), 1);
        assert_eq!(second.counts().read_at_calls(), 1);

        // A read that runs off the end returns what there was.
        let mut tail = [0u8; 16];
        assert_eq!(view.read_at(88, &mut tail).await.unwrap(), 8);
        assert_eq!(tail[..8], member[88..]);
        assert_eq!(view.read_at(96, &mut tail).await.unwrap(), 0);

        // And the body read carried on from where it was.
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, member[8..]);
    }

    /// A view over a view: the nesting the design says falls out rather
    /// than being built -- an archive inside an archive inside a torrent.
    #[tokio::test]
    async fn a_view_reads_a_member_of_another_view() {
        let (outer, _, _) = split_view();
        let member = member();
        let inner = MemberView::new(
            "inner.mkv",
            vec![Arc::new(outer) as Arc<dyn ByteSource>],
            vec![Extent {
                source: 0,
                offset: 60,
                len: 10,
            }],
        )
        .unwrap();
        let mut reader = inner.reader();
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, member[60..70]);
    }

    /// An extent naming a source that is not there, or reaching past the
    /// one it names, is refused where it is stated rather than where it is
    /// read.
    #[test]
    fn an_extent_outside_its_source_is_refused_at_the_index() {
        let source: Arc<dyn ByteSource> =
            Arc::new(MemorySource::new("archive.zip", vec![0u8; 100]));
        assert!(
            MemberView::new(
                "film.mkv",
                vec![source.clone()],
                vec![Extent {
                    source: 1,
                    offset: 0,
                    len: 1
                }],
            )
            .is_err()
        );
        assert!(
            MemberView::new(
                "film.mkv",
                vec![source],
                vec![Extent {
                    source: 0,
                    offset: 90,
                    len: 20
                }],
            )
            .is_err()
        );
    }
}

//! One member of an archive as a reader over the archive itself.
//!
//! A member that is *stored* -- written into the archive uncompressed -- is
//! a plain byte range of the archive, so serving it needs no decoding and no
//! second copy on disk: a [`MemberWindow`] over whatever the archive is read
//! through answers every read and every seek in the member's own
//! coordinates, and the bytes come from wherever the archive's bytes come
//! from. When that is a torrent, a player's seek becomes a seek in the
//! torrent, which is the one thing the piece store and its retention were
//! built to serve well -- against an extraction, which is a whole second
//! copy of the film written under the cache root before the first byte can
//! be answered.
//!
//! The window is the reader the TAR handler always wanted, too. Its own
//! slice type filled a `ReadBuf::take` sub-buffer and never advanced the
//! parent (`assume_init`/`advance`, as `cache::ProgressiveReader` does), so
//! every read of a `.tar` member reported zero bytes: the member was served
//! as an empty body.

use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, AsyncSeekExt, ReadBuf};

/// `len` bytes of `inner` starting at `start`, as a reader whose own
/// position 0 is that start and whose end is that length.
pub struct MemberWindow<R> {
    inner: R,
    start: u64,
    len: u64,
    /// Position inside the window, kept in step with the inner reader's own
    /// by [`AsyncSeek::poll_complete`] and by every read.
    pos: u64,
}

impl<R: AsyncSeek + Unpin> MemberWindow<R> {
    /// The window, with `inner` left positioned at its start so a reader
    /// that never seeks still reads the member and not the archive.
    pub async fn new(mut inner: R, start: u64, len: u64) -> std::io::Result<Self> {
        inner.seek(SeekFrom::Start(start)).await?;
        Ok(Self {
            inner,
            start,
            len,
            pos: 0,
        })
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for MemberWindow<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Past the member's end is the member's end, whatever the archive
        // still has after it.
        let left = self.len.saturating_sub(self.pos);
        let want = buf.remaining().min(left as usize);
        if want == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut window = buf.take(want);
        let before = window.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, &mut window) {
            Poll::Ready(Ok(())) => {
                let read = window.filled().len() - before;
                // `take` hands out a buffer over the parent's *unfilled*
                // region, and filling it does not move the parent along --
                // so say so, or the caller is told the read produced
                // nothing and reads it as the end of the member.
                // SAFETY: the read initialised `read` bytes of the parent's
                // unfilled region through `window`.
                unsafe { buf.assume_init(read) };
                buf.advance(read);
                self.pos += read as u64;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<R: AsyncSeek + Unpin> AsyncSeek for MemberWindow<R> {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        // Every form is resolved here, against the window's own length and
        // position, and handed to the inner reader as an absolute seek: the
        // inner reader knows nothing of the member and its `End` is the
        // archive's end, not the member's.
        let target = match position {
            SeekFrom::Start(from_start) => from_start,
            SeekFrom::End(from_end) => {
                if from_end < 0 {
                    self.len.saturating_sub(from_end.unsigned_abs())
                } else {
                    self.len.saturating_add(from_end as u64)
                }
            }
            SeekFrom::Current(from_here) => {
                let here = self.pos as i64;
                let there = here + from_here;
                if there < 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Negative seek",
                    ));
                }
                there as u64
            }
        };
        let inner = self.start.saturating_add(target);
        Pin::new(&mut self.inner).start_seek(SeekFrom::Start(inner))
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        let start = self.start;
        match Pin::new(&mut self.inner).poll_complete(cx) {
            Poll::Ready(Ok(landed)) => {
                self.pos = landed.saturating_sub(start);
                Poll::Ready(Ok(self.pos))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    fn archive() -> std::io::Cursor<Vec<u8>> {
        // `header` `member` `trailer`: a member with archive bytes on both
        // sides of it, so a window that reads too far or starts too early
        // is caught.
        let mut bytes = b"HEADER".to_vec();
        bytes.extend((0..64u32).map(|i| (i.wrapping_mul(7) % 251) as u8));
        bytes.extend(b"TRAILER");
        std::io::Cursor::new(bytes)
    }

    fn member() -> Vec<u8> {
        (0..64u32)
            .map(|i| (i.wrapping_mul(7) % 251) as u8)
            .collect()
    }

    /// A window reads its member and stops at its end -- the archive's own
    /// bytes after it are not the member's -- and reports what it read, so a
    /// caller is not told the member is empty.
    #[tokio::test]
    async fn a_window_reads_the_member_and_nothing_after_it() {
        let mut window = MemberWindow::new(archive(), 6, 64).await.unwrap();
        let mut read = Vec::new();
        window.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, member());
    }

    /// Seeks are in the member's coordinates: `Start` from its first byte,
    /// `End` from its last, `Current` from where the window is -- never the
    /// archive's.
    #[tokio::test]
    async fn seeks_are_in_the_members_own_coordinates() {
        let mut window = MemberWindow::new(archive(), 6, 64).await.unwrap();

        assert_eq!(window.seek(SeekFrom::End(0)).await.unwrap(), 64);
        assert_eq!(window.seek(SeekFrom::Start(0)).await.unwrap(), 0);

        // The range a player asks for after its seek.
        assert_eq!(window.seek(SeekFrom::Start(32)).await.unwrap(), 32);
        let mut rest = Vec::new();
        window.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, member()[32..]);

        assert_eq!(window.seek(SeekFrom::End(-4)).await.unwrap(), 60);
        let mut tail = [0u8; 4];
        window.read_exact(&mut tail).await.unwrap();
        assert_eq!(tail, member()[60..]);

        assert_eq!(window.seek(SeekFrom::Current(-64)).await.unwrap(), 0);
        assert!(window.seek(SeekFrom::Current(-1)).await.is_err());
    }
}

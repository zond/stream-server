//! Sources a test can hold in its hand: bytes in memory, and a wrapper
//! that counts what was asked of the source underneath.
//!
//! **Not behind `#[cfg(test)]`.** What [`CountingSource`] is for is the
//! claim the whole design rests on -- that reading a container's index
//! reads the index and not the film -- and that claim is made by the
//! integration tests as well as the unit ones. A source that only existed
//! in unit builds would have the end-to-end tests proving nothing about it.
//! Nothing here reads a file, opens a socket or writes a byte, so carrying
//! it in a release build costs a few hundred bytes of text and buys one
//! definition instead of two.

use super::{ByteSource, ReadHint, SourceReader};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// Bytes in memory as a source: what a translator's tests index instead of
/// standing up a torrent.
pub struct MemorySource {
    name: String,
    bytes: Vec<u8>,
    /// Whether an opened reader stops at its hint. A [`ReadHint`] is a
    /// bound a reader *may* end at and not one it must, so both answers
    /// are conforming -- and what reads from a source that carries on is
    /// whatever is reading it. See [`MemorySource::reading_past_the_hint`].
    stops_at_the_hint: bool,
}

impl MemorySource {
    pub fn new(name: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            bytes: bytes.into(),
            stops_at_the_hint: true,
        }
    }

    /// The same source, with readers that run on to the end of it whatever
    /// they were hinted -- a torrent's file handle is one of these, since
    /// the engine spends the hint on the swarm's lookahead and not on where
    /// the reader stops. A caller that must not read past a span is the one
    /// that has to keep it inside: [`crate::sources::MemberReader`] does,
    /// with the run of the extent it is in.
    pub fn reading_past_the_hint(mut self) -> Self {
        self.stops_at_the_hint = false;
        self
    }
}

#[async_trait::async_trait]
impl ByteSource for MemorySource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn describe(&self) -> String {
        self.name.clone()
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let Ok(offset) = usize::try_from(offset) else {
            return Ok(0);
        };
        if offset >= self.bytes.len() {
            return Ok(0);
        }
        let take = buf.len().min(self.bytes.len() - offset);
        buf[..take].copy_from_slice(&self.bytes[offset..offset + take]);
        Ok(take)
    }

    async fn open(&self, offset: u64, hint: ReadHint) -> io::Result<Box<dyn SourceReader>> {
        // Ending at the hint, as an HTTP source does: a reader opened for
        // a header must not be able to read the whole file through, or the
        // tests would let a translator do exactly what this design forbids.
        let hint = if self.stops_at_the_hint {
            hint
        } else {
            ReadHint::REST
        };
        let Some(last) = hint.last_byte(offset, self.len()) else {
            return Ok(Box::new(io::Cursor::new(Vec::new())));
        };
        let span = self.bytes[offset as usize..=last as usize].to_vec();
        Ok(Box::new(io::Cursor::new(span)))
    }
}

/// What a [`CountingSource`] has been asked for, live.
///
/// Read-ats and opens are counted apart because they answer different
/// questions: how many small scattered reads an index cost, and how many
/// bytes a body read actually pulled.
#[derive(Debug, Default)]
pub struct Counts {
    read_at_calls: AtomicU64,
    read_at_bytes: AtomicU64,
    opens: AtomicU64,
    opened_bytes: AtomicU64,
}

impl Counts {
    /// How many times [`ByteSource::read_at`] was called.
    pub fn read_at_calls(&self) -> u64 {
        self.read_at_calls.load(Ordering::Relaxed)
    }

    /// How many bytes those calls returned.
    pub fn read_at_bytes(&self) -> u64 {
        self.read_at_bytes.load(Ordering::Relaxed)
    }

    /// How many readers [`ByteSource::open`] handed out.
    pub fn opens(&self) -> u64 {
        self.opens.load(Ordering::Relaxed)
    }

    /// How many bytes were read through them.
    pub fn opened_bytes(&self) -> u64 {
        self.opened_bytes.load(Ordering::Relaxed)
    }

    /// Every byte this source was asked to produce, either way: the figure
    /// an index bound is stated in.
    pub fn bytes(&self) -> u64 {
        self.read_at_bytes() + self.opened_bytes()
    }
}

/// A source that records every read of the one underneath, so a test can
/// state what reading an index may cost and have the number checked.
pub struct CountingSource {
    inner: Arc<dyn ByteSource>,
    counts: Arc<Counts>,
}

impl CountingSource {
    pub fn new(inner: Arc<dyn ByteSource>) -> Self {
        Self {
            inner,
            counts: Arc::default(),
        }
    }

    /// What has been asked of it. An `Arc`, so a test can keep the tally
    /// after handing the source itself to whatever is being measured.
    pub fn counts(&self) -> Arc<Counts> {
        self.counts.clone()
    }
}

#[async_trait::async_trait]
impl ByteSource for CountingSource {
    fn len(&self) -> u64 {
        self.inner.len()
    }

    fn describe(&self) -> String {
        self.inner.describe()
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read_at(offset, buf).await?;
        self.counts.read_at_calls.fetch_add(1, Ordering::Relaxed);
        self.counts
            .read_at_bytes
            .fetch_add(read as u64, Ordering::Relaxed);
        Ok(read)
    }

    async fn open(&self, offset: u64, hint: ReadHint) -> io::Result<Box<dyn SourceReader>> {
        let reader = self.inner.open(offset, hint).await?;
        self.counts.opens.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(CountingReader {
            reader,
            counts: self.counts.clone(),
        }))
    }
}

/// The reader half of the tally: a body read is counted as it is read, not
/// as it is opened, because what a test asks is how many bytes actually
/// left the source.
struct CountingReader {
    reader: Box<dyn SourceReader>,
    counts: Arc<Counts>,
}

impl AsyncRead for CountingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let polled = Pin::new(&mut this.reader).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &polled {
            let read = (buf.filled().len() - before) as u64;
            this.counts.opened_bytes.fetch_add(read, Ordering::Relaxed);
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// A reader ends at its hint, so nothing that opened one for a header
    /// can read the file through it.
    #[tokio::test]
    async fn an_opened_reader_stops_at_the_hint() {
        let source = MemorySource::new("archive.zip", (0..100u8).collect::<Vec<_>>());
        let mut reader = source.open(10, ReadHint::of(4)).await.unwrap();
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, vec![10, 11, 12, 13]);

        let mut rest = source.open(96, ReadHint::REST).await.unwrap();
        let mut read = Vec::new();
        rest.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, vec![96, 97, 98, 99]);
    }

    /// The tally counts what was read rather than what was asked for.
    #[tokio::test]
    async fn the_counts_follow_the_bytes() {
        let counting = CountingSource::new(Arc::new(MemorySource::new(
            "archive.zip",
            (0..100u8).collect::<Vec<_>>(),
        )));
        let counts = counting.counts();

        let mut buf = [0u8; 8];
        assert_eq!(counting.read_at(0, &mut buf).await.unwrap(), 8);
        assert_eq!(counting.read_at(98, &mut buf).await.unwrap(), 2);
        assert_eq!(counts.read_at_calls(), 2);
        assert_eq!(counts.read_at_bytes(), 10);

        let mut reader = counting.open(0, ReadHint::of(20)).await.unwrap();
        let mut read = Vec::new();
        reader.read_to_end(&mut read).await.unwrap();
        assert_eq!(counts.opens(), 1);
        assert_eq!(counts.opened_bytes(), 20);
        assert_eq!(counts.bytes(), 30);
    }
}

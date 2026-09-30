//! **A complete entry of the proxy cache as a [`ByteSource`]**: a finished
//! download, read off `.proxy` with no origin behind it.
//!
//! What lets a pinned link or Drive file play on a device with no network
//! (`docs/design/media-pipeline.md` §2.3). A [`super::ProxySource`] probes
//! its origin before it is a source at all, which is exactly what cannot
//! happen offline; a download that is whole on the disk needs no probe,
//! and every read of it is a lookup of the entry ([`Entry::look_up`]) and
//! its chunks ([`Cached::body`]) -- the same lookup `/proxy` answers a hit
//! with, and `/downloads/{key}/stream` a finished download.
//!
//! A read that finds a chunk gone fails rather than fetching it: there is
//! nothing here to fetch it with. A pin's chunks are not reclaimed, so
//! that is a download unpinned under its reader.
//!
//! [`Cached::body`]: crate::proxy_cache::Cached::body

use super::{ByteSource, ReadHint, SeekableReader, read_filling};
use crate::proxy_cache::Entry;
use bytes::Bytes;
use futures_util::Stream;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

/// A download the proxy cache holds whole.
pub(crate) struct HeldSource {
    /// A viewer's entry, not a quiet one: its reads claim the live entity
    /// and note the playhead, as `/downloads/{key}/stream`'s do.
    entry: Entry,
    total: u64,
    content_type: String,
}

impl HeldSource {
    /// `entry` as a source, when every byte of its one entity is on the
    /// disk; `None` otherwise -- then the origin has to be asked.
    pub(crate) async fn complete(entry: Entry) -> Option<Self> {
        let asked = entry.clone().quiet();
        let facts = tokio::task::spawn_blocking(move || asked.held_facts())
            .await
            .ok()
            .flatten()
            .filter(|facts| facts.complete)?;
        Some(Self {
            entry,
            total: facts.total,
            content_type: facts.content_type,
        })
    }

    /// What the origin labelled the bytes when they were fetched.
    pub(crate) fn content_type(&self) -> &str {
        &self.content_type
    }

    /// The key directory the download is filed under.
    pub(crate) fn key_dir(&self) -> &std::path::Path {
        self.entry.dir()
    }
}

#[async_trait::async_trait]
impl ByteSource for HeldSource {
    fn len(&self) -> u64 {
        self.total
    }

    fn describe(&self) -> String {
        "a download held on this device".to_string()
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if offset >= self.total || buf.is_empty() {
            return Ok(0);
        }
        let mut reader = self.open(offset, ReadHint::of(buf.len() as u64)).await?;
        let want = (buf.len() as u64).min(self.total - offset) as usize;
        read_filling(&mut reader, &mut buf[..want]).await
    }

    async fn open(&self, offset: u64, _hint: ReadHint) -> io::Result<Box<dyn SeekableReader>> {
        Ok(Box::new(HeldReader {
            entry: self.entry.clone(),
            total: self.total,
            pos: offset,
            state: State::Idle,
        }))
    }
}

type Body =
    tokio_util::io::StreamReader<Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>, Bytes>;

/// A reader of the held entity: a lookup from its position and the
/// chunks after it, looked up again after a seek.
struct HeldReader {
    entry: Entry,
    total: u64,
    pos: u64,
    state: State,
}

enum State {
    Idle,
    Looking(Pin<Box<dyn Future<Output = io::Result<Body>> + Send>>),
    Reading(Body),
}

/// The chunks from `pos` to the end, off the disk.
async fn body_from(entry: Entry, pos: u64) -> io::Result<Body> {
    let cached = tokio::task::spawn_blocking(move || entry.look_up(Some(&format!("bytes={pos}-"))))
        .await
        .map_err(io::Error::other)?
        .filter(|cached| cached.complete())
        .ok_or_else(|| io::Error::other("the download is no longer whole on this device"))?;
    let body: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>> = Box::pin(cached.body());
    Ok(tokio_util::io::StreamReader::new(body))
}

impl AsyncRead for HeldReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let this = &mut *self;
            match &mut this.state {
                State::Idle => {
                    if this.pos >= this.total {
                        return Poll::Ready(Ok(()));
                    }
                    this.state = State::Looking(Box::pin(body_from(this.entry.clone(), this.pos)));
                }
                State::Looking(looking) => {
                    let body = ready!(looking.as_mut().poll(cx))?;
                    this.state = State::Reading(body);
                }
                State::Reading(body) => {
                    let before = buf.filled().len();
                    ready!(Pin::new(body).poll_read(cx, buf))?;
                    this.pos += (buf.filled().len() - before) as u64;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl AsyncSeek for HeldReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let to = match position {
            SeekFrom::Start(to) => Some(to),
            SeekFrom::Current(by) => self.pos.checked_add_signed(by),
            SeekFrom::End(by) => self.total.checked_add_signed(by),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "a seek before the start"))?;
        if to != self.pos {
            self.pos = to;
            self.state = State::Idle;
        }
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.pos))
    }
}

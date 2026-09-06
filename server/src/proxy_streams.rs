//! The proxied streams players are holding open, and the one operation that
//! ends one on purpose.
//!
//! A client that wants a player torn down has, until now, had to wait for
//! the read to time out -- and `network-timeout` is deliberately generous,
//! because a slow swarm must not be mistaken for a dead connection. So the
//! wait was tens of seconds of a player that is already unwanted, holding
//! its packet memory, its socket and the engine that socket pins.
//!
//! The missing piece was never the closing, it was the correlation: this
//! server knows its streams by an id it minted, and the client has no way to
//! say which one is *its* player's. So the client mints the name instead. It
//! puts a token of its own in the proxy URL it hands the player (`p=` --
//! see [`crate::routes::proxy`]), every stream opened through such a URL is
//! registered here under that token, and one control call closes every
//! stream bearing it. A pleasant side effect: the count of live streams for
//! a token is the count of players actually attached, which nothing outside
//! this process could work out before.
//!
//! **What this ends, and what it does not.** Closing makes the *read*
//! return: the body yields an error, hyper drops the connection, and the
//! player's demuxer sees its source fail at once instead of at timeout. A
//! demuxer wedged somewhere else -- on a texture handoff, on the audio
//! device -- is not waiting on this read and is untouched by it. And a
//! player that has stopped reading entirely is not polling the body either,
//! so the close is observed when it next reads, or when it goes away.

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// Every proxied stream currently being read, keyed by an id of ours and
/// tagged with the token the client minted for the player reading it.
#[derive(Default)]
pub struct ProxyStreams {
    next_id: AtomicU64,
    live: DashMap<u64, LiveStream>,
}

struct LiveStream {
    /// The client's name for the player this stream belongs to. Several
    /// streams share one: an HLS player is fetching a playlist and its
    /// segments through the same token, and closing means closing all of
    /// them.
    token: String,
    close: oneshot::Sender<()>,
}

impl ProxyStreams {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wraps the origin's byte stream so it can be ended from outside, and
    /// registers it under `token`. Without a token there is nothing to
    /// address the stream by, so it is only wrapped -- the caller gets one
    /// body type either way.
    pub fn attach<S, E>(self: &Arc<Self>, token: Option<String>, inner: S) -> ClosableStream
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let inner = Box::pin(inner.map(|chunk| chunk.map_err(std::io::Error::other)));
        let Some(token) = token else {
            return ClosableStream {
                inner,
                close: None,
                registration: None,
                ended: false,
            };
        };
        let (close, closed) = oneshot::channel();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.live.insert(id, LiveStream { token, close });
        ClosableStream {
            inner,
            close: Some(closed),
            registration: Some(Registration {
                streams: self.clone(),
                id,
            }),
            ended: false,
        }
    }

    /// Ends every live stream carrying `token`, and answers how many that
    /// was. Zero is a perfectly ordinary answer: the player may have
    /// finished, or never have started, and closing is idempotent.
    pub fn close(&self, token: &str) -> usize {
        let ids: Vec<u64> = self
            .live
            .iter()
            .filter(|entry| entry.value().token == token)
            .map(|entry| *entry.key())
            .collect();
        let mut closed = 0;
        for id in ids {
            // A stream that ended between the scan and here has already
            // taken itself out of the map, and is not one we closed.
            if let Some((_, live)) = self.live.remove(&id)
                && live.close.send(()).is_ok()
            {
                closed += 1;
            }
        }
        closed
    }

    /// How many proxied streams are being read right now, over all tokens.
    pub fn live(&self) -> usize {
        self.live.len()
    }
}

/// Takes the stream back out of the registry when it ends, however it ends
/// -- read to completion, closed, or dropped because the player went away.
struct Registration {
    streams: Arc<ProxyStreams>,
    id: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.streams.live.remove(&self.id);
    }
}

/// The origin's bytes, endable from outside.
///
/// A close yields an **error**, not an end of stream: hyper then abandons
/// the connection instead of writing a clean terminator, and the player
/// reads a broken source rather than a file that ended early. For a
/// content-length body a clean end would be a broken source anyway; for a
/// chunked or close-delimited one it would look exactly like the film being
/// over, which is the one thing this must not be mistaken for.
pub struct ClosableStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    close: Option<oneshot::Receiver<()>>,
    registration: Option<Registration>,
    ended: bool,
}

impl Stream for ClosableStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        // The close is polled first and on every poll, so its waker is
        // always the current one: a stream parked on a silent origin is
        // woken by the close itself rather than by the next byte.
        if let Some(close) = this.close.as_mut()
            && Pin::new(close).poll(cx).is_ready()
        {
            this.ended = true;
            this.registration = None;
            return Poll::Ready(Some(Err(std::io::Error::other(
                "proxied stream closed by control request",
            ))));
        }
        let next = this.inner.as_mut().poll_next(cx);
        if matches!(next, Poll::Ready(None)) {
            this.ended = true;
            this.registration = None;
        }
        next
    }
}

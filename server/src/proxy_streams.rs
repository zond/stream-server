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
//! **What this ends is the read *and the token*.** Closing makes the body
//! yield an error, so hyper drops the connection and the player's demuxer
//! sees its source fail at once instead of at timeout. On its own that is
//! not the end of the stream: ffmpeg is started with `reconnect=1`, and a
//! body that broke mid-file is re-fetched through the very same
//! `/proxy/...&p=<token>` URL -- measured, three times over on one live
//! reader, each close answering `{"closed":1}` and each answer followed by
//! a fresh origin fetch at the offset the last one died at. So the token is
//! struck off too: [`ProxyStreams::is_closed`] stays true for it, and
//! `/proxy` refuses any later request bearing it. The pair is what ends a
//! stream -- the read fails, and the retry has nowhere to go.
//!
//! **Ask the player to quit first, then close.** A cancelled demuxer never
//! reaches the reconnect at all (ffmpeg checks its interrupt callback
//! before the retry sleep, before every `url_read` and inside the socket
//! poll), so the close then finds nothing left to close and answers
//! `{"closed":0}` -- which is the quiet, correct outcome. Closing *first*
//! races the cancel, and losing that race is a reconnect provoked on the
//! way out: one more origin connection for a player that is leaving.
//!
//! A demuxer wedged somewhere else -- on a texture handoff, on the audio
//! device -- is not waiting on this read and is untouched by it. And a
//! player that has stopped reading entirely is not polling the body either,
//! so the close is observed when it next reads, or when it goes away.

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{Stream, StreamExt};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// How many closed tokens are remembered. A token names one player, and a
/// client mints a new one for the next playback, so the only reader a
/// forgotten token could let back in is a reconnect from a player that was
/// closed several hundred playbacks ago -- and ffmpeg reconnects within
/// seconds or not at all. The bound is here so a long-lived server's memory
/// is a function of nothing.
const CLOSED_TOKENS_REMEMBERED: usize = 512;

/// Every proxied stream currently being read, keyed by an id of ours and
/// tagged with the token the client minted for the player reading it --
/// plus the tokens that have been closed, which are refused a new one.
#[derive(Default)]
pub struct ProxyStreams {
    next_id: AtomicU64,
    live: DashMap<u64, LiveStream>,
    /// The most recently closed tokens, oldest first. A `VecDeque` rather
    /// than a set because the bound needs an order to evict by, and it is
    /// scanned rather than hashed because it is at most
    /// [`CLOSED_TOKENS_REMEMBERED`] short strings and the scan happens once
    /// per proxied request, not once per byte.
    closed: Mutex<VecDeque<String>>,
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
    ///
    /// `None` means the token was retired while this stream was being
    /// opened, and the caller must refuse the read rather than serve it.
    /// `/proxy` asks [`ProxyStreams::is_closed`] before it opens the origin,
    /// which is the cheap half -- a refusal that costs the origin nothing --
    /// but that check is over long before the origin's first byte arrives.
    /// The window is the whole time-to-first-byte: measured, a close during
    /// one answered `{"closed":0}` and 3.9 MB was relayed afterwards, under
    /// a token nothing could name any more. So the retirement and the
    /// registration are decided under the same lock, and a stream either
    /// belongs to a token that is still live or is never registered at all.
    pub fn attach<S, E>(self: &Arc<Self>, token: Option<String>, inner: S) -> Option<ClosableStream>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Some(self.register(token)?.wrap(inner))
    }

    /// The decision half of [`ProxyStreams::attach`] on its own: register a
    /// stream under `token` -- or refuse, `None`, exactly as `attach` would
    /// -- and hand back the [`Handle`] that wraps the body later.
    ///
    /// For a body that is not simply the origin's stream. `/proxy` builds a
    /// stitched response as the cached head chained in front of the origin's
    /// tail, with the cache writer over the tail, and two rules pull that
    /// body in two directions. The writer may not be built until the
    /// registration is decided, since a refusal serves no bytes and a writer
    /// started before it would commit chunks off a stream nobody reads. And
    /// the registry's stream must go around the *whole* body: `Chain` never
    /// polls its second stream until the first has ended, so a close during
    /// the head of a body that registered only the tail was answered
    /// `{"closed":1}` while every remaining chunk of the head kept coming off
    /// disk -- the close was polled, but only by the half that was not being
    /// read. Deciding first and wrapping last is how both hold.
    pub fn register(self: &Arc<Self>, token: Option<String>) -> Option<Handle> {
        let Some(token) = token else {
            return Some(Handle {
                close: None,
                registration: None,
            });
        };
        let (close, closed) = oneshot::channel();
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        {
            // The retired list is the lock both sides take, and each takes
            // it before touching `live`: `close` writes the token down and
            // then reads `live`, this reads the token and then writes
            // `live`. There is no interleaving left in which a close sees no
            // stream and the stream sees no closure. A poisoned lock refuses
            // the read, which is the safe direction: it can only cost a
            // player a stream it can ask for again.
            let Ok(retired) = self.closed.lock() else {
                return None;
            };
            if retired.iter().any(|retired| retired == &token) {
                return None;
            }
            self.live.insert(id, LiveStream { token, close });
        }
        Some(Handle {
            close: Some(closed),
            registration: Some(Registration {
                streams: self.clone(),
                id,
            }),
        })
    }

    /// Ends every live stream carrying `token`, and answers how many that
    /// was. Zero is a perfectly ordinary answer: the player may have
    /// finished, or never have started, and closing is idempotent.
    ///
    /// The token is struck off first and for good (see
    /// [`ProxyStreams::is_closed`]), because the count this returns is the
    /// count of reads that were *live*, and the reader that matters most is
    /// the one that is about to come back: a player that reconnects after
    /// its body broke must not be handed a new stream on a name its client
    /// has finished with. Striking off before closing leaves no window in
    /// which the reconnect arrives while the token is still good.
    ///
    /// Striking off and reading `live` happen under one lock, for the
    /// stream that is not back yet but is already on its way: a `/proxy`
    /// request waiting on an origin's headers is registered by
    /// [`ProxyStreams::attach`] under that same lock, so it is either
    /// counted here or refused there.
    pub fn close(&self, token: &str) -> usize {
        let ids: Vec<u64> = {
            let Ok(mut retired) = self.closed.lock() else {
                return 0;
            };
            Self::remember_closed(&mut retired, token);
            self.live
                .iter()
                .filter(|entry| entry.value().token == token)
                .map(|entry| *entry.key())
                .collect()
        };
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

    /// Whether `token` has been closed, and so must not be given another
    /// stream.
    ///
    /// This is what makes closing stick. ffmpeg's `reconnect=1` re-fetches
    /// an aborted body through the URL it already has, token and all, so
    /// without this the close is a stutter in the playback it was meant to
    /// end. `/proxy` asks this before it opens anything, so a refused
    /// request costs the origin nothing -- and asks again, by way of
    /// [`ProxyStreams::attach`], once the origin has answered, because a
    /// token can be retired while a fetch is in flight.
    pub fn is_closed(&self, token: &str) -> bool {
        self.closed
            .lock()
            .is_ok_and(|closed| closed.iter().any(|closed| closed == token))
    }

    /// Writes `token` down as closed, evicting the oldest once there are
    /// more than [`CLOSED_TOKENS_REMEMBERED`] of them. Takes the list rather
    /// than the lock, because its caller holds that lock across more than
    /// this.
    fn remember_closed(closed: &mut VecDeque<String>, token: &str) {
        if closed.iter().any(|closed| closed == token) {
            return;
        }
        closed.push_back(token.to_string());
        while closed.len() > CLOSED_TOKENS_REMEMBERED {
            closed.pop_front();
        }
    }
}

/// A stream's place in the registry, decided but not yet wrapped around a
/// body (see [`ProxyStreams::register`]). Dropped unwrapped, it takes the
/// stream out of the registry again, as an ended body would.
pub struct Handle {
    close: Option<oneshot::Receiver<()>>,
    registration: Option<Registration>,
}

impl Handle {
    /// The body this registration is for, endable from outside. Everything
    /// the player will read goes inside -- the close is polled only by polls
    /// of what is wrapped, so a part of the body left outside is a part a
    /// close cannot reach.
    pub fn wrap<S, E>(self, inner: S) -> ClosableStream
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        ClosableStream {
            inner: Box::pin(inner.map(|chunk| chunk.map_err(std::io::Error::other))),
            close: self.close,
            registration: self.registration,
            ended: false,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Closing is what retires a token, and it stays retired: the reconnect
    /// this exists to refuse arrives *after* the close, on a registry that
    /// no longer has a live stream to count.
    #[test]
    fn a_closed_token_stays_closed_even_though_nothing_was_live() {
        let streams = ProxyStreams::new();
        assert!(!streams.is_closed("player-one"));
        assert_eq!(streams.close("player-one"), 0);
        assert!(streams.is_closed("player-one"));
        assert!(!streams.is_closed("player-two"));
        // Closing twice is harmless, and does not double the bookkeeping.
        assert_eq!(streams.close("player-one"), 0);
        assert!(streams.is_closed("player-one"));
    }

    /// A stream opened while its token was live but registered after the
    /// close: the fetch was in flight the whole time, which is the window
    /// `/proxy`'s pre-fetch check cannot see. It is refused rather than
    /// registered, so the close that already answered `{"closed":0}` is
    /// still the end of that player's stream.
    #[test]
    fn a_token_retired_while_its_stream_was_opening_is_refused_the_stream() {
        let streams = Arc::new(ProxyStreams::new());
        assert_eq!(streams.close("player-one"), 0, "nothing was live yet");
        let empty = futures_util::stream::empty::<Result<Bytes, std::io::Error>>();
        assert!(
            streams
                .attach(Some("player-one".to_string()), empty)
                .is_none(),
            "the origin answered, and there is nobody left to answer it to"
        );
        assert_eq!(streams.live(), 0, "and nothing was registered");
    }

    /// The bound is a bound: the oldest token is forgotten rather than the
    /// list growing for the life of the process.
    #[test]
    fn only_the_last_few_hundred_closed_tokens_are_remembered() {
        let streams = ProxyStreams::new();
        for n in 0..=CLOSED_TOKENS_REMEMBERED {
            streams.close(&format!("player-{n}"));
        }
        assert!(
            !streams.is_closed("player-0"),
            "the oldest has been evicted"
        );
        assert!(streams.is_closed("player-1"));
        assert!(streams.is_closed(&format!("player-{CLOSED_TOKENS_REMEMBERED}")));
    }
}

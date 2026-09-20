//! **An indexed container, in memory, leased** -- what a `/{fmt}/create`
//! leaves behind and every response body reads from -- and the keyed map
//! that hands them out and sweeps them when nobody has used them.
//!
//! **A session owns no file.** An archive session used to own a download
//! under `<cacheRoot>/.archives` and one extraction per member beside it,
//! and the sweep that took the session unlinked them. What is here instead
//! is the container's [`Index`] and the [`ByteSource`]s it was read from,
//! so what the sweep frees is memory and -- for a torrent -- the stream
//! registration the source holds. For a proxied URL it frees nothing at
//! all: the bytes are the proxy cache's, under its own retention owner,
//! exactly as if the player had fetched the file through `/proxy` itself.
//!
//! A re-index after a sweep is therefore a few small ranged reads, which
//! the proxy cache answers from disk and a torrent from its piece store.
//! That is the whole cost of forgetting one.
//!
//! ## The map
//!
//! A session is created by one request (`/create`) and read by others
//! (`/stream/{key}/...`), so it has to outlive the request that made it,
//! and nothing tells the server when the player is done with it. Until
//! [`Sessions`] the answer was "never": the map was a plain `DashMap` with
//! an `insert` and a `get` and no `remove` anywhere, so every play left
//! behind whatever the session owned -- in those days a downloaded archive
//! on disk -- for the life of the process.
//!
//! [`Sessions`] gives a session a lifetime measured from its last use. A
//! use is a lookup: [`Sessions::get`] hands out a [`Lease`], and a session
//! with a lease outstanding is in use for as long as the lease lives -- a
//! route keeps the lease inside the response body it is streaming, so a
//! player reading for two hours holds the session for two hours -- and the
//! idle clock starts when the last lease is dropped. Sessions idle for
//! longer than [`SESSION_IDLE_TIMEOUT`] are removed by a sweep.
//!
//! The sweep runs on every insert, and from a janitor task the map starts
//! for itself on the first insert made inside a tokio runtime. The task
//! holds only a `Weak` reference, so it ends when the map does and keeps
//! nothing alive.

use super::{Body, Index, Member};
use crate::sources::{ByteSource, MemberView};
use dashmap::DashMap;
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
// tokio's Instant rather than std's so a paused test clock moves the idle
// clock too.
use tokio::time::Instant;

/// How long a session outlives its last use before it is swept.
///
/// A use is a request, or a response body still being read. The clock
/// therefore starts when the player has closed every connection to the
/// session, and what the timeout has to cover is the player that comes
/// back after that: one that fetches by fixed-size range and closes
/// between fetches, paused, or one restarting after an error. Its session
/// key is in the URL it holds and nothing else can mint that key again, so
/// a session swept under it is a failed resume. Ten minutes is long for a
/// pause that stays paused and cheap against what a session costs while it
/// waits -- which, since nothing here owns a file any more, is an index in
/// memory and, for a torrent, nothing at all (see [`SessionSources`]).
pub const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Sessions of one kind, keyed by the string the client uses in URLs.
pub struct Sessions<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for Sessions<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<T> {
    map: DashMap<String, Entry<T>>,
    idle_timeout: Duration,
    janitor_started: AtomicBool,
}

struct Entry<T> {
    value: Arc<T>,
    usage: Arc<Usage>,
}

/// When a session was last used, and how many leases are out on it now.
struct Usage {
    leases: AtomicUsize,
    last_used: Mutex<Instant>,
}

impl Usage {
    fn touch(&self) {
        *self
            .last_used
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Instant::now();
    }

    fn idle_for(&self, now: Instant) -> Duration {
        now.saturating_duration_since(
            *self
                .last_used
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

/// A session handed out by [`Sessions::get`]. Dereferences to the session;
/// while it exists the session cannot be swept, and dropping it restarts the
/// session's idle clock.
pub struct Lease<T> {
    value: Arc<T>,
    usage: Arc<Usage>,
}

impl<T> Lease<T> {
    fn new(entry: &Entry<T>) -> Self {
        entry.usage.leases.fetch_add(1, Ordering::SeqCst);
        entry.usage.touch();
        Self {
            value: entry.value.clone(),
            usage: entry.usage.clone(),
        }
    }

    /// The session behind the lease, for a stream that needs to own it
    /// beyond the lease's own lifetime.
    pub fn shared(&self) -> &Arc<T> {
        &self.value
    }
}

impl<T> Deref for Lease<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> Drop for Lease<T> {
    fn drop(&mut self) {
        self.usage.touch();
        self.usage.leases.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: Send + Sync + 'static> Sessions<T> {
    /// A registry whose sessions are removed once no lease has been out on
    /// them for `idle_timeout`.
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Inner {
                map: DashMap::new(),
                idle_timeout,
                janitor_started: AtomicBool::new(false),
            }),
        }
    }

    /// Store `value` under `key`, replacing a session already there. Expired
    /// sessions are swept first, so a registry nobody looks at between
    /// inserts still does not grow without bound.
    pub fn insert(&self, key: String, value: T) {
        self.inner.sweep(Instant::now());
        self.ensure_janitor();
        self.inner.map.insert(
            key,
            Entry {
                value: Arc::new(value),
                usage: Arc::new(Usage {
                    leases: AtomicUsize::new(0),
                    last_used: Mutex::new(Instant::now()),
                }),
            },
        );
    }

    /// The session under `key`, leased -- see [`Lease`]. No lock on the map
    /// is held once this returns, so the lease may be kept across awaits.
    pub fn get(&self, key: &str) -> Option<Lease<T>> {
        self.inner.map.get(key).map(|entry| Lease::new(&entry))
    }

    /// The session under `key`, leased, created by `make` when there is
    /// none.
    ///
    /// One session per key even when requests race for it, which is the
    /// point: a player's opening is several requests at once on the same
    /// member, and two sessions for it would each do the work the session
    /// exists to do once. `make` runs under the map's own lock, so it is a
    /// constructor and nothing else -- no I/O, no await.
    pub fn get_or_insert_with(&self, key: &str, make: impl FnOnce() -> T) -> Lease<T> {
        self.inner.sweep(Instant::now());
        self.ensure_janitor();
        let entry = self
            .inner
            .map
            .entry(key.to_string())
            .or_insert_with(|| Entry {
                value: Arc::new(make()),
                usage: Arc::new(Usage {
                    leases: AtomicUsize::new(0),
                    last_used: Mutex::new(Instant::now()),
                }),
            });
        Lease::new(&entry)
    }

    /// The first session `matches` accepts, leased. For finding a session
    /// that already holds what a new one would otherwise fetch again.
    pub fn find(&self, mut matches: impl FnMut(&T) -> bool) -> Option<Lease<T>> {
        self.inner
            .map
            .iter()
            .find(|entry| matches(&entry.value))
            .map(|entry| Lease::new(&entry))
    }

    pub fn len(&self) -> usize {
        self.inner.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.map.is_empty()
    }

    /// Remove every session with no lease out that has been idle for the
    /// timeout, as of `now`. The janitor and every insert call this; a test
    /// may call it with a chosen `now`.
    pub fn sweep(&self, now: Instant) {
        self.inner.sweep(now);
    }

    /// Start the janitor once, and only when there is a runtime to run it
    /// on. Outside a runtime the sweep on insert is all there is, which is
    /// enough for a registry that only ever sees inserts from request
    /// handlers -- those always run inside one.
    fn ensure_janitor(&self) {
        if self.inner.janitor_started.load(Ordering::SeqCst) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if self
            .inner
            .janitor_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let registry: Weak<Inner<T>> = Arc::downgrade(&self.inner);
        // Often enough that a session outlives its timeout by a fraction of
        // it, not so often that the map is walked for nothing.
        let period = (self.inner.idle_timeout / 4).max(Duration::from_secs(1));
        runtime.spawn(async move {
            let mut ticks = tokio::time::interval(period);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticks.tick().await;
                let Some(registry) = registry.upgrade() else {
                    break;
                };
                registry.sweep(Instant::now());
            }
        });
    }
}

impl<T> Inner<T> {
    fn sweep(&self, now: Instant) {
        let idle_timeout = self.idle_timeout;
        // `retain` holds each shard's write lock while it decides, and
        // `get` takes a lease under the shard's read lock, so a session
        // cannot be leased and removed at once.
        self.map.retain(|_, entry| {
            entry.usage.leases.load(Ordering::SeqCst) > 0
                || entry.usage.idle_for(now) < idle_timeout
        });
    }
}

/// One container, indexed: where its bytes come from, what is in it, and
/// which member the `/create` that made it chose.
pub struct TranslatedSession {
    /// What this was made from -- the volume list, or a
    /// `torrent:<hash>/<path>` key. A set is **every** volume's URL and
    /// not the first alone (`routes::archive::set_origin`): two sets can
    /// share a `.part1.rar` and differ after it, and a session found by
    /// the first URL would then serve one set's index over another's
    /// bytes. Two creates naming the same origin are the same session's
    /// business; one naming a different origin under a key already in use
    /// is refused, because every `/{fmt}/stream/{key}/...` after it would
    /// read a different archive (`routes::archive`).
    origin: String,
    /// Where the container's bytes come from -- see [`SessionSources`].
    sources: SessionSources,
    index: Index,
    /// The member `fileIdx`/`fileMustInclude` picked, if the create picked
    /// one: what `/stream/{key}` with no member path redirects to.
    selected: Option<usize>,
}

impl TranslatedSession {
    pub fn new(
        origin: impl Into<String>,
        sources: SessionSources,
        index: Index,
        selected: Option<usize>,
    ) -> Self {
        Self {
            origin: origin.into(),
            sources,
            index,
            selected,
        }
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn sources(&self) -> &SessionSources {
        &self.sources
    }

    /// The member this session was created for, if any.
    pub fn selected(&self) -> Option<&Member> {
        self.selected.and_then(|at| self.index.members.get(at))
    }

    /// The member called `name`.
    pub fn member(&self, name: &str) -> Option<&Member> {
        self.index.find(name).map(|(_, member)| member)
    }

    /// `member` as a file: the extents it is made of, over `sources`. An
    /// error here is a translator that computed an offset outside its
    /// source, which `MemberView::new` refuses to build rather than
    /// leaving for the middle of a film.
    pub fn view(
        &self,
        member: &Member,
        sources: Vec<Arc<dyn ByteSource>>,
    ) -> io::Result<MemberView> {
        let Body::Direct(extents) = &member.body else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not served by range", member.name),
            ));
        };
        MemberView::new(member.name.clone(), sources, extents.clone())
    }
}

/// Where a session's bytes come from, and -- the part that matters -- for
/// how long it holds the way in.
pub enum SessionSources {
    /// Held for the session's life. An HTTP entity read through the proxy
    /// cache owns nothing and registers nothing, so keeping the source is
    /// keeping a URL, a length and a validator.
    Held(Vec<Arc<dyn ByteSource>>),
    /// The files of a torrent the container is made of -- one for an
    /// ordinary archive, the volumes in order for a set -- **opened per
    /// read and dropped with the body**.
    ///
    /// A `TorrentFileSource` registers a stream on its torrent for as long
    /// as it lives (`sources::torrent::TorrentMemberStream`), and that
    /// registration is what tells this server a player is reading: hold
    /// one for the session's ten idle minutes and the reconciler cannot
    /// stop a torrent the viewer left ten minutes ago. So the session
    /// keeps the *index*, which is what was expensive to read, and each
    /// body opens its own sources -- which is a file lookup and a
    /// reconcile per volume, not a fetch.
    ///
    /// The **paths** are kept and not the file indices, because the index
    /// list is the torrent's and a session outlives a restart of it; they
    /// are what `Translator::volumes` answered when the index was read, so
    /// a body reopens the same volumes in the same order the extents were
    /// computed against.
    Torrent {
        info_hash: String,
        paths: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A session's owned state, whose drop the tests observe.
    struct Owned(Arc<AtomicBool>);

    impl Drop for Owned {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn session(dropped: &Arc<AtomicBool>) -> Owned {
        Owned(dropped.clone())
    }

    const TIMEOUT: Duration = Duration::from_secs(600);

    /// A session nobody has looked at since the timeout goes, one with a
    /// lease out stays however long it has been, and the clock counts from
    /// the lease's drop -- when it goes, the session's owned state goes
    /// with it.
    #[tokio::test(start_paused = true)]
    async fn idle_sessions_go_leased_ones_stay() {
        let sessions = Sessions::new(TIMEOUT);
        let idle_dropped = Arc::new(AtomicBool::new(false));
        let leased_dropped = Arc::new(AtomicBool::new(false));
        sessions.insert("idle".into(), session(&idle_dropped));
        sessions.insert("leased".into(), session(&leased_dropped));
        let lease = sessions.get("leased").expect("just inserted");

        tokio::time::advance(TIMEOUT).await;
        sessions.sweep(Instant::now());
        assert!(sessions.get("idle").is_none(), "idle past the timeout");
        assert!(idle_dropped.load(Ordering::SeqCst), "and dropped with it");
        assert!(sessions.get("leased").is_some(), "a lease is a use");
        assert!(!leased_dropped.load(Ordering::SeqCst));

        // Two leases were out (the one above and the check's); the idle
        // clock starts when the last of them goes, not the first.
        drop(lease);
        tokio::time::advance(TIMEOUT - Duration::from_secs(1)).await;
        sessions.sweep(Instant::now());
        assert!(sessions.get("leased").is_some(), "not idle for long enough");
        tokio::time::advance(TIMEOUT).await;
        sessions.sweep(Instant::now());
        assert!(sessions.get("leased").is_none());
        assert!(leased_dropped.load(Ordering::SeqCst));
    }

    /// The janitor removes an expired session with nothing else calling in.
    #[tokio::test(start_paused = true)]
    async fn the_janitor_sweeps_on_its_own() {
        let sessions = Sessions::new(TIMEOUT);
        let dropped = Arc::new(AtomicBool::new(false));
        sessions.insert("k".into(), session(&dropped));
        assert_eq!(sessions.len(), 1);

        // The paused clock advances only as far as the next timer, so this
        // runs the janitor's ticks rather than waiting on them; the bound
        // is there so a janitor that never sweeps fails instead of hanging.
        for _ in 0..32 {
            tokio::time::sleep(TIMEOUT / 4).await;
            if sessions.is_empty() {
                break;
            }
        }
        assert!(sessions.is_empty(), "the janitor never swept");
        assert!(dropped.load(Ordering::SeqCst));
    }

    /// `get_or_insert_with` makes one session per key however many callers
    /// ask at once -- a player's opening is several requests on the same
    /// member -- and leases what it hands back, so a session created for
    /// one request is in use by it.
    #[tokio::test(start_paused = true)]
    async fn racing_callers_get_one_session() {
        let sessions: Sessions<u32> = Sessions::new(TIMEOUT);
        let made = Arc::new(AtomicUsize::new(0));
        let make = || {
            let made = made.clone();
            move || {
                made.fetch_add(1, Ordering::SeqCst);
                7u32
            }
        };

        let first = sessions.get_or_insert_with("k", make());
        let second = sessions.get_or_insert_with("k", make());
        assert_eq!(*first, 7);
        assert_eq!(*second, 7);
        assert_eq!(made.load(Ordering::SeqCst), 1, "one session, not two");
        assert_eq!(sessions.len(), 1);

        // Both leases are uses: the session stays while either is out.
        drop(first);
        tokio::time::advance(TIMEOUT * 2).await;
        sessions.sweep(Instant::now());
        assert_eq!(sessions.len(), 1, "a lease is a use");
        drop(second);
        tokio::time::advance(TIMEOUT * 2).await;
        sessions.sweep(Instant::now());
        assert!(sessions.is_empty());

        // And a key that has been swept is made again rather than missing.
        let again = sessions.get_or_insert_with("k", make());
        assert_eq!(*again, 7);
        assert_eq!(made.load(Ordering::SeqCst), 2);
    }

    /// And the same under a real race: four callers arriving at once on a
    /// key that is not there yet make **one** session between them, because
    /// the map decides who creates it while it holds the key.
    ///
    /// This is the case the API exists for -- a player's opening is several
    /// requests on one member at the same instant -- and the one a
    /// lookup-then-insert pair gets wrong while reading exactly right in a
    /// sequential test. The maker sleeps so that a pair that lets more than
    /// one caller in has them all inside it; the assertion is the count, not
    /// the time.
    #[test]
    fn one_maker_runs_when_callers_race() {
        let sessions: Sessions<u32> = Sessions::new(TIMEOUT);
        let made = Arc::new(AtomicUsize::new(0));
        let start = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let sessions = sessions.clone();
                let made = made.clone();
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    let lease = sessions.get_or_insert_with("k", || {
                        std::thread::sleep(Duration::from_millis(100));
                        made.fetch_add(1, Ordering::SeqCst);
                        7u32
                    });
                    assert_eq!(*lease, 7);
                });
            }
        });
        assert_eq!(made.load(Ordering::SeqCst), 1, "one session, not four");
        assert_eq!(sessions.len(), 1);
    }

    /// `find` leases what it finds, so a session found for reuse is a
    /// session in use.
    #[tokio::test(start_paused = true)]
    async fn find_leases_the_match() {
        let sessions: Sessions<u32> = Sessions::new(TIMEOUT);
        sessions.insert("a".into(), 1);
        sessions.insert("b".into(), 2);
        let found = sessions.find(|value| *value == 2).expect("b");
        assert_eq!(*found, 2);
        tokio::time::advance(TIMEOUT).await;
        sessions.sweep(Instant::now());
        assert!(sessions.get("a").is_none());
        assert!(sessions.get("b").is_some(), "found is leased");
    }
}

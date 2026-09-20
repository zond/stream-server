//! **An indexed container, in memory, leased** -- what a `/{fmt}/create`
//! leaves behind and every response body reads from -- and the keyed map
//! that hands them out and drops them when the viewer has moved on.
//!
//! **A session owns no file.** An archive session used to own a download
//! under `<cacheRoot>/.archives` and one extraction per member beside it,
//! and the sweep that took the session unlinked them. What is here instead
//! is the container's [`Index`] and the [`ByteSource`]s it was read from,
//! so what dropping one frees is memory. For a proxied URL it frees nothing
//! at all: the bytes are the proxy cache's, under its own retention owner,
//! exactly as if the player had fetched the file through `/proxy` itself.
//!
//! A re-index after a drop is therefore a few small ranged reads, which
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
//! ## What ends a session: a *what*, not a *when*
//!
//! [`Sessions`] used to give a session a lifetime measured from its last
//! use: ten idle minutes and it was swept. **That was a clock, and the
//! retention design abolished clocks for exactly this question** -- see
//! `enginefs::retention::live`, which says it plainly: "a stream that has
//! stopped is not a stream that has been replaced. Pausing for an hour
//! changes nothing on the disk; opening something else changes it at
//! once." Torrent pieces and proxy ranges are kept while nothing else has
//! become live and go the moment something has, bounded by the cache
//! budget. A session is the index of the container those bytes are in, and
//! it now has the same life:
//!
//! * A session with a [`Lease`] out is in use and is never taken -- a route
//!   keeps the lease inside the response body it is streaming, so a player
//!   reading for two hours holds the session for two hours, and a cast
//!   receiver reading holds it while it reads.
//! * A session whose container **is** the entity being played is kept
//!   ([`TranslatedSession::is_live`]).
//! * Every other session goes when the live entity moves to something that
//!   is not it. The map does not decide that: [`Sessions::retain`] is
//!   handed the rule by whoever watches the cell, which for this server is
//!   the switch task in `crate::run` -- the same signal the torrent and
//!   proxy owners drop their slack on.
//! * Nothing else ends one. A session left with no lease and no switch
//!   after it survives the process, which is the point: a cast paused
//!   overnight comes back to the session it was reading, and
//!   `/{fmt}/create` is not on the LAN listener, so a receiver that lost
//!   one had no way to make another (`docs/CASTING.md` in xtremio).
//!
//! ## The backstop, and why it is not a timer either
//!
//! A live entity that never moves must not let the map grow without bound,
//! so there is a **cap on entries** ([`SESSION_CAP`]) and the least
//! recently used unleased session is evicted when a new one takes the map
//! over it. A count, not a clock: the thing that must not run away is the
//! number of indexes held, and evicting by *how long ago* a session was
//! read would be the timer coming back in through the window. Nothing here
//! calls `Instant::now()`, and a session that nothing displaces is not
//! displaced by time passing.
//!
//! "Least recently used" is an ordering, and it is kept as one -- a
//! monotonic counter stamped on the map's own uses ([`Usage`]) -- rather
//! than as an instant that could be compared against a duration.

use super::{Body, Index, Member};
use crate::sources::{ByteSource, MemberView};
use dashmap::DashMap;
use enginefs::retention::live::Reading;
use std::io;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// How many sessions one map holds before a new one evicts the least
/// recently used session that has no lease out.
///
/// The backstop and nothing more: what ordinarily ends a session is the
/// viewer opening something else (see the module doc), and this is the
/// bound for the case where they never do. An index is kilobytes and a
/// viewer's real working set is one container, or the handful a set of
/// volumes and a subtitle make; thirty-two distinct containers opened
/// without one single switch of the live entity between them is not a
/// viewing pattern, it is a leak, and the oldest unread of them is the
/// honest thing to drop.
pub const SESSION_CAP: usize = 32;

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
    cap: usize,
    /// The map's own use counter: the ordering eviction is by. Bumped once
    /// per lease taken and once per lease dropped, and never read as a
    /// time.
    uses: Arc<AtomicU64>,
}

struct Entry<T> {
    value: Arc<T>,
    usage: Arc<Usage>,
}

/// Where a session sits in the map's use order, and how many leases are out
/// on it now.
struct Usage {
    leases: AtomicUsize,
    /// The use counter's value at this session's last lease taken or
    /// dropped. **An ordering, not an instant**: it is only ever compared
    /// with another session's, never with a duration.
    last_used: AtomicU64,
    uses: Arc<AtomicU64>,
}

impl Usage {
    fn new(uses: Arc<AtomicU64>) -> Self {
        let usage = Self {
            leases: AtomicUsize::new(0),
            last_used: AtomicU64::new(0),
            uses,
        };
        usage.touch();
        usage
    }

    fn touch(&self) {
        let stamp = self.uses.fetch_add(1, Ordering::SeqCst);
        self.last_used.store(stamp, Ordering::SeqCst);
    }

    fn stamp(&self) -> u64 {
        self.last_used.load(Ordering::SeqCst)
    }

    fn leased(&self) -> bool {
        self.leases.load(Ordering::SeqCst) > 0
    }
}

/// A session handed out by [`Sessions::get`]. Dereferences to the session;
/// while it exists the session cannot be taken, by an eviction or by the
/// live entity moving off it, and dropping it moves the session to the
/// front of the use order.
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
    /// A registry holding at most `cap` sessions -- see [`SESSION_CAP`] for
    /// what the cap is and is not.
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                map: DashMap::new(),
                cap: cap.max(1),
                uses: Arc::new(AtomicU64::new(0)),
            }),
        }
    }

    /// Store `value` under `key`, replacing a session already there, and
    /// hand back a lease on it.
    ///
    /// **Leased, and not merely inserted.** The caller of an insert is
    /// about to use what it inserted, and between the insert and a
    /// following `get` the live entity can move -- a container's first read
    /// is what moves it -- so a session inserted and then looked up again
    /// was a session that could be gone by the time its own maker asked for
    /// it.
    pub fn insert(&self, key: String, value: T) -> Lease<T> {
        let entry = Entry {
            value: Arc::new(value),
            usage: Arc::new(Usage::new(self.inner.uses.clone())),
        };
        let lease = Lease::new(&entry);
        self.inner.map.insert(key, entry);
        self.inner.evict_over_cap();
        lease
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
        let uses = self.inner.uses.clone();
        // The map's lock is dropped before the eviction walk, which takes
        // shard locks of its own.
        let lease = {
            let entry = self
                .inner
                .map
                .entry(key.to_string())
                .or_insert_with(|| Entry {
                    value: Arc::new(make()),
                    usage: Arc::new(Usage::new(uses)),
                });
            Lease::new(&entry)
        };
        self.inner.evict_over_cap();
        lease
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

    /// Drop every session with no lease out that `keep` does not accept.
    ///
    /// **The rule is the caller's**, which is what keeps this map honest
    /// for anything else that ever stores something in one: a `Sessions<T>`
    /// knows what a lease is and knows nothing about what a `T` is for. For
    /// this server the caller is the switch task in `crate::run`, the rule
    /// is [`TranslatedSession::is_live`], and the signal is the one the
    /// retention owners answer -- the live entity moving.
    ///
    /// A lease outranks the rule: a body streaming to a player, or to a
    /// cast receiver, is not taken out from under its reader whatever the
    /// cell says. `retain` holds each shard's write lock while it decides
    /// and [`Sessions::get`] takes its lease under the shard's read lock,
    /// so a session cannot be leased and dropped at once.
    pub fn retain(&self, keep: impl Fn(&T) -> bool) {
        self.inner
            .map
            .retain(|_, entry| entry.usage.leased() || keep(&entry.value));
    }

    pub fn len(&self) -> usize {
        self.inner.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.map.is_empty()
    }
}

impl<T> Inner<T> {
    /// Bring the map back to its cap by dropping unleased sessions, oldest
    /// in the use order first.
    ///
    /// The candidates are read into a list before anything is removed: the
    /// iterator holds shard locks, and a removal taken under it would be a
    /// deadlock rather than an eviction.
    ///
    /// **Whether a session is leased is asked at the removal and not while
    /// the list is being read**, under the shard's write lock, which is
    /// the only place the answer cannot go stale between the asking and
    /// the unlink -- exactly as [`Sessions::retain`] and
    /// [`Sessions::get`] pair. A map whose oldest sessions are all leased
    /// therefore stays over its cap until one of them is let go, which is
    /// the right way round: the cap bounds what is *kept*, and a reader is
    /// never dropped under for it.
    fn evict_over_cap(&self) {
        if self.map.len() <= self.cap {
            return;
        }
        let mut candidates: Vec<(u64, String)> = self
            .map
            .iter()
            .map(|entry| (entry.usage.stamp(), entry.key().clone()))
            .collect();
        candidates.sort_unstable_by_key(|(stamp, _)| *stamp);
        for (_, key) in candidates {
            if self.map.len() <= self.cap {
                break;
            }
            self.map.remove_if(&key, |_, entry| !entry.usage.leased());
        }
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

    /// Whether this session's container is the entity being played -- the
    /// one question [`Sessions::retain`] is given for this map (see the
    /// module doc).
    ///
    /// **Per torrent, not per file of one.** A set's volumes are several
    /// files of one torrent and a body crosses from one to the next as it
    /// reads, so the cell names `part1` for a while and `part2` after it;
    /// a rule that asked for the exact file would drop the session at the
    /// volume boundary of the very container it is reading. The same
    /// reasoning makes a *link*-borne container's rule "any of my sources
    /// is the live one" rather than "the first is".
    ///
    /// A source that reads bytes this server does not retain answers
    /// `false` and cannot be live (`ByteSource::is_live`), which is why
    /// nothing has to special-case one.
    pub fn is_live(&self, reading: &Reading) -> bool {
        match &self.sources {
            SessionSources::Held(sources) => sources.iter().any(|source| source.is_live(reading)),
            SessionSources::Torrent { info_hash, .. } => reading.is_torrent(info_hash),
        }
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
    /// one for the session's life and the reconciler could never stop a
    /// torrent whose container was once opened -- which, now that a
    /// session lives until the viewer opens something else, would be for
    /// as long as they stay on it. So the session
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
    use enginefs::retention::live::{Live, LiveEntity};
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

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

    /// The old idle timeout, so the test that says time does not end a
    /// session can say it in the units the clock was written in.
    const OLD_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

    /// **Time does not end a session.** Nothing reads this one, nothing
    /// leases it and nothing else becomes live, and a day of the clock
    /// goes past: it is still there, because what would take it is a
    /// viewer opening something else and no viewer has.
    ///
    /// The paused cast is the case in the field (`docs/CASTING.md` in
    /// xtremio): the receiver stops reading, its lease goes, and the key
    /// in the URL it holds is one nothing else can mint again -- there is
    /// no `/create` on the LAN listener -- so a session taken from under
    /// it is a `404` with no way back.
    #[tokio::test(start_paused = true)]
    async fn a_session_nothing_reads_outlives_any_clock() {
        let sessions = Sessions::new(SESSION_CAP);
        let dropped = Arc::new(AtomicBool::new(false));
        drop(sessions.insert("paused".into(), session(&dropped)));

        // Advanced in the janitor's old period, so a sweep on a timer
        // would have run its ticks rather than been skipped over by one
        // long jump of the paused clock.
        for _ in 0..(24 * 4) {
            tokio::time::sleep(OLD_IDLE_TIMEOUT).await;
        }
        assert!(sessions.get("paused").is_some(), "a clock took the session");
        assert!(!dropped.load(Ordering::SeqCst));
    }

    /// What does end one: the rule its caller hands to [`Sessions::retain`].
    /// Everything the rule rejects goes, and its owned state with it.
    #[test]
    fn retain_drops_what_the_rule_rejects() {
        let sessions = Sessions::new(SESSION_CAP);
        let kept = Arc::new(AtomicBool::new(false));
        let taken = Arc::new(AtomicBool::new(false));
        drop(sessions.insert("keep".into(), session(&kept)));
        drop(sessions.insert("drop".into(), session(&taken)));

        sessions.retain(|value| Arc::ptr_eq(&value.0, &kept));
        assert!(sessions.get("keep").is_some());
        assert!(!kept.load(Ordering::SeqCst));
        assert!(sessions.get("drop").is_none());
        assert!(taken.load(Ordering::SeqCst), "and dropped with it");
    }

    /// **A lease outranks the rule.** A body streaming to a player, or to
    /// a cast receiver, is not taken out from under its reader whatever
    /// the cell says -- and the session goes on the first rule that runs
    /// after the lease is dropped.
    #[test]
    fn a_leased_session_is_not_taken_by_a_rule_that_rejects_it() {
        let sessions = Sessions::new(SESSION_CAP);
        let dropped = Arc::new(AtomicBool::new(false));
        let lease = sessions.insert("reading".into(), session(&dropped));

        sessions.retain(|_| false);
        assert!(sessions.get("reading").is_some(), "a lease is a use");
        assert!(!dropped.load(Ordering::SeqCst));

        drop(lease);
        sessions.retain(|_| false);
        assert!(sessions.get("reading").is_none());
        assert!(dropped.load(Ordering::SeqCst));
    }

    /// The backstop: over the cap, the least recently used session with no
    /// lease out goes, and a leased one is passed over however old it is.
    #[test]
    fn the_cap_evicts_the_least_recently_used_unleased_session() {
        let sessions: Sessions<u32> = Sessions::new(2);
        drop(sessions.insert("oldest".into(), 1));
        drop(sessions.insert("middle".into(), 2));
        // Used again, so "middle" is now the older of the two and the two
        // are told apart by their order and not by their age.
        drop(sessions.get("oldest"));
        assert_eq!(sessions.len(), 2);

        drop(sessions.insert("newest".into(), 3));
        assert_eq!(sessions.len(), 2);
        assert!(sessions.get("middle").is_none(), "the least recently used");
        assert!(
            sessions.get("oldest").is_some(),
            "used since \"middle\" was"
        );
        assert!(sessions.get("newest").is_some());
    }

    /// **A leased session is passed over by the eviction even when it is
    /// the oldest thing in the map**, which leaves the map over its cap
    /// until the lease goes: the cap is a bound on what is *kept*, and a
    /// reader is never dropped under for it.
    #[test]
    fn the_eviction_passes_over_a_leased_session_and_takes_it_once_it_is_free() {
        let sessions: Sessions<u32> = Sessions::new(1);
        let reading = sessions.insert("reading".into(), 1);
        drop(sessions.insert("other".into(), 2));
        assert!(
            sessions.get("reading").is_some(),
            "a reader was dropped under the cap"
        );
        assert_eq!(sessions.len(), 2, "over the cap, deliberately");

        // And once the lease is gone it is an ordinary candidate, oldest
        // first.
        drop(reading);
        drop(sessions.insert("third".into(), 3));
        assert_eq!(sessions.len(), 1);
        assert!(sessions.get("reading").is_none());
        assert!(sessions.get("other").is_none());
        assert!(sessions.get("third").is_some());
    }

    /// `insert` hands back a lease, so a session cannot be taken between
    /// being made and being used by its maker -- which is exactly the
    /// window a container's first read opens, because that read is what
    /// moves the live entity.
    #[test]
    fn insert_leases_what_it_made() {
        let sessions: Sessions<u32> = Sessions::new(SESSION_CAP);
        let lease = sessions.insert("k".into(), 7);
        sessions.retain(|_| false);
        assert_eq!(*lease, 7);
        assert!(sessions.get("k").is_some(), "taken from under its maker");
    }

    /// `get_or_insert_with` makes one session per key however many callers
    /// ask at once -- a player's opening is several requests on the same
    /// member -- and leases what it hands back, so a session created for
    /// one request is in use by it.
    #[test]
    fn racing_callers_get_one_session() {
        let sessions: Sessions<u32> = Sessions::new(SESSION_CAP);
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
        sessions.retain(|_| false);
        assert_eq!(sessions.len(), 1, "a lease is a use");
        drop(second);
        sessions.retain(|_| false);
        assert!(sessions.is_empty());

        // And a key that has been taken is made again rather than missing.
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
        let sessions: Sessions<u32> = Sessions::new(SESSION_CAP);
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
    #[test]
    fn find_leases_the_match() {
        let sessions: Sessions<u32> = Sessions::new(SESSION_CAP);
        drop(sessions.insert("a".into(), 1));
        drop(sessions.insert("b".into(), 2));
        let found = sessions.find(|value| *value == 2).expect("b");
        assert_eq!(*found, 2);
        sessions.retain(|_| false);
        assert!(sessions.get("a").is_none());
        assert!(sessions.get("b").is_some(), "found is leased");
    }

    fn torrent_session(info_hash: &str) -> TranslatedSession {
        TranslatedSession::new(
            format!("torrent:{info_hash}/film.rar"),
            SessionSources::Torrent {
                info_hash: info_hash.to_string(),
                paths: vec!["film.part1.rar".into(), "film.part2.rar".into()],
            },
            Index {
                members: Vec::new(),
            },
            None,
        )
    }

    /// The rule this map is given: **a session whose container is the live
    /// entity stays, whichever of its files is being read**, and one whose
    /// container is not goes.
    ///
    /// The second half is the volume boundary: a set's parts are separate
    /// files of one torrent and the cell names whichever the body is
    /// inside, so a per-file rule would take the session of the very
    /// container the viewer is watching as it crossed from part one into
    /// part two.
    #[test]
    fn a_switch_keeps_the_live_containers_session_and_takes_the_rest() {
        let sessions = Sessions::new(SESSION_CAP);
        let live = Live::new();
        drop(sessions.insert("aa".into(), torrent_session("aa")));
        drop(sessions.insert("bb".into(), torrent_session("bb")));

        let switch = |live: &Live, entity| {
            live.open(entity, false);
            let reading = live.reading();
            sessions.retain(|value: &TranslatedSession| value.is_live(&reading));
        };

        switch(
            &live,
            LiveEntity::Torrent {
                info_hash: "aa".into(),
                file_idx: 0,
            },
        );
        assert!(sessions.get("aa").is_some(), "its own container is playing");
        assert!(sessions.get("bb").is_none(), "and nothing else is");

        // The body crosses into the second volume: another file of the
        // same torrent, and the same container.
        switch(
            &live,
            LiveEntity::Torrent {
                info_hash: "aa".into(),
                file_idx: 1,
            },
        );
        assert!(sessions.get("aa").is_some(), "the set is one container");

        // A proxied body opens: nothing of this torrent is playing now.
        switch(&live, LiveEntity::Proxy { dir: "/one".into() });
        assert!(sessions.get("aa").is_none());
    }
}

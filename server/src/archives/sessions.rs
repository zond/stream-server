//! Keyed sessions that go away when nobody has used them for a while.
//!
//! An archive or NZB session is created by one request (`/create`) and read
//! by others (`/stream/{key}/...`), so it has to outlive the request that
//! made it and nothing tells the server when the player is done with it.
//! Until this module the answer was "never": both session maps were plain
//! `DashMap`s with an `insert` and a `get` and no `remove` anywhere, so every
//! play left behind whatever the session owned -- a downloaded archive on
//! disk, a pool of open NNTP connections -- for the life of the process.
//!
//! [`Sessions`] gives a session a lifetime measured from its last use. A use
//! is a lookup: [`Sessions::get`] hands out a [`Lease`], and a session with a
//! lease outstanding is in use for as long as the lease lives -- a route
//! keeps the lease inside the response body it is streaming, so a player
//! reading for two hours holds the session for two hours -- and the idle
//! clock starts when the last lease is dropped. Sessions idle for longer
//! than the timeout are removed by a sweep, which drops what they owned:
//! dropping an NZB session closes its connections, dropping an archive
//! session deletes its files.
//!
//! The sweep runs on every insert, and from a janitor task the registry
//! starts for itself on the first insert made inside a tokio runtime. The
//! task holds only a `Weak` reference, so it ends when the registry does and
//! keeps nothing alive.

use dashmap::DashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
// tokio's Instant rather than std's so a paused test clock moves the idle clock too.
use tokio::time::Instant;

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

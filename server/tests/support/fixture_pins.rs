//! The pin set a test that seeds its own torrent data has to run under.
//!
//! **Nothing seeds a test fixture.** A test torrent is built by
//! `create_torrent` out of a temporary directory and its pieces are written
//! straight into the piece store; there is no swarm, no tracker and no
//! second process anywhere in this suite. So a piece the server lets go of
//! is a piece that never comes back, and a read of it parks for ever.
//!
//! The server lets go of them on a timer. Under
//! `ServerConfig::pins: Some(Default::default())` -- "an embedder that keeps
//! a pin record and has named nothing in it" -- the reconciler's retention
//! pass reaches `Engine::reclaim_rest` for every torrent nobody is reading,
//! and that call takes **every held piece outside every holding extent**: a
//! torrent nobody plays and nobody pinned is slack, all of it. A torrent
//! that was just added has no reader and so no extent, so the first tick
//! after its initial check takes the lot -- measured at about two seconds
//! after the add, which is one `reconcile::RECONCILE_INTERVAL`.
//!
//! Free space has nothing to do with it: `reclaim_rest` consults no volume
//! and no cap, so `pretend_volume_space(root, u64::MAX)` does not save a
//! fixture. Neither does being quick enough -- that is what the tests were
//! doing, and why a test that grew a second slower started failing.
//!
//! `None` is the cure, and it is the only one: it is "nobody said", which
//! sets `PinsUnknown` and makes every file of every torrent read as pinned,
//! so `reclaim_rest` breaks before it takes anything and every slack pass
//! keeps its bytes. Proved by `a_seeded_fixture_is_still_read_after_the_pass
//! _that_would_have_taken_it` in `embed.rs`, whose sibling
//! `an_embedder_that_has_pinned_nothing_keeps_no_torrent_nobody_plays`
//! states the behaviour this steps around.
//!
//! **What it costs, and who may not pay it.** Under `None` a torrent is also
//! *reported* as pinned (`Engine::is_pinned`), which exempts it from idle
//! removal and has the reconciler keep it running. So this is for a test
//! whose subject is bytes -- a range served, an archive member mapped, an
//! ISO member read. A test whose subject is retention, idle pausing, the
//! reconciler or the pin routes themselves must keep the empty record and
//! control the timer itself: under `None` it would be asserting about a
//! cache that may not be touched and a torrent that may not be stopped.
//!
//! **The cost, measured.** "May not be stopped" is not a manner of
//! speaking: `reader_less_fetch.rs` puts a real seeder in front of both
//! settings and reads what each one fetches with nothing reading the
//! torrent. Under the empty record the fetching stops one reconcile
//! interval after the add and never resumes; under `None` it never stops,
//! and unthrottled it took a whole 256 MiB torrent in 2.6 seconds. That
//! file is the one test here that may name `None` as its *subject* rather
//! than as scaffolding, and it seeds no store at all.

use stream_server::ServerConfig;

/// `config` with the pin set unknown, so nothing this test seeded is ever
/// reclaimed. See the module docs for what that means and when it is wrong.
pub fn keep_what_the_fixture_seeded(config: ServerConfig) -> ServerConfig {
    ServerConfig {
        pins: None,
        ..config
    }
}

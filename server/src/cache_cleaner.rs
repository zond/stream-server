//! The cache's figures, and the one call that gives its slack back.
//!
//! **There is no cleaner here any more, and the module keeps the name it
//! was given.** What it held was a filesystem watch, a debounce, a walk of
//! the torrent-data root, an age rule, a size rule and a tier order -- a
//! second owner of every byte under that root, choosing victims from a
//! reading of the disk that was minutes old by the time it acted on it.
//! Every byte now has exactly one owner that knows whether anybody wants
//! it: the torrent engine's retention passes and the proxy cache's, which
//! delete what nobody is playing and nobody is reading at the tick, at a
//! switch, on the bell and at boot. So "clean" is no longer a choice of
//! victims but a request to those owners for their slack
//! ([`drop_slack`]), and the figures a client reads
//! ([`usage`]) come from what the owners say they hold rather than from a
//! walk of what is there.
//!
//! What is left is therefore the wire types ([`CacheUsage`],
//! [`EvictionReport`]) and the two functions behind `GET /cache.json` and
//! `POST /cache/clean`.

use crate::cache_budget::CacheLimit;
use crate::state::AppState;

/// Give back everything nobody is playing and nobody is reading, now, and
/// report what is left -- what `POST /cache/clean` and
/// `ServerHandle::clean_cache_now` answer.
///
/// **"Clean" is no longer a choice of victims.** It used to be a walk of
/// the root that sorted what it found by age and size and evicted until it
/// was under the cap, which meant a user pressing "clean now" could lose
/// the film they were about to resume while a stale one survived on a
/// tie-break. Every byte under the root now has an owner that knows
/// whether anybody wants it, so this asks both of them for their slack --
/// the same passes the tick and the switch run -- and nothing else is
/// touched: a pin is kept until it is unpinned, and the window of the one
/// entity being played is kept until something else is.
///
/// So a clean that frees nothing is the ordinary answer on a device with
/// one film playing and one pinned, and [`EvictionReport::over_limit`] is
/// what says the cache is still over its cap. The cap is restated first
/// ([`crate::cache_budget::publish_now`]), so the owners are sized against
/// the volume as the clean left it. The `limit` reported is the one `GET
/// /cache.json` answers, which is the same rule applied to a different
/// figure: [`usage`]'s total also counts the bytes no owner's count prices
/// -- a torrent held in Error, a directory a previous process left, the
/// staged copies of pieces being written -- and the published cap does
/// not. With any of those on the disk the reported
/// limit is larger than the cap the owners are sized against, by up to
/// those bytes (not when `cacheSize` is what binds), so `over_limit` is
/// the excess of the whole root and not of what the owners hold.
pub(crate) async fn drop_slack(state: &AppState) -> EvictionReport {
    let before = usage(state).await;
    let deleted =
        state.engine.drop_slack().await + state.proxy_cache.retention().drop_slack().await;
    crate::cache_budget::publish_now(state).await;
    let after = usage(state).await;
    EvictionReport {
        total: after.total_bytes,
        protected: after.protected_bytes,
        protected_files: after.protected_files,
        freed: before.total_bytes.saturating_sub(after.total_bytes),
        deleted,
        limit: after.limit_bytes,
        over_limit: after
            .total_bytes
            .saturating_sub(after.limit_bytes.unwrap_or(u64::MAX)),
    }
}

/// What the cache currently occupies against its configured limit
/// ([`CacheUsage`]), from the owners that hold it rather than from a walk
/// of it.
///
/// **No listing of the root, and none of the tree.** The piece store counts
/// its own bytes from the bits it keeps, the proxy cache counts its chunks
/// as they land and as they go, and the only filesystem work left is
/// `enginefs::piece_store::StoreRegistry::unregistered_bytes`: one
/// `read_dir` of the store root and a `stat` of each directory no live
/// store speaks for -- a torrent held in Error, a directory a previous
/// process left -- plus a `stat` of each staged (`.part`) copy a registered
/// store has, because its held bits price complete pieces only. On the
/// device this exists for that is a handful of syscalls where it used to be
/// a `statx` of sixteen thousand files, and the answer is current rather
/// than as old as the walk that produced it.
///
/// What it does not count is a **legacy whole-file download** left by an
/// earlier version of this server, which lives beside the store
/// (`<download dir>/<torrent name>/<file>`) rather than under it: no store
/// speaks for it and nothing here wrote it. It is not converted -- "there
/// is no migration" (see `enginefs::piece_store`) -- but it is deleted:
/// `enginefs::piece_store::sweep_legacy_downloads` removes whatever under
/// the download root is not another component's directory or a session
/// file, at launch, on every boot that is handed a pin set. On a boot with
/// none the sweep does not run, and an unpin asked to delete the file's
/// data is then the only thing that takes one -- as it always is for a
/// copy in an output folder outside the root. Named rather than hidden:
/// adding a category of byte with no deleter is how the disk becomes
/// unbounded again.
///
/// Shared by `routes::cache::cache_usage` (`ServerHandle::cache_usage` and
/// `GET /cache.json`).
pub(crate) async fn usage(state: &AppState) -> CacheUsage {
    let configured = {
        let settings = state.settings.read().await;
        crate::routes::system::cache_size_bytes(settings.cache_size)
    };
    // The root the session was opened on, not `settings.cacheRoot`: the
    // setting is where the data will be after the next start, and the
    // engine is where it is now.
    let limit = CacheLimit {
        configured,
        available: crate::cache_budget::available_space_off_the_reactor(
            state.engine.download_dir.clone(),
        )
        .await,
    };
    let torrents = state.engine.cache_holdings().await;
    let proxy = state.proxy_cache.retention();
    let protection = proxy.protected().await;
    cache_usage(torrents, proxy.occupancy(), protection, limit)
}

/// [`usage`]'s arithmetic, with the `AppState` plumbing taken off: the two
/// owners' readings against the cap.
///
/// Protection is added rather than intersected because the two owners hold
/// two disjoint sets of bytes -- a piece of a torrent is never a chunk of a
/// proxied URL -- and each has already made its own sum over sets that do
/// overlap inside it.
///
/// **And then held to the total, because the two are priced from different
/// bases.** A protection is read off the disk -- the pieces a store holds,
/// the chunks in a live window that are really there -- while the proxy's
/// half of the total is what *this process* booked as it wrote it
/// (`crate::proxy_retention::ProxyRetention::occupancy`). A cache the last
/// process filled is therefore protected bytes against a total that has
/// not heard of them: resume a fully cached film after a restart and the
/// window protects four gigabytes the count reads as nothing. `protected`
/// is documented as part of `total` and read as one -- a client compares
/// the two to decide whether cleaning can help -- so the sum is held to it
/// rather than allowed to exceed it. What removes the divergence is the
/// launch sweep emptying the proxy cache before the session opens, after
/// which every chunk on the disk is one this process wrote.
fn cache_usage(
    torrents: enginefs::CacheHoldings,
    proxy_bytes: u64,
    proxy_protection: crate::proxy_retention::ProxyProtection,
    limit: CacheLimit,
) -> CacheUsage {
    let total_bytes = torrents.total_bytes + proxy_bytes;
    CacheUsage {
        total_bytes,
        limit_bytes: limit
            .effective(total_bytes)
            .filter(|limit| *limit != u64::MAX),
        protected_bytes: (torrents.protected_bytes + proxy_protection.bytes).min(total_bytes),
        protected_files: torrents.protected_files + proxy_protection.entities,
    }
}

/// What the cache currently occupies against its configured limit
/// ([`usage`]), in the one occupancy accounting this repository has
/// ([`enginefs::chunk_store::occupied_bytes`] -- allocated blocks, never
/// apparent length). `serde`-serializable so it crosses the `GET
/// /cache.json` / `ServerHandle::cache_usage` boundary as is.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheUsage {
    /// Occupancy of the torrent store and the proxy cache right now, as
    /// their owners count it. A whole-file download an earlier version of
    /// this server left under the root belongs to neither and is not in it.
    pub total_bytes: u64,
    /// The limit actually enforced, in the same accounting: the smaller of
    /// `settings.cacheSize` and what the volume can give while keeping
    /// [`crate::cache_budget::CACHE_FREE_SPACE_FLOOR`] free. `None` only
    /// when neither caps anything -- `cacheSize` unlimited (JSON `null`)
    /// *and* the volume's free space unreadable.
    pub limit_bytes: Option<u64>,
    /// How much of `total_bytes` nothing may take right now: a pin keeps
    /// it, or it is inside the window of the one stream being played. When
    /// this equals `total_bytes` and the cache is still over `limit_bytes`,
    /// nothing is evictable -- cleaning cannot help until playback moves on
    /// or something is unpinned. Never more than `total_bytes`: it is a
    /// part of that figure and a client reads it as one (see
    /// [`cache_usage`] for the one case where the two readings would
    /// otherwise disagree).
    pub protected_bytes: u64,
    /// How many **files and proxied entities** that is -- not how many
    /// piece files. A pinned file is one, whatever it is stored as, which
    /// is the unit a client can put in front of a user.
    pub protected_files: usize,
}

/// What one [`drop_slack`] call left behind, in occupancy bytes
/// ([`enginefs::chunk_store::occupied_bytes`]) throughout.
/// `serde`-serializable so it crosses the `POST /cache/clean` /
/// `ServerHandle::clean_cache_now` boundary as is.
///
/// The name is the wire's and stays: nothing is evicted here any more --
/// the owners are asked for their slack and report what is left -- but the
/// JSON an app already reads is the same JSON.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EvictionReport {
    /// Occupancy of the cache once the owners had given their slack back,
    /// as [`CacheUsage::total_bytes`] counts it.
    pub total: u64,
    /// How much of `total` nothing may take: a pin keeps it, or it is
    /// inside the window of the one entity being played, or an open body
    /// was framed to deliver it. Same reading as
    /// [`CacheUsage::protected_bytes`], taken after the passes ran.
    pub protected: u64,
    /// How many files and proxied entities that is.
    pub protected_files: usize,
    /// How far `total` fell across the call -- what the two passes really
    /// took off the volume. Zero is the ordinary answer on a device with
    /// one film playing and one pinned: there was no slack to give.
    pub freed: u64,
    /// How many piece files and chunks that took.
    pub deleted: usize,
    /// The limit in force when this answered: the smaller of
    /// `settings.cacheSize` and what the volume could give while keeping
    /// [`crate::cache_budget::CACHE_FREE_SPACE_FLOOR`] free, so on a device
    /// with no `cacheSize` set this is still a number. Worked out over
    /// [`Self::total`], which is not quite the cap the owners are sized
    /// against (see `drop_slack`). `None` only when neither caps
    /// anything -- `cacheSize` unlimited *and* the volume's free space
    /// unreadable, matching [`CacheUsage::limit_bytes`].
    ///
    /// Not a `u64` with 0 for "none": a cap of exactly 0 is reachable -- any
    /// volume whose occupancy plus free space is under the floor gets one --
    /// and it is the tightest cap there is, the opposite of no cap. Read as
    /// the sentinel it silenced [`Self::shortfall_message`] on the one
    /// device that needed it and told a client the cache was unlimited.
    pub limit: Option<u64>,
    /// How far over its cap the cache still is (`total - limit`), and the
    /// only thing [`Self::shortfall_message`] reads. Reported rather than
    /// left to the client to derive, so that "still stuck" is one field
    /// and not a comparison every reader has to get right. With no walk
    /// left to choose victims, this is what says a cache over its cap is
    /// over it because a pin and a live window are holding it.
    pub over_limit: u64,
}

impl EvictionReport {
    /// Whether this call reclaimed anything. Nothing in this process reads
    /// it -- what decides whether a torrent a full disk stopped goes back
    /// to work is the reconciler's own reading of the volume, and never
    /// whether some pass happened to free a byte first -- but it is part of
    /// the type an embedder is handed, and a clean that freed nothing is
    /// still the thing a storage screen wants to know about.
    pub fn made_room(&self) -> bool {
        self.freed > 0
    }

    /// The line to log when the cache is still over the limit, naming what
    /// protection kept -- "cleaned up 0 files, freed 0 bytes" on a phone
    /// that is filling up says nothing about *why*, and the why is always
    /// that the rest of the cache belongs to a live or pinned entity. That
    /// includes a pinned download the user has not unpinned. `None` when
    /// the cache is under its limit (or has none).
    pub fn shortfall_message(&self) -> Option<String> {
        if self.over_limit == 0 {
            return None;
        }
        Some(format!(
            "Cache is {} bytes over its limit after freeing {} bytes from {} files: \
             {} bytes in {} files are protected (a live torrent is writing them, or a pinned \
             download keeps them) and cannot be evicted",
            self.over_limit, self.freed, self.deleted, self.protected, self.protected_files,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{CacheLimit, EvictionReport, cache_usage};
    use enginefs::chunk_store::occupied_bytes;
    use enginefs::piece_store::{FileSpec, PieceLayout, PieceStore, StoreRegistry, StoreRoot};
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";
    const OTHER_HASH: &str = "fedcba9876543210fedcba9876543210fedcba98";

    /// The piece store inside a torrent-data root, exactly as the engine
    /// builds it.
    fn store(root: &Path) -> StoreRoot {
        StoreRoot::in_download_dir(root)
    }

    /// One piece file of `info_hash`, written where the store will find it
    /// and aged, and its path so a test can say whether it is still there.
    ///
    /// Placed through the store's own `PieceStore`, never by spelling the
    /// bucketed layout out here: a test that hardcoded `<hash>/<n/1000>/<n>`
    /// would be a second copy of the very knowledge this layering exists to
    /// keep in one place, and would keep passing after the shape changed
    /// under it.
    fn piece_store_for(root: &Path, info_hash: &str, pieces: u32) -> PieceStore {
        // One file spanning the whole torrent: the layout only has to be
        // wide enough to name the piece, since nothing here reads through it.
        let piece_len = 4u64 << 20;
        let total = piece_len * u64::from(pieces);
        let layout = PieceLayout::new(
            piece_len,
            total,
            [FileSpec {
                len: total,
                padding: false,
            }],
        )
        .unwrap();
        PieceStore::new(store(root).torrent_dir(info_hash), Arc::new(layout))
    }

    /// A file with bytes in it and an mtime, where a usage figure will
    /// find it.
    fn write_aged(path: &Path, bytes: &[u8], age: Duration) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - age)
            .unwrap();
    }

    /// What a usage figure will count this file as. Derived, never
    /// hardcoded: a 4 KiB payload occupies one block on ext4 and rather
    /// more on a filesystem with a bigger allocation unit, so a figure
    /// written as a literal would be a filesystem assumption, not an
    /// assertion.
    fn occupancy(path: &Path) -> u64 {
        occupied_bytes(&std::fs::metadata(path).unwrap())
    }

    /// **A usage figure counts a sparse file by what it allocated**, and
    /// it is the bytes no live store speaks for that make the question
    /// arise at all: those are `stat`ed, where a registered torrent's are
    /// summed from the store's own bits and cannot be sparse.
    ///
    /// librqbit's old filesystem storage pre-allocated every file it wanted
    /// at its full length, so a part-streamed film was a multi-gigabyte
    /// apparent length over a handful of blocks -- a "Storage" screen
    /// reading `len()` reported 17 GB on a device holding 3.85 GB.
    #[cfg(unix)]
    #[test]
    fn usage_reports_occupancy_not_apparent_length_for_a_sparse_file() {
        use std::io::{Seek, SeekFrom, Write};

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let registry = StoreRegistry::new(store(&root));
        // A piece of a torrent no store is registered for: a previous
        // process's, or one this session holds in Error.
        let sparse = piece_store_for(&root, HASH, 1).piece_path(0);
        std::fs::create_dir_all(sparse.parent().unwrap()).unwrap();

        let apparent = 4u64 << 30;
        let mut file = std::fs::File::create(&sparse).unwrap();
        file.set_len(apparent).unwrap();
        file.seek(SeekFrom::Start(apparent - 1)).unwrap();
        file.write_all(&[1]).unwrap();
        drop(file);
        let allocated = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&sparse).unwrap().blocks() * 512
        };
        if allocated >= apparent {
            // No sparse-file support on this filesystem -- nothing to
            // assert about occupancy that would not be a tautology.
            return;
        }

        assert_eq!(
            registry.unregistered_bytes(),
            allocated,
            "occupancy, not len()"
        );
        assert!(
            registry.unregistered_bytes() < 1 << 20,
            "4 GiB of apparent length reported as {} bytes",
            registry.unregistered_bytes()
        );
    }

    /// **The two halves of what the cache holds: a registered store's own
    /// count, and a `stat` of what no store speaks for.**
    ///
    /// A running torrent's occupancy is the bits its store keeps priced by
    /// the layout -- no syscall, so it is current the instant a piece
    /// lands. Nothing keeps bits for a directory no store is registered for
    /// (a previous process's, a torrent held in Error, debris the store
    /// would never have written), and those bytes are on the volume, so
    /// they are `stat`ed: a figure that left them out would read smaller
    /// than the disk does, and a caller told "over the limit and nothing is
    /// evictable" would see two numbers that do not add up.
    ///
    /// The second half is what the owners hand up: protection is theirs to
    /// decide -- a pin, a live window -- and this only has to add the two
    /// owners' answers and cap the total.
    #[tokio::test]
    async fn usage_counts_a_registered_store_and_stats_what_no_store_speaks_for() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("rqbit-downloads");
        let registry = Arc::new(StoreRegistry::new(store(&root)));

        // A registered torrent, holding one piece of its four.
        let layout = Arc::new(
            PieceLayout::new(
                4096,
                4 * 4096,
                [FileSpec {
                    len: 4 * 4096,
                    padding: false,
                }],
            )
            .unwrap(),
        );
        let live = PieceStore::under(Arc::clone(&registry), HASH, layout);
        live.init_for_tests().unwrap();
        let piece = live.piece_path(0);
        std::fs::create_dir_all(piece.parent().unwrap()).unwrap();
        std::fs::write(&piece, [0u8; 4096]).unwrap();
        live.init_for_tests().unwrap();

        // And a directory nothing is registered for, with debris in it that
        // no delete could ever address back.
        let debris = store(&root).torrent_dir(OTHER_HASH).join("0").join("00");
        write_aged(&debris, &[0u8; 4096], Duration::from_secs(60));
        let debris_bytes = occupancy(&debris);

        assert_eq!(registry.occupancy(), 4096, "the bit the store set");
        assert_eq!(
            registry.unregistered_bytes(),
            debris_bytes,
            "and the directory no store speaks for, `stat`ed"
        );

        // `u64::MAX` is what `cache_size_bytes(None)` produces for an
        // unlimited cache -- the only value [`cache_usage`] treats as "no
        // limit". `0` is a distinct, explicit zero-size cap, per
        // `ServerSettings.cache_size` -- `Some(0.0)`, not `None` -- and
        // `CacheUsage` must not blur the two the way
        // `EvictionReport::shortfall_message` does.
        // What the proxy owner would hand up beside it: one chunk of a
        // stream a player is inside.
        const CHUNK: u64 = crate::proxy_cache::CHUNK_BYTES;
        let holdings = enginefs::CacheHoldings {
            total_bytes: registry.occupancy() + registry.unregistered_bytes(),
            protected_bytes: 4096,
            protected_files: 1,
        };
        let usage = cache_usage(
            holdings,
            CHUNK,
            crate::proxy_retention::ProxyProtection {
                bytes: CHUNK,
                entities: 1,
            },
            CacheLimit::configured(u64::MAX),
        );

        assert_eq!(usage.total_bytes, 4096 + debris_bytes + CHUNK);
        assert_eq!(
            usage.protected_bytes,
            4096 + CHUNK,
            "the two owners hold disjoint bytes, so their protections add"
        );
        assert_eq!(usage.protected_files, 2);
        assert_eq!(usage.limit_bytes, None, "u64::MAX means unlimited");

        // Reading usage never deletes anything, unlike a clean pass.
        assert!(piece.is_file());
        assert!(debris.is_file());

        // A `CacheUsage` crosses `GET /cache.json` and `ServerHandle::cache_usage`
        // as JSON, camelCase like every other response type.
        let json = serde_json::to_value(&usage).unwrap();
        assert_eq!(json["totalBytes"], 4096 + debris_bytes + CHUNK);
        assert_eq!(json["protectedBytes"], 4096 + CHUNK);
        assert_eq!(json["protectedFiles"], 2);
        assert_eq!(json["limitBytes"], serde_json::Value::Null);
    }

    /// **A protection is never more than the total it is part of.**
    ///
    /// The two figures are priced from different bases and one of them can
    /// name bytes the other has not heard of. A protection is read off the
    /// disk -- the pieces a store holds, the chunks of a live window that
    /// are really there -- while the proxy's half of the total is what
    /// *this process* booked as it wrote it. So the ordinary case on the
    /// device this is for: the server restarts over a cache full of a film
    /// somebody watched yesterday, a player resumes it, the entity's window
    /// covers four gigabytes of chunks on the disk, and the count says the
    /// proxy cache holds nothing, because this process has written nothing.
    ///
    /// `protected_bytes` is documented as part of `total_bytes` and read as
    /// one -- a client compares them to decide whether cleaning can help --
    /// so it is held to it. What removes the divergence is the launch sweep
    /// emptying the proxy cache before the session opens.
    #[test]
    fn a_protection_is_never_more_than_the_total_it_is_part_of() {
        const CHUNK: u64 = crate::proxy_cache::CHUNK_BYTES;
        let usage = cache_usage(
            enginefs::CacheHoldings::default(),
            // Nothing booked: every chunk under the proxy root was written
            // by the process before this one.
            0,
            crate::proxy_retention::ProxyProtection {
                bytes: 16 * CHUNK,
                entities: 1,
            },
            CacheLimit::configured(u64::MAX),
        );
        assert_eq!(usage.total_bytes, 0);
        assert_eq!(
            usage.protected_bytes, 0,
            "a part of nothing is nothing, whatever the window covers"
        );

        // And with the same chunks booked, the protection is the whole of
        // the figure, which is what "cleaning cannot help" honestly looks
        // like.
        let counted = cache_usage(
            enginefs::CacheHoldings::default(),
            16 * CHUNK,
            crate::proxy_retention::ProxyProtection {
                bytes: 16 * CHUNK,
                entities: 1,
            },
            CacheLimit::configured(u64::MAX),
        );
        assert_eq!(
            (counted.total_bytes, counted.protected_bytes),
            (16 * CHUNK, 16 * CHUNK)
        );
    }

    #[test]
    fn shortfall_message_is_only_for_a_run_that_stayed_over_the_limit() {
        let under = EvictionReport {
            total: 10,
            limit: Some(20),
            ..EvictionReport::default()
        };
        assert_eq!(under.shortfall_message(), None);
        let unlimited = EvictionReport {
            total: u64::MAX,
            limit: None,
            ..EvictionReport::default()
        };
        assert_eq!(unlimited.shortfall_message(), None, "nothing caps it");
        let over = EvictionReport {
            total: 30,
            limit: Some(20),
            over_limit: 10,
            protected: 25,
            protected_files: 3,
            freed: 5,
            deleted: 1,
        };
        assert!(over.shortfall_message().is_some());

        // A cap of exactly 0 is the tightest cap there is, not the absence
        // of one: any volume whose occupancy plus free space is under the
        // floor gets one, and that is the device most in need of the line
        // that says what is holding the cache up.
        let capped_at_nothing = EvictionReport {
            total: 4096,
            protected: 4096,
            protected_files: 1,
            limit: Some(0),
            over_limit: 4096,
            ..EvictionReport::default()
        };
        assert!(
            capped_at_nothing.shortfall_message().is_some(),
            "a cap of 0 is a cap, and this run is over it"
        );
    }

    /// The shape the two sentinels cross the wire in. `POST /cache/clean`,
    /// `ServerHandle::clean_cache_now` and the FFI call behind the app's
    /// storage screen all read this JSON, and the app distinguishes "no cap"
    /// from "a cap of nothing" by exactly this difference -- so it is pinned
    /// here rather than left to serde's defaults being what one assumes.
    #[test]
    fn the_reported_limit_crosses_as_null_or_a_number_never_a_sentinel() {
        let json = |limit| {
            serde_json::to_value(EvictionReport {
                limit,
                ..EvictionReport::default()
            })
            .unwrap()["limit"]
                .clone()
        };
        assert_eq!(json(None), serde_json::Value::Null, "no cap at all");
        assert_eq!(json(Some(0)), serde_json::json!(0), "a cap of nothing");
        assert_eq!(json(Some(1024)), serde_json::json!(1024));

        // And the shortfall beside it, camelCase like the rest: it is what a
        // client reads to know the cache is still stuck.
        let over = serde_json::to_value(EvictionReport {
            over_limit: 4096,
            ..EvictionReport::default()
        })
        .unwrap();
        assert_eq!(over["overLimit"], serde_json::json!(4096));

        // And back, since the same type is what a library embedder reads.
        for limit in [None, Some(0), Some(1024)] {
            let report = EvictionReport {
                limit,
                ..EvictionReport::default()
            };
            let round_tripped: EvictionReport =
                serde_json::from_value(serde_json::to_value(&report).unwrap()).unwrap();
            assert_eq!(round_tripped, report);
        }
    }

    /// A clean is worth reporting as having done something only when it
    /// actually reclaimed bytes. Nothing in this process reads the answer
    /// -- what decides whether a torrent a full disk stopped goes back to
    /// work is the reconciler's own reading of the volume, never whether
    /// some pass happened to free a byte first -- but it is part of the
    /// type an embedder is handed, and "clean freed nothing" is the answer
    /// a storage screen has to be able to tell from "clean freed
    /// something". A run that freed nothing still says what protection
    /// held instead.
    #[test]
    fn a_clean_made_room_only_when_it_actually_reclaimed_something() {
        let freed_nothing = EvictionReport {
            total: 4096,
            protected: 4096,
            protected_files: 1,
            limit: Some(1024),
            over_limit: 3072,
            ..EvictionReport::default()
        };
        assert!(!freed_nothing.made_room());
        assert!(
            freed_nothing.shortfall_message().is_some(),
            "and the run says what protection held instead"
        );

        let freed_something = EvictionReport {
            total: 1024,
            freed: 4096,
            deleted: 1,
            limit: Some(2048),
            ..EvictionReport::default()
        };
        assert!(freed_something.made_room());
        assert!(freed_something.shortfall_message().is_none());
    }
}

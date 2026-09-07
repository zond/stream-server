//! Bytes through the torrent storage, and the one question a client asks of
//! them.
//!
//! # What the question is
//!
//! "Is this server using your connection while you are not watching?" Serving
//! a peer counts. A background offline download counts. Verifying a piece that
//! download just fetched counts, because it is part of that download. One
//! meaning, and no taxonomy of who is at the other end -- what a viewer is
//! being told about is the connection, not the peer.
//!
//! # Why it is counted at the storage
//!
//! Every one of those crosses [`librqbit::storage::TorrentStorage`]: a peer's
//! request and the player's own read both arrive as `pread_exact`, and a
//! download's bytes leave as `pwrite_all` or `pwrite_all_vectored`. So rather
//! than a counter per source -- one in the peer loop, one on the download
//! path, one wherever hashing reads -- there is one wrapper
//! ([`CountingStorageFactory`]) around whatever storage the session was
//! given. Today that is librqbit's own `FilesystemStorage`; when
//! [`crate::piece_store`] is wired in it will be that instead, and the
//! wrapper does not have to know which, because it wraps the trait.
//!
//! It counts what the session's **default** storage moves. An add that brings
//! a storage factory of its own bypasses the default and is not counted --
//! which is not the hole it looks like: at the rev this crate pins, a torrent
//! that is persisted at all has to come back on the session default
//! (`StorageFactory::ensure_persistable`), so the storage this server means to
//! run everything on is the one this wrapper is wrapped around.
//!
//! # A wrapper answers for what it wraps, or it answers instead of it
//!
//! Both traits have defaulted methods, and a wrapper that leaves one alone
//! does not "inherit" it -- it substitutes the default's answer for the real
//! storage's. That is not hypothetical: it is the bug that was just fixed in
//! rqbit's own `timing`, `slow` and `write_through_cache` middlewares, where a
//! `has_piece` the storage underneath answered "no" to came back as the
//! default "yes, and I would know otherwise". So every defaulted method of
//! both traits is forwarded here, with one deliberate exception --
//! `StorageFactory::create_and_init`, whose default is `create()` followed by
//! `init()`. Forwarding *that* to the underlying factory would build the
//! underlying storage and hand it over unwrapped, which is the one method a
//! wrapper must let the default compose. The list is not checked by reading
//! it: `every_defaulted_storage_method_reaches_the_storage_underneath` and
//! `the_factory_answers_for_the_one_it_wraps` below drive each defaulted
//! method against a storage that answers the opposite of its default, so a
//! forwarding that goes missing fails a test instead of quietly answering.

use std::io::IoSlice;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use librqbit::storage::{StorageFactory, StorageFactoryExt, TorrentStorage};

/// The bytes torrent storage has moved since this process started.
///
/// Two counters, shared by every storage the factory creates. Sharing is the
/// point rather than a collision to avoid: what the light needs is one
/// process-wide total, and "which torrent" is a question nobody asks of it.
///
/// Totals only, never an event per call -- `pread_exact` is on the hot path
/// (it is how a peer is served and how a piece is hashed), so what happens
/// there is one relaxed `fetch_add`. Relaxed is the whole ordering these
/// need: nothing is published through them, and the only reader compares a
/// total against an earlier reading of the same total.
#[derive(Debug, Default)]
pub struct StorageTraffic {
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

impl StorageTraffic {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Counters nothing ever increments, for a backend that does not meter its
    /// storage.
    ///
    /// The signal built on these is a claim that we are using someone's
    /// connection, so a backend that cannot support the claim must not make
    /// it: totals that never grow read as "nothing is moving" and the light
    /// stays dark. One shared instance because there is nothing to
    /// distinguish two of them.
    pub fn uncounted() -> Arc<Self> {
        static UNCOUNTED: std::sync::OnceLock<Arc<StorageTraffic>> = std::sync::OnceLock::new();
        UNCOUNTED.get_or_init(StorageTraffic::new).clone()
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    fn add_read(&self, bytes: usize) {
        self.bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn add_written(&self, bytes: usize) {
        self.bytes_written
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

/// Wraps a storage factory so that every storage it builds counts what it
/// moves into one shared [`StorageTraffic`].
#[derive(Clone)]
pub struct CountingStorageFactory<U> {
    underlying: U,
    traffic: Arc<StorageTraffic>,
}

impl<U> CountingStorageFactory<U> {
    pub fn new(underlying: U, traffic: Arc<StorageTraffic>) -> Self {
        Self {
            underlying,
            traffic,
        }
    }
}

impl<U: StorageFactory + Clone> StorageFactory for CountingStorageFactory<U> {
    type Storage = CountingStorage<U::Storage>;

    fn create(
        &self,
        shared: &librqbit::ManagedTorrentShared,
        metadata: &librqbit::TorrentMetadata,
    ) -> anyhow::Result<Self::Storage> {
        Ok(CountingStorage {
            underlying: self.underlying.create(shared, metadata)?,
            traffic: self.traffic.clone(),
        })
    }

    // `create_and_init` is deliberately not here: its default is this
    // factory's `create` followed by `init`, and forwarding it to the
    // underlying factory would build the underlying storage directly and hand
    // it over with no counter around it.

    /// What a caller asking this wants to know is what the storage really is,
    /// and counting is not a storage of our own -- so answer for what we wrap.
    fn is_type_id(&self, type_id: std::any::TypeId) -> bool {
        self.underlying.is_type_id(type_id)
    }

    /// Counting changes nothing about where the bytes end up, so the promise
    /// is exactly the wrapped factory's to make or refuse. Getting this wrong
    /// is not a subtle failure: session persistence is on, `update_db` asks
    /// this of every torrent as it is added, and the default is a refusal --
    /// so a wrapper that swallowed the question would fail every add at once.
    fn ensure_persistable(&self) -> anyhow::Result<()> {
        self.underlying.ensure_persistable()
    }

    fn clone_box(&self) -> librqbit::storage::BoxStorageFactory {
        self.clone().boxed()
    }
}

/// One torrent's storage with the byte counters around it.
pub struct CountingStorage<U> {
    underlying: U,
    traffic: Arc<StorageTraffic>,
}

impl<U: TorrentStorage> TorrentStorage for CountingStorage<U> {
    fn init(
        &mut self,
        shared: &librqbit::ManagedTorrentShared,
        metadata: &librqbit::TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.underlying.init(shared, metadata)
    }

    /// Counted after the read returns, not before: a read that failed moved
    /// nothing, or moved an amount nobody can name.
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.underlying.pread_exact(file_id, offset, buf)?;
        self.traffic.add_read(buf.len());
        Ok(())
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.underlying.pwrite_all(file_id, offset, buf)?;
        self.traffic.add_written(buf.len());
        Ok(())
    }

    /// One call, not two: this is a Piece payload that wrapped around the
    /// peer's ring buffer, and the storage underneath gets it whole. Counting
    /// the length it reports rather than the lengths handed in keeps the two
    /// answers from ever disagreeing.
    fn pwrite_all_vectored(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        let written = self.underlying.pwrite_all_vectored(file_id, offset, bufs)?;
        self.traffic.add_written(written);
        Ok(written)
    }

    fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
        self.underlying.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        self.underlying.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.underlying.ensure_file_length(file_id, length)
    }

    /// The successor keeps counting into the same totals: pausing a torrent
    /// is what this is for, and a paused torrent that is resumed must not
    /// come back as storage nobody is watching.
    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(CountingStorage {
            underlying: self.underlying.take()?,
            traffic: self.traffic.clone(),
        }))
    }

    fn on_piece_completed(
        &self,
        piece_index: librqbit_core::lengths::ValidPieceIndex,
    ) -> anyhow::Result<()> {
        self.underlying.on_piece_completed(piece_index)
    }

    fn has_piece(
        &self,
        piece_index: librqbit_core::lengths::ValidPieceIndex,
    ) -> anyhow::Result<bool> {
        self.underlying.has_piece(piece_index)
    }
}

/// The stretch of time "traffic is moving" is judged over.
///
/// A counter that has not grown since the last reading is the measurement --
/// a single sample of a total is not a rate, and there is nothing else here
/// to take a rate from. Five seconds is short enough that the answer is about
/// now, and long enough to cover the gaps a live connection has anyway: a
/// peer that requests a block every couple of seconds, or a download between
/// two chunks, is inside one window and keeps the light lit.
///
/// What it costs is honesty at the edges, and that is the right side to be
/// on. A torrent that is stalled -- connected, announcing, waiting on peers
/// -- moves no bytes and reads as idle here, because this is a light about
/// traffic and stalled traffic is no traffic. And nothing can be said at all
/// until one window has closed.
pub const TRAFFIC_WINDOW: Duration = Duration::from_secs(5);

/// How long a window may stretch before its verdict is thrown away rather
/// than reported.
///
/// The window is closed by whoever asks, so its length is really the caller's
/// polling interval, and a caller can stop asking: an app is backgrounded, or
/// a phone suspends the whole process mid-download. What comes back is a
/// window of minutes that nobody observed the middle of -- long enough that
/// "nothing was playing" is a claim about a stretch of time we watched a
/// vanishing fraction of. Past this bound the reading is used as a fresh
/// baseline and the answer is "not moving" until a window of ordinary length
/// closes on top of it.
pub const TRAFFIC_WINDOW_STALE_AFTER: Duration = Duration::from_secs(20);

const _: () = assert!(
    TRAFFIC_WINDOW.as_secs() < TRAFFIC_WINDOW_STALE_AFTER.as_secs(),
    "a window of the ordinary length must not be stale the moment it closes"
);

/// What a client's activity light is: traffic over the last window, and
/// whether anything was playing while it moved.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackgroundTraffic {
    /// Bytes moved over the last closed window and nothing was playing over
    /// it or since -- "we are using your connection while you are not
    /// watching". This is the whole signal; the fields below exist so the
    /// answer can be explained rather than only shown.
    pub active: bool,
    /// Whether bytes moved over that window at all, player or not.
    pub moving: bool,
    /// Whether a player is reading from this server as this is answered.
    pub playing: bool,
    /// Totals since the process started, not per window: what a rate would be
    /// computed from, offered so a caller can compute one rather than
    /// re-deriving these.
    pub bytes_read: u64,
    pub bytes_written: u64,
    /// The window `moving` was judged over ([`TRAFFIC_WINDOW`]), in seconds.
    pub window_secs: u64,
}

/// The state one traffic verdict is kept in between readings.
///
/// Shared, not per caller: two callers asking must get the same answer, and a
/// per-caller delta would have each of them consuming windows the other never
/// sees. A call inside the current window returns the standing verdict
/// unchanged -- which is also what keeps a light from flickering at the
/// caller's polling rate rather than at the window's.
#[derive(Debug, Default)]
pub struct TrafficWindow {
    state: parking_lot::Mutex<WindowState>,
}

#[derive(Debug, Default)]
struct WindowState {
    /// False until a first reading has been taken. Nothing can be said before
    /// that: one sample of a total says only what the total is.
    started: bool,
    /// When the standing verdict's window closed, and the totals then.
    at_secs: u64,
    bytes_read: u64,
    bytes_written: u64,
    /// The standing verdict: the counters grew over that window.
    moved: bool,
    /// Nothing was seen playing at any observation of that window.
    was_quiet: bool,
    /// Whether anything has been seen playing since it closed.
    seen_playing: bool,
}

impl TrafficWindow {
    /// Read the counters against the last reading and answer the light's
    /// question. `playing` is whether a player is reading right now (see
    /// `StreamActivitySnapshot::playback_is_live`), and it is judged over the
    /// same window as the traffic: a window is "not watched" only if no
    /// observation of it, or of the time since, found a player.
    ///
    /// That is deliberately the pessimistic reading, and it is what keeps the
    /// light from blaming the viewer's own playback on the background. The
    /// bytes a player's read moved are still in the counters after they stop
    /// watching, so a verdict that asked "is anything playing *now*" would
    /// light up for one window every time playback ends. The cost is the
    /// other direction: after playback stops, the light can take up to two
    /// windows to come on for traffic that really is ours. Claiming nothing
    /// for ten seconds is a smaller error than claiming something untrue.
    pub fn sample(
        &self,
        now_secs: u64,
        traffic: &StorageTraffic,
        playing: bool,
    ) -> BackgroundTraffic {
        let bytes_read = traffic.bytes_read();
        let bytes_written = traffic.bytes_written();

        let mut state = self.state.lock();
        state.seen_playing |= playing;

        let elapsed = now_secs.saturating_sub(state.at_secs);
        if !state.started || elapsed >= TRAFFIC_WINDOW.as_secs() {
            // A first reading is a baseline and nothing else, and so is one
            // taken after a gap nobody watched -- see
            // `TRAFFIC_WINDOW_STALE_AFTER`.
            let judged = state.started && elapsed <= TRAFFIC_WINDOW_STALE_AFTER.as_secs();
            state.moved =
                judged && (bytes_read > state.bytes_read || bytes_written > state.bytes_written);
            state.was_quiet = judged && !state.seen_playing;
            state.started = true;
            state.at_secs = now_secs;
            state.bytes_read = bytes_read;
            state.bytes_written = bytes_written;
            // This observation belongs to the window that just opened.
            state.seen_playing = playing;
        }

        BackgroundTraffic {
            active: state.moved && state.was_quiet && !state.seen_playing,
            moving: state.moved,
            playing,
            bytes_read,
            bytes_written,
            window_secs: TRAFFIC_WINDOW.as_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use librqbit_core::lengths::{Lengths, ValidPieceIndex};
    use parking_lot::Mutex;

    const PIECE_LEN: u32 = 16384;

    fn piece(index: u32) -> ValidPieceIndex {
        Lengths::new(PIECE_LEN as u64 * 4, PIECE_LEN)
            .unwrap()
            .validate_piece_index(index)
            .unwrap()
    }

    /// A storage that records what reached it, and answers the defaulted
    /// methods differently from their defaults so that a wrapper which fails
    /// to forward one is caught by the default's answer coming back.
    ///
    /// The same shape as rqbit's own `storage::test_util::Probe`, rewritten
    /// here because that one is `#[cfg(test)]` inside librqbit and so does not
    /// exist for us.
    #[derive(Default)]
    struct Probe {
        completed: Mutex<Vec<u32>>,
        vectored: Mutex<Vec<(u64, usize)>>,
        reads: Mutex<Vec<(usize, u64, usize)>>,
        writes: Mutex<Vec<(usize, u64, usize)>>,
        removed_files: Mutex<Vec<usize>>,
        removed_dirs: Mutex<Vec<std::path::PathBuf>>,
        lengths: Mutex<Vec<(usize, u64)>>,
        fail_reads: bool,
    }

    impl TorrentStorage for Probe {
        fn init(
            &mut self,
            _shared: &librqbit::ManagedTorrentShared,
            _metadata: &librqbit::TorrentMetadata,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
            if self.fail_reads {
                anyhow::bail!("no");
            }
            self.reads.lock().push((file_id, offset, buf.len()));
            Ok(())
        }

        fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
            self.writes.lock().push((file_id, offset, buf.len()));
            Ok(())
        }

        fn pwrite_all_vectored(
            &self,
            _file_id: usize,
            offset: u64,
            bufs: [IoSlice<'_>; 2],
        ) -> anyhow::Result<usize> {
            let len = bufs[0].len() + bufs[1].len();
            self.vectored.lock().push((offset, len));
            Ok(len)
        }

        fn remove_file(&self, file_id: usize, _filename: &Path) -> anyhow::Result<()> {
            self.removed_files.lock().push(file_id);
            Ok(())
        }

        fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
            self.removed_dirs.lock().push(path.to_path_buf());
            Ok(())
        }

        fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
            self.lengths.lock().push((file_id, length));
            Ok(())
        }

        fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
            Ok(Box::new(Probe::default()))
        }

        fn on_piece_completed(&self, piece_index: ValidPieceIndex) -> anyhow::Result<()> {
            self.completed.lock().push(piece_index.get());
            Ok(())
        }

        // The default is "yes, and I would know otherwise". This one knows
        // otherwise, so a wrapper that does not forward is caught by the
        // answer coming back as the default.
        fn has_piece(&self, _piece_index: ValidPieceIndex) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    fn counted(probe: Probe) -> (CountingStorage<Probe>, Arc<StorageTraffic>) {
        let traffic = StorageTraffic::new();
        (
            CountingStorage {
                underlying: probe,
                traffic: traffic.clone(),
            },
            traffic,
        )
    }

    /// Every method of [`TorrentStorage`] that has a default, checked against
    /// the storage underneath. This is the list the module doc is about: a
    /// method missing from the wrapper still compiles and then answers
    /// something plausible of its own.
    #[test]
    fn every_defaulted_storage_method_reaches_the_storage_underneath() {
        let (storage, _) = counted(Probe::default());

        storage.on_piece_completed(piece(1)).unwrap();
        assert_eq!(
            *storage.underlying.completed.lock(),
            vec![1],
            "on_piece_completed did not reach the storage: a store that makes a piece \
             visible there is never told the piece is done"
        );

        assert!(
            !storage.has_piece(piece(1)).unwrap(),
            "has_piece did not reach the storage: it answered the default yes over a \
             piece the storage says is gone"
        );

        let (a, b) = ([1u8; 4], [2u8; 6]);
        let written = storage
            .pwrite_all_vectored(0, 7, [IoSlice::new(&a), IoSlice::new(&b)])
            .unwrap();
        assert_eq!(written, a.len() + b.len());
        assert_eq!(
            *storage.underlying.vectored.lock(),
            vec![(7, a.len() + b.len())],
            "pwrite_all_vectored did not reach the storage whole: the default split it \
             into two pwrite_alls, which is the one call this exists to avoid"
        );
    }

    /// The undefaulted rest, so that the wrapper is transparent and not just
    /// non-lying.
    #[test]
    fn the_rest_of_the_storage_trait_reaches_it_too() {
        let (storage, _) = counted(Probe::default());

        storage.pread_exact(1, 2, &mut [0u8; 3]).unwrap();
        assert_eq!(*storage.underlying.reads.lock(), vec![(1, 2, 3)]);
        storage.pwrite_all(4, 5, &[0u8; 6]).unwrap();
        assert_eq!(*storage.underlying.writes.lock(), vec![(4, 5, 6)]);
        storage.remove_file(7, Path::new("a")).unwrap();
        assert_eq!(*storage.underlying.removed_files.lock(), vec![7]);
        storage.remove_directory_if_empty(Path::new("d")).unwrap();
        assert_eq!(
            *storage.underlying.removed_dirs.lock(),
            vec![std::path::PathBuf::from("d")]
        );
        storage.ensure_file_length(8, 9).unwrap();
        assert_eq!(*storage.underlying.lengths.lock(), vec![(8, 9)]);
    }

    #[test]
    fn reads_and_writes_land_in_their_own_counters() {
        let (storage, traffic) = counted(Probe::default());

        storage.pread_exact(0, 0, &mut [0u8; 100]).unwrap();
        storage.pwrite_all(0, 0, &[0u8; 10]).unwrap();
        let (a, b) = ([0u8; 4], [0u8; 6]);
        storage
            .pwrite_all_vectored(0, 0, [IoSlice::new(&a), IoSlice::new(&b)])
            .unwrap();

        assert_eq!(traffic.bytes_read(), 100);
        assert_eq!(
            traffic.bytes_written(),
            20,
            "a vectored write counts both slices, once"
        );
    }

    #[test]
    fn a_read_that_failed_moved_nothing() {
        let (storage, traffic) = counted(Probe {
            fail_reads: true,
            ..Default::default()
        });
        assert!(storage.pread_exact(0, 0, &mut [0u8; 100]).is_err());
        assert_eq!(traffic.bytes_read(), 0);
    }

    #[test]
    fn a_paused_torrents_successor_keeps_counting() {
        let (storage, traffic) = counted(Probe::default());
        let successor = storage.take().unwrap();
        successor.pwrite_all(0, 0, &[0u8; 42]).unwrap();
        assert_eq!(
            traffic.bytes_written(),
            42,
            "take() handed back a storage with no counter on it, so everything a paused \
             and resumed torrent moves is invisible"
        );
    }

    /// A factory that promises session persistence nothing, so that the
    /// wrapper's refusal can be told from a wrapper that answers for itself.
    #[derive(Clone, Default)]
    struct OpaqueFactory;

    impl StorageFactory for OpaqueFactory {
        type Storage = Box<dyn TorrentStorage>;

        fn create(
            &self,
            _shared: &librqbit::ManagedTorrentShared,
            _metadata: &librqbit::TorrentMetadata,
        ) -> anyhow::Result<Self::Storage> {
            anyhow::bail!("not used")
        }

        fn clone_box(&self) -> librqbit::storage::BoxStorageFactory {
            self.clone().boxed()
        }
    }

    #[test]
    fn the_factory_answers_for_the_one_it_wraps() {
        use librqbit::storage::filesystem::FilesystemStorageFactory;
        let filesystem = std::any::TypeId::of::<FilesystemStorageFactory>();

        let counting =
            CountingStorageFactory::new(FilesystemStorageFactory::default(), StorageTraffic::new());
        assert!(
            counting.is_type_id(filesystem),
            "counting hid what it wraps: a caller asking which storage this is gets the \
             wrapper's own type"
        );
        assert!(
            counting.clone().boxed().is_type_id(filesystem),
            "and it has to survive boxing, which is what the session holds"
        );
        counting
            .ensure_persistable()
            .expect("the filesystem storage promises persistence, so this one does");

        let opaque = CountingStorageFactory::new(OpaqueFactory, StorageTraffic::new());
        let err = format!(
            "{:#}",
            opaque
                .ensure_persistable()
                .expect_err("a factory that promises nothing is still refused through the wrapper")
        );
        assert!(
            err.contains("OpaqueFactory"),
            "the refusal has to name what could not promise, not the wrapper: {err}"
        );
    }
}

#[cfg(test)]
mod window_tests {
    use super::*;

    const WINDOW: u64 = TRAFFIC_WINDOW.as_secs();

    struct Fixture {
        window: TrafficWindow,
        traffic: Arc<StorageTraffic>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                window: TrafficWindow::default(),
                traffic: StorageTraffic::new(),
            }
        }

        fn download(&self, bytes: usize) {
            self.traffic.add_written(bytes);
        }

        fn ask(&self, now: u64, playing: bool) -> BackgroundTraffic {
            self.window.sample(now, &self.traffic, playing)
        }
    }

    #[test]
    fn nothing_can_be_said_from_a_single_reading() {
        let f = Fixture::new();
        f.download(1_000_000);
        let first = f.ask(0, false);
        assert!(
            !first.moving && !first.active,
            "the first reading is a baseline: a total is not a rate, and everything the \
             process ever moved is in it"
        );

        f.download(1);
        assert!(
            f.ask(WINDOW, false).active,
            "the second reading is the rate"
        );
    }

    #[test]
    fn a_counter_that_stopped_growing_puts_the_light_out() {
        let f = Fixture::new();
        f.ask(0, false);
        f.download(1);
        assert!(f.ask(WINDOW, false).active);

        // Nothing moves over the next window.
        let quiet = f.ask(WINDOW * 2, false);
        assert!(!quiet.moving && !quiet.active);
    }

    #[test]
    fn the_verdict_holds_for_the_length_of_a_window() {
        let f = Fixture::new();
        f.ask(0, false);
        f.download(1);
        assert!(f.ask(WINDOW, false).active);

        // Every caller inside the window gets the standing verdict, so the
        // light cannot flicker at the polling rate -- and asking twice does
        // not consume the window the other caller is measuring.
        for now in WINDOW + 1..WINDOW * 2 {
            assert!(f.ask(now, false).active, "verdict changed at {now}");
        }
        assert!(!f.ask(WINDOW * 2, false).active);
    }

    #[test]
    fn traffic_a_player_was_there_for_is_not_background() {
        let f = Fixture::new();
        f.ask(0, true);
        f.download(1_000);

        let playing = f.ask(WINDOW, true);
        assert!(playing.moving, "the bytes moved either way");
        assert!(!playing.active, "but somebody was watching them move");

        // Playback ends here, and the bytes it moved are still in the totals.
        // The window they moved in saw a player, so it is not ours to claim.
        f.download(1_000);
        assert!(
            !f.ask(WINDOW * 2, false).active,
            "the light lit for the window playback was in, one window after it ended"
        );

        // A window that nobody was watching any part of, and traffic in it.
        f.download(1_000);
        assert!(f.ask(WINDOW * 3, false).active);
    }

    #[test]
    fn a_player_seen_since_the_window_closed_puts_it_out_at_once() {
        let f = Fixture::new();
        f.ask(0, false);
        f.download(1);
        assert!(f.ask(WINDOW, false).active);

        let started = f.ask(WINDOW + 1, true);
        assert!(
            !started.active,
            "playback that starts mid-window has to take the light with it, not wait \
             for the window to close"
        );
        assert!(
            !f.ask(WINDOW + 2, false).active,
            "and the rest of that window is spoken for, whatever the next call sees"
        );
    }

    #[test]
    fn a_gap_nobody_watched_starts_the_measurement_over() {
        let f = Fixture::new();
        f.ask(0, false);
        f.download(1);
        assert!(f.ask(WINDOW, false).active);

        // The process was suspended, or the caller stopped asking. Whatever
        // moved in the meantime moved over a stretch we cannot say anything
        // about -- a player could have been in any of it.
        f.download(1_000_000);
        let after_the_gap = f.ask(WINDOW + TRAFFIC_WINDOW_STALE_AFTER.as_secs() + 1, false);
        assert!(
            !after_the_gap.moving && !after_the_gap.active,
            "a window that stretched past the stale bound is a baseline, not a verdict"
        );

        // And it is a baseline, so the next ordinary window works again.
        f.download(1);
        assert!(
            f.ask(
                WINDOW + TRAFFIC_WINDOW_STALE_AFTER.as_secs() + 1 + WINDOW,
                false
            )
            .active
        );
    }

    #[test]
    fn the_totals_are_reported_whatever_the_verdict() {
        let f = Fixture::new();
        f.traffic.add_read(7);
        f.download(11);
        let answer = f.ask(0, false);
        assert_eq!((answer.bytes_read, answer.bytes_written), (7, 11));
        assert_eq!(answer.window_secs, WINDOW);
    }
}

/// The wrapper in a real librqbit session, where the trap it exists for --
/// `ensure_persistable` -- is a live failure rather than a compile error.
#[cfg(test)]
mod librqbit_tests {
    use super::*;
    use librqbit::storage::filesystem::FilesystemStorageFactory;
    use std::time::Instant;

    /// Generous on purpose: it exists so a regression fails instead of
    /// hanging, not as a timing assertion.
    const WAIT_BOUND: Duration = Duration::from_secs(60);

    /// A session as this server runs one -- persistence on, which is what
    /// makes `ensure_persistable` load-bearing -- but with no DHT and no
    /// listener, so it touches no network.
    async fn session_with_persistence(
        dir: &Path,
        traffic: Arc<StorageTraffic>,
    ) -> Arc<librqbit::Session> {
        let output = dir.join("out");
        tokio::fs::create_dir_all(&output).await.unwrap();
        librqbit::Session::new_with_opts(
            output,
            librqbit::SessionOptions {
                dht: None,
                listen: None,
                persistence: Some(librqbit::SessionPersistenceConfig::Json {
                    folder: Some(dir.join("session")),
                }),
                default_storage_factory: Some(
                    CountingStorageFactory::new(FilesystemStorageFactory::default(), traffic)
                        .boxed(),
                ),
                ..Default::default()
            },
        )
        .await
        .expect("session")
    }

    async fn a_torrent_with_content(root: &Path) -> Vec<u8> {
        let src = root.join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        for (name, len) in [("a.bin", 40_000usize), ("b.bin", 24_000)] {
            let payload: Vec<u8> = (0..len).map(|i| (i.wrapping_mul(31) + 7) as u8).collect();
            tokio::fs::write(src.join(name), payload).await.unwrap();
        }
        librqbit::create_torrent(
            &src,
            librqbit::CreateTorrentOptions {
                name: None,
                trackers: Vec::new(),
                piece_length: Some(16384),
            },
            &librqbit::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .expect("create torrent")
        .as_bytes()
        .expect("serialize")
        .to_vec()
    }

    async fn settled(handle: &Arc<librqbit::ManagedTorrent>) -> librqbit::TorrentStats {
        let deadline = Instant::now() + WAIT_BOUND;
        loop {
            let stats = handle.stats();
            if !matches!(
                stats.state,
                librqbit::TorrentStatsState::Initializing { .. }
            ) || Instant::now() > deadline
            {
                return stats;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The one that would break everything at once: `update_db` asks the
    /// storage factory to promise persistence as each torrent is added, and
    /// the default answer is a refusal -- so a wrapper that does not forward
    /// `ensure_persistable` fails every add in a session that persists.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_counted_storage_can_still_be_persisted() {
        let tmp = tempfile::tempdir().unwrap();
        let bytes = a_torrent_with_content(tmp.path()).await;
        let traffic = StorageTraffic::new();
        let session = session_with_persistence(tmp.path(), traffic).await;

        let response = session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes),
                Some(librqbit::AddTorrentOptions {
                    paused: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("a counted default storage must still be persistable");
        assert!(matches!(
            response,
            librqbit::AddTorrentResponse::Added(..)
                | librqbit::AddTorrentResponse::AlreadyManaged(..)
        ));
        assert!(
            tmp.path().join("session/session.json").exists(),
            "the torrent was accepted but nothing was written for a restart to find"
        );
    }

    /// And the counters are really in the path of a live session: librqbit's
    /// own initial check reads every piece of the data that is already there,
    /// through the storage it was given.
    #[tokio::test(flavor = "multi_thread")]
    async fn what_a_real_session_reads_is_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let bytes = a_torrent_with_content(tmp.path()).await;
        let traffic = StorageTraffic::new();
        let session = session_with_persistence(tmp.path(), traffic.clone()).await;

        // Output folder = where the files already are (the torrent's own
        // relative paths sit directly under it), so the initial check finds a
        // complete torrent and reads all of it back to hash it.
        let response = session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes),
                Some(librqbit::AddTorrentOptions {
                    output_folder: Some(tmp.path().join("src").to_string_lossy().into_owned()),
                    // The files are already there; that is the point.
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("add");
        let (librqbit::AddTorrentResponse::Added(_, handle)
        | librqbit::AddTorrentResponse::AlreadyManaged(_, handle)) = response
        else {
            panic!("torrent not added");
        };

        let stats = settled(&handle).await;
        assert_eq!(stats.error, None);
        assert_eq!(
            stats.progress_bytes, stats.total_bytes,
            "the initial check should have found the files whole"
        );
        assert!(
            traffic.bytes_read() >= stats.total_bytes,
            "the hash check read {} bytes through the storage and the counter saw {}",
            stats.total_bytes,
            traffic.bytes_read()
        );
    }
}

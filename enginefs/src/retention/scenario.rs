//! **The scenario harness: a film, a disk, a script of timed reads, and a
//! record of what the policy did to them.**
//!
//! It exists because the retention policy is about to be replaced whole
//! (`docs/read-pattern-retention.md`). The old one cannot be left running
//! beside the new one -- it would have to be the one deciding, which is the
//! thing being replaced -- so the new one is built standalone and proved
//! against scaffolded scenarios before anything is swapped. A harness that
//! is written at the same time as the policy it checks proves nothing about
//! either, so this one is written first and proves itself against the
//! **old** policy, by reproducing a failure the field has already seen:
//! `scenarios::the_second_track_at_the_tail_*`.
//!
//! # Why here, and not under `owner`
//!
//! Every piece of this except the harness itself used to be `owner.rs`'s
//! `mod tests` preamble, where only that module's own tests could reach it.
//! It is a sibling of `owner` now, `pub(crate)` under `cfg(test)`, for one
//! reason: **the thing it has to outlive is `owner` itself**. A fixture
//! that is a child of the module being replaced has to move again at the
//! swap, and a scenario written against it would have to be rewritten with
//! it -- which is the one thing that must not happen to the scenarios,
//! because their whole job is to be run unchanged against both policies and
//! compared.
//!
//! For the same reason the harness reaches the owner through its **public**
//! surface only ([`Retention::install`], [`Retention::reader_on`],
//! [`Reader::note_at`], [`Retention::turn`], [`Retention::pass`]) and never
//! through `State` or the policy. That is a constraint on the harness and
//! not a courtesy: the seam it is allowed to use is the seam the
//! replacement has to be drivable through too.
//!
//! # The clock
//!
//! There is no sleep here and no runtime timer, and there cannot be: a
//! scenario is a list of `(Duration, Step)` beats, the harness holds one
//! `t0` and adds to it, and every call that takes a `now` is handed the
//! scenario's own. The house rule is the reason -- nothing takes
//! `Instant::now()` internally -- and the replacement is the motive: its
//! retention tier is an LRU keyed on `max(fetched_at, last_read_at)`, so a
//! scenario that could not put two reads a measured distance apart could
//! not test it at all. Times are built by adding to `t0` and never by
//! `checked_sub`, which returns `None` on a freshly started process.
//!
//! Each pass is run to completion on a current-thread runtime the harness
//! owns, inside [`tracing::subscriber::with_default`], so the pass's own
//! trace lines are captured on the thread that produced them. That is what
//! lets a scenario assert on the field's evidence in the form the field
//! reported it -- a log line -- rather than on a reconstruction of it.
//!
//! # What a scenario can say
//!
//! A [`Step`] is one of the things that really happens to a cache: a
//! response opens, a response is served some bytes, a response ends, the
//! player says where it is, the swarm delivers what the backend still
//! wants, a retention pass runs. Nothing in the list is a retention
//! concept, on purpose. What comes back is a [`Log`] with, per pass, the
//! windows kept, the windows wanted, the pieces the pass stopped wanting,
//! the pieces that really left the disk and the trace lines the pass
//! emitted -- and, per read, whether it was served, served short, or
//! blocked, and for how long.
//!
//! That split is the point. **Kept, wanted and reclaimed are three
//! different answers**, and the disagreement this harness was built to
//! settle -- whether the field's starvation came from the keep set or the
//! want set -- cannot be settled by a fixture that reports one number.

use std::collections::{BTreeSet, HashMap};
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::piece_store::{Buffering, RetentionPolicy, Share};
use crate::retention::RetentionBudget;
use crate::retention::owner::{
    Backing, Claim, Door, Install, InstallOutcome, Mode, Outcome, Reader, Reading, Retention,
    Trigger,
};

pub(crate) const PIECE: u64 = 1000;

/// The two shapes of driver the owner has to carry, as consts a fake
/// can be built over.
pub(crate) trait Side: Send + Sync + 'static {
    const SHARE: Share;
    const TRIGGER: Trigger;
    const INSTALL: Install;
}

/// The torrent's shape: half the budget shared, the tick as trigger,
/// installed before the reader opens.
pub(crate) struct TorrentSide;
impl Side for TorrentSide {
    const SHARE: Share = Share::Half;
    const TRIGGER: Trigger = Trigger::External;
    const INSTALL: Install = Install::OnOpen;
}

/// The proxy's shape: nothing shared, the delivered byte as trigger,
/// installed on that byte.
pub(crate) struct ProxySide;
impl Side for ProxySide {
    const SHARE: Share = Share::Nothing;
    const TRIGGER: Trigger = Trigger::OnMove {
        passes_per_window: 20,
    };
    const INSTALL: Install = Install::OnDeliveredByte;
}

/// One file: a piece range, the piece length those pieces are of, and the
/// bytes they really hold.
///
/// The piece length and the byte count were [`PIECE`] and `pieces * PIECE`
/// until a scenario had to be about a real film. Both are fields now
/// because the two numbers the field's failure turns on -- a 4 MiB piece
/// and a last piece that is short -- cannot be spelled with either as a
/// constant: [`RetentionPolicy::new`] checks that the four numbers
/// describe the same file and refuses a byte count rounded up to a whole
/// multiple of the piece length.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct FakeDomain {
    pub(crate) file: usize,
    pub(crate) pieces: Range<u32>,
    /// Bytes per piece; every piece is full except possibly the last.
    pub(crate) piece: u64,
    /// A domain no policy can be sized for: `policy` returns the error
    /// the owner has to carry out from under L2.
    pub(crate) broken: bool,
}

pub(crate) fn domain(file: usize, pieces: Range<u32>) -> FakeDomain {
    FakeDomain {
        file,
        pieces,
        piece: PIECE,
        broken: false,
    }
}

/// A file of a stated size at a stated piece length: the shape a scenario
/// about a real film needs, where [`domain`] is the eight-piece toy the
/// owner's own tests are written against.
pub(crate) fn film_domain(file: usize, piece: u64, bytes: u64) -> FakeDomain {
    let pieces = u32::try_from(bytes.div_ceil(piece)).expect("a film of fewer than 4G pieces");
    FakeDomain {
        file,
        pieces: 0..pieces,
        piece,
        broken: false,
    }
}

pub(crate) fn broken(file: usize, pieces: Range<u32>) -> FakeDomain {
    FakeDomain {
        broken: true,
        ..domain(file, pieces)
    }
}

/// A position in the fake's coordinates: which file, and the byte
/// offset into it.
pub(crate) type At = (usize, u64);

pub(crate) type Hook<S> = Box<dyn Fn(&Door<FakeBacking<S>>) + Send + Sync>;

/// An in-memory backing with the knobs the tests need: the disk as a
/// set, recorders for every advertise and reclaim call, a park inside
/// `held`, `advertise` and `reclaim` (a oneshot the test releases, and
/// one it is told through when the park begins), a settable
/// `keeps_everything`, an `advertise` that can fail and a `reclaim`
/// whose blocking closure can die.
pub(crate) struct FakeBacking<S: Side> {
    pub(crate) domains: parking_lot::Mutex<HashMap<usize, FakeDomain>>,
    pub(crate) held: parking_lot::Mutex<BTreeSet<u32>>,
    pub(crate) advertised: parking_lot::Mutex<Vec<(Range<u32>, bool)>>,
    pub(crate) fail_advertise: AtomicBool,
    pub(crate) fail_held: AtomicBool,
    /// How many listings were asked for.
    pub(crate) listings: AtomicU64,
    pub(crate) keeps_everything: AtomicBool,
    /// What [`Backing::is_live`] answers: the entity the fake is
    /// playing right now.
    pub(crate) is_live: AtomicBool,
    /// What [`Backing::epoch`] answers: moved by a test to say that the
    /// backend threw away everything it was told about what to hold
    /// back, as a restart out of an error does.
    pub(crate) epoch: AtomicU64,
    /// The runs each `reclaim` call was handed.
    pub(crate) reclaims: parking_lot::Mutex<Vec<Vec<Range<u32>>>>,
    /// The extent of every `want_all` call, in order.
    pub(crate) wanted_all: parking_lot::Mutex<Vec<Range<u32>>>,
    /// The windows every `want` call was handed, in order: what the
    /// pass ordered the backend to fetch, as against what it kept.
    pub(crate) wanted: parking_lot::Mutex<Vec<Vec<Range<u32>>>>,
    /// **What the backend has selected for download**, which is the half
    /// of librqbit a recorder cannot stand in for.
    ///
    /// `want` is the only order the owner gives the swarm, and a piece it
    /// drops is one no peer is ever asked for. A fake that only *records*
    /// the windows can say what the pass decided and can never say what
    /// the decision cost, so a scenario about a read that starved could
    /// only ever assert on the plan. Here the set is real: seeded with
    /// every piece of every file, as librqbit selects a file it is asked
    /// for, narrowed by [`Backing::want`] and widened by
    /// [`Backing::want_all`], and it is what
    /// [`crate::retention::scenario::Step::Swarm`] delivers from.
    pub(crate) selected: parking_lot::Mutex<BTreeSet<u32>>,
    /// The piece ranges live streams are reading over, as librqbit's
    /// `streams.wanted_ranges` holds them.
    ///
    /// Not a recorder either: **the fork's `drop_pieces` refuses a piece
    /// inside a live stream's lookahead** (see [`Buffering`]), so a piece
    /// the pass drops is not dropped at all while a response is reading
    /// ahead over it, and it is fetched whatever the want-set says. Left
    /// out, this fake would answer "the want-set starved it" to every
    /// question, which is exactly the answer the field disagreed about.
    pub(crate) streams: parking_lot::Mutex<Vec<Range<u32>>>,
    /// What each `want` call really took out of [`Self::selected`]: the
    /// pieces that pass stopped the swarm from ever fetching.
    pub(crate) dropped: parking_lot::Mutex<Vec<Vec<u32>>>,
    /// The runs each `reclaim` call really asked the door about, which
    /// stops at the first `window_now` of `None`.
    pub(crate) asked: parking_lot::Mutex<Vec<Vec<Range<u32>>>>,
    pub(crate) reclaim_panics: AtomicBool,
    pub(crate) park_held: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub(crate) park_advertise: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub(crate) park_reclaim: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    pub(crate) entered: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    /// Run inside `advertise`, with whatever the test wants to try
    /// there.
    pub(crate) on_advertise: parking_lot::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    /// Run inside `reclaim` with the door, before any run is walked.
    pub(crate) on_reclaim: parking_lot::Mutex<Option<Hook<S>>>,
    /// Run between two runs of one reclaim.
    pub(crate) between_runs: parking_lot::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    pub(crate) _side: PhantomData<S>,
}

impl<S: Side> FakeBacking<S> {
    pub(crate) fn new(domains: impl IntoIterator<Item = FakeDomain>) -> Arc<Self> {
        let domains: HashMap<usize, FakeDomain> =
            domains.into_iter().map(|d| (d.file, d)).collect();
        // A torrent whose files have been asked for is selected whole
        // until a pass narrows it; that is the state every pass here
        // starts from.
        let selected: BTreeSet<u32> = domains
            .values()
            .flat_map(|domain| domain.pieces.clone())
            .collect();
        Arc::new(Self {
            domains: parking_lot::Mutex::new(domains),
            held: parking_lot::Mutex::new(BTreeSet::new()),
            advertised: parking_lot::Mutex::new(Vec::new()),
            fail_advertise: AtomicBool::new(false),
            fail_held: AtomicBool::new(false),
            listings: AtomicU64::new(0),
            keeps_everything: AtomicBool::new(false),
            is_live: AtomicBool::new(false),
            epoch: AtomicU64::new(1),
            reclaims: parking_lot::Mutex::new(Vec::new()),
            wanted_all: parking_lot::Mutex::new(Vec::new()),
            wanted: parking_lot::Mutex::new(Vec::new()),
            selected: parking_lot::Mutex::new(selected),
            streams: parking_lot::Mutex::new(Vec::new()),
            dropped: parking_lot::Mutex::new(Vec::new()),
            asked: parking_lot::Mutex::new(Vec::new()),
            reclaim_panics: AtomicBool::new(false),
            park_held: parking_lot::Mutex::new(None),
            park_advertise: parking_lot::Mutex::new(None),
            park_reclaim: parking_lot::Mutex::new(None),
            entered: parking_lot::Mutex::new(None),
            on_advertise: parking_lot::Mutex::new(None),
            on_reclaim: parking_lot::Mutex::new(None),
            between_runs: parking_lot::Mutex::new(None),
            _side: PhantomData,
        })
    }

    pub(crate) fn holds(&self, pieces: impl IntoIterator<Item = u32>) {
        self.held.lock().extend(pieces);
    }

    pub(crate) fn on_disk(&self) -> Vec<u32> {
        self.held.lock().iter().copied().collect()
    }

    /// Park the next call of the named kind: the returned receiver
    /// fires when the pass is inside it, and the sender lets it go.
    pub(crate) fn park(
        slot: &parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        entered: &parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *slot.lock() = Some(release_rx);
        *entered.lock() = Some(entered_tx);
        (entered_rx, release_tx)
    }

    pub(crate) fn park_held(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        Self::park(&self.park_held, &self.entered)
    }

    pub(crate) fn park_reclaim(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        Self::park(&self.park_reclaim, &self.entered)
    }

    pub(crate) fn park_advertise(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        Self::park(&self.park_advertise, &self.entered)
    }

    pub(crate) async fn parked(
        slot: &parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        entered: &parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    ) {
        let release = slot.lock().take();
        if let Some(release) = release {
            if let Some(entered) = entered.lock().take() {
                let _ = entered.send(());
            }
            let _ = release.await;
        }
    }
}

impl<S: Side> Backing for FakeBacking<S> {
    type Key = usize;
    type Position = At;
    type Domain = FakeDomain;
    type Want = usize;
    type Store = ();
    const SHARE: Share = S::SHARE;
    const TRIGGER: Trigger = S::TRIGGER;
    const INSTALL: Install = S::INSTALL;

    async fn resolve(&self, want: usize) -> Option<FakeDomain> {
        self.domains.lock().get(&want).cloned()
    }

    fn governs(domain: &FakeDomain, want: usize) -> bool {
        domain.file == want
    }

    /// The extent in bytes, which is what the production backing answers
    /// too: `Engine::bytes` is the file's PIECE SPAN and not its exact
    /// length -- they differ by up to a piece, 0.03% on the field's film.
    /// Carrying the exact length here would model something the owner never
    /// sees, and a scenario that could tell the two apart would be asserting
    /// a fiction.
    fn bytes(domain: &FakeDomain) -> Option<u64> {
        Some(u64::from(domain.pieces.end - domain.pieces.start) * domain.piece)
    }

    fn extent(domain: &FakeDomain) -> Range<u32> {
        domain.pieces.clone()
    }

    fn policy(
        domain: &FakeDomain,
        budget: u64,
        buffering: Buffering,
    ) -> anyhow::Result<RetentionPolicy> {
        if domain.broken {
            anyhow::bail!("a domain nothing can be sized for");
        }
        RetentionPolicy::new(
            budget,
            domain.piece,
            domain.pieces.clone(),
            Self::bytes(domain).unwrap_or_default(),
            S::SHARE,
            buffering,
        )
    }

    fn position_at(domain: &FakeDomain, offset: u64) -> Option<At> {
        Some((domain.file, offset))
    }

    fn index_of(domain: &FakeDomain, (file, offset): At) -> Option<u32> {
        if file != domain.file {
            return None;
        }
        let last = u64::from(domain.pieces.end - 1);
        Some((u64::from(domain.pieces.start) + offset / domain.piece).min(last) as u32)
    }

    fn keeps_everything(&self, _key: &usize) -> bool {
        self.keeps_everything.load(Ordering::SeqCst)
    }

    fn is_live(&self, _key: &usize) -> bool {
        self.is_live.load(Ordering::SeqCst)
    }

    async fn held(&self, _store: &(), _domain: &FakeDomain) -> Option<BTreeSet<u32>> {
        self.listings.fetch_add(1, Ordering::SeqCst);
        Self::parked(&self.park_held, &self.entered).await;
        if self.fail_held.load(Ordering::SeqCst) {
            return None;
        }
        Some(self.held.lock().clone())
    }

    async fn advertise(&self, pieces: Range<u32>, on: bool) -> anyhow::Result<()> {
        if let Some(hook) = self.on_advertise.lock().as_ref() {
            hook();
        }
        Self::parked(&self.park_advertise, &self.entered).await;
        if self.fail_advertise.load(Ordering::SeqCst) {
            anyhow::bail!("the backend would not change what it advertises");
        }
        self.advertised.lock().push((pieces, on));
        Ok(())
    }

    fn epoch(&self, _store: &()) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    async fn alone(&self, _domain: &FakeDomain, pieces: &[u32]) -> Vec<u32> {
        pieces.to_vec()
    }

    async fn want_all(&self, domain: &FakeDomain) {
        self.wanted_all.lock().push(domain.pieces.clone());
        self.selected.lock().extend(domain.pieces.clone());
    }

    /// Only what the pass ordered fetched, which is the half of the
    /// window list a probe is left out of -- and then the selection it
    /// costs, by `TorrentBacking::want`'s own rule: re-select the windows,
    /// then stop wanting every piece of the file that is in no window and
    /// not already on the disk.
    ///
    /// Two clauses of the real one are here because the answer depends on
    /// them. A piece inside a live stream's lookahead is **not** dropped
    /// -- the fork's `drop_pieces` refuses it -- so a response that is
    /// reading ahead over a piece keeps it ordered whatever the pass
    /// decided. And what is dropped is recorded per pass, so a scenario
    /// can say which pieces stopped being fetchable and when.
    async fn want(
        &self,
        _store: &(),
        domain: &FakeDomain,
        windows: &[Range<u32>],
        held: &BTreeSet<u32>,
        _door: &Door<Self>,
    ) {
        self.wanted.lock().push(windows.to_vec());
        let streams = self.streams.lock().clone();
        let mut selected = self.selected.lock();
        for window in windows {
            selected.extend(window.clone());
        }
        let unwanted: Vec<u32> = Self::extent(domain)
            .filter(|piece| {
                !windows.iter().any(|window| window.contains(piece))
                    && !held.contains(piece)
                    && !streams.iter().any(|range| range.contains(piece))
            })
            .collect();
        for piece in &unwanted {
            selected.remove(piece);
        }
        drop(selected);
        self.dropped.lock().push(unwanted);
    }

    /// **TEMPORARY**, with [`crate::retention::trace`]: answering it at
    /// all is what makes the pass emit the field's own trace lines, which
    /// is how a scenario reproduces a failure that was reported as a log
    /// line rather than as a number. The counters are zero -- this fake
    /// has no swarm to have paid for anything -- and only the piece
    /// length is read, by the block that turns a reader's lookahead in
    /// bytes into the piece range it covers.
    fn trace(&self, domain: &FakeDomain) -> Option<crate::retention::trace::Backing> {
        Some(crate::retention::trace::Backing {
            fetched: 0,
            verified: 0,
            refused: 0,
            piece_length: domain.piece,
        })
    }

    /// Both shapes at once: `window_now` gates each run as the torrent
    /// does, `refuses` gates each index as the proxy does, and the disk
    /// loses what neither refused.
    async fn reclaim(
        &self,
        _store: &(),
        _domain: &FakeDomain,
        runs: Vec<Range<u32>>,
        door: Door<Self>,
    ) -> usize {
        if self.reclaim_panics.load(Ordering::SeqCst) {
            // The proxy's shape of failure: the blocking closure dies,
            // and the join error is the whole of what the pass hears.
            return tokio::task::spawn_blocking(|| -> usize { panic!("the unlink closure died") })
                .await
                .unwrap_or(0);
        }
        Self::parked(&self.park_reclaim, &self.entered).await;
        self.reclaims.lock().push(runs.clone());
        if let Some(hook) = self.on_reclaim.lock().as_ref() {
            hook(&door);
        }
        let mut asked = Vec::new();
        let mut freed = 0;
        for run in runs {
            if door.window_now().is_none() {
                break;
            }
            asked.push(run.clone());
            for index in run {
                if !door.refuses(index) && self.held.lock().remove(&index) {
                    freed += 1;
                }
            }
            if let Some(hook) = self.between_runs.lock().as_ref() {
                hook();
            }
        }
        self.asked.lock().push(asked);
        freed
    }
}

pub(crate) type Proxy = FakeBacking<ProxySide>;
pub(crate) type Torrent = FakeBacking<TorrentSide>;

/// An eight-piece entity under a budget of four pieces: the proxy
/// shape gives the whole budget to the window, so the window is four
/// pieces and the stride is one.
pub(crate) fn proxy() -> (Arc<Proxy>, Arc<Retention<Proxy>>, Arc<RetentionBudget>) {
    let backing = Proxy::new([domain(0, 0..8)]);
    backing.holds(0..8);
    let budget = Arc::new(RetentionBudget::default());
    budget.set(Some(4 * PIECE));
    let owner = Retention::new(backing.clone(), budget.clone());
    (backing, owner, budget)
}

/// Two eight-piece files under a budget of four pieces: the torrent
/// shape splits it two and two.
pub(crate) fn torrent() -> (Arc<Torrent>, Arc<Retention<Torrent>>, Arc<RetentionBudget>) {
    let backing = Torrent::new([domain(0, 0..8), domain(1, 8..16)]);
    backing.holds(0..16);
    let budget = Arc::new(RetentionBudget::default());
    budget.set(Some(4 * PIECE));
    let owner = Retention::new(backing.clone(), budget.clone());
    (backing, owner, budget)
}

/// Run a pass to its end in a task of its own, so the test can be
/// inside it while it is parked.
pub(crate) fn spawn_pass<S: Side>(
    owner: &Arc<Retention<FakeBacking<S>>>,
    key: usize,
    claim: Claim,
) -> tokio::task::JoinHandle<Outcome> {
    let owner = owner.clone();
    tokio::spawn(async move { owner.pass(&key, &(), claim, Mode::Live).await })
}

// ---------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------

/// The one file a scenario is about, as the numbers a player and a torrent
/// really state: how many bytes, how big a piece, how long the film is.
///
/// The length is here rather than on a step because it is a property of the
/// film and not of a report ([`Retention::note_duration`]): it is what turns
/// the file's size into a bitrate, and a bitrate is what a window measured
/// in seconds needs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Film {
    pub(crate) bytes: u64,
    pub(crate) piece: u64,
    pub(crate) duration: Duration,
}

impl Film {
    /// How many pieces the film is; the last one is short.
    pub(crate) fn pieces(&self) -> u32 {
        u32::try_from(self.bytes.div_ceil(self.piece)).expect("a film of fewer than 4G pieces")
    }

    /// The first byte of `piece`.
    pub(crate) fn at_piece(&self, piece: u32) -> u64 {
        u64::from(piece) * self.piece
    }
}

/// **The film the field failed on.**
///
/// The size and the piece length are the reported ones: 23,346,250,742
/// bytes at 4 MiB, which is 5,567 pieces with 754,678 bytes in the last,
/// and which puts the first byte of piece 5560 at 23,320,330,240 -- the
/// offset every one of the failing reads stopped at.
///
/// The length is *not* reported, and it is needed, because it is what the
/// window's time cap is sized from. Two hours twenty is what 23.3 GB comes
/// to at 22 Mbps, which is what a 4K film this size is; what the scenario
/// turns on is stated where it uses it.
pub(crate) const FIELD_FILM: Film = Film {
    bytes: 23_346_250_742,
    piece: 4 * 1024 * 1024,
    duration: Duration::from_secs(8400),
};

/// The lookahead librqbit grants a read labelled `ContainerMetadata`
/// (`priorities::MAX_CONTAINER_METADATA_WINDOW_BYTES`), and the number the
/// field's trace line carries: `lookahead_bytes=16777216`. Named here so a
/// scenario says which read it is opening rather than a magic number.
pub(crate) const CONTAINER_METADATA_LOOKAHEAD: u64 = 16 * 1024 * 1024;

/// The lookahead a seek or a sequential playback read is granted at the
/// `Normal` profile (`priorities::MAX_SEEK_HOT_WINDOW_BYTES`).
pub(crate) const PLAYBACK_LOOKAHEAD: u64 = 128 * 1024 * 1024;

/// The seconds of stream the `Normal` buffer profile asks to hold
/// (`BufferProfile::window_seconds`).
pub(crate) const NORMAL_WINDOW_SECONDS: u64 = 90;

/// The seconds of watched video the committed set may hold
/// (`priorities::COMMITTED_SECONDS`).
pub(crate) const COMMITTED_SECONDS: u64 = 90;

/// The one file, and the one entity key over it. A scenario is about one
/// film; a second file would be a second entity with a turn of its own and
/// nothing this harness is for.
const FILE: usize = 0;

/// One beat of a scenario: something that really happens to a cache.
///
/// **Nothing here is a retention concept.** A step says a response opened,
/// was served, ended; that the player said where it is; that the swarm
/// delivered; that a pass ran. What the policy makes of any of it is what
/// the scenario is asking about, so a step that named a window or a
/// want-set would be assuming the answer.
#[derive(Debug, Clone)]
pub(crate) enum Step {
    /// A response opens at `offset` and is registered with the owner as
    /// `reading`, having been granted `lookahead` bytes of read-ahead and
    /// carrying a buffer profile worth `window_seconds`.
    ///
    /// The lookahead is also a real range of pieces the backend is now
    /// fetching for this response and will refuse to drop while it is open
    /// ([`FakeBacking::streams`]), which is librqbit's behaviour and not a
    /// bookkeeping detail: it is half of what decides whether a starved
    /// read was starved by the want-set.
    Opens {
        reader: &'static str,
        offset: u64,
        reading: Reading,
        lookahead: u64,
        window_seconds: Option<u64>,
    },
    /// The response is polled for up to `bytes` more bytes.
    ///
    /// It is served what the disk actually holds, stopping at the first
    /// piece that is missing -- one `poll_read` returns what is contiguous
    /// and no more, which is why the field's failing reads each ended
    /// exactly at a piece boundary. A read whose *first* byte is missing
    /// parks instead: it delivers nothing and promises the piece under its
    /// cursor, exactly as `files.rs::poll_read` does on `Poll::Pending`.
    /// The difference between those two is load-bearing -- only the second
    /// one tells the pass what the read is stuck on.
    Reads { reader: &'static str, bytes: u64 },
    /// The response ends: the player gave up, or got what it asked for.
    Closes { reader: &'static str },
    /// The player reports where it is in the picture.
    Says { film: Duration },
    /// The swarm delivers up to `pieces` pieces the backend still wants and
    /// the disk does not hold -- what a response's own lookahead is pulling
    /// first, then whatever else is selected.
    Swarm { pieces: usize },
    /// A retention pass over the film, [`Mode::Live`].
    Pass,
}

/// One `enginefs::retention::trace` line, captured as the field would read
/// it.
#[derive(Debug, Clone)]
pub(crate) struct Line {
    pub(crate) message: String,
    pub(crate) fields: Vec<(&'static str, String)>,
}

impl Line {
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(field, _)| *field == name)
            .map(|(_, value)| value.as_str())
    }

    fn take(&mut self, field: &'static str, value: String) {
        if field == "message" {
            self.message = value;
        } else {
            self.fields.push((field, value));
        }
    }
}

/// What one pass did, in the three kinds it has to be asked about
/// separately.
#[derive(Debug, Clone)]
pub(crate) struct PassLog {
    pub(crate) at: Duration,
    /// The windows the pass promised not to delete inside: the **keep**
    /// set, as the pass concluded it.
    pub(crate) kept: Vec<Range<u32>>,
    /// The windows the pass ordered the backend to fill
    /// ([`Backing::want`]): the **want** set.
    pub(crate) wanted: Vec<Range<u32>>,
    /// The pieces that `want` call took out of the backend's selection:
    /// what the swarm will not fetch again until something wants it.
    pub(crate) unselected: Vec<u32>,
    /// The pieces that really left the disk under this pass.
    pub(crate) unlinked: Vec<u32>,
    /// The trace lines this pass emitted, in order.
    pub(crate) lines: Vec<Line>,
}

impl PassLog {
    /// The `planned to reclaim inside an open stream's lookahead` lines of
    /// this pass -- the field's own evidence, in the field's own form.
    pub(crate) fn inside_a_lookahead(&self) -> Vec<&Line> {
        self.lines
            .iter()
            .filter(|line| line.message.contains("inside an open stream's lookahead"))
            .collect()
    }
}

/// One poll of one response.
#[derive(Debug, Clone)]
pub(crate) struct ReadLog {
    pub(crate) at: Duration,
    pub(crate) reader: &'static str,
    /// Where the response's cursor was when it was polled.
    pub(crate) offset: u64,
    /// The piece that offset is in.
    pub(crate) piece: u32,
    pub(crate) asked: u64,
    pub(crate) delivered: u64,
    /// How long this reader has been unable to get the byte at `offset`,
    /// across however many responses it has opened trying -- the field's
    /// "reads blocked up to 20 s on those pieces".
    pub(crate) waiting: Duration,
}

impl ReadLog {
    pub(crate) fn blocked(&self) -> bool {
        self.delivered == 0
    }
}

/// Everything a scenario recorded.
#[derive(Debug, Default)]
pub(crate) struct Log {
    pub(crate) passes: Vec<PassLog>,
    pub(crate) reads: Vec<ReadLog>,
}

impl Log {
    /// Every trace line of every pass whose message contains `needle`.
    pub(crate) fn lines(&self, needle: &str) -> Vec<&Line> {
        self.passes
            .iter()
            .flat_map(|pass| &pass.lines)
            .filter(|line| line.message.contains(needle))
            .collect()
    }

    /// The longest a reader was left unable to get its next byte.
    pub(crate) fn longest_block(&self, reader: &str) -> Duration {
        self.reads
            .iter()
            .filter(|read| read.reader == reader)
            .map(|read| read.waiting)
            .max()
            .unwrap_or_default()
    }

    /// Every piece any pass took off the disk, with the time it went.
    pub(crate) fn unlinked(&self) -> Vec<(Duration, Vec<u32>)> {
        self.passes
            .iter()
            .filter(|pass| !pass.unlinked.is_empty())
            .map(|pass| (pass.at, pass.unlinked.clone()))
            .collect()
    }
}

/// One open response.
struct Open {
    reader: Reader<Torrent>,
    /// How far this response has served, in bytes of the file.
    cursor: u64,
    /// The pieces its lookahead has the backend fetching.
    stream: Range<u32>,
}

/// A film, a disk, a budget and a clock, driven by a script of [`Step`]s.
pub(crate) struct Scenario {
    backing: Arc<Torrent>,
    owner: Arc<Retention<Torrent>>,
    runtime: tokio::runtime::Runtime,
    film: Film,
    /// The one instant this scenario's clock is built from, by adding.
    /// Never `checked_sub`: on a freshly started process it answers `None`.
    t0: Instant,
    now: Duration,
    open: HashMap<&'static str, Open>,
    /// When each reader last failed to get the byte under its cursor, kept
    /// **per name rather than per response** so a block that outlives the
    /// response measuring it is still one block. The field's second track
    /// opened forty-six responses in seventy seconds.
    blocked_since: HashMap<&'static str, Duration>,
    log: Log,
}

impl Scenario {
    /// A scenario over `film` with `budget` bytes of cache, its policy
    /// installed and nothing on the disk.
    pub(crate) fn new(film: Film, budget: u64) -> Self {
        let domain = film_domain(FILE, film.piece, film.bytes);
        let backing = Torrent::new([domain]);
        let cap = Arc::new(RetentionBudget::default());
        cap.set(Some(budget));
        let owner = Retention::new(backing.clone(), cap);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime");
        let installed = runtime.block_on(owner.install(FILE, FILE));
        assert_eq!(
            installed,
            InstallOutcome::Installed,
            "the scenario's budget does not bound its film"
        );
        owner.note_duration(&FILE, film.duration);
        Self {
            backing,
            owner,
            runtime,
            film,
            t0: Instant::now(),
            now: Duration::ZERO,
            open: HashMap::new(),
            blocked_since: HashMap::new(),
            log: Log::default(),
        }
    }

    /// Seed the disk: these pieces are already held.
    pub(crate) fn on_disk(self, pieces: Range<u32>) -> Self {
        self.backing.holds(pieces);
        self
    }

    /// Run the script and hand back what it recorded. Beats must be in
    /// non-decreasing time order; the clock never moves backwards and
    /// nothing sleeps.
    pub(crate) fn run(mut self, script: &[(Duration, Step)]) -> Log {
        for (at, step) in script {
            assert!(
                *at >= self.now,
                "a scenario beat at {at:?} follows one at {:?}",
                self.now
            );
            self.now = *at;
            self.beat(step);
        }
        self.log
    }

    /// The scenario's clock as the owner takes it.
    fn instant(&self) -> Instant {
        self.t0 + self.now
    }

    fn piece_of(&self, offset: u64) -> u32 {
        u32::try_from(offset / self.film.piece)
            .unwrap_or(u32::MAX)
            .min(self.film.pieces() - 1)
    }

    fn beat(&mut self, step: &Step) {
        match step {
            Step::Opens {
                reader,
                offset,
                reading,
                lookahead,
                window_seconds,
            } => self.opens(reader, *offset, *reading, *lookahead, *window_seconds),
            Step::Reads { reader, bytes } => self.reads(reader, *bytes),
            Step::Closes { reader } => {
                self.open.remove(reader);
                self.republish_streams();
            }
            Step::Says { film } => {
                self.owner
                    .note_playhead_at(&FILE, *film, None, self.instant());
            }
            Step::Swarm { pieces } => self.swarm(*pieces),
            Step::Pass => self.pass(),
        }
    }

    fn opens(
        &mut self,
        reader: &'static str,
        offset: u64,
        reading: Reading,
        lookahead: u64,
        window_seconds: Option<u64>,
    ) {
        let handle = self
            .owner
            .reader_on(
                &FILE,
                (FILE, offset),
                reading,
                Buffering {
                    lookahead_bytes: lookahead,
                    window_seconds,
                    committed_seconds: Some(COMMITTED_SECONDS),
                    // The entity's, from its size and the film's length.
                    bytes_per_second: None,
                    // The entity's draw, not this read's.
                    seed: 0,
                },
            )
            .expect("an entity to open a read on");
        let stream = self.stream_of(offset, lookahead);
        self.open.insert(
            reader,
            Open {
                reader: handle,
                cursor: offset,
                stream,
            },
        );
        self.republish_streams();
    }

    /// The pieces a response opened at `offset` with `lookahead` bytes of
    /// read-ahead has the backend fetching for it.
    fn stream_of(&self, offset: u64, lookahead: u64) -> Range<u32> {
        let first = self.piece_of(offset);
        let ahead = u32::try_from(lookahead.div_ceil(self.film.piece)).unwrap_or(u32::MAX);
        first..first.saturating_add(ahead).min(self.film.pieces())
    }

    /// What the backend is fetching for the responses that are open, which
    /// is what its `drop_pieces` refuses to give up.
    fn republish_streams(&self) {
        *self.backing.streams.lock() = self
            .open
            .values()
            .map(|open| open.stream.clone())
            .collect::<Vec<_>>();
    }

    fn reads(&mut self, reader: &'static str, bytes: u64) {
        let cursor = self.open.get(reader).expect("an open response").cursor;
        let held = self.backing.held.lock().clone();
        let piece = self.piece_of(cursor);
        if !held.contains(&piece) {
            // `poll_read` returned `Pending`: nothing delivered, and the
            // piece under the cursor is promised, which is the only thing
            // that tells the pass what this read is stuck on.
            let since = *self.blocked_since.entry(reader).or_insert(self.now);
            self.open
                .get(reader)
                .expect("an open response")
                .reader
                .promises_at((FILE, cursor));
            self.log.reads.push(ReadLog {
                at: self.now,
                reader,
                offset: cursor,
                piece,
                asked: bytes,
                delivered: 0,
                waiting: self.now.saturating_sub(since),
            });
            return;
        }
        // Served to the first piece we do not hold, and no further: one
        // `poll_read` hands out what is contiguous.
        let mut edge = piece;
        while edge < self.film.pieces() && held.contains(&edge) {
            edge += 1;
        }
        let boundary = self.film.at_piece(edge).min(self.film.bytes);
        let delivered = bytes.min(boundary.saturating_sub(cursor));
        assert!(delivered > 0, "a read of no bytes is not a read");
        let moved = cursor + delivered;
        self.blocked_since.remove(reader);
        let stream = {
            let ahead = {
                let open = self.open.get(reader).expect("an open response");
                open.stream.end - open.stream.start
            };
            self.stream_of(moved, u64::from(ahead) * self.film.piece)
        };
        {
            let now = self.instant();
            let open = self.open.get_mut(reader).expect("an open response");
            open.cursor = moved;
            open.stream = stream;
            let claim = open.reader.note_at((FILE, moved), now);
            assert!(
                claim.is_none(),
                "a delivered byte claimed a torrent file's turn; the tick is its trigger"
            );
        }
        self.republish_streams();
        self.log.reads.push(ReadLog {
            at: self.now,
            reader,
            offset: cursor,
            piece,
            asked: bytes,
            delivered,
            waiting: Duration::ZERO,
        });
    }

    /// The swarm delivers, and it delivers **what the backend is still
    /// asking for and nothing else**.
    ///
    /// A response's own lookahead is what it fetches first, as librqbit's
    /// deadline path does, and then whatever else is selected in index
    /// order; but the only thing that decides whether a piece can arrive at
    /// all is whether it is in the selection. That is the whole point of
    /// modelling the selection rather than recording it: a scenario says
    /// how many pieces the swarm manages in a beat, and
    /// [`Step::Swarm`]'s number can be made larger than the film, which
    /// turns the question "why did this piece not arrive?" into one with
    /// exactly one possible answer.
    fn swarm(&mut self, pieces: usize) {
        let held = self.backing.held.lock().clone();
        let selected = self.backing.selected.lock().clone();
        let streams = self.backing.streams.lock().clone();
        // An open response's lookahead first, and **not** filtered by what
        // the backend has selected. librqbit's priority loop reserves a
        // piece on a stream's list having checked only that it is not had,
        // not releasing, not mid-hash-check, and that the peer has it --
        // never that it is queued. Its own comment says so: "Only this loop
        // can reserve such a piece -- `iter_queued_pieces` cannot, its bit
        // is long gone." So a piece a pass dropped is still pulled back by
        // a response reading over it, which is the difference between an
        // eviction that is a loop and one that is a wall.
        let order = streams
            .iter()
            .flat_map(|range| range.clone())
            .chain(selected.iter().copied());
        let mut arriving: BTreeSet<u32> = BTreeSet::new();
        for piece in order {
            if arriving.len() >= pieces {
                break;
            }
            if !held.contains(&piece) {
                arriving.insert(piece);
            }
        }
        self.backing.holds(arriving);
    }

    fn pass(&mut self) {
        let before = self.backing.held.lock().clone();
        let claim = self
            .runtime
            .block_on(self.owner.turn(&FILE))
            .expect("the entity's turn");
        let lines = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let outcome = {
            let owner = self.owner.clone();
            let runtime = &self.runtime;
            tracing::subscriber::with_default(Collector(lines.clone()), || {
                runtime.block_on(owner.pass(&FILE, &(), claim, Mode::Live))
            })
        };
        let after = self.backing.held.lock().clone();
        let kept = outcome
            .concluded
            .map(|concluded| concluded.windows)
            .unwrap_or_default();
        let wanted = self
            .backing
            .wanted
            .lock()
            .last()
            .cloned()
            .unwrap_or_default();
        let unselected = self
            .backing
            .dropped
            .lock()
            .last()
            .cloned()
            .unwrap_or_default();
        let lines = std::mem::take(&mut *lines.lock());
        self.log.passes.push(PassLog {
            at: self.now,
            kept,
            wanted,
            unselected,
            unlinked: before.difference(&after).copied().collect(),
            lines,
        });
    }
}

/// A subscriber that keeps the retention module's own trace lines, so a
/// scenario can assert on the evidence the field reported rather than on a
/// reconstruction of it.
///
/// `register_callsite` deliberately answers `sometimes`: an `Interest` of
/// `never` is cached per callsite for the life of the process, and these
/// tests run in parallel with every other test in the crate.
struct Collector(Arc<parking_lot::Mutex<Vec<Line>>>);

impl tracing::Subscriber for Collector {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().starts_with("enginefs::retention::trace")
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut line = Line {
            message: String::new(),
            fields: Vec::new(),
        };
        event.record(&mut line);
        self.0.lock().push(line);
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

impl tracing::field::Visit for Line {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.take(field.name(), value.to_string());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.take(field.name(), format!("{value:?}"));
    }
}

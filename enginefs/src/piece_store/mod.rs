//! Torrent data stored as one file per piece.
//!
//! Uniformly, for the streaming cache and for offline downloads alike. Whole
//! `.mkv` files are never produced: downloads exist so content plays offline
//! inside the client, not to hand the user a file.
//!
//! # Why
//!
//! * **Reclaim becomes `fs::remove_file`**, on every filesystem. Punching
//!   holes into a whole-file layout needed `fallocate(FALLOC_FL_PUNCH_HOLE)`,
//!   which a filesystem is free to answer with `EOPNOTSUPP` -- clearing the
//!   have-bits while freeing nothing -- and whose ordering against the
//!   bitfield flush left a crash window in which the recorded haves were
//!   holes.
//! * **Piece presence is the have-set.** There is no record of ours for a
//!   crash to leave disagreeing with the disk; the disk *is* the record. That
//!   is why a piece's bytes are written under a staging name and renamed into
//!   place only once librqbit's hash check has passed
//!   ([`store::STAGING_SUFFIX`]): presence has to mean *complete*, and a
//!   file created by a piece's first 16 KiB chunk is there for the whole of
//!   the download. The errors are not symmetric -- a wrong "no" costs a
//!   re-download, a wrong "yes" leaves a have-bit standing over bytes nothing
//!   verified, and the fastresume check that might have caught it samples
//!   ~65 pieces of a torrent however large.
//!
//!   With one exception, stated here because this is where the invariant is
//!   claimed: a piece lying *entirely* inside a BEP-47 padding file has no
//!   owner to write it, so it never gets a file and
//!   [`store::PieceStore::has_piece`] is false for it for ever. Nothing
//!   transfers those bytes -- librqbit skips padding on every storage path
//!   -- so no read can reach the piece and no hash check waits on it; it is
//!   not payload. Giving it a zero-byte file would put a have-record on disk
//!   that no write ever justified, which is the failure mode this design
//!   exists to remove, so the hole is named rather than filled. A conforming
//!   torrent cannot open one anyway: padding runs to the next piece boundary
//!   and is therefore always shorter than a piece. The question
//!   [`librqbit::storage::TorrentStorage::has_piece`] asks is a different one
//!   -- "can this storage have lost the piece?" -- and for such a piece the
//!   answer is no, so it does not have the hole.
//! * **It is not an rqbit core change.** `TorrentStorage` and `StorageFactory`
//!   are existing public seams, and enginefs reads through
//!   `librqbit::FileStreamOptions` rather than opening files itself, so this
//!   backend lives in our crate.
//! * **Uniform means no per-purpose branch.** Streaming and downloads share
//!   one engine, so a torrent has no purpose at add time and a two-backend
//!   design would have to convert live storage under a reading player.
//!
//! # The parts
//!
//! [`layout`] is the arithmetic of *where* a byte lives -- `(file_id, offset)`
//! to `(piece, offset_in_piece)` -- and [`policy`] is the arithmetic of *which*
//! pieces we keep and which of those we share; neither has a filesystem or a
//! librqbit type in it. [`store`] is the
//! [`librqbit::storage::TorrentStorage`] implementation over the first.
//! [`sweep`] reconciles the store against the session at launch.
//!
//! # Not wired into the session yet, and why
//!
//! [`store::PieceStoreFactory`] is not passed to librqbit anywhere. Three
//! things had to be decided in the rqbit fork before it could be, and all
//! three are about the have-set rather than about storage. One of them is
//! settled at the rev this crate pins; two are not.
//!
//! **Settled.** `initial_check` walks files in order and, on the first read
//! error in a file, marks the rest of that file not-have without reading it. A
//! sparsely populated cache -- which is the normal state here -- would report
//! the pieces before the first gap and nothing after it. The fork's answer is
//! that the have-set no longer comes from a read-everything pass: it is the
//! resume bitfield intersected with
//! [`librqbit::storage::TorrentStorage::has_piece`], asked of the storage, and
//! [`store::PieceStore`] answers it. A read of an absent piece still has to
//! fail ([`store::MissingPiece`]) rather than return zeroes, or a hash check
//! would call a hole a verified piece, and it does.
//!
//! **Not settled: persistence.** `JsonSessionPersistenceStore::update_db`
//! refuses to persist a torrent whose storage factory is not
//! `FilesystemStorageFactory`, and this session runs with persistence on
//! (`LibrqbitBackend::new` hands it `SessionPersistenceConfig::Json`), so an
//! add with this factory fails outright. What the refusal is really about is
//! what the record omits: a `SerializedTorrent` names an output folder and a
//! file selection and no storage at all, so a restart replays it onto the
//! session's *default* factory. The fork's answer is a
//! `StorageFactory::ensure_persistable` promise -- a restart finds this data
//! again, and the have-bitfield will not outlive it -- which this store can
//! make and the filesystem one already keeps. That commit exists on the fork's
//! `pinned` branch but has not been pushed, so there is no rev to move to:
//! bumping the pin to an unpushed commit would not build anywhere else, and
//! this repo's rev is bumped deliberately. Until it is, nothing here can be
//! handed to a torrent.
//!
//! **Not settled: a have-bit that is not an announcement.** [`policy`] decides
//! that only the committed set is advertised and that a window piece is held
//! and readable and *not* announced. That is not expressible through the
//! fork's API today: `have` implies announced on both paths -- the `have`
//! broadcast (`should_transmit_have` reaching
//! `TorrentStateLive::should_advertise_have`) and the handshake bitfield,
//! which serialises `get_have_pieces()` whole -- and a window piece has to be
//! `have` for the stream to read it. `drop_pieces` gives *not* have, not
//! wanted, not advertised, so there is no third state to put a window piece
//! in. A store wired up without one would announce every window piece and
//! withdraw it again as the window moved, which is the advertise-then-refuse
//! the policy exists to avoid -- so this blocks the policy reaching a swarm
//! even once persistence stops blocking the storage reaching a torrent.
//! Nothing in this crate can lift it.
//!
//! The first two are exercised below rather than assumed: the storage is
//! driven through a real librqbit session's own initial check. The third
//! cannot be -- there is no API to observe an announcement separately from a
//! have-bit, which is the whole of the problem.

pub mod layout;
pub mod policy;
pub mod store;
pub mod sweep;

pub use layout::{FileSpec, PieceLayout, Segment};
pub use policy::{Decision, RetentionPolicy, Shape};
pub use store::{MissingPiece, PIECES_PER_DIRECTORY, PieceStore, PieceStoreFactory, layout_of};
pub use sweep::{SweepReport, session_recorded_hashes, sweep_unadopted};

/// The store's directory inside a torrent cache root.
///
/// Dot-prefixed like the engine's other private subdirectories (`.cache`,
/// `.metadata`) so that a torrent named `pieces` cannot land on top of it.
///
/// Inside the cache root rather than beside it, deliberately: piece files are
/// cache, and the cache cleaner has to be able to see and count them. It is
/// not one of `cache_cleaner::is_session_artifact`'s exempt directories for
/// the same reason -- pieces are exactly what a full disk should be reclaiming.
pub const PIECE_STORE_DIR: &str = ".pieces";

/// Where a torrent cache root keeps its piece store.
pub fn root_in(download_dir: &std::path::Path) -> std::path::PathBuf {
    download_dir.join(PIECE_STORE_DIR)
}

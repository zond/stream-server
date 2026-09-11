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
//!   ([`crate::chunk_store::STAGING_SUFFIX`]): presence has to mean
//!   *complete*, and a
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
//! The bytes are not this module's. They are
//! [`crate::chunk_store::ChunkDir`]'s -- the one chunk store, shared with
//! `/proxy`'s cache, which owns the bucketed directory shape, the two
//! staging spellings, the rename that makes presence mean complete, the
//! listings, the occupancy, the typed read and the open-handle LRU. What is
//! here is the torrent half.
//!
//! [`layout`] is the arithmetic of *where* a byte lives -- `(file_id, offset)`
//! to `(piece, offset_in_piece)` -- and [`policy`] is the arithmetic of *which*
//! pieces we keep and which of those we share; neither has a filesystem or a
//! librqbit type in it. [`store`] is the
//! [`librqbit::storage::TorrentStorage`] adapter over the first: the piece
//! and file-table arithmetic, BEP-47 padding, info-hash keying and its
//! spelling rule, and the have-set interlock -- everything a chunk store
//! cannot know. [`registry`] is how the rest of the process reaches the
//! store librqbit holds: a store registers there once `init` has seeded its
//! held set, the retention pass reads the set through it instead of listing
//! the directory, and every unlink of a registered torrent's piece goes
//! through the registered store. [`sweep`] reconciles the store against the
//! session at launch. What drives [`policy`] against a real torrent -- the playhead, the
//! hold-back, the reclaim, and the claim that keeps a delete atomic with the
//! have-set -- is [`crate::retention`].
//!
//! Two things about the chunk store are *parameters* an adapter sets, and
//! this one sets both differently from `/proxy`. Its staged copy is
//! **addressable** (`<piece>.part`, reopened across many 16 KiB writes, read
//! back by the hash check through one handle, recovered at `init`), where a
//! `/proxy` fill stages anonymously because two uncoalesced fillers may be
//! writing one chunk. And it commits with **no expected length**: librqbit
//! never writes padding, so a piece whose tail is padding is committed
//! short, and there is no length a legal padded piece would satisfy. A URL
//! response, which has no hash to be checked against, passes the byte count
//! it buffered instead.
//!
//! # How it is wired in
//!
//! [`store::PieceStoreFactory`] is the librqbit session's **default** storage
//! factory ([`crate::backend::librqbit::LibrqbitBackend`]'s
//! `session_storage_factory`, installed in `open_session`), rooted at
//! [`store::StoreRoot::in_download_dir`] of the same `download_dir` librqbit
//! persists the session into.
//! It is passed to no individual add, and that is not a stylistic choice:
//! three things had to be decided in the rqbit fork before it could be wired
//! at all, and all three are about the have-set rather than about storage.
//! All three are settled at the rev this crate pins, and the last of them is
//! what let [`policy`] be wired up ([`crate::retention`]).
//!
//! **Settled: where the have-set comes from.** `initial_check` walks files in
//! order and, on the first read error in a file, marks the rest of that file
//! not-have without reading it. A sparsely populated cache -- which is the
//! normal state here -- would report the pieces before the first gap and
//! nothing after it. The fork's answer is that the have-set no longer comes
//! from a read-everything pass: it is the resume bitfield intersected with
//! [`librqbit::storage::TorrentStorage::has_piece`], asked of the storage, and
//! [`store::PieceStore`] answers it. A read of an absent piece still has to
//! fail ([`store::MissingPiece`]) rather than return zeroes, or a hash check
//! would call a hole a verified piece, and it does.
//!
//! **Settled: persistence, and why "default" is the only place this can go.**
//! `JsonSessionPersistenceStore::update_db` used to refuse a torrent whose
//! storage factory was not `FilesystemStorageFactory`, and this session runs
//! with persistence on (`LibrqbitBackend::new` hands it
//! `SessionPersistenceConfig::Json`), so an add with this factory failed
//! outright. What that refusal was really about is what the record omits: a
//! `SerializedTorrent` names an output folder and a file selection and no
//! storage at all, so a restart replays it onto the session's *default*
//! factory. At the rev this crate pins the type check is gone and in its
//! place is a `StorageFactory::ensure_persistable` promise -- a restart finds
//! this data again, and the have-bitfield will not outlive it.
//!
//! [`store::PieceStoreFactory`] makes that promise
//! (its `StorageFactory` impl in [`store`]) and may only make it
//! because it is the default and its root is derived from the session's own
//! `download_dir`: the next process builds a store over the same directory
//! and finds the same pieces under the same info hash. Handed to a single
//! add instead, the promise would be a lie the next restart collects on.
//!
//! **Settled: reclaim capability.** The rev this crate pins adds a second
//! factory promise, `StorageFactory::ensure_can_release_pieces`, and
//! `Session::add_torrent` refuses `AddTorrentOptions::piece_reclaim` on a
//! factory that does not make it -- because `drop_pieces` frees nothing on a
//! storage that cannot let one piece go. [`store::PieceStoreFactory`] makes
//! this one too: a piece is a file, and releasing it is
//! [`store::PieceStore::delete_piece`]. That is a statement about the layout
//! alone, true whether or not this is the default factory, unlike
//! `ensure_persistable` above. Because this store *is* the default, the
//! session's answer to "can my storage release a piece?" is now yes, so
//! every add sets `piece_reclaim` and `drop_file_pieces` works. It also means
//! librqbit restores **every** torrent paused -- the piece-level want-set is
//! not in the persisted record -- and the engine layer is what starts them
//! again, once `BackendEngineFS::restore_pinned_downloads` has put the
//! want-set back (see [`crate::reconcile::Conditions::settled`]).
//!
//! **There is no migration, by decision.** A whole-file download an earlier
//! version wrote is neither converted nor read: the torrent that owns it
//! comes up with an empty have-set and re-downloads as pieces, and the old
//! bytes belong to nobody: no store speaks for them, so they are counted in
//! a usage figure (`StoreRoot::unregistered_bytes`) and removed whole by the
//! launch sweep, which keeps the pinned directories and nothing else.
//!
//! Nothing else has a stake in the default factory. The client's activity
//! signal briefly did -- its counters were a storage wrapper around the
//! default, and this store would have had to go inside it -- until that
//! wrapper counted the initial check's read-back of every restored torrent
//! as traffic. The signal now reads librqbit's own peer counters
//! ([`crate::traffic`]) and never sees the storage, so this factory is the
//! bare default, and its own initial-check reads are nobody's traffic.
//!
//! **Settled: a have-bit that is not an announcement.** [`policy`] decides
//! that only the committed set is advertised and that a window piece is held
//! and readable and *not* announced. That used to be inexpressible: `have`
//! implied announced on both paths -- the `have` broadcast and the handshake
//! bitfield, which serialises `get_have_pieces()` whole -- and a window piece
//! has to be `have` for the stream to read it, while `drop_pieces` gives
//! *not* have, not wanted, not advertised. The fork's
//! `ManagedTorrent::set_pieces_advertised` is the third state: a suppression
//! set on the chunk tracker, independent of both the have-set and the reclaim
//! want-set, and settable before a piece is downloaded, which is the only
//! ordering under which no Have ever goes out for a window piece.
//! [`crate::retention`] is the wiring, and it is why the policy is no longer
//! a decision nothing performs.
//!
//! All three are exercised rather than assumed. The storage is driven through
//! a real librqbit session's own initial check below, and through a real
//! *persisted* session across a restart in `backend::librqbit`'s
//! `a_restart_on_the_piece_store_finds_its_data_and_the_reconciler_starts_it`.
//! The third is exercised over two real sessions on the wire, in the same
//! module's `a_peer_is_never_told_about_a_piece_we_later_reclaim`: what the
//! peer ends up holding is what we announced, and every one of those pieces
//! is still on our disk when the stream is over.

pub mod layout;
pub mod pin_record;
pub mod policy;
pub mod registry;
pub mod store;
pub mod sweep;

pub use crate::chunk_store::StoredChunk as StoredPiece;
pub use layout::{FileSpec, PieceLayout, Segment};
pub use pin_record::{PinRecord, PinsUnknown};
pub use policy::{Decision, RetentionPolicy, Shape, Share};
pub use registry::{DeleteOutcome, StoreRegistry};
pub use store::{
    HeldSnapshot, MissingPiece, PieceStore, PieceStoreFactory, StoreRoot, StoredTorrent, layout_of,
};
pub use sweep::{SweepReport, sweep_before_session, sweep_unadopted};

/// The store's directory inside a torrent cache root.
///
/// Dot-prefixed like the engine's other private subdirectories (`.cache`,
/// `.metadata`) so that a torrent named `pieces` cannot land on top of it.
///
/// Inside the cache root rather than beside it, deliberately: piece files
/// are cache, and whoever is counting the cache has to be able to see them
/// -- which they do by asking the store ([`store::StoreRoot::stat`] and
/// [`registry::StoreRegistry::occupancy`]), never by walking in here.
/// Nothing exempts the directory from the launch sweep either: pieces are
/// exactly what a full disk should be reclaiming.
pub(crate) const PIECE_STORE_DIR: &str = ".pieces";

/// Where a torrent cache root keeps its piece store.
///
/// Crate-private, and reached from outside only through
/// [`store::StoreRoot::in_download_dir`]: naming the store is the one thing
/// a caller needs, and giving it the *path* is how the directory shape
/// escaped into the `server` crate's cache cleaner in the first place.
pub(crate) fn root_in(download_dir: &std::path::Path) -> std::path::PathBuf {
    download_dir.join(PIECE_STORE_DIR)
}

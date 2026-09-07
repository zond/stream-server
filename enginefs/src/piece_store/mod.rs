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
//!   crash to leave disagreeing with the disk; the disk *is* the record.
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
//! [`layout`] is the arithmetic -- `(file_id, offset)` to `(piece,
//! offset_in_piece)` -- with no filesystem and no librqbit types in it.
//! [`store`] is the [`librqbit::storage::TorrentStorage`] implementation over
//! it. The launch-time reconcile of the store against the session follows.
//!
//! # Not wired into the session yet, and why
//!
//! [`store::PieceStoreFactory`] is not passed to librqbit anywhere. Two things
//! have to be decided in the rqbit fork before it can be, and both are about
//! the have-set rather than about storage:
//!
//! 1. `JsonSessionPersistenceStore::update_db` refuses to persist a torrent
//!    whose storage factory is not `FilesystemStorageFactory`, and the session
//!    runs with persistence on, so today every add with this factory would
//!    fail outright. Relaxing that is not mechanical: under this design the
//!    `.bitv` fastresume bitfields stop being the have-record, because the
//!    piece files are.
//! 2. `initial_check` walks files in order and, on the first read error in a
//!    file, marks the rest of that file not-have without reading it. A
//!    sparsely populated cache -- which is the normal state here -- would
//!    report the pieces before the first gap and nothing after it. A read of
//!    an absent piece must keep failing ([`store::MissingPiece`]) rather than
//!    returning zeroes, or a hash check would call a hole a verified piece; so
//!    the have-set has to come from the store instead of from a
//!    read-everything pass.
//!
//! Both are exercised below rather than assumed: the storage is driven through
//! a real librqbit session's own initial check.

pub mod layout;
pub mod store;

pub use layout::{FileSpec, PieceLayout, Segment};
pub use store::{MissingPiece, PIECES_PER_DIRECTORY, PieceStore, PieceStoreFactory, layout_of};

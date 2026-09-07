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
//! offset_in_piece)` -- with no filesystem and no librqbit types in it. The
//! `TorrentStorage` implementation over it, and the launch-time reconcile of
//! the store against the session, follow.

pub mod layout;

pub use layout::{FileSpec, PieceLayout, Segment};

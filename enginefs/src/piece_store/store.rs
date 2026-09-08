//! The [`librqbit::storage::TorrentStorage`] implementation itself: one file
//! per piece, under one directory per torrent.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use librqbit::storage::{StorageFactory, TorrentStorage};
use parking_lot::Mutex;

use super::layout::{FileSpec, PieceLayout};

/// How many piece files share one directory.
///
/// Flat would work on ext4 and f2fs, which hash directory entries, but not
/// everywhere the cache can land: a 27 GB torrent at 4 MB pieces is ~6,750
/// files, and on a filesystem that scans a directory linearly (exFAT and
/// FAT32 on a phone's SD card, which is exactly where a large offline
/// download goes) every open in a 6,750-entry directory walks the entries.
/// Bucketing by a thousand puts a ceiling on that -- seven directories of at
/// most a thousand for that torrent -- for the cost of one extra path
/// component. A thousand rather than a power of two because the names are
/// decimal, and `4/4200` reading as "piece 4200" is worth more when reading a
/// directory listing by hand than the shift it saves.
pub const PIECES_PER_DIRECTORY: u32 = 1000;

/// What a piece file is called while it is still being written.
///
/// A piece arrives 16 KiB at a time, so a file created by its first chunk is
/// there for the whole of the download -- and the storage contract is that
/// presence means **complete**, because a wrong "yes" is not a re-download but
/// silent corruption: the have-set a restart starts from is the resume data
/// intersected with what the storage says it still holds, the intersection can
/// only clear bits, and the fastresume hash check samples ~65 pieces of a
/// torrent however large. So the bytes go to this name and are renamed into
/// place in [`TorrentStorage::on_piece_completed`], which runs after the hash
/// check. A rename within one directory is atomic on every filesystem here.
pub const STAGING_SUFFIX: &str = ".part";

/// A read asked for a piece that is not on disk.
///
/// Distinct from an I/O failure on purpose. Under this design the presence of
/// a piece file *is* the have-record, so "not there" is an ordinary state a
/// reclaim policy creates deliberately, and the layer above has to be able to
/// tell it apart from a disk that is failing.
#[derive(Debug, thiserror::Error)]
#[error("piece {piece} is not on disk")]
pub struct MissingPiece {
    pub piece: u32,
}

/// One torrent's piece files.
///
/// # What is stored
///
/// `<root>/<info hash>/<bucket>/<piece index>`, one file per piece, holding
/// exactly that piece's bytes. Nothing is pre-allocated and nothing is padded:
/// a piece file is as long as the highest byte written to it so far, and a
/// piece nobody has written has no file. That is the whole of the have-record
/// -- there is no separate bitfield of ours that a crash could leave
/// disagreeing with the disk.
///
/// # What reclaim costs
///
/// One `fs::remove_file` per piece, on every filesystem. The earlier attempt
/// at reclaim punched holes with `fallocate(FALLOC_FL_PUNCH_HOLE)`, which an
/// `EOPNOTSUPP` filesystem answered by clearing the have-bits while freeing
/// nothing, and whose ordering against the bitfield flush left a crash window
/// where the recorded haves were holes. Neither failure has anywhere to live
/// here.
///
/// # Files
///
/// librqbit addresses storage by `(file_id, offset)` and this backend has no
/// files in that sense; every request is resolved through [`PieceLayout`] into
/// the piece files it covers. A file is therefore not a unit of storage but a
/// range of the torrent's global bytes, and two files that share a piece share
/// its file -- which is what [`Self::remove_file`] has to be careful about.
///
/// # Open handles
///
/// librqbit writes a piece 16 KiB at a time and a stream reads it in 8 KiB
/// or so, and the first version of this store opened and closed the piece
/// file for every one of those -- and, for a read, probed the staging name
/// first, so a complete piece cost two opens per read. On ext4 that is
/// microseconds; on the FUSE-mediated external storage and exFAT cards a
/// large offline download lands on, an open is 50-200 µs and the read path
/// was a third of the wall time. So the store keeps the last few handles it
/// opened ([`OPEN_HANDLES`], keyed by piece and by which copy) and knows in
/// memory which pieces have a staged copy, and a piece written or streamed
/// end to end is one open.
///
/// The cache changes nothing about what is on disk, and it must not change
/// what a read sees either: a handle is forgotten before the file it names
/// is renamed into place or deleted, and a piece's cached *complete* handle
/// goes the moment a new staged copy is begun, since a read has to prefer
/// the staged one. A handle still in a reader's hands survives that -- it
/// is an `Arc` -- and keeps reading the bytes it had, which is the same
/// thing a read that had already begun would do.
pub struct PieceStore {
    /// `<root>/<info hash>`.
    dir: PathBuf,
    layout: Arc<PieceLayout>,
    /// File ids [`Self::remove_file`] has been asked to drop. A piece may
    /// only go when every file that owns bytes in it is in here.
    removed_files: Mutex<BTreeSet<usize>>,
    /// Pieces that have a staged copy, as far as this process knows: added
    /// by the first write to one, removed when it is completed or deleted,
    /// seeded by `init` from what a previous process left. What lets a read
    /// go straight to the complete copy of a piece nothing is re-writing,
    /// instead of asking the filesystem for a staged one first every time.
    /// Advisory only -- a read that finds no staged file where this says
    /// there is one falls through to the complete copy -- so a stale entry
    /// costs one probe, never a wrong answer.
    staged: Mutex<BTreeSet<u32>>,
    /// The handles most recently opened, most recent last -- see the type
    /// doc. Empty on a store that has just been created or taken.
    handles: Mutex<Vec<OpenHandle>>,
    /// False once [`TorrentStorage::take`] has handed the data path to a
    /// successor. The path-based operations keep working on a taken store,
    /// exactly as the filesystem backend's do: `Session::delete` calls
    /// `remove_file` on the storage it took.
    live: AtomicBool,
    /// Files opened, and staging names probed and found absent, for the
    /// tests that pin the open count -- the whole reason the cache exists.
    #[cfg(test)]
    opens: AtomicUsize,
    #[cfg(test)]
    staging_probes: AtomicUsize,
}

/// How many piece files a store keeps open.
///
/// librqbit has a handful of pieces in flight for a torrent and a stream
/// reads one piece at a time in order, so a few entries cover the working
/// set; the point is the ratio (one open per piece instead of one per
/// chunk), not a hit rate, and eight of them is eight descriptors per
/// torrent rather than the filesystem backend's one per file.
pub const OPEN_HANDLES: usize = 8;

/// One entry of [`PieceStore::handles`]: which piece, which copy of it
/// (the staged one is a different file from the complete one), and the
/// handle. The `Arc` is what a read or write borrows, so forgetting an
/// entry never closes a file mid-operation.
struct OpenHandle {
    piece: u32,
    staged: bool,
    file: Arc<File>,
}

impl PieceStore {
    /// A store for one torrent. Creates nothing -- `init` does that, and
    /// librqbit has a path (`Session::delete` with no live storage to
    /// recover) that constructs a storage purely to delete through it.
    pub fn new(dir: PathBuf, layout: Arc<PieceLayout>) -> Self {
        Self {
            dir,
            layout,
            removed_files: Mutex::new(BTreeSet::new()),
            staged: Mutex::new(BTreeSet::new()),
            handles: Mutex::new(Vec::new()),
            live: AtomicBool::new(true),
            #[cfg(test)]
            opens: AtomicUsize::new(0),
            #[cfg(test)]
            staging_probes: AtomicUsize::new(0),
        }
    }

    /// The directory this torrent's pieces live in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn layout(&self) -> &Arc<PieceLayout> {
        &self.layout
    }

    /// Where one piece is stored once it is whole.
    pub fn piece_path(&self, piece: u32) -> PathBuf {
        piece_path(&self.dir, piece)
    }

    /// Where its bytes go while it is being written -- see
    /// [`STAGING_SUFFIX`].
    pub fn staging_path(&self, piece: u32) -> PathBuf {
        staging_path(&self.dir, piece)
    }

    /// Whether this piece is on disk, **complete**. A piece halfway through
    /// being downloaded is not: its bytes are under [`Self::staging_path`]
    /// until the hash check passes.
    ///
    /// False for ever for a piece lying entirely inside a BEP-47 padding
    /// file: nobody transfers those bytes, so nothing ever writes it. That is
    /// the one hole in "piece presence is the have-set", and the module docs
    /// say why it is left open rather than papered over with an empty file.
    /// [`TorrentStorage::has_piece`] answers a different question and does not
    /// have that hole -- see there.
    pub fn has_piece(&self, piece: u32) -> bool {
        self.piece_path(piece).is_file()
    }

    /// Promote a written piece to a complete one. Nothing may read it as ours
    /// before this returns and everything may afterwards, so this is the
    /// single instant at which the have-record for a piece comes into being.
    ///
    /// Idempotent in the direction that matters: called for a piece already in
    /// place with nothing staged, it says so rather than failing, because
    /// librqbit logs a failure here at debug and marks the piece have anyway.
    pub fn complete_piece(&self, piece: u32) -> anyhow::Result<()> {
        let staged = self.staging_path(piece);
        let path = self.piece_path(piece);
        // Before the rename: the staged handle names a file about to become
        // the complete one, and a cached complete handle -- the old copy a
        // re-download is replacing -- names bytes about to be unlinked.
        self.forget_handles(piece);
        let completed = match std::fs::rename(&staged, &path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound && path.is_file() => Ok(()),
            Err(e) => Err(anyhow::Error::new(e).context(format!(
                "could not move the completed piece {} into place at {}",
                staged.display(),
                path.display()
            ))),
        };
        if completed.is_ok() {
            self.staged.lock().remove(&piece);
        }
        completed
    }

    /// Reclaim one piece. This is the entry point the policy layer drives;
    /// it is deliberately not on the storage trait, because librqbit has no
    /// concept of giving a verified piece back.
    ///
    /// Takes the staged copy with it. A piece being downloaded again over one
    /// the caller is releasing has both, and half of a piece nobody wants is
    /// worth exactly as little as the whole of it.
    ///
    /// Returns whether a file was actually removed, so a caller counting what
    /// it freed does not have to stat first.
    pub fn delete_piece(&self, piece: u32) -> anyhow::Result<bool> {
        // Before the unlink, or a later read of the same piece would be
        // served the deleted bytes through the handle that outlived them.
        self.forget_handles(piece);
        self.staged.lock().remove(&piece);
        let mut removed = false;
        for path in [self.staging_path(piece), self.piece_path(piece)] {
            match std::fs::remove_file(&path) {
                Ok(()) => removed = true,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(anyhow::Error::new(e)
                        .context(format!("could not delete piece file {}", path.display())));
                }
            }
        }
        Ok(removed)
    }

    /// Whether any file of the torrent owns payload bytes in this piece.
    ///
    /// False only for a piece lying entirely inside padding or zero-length
    /// files, which nothing transfers and nothing ever writes.
    fn piece_has_an_owner(&self, piece: u32) -> bool {
        self.layout
            .files_overlapping_piece(piece)
            .any(|file| self.layout.owns_bytes(file))
    }

    /// Throw away staged bytes that shadow a piece we already have whole.
    ///
    /// A read prefers the staged copy, because the one time both exist within
    /// a session is a piece being downloaded again over one the policy layer
    /// dropped but has not deleted yet, and it is the new bytes the hash check
    /// has to see. A process that dies mid-re-download leaves that pair behind
    /// with the chunk bookkeeping that explained it gone, and the stale half
    /// would then shadow a verified piece for reads *and* for peers, since the
    /// complete file still standing is what makes it ours.
    ///
    /// A staged piece with no complete copy is left alone: it shadows nothing,
    /// [`Self::has_piece`] is false for it, and a pause is allowed to keep its
    /// in-flight work. Those become [`Self::staged`] -- the walk has just
    /// seen every staged file there is -- and a handle cached on a shadow
    /// that went is forgotten with it, so this is safe to run on a store
    /// that has been used, not only on a fresh one.
    fn discard_shadowing_staged(&self) -> anyhow::Result<()> {
        let mut kept = BTreeSet::new();
        let buckets = match std::fs::read_dir(&self.dir) {
            Ok(buckets) => buckets,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                *self.staged.lock() = kept;
                return Ok(());
            }
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!(
                    "could not read piece directory {}",
                    self.dir.display()
                )));
            }
        };
        for bucket in buckets.flatten() {
            let Ok(entries) = std::fs::read_dir(bucket.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let staged = entry.path();
                // Both halves of this go through `STAGING_SUFFIX` rather than
                // through a spelling of it. What a staged file is called is
                // one decision, taken in one place by `staging_path`, and a
                // second copy of it here would let the constant change while
                // this pass silently stopped finding anything -- which is not
                // a cosmetic failure: the stale shadow it exists to delete is
                // exactly what gets a half-written piece served as a verified
                // one.
                let name = entry.file_name();
                let Some(complete) = name
                    .to_str()
                    .and_then(|name| name.strip_suffix(STAGING_SUFFIX))
                else {
                    continue;
                };
                let piece = complete.parse::<u32>().ok();
                if staged.with_file_name(complete).is_file() {
                    if let Some(piece) = piece {
                        self.forget_handles(piece);
                    }
                    let _ = std::fs::remove_file(&staged);
                } else if let Some(piece) = piece {
                    kept.insert(piece);
                }
            }
        }
        *self.staged.lock() = kept;
        Ok(())
    }

    /// The cached handle for one copy of a piece, made the most recently
    /// used.
    fn cached_handle(&self, piece: u32, staged: bool) -> Option<Arc<File>> {
        let mut handles = self.handles.lock();
        let at = handles
            .iter()
            .position(|h| h.piece == piece && h.staged == staged)?;
        let handle = handles.remove(at);
        let file = handle.file.clone();
        handles.push(handle);
        Some(file)
    }

    /// Keep a freshly opened handle, dropping the least recently used one
    /// past [`OPEN_HANDLES`].
    fn remember_handle(&self, piece: u32, staged: bool, file: &Arc<File>) {
        let mut handles = self.handles.lock();
        handles.retain(|h| !(h.piece == piece && h.staged == staged));
        if handles.len() >= OPEN_HANDLES {
            handles.remove(0);
        }
        handles.push(OpenHandle {
            piece,
            staged,
            file: file.clone(),
        });
    }

    /// Drop every cached handle of a piece: its files are about to be
    /// renamed, deleted or shadowed.
    fn forget_handles(&self, piece: u32) {
        self.handles.lock().retain(|h| h.piece != piece);
    }

    #[cfg(test)]
    fn count_open(&self) {
        self.opens.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(not(test))]
    fn count_open(&self) {}

    #[cfg(test)]
    fn count_staging_probe(&self) {
        self.staging_probes.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(not(test))]
    fn count_staging_probe(&self) {}

    fn ensure_live(&self) -> anyhow::Result<()> {
        if self.live.load(Ordering::Acquire) {
            Ok(())
        } else {
            anyhow::bail!("this storage was taken; the torrent is paused or gone")
        }
    }

    /// The staged copy of a piece, open for writing -- the cached handle
    /// when there is one, otherwise the file, created along with its bucket
    /// directory if this is the first byte to land in it. The retry is not
    /// belt-and-braces: `remove_directory_if_empty` prunes bucket directories
    /// that have gone empty, so a bucket really can disappear between one
    /// write and the next.
    fn open_for_write(&self, piece: u32) -> anyhow::Result<Arc<File>> {
        if let Some(file) = self.cached_handle(piece, true) {
            return Ok(file);
        }
        if self.staged.lock().insert(piece) {
            // A new staged copy over a complete one: from here a read has
            // to see the staged bytes, so a cached complete handle -- the
            // old copy -- must not answer for the piece any more.
            self.forget_handles(piece);
        }
        let path = self.staging_path(piece);
        let mut opts = OpenOptions::new();
        // Read as well as write: the handle is cached under the staged copy
        // and the hash check reads that copy back through the same entry.
        opts.read(true).write(true).create(true).truncate(false);
        let file = match opts.open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("could not create piece directory {}", parent.display())
                    })?;
                }
                opts.open(&path)
                    .with_context(|| format!("could not create piece file {}", path.display()))?
            }
            Err(e) => {
                return Err(anyhow::Error::new(e)
                    .context(format!("could not open piece file {}", path.display())));
            }
        };
        self.count_open();
        let file = Arc::new(file);
        self.remember_handle(piece, true, &file);
        Ok(file)
    }

    /// The newest copy of a piece: the staged one if there is one, the
    /// complete one otherwise.
    ///
    /// That order and not the other. The two exist together only while a piece
    /// the policy layer dropped is being downloaded again before its old copy
    /// has been deleted, and the read that decides whether to keep the new
    /// bytes is the hash check itself -- served the old copy it would pass over
    /// bytes nothing wrote and promote whatever the new download had managed
    /// so far. Reading the newer copy cannot go wrong the other way, because
    /// rqbit does not count a piece it is downloading as ours: a peer's request
    /// for it is refused before it reaches storage, and a stream waits.
    ///
    /// Whether there is a staged copy is answered from [`Self::staged`], not
    /// by trying to open one: the store is told about every staged copy it
    /// creates and finds the rest at `init`, so a piece nothing is
    /// re-writing goes straight to its complete file. The set is advisory --
    /// a staged copy it names may have been completed a moment ago -- so
    /// its "yes" is still checked against the filesystem and falls through;
    /// only its "no" is trusted, and a wrong "no" would need a staged file
    /// this process neither wrote nor saw at `init`, which nothing makes.
    fn open_for_read(&self, piece: u32) -> anyhow::Result<Arc<File>> {
        if self.staged.lock().contains(&piece) {
            if let Some(file) = self.cached_handle(piece, true) {
                return Ok(file);
            }
            let staged = self.staging_path(piece);
            match File::open(&staged) {
                Ok(f) => {
                    self.count_open();
                    let file = Arc::new(f);
                    self.remember_handle(piece, true, &file);
                    return Ok(file);
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => self.count_staging_probe(),
                Err(e) => {
                    return Err(anyhow::Error::new(e)
                        .context(format!("could not open staged piece {}", staged.display())));
                }
            }
        }
        if let Some(file) = self.cached_handle(piece, false) {
            return Ok(file);
        }
        let path = self.piece_path(piece);
        match File::open(&path) {
            Ok(f) => {
                self.count_open();
                let file = Arc::new(f);
                self.remember_handle(piece, false, &file);
                Ok(file)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                Err(anyhow::Error::new(MissingPiece { piece }))
            }
            Err(e) => Err(anyhow::Error::new(e)
                .context(format!("could not open piece file {}", path.display()))),
        }
    }
}

/// `<dir>/<bucket>/<piece>` -- see [`PIECES_PER_DIRECTORY`].
pub(super) fn piece_path(dir: &Path, piece: u32) -> PathBuf {
    let mut path = dir.join((piece / PIECES_PER_DIRECTORY).to_string());
    path.push(piece.to_string());
    path
}

/// The same path with [`STAGING_SUFFIX`] on it: in the same bucket directory,
/// so promoting it is a rename and not a move across directories.
pub(super) fn staging_path(dir: &Path, piece: u32) -> PathBuf {
    let mut path = piece_path(dir, piece).into_os_string();
    path.push(STAGING_SUFFIX);
    path.into()
}

impl TorrentStorage for PieceStore {
    /// Nothing to open and nothing to pre-allocate -- just the torrent's own
    /// directory, so the first write does not have to race to create it, and
    /// the one piece of reconciliation the store owes a fresh process
    /// (`Self::discard_shadowing_staged`). The bucket directories are made on
    /// demand.
    fn init(
        &mut self,
        _shared: &librqbit::ManagedTorrentShared,
        _metadata: &librqbit::TorrentMetadata,
    ) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("could not create piece directory {}", self.dir.display()))?;
        self.discard_shadowing_staged()
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.ensure_live()?;
        let mut filled = 0usize;
        for segment in self.layout.segments(file_id, offset, buf.len() as u64)? {
            let len = segment.len as usize;
            let file = self.open_for_read(segment.piece)?;
            pread_exact_at(
                &file,
                segment.offset_in_piece,
                &mut buf[filled..filled + len],
            )
            .map_err(|e| {
                anyhow::Error::new(e).context(format!(
                    "reading {len} bytes at {} of piece {}",
                    segment.offset_in_piece, segment.piece
                ))
            })?;
            filled += len;
        }
        Ok(())
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.ensure_live()?;
        let mut written = 0usize;
        for segment in self.layout.segments(file_id, offset, buf.len() as u64)? {
            let len = segment.len as usize;
            let file = self.open_for_write(segment.piece)?;
            pwrite_all_at(&file, segment.offset_in_piece, &buf[written..written + len]).map_err(
                |e| {
                    anyhow::Error::new(e).context(format!(
                        "writing {len} bytes at {} of piece {}",
                        segment.offset_in_piece, segment.piece
                    ))
                },
            )?;
            written += len;
        }
        Ok(())
    }

    /// Drop what this file alone occupies.
    ///
    /// `filename` is ignored: this backend has no file by that name, the way
    /// the filesystem backend ignores `file_id` because it has the name. A
    /// piece is deleted only once every file that owns payload bytes in it has
    /// been removed, so unpinning one file of a multi-file torrent cannot take
    /// the neighbour's first or last piece with it. Padding and zero-length
    /// files own nothing and never hold a piece back.
    ///
    /// Whatever is left of a boundary piece after the last remove is picked up
    /// by [`super::sweep`] at the next launch, if the torrent is gone.
    fn remove_file(&self, file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        let pieces = self.layout.pieces_overlapping_file(file_id)?;
        let removed = {
            let mut removed = self.removed_files.lock();
            removed.insert(file_id);
            removed.clone()
        };
        let mut first_error = None;
        for piece in pieces {
            let still_wanted = self
                .layout
                .files_overlapping_piece(piece)
                .any(|other| self.layout.owns_bytes(other) && !removed.contains(&other));
            if still_wanted {
                continue;
            }
            if let Err(e) = self.delete_piece(piece) {
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The torrent's own directory (`path` empty, which is what
    /// `Session::delete` finishes with), or nothing.
    ///
    /// A torrent's directory tree does not exist here -- there is only the
    /// flat bucketed piece store -- so a request for one of its
    /// subdirectories has nothing to remove and says so by succeeding.
    /// Emptying the store leaves the bucket directories behind, so those are
    /// pruned first; "empty" then means "holds no pieces".
    fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
        if path != Path::new("") && path != Path::new(".") {
            return Ok(());
        }
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!(
                    "could not read piece directory {}",
                    self.dir.display()
                )));
            }
        };
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                // Fails while the bucket still holds pieces, which is the
                // answer we want.
                let _ = std::fs::remove_dir(entry.path());
            }
        }
        let _ = std::fs::remove_dir(&self.dir);
        Ok(())
    }

    /// A no-op: the piece file is the allocation unit and it grows as bytes
    /// arrive.
    ///
    /// This exists for the filesystem backend's pre-allocation, which is the
    /// reason the cache cleaner has to count `st_blocks` rather than lengths.
    /// Nothing here is ever longer than the bytes it holds, so there is
    /// nothing to grow and shrinking would throw data away. librqbit warns and
    /// carries on when this fails, so the honest answer is to succeed.
    fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
        Ok(())
    }

    /// Make a downloaded piece ours. Called after the hash check, so this is
    /// where the staged bytes become the have-record -- see
    /// [`Self::complete_piece`] and [`STAGING_SUFFIX`].
    ///
    /// At the pinned rev librqbit runs this *before* it sets the piece's
    /// have-bit, and treats an `Err` here as fatal to the torrent rather
    /// than advertise a piece it could not commit -- so "presence means
    /// complete" is now exactly the contract librqbit relies on: nothing is
    /// counted, advertised, served or readable through a stream until the
    /// rename here has returned `Ok`, and a rename that fails stops the
    /// torrent instead of leaving a have-bit over a half-committed piece.
    /// The staging-then-rename this store already did is what makes that
    /// safe, unchanged.
    fn on_piece_completed(
        &self,
        piece_index: librqbit_core::lengths::ValidPieceIndex,
    ) -> anyhow::Result<()> {
        self.complete_piece(piece_index.get())
    }

    /// Whether the data of this piece is still here, complete.
    ///
    /// This is what keeps the have-bitfield honest across a crash: librqbit
    /// starts from the resume data intersected with this, and the intersection
    /// can only clear bits. It has to be implemented here precisely because
    /// this store *can* lose a single piece behind librqbit's back -- that is
    /// what it is for -- and the default answer of "yes, and I would know
    /// otherwise" is right only for a storage that grows files and never
    /// punches holes in them.
    ///
    /// A piece with no owner answers yes. It lies entirely inside padding or
    /// zero-length files, nothing ever transfers or writes those bytes, so
    /// there is no file for [`Self::has_piece`] to find and there never will
    /// be -- and a piece nothing can write is a piece nothing can lose, which
    /// is exactly the question being asked. Saying no instead would clear a
    /// have-bit for a piece no download can ever set again.
    fn has_piece(
        &self,
        piece_index: librqbit_core::lengths::ValidPieceIndex,
    ) -> anyhow::Result<bool> {
        let piece = piece_index.get();
        Ok(self.has_piece(piece) || !self.piece_has_an_owner(piece))
    }

    /// Hand the data path to a successor and go dead, which is how librqbit
    /// pauses a torrent.
    ///
    /// The successor keeps the directory, the layout and the record of which
    /// files have been removed -- `Session::delete` takes the storage and then
    /// deletes *through what it got back*, so a successor that had forgotten
    /// where the pieces are would delete nothing.
    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        let successor = PieceStore {
            dir: self.dir.clone(),
            layout: self.layout.clone(),
            removed_files: Mutex::new(self.removed_files.lock().clone()),
            // The successor keeps knowing which pieces are staged -- a
            // paused torrent resumes writing them -- and opens its own
            // handles; the dead store's are closed, so a paused torrent
            // holds no descriptors.
            staged: Mutex::new(self.staged.lock().clone()),
            handles: Mutex::new(Vec::new()),
            live: AtomicBool::new(true),
            #[cfg(test)]
            opens: AtomicUsize::new(0),
            #[cfg(test)]
            staging_probes: AtomicUsize::new(0),
        };
        self.live.store(false, Ordering::Release);
        self.handles.lock().clear();
        Ok(Box::new(successor))
    }
}

/// Builds a [`PieceStore`] per torrent under one root.
///
/// The root is carried here rather than read from the torrent's own output
/// folder because `ManagedTorrentShared::options` is crate-private to
/// librqbit: an out-of-crate factory can see the info hash and the metadata
/// and nothing else. That is no loss -- the store wants one root of its own,
/// with a directory per info hash, and not the human-readable tree librqbit
/// would have written.
#[derive(Clone)]
pub struct PieceStoreFactory {
    root: Arc<PathBuf>,
}

impl PieceStoreFactory {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root: Arc::new(root),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// The layout a torrent's metadata implies.
///
/// Both numbers and the file table come from the same `TorrentMetadata`, so
/// this cannot see a torrent librqbit has not already validated.
pub fn layout_of(metadata: &librqbit::TorrentMetadata) -> anyhow::Result<PieceLayout> {
    let lengths = metadata.lengths();
    PieceLayout::new(
        lengths.default_piece_length() as u64,
        lengths.total_length(),
        metadata.file_infos.iter().map(|f| FileSpec {
            len: f.len,
            padding: f.attrs.padding,
        }),
    )
}

impl StorageFactory for PieceStoreFactory {
    type Storage = PieceStore;

    fn create(
        &self,
        shared: &librqbit::ManagedTorrentShared,
        metadata: &librqbit::TorrentMetadata,
    ) -> anyhow::Result<PieceStore> {
        let layout = layout_of(metadata)?;
        Ok(PieceStore::new(
            self.root.join(shared.info_hash.as_string()),
            Arc::new(layout),
        ))
    }

    /// Yes: this store is one file per piece, and releasing a piece is
    /// deleting its file ([`PieceStore::delete_piece`]) -- exactly what
    /// [`librqbit::AddTorrentOptions::piece_reclaim`] needs, and what
    /// `has_piece` answers from afterwards. So a torrent added on this
    /// factory may set `piece_reclaim`, and `drop_pieces` frees real bytes.
    /// This is a statement about the storage layout alone and is true
    /// whether or not the factory is the session default.
    fn ensure_can_release_pieces(&self) -> anyhow::Result<()> {
        Ok(())
    }

    // ensure_persistable stays the trait's default -- a bail naming this
    // factory -- deliberately, and it is not the same promise as the one
    // above. It asks whether a *restart* finds the data again, and a
    // restart replays the persisted record onto the session's *default*
    // factory (the record names an output folder and a file selection, no
    // storage), so this store can only keep that promise by *being* that
    // default. It is not wired in as the default yet (see the module doc's
    // "Not wired into the session yet"), so promising persistability now
    // would have a persistent session accept an add whose data the next
    // restart would look for on the filesystem factory and not find. The
    // wiring commit that makes this the default is what may make the
    // promise.

    fn clone_box(&self) -> librqbit::storage::BoxStorageFactory {
        use librqbit::storage::StorageFactoryExt;
        self.clone().boxed()
    }
}

#[cfg(unix)]
fn pread_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(unix)]
fn pwrite_all_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

/// Windows has no `pread`/`pwrite`; `seek_read`/`seek_write` are positioned
/// but may come back short, so both loops are written out here. Same shape as
/// librqbit's own Windows path.
#[cfg(windows)]
fn pread_exact_at(file: &File, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "piece file ended before the read was filled",
                ));
            }
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn pwrite_all_at(file: &File, mut offset: u64, mut buf: &[u8]) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "could not write the whole piece segment",
                ));
            }
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::piece_store::layout::FileSpec;

    /// A torrent shaped to exercise every case at once over 8-byte pieces:
    ///
    /// ```text
    /// global   0        8        16       24    30
    ///          |--------|--------|--------|------|   pieces 0,1,2,3 (3 is short)
    /// files    [ f0: 10 ][f1 pad:6][ f2: 13    ][f3]
    /// ```
    ///
    /// Piece 0 is file 0's alone; piece 1 is two bytes of file 0 and then
    /// padding nobody writes; piece 2 is file 2's alone; piece 3 spans files 2
    /// and 3.
    const SPECS: [FileSpec; 4] = [
        FileSpec {
            len: 10,
            padding: false,
        },
        FileSpec {
            len: 6,
            padding: true,
        },
        FileSpec {
            len: 13,
            padding: false,
        },
        FileSpec {
            len: 1,
            padding: false,
        },
    ];
    const PIECE_LENGTH: u64 = 8;

    /// The torrent's global bytes, distinct enough that a byte landing one
    /// piece over is visible.
    fn global_bytes(total: u64) -> Vec<u8> {
        (0..total).map(|i| (i.wrapping_mul(7) + 3) as u8).collect()
    }

    fn file_offset(file_id: usize) -> u64 {
        SPECS[..file_id].iter().map(|f| f.len).sum()
    }

    fn open_store(dir: &Path, piece_length: u64, specs: &[FileSpec]) -> PieceStore {
        let total = specs.iter().map(|f| f.len).sum();
        let layout = PieceLayout::new(piece_length, total, specs.iter().copied()).expect("layout");
        std::fs::create_dir_all(dir).unwrap();
        PieceStore::new(dir.to_path_buf(), Arc::new(layout))
    }

    /// Write every payload file the way librqbit does: in small chunks whose
    /// alignment to pieces is whatever the file's own offset makes it, and then
    /// complete every piece the writes finished. Padding files are not written,
    /// because librqbit never writes one.
    ///
    /// The completion step is not ceremony. Bytes land under the staging name
    /// and only [`PieceStore::complete_piece`] makes a piece ours, so a fill
    /// that skipped it would leave a store with nothing in it as far as
    /// [`PieceStore::has_piece`] is concerned -- which is the whole point.
    fn fill(store: &PieceStore, global: &[u8], chunk: u64) {
        write_only(store, global, chunk);
        for piece in 0..store.layout().piece_count() {
            if store.piece_has_an_owner(piece) {
                store.complete_piece(piece).expect("complete");
            }
        }
    }

    /// [`fill`] without the completion step: what a download in flight looks
    /// like.
    fn write_only(store: &PieceStore, global: &[u8], chunk: u64) {
        for (file_id, spec) in SPECS.iter().enumerate() {
            if spec.padding {
                continue;
            }
            let base = file_offset(file_id) as usize;
            let mut offset = 0u64;
            while offset < spec.len {
                let len = chunk.min(spec.len - offset);
                let at = base + offset as usize;
                store
                    .pwrite_all(file_id, offset, &global[at..at + len as usize])
                    .expect("write");
                offset += len;
            }
        }
    }

    #[test]
    fn bytes_written_through_the_mapping_read_back_the_same() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let total = store.layout().total_length();
        let global = global_bytes(total);
        // Chunk sizes chosen so writes land piece-aligned, straddling one
        // boundary, and straddling two.
        for chunk in [1u64, 3, 7, 8, 13, 32] {
            fill(&store, &global, chunk);
            for (file_id, spec) in SPECS.iter().enumerate() {
                if spec.padding {
                    continue;
                }
                let base = file_offset(file_id) as usize;
                for offset in 0..=spec.len {
                    for len in 0..=(spec.len - offset) {
                        let mut buf = vec![0u8; len as usize];
                        store
                            .pread_exact(file_id, offset, &mut buf)
                            .unwrap_or_else(|e| {
                                panic!("chunk {chunk}, file {file_id} at {offset}+{len}: {e:#}")
                            });
                        let at = base + offset as usize;
                        assert_eq!(
                            buf,
                            &global[at..at + len as usize],
                            "chunk {chunk}, file {file_id} at {offset}+{len}"
                        );
                    }
                }
            }
        }
    }

    /// The proof that the mapping is right is on the volume, not in our own
    /// accounting: each piece file must hold exactly that piece's slice of the
    /// torrent's global bytes.
    #[test]
    fn each_piece_file_holds_exactly_its_own_slice_of_the_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 3);

        assert_eq!(std::fs::read(store.piece_path(0)).unwrap(), global[0..8]);
        assert_eq!(
            std::fs::read(store.piece_path(1)).unwrap(),
            global[8..10],
            "piece 1 ends in a padding file nobody writes, so its file stops \
             at the last real byte instead of being padded out"
        );
        assert_eq!(std::fs::read(store.piece_path(2)).unwrap(), global[16..24]);
        assert_eq!(
            std::fs::read(store.piece_path(3)).unwrap(),
            global[24..30],
            "the last piece is short and spans two files"
        );
        assert_eq!(store.layout().piece_count(), 4);
        assert!(!store.piece_path(4).exists());
    }

    #[test]
    fn a_deleted_piece_fails_cleanly_instead_of_reading_as_zeroes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 4);

        assert!(store.has_piece(2));
        assert!(store.delete_piece(2).unwrap(), "it was there");
        assert!(
            !store.delete_piece(2).unwrap(),
            "and deleting is idempotent"
        );
        assert!(!store.piece_path(2).exists());

        // File 2 begins exactly at piece 2, so its first eight bytes are the
        // ones that went.
        let err = store.pread_exact(2, 0, &mut [0u8; 8]).unwrap_err();
        assert!(
            err.chain().any(|e| e
                .downcast_ref::<MissingPiece>()
                .is_some_and(|m| m.piece == 2)),
            "a missing piece is its own error, not an I/O failure: {err:#}"
        );
        let mut buf = [0xffu8; 5];
        store
            .pread_exact(2, 8, &mut buf)
            .expect("the pieces either side are untouched");
        assert_eq!(buf, global[24..29]);
    }

    /// "Gone" has to mean gone from the volume, so this counts allocated
    /// blocks rather than trusting the store's own bookkeeping. A piece large
    /// enough that the answer is not rounding.
    #[test]
    fn deleting_a_piece_gives_the_bytes_back_to_the_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let piece_length = 256 * 1024;
        let specs = [FileSpec::payload(piece_length * 4)];
        let store = open_store(tmp.path(), piece_length, &specs);
        let payload = vec![0xa5u8; piece_length as usize * 4];
        store.pwrite_all(0, 0, &payload).unwrap();
        for piece in 0..4 {
            store.complete_piece(piece).unwrap();
        }

        let before = allocated(tmp.path());
        assert!(
            before >= piece_length * 4,
            "the payload is really on disk: {before}"
        );
        assert!(store.delete_piece(1).unwrap());
        let after = allocated(tmp.path());
        assert!(
            before - after >= piece_length,
            "deleting a piece freed {} bytes, not the {piece_length} it held",
            before - after
        );
    }

    /// Ask the storage trait's own question, which needs a `ValidPieceIndex`
    /// and therefore the torrent's `Lengths`.
    fn storage_has_piece(store: &PieceStore, piece: u32) -> bool {
        let lengths = librqbit_core::lengths::Lengths::new(
            store.layout().total_length(),
            store.layout().default_piece_length() as u32,
        )
        .expect("lengths");
        TorrentStorage::has_piece(
            store,
            lengths.validate_piece_index(piece).expect("in range"),
        )
        .expect("has_piece")
    }

    /// A piece is ours at one instant and not before: the one where the hash
    /// check has passed and `on_piece_completed` moves it into place. A wrong
    /// "yes" here is not a re-download, it is a have-bit standing over bytes
    /// nothing verified.
    #[test]
    fn a_piece_is_not_ours_until_it_is_complete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());

        // File 0's first chunk. It creates piece 0's file, and the obvious
        // answer -- "there is a file" -- would say yes for the whole of the
        // download.
        store.pwrite_all(0, 0, &global[0..2]).unwrap();
        assert!(store.staging_path(0).is_file(), "the bytes are on disk");
        assert!(!store.has_piece(0), "and the piece is not ours yet");
        assert!(!storage_has_piece(&store, 0));

        write_only(&store, &global, 4);
        assert!(!store.has_piece(0), "still not, with every byte written");
        store.complete_piece(0).unwrap();
        assert!(store.has_piece(0) && storage_has_piece(&store, 0));
        assert!(
            !store.staging_path(0).exists(),
            "and the staged name is gone"
        );
        assert_eq!(std::fs::read(store.piece_path(0)).unwrap(), global[0..8]);
        assert!(!store.has_piece(2), "the others are untouched by it");

        store
            .complete_piece(0)
            .expect("completing a piece already in place is not a failure");
    }

    /// A piece nothing can write is a piece nothing can lose. Piece 1 here is
    /// the payload end of file 0 plus padding, so it does get a file; the
    /// question is asked of a torrent shaped so that a piece has no owner at
    /// all, which is the hole in "presence is the have-set" the module doc
    /// names.
    #[test]
    fn a_piece_no_file_owns_can_never_be_lost() {
        let specs = [
            FileSpec::payload(8),
            FileSpec::padding(8),
            FileSpec::payload(8),
        ];
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &specs);
        assert!(store.piece_has_an_owner(0) && store.piece_has_an_owner(2));
        assert!(!store.piece_has_an_owner(1), "piece 1 is padding alone");

        assert!(!store.has_piece(1), "nothing will ever write it");
        assert!(
            storage_has_piece(&store, 1),
            "so it cannot go missing, and answering no would clear a have-bit \
             no download could ever set again"
        );
        assert!(
            !storage_has_piece(&store, 0),
            "a piece with an owner is not"
        );
    }

    /// The one time a piece has two copies is a dropped piece being downloaded
    /// again before the policy layer has deleted the old one, and the read that
    /// decides whether to keep the new bytes is the hash check itself.
    #[test]
    fn a_read_gets_the_newest_copy_of_a_piece() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 8);

        // Piece 2 is file 2's alone, and file 2 starts there. Read first,
        // so the complete copy's handle is the cached one when the new
        // staged copy begins -- the cache must not let it answer.
        let mut buf = [0u8; 8];
        store.pread_exact(2, 0, &mut buf).unwrap();
        assert_eq!(buf, global[16..24]);
        let again: Vec<u8> = global[16..24].iter().map(|b| !b).collect();
        store.pwrite_all(2, 0, &again).unwrap();
        assert!(
            store.has_piece(2),
            "the complete copy is still there: it is what the caller has not \
             released yet"
        );
        store.pread_exact(2, 0, &mut buf).unwrap();
        assert_eq!(buf.to_vec(), again, "a read gets what was written last");

        store.complete_piece(2).unwrap();
        assert_eq!(std::fs::read(store.piece_path(2)).unwrap(), again);
        store.pread_exact(2, 0, &mut buf).unwrap();
        assert_eq!(buf.to_vec(), again);
    }

    /// The open count is the reason the handle cache exists: a piece is
    /// written in 16 KiB chunks and streamed in 8 KiB reads, and each of
    /// those used to be an open and a close -- two opens for a read, since
    /// the staging name was probed first. Now a piece written end to end
    /// and a piece streamed end to end are one open each, and a complete
    /// piece's read never asks the filesystem about a staged copy.
    #[test]
    fn a_piece_is_opened_once_to_write_and_once_to_stream() {
        let tmp = tempfile::tempdir().unwrap();
        let piece_length = 256 * 1024;
        let pieces = 4u64;
        let specs = [FileSpec::payload(piece_length * pieces)];
        let store = open_store(tmp.path(), piece_length, &specs);
        let payload: Vec<u8> = (0..piece_length * pieces)
            .map(|i| (i % 253) as u8)
            .collect();

        for (n, chunk) in payload.chunks(16 * 1024).enumerate() {
            store.pwrite_all(0, (n * 16 * 1024) as u64, chunk).unwrap();
        }
        assert_eq!(
            store.opens.load(Ordering::Relaxed),
            pieces as usize,
            "one open per piece written, not per chunk"
        );
        for piece in 0..pieces as u32 {
            store.complete_piece(piece).unwrap();
        }

        store.opens.store(0, Ordering::Relaxed);
        let mut buf = vec![0u8; 8 * 1024];
        for n in 0..(payload.len() / buf.len()) {
            let at = n * buf.len();
            store.pread_exact(0, at as u64, &mut buf).unwrap();
            assert_eq!(buf, payload[at..at + buf.len()]);
        }
        assert_eq!(
            store.opens.load(Ordering::Relaxed),
            pieces as usize,
            "one open per piece streamed, not per read"
        );
        assert_eq!(
            store.staging_probes.load(Ordering::Relaxed),
            0,
            "a complete piece nothing is re-writing is never probed for a staged copy"
        );
    }

    /// A cached handle keeps a deleted file's bytes readable for as long as
    /// it is held, so the cache must let go of a piece before the piece is
    /// deleted -- or a read after the delete would answer with bytes the
    /// store just said were gone.
    #[test]
    fn a_cached_handle_does_not_outlive_its_piece() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 8);

        let mut buf = [0u8; 8];
        store.pread_exact(2, 0, &mut buf).unwrap();
        assert!(store.delete_piece(2).unwrap());
        let err = store.pread_exact(2, 0, &mut buf).unwrap_err();
        assert!(
            err.chain()
                .any(|e| e.downcast_ref::<MissingPiece>().is_some()),
            "the read sees the deletion, not the old handle: {err:#}"
        );

        // The same for a file's removal, which goes through delete_piece.
        store.pread_exact(0, 0, &mut buf).unwrap();
        store.remove_file(0, Path::new("f0")).unwrap();
        assert!(store.pread_exact(0, 0, &mut buf).is_err());
    }

    /// What a fresh process knows about staged copies comes from `init`'s
    /// walk, and it has to: a read of a staged piece that only a previous
    /// process wrote must still get the staged bytes, not "missing".
    #[test]
    fn init_learns_which_pieces_a_previous_process_left_staged() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        // Piece 0 staged only; piece 2 complete with a stale shadow.
        store.pwrite_all(0, 0, &global[0..8]).unwrap();
        fill_piece(&store, &global, 2);
        store.pwrite_all(2, 0, &[0u8; 3]).unwrap();

        let fresh = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        assert!(
            fresh.staged.lock().is_empty(),
            "a store that has not run init knows nothing yet"
        );
        fresh.discard_shadowing_staged().unwrap();
        assert_eq!(
            *fresh.staged.lock(),
            BTreeSet::from([0]),
            "the lone staged piece is known; the shadow was discarded, not recorded"
        );
        let mut buf = [0u8; 8];
        fresh.pread_exact(0, 0, &mut buf).unwrap();
        assert_eq!(buf, global[0..8], "read through the staged copy");
        fresh.pread_exact(2, 0, &mut buf).unwrap();
        assert_eq!(
            buf,
            global[16..24],
            "and the verified one where the shadow was"
        );
    }

    /// Complete exactly one piece of the store: written under its staging
    /// name and moved into place, like a download of only that piece.
    fn fill_piece(store: &PieceStore, global: &[u8], piece: u32) {
        let start = piece as u64 * PIECE_LENGTH;
        let end = (start + PIECE_LENGTH).min(global.len() as u64);
        for (file_id, spec) in SPECS.iter().enumerate() {
            let base = file_offset(file_id);
            if spec.padding || base + spec.len <= start || base >= end {
                continue;
            }
            let from = start.max(base);
            let to = end.min(base + spec.len);
            store
                .pwrite_all(file_id, from - base, &global[from as usize..to as usize])
                .expect("write");
        }
        store.complete_piece(piece).expect("complete");
    }

    /// The pair above is explained by bookkeeping that does not survive the
    /// process. What is left after a kill is a half-written copy shadowing a
    /// piece we really do have -- and the complete file is what makes it ours,
    /// so peers would be served the shadow. `init` is where that is undone.
    #[test]
    fn a_stale_staged_copy_over_a_complete_piece_goes_at_init() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 8);
        store.pwrite_all(2, 0, &[0u8; 3]).unwrap();
        // And a piece whose only copy is staged: in flight when the process
        // died, shadowing nothing.
        store.delete_piece(0).unwrap();
        store.pwrite_all(0, 0, &global[0..4]).unwrap();

        // What `init` does on the next launch, which is the only place a
        // half-written piece can be told apart from one being written now.
        store.discard_shadowing_staged().unwrap();

        assert!(!store.staging_path(2).exists(), "the shadow went");
        let mut buf = [0u8; 8];
        store.pread_exact(2, 0, &mut buf).unwrap();
        assert_eq!(buf, global[16..24], "and the verified piece is what reads");
        assert!(
            store.staging_path(0).is_file(),
            "a staged piece with nothing behind it shadows nothing and stays"
        );
        assert!(!store.has_piece(0));
    }

    /// Deleting a piece takes both copies. Half of a piece nobody wants is
    /// worth exactly as little as the whole of it.
    #[test]
    fn deleting_a_piece_takes_the_staged_copy_with_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 8);
        store.pwrite_all(2, 0, &[0u8; 3]).unwrap();
        assert!(store.staging_path(2).is_file() && store.piece_path(2).is_file());

        assert!(store.delete_piece(2).unwrap());
        assert!(!store.staging_path(2).exists() && !store.piece_path(2).exists());
        assert!(!store.delete_piece(2).unwrap(), "and it stays idempotent");

        // Same for a file being removed, which goes through delete_piece.
        store.pwrite_all(0, 0, &[0u8; 3]).unwrap();
        store.remove_file(0, Path::new("f0")).unwrap();
        assert!(!store.staging_path(0).exists() && !store.piece_path(0).exists());
    }

    /// Removing one file must not take a boundary piece its neighbour still
    /// wants -- the failure mode a whole-file backend cannot have, since there
    /// the file *is* the unit.
    #[test]
    fn removing_a_file_spares_the_pieces_its_neighbours_still_want() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 5);

        // File 2 ends inside piece 3, which file 3 also has a byte in.
        store.remove_file(2, Path::new("f2")).unwrap();
        assert!(!store.has_piece(2), "piece 2 was file 2's alone");
        assert!(store.has_piece(3), "piece 3 still holds file 3's last byte");
        assert_eq!(
            store.pread_exact(3, 0, &mut [0u8; 1]).ok(),
            Some(()),
            "and file 3 can still be read"
        );

        store.remove_file(3, Path::new("f3")).unwrap();
        assert!(!store.has_piece(3), "with its last owner gone, so does it");

        // A padding file owns nothing, so file 0 leaving takes both the piece
        // it had to itself and the one it only shares with padding.
        assert!(store.has_piece(0) && store.has_piece(1));
        store.remove_file(0, Path::new("f0")).unwrap();
        assert!(!store.has_piece(0));
        assert!(
            !store.has_piece(1),
            "padding cannot be what keeps a piece alive"
        );
    }

    #[test]
    fn taking_the_storage_moves_the_data_path_and_keeps_the_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 8);
        store.remove_file(2, Path::new("f2")).unwrap();

        // A piece being re-downloaded when the torrent pauses: the
        // successor has to know its staged copy is the one to read.
        let again: Vec<u8> = global[0..8].iter().map(|b| !b).collect();
        store.pwrite_all(0, 0, &again).unwrap();
        store.pread_exact(0, 0, &mut [0u8; 4]).unwrap();
        assert!(!store.handles.lock().is_empty(), "handles are open");

        let successor = store.take().unwrap();
        assert!(
            store.pread_exact(0, 0, &mut [0u8; 4]).is_err(),
            "the taken storage is dead"
        );
        assert!(store.pwrite_all(0, 0, &[0u8; 4]).is_err());
        assert!(
            store.handles.lock().is_empty(),
            "and holds no descriptors: a paused torrent keeps no files open"
        );
        let mut buf = [0u8; 4];
        successor.pread_exact(0, 0, &mut buf).unwrap();
        assert_eq!(
            buf,
            again[0..4],
            "the successor kept the directory, and knows which copy is newest"
        );

        // `Session::delete` deletes through what `take` gave it, so the
        // successor has to remember that file 2 is already gone -- otherwise
        // piece 3 would outlive every one of its owners.
        successor.remove_file(3, Path::new("f3")).unwrap();
        assert!(!tmp.path().join("0").join("3").exists());
    }

    #[test]
    fn emptying_the_store_removes_the_buckets_and_then_the_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("abc");
        let store = open_store(&dir, PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 8);

        store
            .remove_directory_if_empty(Path::new("some/sub/dir"))
            .expect("no such tree here, and nothing to report about it");
        store.remove_directory_if_empty(Path::new("")).unwrap();
        assert!(dir.is_dir(), "it still holds pieces");

        for piece in 0..store.layout().piece_count() {
            store.delete_piece(piece).unwrap();
        }
        store.remove_directory_if_empty(Path::new("")).unwrap();
        assert!(!dir.exists(), "buckets and all");
        store
            .remove_directory_if_empty(Path::new(""))
            .expect("and again on a directory that is already gone");
    }

    /// Pre-allocation is what makes the cache full of sparse files whose
    /// apparent length is the whole film. There is nothing here to
    /// pre-allocate, and shrinking would throw data away.
    #[test]
    fn ensure_file_length_changes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open_store(tmp.path(), PIECE_LENGTH, &SPECS);
        let global = global_bytes(store.layout().total_length());
        fill(&store, &global, 8);
        let before = allocated(tmp.path());
        store.ensure_file_length(0, 10).unwrap();
        store.ensure_file_length(2, 13).unwrap();
        assert_eq!(allocated(tmp.path()), before);
        assert_eq!(std::fs::read(store.piece_path(0)).unwrap(), global[0..8]);
    }

    /// Inode pressure is a known cost of this design, not a bug: a 27 GB
    /// torrent at 4 MB pieces is ~6,750 files. What must not happen is all of
    /// them in one directory, which is a linear scan per open on the
    /// filesystems a phone's removable storage actually uses.
    #[test]
    fn a_large_torrent_fans_out_instead_of_filling_one_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let pieces = 6750u64;
        let piece_length = 16u64;
        let specs = [FileSpec::payload(pieces * piece_length)];
        let store = open_store(tmp.path(), piece_length, &specs);
        for piece in 0..pieces {
            store.pwrite_all(0, piece * piece_length, &[7u8]).unwrap();
            store.complete_piece(piece as u32).unwrap();
        }

        let mut buckets = 0usize;
        let mut widest = 0usize;
        let mut files = 0usize;
        for entry in std::fs::read_dir(tmp.path()).unwrap() {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_dir(), "buckets only");
            buckets += 1;
            let held = std::fs::read_dir(entry.path()).unwrap().count();
            widest = widest.max(held);
            files += held;
        }
        assert_eq!(files, pieces as usize, "one file per piece, no more");
        assert_eq!(
            buckets, 7,
            "6750 pieces over {PIECES_PER_DIRECTORY} a bucket"
        );
        assert!(
            widest <= PIECES_PER_DIRECTORY as usize,
            "a directory grew to {widest} entries"
        );
        assert!(store.has_piece(6749) && !store.has_piece(6750));
    }

    fn allocated(dir: &Path) -> u64 {
        let mut total = 0;
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                total += allocated(&entry.path());
            } else {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    total += meta.blocks() * 512;
                }
                #[cfg(not(unix))]
                {
                    total += meta.len();
                }
            }
        }
        total
    }
}

/// The storage driven by a real librqbit session, which is the only way to
/// prove the mapping against the code that will use it: librqbit verifies a
/// piece by reading it back, and its initial check reads every piece of every
/// file through `pread_exact` at whatever alignment the torrent happens to
/// have.
#[cfg(test)]
mod librqbit_tests {
    use super::*;
    use librqbit::storage::StorageFactoryExt;
    use std::time::{Duration, Instant};

    /// Generous on purpose: it exists so a regression fails instead of
    /// hanging, not as a timing assertion.
    const WAIT_BOUND: Duration = Duration::from_secs(60);

    async fn hermetic_session(dir: std::path::PathBuf) -> Arc<librqbit::Session> {
        tokio::fs::create_dir_all(&dir).await.unwrap();
        librqbit::Session::new_with_opts(
            dir,
            librqbit::SessionOptions {
                // No DHT, no listener, no persistence: this test must not
                // touch the network or leave state behind.
                dht: None,
                listen: None,
                persistence: None,
                ..Default::default()
            },
        )
        .await
        .expect("hermetic session")
    }

    async fn add(
        session: &Arc<librqbit::Session>,
        bytes: &[u8],
        root: &Path,
    ) -> Arc<librqbit::ManagedTorrent> {
        let response = session
            .add_torrent(
                librqbit::AddTorrent::from_bytes(bytes.to_vec()),
                Some(librqbit::AddTorrentOptions {
                    storage_factory: Some(PieceStoreFactory::new(root.to_path_buf()).boxed()),
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
        handle
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

    #[tokio::test(flavor = "multi_thread")]
    async fn librqbit_verifies_every_piece_it_reads_back_through_the_mapping() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        tokio::fs::create_dir_all(&src).await.unwrap();
        // Lengths that are not multiples of the 16 KiB piece length, so a
        // piece spans the two files and the last piece is short.
        for (name, len) in [("a.bin", 20_000usize), ("b.bin", 12_345)] {
            let payload: Vec<u8> = (0..len).map(|i| (i.wrapping_mul(31) + 7) as u8).collect();
            tokio::fs::write(src.join(name), payload).await.unwrap();
        }
        let torrent = librqbit::create_torrent(
            &src,
            librqbit::CreateTorrentOptions {
                name: None,
                trackers: Vec::new(),
                piece_length: Some(16384),
            },
            &librqbit::spawn_utils::BlockingSpawner::new(1),
        )
        .await
        .expect("create torrent");
        let bytes = torrent.as_bytes().expect("serialize").to_vec();
        let info_hash = torrent.info_hash().as_string();
        let root = tmp.path().join("pieces");

        // An empty store is an ordinary state, not an error: every read of
        // the initial check fails with `MissingPiece` and the torrent comes
        // up with nothing.
        let empty = hermetic_session(tmp.path().join("s1")).await;
        let handle = add(&empty, &bytes, &root).await;
        let stats = settled(&handle).await;
        assert_eq!(stats.error, None, "a store with no pieces is not a failure");
        assert_eq!(stats.progress_bytes, 0);
        let total = stats.total_bytes;

        // Fill the store the way a swarm would, by file and offset.
        let layout = Arc::new(
            handle
                .with_metadata(|m| layout_of(m))
                .expect("metadata")
                .expect("layout"),
        );
        let store = PieceStore::new(root.join(&info_hash), layout.clone());
        let files: Vec<(usize, std::path::PathBuf)> = handle
            .with_metadata(|m| {
                m.file_infos
                    .iter()
                    .enumerate()
                    .map(|(idx, fi)| (idx, src.join(&fi.relative_filename)))
                    .collect()
            })
            .expect("metadata");
        for (file_id, path) in files {
            let payload = std::fs::read(&path).unwrap();
            // 3000-byte writes so nothing is piece-aligned.
            for (n, chunk) in payload.chunks(3000).enumerate() {
                store
                    .pwrite_all(file_id, (n * 3000) as u64, chunk)
                    .expect("write");
            }
        }
        // Every piece is whole, so promote them all: until that happens the
        // store holds no piece at all, which is what makes presence mean
        // complete.
        for piece in 0..layout.piece_count() {
            assert!(!store.has_piece(piece), "staged, not ours");
            store.complete_piece(piece).expect("complete");
            assert!(store.has_piece(piece));
        }
        empty
            .delete(handle.info_hash().into(), false)
            .await
            .expect("stop");

        // A fresh session over the same store: its initial check reads every
        // piece back through the mapping and hashes it against the torrent.
        let filled = hermetic_session(tmp.path().join("s2")).await;
        let handle = add(&filled, &bytes, &root).await;
        let stats = settled(&handle).await;
        assert_eq!(stats.error, None, "{stats}");
        assert_eq!(
            stats.progress_bytes, total,
            "every piece hashed to what the torrent says it should: {stats}"
        );
        assert!(stats.finished);

        // Reclaim the last piece. Its bytes are the only ones that go: the
        // pieces around it still verify.
        //
        // The *last* piece on purpose. librqbit's initial check marks a file
        // broken on its first read error and skips the rest of that file
        // without reading it, so a gap in the middle would report every later
        // piece of that file as missing too, whatever is on disk. That is the
        // reason the have-set cannot come from the check -- see the module
        // doc -- and not something this backend can fix from below.
        let last = layout.piece_count() - 1;
        assert!(store.delete_piece(last).unwrap());
        let short = hermetic_session(tmp.path().join("s3")).await;
        let handle = add(&short, &bytes, &root).await;
        let stats = settled(&handle).await;
        assert_eq!(stats.error, None, "{stats}");
        assert!(!stats.finished);
        assert_eq!(
            stats.progress_bytes,
            total - layout.piece_length_of(last),
            "exactly the reclaimed piece is missing: {stats}"
        );
    }
}

//! One chunk store, for the torrent pieces and for `/proxy` alike.
//!
//! A chunk store is "store chunks; read them back as chunks (to serve a peer)
//! or as a random-access byte range (to play)". That is what a torrent's
//! pieces need and it is what a cache for HTTP range requests needs, so there
//! is one of them and not two. What is here is the whole of it: the directory
//! shape, the bucketing, the staging spellings, the typed read, the listing,
//! the occupancy, and the LRU of open handles.
//!
//! # The two adapters
//!
//! * [`crate::piece_store::store::PieceStore`] -- a
//!   [`librqbit::storage::TorrentStorage`] over a [`ChunkDir`] and a
//!   [`crate::piece_store::PieceLayout`], which is where a multi-file
//!   torrent's global byte space and its BEP-47 padding live. That
//!   arithmetic sits *above* a chunk store; none of it is here.
//! * `server::proxy_cache` -- a byte-range reader and writer over a
//!   [`ChunkDir`] per cached entity, whose arithmetic is `offset / CHUNK`.
//!
//! # The two parameters, and they are the only two
//!
//! * **Staging identity.** A chunk's bytes go to a name beside the final one
//!   and are renamed into place, so presence at the final name means
//!   *complete* -- for both adapters, and it already did for both. What
//!   differs is whether the staged copy is *addressable*. librqbit writes a
//!   piece 16 KiB at a time, reads it back through the same handle for the
//!   hash check, and may be interrupted and resume it, so its staged copy has
//!   one fixed name ([`ChunkDir::staging_path`]) and survives a restart. A
//!   `/proxy` chunk is buffered whole in memory and written once, and two
//!   readers of one stream may be filling the same chunk at the same time, so
//!   its staged copy is anonymous ([`ChunkDir::write_whole`]) and a kill
//!   leaves nothing that could be resumed. Collapsing to the named form alone
//!   would let two fillers interleave into one file; collapsing to the
//!   anonymous form alone would break the incremental write and the recovery
//!   of a staged piece at launch.
//! * **The commit trigger.** [`ChunkDir::commit`] is the rename and nothing
//!   else; *when* it runs is the adapter's. librqbit calls it after its own
//!   SHA-1 check. A URL response has no hash, so the proxy calls it once the
//!   expected byte count has been buffered, and passes that count as
//!   `expected_len` so the store refuses a commit that disagrees with it.
//!   The torrent adapter passes `None`, and must: librqbit never writes
//!   BEP-47 padding, so a piece whose tail is padding is committed *short*,
//!   and there is no length a legal padded piece would satisfy.
//!
//! # What is deliberately not parameterised
//!
//! The chunk size. A `/proxy` chunk is 256 KiB because of what a chunk
//! boundary costs a fetch and what an uncompleted chunk costs in memory; a
//! torrent's piece length is the swarm's, and is the unit its SHA-1s are
//! over. The store is *told* an index and never computes one, so it owns
//! neither number.
//!
//! # Reads are typed, listings are lenient
//!
//! [`ChunkDir::open_complete`] tells [`ChunkError::Missing`] from
//! [`ChunkError::Io`], because under this design the presence of a chunk file
//! *is* the have-record, so "not there" is an ordinary state a reclaim
//! creates deliberately and the layer above has to be able to tell it from a
//! disk that is failing -- a read of an absent piece that returned zeroes
//! would have a hash check call a hole a verified piece. The listings
//! ([`ChunkDir::held`], [`ChunkDir::held_in_bucket`], [`ChunkDir::stat`])
//! swallow instead: a caller counting the disk must not be stopped by one
//! directory it may not enter.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{File, Metadata, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// How many chunk files share one directory.
///
/// Flat would work on ext4 and f2fs, which hash directory entries, but not
/// everywhere the cache can land: a 27 GB torrent at 4 MB pieces is ~6,750
/// files, and a 2 GB proxied film is eight thousand, and on a filesystem that
/// scans a directory linearly (exFAT and FAT32 on a phone's SD card, which is
/// exactly where a large offline download and a phone's proxy cache go) every
/// open in a directory that size walks the entries. Bucketing by a thousand
/// puts a ceiling on that -- seven directories of at most a thousand for that
/// torrent -- for the cost of one extra path component. A thousand rather
/// than a power of two because the names are decimal, and `4/4200` reading as
/// "chunk 4200" is worth more when reading a directory listing by hand than
/// the shift it saves.
pub const CHUNKS_PER_DIRECTORY: u64 = 1000;

/// What a chunk file is called while it is still being written.
///
/// A torrent piece arrives 16 KiB at a time, so a file created by its first
/// chunk is there for the whole of the download -- and the contract is that
/// presence means **complete**, because a wrong "yes" is not a re-download
/// but silent corruption: the have-set a restart starts from is the resume
/// data intersected with what the storage says it still holds, the
/// intersection can only clear bits, and the fastresume hash check samples
/// ~65 pieces of a torrent however large. So the bytes go to a name carrying
/// this suffix and are renamed into place by [`ChunkDir::commit`]. A rename
/// within one directory is atomic on every filesystem here.
pub const STAGING_SUFFIX: &str = ".part";

/// How many chunk files an [`OpenChunks`] keeps open.
///
/// librqbit has a handful of pieces in flight for a torrent and a stream
/// reads one piece at a time in order, so a few entries cover the working
/// set; the point is the ratio (one open per chunk instead of one per 16 KiB
/// write), not a hit rate, and eight of them is eight descriptors per torrent
/// rather than the filesystem backend's one per file.
pub const OPEN_HANDLES: usize = 8;

/// Names each anonymously staged chunk apart from every other one in the
/// process. The process id goes with it, for a kill that leaves one behind
/// while another process is writing the same chunk.
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

/// Why a read of a chunk did not produce a file.
///
/// The distinction is the point -- see the module docs.
#[derive(Debug, thiserror::Error)]
pub enum ChunkError {
    /// There is no file at the chunk's final name. An ordinary state: a
    /// reclaim creates it deliberately.
    #[error("chunk {index} is not on disk")]
    Missing { index: u64 },
    /// The filesystem said something else.
    #[error("could not open chunk {index}")]
    Io {
        index: u64,
        #[source]
        source: io::Error,
    },
}

/// One directory of bucketed chunk files: `<dir>/<index / 1000>/<index>`,
/// with a staged spelling beside it.
///
/// Cheap to clone -- it is a path and nothing else. Whatever state an adapter
/// wants over it (which chunks it knows to be staged, an [`OpenChunks`]) is
/// the adapter's, because the two adapters want different state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkDir {
    dir: PathBuf,
}

/// One chunk on disk: both copies of it, because `ChunkDir::remove` takes
/// them together -- half of a chunk nobody wants is worth exactly as little
/// as the whole of it.
#[derive(Debug)]
pub struct StoredChunk {
    pub index: u64,
    /// The complete copy: the file whose presence is the have-record.
    pub complete: Option<Metadata>,
    /// The staged copy, being written now or left behind by a process that
    /// died mid-chunk.
    pub staged: Option<Metadata>,
}

impl StoredChunk {
    /// Both copies' metadata, for a caller totting up what deleting this
    /// chunk would free. Never empty: a stat reports no chunk it found no
    /// file for.
    pub fn files(&self) -> impl Iterator<Item = &Metadata> {
        self.complete.iter().chain(self.staged.iter())
    }

    /// The more recent modification time of the two copies, or `None` when
    /// neither can be read.
    ///
    /// The more recent, not the older: the two exist together only while a
    /// chunk is being written again over one not yet deleted, and the age
    /// that describes those bytes is the age of the write, not of the copy
    /// it is replacing.
    pub fn modified(&self) -> Option<std::time::SystemTime> {
        self.files().filter_map(|file| file.modified().ok()).max()
    }
}

/// Everything one [`ChunkDir::stat`] found.
#[derive(Debug, Default)]
pub struct StoredChunks {
    /// Every chunk with a file, in ascending index order.
    pub chunks: Vec<StoredChunk>,
    /// Metadata of the files under the directory that are not chunk files --
    /// a name this store never wrote, or a chunk file in the wrong bucket.
    /// Counted, because they are on the volume; never offered as chunks,
    /// because a delete addressed to them would free nothing.
    pub strays: Vec<Metadata>,
}

/// One thing a [`ChunkDir::walk`] found.
pub enum Entry {
    /// A complete chunk, spelled the way a delete would spell it.
    Complete(u64),
    /// A staged file, addressable or not.
    Staged(StagedFile),
}

/// One staged file a walk found.
pub struct StagedFile {
    /// The index its name spells, or `None` for an anonymous staged copy
    /// (`<index>.<pid>-<n>.part`) or a name this store never wrote.
    pub index: Option<u64>,
    pub staged: PathBuf,
    /// Where the complete copy would be, if the name spells one.
    pub complete: Option<PathBuf>,
}

impl ChunkDir {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Where one chunk lives once it is whole.
    pub fn chunk_path(&self, index: u64) -> PathBuf {
        let mut path = self.dir.join((index / CHUNKS_PER_DIRECTORY).to_string());
        path.push(index.to_string());
        path
    }

    /// The addressable staged name: the same path with [`STAGING_SUFFIX`] on
    /// it, in the same bucket directory, so promoting it is a rename and not
    /// a move across directories.
    pub fn staging_path(&self, index: u64) -> PathBuf {
        let mut path = self.chunk_path(index).into_os_string();
        path.push(STAGING_SUFFIX);
        path.into()
    }

    /// Whether this chunk is on disk, **complete**.
    pub fn has_chunk(&self, index: u64) -> bool {
        self.chunk_path(index).is_file()
    }

    /// The complete copy, open for reading.
    pub fn open_complete(&self, index: u64) -> Result<File, ChunkError> {
        match File::open(self.chunk_path(index)) {
            Ok(file) => Ok(file),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                Err(ChunkError::Missing { index })
            }
            Err(source) => Err(ChunkError::Io { index, source }),
        }
    }

    /// The addressable staged copy, open for reading, or `None` when there
    /// is none. An I/O failure is still an error: only "not there" is an
    /// answer.
    pub fn open_staged(&self, index: u64) -> Result<Option<File>, ChunkError> {
        match File::open(self.staging_path(index)) {
            Ok(file) => Ok(Some(file)),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(ChunkError::Io { index, source }),
        }
    }

    /// The addressable staged copy, open for writing -- created along with
    /// its bucket directory if this is the first byte to land in it.
    ///
    /// The retry is not belt-and-braces: [`Self::remove_if_empty`] prunes
    /// bucket directories that have gone empty, so a bucket really can
    /// disappear between one write and the next.
    ///
    /// Read as well as write: a caller caches the handle under the staged
    /// copy and reads that copy back through the same entry.
    pub fn open_staged_for_write(&self, index: u64) -> io::Result<File> {
        let path = self.staging_path(index);
        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(false);
        match opts.open(&path) {
            Ok(file) => Ok(file),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                opts.open(&path)
            }
            Err(e) => Err(e),
        }
    }

    /// Promote the addressable staged copy to the complete one: one rename
    /// within one directory, which is the single instant at which the
    /// have-record for a chunk comes into being.
    ///
    /// `expected_len` is the commit criterion the adapter owns -- see the
    /// module docs. `Some(n)` refuses a staged copy that is not `n` bytes,
    /// which is how a URL response's completeness is established when there
    /// is no hash to check it against. `None` commits whatever is there,
    /// which is what a torrent piece needs: librqbit never writes BEP-47
    /// padding, so a piece whose tail is padding is legally *short*.
    ///
    /// Idempotent in the direction that matters: called for a chunk already
    /// in place with nothing staged, it says so rather than failing, because
    /// librqbit logs a failure here at debug and marks the piece have anyway.
    ///
    /// The caller forgets its cached handles first: the staged handle names a
    /// file about to become the complete one.
    pub fn commit(&self, index: u64, expected_len: Option<u64>) -> io::Result<()> {
        let staged = self.staging_path(index);
        let path = self.chunk_path(index);
        if let Some(want) = expected_len {
            let len = std::fs::metadata(&staged)?.len();
            if len != want {
                return Err(io::Error::other(format!(
                    "staged chunk {index} is {len} bytes where its entity says {want}"
                )));
            }
        }
        match std::fs::rename(&staged, &path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound && path.is_file() => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Write a whole chunk through an *anonymous* staged copy: a name no
    /// other writer can be using, then the same rename [`Self::commit`]
    /// does.
    ///
    /// This is the concurrent-writer form. Two fillers of one chunk each
    /// write their own temporary and each rename it over the final name;
    /// neither can see or truncate the other's bytes, and the loser's rename
    /// replaces a file with an identical one. Through the addressable staged
    /// name they would instead interleave into one file, and the second
    /// rename would name a path the first had already moved away.
    ///
    /// `expected_len` is checked against the buffer before anything is
    /// written -- the same criterion [`Self::commit`] takes, applied where
    /// the bytes still are.
    pub fn write_whole(
        &self,
        index: u64,
        chunk: &[u8],
        expected_len: Option<u64>,
    ) -> io::Result<()> {
        if let Some(want) = expected_len
            && chunk.len() as u64 != want
        {
            return Err(io::Error::other(format!(
                "chunk {index} is {} bytes where its entity says {want}",
                chunk.len()
            )));
        }
        let path = self.chunk_path(index);
        let Some(bucket) = path.parent() else {
            return Err(io::Error::other("a chunk path has no bucket directory"));
        };
        std::fs::create_dir_all(bucket)?;
        let temp = bucket.join(format!(
            "{index}.{}-{}{STAGING_SUFFIX}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        if let Err(e) = std::fs::write(&temp, chunk) {
            let _ = std::fs::remove_file(&temp);
            return Err(e);
        }
        if let Err(e) = std::fs::rename(&temp, &path) {
            let _ = std::fs::remove_file(&temp);
            return Err(e);
        }
        Ok(())
    }

    /// Unlink both copies of `index`.
    ///
    /// **Crate-private, and that is the point.** There is one door into the
    /// unlink per adapter and this is not a third one: the torrent
    /// adapter's is `StoreRoot::delete_pieces`, itself crate-private and
    /// called only from `retention`, which either holds the
    /// `DroppedFilePieces` claim that keeps the delete atomic with
    /// librqbit's have-set or has established there is no have-set to
    /// interlock against; and the `/proxy` adapter's is its own. A
    /// generic *public* `remove` on the shared store would be exactly the
    /// second door that interlock exists to refuse -- somewhere to unlink a
    /// torrent's piece behind the backend's back and have it go on
    /// advertising bytes it no longer has -- so the `server` crate, cleaner
    /// and routes and `/proxy` alike, cannot name this at all.
    ///
    /// The bucket directory is left behind; it is pruned when the whole
    /// directory goes ([`Self::remove_if_empty`]). Removing it here would be
    /// a rmdir per chunk against a directory a concurrent write may have just
    /// created and not yet opened its file in.
    ///
    /// A chunk with no file was not on the disk to leave it, and is not an
    /// error -- the caller asked for bytes back and there were none. A
    /// *directory* wearing either of a chunk's two names is not a chunk
    /// either, and is the same non-event. It has to be: this is one chunk of
    /// a whole run whose have-bits the caller has **already** cleared, and
    /// `remove_file` answers `EISDIR` for a directory rather than
    /// `NotFound`, so treating it as a failure abandoned every chunk after
    /// it in the run. [`Self::held_in_bucket`] refuses to offer a directory
    /// wearing the *complete* name, and the two halves of that rule only
    /// close together: a directory wearing the *staged* name spells no index
    /// at all, so no listing can filter it and only the delete ever meets
    /// it.
    pub(crate) fn remove(&self, index: u64) -> io::Result<bool> {
        let mut removed = false;
        for path in [self.staging_path(index), self.chunk_path(index)] {
            match std::fs::remove_file(&path) {
                Ok(()) => removed = true,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                // Asked of the path rather than of the errno: `EISDIR` is
                // Linux's answer to unlinking a directory and not every
                // platform's, and what this arm means is "there is no chunk
                // here", which is a question about the name.
                Err(_) if path.is_dir() => {}
                Err(e) => return Err(e),
            }
        }
        Ok(removed)
    }

    /// Prune the bucket directories that have gone empty, then the directory
    /// itself. "Empty" therefore means "holds no chunks".
    pub fn remove_if_empty(&self) -> io::Result<()> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                // Fails while the bucket still holds chunks, which is the
                // answer we want.
                let _ = std::fs::remove_dir(entry.path());
            }
        }
        let _ = std::fs::remove_dir(&self.dir);
        Ok(())
    }

    /// Which chunks of one bucket are **complete** on disk.
    ///
    /// Names and nothing else: one `read_dir`, no `stat`. The name is matched
    /// by re-spelling ([`canonical_index`]), and the index has to belong to
    /// the bucket it was found in, because a chunk is only *reachable* when
    /// both halves of its address are spelled the way a delete will spell
    /// them. Anything else is reported held by nobody: a listing must never
    /// promise bytes a delete could not take. A staged copy carries a suffix
    /// and so spells no index, which is how a chunk still being written stays
    /// invisible to a read.
    ///
    /// **A bucket that cannot be read is not an empty bucket.** A bucket
    /// that does not exist is one nothing has been written to, and it is
    /// empty; a bucket the filesystem would not list -- `EIO`, an
    /// `EMFILE` on a television that has run out of descriptors, a
    /// permission it lost -- holds whatever it held, and the answer is that
    /// there is no answer. Reported as empty, one such tick told the
    /// retention policy that every committed piece of a file had left the
    /// disk: it withdrew the lot from what we announce -- after peers had
    /// been told, and there is no un-Have -- and, the next tick, with the
    /// listing back and the pieces outside the window and no longer
    /// committed, reclaimed them. One transient directory error, and the
    /// promise the whole design rests on -- what we announce is what nothing
    /// will ever reclaim -- was broken for every piece of the file.
    pub fn held_in_bucket(&self, bucket: u64) -> io::Result<HashSet<u64>> {
        let mut held = HashSet::new();
        let entries = match std::fs::read_dir(self.dir.join(bucket.to_string())) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(held),
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let Some(index) = name.to_str().and_then(canonical_index) else {
                continue;
            };
            if index / CHUNKS_PER_DIRECTORY != bucket {
                continue;
            }
            // A *directory* named like a chunk is not a chunk. It was
            // reported as one by the piece store's hot listing and by
            // nothing else, and the delete that followed failed with
            // `EISDIR` -- not `NotFound` -- which abandoned the whole run of
            // chunks it was part of.
            if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
                continue;
            }
            held.insert(index);
        }
        Ok(held)
    }

    /// Which chunks are **complete** on disk, across every bucket.
    ///
    /// [`Self::stat`] answers the same question and more, and the more is
    /// what makes it the wrong call on a hot path: it costs a `metadata` per
    /// file, and the retention pass asks this of a streaming torrent every
    /// couple of seconds -- some 6,750 `statx` calls a pass for a 27 GB
    /// torrent, for two numbers it does not want.
    ///
    /// `Err` is a directory that could not be listed, and it is the whole
    /// answer: a listing with one bucket missing from it would be an
    /// account of the disk that is wrong about that bucket's every chunk.
    /// See [`Self::held_in_bucket`] for what treating it as empty cost.
    pub fn held(&self) -> io::Result<BTreeSet<u64>> {
        let mut held = BTreeSet::new();
        let buckets = match std::fs::read_dir(&self.dir) {
            Ok(buckets) => buckets,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(held),
            Err(error) => return Err(error),
        };
        for bucket in buckets {
            let bucket = bucket?;
            if !bucket.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let name = bucket.file_name();
            // The bucket's *spelling*, not merely the number it parses to:
            // `00/0` and `+0/0` both parse as bucket 0, and a delete would go
            // to `0/0` and free nothing.
            let Some(bucket) = name.to_str().and_then(canonical_index) else {
                continue;
            };
            held.extend(self.held_in_bucket(bucket)?);
        }
        Ok(held)
    }

    /// Every chunk under this directory with the metadata of both its
    /// copies, and everything else it holds as strays.
    ///
    /// One `read_dir` per bucket and one `metadata` per file -- the same
    /// filesystem work walking the tree would cost -- but the caller is
    /// handed chunk indices rather than paths, so whatever it means to do
    /// next it has to ask this type to do.
    ///
    /// An unreadable entry is skipped, not reported as an error: a stat is a
    /// reading of what is there, and a caller counting the disk must not be
    /// stopped by one directory it may not enter. A directory that does not
    /// exist stats empty, which is the ordinary state before the first write.
    pub fn stat(&self) -> StoredChunks {
        let mut stored = StoredChunks::default();
        let Ok(buckets) = std::fs::read_dir(&self.dir) else {
            return stored;
        };
        // Both copies of a chunk are one entry, so they are counted, aged and
        // deleted together -- keyed by index while the walk runs, because the
        // staged file and the complete one are two directory entries that may
        // arrive in either order.
        let mut chunks: BTreeMap<u64, StoredChunk> = BTreeMap::new();
        for bucket in buckets.flatten() {
            // Anything but a bucket directory at this level is debris an
            // interrupted write left. Counted, because it is on the disk.
            if !bucket.file_type().is_ok_and(|t| t.is_dir()) {
                if let Ok(metadata) = bucket.metadata() {
                    stored.strays.push(metadata);
                }
                continue;
            }
            let bucket_name = bucket.file_name();
            let bucket_index = bucket_name.to_str().and_then(canonical_index);
            let Ok(entries) = std::fs::read_dir(bucket.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let Ok(metadata) = entry.metadata() else {
                    continue;
                };
                if metadata.is_dir() {
                    // Nothing this store makes, so everything under it is
                    // debris -- but it is occupying the volume, and a caller
                    // counting the disk must see all of it.
                    collect_strays(&entry.path(), &mut stored.strays);
                    continue;
                }
                let name = entry.file_name();
                // A name this store never wrote, or a chunk file sitting in a
                // bucket it does not belong to: a delete would look for it
                // somewhere else, so reporting it as that chunk would promise
                // bytes back that no delete could take.
                let stray = match name.to_str().and_then(chunk_of_name) {
                    Some((index, staged)) if bucket_index == Some(index / CHUNKS_PER_DIRECTORY) => {
                        let slot = chunks.entry(index).or_insert(StoredChunk {
                            index,
                            complete: None,
                            staged: None,
                        });
                        if staged {
                            slot.staged = Some(metadata);
                        } else {
                            slot.complete = Some(metadata);
                        }
                        None
                    }
                    _ => Some(metadata),
                };
                if let Some(metadata) = stray {
                    stored.strays.push(metadata);
                }
            }
        }
        stored.chunks = chunks.into_values().collect();
        stored
    }

    /// Everything under this directory that is a chunk of ours: every
    /// complete chunk by index, every staged file by name. One `read_dir`
    /// per bucket, names and types only, no `stat` of a chunk file.
    ///
    /// The one place a staged file is *recognised*, so that what a staged
    /// file is called stays one decision taken by [`Self::staging_path`] and
    /// [`Self::write_whole`]. A second spelling of the suffix elsewhere would
    /// let the constant change while a reconciliation pass silently stopped
    /// finding anything -- and the stale shadow such a pass exists to delete
    /// is exactly what gets a half-written chunk served as a complete one.
    ///
    /// A complete chunk is reported under the same rules as
    /// [`Self::held_in_bucket`]: the name re-spells ([`canonical_index`]),
    /// the index belongs to the bucket it was found in, and the entry is a
    /// file -- a *directory* wearing a chunk's name is not a chunk, and
    /// offered as one it would meet the delete as `EISDIR`. Anything else is
    /// passed over: a walk must never name a chunk a delete could not
    /// address.
    ///
    /// **Strict, in every bucket.** A directory that does not exist yet is
    /// the ordinary state before the first write and walks empty; anything
    /// that exists and will not list -- the directory itself, a bucket, an
    /// entry whose kind cannot be read -- is an `Err`, and the whole walk is.
    /// The caller is seeding what it knows about the disk from this, and
    /// there is no later event that adds a chunk already complete: a bucket
    /// skipped here would be a set short of that bucket's every chunk for
    /// the rest of the process. The earlier form of this walk skipped a
    /// bucket it could not read, which was tolerable while it fed only the
    /// advisory staged set.
    pub fn walk(&self) -> io::Result<Vec<Entry>> {
        self.count_walk();
        let mut found = Vec::new();
        let buckets = match std::fs::read_dir(&self.dir) {
            Ok(buckets) => buckets,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(found),
            Err(e) => return Err(e),
        };
        for bucket in buckets {
            let bucket = bucket?;
            // Anything but a directory at this level is debris an
            // interrupted write left, and holds no chunk.
            if !bucket.file_type()?.is_dir() {
                continue;
            }
            let bucket_name = bucket.file_name();
            let bucket_index = bucket_name.to_str().and_then(canonical_index);
            for entry in std::fs::read_dir(bucket.path())? {
                let entry = entry?;
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    continue;
                };
                if let Some(stem) = staged_stem(name) {
                    let staged = entry.path();
                    let index = canonical_index(stem);
                    let complete = index.map(|_| staged.with_file_name(stem));
                    found.push(Entry::Staged(StagedFile {
                        index,
                        staged,
                        complete,
                    }));
                    continue;
                }
                let Some(index) = canonical_index(name) else {
                    continue;
                };
                if bucket_index != Some(index / CHUNKS_PER_DIRECTORY) {
                    continue;
                }
                if !entry.file_type()?.is_file() {
                    continue;
                }
                found.push(Entry::Complete(index));
            }
        }
        Ok(found)
    }
}

impl ChunkDir {
    #[cfg(test)]
    fn count_walk(&self) {
        *WALKS.lock().entry(self.dir.clone()).or_insert(0) += 1;
    }

    #[cfg(not(test))]
    fn count_walk(&self) {}
}

/// How many times [`ChunkDir::walk`] has run over each directory: the probe
/// for the tests that pin "the pass lists nothing" -- one walk per torrent
/// start, and not one more however many passes run. Keyed by directory so
/// that tests running in parallel, each in a scratch root of its own, do not
/// read one another's count.
#[cfg(test)]
pub static WALKS: Mutex<BTreeMap<PathBuf, usize>> = Mutex::new(BTreeMap::new());

/// What a staged file's name says before [`STAGING_SUFFIX`], or `None` for a
/// name that is not a staged one at all.
///
/// Anonymous staged copies (`<index>.<pid>-<n>.part`) come back as their
/// whole prefix, which [`canonical_index`] then refuses -- they name no
/// addressable chunk, which is exactly right: nothing may promote one but the
/// writer that holds it.
pub fn staged_stem(name: &str) -> Option<&str> {
    name.strip_suffix(STAGING_SUFFIX)
}

/// Whether a file name is a staged copy of some chunk, addressable or not.
pub fn is_staged_name(name: &str) -> bool {
    staged_stem(name).is_some()
}

/// A file name in a bucket directory back to the chunk it holds and which
/// copy of it, or `None` when it is not a name this store writes.
pub fn chunk_of_name(name: &str) -> Option<(u64, bool)> {
    let (index, staged) = match staged_stem(name) {
        Some(index) => (index, true),
        None => (name, false),
    };
    canonical_index(index).map(|index| (index, staged))
}

/// The number a name spells, or `None` when it is not how this store spells
/// that number.
///
/// `str::parse` would accept a leading `+` and leading zeroes; the store
/// writes neither, so `00`, `+0` and `0` all parse to the same chunk while
/// only the last of them is a name [`ChunkDir::chunk_path`] would ever
/// produce. Both halves of a chunk's address go through this -- the bucket
/// directory and the file in it -- because a chunk is only *reachable* when
/// both are spelled the way a delete will spell them. Reporting debris as a
/// chunk would promise bytes back that no delete could take: the delete would
/// build `<index / CHUNKS_PER_DIRECTORY>/<index>`, find nothing there and
/// free nothing, while the file sat on the volume being re-found by every
/// later pass.
pub fn canonical_index(name: &str) -> Option<u64> {
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if name.len() > 1 && name.starts_with('0') {
        return None;
    }
    name.parse::<u64>().ok()
}

/// Every file under `dir`, however deep, as debris.
///
/// The store makes no directory below a bucket, so a tree found there is
/// somebody else's -- but its blocks are on the same volume, and a listing
/// that stayed silent about them would be inventing exactly the invisible
/// disk usage one file per chunk exists to abolish.
pub fn collect_strays(dir: &Path, out: &mut Vec<Metadata>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            collect_strays(&entry.path(), out);
        } else {
            out.push(metadata);
        }
    }
}

/// What a file occupies, never its apparent length. **The one copy of this
/// arithmetic**: the cache cleaner, the piece sweep and the proxy cache each
/// had their own, and three readings of "how much would deleting this give
/// back" is three numbers that can disagree.
///
/// On Unix `st_blocks` is the allocated block count in 512-byte units *by
/// definition* -- the unit is POSIX, not the filesystem's block size -- so
/// `blocks() * 512` is the occupancy including any tail slack. A chunk file
/// written by one 16 KiB write out of many is a hole plus 16 KiB, and
/// reporting its full length as freed would be a number the disk never gives
/// back. Windows has no equivalent through `std` (it needs
/// `GetCompressedFileSize` or `FSCTL_QUERY_ALLOCATED_RANGES` through the
/// Win32 API), so there the apparent length stands in: an over-estimate for a
/// sparse file, which errs towards cleaning too eagerly rather than letting a
/// disk fill.
pub fn occupied_bytes(metadata: &Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        metadata.len()
    }
}

/// One entry of an [`OpenChunks`]: which chunk, which copy of it (the staged
/// one is a different file from the complete one), and the handle. The `Arc`
/// is what a read or write borrows, so forgetting an entry never closes a
/// file mid-operation.
struct OpenHandle {
    index: u64,
    staged: bool,
    file: Arc<File>,
}

/// The last few chunk files opened, most recently used last.
///
/// The first version of the piece store opened and closed the chunk file for
/// every 16 KiB write and every 8 KiB read -- and, for a read, probed the
/// staging name first, so a complete chunk cost two opens per read. On ext4
/// that is microseconds; on the FUSE-mediated external storage and exFAT
/// cards a large offline download lands on, an open is 50-200 µs and the read
/// path was a third of the wall time. With this, a chunk written or streamed
/// end to end is one open.
///
/// The cache changes nothing about what is on disk, and it must not change
/// what a read sees either: a handle is forgotten before the file it names is
/// renamed into place or deleted, and a chunk's cached *complete* handle goes
/// the moment a new staged copy is begun, since a read has to prefer the
/// staged one. A handle still in a reader's hands survives that -- it is an
/// `Arc` -- and keeps reading the bytes it had, which is the same thing a
/// read that had already begun would do.
///
/// An adapter that reads and writes a chunk once (a `/proxy` fill is one
/// `write_whole`, a `/proxy` read one `fs::read`) has nothing to gain from
/// this and does not have to hold one.
#[derive(Default)]
pub struct OpenChunks {
    handles: Mutex<Vec<OpenHandle>>,
}

impl OpenChunks {
    pub fn new() -> Self {
        Self::default()
    }

    /// The cached handle for one copy of a chunk, made the most recently
    /// used.
    pub fn get(&self, index: u64, staged: bool) -> Option<Arc<File>> {
        let mut handles = self.handles.lock();
        let at = handles
            .iter()
            .position(|h| h.index == index && h.staged == staged)?;
        let handle = handles.remove(at);
        let file = handle.file.clone();
        handles.push(handle);
        Some(file)
    }

    /// Keep a freshly opened handle, dropping the least recently used one
    /// past [`OPEN_HANDLES`].
    pub fn remember(&self, index: u64, staged: bool, file: &Arc<File>) {
        let mut handles = self.handles.lock();
        handles.retain(|h| !(h.index == index && h.staged == staged));
        if handles.len() >= OPEN_HANDLES {
            handles.remove(0);
        }
        handles.push(OpenHandle {
            index,
            staged,
            file: file.clone(),
        });
    }

    /// Drop every cached handle of a chunk: its files are about to be
    /// renamed, deleted or shadowed.
    pub fn forget(&self, index: u64) {
        self.handles.lock().retain(|h| h.index != index);
    }

    /// Drop all of them.
    pub fn clear(&self) {
        self.handles.lock().clear();
    }

    /// Whether any handle is being held open. What a test asking "does a
    /// paused torrent keep descriptors?" reads.
    pub fn is_empty(&self) -> bool {
        self.handles.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> (tempfile::TempDir, ChunkDir) {
        let tmp = tempfile::tempdir().expect("a scratch root");
        let chunks = ChunkDir::new(tmp.path().join("d"));
        (tmp, chunks)
    }

    /// The bucketing is the same arithmetic for both adapters, and it is
    /// written down once.
    #[test]
    fn a_chunk_is_bucketed_a_thousand_to_a_directory() {
        let (tmp, chunks) = dir();
        assert_eq!(chunks.chunk_path(0), tmp.path().join("d/0/0"));
        assert_eq!(chunks.chunk_path(999), tmp.path().join("d/0/999"));
        assert_eq!(chunks.chunk_path(1000), tmp.path().join("d/1/1000"));
        assert_eq!(chunks.staging_path(1000), tmp.path().join("d/1/1000.part"));
    }

    /// A `/proxy` fill and a piece-store write of the same index are the
    /// same rename onto the same name, and only the staging identity
    /// differs.
    #[test]
    fn both_staging_forms_end_at_the_same_name() {
        let (_tmp, chunks) = dir();
        use std::io::Write;
        let mut staged = chunks.open_staged_for_write(7).unwrap();
        staged.write_all(b"abcd").unwrap();
        drop(staged);
        chunks.commit(7, None).unwrap();
        assert_eq!(std::fs::read(chunks.chunk_path(7)).unwrap(), b"abcd");

        chunks.write_whole(8, b"efgh", Some(4)).unwrap();
        assert_eq!(std::fs::read(chunks.chunk_path(8)).unwrap(), b"efgh");
        assert_eq!(chunks.held().unwrap(), BTreeSet::from([7, 8]));
    }

    /// **A directory that is not there is empty; one that cannot be read is
    /// unknown.** The two used to be one answer, and the second read as the
    /// first cost every committed piece of a file its announcement -- see
    /// `held_in_bucket`.
    #[test]
    fn a_directory_that_cannot_be_listed_is_not_an_empty_one() {
        let tmp = tempfile::tempdir().unwrap();
        let chunks = ChunkDir::new(tmp.path().join("entity"));
        assert_eq!(
            chunks.held().unwrap(),
            BTreeSet::new(),
            "nothing has been written here, so nothing is held"
        );
        assert_eq!(chunks.held_in_bucket(3).unwrap(), HashSet::new());

        // A bucket that is a file: `read_dir` fails with something other
        // than not-found, and that is not an empty bucket.
        std::fs::create_dir_all(chunks.path()).unwrap();
        std::fs::write(chunks.path().join("3"), b"not a bucket").unwrap();
        assert!(chunks.held_in_bucket(3).is_err());
        // The tree listing skips a file where a bucket should be -- it is
        // not a bucket -- so it still answers for the buckets that are.
        chunks.write_whole(7, b"abcd", Some(4)).unwrap();
        assert_eq!(chunks.held().unwrap(), BTreeSet::from([7]));

        // And an entity directory that is a file cannot be listed at all.
        let file = ChunkDir::new(tmp.path().join("file"));
        std::fs::write(file.path(), b"not a directory").unwrap();
        assert!(file.held().is_err());

        // The walk that seeds the piece store answers the same way: a file
        // where a bucket should be is no bucket and holds nothing, and a
        // directory that will not list is no answer.
        let walked = chunks.walk().unwrap();
        assert!(
            matches!(walked.as_slice(), [Entry::Complete(7)]),
            "the one chunk, in the one bucket that is one"
        );
        assert!(file.walk().is_err());
    }

    /// `expected_len` is the commit criterion, and it refuses.
    #[test]
    fn a_length_that_disagrees_with_the_entity_is_not_committed() {
        let (_tmp, chunks) = dir();
        assert!(chunks.write_whole(1, b"short", Some(9)).is_err());
        assert!(!chunks.has_chunk(1));
    }

    /// Two fillers of one chunk cannot corrupt it, and that is what the
    /// anonymous staged name buys.
    ///
    /// `/proxy` does not coalesce: two players reading one stream both fetch
    /// a missing chunk and both write it, and that is the only concurrency
    /// either adapter has over one index. What makes it safe is that a fill
    /// touches no name another writer could be holding -- so this drives one
    /// fill while another writer's staged copy of the same chunk is sitting
    /// on the disk, and asks what happened to both.
    ///
    /// Through the *addressable* staged name the two would be one file: this
    /// fill would truncate the other writer's bytes and then rename them
    /// away, so what got committed would be neither writer's chunk and the
    /// other writer's rename would name a path that had already moved.
    #[test]
    fn a_fill_touches_no_name_another_writer_could_hold() {
        let (_tmp, chunks) = dir();
        // Another writer, mid-chunk: its bytes are under the addressable
        // staged name and are not this fill's.
        std::fs::create_dir_all(chunks.chunk_path(5).parent().unwrap()).unwrap();
        let other = chunks.staging_path(5);
        std::fs::write(&other, [0x5au8; 64]).unwrap();

        chunks.write_whole(5, &[0xa5u8; 256], Some(256)).unwrap();

        assert_eq!(
            std::fs::read(chunks.chunk_path(5)).unwrap(),
            [0xa5u8; 256],
            "the fill committed its own bytes, whole"
        );
        assert_eq!(
            std::fs::read(&other).unwrap(),
            [0x5au8; 64],
            "and left the other writer's staged copy exactly as it was"
        );
    }

    #[test]
    fn a_missing_chunk_is_its_own_error() {
        let (_tmp, chunks) = dir();
        assert!(matches!(
            chunks.open_complete(3),
            Err(ChunkError::Missing { index: 3 })
        ));
    }
}

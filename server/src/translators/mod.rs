//! **What a container says about the bytes inside it.**
//!
//! A translator reads a container's own index -- a ZIP's central
//! directory, a TAR's headers -- off a [`ByteSource`] and says, for every
//! member, either *which byte ranges of which sources hold it* or *why it
//! cannot be served that way*. It decodes nothing, writes nothing and
//! reads no member's data: an index is headers, directories and trailers,
//! and the test suite counts the bytes to prove it (`Budget`).
//!
//! **A member is [`Body::Direct`] or it is refused, and there is no third
//! state.** No "extract it then", no partial decode, no "serve
//! sequentially but refuse seeks": a compressed film is a [`Refusal`] with
//! a sentence the player shows. That is the decision the whole design
//! rests on -- only the thing that downloads a file may store it -- and it
//! is why `.archives`, the extraction cache under the cache root, ceases
//! to exist for the formats that have come through here. See
//! `docs/translated-sources.md`.
//!
//! Verification is the fetcher's: a torrent's bytes are piece-verified by
//! librqbit and a proxied entity is what the origin served, so nothing
//! here checksums a member on the way past -- a CRC over a member is a
//! read of the whole member, which is the thing this design forbids. A
//! *header* checksum a format carries for its own index is small, and is
//! checked (see `tar`).

use crate::sources::{ByteSource, Extent};
use async_trait::async_trait;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub mod iso;
#[cfg(feature = "rar")]
pub mod rar;
pub mod session;
pub mod sevenz;
pub mod tar;
pub mod zip;

/// One member of a container, as ranges of the container's own bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    /// As the container states it, `/`-separated and without a leading
    /// slash -- which is what the route matches the URL's wildcard against.
    pub name: String,
    /// The unpacked length, which for a direct member is the sum of its
    /// extents.
    pub len: u64,
    pub body: Body,
}

/// Where a member's bytes are, or why they cannot be pointed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// The member's bytes are these ranges of these sources, in order.
    Direct(Vec<Extent>),
    /// The member cannot be served by range, and this is why.
    Opaque(Refusal),
}

/// Why a member -- or a whole container -- will not be served as byte
/// ranges. Every variant is a sentence a player can show, and none of them
/// means "probably nothing here".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// Deflate, LZMA, RAR "Normal", gzip: the bytes in the container are
    /// not the file's bytes.
    Compressed {
        format: &'static str,
        method: String,
    },
    /// Any encryption, until there is a password UX: a guess is ciphertext
    /// to the player.
    Encrypted,
    /// A solid block holding several files, which cannot be entered at one
    /// of them.
    Solid,
    /// The container contradicts itself, points outside itself, or is
    /// truncated.
    Malformed(String),
    /// The container as a whole has no way in at the middle: `tar.gz` is
    /// one gzip stream, and reaching its end means inflating all of it.
    NoRandomAccess { format: &'static str },
    /// A well-formed container using a structure this server does not
    /// read yet, named: a UDF image's metadata partition map, which remaps
    /// every block, so an extent computed without it would be the wrong
    /// bytes. Refused rather than guessed at, and `415` like the others a
    /// player cannot do anything about.
    Unsupported { format: &'static str, what: String },
}

impl Refusal {
    /// The short name the client sees in `{"refused": ...}`, and the one
    /// xtremio switches on. Stable: it is part of the route's contract.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Compressed { .. } => "compressed",
            Self::Encrypted => "encrypted",
            Self::Solid => "solid",
            Self::Malformed(_) => "malformed",
            Self::NoRandomAccess { .. } => "noRandomAccess",
            Self::Unsupported { .. } => "unsupported",
        }
    }
}

impl fmt::Display for Refusal {
    /// One sentence, for a player to show a viewer. It says what is the
    /// matter with the file and not what this server's code did about it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compressed { format, method } => write!(
                f,
                "this file is compressed inside the {format} ({method}), and playing it would \
                 mean unpacking the whole archive first"
            ),
            Self::Encrypted => write!(
                f,
                "this file is encrypted inside the archive, and there is no way to ask you for \
                 the password"
            ),
            Self::Solid => write!(
                f,
                "this file shares a solid block with the others in the archive, so it cannot be \
                 read on its own"
            ),
            Self::Malformed(detail) => write!(f, "this archive could not be read: {detail}"),
            Self::NoRandomAccess { format } => write!(
                f,
                "a {format} is one compressed stream with no way in at the middle, so playing it \
                 would mean unpacking the whole thing first"
            ),
            Self::Unsupported { format, what } => write!(
                f,
                "this {format} uses {what}; that is not supported yet, so it cannot be read as \
                 byte ranges"
            ),
        }
    }
}

impl std::error::Error for Refusal {}

/// What a container holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    /// Every member with bytes of its own, in the container's own order.
    /// Directories are not here: a directory is not a file and has nothing
    /// to serve.
    pub members: Vec<Member>,
}

impl Index {
    /// The member called `name`, and where it is in [`Index::members`].
    pub fn find(&self, name: &str) -> Option<(usize, &Member)> {
        self.members
            .iter()
            .enumerate()
            .find(|(_, member)| member.name == name)
    }
}

/// A container format, read from its sources.
///
/// The design sketch wrote this as an associated function taking the
/// sources alone. It takes `&self` instead, because that is what makes the
/// trait object-safe: the route has a format prefix in the URL and needs
/// *a* translator for it at run time ([`crate::routes::archive::Format`]),
/// which a static method cannot give it. Every implementation is a
/// zero-sized unit struct, so `&self` costs nothing.
#[async_trait]
pub trait Translator: Send + Sync {
    /// What this reads, for the sentences a refusal is made of.
    fn format(&self) -> &'static str;

    /// Read the container's index from its sources.
    ///
    /// Reads what the format needs and nothing else. `sources` is the
    /// volume list, in order; a single-volume format reads `sources[0]`
    /// and says so about the rest.
    async fn index(&self, sources: &[Arc<dyn ByteSource>]) -> Result<Index, Refusal>;

    /// The files among `siblings` that make up the container `named` is a
    /// volume of, in volume order -- what the `torrent:` form hands to
    /// [`Translator::index`] when the container is a file of a torrent and
    /// the rest of it is the files beside that one. Every single-file
    /// format is its own answer, which is the default; a format that comes
    /// in sets (RAR) knows its naming rules and says which siblings are
    /// its volumes, or that one of them is missing.
    fn volumes(&self, named: &str, _siblings: &[String]) -> Result<Vec<String>, Refusal> {
        Ok(vec![named.to_string()])
    }
}

/// How many bytes reading one container's index may cost.
///
/// Not a guideline: every translator reads through [`Budget`] and nothing
/// else, so a container that would make the walk read for ever -- a TAR of
/// a million empty files, a directory claiming a gigabyte of entries --
/// stops at a stated number with a refusal that says so, rather than being
/// found out with a stopwatch. Sixteen mebibytes is far above any real
/// archive's index (a ZIP central directory runs about 60 bytes per member)
/// and far below a film.
pub const INDEX_BUDGET_BYTES: u64 = 16 * 1024 * 1024;

/// A source, and what is left of the reading an index may do to it.
pub struct Budget<'a> {
    source: &'a dyn ByteSource,
    format: &'static str,
    left: u64,
    used: u64,
}

impl<'a> Budget<'a> {
    pub fn new(source: &'a dyn ByteSource, format: &'static str) -> Self {
        Self {
            source,
            format,
            left: INDEX_BUDGET_BYTES,
            used: 0,
        }
    }

    /// How long the container is.
    pub fn source_len(&self) -> u64 {
        self.source.len()
    }

    /// How many bytes have been read through this so far.
    pub fn used(&self) -> u64 {
        self.used
    }

    /// `len` bytes at `offset`, or a refusal naming which it was: a short
    /// read is a truncated container and not a shorter answer, since every
    /// caller here is reading a fixed-size structure.
    pub async fn read_exact(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, Refusal> {
        let wanted = len as u64;
        if wanted > self.left {
            return Err(self.malformed(format!(
                "reading the index passed {INDEX_BUDGET_BYTES} bytes, so it is not an index"
            )));
        }
        self.left -= wanted;
        self.used += wanted;
        let mut buf = vec![0u8; len];
        let mut filled = 0;
        while filled < len {
            // A `read_at` may answer short of the buffer at any time (a
            // torrent's reader ends at a piece, an HTTP body at a chunk);
            // only a zero says there is nothing more there.
            let read = self
                .source
                .read_at(offset + filled as u64, &mut buf[filled..])
                .await
                .map_err(|error| {
                    Refusal::Malformed(format!(
                        "{} could not be read at {offset}: {error}",
                        self.source.describe()
                    ))
                })?;
            if read == 0 {
                return Err(self.malformed(format!(
                    "wanted {len} bytes at {offset} of a {}-byte {} and found {filled}",
                    self.source.len(),
                    self.format,
                )));
            }
            filled += read;
        }
        Ok(buf)
    }

    /// Up to `len` bytes ending at the container's end, for a trailer whose
    /// position is not known until it is found (a ZIP's end-of-central-
    /// directory record).
    pub async fn read_tail(&mut self, len: u64) -> Result<Vec<u8>, Refusal> {
        let total = self.source.len();
        let want = len.min(total);
        self.read_exact(total - want, want as usize).await
    }

    pub fn malformed(&self, detail: impl Into<String>) -> Refusal {
        Refusal::Malformed(format!("{}: {}", self.format, detail.into()))
    }
}

/// A source as a blocking `Read + Seek`, for the format crates whose
/// parsers are synchronous (RAR's `parse_volume_facts`, 7z's
/// `Archive::read`): [`Budget`]'s twin for a parser that does its own
/// reading.
///
/// It runs on the blocking pool and reads through the runtime handle it
/// was made under, so the source's `read_at` -- a seek of the piece store,
/// a ranged request through the proxy cache -- is the same call an index
/// read makes anywhere else, and lands in a [`crate::sources::testing::
/// CountingSource`] the same way. A seek here is a number: nothing is
/// fetched until the parser reads, which is how a walk that seeks past
/// every member's data by its size costs the headers alone.
///
/// The same bound as [`Budget`], per reader -- one reader per volume, so a
/// set costs at most the bound per volume -- and reached the same way: a
/// read past it is an error the parser surfaces and the translator turns
/// into a refusal ([`IndexReader::is_over_budget`]), not a slower answer.
pub struct IndexReader {
    source: Arc<dyn ByteSource>,
    runtime: tokio::runtime::Handle,
    position: u64,
    left: u64,
    /// Shared, because the parser consumes the reader and the caller still
    /// wants the figure afterwards.
    used: Arc<AtomicU64>,
}

/// The error a read past [`INDEX_BUDGET_BYTES`] answers with, wrapped in
/// an `io::Error` so a synchronous parser carries it out unchanged.
#[derive(Debug)]
pub struct OverBudget;

impl fmt::Display for OverBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "reading the index passed {INDEX_BUDGET_BYTES} bytes, so it is not an index"
        )
    }
}

impl std::error::Error for OverBudget {}

impl IndexReader {
    /// Over `source`, positioned at its start, with a whole budget to
    /// spend. Made on a runtime thread (it takes the current handle) and
    /// used off one.
    pub fn new(source: Arc<dyn ByteSource>) -> Self {
        Self {
            source,
            runtime: tokio::runtime::Handle::current(),
            position: 0,
            left: INDEX_BUDGET_BYTES,
            used: Arc::default(),
        }
    }

    /// The running count of bytes read through this reader, to keep after
    /// the parser has taken the reader itself.
    pub fn tally(&self) -> Arc<AtomicU64> {
        self.used.clone()
    }

    /// Whether `error` -- one a parser handed back -- is this reader
    /// refusing to read past the budget.
    pub fn is_over_budget(error: &io::Error) -> bool {
        error
            .get_ref()
            .is_some_and(|inner| inner.is::<OverBudget>())
    }
}

impl io::Read for IndexReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.source.len();
        if buf.is_empty() || self.position >= len {
            return Ok(0);
        }
        if self.left == 0 {
            return Err(io::Error::other(OverBudget));
        }
        let want = (buf.len() as u64).min(len - self.position).min(self.left) as usize;
        let read = self
            .runtime
            .block_on(self.source.read_at(self.position, &mut buf[..want]))?;
        self.position += read as u64;
        self.left -= read as u64;
        self.used.fetch_add(read as u64, Ordering::Relaxed);
        Ok(read)
    }
}

impl io::Seek for IndexReader {
    fn seek(&mut self, to: io::SeekFrom) -> io::Result<u64> {
        let (base, delta) = match to {
            io::SeekFrom::Start(at) => (at, 0i64),
            io::SeekFrom::End(delta) => (self.source.len(), delta),
            io::SeekFrom::Current(delta) => (self.position, delta),
        };
        self.position = base.checked_add_signed(delta).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "a seek before the start of the source",
            )
        })?;
        Ok(self.position)
    }
}

/// `sources[0]`, or a refusal when there are none: every translator here
/// reads one volume, and a create with no URL at all is refused before it
/// reaches one.
pub(crate) fn only_source(
    sources: &[Arc<dyn ByteSource>],
    format: &'static str,
) -> Result<Arc<dyn ByteSource>, Refusal> {
    match sources {
        [] => Err(Refusal::Malformed(format!("no {format} to read"))),
        [source] => Ok(source.clone()),
        _ => Err(Refusal::Malformed(format!(
            "a {format} is one file, and {} were given",
            sources.len()
        ))),
    }
}

/// `.tar.gz`, which is not a container this can index at all: one gzip
/// stream, whose last byte is reachable only by inflating every byte
/// before it.
///
/// It is a translator rather than a special case in the route because the
/// refusal is the format's own fact, and belongs where the formats are.
pub struct TarGz;

#[async_trait]
impl Translator for TarGz {
    fn format(&self) -> &'static str {
        "tar.gz"
    }

    async fn index(&self, _sources: &[Arc<dyn ByteSource>]) -> Result<Index, Refusal> {
        Err(Refusal::NoRandomAccess { format: "tar.gz" })
    }
}

/// Little-endian integers out of a header, `None` past its end -- a
/// truncated structure is a refusal the caller words, never a panic.
pub(crate) fn le_u16(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}

pub(crate) fn le_u32(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

pub(crate) fn le_u64(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

/// The one extent of a member that is one run of one source, checked
/// against that source's length: an offset a container states wrongly is a
/// malformed container, told at the index, not a body that ends in a read
/// error halfway through a film.
pub(crate) fn direct_extent(
    source: &dyn ByteSource,
    format: &'static str,
    name: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<Extent>, Refusal> {
    let end = offset.checked_add(len).ok_or_else(|| {
        Refusal::Malformed(format!("{format} member {name} runs past the end of a u64"))
    })?;
    if end > source.len() {
        return Err(Refusal::Malformed(format!(
            "{format} member {name} claims {offset}..{end} of a {}-byte file",
            source.len()
        )));
    }
    Ok(vec![Extent {
        source: 0,
        offset,
        len,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::testing::{CountingSource, MemorySource};

    fn source(bytes: Vec<u8>) -> Arc<dyn ByteSource> {
        Arc::new(MemorySource::new("fixture", bytes))
    }

    /// A refusal is one sentence about the file, and its kind is the word
    /// the client switches on.
    #[test]
    fn every_refusal_names_itself_and_says_why() {
        let refusals = [
            Refusal::Compressed {
                format: "zip",
                method: "deflate".into(),
            },
            Refusal::Encrypted,
            Refusal::Solid,
            Refusal::Malformed("the central directory is not there".into()),
            Refusal::NoRandomAccess { format: "tar.gz" },
            Refusal::Unsupported {
                format: "UDF image",
                what: "a type 2 partition map".into(),
            },
        ];
        for refusal in refusals {
            let sentence = refusal.to_string();
            assert!(!refusal.kind().is_empty());
            assert!(sentence.len() > 20, "{sentence}");
            assert!(!sentence.contains("Refusal"), "{sentence}");
        }
    }

    /// A `tar.gz` is refused whole, before a byte of it is read: there is
    /// no member of it this server can point at.
    #[tokio::test]
    async fn a_tar_gz_is_refused_as_a_whole() {
        let refusal = TarGz
            .index(&[source(b"\x1f\x8b\x08 and a gzip stream".to_vec())])
            .await
            .expect_err("gzip has no random access");
        assert_eq!(refusal, Refusal::NoRandomAccess { format: "tar.gz" });
    }

    /// The budget is what stops a container that claims an index larger
    /// than any index: it refuses rather than reading on.
    #[tokio::test]
    async fn a_read_past_the_budget_is_refused() {
        let held = source(vec![0u8; 64]);
        let mut budget = Budget::new(held.as_ref(), "zip");
        let refusal = budget
            .read_exact(0, INDEX_BUDGET_BYTES as usize + 1)
            .await
            .expect_err("past the budget");
        assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
        assert_eq!(budget.used(), 0, "and nothing was read for it");
    }

    /// A structure that runs off the end of the container is malformed,
    /// not a short answer the parser then believes.
    #[tokio::test]
    async fn a_short_read_is_a_truncated_container() {
        let held = source(vec![7u8; 10]);
        let mut budget = Budget::new(held.as_ref(), "tar");
        assert_eq!(budget.read_exact(0, 10).await.unwrap(), vec![7u8; 10]);
        let refusal = budget.read_exact(4, 10).await.expect_err("truncated");
        assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
    }

    /// The blocking shim is a handle a synchronous parser can seek about
    /// in, and **a seek fetches nothing**: what a walk that skips past
    /// every member's data by size costs is the headers it reads, which is
    /// what the counting source underneath sees.
    #[tokio::test]
    async fn the_blocking_shim_reads_what_it_is_asked_and_seeks_for_free() {
        use std::io::{Read, Seek, SeekFrom};
        let counting = Arc::new(CountingSource::new(Arc::new(MemorySource::new(
            "volume.rar",
            (0..=255u8).cycle().take(100_000).collect::<Vec<_>>(),
        ))));
        let counts = counting.counts();
        let mut reader = IndexReader::new(counting.clone() as Arc<dyn ByteSource>);
        let tally = reader.tally();
        let read = tokio::task::spawn_blocking(move || {
            let mut head = [0u8; 8];
            reader.read_exact(&mut head).unwrap();
            // Past the member's data by its size, then the trailer.
            assert_eq!(reader.seek(SeekFrom::Current(90_000)).unwrap(), 90_008);
            let mut tail = [0u8; 4];
            reader.read_exact(&mut tail).unwrap();
            assert_eq!(reader.seek(SeekFrom::End(-2)).unwrap(), 99_998);
            let mut end = Vec::new();
            reader.read_to_end(&mut end).unwrap();
            assert_eq!(end.len(), 2);
            (head, tail)
        })
        .await
        .unwrap();
        assert_eq!(read.0, [0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(read.1[0], (90_008 % 256) as u8);
        assert_eq!(tally.load(Ordering::Relaxed), 14);
        assert_eq!(counts.read_at_bytes(), 14);
        assert!(!counts.read_any_of(8, 90_000), "{:?}", counts.ranges());
        assert_eq!(counts.opens(), 0);
    }

    /// And it stops at the budget the way `Budget` does: with an error the
    /// parser hands back and the translator recognises, not by reading on.
    #[tokio::test]
    async fn the_blocking_shim_refuses_to_read_past_the_budget() {
        use std::io::Read;
        let held = source(vec![0u8; 1024]);
        let mut reader = IndexReader::new(held);
        reader.left = 10;
        let error = tokio::task::spawn_blocking(move || {
            let mut buf = [0u8; 32];
            assert_eq!(reader.read(&mut buf).unwrap(), 10);
            reader.read(&mut buf).unwrap_err()
        })
        .await
        .unwrap();
        assert!(IndexReader::is_over_budget(&error), "{error}");
        assert!(!IndexReader::is_over_budget(&io::Error::other(
            "something else"
        )));
    }

    /// An extent a container states outside itself is refused where it is
    /// read, rather than becoming a body that fails mid-film.
    #[test]
    fn an_extent_past_the_container_is_malformed() {
        let held = source(vec![0u8; 100]);
        assert_eq!(
            direct_extent(held.as_ref(), "zip", "movie.mkv", 40, 60).unwrap(),
            vec![Extent {
                source: 0,
                offset: 40,
                len: 60
            }]
        );
        assert!(direct_extent(held.as_ref(), "zip", "movie.mkv", 40, 61).is_err());
        assert!(direct_extent(held.as_ref(), "zip", "movie.mkv", u64::MAX, 2).is_err());
    }
}

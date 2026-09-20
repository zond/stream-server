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
use std::sync::Arc;

pub mod session;
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
    use crate::sources::testing::MemorySource;

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

//! Disc images, as an index: which byte ranges of the image are each file's
//! data.
//!
//! This module answers one question and nothing else. Given an image -- an
//! ISO 9660 CD/DVD image, a UDF Blu-ray image, or a bridge disc that is both
//! -- it reads the descriptors and the directory tree and returns, per file,
//! the ranges of the *image's own bytes* that hold that file's data. It does
//! not decode, does not copy, does not decide policy and does not write. A
//! caller that wants a file's bytes reads those ranges from wherever the
//! image's bytes come from, which is the whole point: a disc image is a
//! translation of byte ranges, never a download.
//!
//! What follows from that:
//!
//! * **Reads are bounded and small.** Indexing reads volume descriptors,
//!   directory extents and file entries. It never reads a byte of file data.
//!   [`Budget`] enforces that as a hard number rather than as an intention,
//!   and the tests assert against it.
//! * **A refusal is typed and names its reason** ([`Refusal`]). There is no
//!   guess: an image whose structure this parser does not implement is a
//!   refusal that says which structure, not a partial answer with files
//!   missing. Everything read out of the image is untrusted -- every offset,
//!   every length, every name -- so every arithmetic step is checked and
//!   every bound is compared against the image's real length.
//! * **A file is a list of extents, in order.** ISO 9660 stores a file over
//!   4 GiB as several directory records for one name, which is several
//!   extents; UDF's allocation descriptors are a list by construction. The
//!   result type carries that for both. Directories are not files.
//!
//! The module is `pub` so that the crate's lint level (`warnings = "deny"`,
//! workspace-wide) does not call a finished, tested parser dead code while
//! the route that will use it is still a step away. The step that follows
//! adapts [`ImageReader`] to the repository's `ByteSource` and maps
//! [`Refusal`] onto the HTTP answers in `docs/translated-sources.md` §3.

use async_trait::async_trait;
use std::fmt;
use std::io;
use std::sync::Arc;

pub mod iso9660;
pub mod udf;

#[cfg(test)]
pub(crate) mod fixtures;

/// A disc image's sectors are 2048 bytes on every optical medium this
/// parser is for, and both formats' fixed locations are stated in them:
/// ISO 9660's primary volume descriptor at sector 16, UDF's anchor at
/// sector 256.
pub const SECTOR: u64 = 2048;

/// Random access to an image's bytes, and nothing more.
///
/// Deliberately the smallest trait that can be implemented over anything:
/// a file, a slice in memory, a torrent's piece store, a ranged HTTP
/// entity. The adapting step wraps the repository's `ByteSource` in one of
/// these (`len` and `read_at` are the same two calls under different
/// names), which is why nothing here asks for seeking, buffering or a
/// stream.
#[async_trait]
pub trait ImageReader: Send + Sync {
    /// The image's length in bytes. Known up front for every source there
    /// is; an index cannot be bounded against an unknown end.
    fn len(&self) -> u64;

    /// Whether the image is empty. Exists because [`ImageReader::len`] does;
    /// an empty image is a refusal, not a parse.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `buf.len()` bytes at `offset`, or fewer at the end of the image.
    /// Zero means end of image. Indexes are small scattered reads.
    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
}

/// An image held in memory, for tests and for the caller that already has
/// the bytes.
pub struct MemoryImage(pub Arc<[u8]>);

impl MemoryImage {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(Arc::from(bytes.into().into_boxed_slice()))
    }
}

#[async_trait]
impl ImageReader for MemoryImage {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let Ok(start) = usize::try_from(offset) else {
            return Ok(0);
        };
        if start >= self.0.len() {
            return Ok(0);
        }
        let n = buf.len().min(self.0.len() - start);
        buf[..n].copy_from_slice(&self.0[start..start + n]);
        Ok(n)
    }
}

/// One contiguous run of the image's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    /// Byte offset in the image itself, not in the file.
    pub offset: u64,
    pub len: u64,
}

impl Extent {
    /// The end offset, or `None` if the image claims a range that does not
    /// fit in a `u64` -- which is a malformed image, not a panic.
    fn end(&self) -> Option<u64> {
        self.offset.checked_add(self.len)
    }
}

/// A file inside an image: its path, its length, and the image's byte
/// ranges that hold its data, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageFile {
    /// Absolute path inside the image, `/`-separated, with a leading `/`.
    pub path: String,
    /// The file's length as the image states it. Equal to the sum of the
    /// extents' lengths except where the last extent is padded to a block
    /// boundary, which UDF allows: the extents are then trimmed to this.
    pub len: u64,
    /// In order. Several extents mean one file written in several pieces,
    /// which is the ordinary case for a file over 4 GiB in ISO 9660 and for
    /// any fragmented file in UDF.
    pub extents: Vec<Extent>,
}

/// Which format the index was read through, for logs and for the caller
/// that wants to say so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageFormat {
    /// ISO 9660. `joliet` is set when the names came from a Joliet
    /// supplementary descriptor, `rock_ridge` when any name came from a
    /// Rock Ridge `NM` entry.
    Iso9660 { joliet: bool, rock_ridge: bool },
    /// UDF, at the revision the logical volume's domain identifier states
    /// (`0x0250` for the Blu-ray case), or `None` when it states none.
    Udf { revision: Option<u16> },
}

impl fmt::Display for ImageFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Iso9660 { joliet, rock_ridge } => {
                f.write_str("ISO 9660")?;
                if *joliet {
                    f.write_str(" + Joliet")?;
                }
                if *rock_ridge {
                    f.write_str(" + Rock Ridge")?;
                }
                Ok(())
            }
            Self::Udf { revision } => match revision {
                Some(r) => write!(f, "UDF {:x}.{:02x}", r >> 8, r & 0xff),
                None => f.write_str("UDF"),
            },
        }
    }
}

/// What an image holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageIndex {
    pub format: ImageFormat,
    /// Every regular file, in the order the tree was walked. Directories
    /// are not here: a directory is not a file and has no bytes to serve.
    pub files: Vec<ImageFile>,
}

/// Why an image was not indexed. Every variant names something concrete;
/// none of them means "probably nothing here".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// Nothing at the fixed locations both formats put their descriptors
    /// at. Not an image, or an image of a filesystem this is not for.
    NotAnImage { detail: String },
    /// A structure this parser does not implement, named. The image is
    /// well-formed as far as it was read; the answer would be wrong rather
    /// than late, so there is no answer.
    Unsupported { format: &'static str, what: String },
    /// The image contradicts itself, or points outside itself, or is
    /// truncated.
    Malformed {
        format: &'static str,
        detail: String,
    },
    /// The image, or the part of it the index needs, is encrypted. There is
    /// no key here and a guess is ciphertext.
    Encrypted { format: &'static str },
    /// The reader failed. The image may be fine; these bytes are not
    /// available.
    Unreadable { detail: String },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAnImage { detail } => write!(f, "not a disc image: {detail}"),
            Self::Unsupported { format, what } => {
                write!(f, "{format} image uses {what}, which is not supported")
            }
            Self::Malformed { format, detail } => write!(f, "malformed {format} image: {detail}"),
            Self::Encrypted { format } => write!(f, "{format} image is encrypted"),
            Self::Unreadable { detail } => write!(f, "image could not be read: {detail}"),
        }
    }
}

impl std::error::Error for Refusal {}

/// Index whatever the image is.
///
/// ISO 9660 is tried first, because a bridge disc -- a DVD-Video image,
/// which is both ISO 9660 and UDF -- holds the same files in both trees and
/// the 9660 tree is the cheaper walk. A UDF-only image (every Blu-ray) has
/// no `CD001` descriptor at all and falls through to UDF.
///
/// The refusal returned when neither recognises the image is the *9660*
/// one only when 9660 got far enough to recognise something; otherwise the
/// UDF refusal is the more informative and is what comes back.
pub async fn index(reader: &dyn ImageReader) -> Result<ImageIndex, Refusal> {
    let iso = iso9660::index(reader).await;
    match iso {
        Ok(index) => Ok(index),
        Err(Refusal::NotAnImage { detail }) => match udf::index(reader).await {
            Ok(index) => Ok(index),
            Err(Refusal::NotAnImage { detail: udf_detail }) => Err(Refusal::NotAnImage {
                detail: format!("{detail}; {udf_detail}"),
            }),
            Err(other) => Err(other),
        },
        Err(other) => Err(other),
    }
}

/// How many bytes an index is allowed to read.
///
/// Not a guideline: the parsers go through this and nothing else, so an
/// image that would make the walk read forever -- a directory claiming a
/// gigabyte of records, a tree of a million empty directories -- stops at a
/// stated number with a refusal that says so, instead of being discovered
/// by a stopwatch. The tests count against the same counter the parser is
/// bounded by.
pub struct Budget<'a> {
    reader: &'a dyn ImageReader,
    /// Bytes still allowed.
    left: u64,
    /// Bytes actually read, for the tests and for logs.
    used: u64,
    format: &'static str,
}

/// The index read budget, per image. Generous against any real disc --
/// a DVD-Video image's descriptors and `VIDEO_TS` directory are a few
/// tens of kilobytes, and a Blu-ray's UDF metadata a few hundred -- and
/// small enough that a hostile image cannot turn an index into a download.
pub const INDEX_READ_BUDGET: u64 = 8 * 1024 * 1024;

impl<'a> Budget<'a> {
    pub fn new(reader: &'a dyn ImageReader, format: &'static str) -> Self {
        Self {
            reader,
            left: INDEX_READ_BUDGET,
            used: 0,
            format,
        }
    }

    pub fn used(&self) -> u64 {
        self.used
    }

    pub fn image_len(&self) -> u64 {
        self.reader.len()
    }

    /// Exactly `len` bytes at `offset`. A short read is the image ending
    /// early, which for a structure the index needs is a truncated image
    /// and therefore malformed -- never a shorter structure parsed anyway.
    pub async fn read_exact(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, Refusal> {
        let need = len as u64;
        if need > self.left {
            return Err(Refusal::Malformed {
                format: self.format,
                detail: format!(
                    "index reads exceed the {INDEX_READ_BUDGET}-byte budget (used {} so far)",
                    self.used
                ),
            });
        }
        // Checked against the image's real length before the read, so a
        // record pointing past the end is a refusal naming the offset
        // rather than a read that happens to come back short.
        let end = offset.checked_add(need).ok_or_else(|| Refusal::Malformed {
            format: self.format,
            detail: format!("a structure at offset {offset} claims {len} bytes, which overflows"),
        })?;
        if end > self.reader.len() {
            return Err(Refusal::Malformed {
                format: self.format,
                detail: format!(
                    "a structure at offset {offset} claims {len} bytes, past the image's {} bytes",
                    self.reader.len()
                ),
            });
        }
        let mut buf = vec![0u8; len];
        let mut got = 0usize;
        while got < len {
            let n = self
                .reader
                .read_at(offset + got as u64, &mut buf[got..])
                .await
                .map_err(|e| Refusal::Unreadable {
                    detail: format!("read at {} failed: {e}", offset + got as u64),
                })?;
            if n == 0 {
                return Err(Refusal::Malformed {
                    format: self.format,
                    detail: format!(
                        "image ends inside a structure at offset {offset} ({len} bytes wanted, {got} read)"
                    ),
                });
            }
            got += n;
        }
        self.left -= need;
        self.used += need;
        Ok(buf)
    }

    /// One sector, or a refusal. The common case of [`Budget::read_exact`].
    pub async fn read_sector(&mut self, sector: u64) -> Result<Vec<u8>, Refusal> {
        let offset = sector
            .checked_mul(SECTOR)
            .ok_or_else(|| Refusal::Malformed {
                format: self.format,
                detail: format!("sector number {sector} overflows a byte offset"),
            })?;
        self.read_exact(offset, SECTOR as usize).await
    }

    pub fn malformed(&self, detail: impl Into<String>) -> Refusal {
        Refusal::Malformed {
            format: self.format,
            detail: detail.into(),
        }
    }

    pub fn unsupported(&self, what: impl Into<String>) -> Refusal {
        Refusal::Unsupported {
            format: self.format,
            what: what.into(),
        }
    }
}

// Little-endian readers over untrusted bytes. Each returns `None` rather
// than panicking when the slice is too short: a descriptor that ends early
// is a malformed image and the caller turns it into that refusal, with the
// name of the field it was reading.
pub(crate) fn le_u16(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?))
}

pub(crate) fn le_u32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

pub(crate) fn le_u64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

/// Trim a file's extents so that they sum to exactly `len`.
///
/// Both formats round the last extent up to a block boundary, and UDF
/// states the real length separately in the file entry. Serving the padding
/// would append the block's tail to the film, so the extents are cut to the
/// stated length here, once, for both parsers. Extents that sum to *less*
/// than the stated length are a contradiction the caller refuses; this
/// returns the sum so it can.
pub(crate) fn trim_to_len(extents: &mut Vec<Extent>, len: u64) -> u64 {
    let mut kept = 0u64;
    let mut cut = extents.len();
    for (i, e) in extents.iter_mut().enumerate() {
        if kept >= len {
            cut = i;
            break;
        }
        let room = len - kept;
        if e.len > room {
            e.len = room;
        }
        kept += e.len;
    }
    extents.truncate(cut);
    extents.retain(|e| e.len > 0);
    kept
}

/// Every extent lies inside the image, and none of them overflows.
pub(crate) fn extents_within(extents: &[Extent], image_len: u64) -> Result<(), String> {
    for e in extents {
        let end = e.end().ok_or_else(|| {
            format!(
                "an extent at {} claims {} bytes, which overflows",
                e.offset, e.len
            )
        })?;
        if end > image_len {
            return Err(format!(
                "an extent at {} claims {} bytes, past the image's {image_len} bytes",
                e.offset, e.len
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::images::fixtures::{self, CountingImage, iso, udf as udf_fx};

    #[tokio::test]
    async fn an_iso_9660_image_is_indexed_through_its_own_tree() {
        let image = MemoryImage::new(iso::minimal_iso());
        let idx = index(&image).await.expect("indexed");
        assert!(matches!(idx.format, ImageFormat::Iso9660 { .. }));
        assert_eq!(idx.files.len(), 1);
        assert_eq!(idx.files[0].path, "/HELLO.TXT");
    }

    #[tokio::test]
    async fn a_udf_only_image_falls_through_to_udf() {
        let image = MemoryImage::new(udf_fx::minimal_udf());
        let idx = index(&image).await.expect("indexed");
        assert!(matches!(idx.format, ImageFormat::Udf { .. }));
        assert_eq!(idx.files[0].path, "/MOVIE.BIN");
    }

    #[tokio::test]
    async fn a_bridge_image_is_indexed_through_its_9660_tree() {
        // Both descriptor sets present: the 9660 answer is the one taken,
        // which is what `docs/translated-sources.md` §2.2 says a bridge
        // disc should cost.
        let image = MemoryImage::new(udf_fx::bridge_image());
        let idx = index(&image).await.expect("indexed");
        assert!(matches!(idx.format, ImageFormat::Iso9660 { .. }));
    }

    #[tokio::test]
    async fn bytes_that_are_neither_format_are_refused_naming_both() {
        let image = MemoryImage::new(vec![0u8; 600 * 1024]);
        let err = index(&image).await.expect_err("refused");
        let Refusal::NotAnImage { detail } = &err else {
            panic!("expected NotAnImage, got {err:?}");
        };
        assert!(detail.contains("CD001"), "{detail}");
        assert!(detail.contains("anchor"), "{detail}");
    }

    #[tokio::test]
    async fn an_empty_reader_is_refused_not_panicked() {
        let image = MemoryImage::new(Vec::new());
        assert!(image.is_empty());
        let err = index(&image).await.expect_err("refused");
        assert!(matches!(err, Refusal::NotAnImage { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn indexing_never_reads_the_file_data() {
        // The one assertion this whole module exists for: the bytes of the
        // file are never among the bytes read. Counted by range, not by
        // total, so a parser that read the film in small pieces would still
        // fail this.
        let bytes = iso::minimal_iso();
        let data_range = iso::data_range(&bytes);
        let image = CountingImage::new(bytes);
        let idx = index(&image).await.expect("indexed");
        assert_eq!(idx.files[0].extents.len(), 1);
        assert_eq!(idx.files[0].extents[0].offset, data_range.0);
        assert!(
            !image.read_any_of(data_range.0, data_range.1),
            "the index read file data: {:?}",
            image.reads()
        );
    }

    #[test]
    fn trimming_cuts_the_padding_and_drops_extents_past_the_end() {
        let mut extents = vec![
            Extent {
                offset: 0,
                len: 2048,
            },
            Extent {
                offset: 4096,
                len: 2048,
            },
        ];
        assert_eq!(trim_to_len(&mut extents, 3000), 3000);
        assert_eq!(
            extents,
            vec![
                Extent {
                    offset: 0,
                    len: 2048
                },
                Extent {
                    offset: 4096,
                    len: 952
                }
            ]
        );

        let mut short = vec![Extent { offset: 0, len: 10 }];
        // A stated length the extents cannot cover comes back as the sum,
        // so the caller can call it the contradiction it is.
        assert_eq!(trim_to_len(&mut short, 99), 10);
    }

    #[tokio::test]
    async fn a_read_past_the_image_is_malformed_not_a_short_parse() {
        let image = MemoryImage::new(vec![0u8; 100]);
        let mut budget = Budget::new(&image, "test");
        let err = budget.read_exact(64, 128).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("past the image")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn the_budget_refuses_rather_than_reading_forever() {
        let image = MemoryImage::new(vec![0u8; (INDEX_READ_BUDGET + SECTOR) as usize]);
        let mut budget = Budget::new(&image, "test");
        let mut sector = 0;
        loop {
            match budget.read_sector(sector).await {
                Ok(_) => sector += 1,
                Err(e) => {
                    assert!(
                        matches!(&e, Refusal::Malformed { detail, .. } if detail.contains("budget")),
                        "{e:?}"
                    );
                    break;
                }
            }
        }
        assert_eq!(budget.used(), INDEX_READ_BUDGET);
    }

    /// A real image, written by a real tool, indexed through both of its
    /// trees -- and the extents checked against the bytes they claim to
    /// be. A fixture built in the test proves the parser matches this
    /// reading of the standard; only a tool's image proves it matches
    /// what tools write.
    ///
    /// Skipped, loudly, when no tool is installed: this must not be the
    /// test that fails on a machine that never had `genisoimage`.
    #[tokio::test]
    async fn a_real_bridge_image_indexes_identically_through_9660_and_udf() {
        let Some(tool) = image_writer() else {
            eprintln!(
                "skipping a_real_bridge_image_indexes_identically_through_9660_and_udf: \
                 none of genisoimage, mkisofs or xorriso is installed"
            );
            return;
        };
        let dir = std::env::temp_dir().join(format!(
            "stream-server-image-fixture-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("after the epoch")
                .as_nanos()
        ));
        let src = dir.join("src");
        std::fs::create_dir_all(src.join("SUBDIR")).expect("scratch dir");
        let top = b"hello from a real image\n";
        let nested = vec![b'N'; 5000];
        std::fs::write(src.join("HELLO.TXT"), top).expect("write");
        std::fs::write(src.join("SUBDIR").join("NESTED.BIN"), &nested).expect("write");
        let iso_path = dir.join("fixture.iso");

        let mut command = std::process::Command::new(&tool.0);
        command.args(&tool.1);
        let status = command
            .arg("-quiet")
            .arg("-r")
            .arg("-J")
            .arg("-udf")
            .arg("-o")
            .arg(&iso_path)
            .arg(&src)
            .status()
            .expect("run the image writer");
        assert!(status.success(), "{} failed: {status}", tool.0);
        let bytes = std::fs::read(&iso_path).expect("read the image");
        let _ = std::fs::remove_dir_all(&dir);

        let image = CountingImage::new(bytes.clone());
        let iso_index = iso9660::index(&image).await.expect("indexed as ISO 9660");
        let udf_index = udf::index(&image).await.expect("indexed as UDF");

        // The same files, at the same ranges of the same image, through
        // two completely separate parsers: the strongest check there is
        // that neither is reading the standard wrong.
        let mut iso_files = iso_index.files.clone();
        let mut udf_files = udf_index.files.clone();
        iso_files.sort_by(|a, b| a.path.cmp(&b.path));
        udf_files.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(
            iso_files, udf_files,
            "the two trees of a bridge image disagree"
        );
        assert_eq!(
            iso_files
                .iter()
                .map(|f| f.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/HELLO.TXT", "/SUBDIR/NESTED.BIN"]
        );

        // And the extents are the bytes they say they are.
        let hello = &iso_files[0];
        assert_eq!(hello.len, top.len() as u64);
        let at = hello.extents[0].offset as usize;
        assert_eq!(&bytes[at..at + top.len()], top);
        let big = &iso_files[1];
        assert_eq!(big.len, nested.len() as u64);
        let at = big.extents[0].offset as usize;
        assert_eq!(&bytes[at..at + nested.len()], &nested[..]);

        // Two indexes of a 5 KB-plus image, and still nothing like a read
        // of its contents.
        assert!(
            image.total() < 64 * SECTOR,
            "read {} bytes to index a real image twice",
            image.total()
        );
    }

    /// `genisoimage`, `mkisofs`, or `xorriso` in its mkisofs persona, with
    /// the arguments that pick that persona.
    fn image_writer() -> Option<(String, Vec<String>)> {
        for (tool, args) in [
            ("genisoimage", vec![]),
            ("mkisofs", vec![]),
            ("xorriso", vec!["-as".to_string(), "mkisofs".to_string()]),
        ] {
            let found = std::process::Command::new("which")
                .arg(tool)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            if found {
                return Some((tool.to_string(), args));
            }
        }
        None
    }

    #[tokio::test]
    async fn a_reader_error_is_unreadable_not_malformed() {
        let image = fixtures::FailingImage { len: 1 << 20 };
        let err = index(&image).await.expect_err("refused");
        assert!(matches!(err, Refusal::Unreadable { .. }), "{err:?}");
    }
}

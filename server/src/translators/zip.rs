//! ZIP, read as an index and never as a decoder.
//!
//! The walk is the format's own: the end-of-central-directory record in
//! the last 64 KiB (through the zip64 locator where one is there), the
//! central directory it points at, and -- for every member that is
//! *stored* -- that member's **local** header, for where its bytes start.
//!
//! **The local header and not the central directory**, because the spec
//! lets a member's extra field differ in length between the two, and an
//! offset wrong by a few bytes is a film that will not decode. That is
//! also the whole of what this costs: 30 bytes per stored member. A
//! compressed member is refused and never has its local header read, so an
//! archive of a thousand deflated files costs the directory and nothing
//! more.
//!
//! Parsed by hand rather than through `async_zip`, which is still a
//! dependency of this workspace but reads through a `BufReader` whose
//! fills are its own business: what is wanted here is a stated number of
//! bytes at stated offsets, counted (see [`Budget`]), because the claim
//! being made is about what was *not* read.

use super::{
    Body, Budget, Index, Member, Refusal, Translator, direct_extent, le_u16, le_u32, le_u64,
    only_source,
};
use crate::sources::ByteSource;
use async_trait::async_trait;
use std::sync::Arc;

/// `PK\x05\x06`, and the rest of the signatures this walks by.
const EOCD_SIGNATURE: u32 = 0x0605_4b50;
const CENTRAL_SIGNATURE: u32 = 0x0201_4b50;
const LOCAL_SIGNATURE: u32 = 0x0403_4b50;
const ZIP64_LOCATOR_SIGNATURE: u32 = 0x0706_4b50;
const ZIP64_EOCD_SIGNATURE: u32 = 0x0606_4b50;

/// The fixed parts, in bytes.
const EOCD_BYTES: u64 = 22;
const CENTRAL_ENTRY_BYTES: usize = 46;
const LOCAL_HEADER_BYTES: u64 = 30;
const ZIP64_LOCATOR_BYTES: usize = 20;
const ZIP64_EOCD_BYTES: usize = 56;

/// How far back the end-of-central-directory record is looked for: its own
/// 22 bytes plus the longest comment the format can put after it.
const EOCD_SEARCH_BYTES: u64 = EOCD_BYTES + 0xffff;

/// General-purpose bit 0: the member's bytes are encrypted.
const ENCRYPTED_FLAG: u16 = 1;

/// The one field value that means "look in the zip64 record instead".
const ZIP64_U32: u32 = 0xffff_ffff;
const ZIP64_U16: u16 = 0xffff;

pub struct Zip;

#[async_trait]
impl Translator for Zip {
    fn format(&self) -> &'static str {
        "zip"
    }

    async fn index(&self, sources: &[Arc<dyn ByteSource>]) -> Result<Index, Refusal> {
        let source = only_source(sources, "zip")?;
        let mut budget = Budget::new(source.as_ref(), "zip");
        let directory = central_directory(&mut budget).await?;
        let mut members = Vec::new();
        let mut at = 0usize;
        while at + CENTRAL_ENTRY_BYTES <= directory.len() {
            if le_u32(&directory, at) != Some(CENTRAL_SIGNATURE) {
                // The directory is a run of entries and nothing else, so a
                // byte that is not one means the record said a size the
                // entries do not fill.
                return Err(budget.malformed(format!(
                    "the central directory holds something other than an entry at {at}"
                )));
            }
            let entry = Entry::parse(&directory, at).ok_or_else(|| {
                budget.malformed(format!("a central directory entry at {at} is truncated"))
            })?;
            at += entry.entry_bytes;
            if entry.is_directory() {
                continue;
            }
            let body = entry.body(&mut budget, source.as_ref()).await?;
            members.push(Member {
                name: entry.name,
                len: entry.uncompressed_size,
                body,
            });
        }
        Ok(Index { members })
    }
}

/// The central directory's bytes, found through the end-of-central-
/// directory record (and the zip64 pair behind it where the counts have
/// run out of 32 bits).
async fn central_directory(budget: &mut Budget<'_>) -> Result<Vec<u8>, Refusal> {
    let (eocd_at, eocd) = end_record(budget).await?;
    let mut offset = u64::from(
        le_u32(&eocd, 16).ok_or_else(|| budget.malformed("the end record is truncated"))?,
    );
    let mut size = u64::from(
        le_u32(&eocd, 12).ok_or_else(|| budget.malformed("the end record is truncated"))?,
    );
    let disk = le_u16(&eocd, 4).unwrap_or(0);
    let directory_disk = le_u16(&eocd, 6).unwrap_or(0);
    if disk != directory_disk && disk != ZIP64_U16 {
        return Err(Refusal::Malformed(
            "this is one part of a split zip, and the other parts were not named".to_string(),
        ));
    }

    // zip64, when the 32-bit fields have run out: the locator sits
    // immediately before the end record and points at the record that
    // carries the real figures. Twenty bytes, read only when a sentinel
    // says they are there.
    if (offset == u64::from(ZIP64_U32) || size == u64::from(ZIP64_U32))
        && eocd_at >= ZIP64_LOCATOR_BYTES as u64
    {
        let locator = budget
            .read_exact(eocd_at - ZIP64_LOCATOR_BYTES as u64, ZIP64_LOCATOR_BYTES)
            .await?;
        if le_u32(&locator, 0) == Some(ZIP64_LOCATOR_SIGNATURE) {
            let record_at = le_u64(&locator, 8)
                .ok_or_else(|| budget.malformed("the zip64 locator is truncated"))?;
            let record = budget.read_exact(record_at, ZIP64_EOCD_BYTES).await?;
            if le_u32(&record, 0) != Some(ZIP64_EOCD_SIGNATURE) {
                return Err(budget.malformed(
                    "the zip64 locator points at something that is not a zip64 end record",
                ));
            }
            size = le_u64(&record, 40).unwrap_or(size);
            offset = le_u64(&record, 48).unwrap_or(offset);
        }
    }

    let end = offset.saturating_add(size);
    if end > budget.source_len() {
        return Err(budget.malformed(format!(
            "the end record puts the central directory at {offset}..{end} of a {}-byte file",
            budget.source_len()
        )));
    }
    budget.read_exact(offset, size as usize).await
}

/// The end-of-central-directory record: where it is, and its bytes from
/// there to the end of the file.
///
/// **Twenty-two bytes first.** A zip with no comment ends exactly at its
/// end record, which is every zip anything writes, and those 22 bytes are
/// the whole of what has to be read to find the directory. The 64 KiB
/// window the format allows for a comment is read only when they are not
/// it -- because that window is not free: over a torrent it is 64 KiB of
/// pieces fetched from the swarm, and it lands in the last member's data,
/// which is the one place an index has no business reading.
async fn end_record(budget: &mut Budget<'_>) -> Result<(u64, Vec<u8>), Refusal> {
    let total = budget.source_len();
    if total >= EOCD_BYTES {
        let at = total - EOCD_BYTES;
        let tail = budget.read_exact(at, EOCD_BYTES as usize).await?;
        if le_u32(&tail, 0) == Some(EOCD_SIGNATURE) && le_u16(&tail, 20) == Some(0) {
            return Ok((at, tail));
        }
    }
    let tail = budget.read_tail(EOCD_SEARCH_BYTES).await?;
    let tail_at = total - tail.len() as u64;
    let found = (0..tail.len().saturating_sub(EOCD_BYTES as usize - 1))
        .rev()
        .find(|at| le_u32(&tail, *at) == Some(EOCD_SIGNATURE))
        .ok_or_else(|| {
            budget.malformed(
                "there is no end-of-central-directory record in the last 64 KiB, so this is not a \
                 zip",
            )
        })?;
    Ok((tail_at + found as u64, tail[found..].to_vec()))
}

/// One central directory entry, as far as this needs it.
struct Entry {
    name: String,
    flags: u16,
    method: u16,
    compressed_size: u64,
    uncompressed_size: u64,
    local_header_at: u64,
    disk: u16,
    /// How long this entry is, so the walk can step over it.
    entry_bytes: usize,
}

impl Entry {
    fn parse(directory: &[u8], at: usize) -> Option<Self> {
        let field16 = |offset: usize| le_u16(directory, at + offset);
        let field32 = |offset: usize| le_u32(directory, at + offset);
        let name_len = field16(28)? as usize;
        let extra_len = field16(30)? as usize;
        let comment_len = field16(32)? as usize;
        let entry_bytes = CENTRAL_ENTRY_BYTES + name_len + extra_len + comment_len;
        if at + entry_bytes > directory.len() {
            return None;
        }
        let name_at = at + CENTRAL_ENTRY_BYTES;
        // Lossy on purpose: a name that is not UTF-8 (general-purpose bit
        // 11 unset means cp437) still names a member, and a member nobody
        // asks for by that name is simply never selected.
        let name = String::from_utf8_lossy(&directory[name_at..name_at + name_len]).into_owned();
        let extra = &directory[name_at + name_len..name_at + name_len + extra_len];
        let mut entry = Self {
            name,
            flags: field16(8)?,
            method: field16(10)?,
            compressed_size: u64::from(field32(20)?),
            uncompressed_size: u64::from(field32(24)?),
            local_header_at: u64::from(field32(42)?),
            disk: field16(34)?,
            entry_bytes,
        };
        entry.apply_zip64(extra);
        Some(entry)
    }

    /// The zip64 extra field (header id 1) carries whichever of the four
    /// figures overflowed 32 bits, in a fixed order and only for those
    /// that did -- so it is read against the sentinels, not by position.
    fn apply_zip64(&mut self, extra: &[u8]) {
        let mut at = 0usize;
        while at + 4 <= extra.len() {
            let id = le_u16(extra, at).unwrap_or(0);
            let len = le_u16(extra, at + 2).unwrap_or(0) as usize;
            let body_at = at + 4;
            if body_at + len > extra.len() {
                return;
            }
            if id == 1 {
                let body = &extra[body_at..body_at + len];
                let mut read = 0usize;
                let next = |read: &mut usize| {
                    let value = le_u64(body, *read);
                    if value.is_some() {
                        *read += 8;
                    }
                    value
                };
                if self.uncompressed_size == u64::from(ZIP64_U32)
                    && let Some(value) = next(&mut read)
                {
                    self.uncompressed_size = value;
                }
                if self.compressed_size == u64::from(ZIP64_U32)
                    && let Some(value) = next(&mut read)
                {
                    self.compressed_size = value;
                }
                if self.local_header_at == u64::from(ZIP64_U32)
                    && let Some(value) = next(&mut read)
                {
                    self.local_header_at = value;
                }
                return;
            }
            at = body_at + len;
        }
    }

    /// A directory entry is not a file: it has no bytes and nothing asks
    /// for it by name.
    fn is_directory(&self) -> bool {
        self.name.ends_with('/')
    }

    /// Where this member's bytes are, or why they cannot be pointed at.
    async fn body(
        &self,
        budget: &mut Budget<'_>,
        source: &dyn ByteSource,
    ) -> Result<Body, Refusal> {
        if self.disk != 0 && self.disk != ZIP64_U16 {
            return Ok(Body::Opaque(Refusal::Malformed(format!(
                "{} is on part {} of a split zip, and only one part was named",
                self.name, self.disk
            ))));
        }
        if self.flags & ENCRYPTED_FLAG != 0 {
            return Ok(Body::Opaque(Refusal::Encrypted));
        }
        if self.method != 0 {
            return Ok(Body::Opaque(Refusal::Compressed {
                format: "zip",
                method: method_name(self.method),
            }));
        }
        if self.compressed_size != self.uncompressed_size {
            return Ok(Body::Opaque(Refusal::Malformed(format!(
                "{} is stored but its two sizes differ ({} and {})",
                self.name, self.compressed_size, self.uncompressed_size
            ))));
        }
        // The one read per stored member, and the reason it is per member:
        // the extra field may be a different length here than in the
        // directory, so only this header says where the bytes start.
        let header = budget
            .read_exact(self.local_header_at, LOCAL_HEADER_BYTES as usize)
            .await?;
        if le_u32(&header, 0) != Some(LOCAL_SIGNATURE) {
            return Ok(Body::Opaque(Refusal::Malformed(format!(
                "{} has no local header where the directory says it does",
                self.name
            ))));
        }
        // Checked again here rather than trusted from the directory: the
        // two copies of the flags may differ, and the local one describes
        // the bytes that follow it.
        if le_u16(&header, 6).unwrap_or(0) & ENCRYPTED_FLAG != 0 {
            return Ok(Body::Opaque(Refusal::Encrypted));
        }
        let names = u64::from(le_u16(&header, 26).unwrap_or(0))
            + u64::from(le_u16(&header, 28).unwrap_or(0));
        let data_at = self.local_header_at + LOCAL_HEADER_BYTES + names;
        match direct_extent(source, "zip", &self.name, data_at, self.uncompressed_size) {
            Ok(extents) => Ok(Body::Direct(extents)),
            // A member whose stated range is outside the file is refused
            // as that member, not as the archive: the rest of it is still
            // readable and the player asked for one file.
            Err(refusal) => Ok(Body::Opaque(refusal)),
        }
    }
}

/// What a compression method is called, for the sentence the player shows.
fn method_name(method: u16) -> String {
    match method {
        1 => "shrunk".to_string(),
        6 => "imploded".to_string(),
        8 => "deflate".to_string(),
        9 => "deflate64".to_string(),
        12 => "bzip2".to_string(),
        14 => "lzma".to_string(),
        93 => "zstd".to_string(),
        95 => "xz".to_string(),
        98 => "ppmd".to_string(),
        other => format!("method {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::Extent;
    use crate::sources::testing::{CountingSource, MemorySource};

    /// Deterministic, mildly incompressible bytes: a member a test can
    /// find in the archive by value.
    pub(crate) fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i.wrapping_mul(31) % 251) as u8).collect()
    }

    /// A zip of `members`, each written with its own compression method.
    pub(crate) async fn zip_of(members: &[(&str, Vec<u8>, async_zip::Compression)]) -> Vec<u8> {
        let mut writer = async_zip::base::write::ZipFileWriter::with_tokio(Vec::new());
        for (name, data, compression) in members {
            writer
                .write_entry_whole(
                    async_zip::ZipEntryBuilder::new((*name).into(), *compression).build(),
                    data,
                )
                .await
                .expect("write the member");
        }
        writer.close().await.expect("close the zip").into_inner()
    }

    fn sources(bytes: Vec<u8>) -> Vec<Arc<dyn ByteSource>> {
        vec![Arc::new(MemorySource::new("fixture.zip", bytes))]
    }

    /// A stored member is one range of the archive, and it is the range
    /// its bytes really are at -- checked against the archive itself, not
    /// against the arithmetic that produced it.
    #[tokio::test]
    async fn a_stored_member_is_the_range_of_the_archive_its_bytes_are_at() {
        let data = payload(4096);
        let archive = zip_of(&[("movie.mkv", data.clone(), async_zip::Compression::Stored)]).await;
        let index = Zip.index(&sources(archive.clone())).await.expect("indexed");
        assert_eq!(index.members.len(), 1);
        let member = &index.members[0];
        assert_eq!(member.name, "movie.mkv");
        assert_eq!(member.len, data.len() as u64);
        let Body::Direct(extents) = &member.body else {
            panic!("a stored member is direct: {:?}", member.body);
        };
        assert_eq!(extents.len(), 1);
        let Extent { offset, len, .. } = extents[0];
        assert_eq!(
            &archive[offset as usize..(offset + len) as usize],
            data.as_slice(),
            "the extent points at bytes that are not the member's"
        );
    }

    /// Everything that is not stored is a typed refusal, and the refusal
    /// names the method so the player can say what it is.
    #[tokio::test]
    async fn a_deflated_member_is_refused_as_compressed() {
        let archive =
            zip_of(&[("movie.mkv", payload(4096), async_zip::Compression::Deflate)]).await;
        let index = Zip.index(&sources(archive)).await.expect("indexed");
        assert_eq!(
            index.members[0].body,
            Body::Opaque(Refusal::Compressed {
                format: "zip",
                method: "deflate".into()
            })
        );
    }

    /// Directories are not members: they have no bytes, and a player that
    /// asked for one by name would be asking for nothing.
    #[tokio::test]
    async fn directories_are_not_members() {
        let archive = zip_of(&[
            ("videos/", Vec::new(), async_zip::Compression::Stored),
            (
                "videos/movie.mkv",
                payload(64),
                async_zip::Compression::Stored,
            ),
        ])
        .await;
        let index = Zip.index(&sources(archive)).await.expect("indexed");
        assert_eq!(index.members.len(), 1);
        assert_eq!(index.members[0].name, "videos/movie.mkv");
    }

    /// **The index reads the index and not the film.** Counted by range
    /// and not by total, so a parser that read the member in small pieces
    /// would still fail this; and counted against the format's own bound,
    /// so one that read the whole archive into a buffer first would too.
    #[tokio::test]
    async fn indexing_reads_the_directory_and_one_local_header_and_nothing_else() {
        const LEN: usize = 512 * 1024;
        let data = payload(LEN);
        let archive = zip_of(&[
            (
                "notes.txt",
                b"small".to_vec(),
                async_zip::Compression::Stored,
            ),
            ("movie.mkv", data.clone(), async_zip::Compression::Stored),
        ])
        .await;
        let counting = Arc::new(CountingSource::new(Arc::new(MemorySource::new(
            "fixture.zip",
            archive.clone(),
        ))));
        let counts = counting.counts();
        let index = Zip
            .index(&[counting.clone() as Arc<dyn ByteSource>])
            .await
            .expect("indexed");

        let Body::Direct(extents) = &index.members[1].body else {
            panic!("stored");
        };
        let (at, len) = (extents[0].offset, extents[0].len);
        assert_eq!(&archive[at as usize..(at + len) as usize], data.as_slice());
        assert!(
            !counts.read_any_of(at, len),
            "the index read the member's own data: {:?}",
            counts.ranges()
        );

        // The stated bound: the end record, the central directory (two
        // entries, well under a kibibyte), and 30 bytes per stored member.
        // Nothing was opened at all -- an index is `read_at`s.
        let bound = EOCD_BYTES + 1024 + 2 * LOCAL_HEADER_BYTES;
        assert!(
            counts.read_at_bytes() <= bound,
            "{} bytes read against a bound of {bound}",
            counts.read_at_bytes()
        );
        assert_eq!(counts.opens(), 0);
    }

    /// Bytes that are not a zip are refused by saying so, not by reading
    /// on looking for something that is not there.
    #[tokio::test]
    async fn bytes_that_are_not_a_zip_are_refused() {
        let refusal = Zip
            .index(&sources(b"not a zip at all".to_vec()))
            .await
            .expect_err("no end record");
        assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
    }

    /// An end record pointing at a directory outside the file is the
    /// archive being truncated, and is said once rather than read at.
    #[tokio::test]
    async fn a_directory_outside_the_file_is_malformed() {
        let mut archive =
            zip_of(&[("movie.mkv", payload(64), async_zip::Compression::Stored)]).await;
        let end = archive.len() - EOCD_BYTES as usize;
        // The central directory's offset, in the end record.
        archive[end + 16..end + 20].copy_from_slice(&u32::MAX.to_le_bytes()[..4]);
        let refusal = Zip.index(&sources(archive)).await.expect_err("truncated");
        assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
    }

    /// A local header that is not where the directory says it is means the
    /// member's bytes are not where the arithmetic would put them, so that
    /// member is refused rather than served from an offset nobody checked.
    #[tokio::test]
    async fn a_member_whose_local_header_is_missing_is_refused() {
        let mut archive =
            zip_of(&[("movie.mkv", payload(64), async_zip::Compression::Stored)]).await;
        archive[0..4].copy_from_slice(b"XXXX");
        let index = Zip.index(&sources(archive)).await.expect("indexed");
        assert!(
            matches!(index.members[0].body, Body::Opaque(Refusal::Malformed(_))),
            "{:?}",
            index.members[0].body
        );
    }

    #[test]
    fn a_method_is_named_for_the_sentence_the_player_shows() {
        assert_eq!(method_name(8), "deflate");
        assert_eq!(method_name(14), "lzma");
        assert_eq!(method_name(77), "method 77");
    }
}

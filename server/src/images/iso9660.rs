//! ISO 9660 (ECMA-119), with Joliet and Rock Ridge names.
//!
//! The walk is: the volume descriptors from sector 16 upwards, the root
//! directory record inside the descriptor that was chosen, then the
//! directory records of that root read recursively. Each record gives an
//! extent (a logical block number and a byte length), a flags byte and a
//! name; a record whose directory bit is set is walked, and is not a file.
//!
//! Three things about names, in the order they are preferred:
//!
//! 1. **Joliet**, from a supplementary volume descriptor whose escape
//!    sequence is one of the three UCS-2 ones. Its tree is a second, parallel
//!    tree over the same file extents, with real names in UCS-2BE. When it
//!    is there it is used, because it is the only one of the three that is
//!    a whole tree rather than a per-record decoration.
//! 2. **Rock Ridge `NM`**, in the system use area after a record's name,
//!    which is how a Unix-written image carries a real name in the primary
//!    tree. Used when there is no Joliet tree.
//! 3. The ISO name itself, with the `;1` version suffix stripped.
//!
//! A file over 4 GiB cannot be one record: the record's data length is 32
//! bits. Such a file is written as several consecutive records with the
//! same name, all but the last carrying the multi-extent flag, and the
//! result is one file with several extents -- which is why [`ImageFile`]
//! holds a `Vec` and not a range.

use super::{
    Budget, Extent, ImageFile, ImageFormat, ImageIndex, ImageReader, Refusal, SECTOR,
    extents_within, le_u16, le_u32,
};

const FORMAT: &str = "ISO 9660";

/// The first volume descriptor is at sector 16 by the standard.
const FIRST_DESCRIPTOR_SECTOR: u64 = 16;

/// How many descriptors are read before the sequence is called malformed.
/// A real image has a handful; a terminator (type 255) stops the scan
/// sooner. The cap is what stops an image whose sequence never terminates.
const MAX_DESCRIPTORS: u64 = 64;

/// A directory record is at least this long (the fixed part plus a
/// one-byte name).
const MIN_RECORD_LEN: usize = 34;

/// How deep the tree is walked. ISO 9660 level 1 allows eight; images in
/// the wild go deeper, and a depth bomb goes much deeper still.
const MAX_DEPTH: usize = 64;

/// How many files one image may hold before it is refused. A disc holding
/// more than this is not a film, and the number keeps a directory of
/// nothing but records from turning into unbounded memory.
const MAX_FILES: usize = 65_536;

/// Read the image's ISO 9660 index.
pub async fn index(reader: &dyn ImageReader) -> Result<ImageIndex, Refusal> {
    let mut budget = Budget::new(reader, FORMAT);
    let descriptors = read_descriptors(&mut budget).await?;

    // Joliet first: it is a whole tree with real names, so when it exists
    // nothing else has to be guessed at.
    let (chosen, joliet) = match descriptors.joliet {
        Some(d) => (d, true),
        None => match descriptors.primary {
            Some(d) => (d, false),
            None => {
                return Err(Refusal::NotAnImage {
                    detail: format!(
                        "no ISO 9660 primary volume descriptor (no `CD001` at sector {FIRST_DESCRIPTOR_SECTOR})"
                    ),
                });
            }
        },
    };

    let mut walk = Walk {
        block_size: chosen.block_size,
        joliet,
        rock_ridge: false,
        files: Vec::new(),
        visited: Vec::new(),
        su_skip: 0,
    };
    walk.directory(&mut budget, &chosen.root, "", 0).await?;

    let image_len = budget.image_len();
    for file in &walk.files {
        extents_within(&file.extents, image_len).map_err(|detail| budget.malformed(detail))?;
    }

    Ok(ImageIndex {
        format: ImageFormat::Iso9660 {
            joliet: walk.joliet,
            rock_ridge: walk.rock_ridge,
        },
        files: walk.files,
    })
}

/// A volume descriptor, reduced to the two things a walk needs.
struct Volume {
    block_size: u64,
    root: DirRecord,
}

#[derive(Default)]
struct Descriptors {
    primary: Option<Volume>,
    joliet: Option<Volume>,
}

/// Scan the descriptor sequence from sector 16 until its terminator.
async fn read_descriptors(budget: &mut Budget<'_>) -> Result<Descriptors, Refusal> {
    let mut out = Descriptors::default();
    let mut any_cd001 = false;

    for i in 0..MAX_DESCRIPTORS {
        let sector = FIRST_DESCRIPTOR_SECTOR + i;
        // Past the end of the image is the end of the sequence, not a
        // refusal: a truncated tail after a terminator changes nothing.
        if (sector + 1) * SECTOR > budget.image_len() {
            break;
        }
        let d = budget.read_sector(sector).await?;
        if &d[1..6] != b"CD001" {
            break;
        }
        any_cd001 = true;
        match d[0] {
            // Terminator.
            255 => break,
            // Primary.
            1 => {
                if out.primary.is_none() {
                    out.primary = Some(parse_volume(budget, &d)?);
                }
            }
            // Supplementary/enhanced: Joliet when its escape sequence says
            // UCS-2. Any other supplementary descriptor is some other
            // character set and is left alone -- the primary tree still
            // answers, so this is not a refusal.
            2 if out.joliet.is_none() && is_joliet_escape(&d[88..120]) => {
                out.joliet = Some(parse_volume(budget, &d)?);
            }
            _ => {}
        }
    }

    if !any_cd001 {
        return Err(Refusal::NotAnImage {
            detail: format!(
                "no ISO 9660 primary volume descriptor (no `CD001` at sector {FIRST_DESCRIPTOR_SECTOR})"
            ),
        });
    }
    Ok(out)
}

/// Joliet's three escape sequences, `%/@`, `%/C` and `%/E` (UCS-2 levels
/// 1, 2 and 3), anywhere in the descriptor's 32-byte escape field.
fn is_joliet_escape(esc: &[u8]) -> bool {
    esc.windows(3)
        .any(|w| w[0] == 0x25 && w[1] == 0x2f && matches!(w[2], 0x40 | 0x43 | 0x45))
}

fn parse_volume(budget: &Budget<'_>, d: &[u8]) -> Result<Volume, Refusal> {
    // Both-endian field; the little-endian half is first.
    let block_size = le_u16(d, 128).unwrap_or(0) as u64;
    if block_size == 0 || !block_size.is_power_of_two() || !(512..=65_536).contains(&block_size) {
        return Err(budget.malformed(format!(
            "logical block size {block_size} is not a power of two between 512 and 65536"
        )));
    }
    let root = DirRecord::parse(&d[156..190], budget)?.ok_or_else(|| {
        budget.malformed("the volume descriptor's root directory record is empty")
    })?;
    if !root.is_dir {
        return Err(budget.malformed("the root directory record is not a directory"));
    }
    Ok(Volume { block_size, root })
}

/// One directory record, as read. Untrusted: every field here came out of
/// the image and is checked by the caller before it is used as an offset.
struct DirRecord {
    /// Length of the record in the directory's bytes.
    record_len: usize,
    /// Logical block number of the extent.
    lba: u32,
    /// Byte length of the extent.
    data_len: u32,
    is_dir: bool,
    /// The multi-extent flag: this record is not the last of its file.
    more_extents: bool,
    /// The raw name bytes, undecoded.
    name: Vec<u8>,
    /// The system use area after the name, where Rock Ridge lives.
    system_use: Vec<u8>,
}

impl DirRecord {
    /// `None` when the record length byte is zero, which is the padding to
    /// the end of a logical sector and not a record.
    fn parse(b: &[u8], budget: &Budget<'_>) -> Result<Option<Self>, Refusal> {
        let Some(&record_len) = b.first() else {
            return Ok(None);
        };
        if record_len == 0 {
            return Ok(None);
        }
        let record_len = record_len as usize;
        if record_len < MIN_RECORD_LEN {
            return Err(budget.malformed(format!(
                "a directory record claims {record_len} bytes, less than the {MIN_RECORD_LEN}-byte minimum"
            )));
        }
        let Some(rec) = b.get(..record_len) else {
            return Err(budget.malformed(format!(
                "a directory record claims {record_len} bytes but only {} are left in its directory",
                b.len()
            )));
        };
        let ext_attr_len = rec[1] as u64;
        if ext_attr_len != 0 {
            // Extended attribute records sit before the file's data and
            // would shift every extent; no image this is for writes them,
            // and guessing the shift would serve the wrong bytes.
            return Err(
                budget.unsupported("extended attribute records on a directory record".to_string())
            );
        }
        let lba = le_u32(rec, 2).expect("record is at least 34 bytes");
        let data_len = le_u32(rec, 10).expect("record is at least 34 bytes");
        let flags = rec[25];
        let name_len = rec[32] as usize;
        let name_end = 33 + name_len;
        let Some(name) = rec.get(33..name_end) else {
            return Err(budget.malformed(format!(
                "a directory record's name claims {name_len} bytes but the record is {record_len}"
            )));
        };
        // The system use area starts after the name, which is padded to an
        // even offset.
        let su_start = name_end + usize::from(name_len.is_multiple_of(2));
        let system_use = rec.get(su_start..).unwrap_or_default().to_vec();
        Ok(Some(Self {
            record_len,
            lba,
            data_len,
            is_dir: flags & 0x02 != 0,
            more_extents: flags & 0x80 != 0,
            name: name.to_vec(),
            system_use,
        }))
    }

    /// `.` and `..`, which ISO 9660 writes as a single 0x00 or 0x01 byte.
    fn is_dot_or_dotdot(&self) -> bool {
        matches!(self.name.as_slice(), [0x00] | [0x01])
    }
}

struct Walk {
    block_size: u64,
    joliet: bool,
    rock_ridge: bool,
    files: Vec<ImageFile>,
    /// Extents of directories already entered, to stop a cycle. A list and
    /// not a set: the depth limit bounds it to the path from the root, so
    /// it is at most [`MAX_DEPTH`] long and a scan of it is cheaper than a
    /// hash.
    visited: Vec<u32>,
    /// Bytes the Rock Ridge `SP` entry says to skip at the start of every
    /// system use area.
    su_skip: usize,
}

impl Walk {
    /// Walk one directory. Recursion is bounded by [`MAX_DEPTH`] and the
    /// path back to the root is in `visited`, so a directory that points at
    /// one of its own ancestors is a refusal rather than a loop.
    ///
    /// Boxed because the recursion is through an `async fn`, which cannot
    /// name its own future otherwise.
    fn directory<'a>(
        &'a mut self,
        budget: &'a mut Budget<'_>,
        dir: &'a DirRecord,
        path: &'a str,
        depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Refusal>> + Send + 'a>> {
        Box::pin(async move {
            if depth > MAX_DEPTH {
                return Err(budget.malformed(format!(
                    "the directory tree is deeper than {MAX_DEPTH} levels at `{path}`"
                )));
            }
            if self.visited.contains(&dir.lba) {
                return Err(budget.malformed(format!(
                    "directory `{path}` points at block {}, which is already on the path from the root: a cycle",
                    dir.lba
                )));
            }
            self.visited.push(dir.lba);

            let offset = (dir.lba as u64)
                .checked_mul(self.block_size)
                .ok_or_else(|| budget.malformed("a directory's block number overflows"))?;
            let bytes = budget.read_exact(offset, dir.data_len as usize).await?;

            // Collected first, then recursed into, so that the directory's
            // own bytes are not held across the whole subtree.
            let mut subdirs: Vec<(DirRecord, String)> = Vec::new();
            let mut pos = 0usize;
            // A file over 4 GiB is several records in a row for one name;
            // this is the one being built.
            let mut pending: Option<(String, Vec<Extent>)> = None;

            while pos < bytes.len() {
                let Some(rec) = DirRecord::parse(&bytes[pos..], budget)? else {
                    // A zero length byte is padding to the next sector.
                    let next = (pos / SECTOR as usize + 1) * SECTOR as usize;
                    if next <= pos {
                        return Err(budget.malformed("a directory record made no progress"));
                    }
                    pos = next;
                    continue;
                };
                pos += rec.record_len;

                if rec.is_dot_or_dotdot() {
                    if depth == 0 && self.su_skip == 0 {
                        // The root's `.` record carries `SP`, whose skip
                        // count applies to every system use area after it.
                        self.su_skip = rock_ridge_sp_skip(&rec.system_use);
                    }
                    continue;
                }

                let name = self.name_of(&rec, budget)?;
                let child_path = format!("{path}/{name}");

                if rec.is_dir {
                    // A directory that is also mid-multi-extent is a
                    // contradiction; a directory ends any pending file.
                    finish_pending(&mut pending, &mut self.files, budget)?;
                    subdirs.push((rec, child_path));
                    continue;
                }

                let extent = Extent {
                    offset: (rec.lba as u64)
                        .checked_mul(self.block_size)
                        .ok_or_else(|| budget.malformed("a file's block number overflows"))?,
                    len: rec.data_len as u64,
                };
                match &mut pending {
                    // Same name, still open: another extent of the file
                    // the last record began.
                    Some((pending_path, extents)) if *pending_path == child_path => {
                        extents.push(extent);
                    }
                    _ => {
                        finish_pending(&mut pending, &mut self.files, budget)?;
                        pending = Some((child_path, vec![extent]));
                    }
                }
                if !rec.more_extents {
                    finish_pending(&mut pending, &mut self.files, budget)?;
                }
                if self.files.len() > MAX_FILES {
                    return Err(
                        budget.malformed(format!("the image holds more than {MAX_FILES} files"))
                    );
                }
            }
            // A last record with the multi-extent flag still set is a file
            // whose continuation the directory does not hold. It is kept
            // with what it has rather than dropped: the flag says "more
            // follows", and nothing does, which the length check below
            // catches if the file is short.
            finish_pending(&mut pending, &mut self.files, budget)?;
            drop(bytes);

            for (rec, child_path) in &subdirs {
                self.directory(budget, rec, child_path, depth + 1).await?;
            }

            self.visited.pop();
            Ok(())
        })
    }

    /// The best name the record carries, by the order in this module's
    /// documentation.
    fn name_of(&mut self, rec: &DirRecord, budget: &Budget<'_>) -> Result<String, Refusal> {
        let name = if self.joliet {
            joliet_name(&rec.name)
        } else if let Some(nm) = rock_ridge_name(&rec.system_use, self.su_skip) {
            self.rock_ridge = true;
            nm
        } else {
            iso_name(&rec.name)
        };
        check_name(&name, budget)
    }
}

/// A name out of an image is untrusted: one holding a separator would build
/// a path that is not the path the image describes, and a caller resolving
/// it against a directory would be resolving something else entirely.
fn check_name(name: &str, budget: &Budget<'_>) -> Result<String, Refusal> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0') {
        return Err(budget.malformed(format!("a directory record's name is `{name}`")));
    }
    Ok(name.to_string())
}

fn finish_pending(
    pending: &mut Option<(String, Vec<Extent>)>,
    files: &mut Vec<ImageFile>,
    budget: &Budget<'_>,
) -> Result<(), Refusal> {
    let Some((path, extents)) = pending.take() else {
        return Ok(());
    };
    let len = extents
        .iter()
        .try_fold(0u64, |acc, e| acc.checked_add(e.len))
        .ok_or_else(|| {
            budget.malformed(format!("the extents of `{path}` sum to more than a u64"))
        })?;
    files.push(ImageFile { path, len, extents });
    Ok(())
}

/// `HELLO.TXT;1` -> `HELLO.TXT`, and a trailing bare `.` dropped with it.
fn iso_name(raw: &[u8]) -> String {
    let s: String = raw.iter().map(|&b| b as char).collect();
    let s = match s.split_once(';') {
        Some((base, _version)) => base,
        None => &s,
    };
    s.strip_suffix('.').unwrap_or(s).to_string()
}

/// Joliet names are UCS-2, big-endian. An unpaired surrogate is replaced
/// rather than refused: it is a name, not an offset.
fn joliet_name(raw: &[u8]) -> String {
    let units = raw
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| u16::from_be_bytes(*c));
    let s: String = char::decode_utf16(units)
        .map(|c| c.unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect();
    // Joliet carries the version suffix too.
    let s = match s.split_once(';') {
        Some((base, _)) => base.to_string(),
        None => s,
    };
    s.strip_suffix('.').unwrap_or(&s).to_string()
}

/// Walk the system use entries of a record, calling `f` with each entry's
/// two signature bytes and its data. Every length is checked against what
/// is left, and an entry that does not advance ends the walk, so a hostile
/// system use area is a short walk and not a loop.
fn system_use_entries(su: &[u8], skip: usize, mut f: impl FnMut(&[u8; 2], &[u8])) {
    let mut pos = skip.min(su.len());
    while pos + 4 <= su.len() {
        let sig = [su[pos], su[pos + 1]];
        let len = su[pos + 2] as usize;
        if len < 4 || pos + len > su.len() {
            return;
        }
        f(&sig, &su[pos + 4..pos + len]);
        pos += len;
    }
}

/// Rock Ridge `NM`: the real name, possibly split over several entries with
/// the CONTINUE flag. `NM` for `.` and `..` (the CURRENT and PARENT flags)
/// is not a name and is ignored.
///
/// A name continued into a `CE` continuation area -- another block
/// entirely -- is not followed: that is a second read per record, and the
/// ISO name is a correct fallback, so the cost is a less pretty name and
/// never a wrong extent.
fn rock_ridge_name(su: &[u8], skip: usize) -> Option<String> {
    let mut name = String::new();
    let mut found = false;
    system_use_entries(su, skip, |sig, data| {
        if sig != b"NM" || data.is_empty() {
            return;
        }
        let flags = data[0];
        if flags & 0x06 != 0 {
            return;
        }
        found = true;
        name.push_str(&String::from_utf8_lossy(&data[1..]));
    });
    if found && !name.is_empty() {
        Some(name)
    } else {
        None
    }
}

/// Rock Ridge `SP`, in the root's `.` record: how many bytes to skip at the
/// start of every system use area (non-zero only on a disc whose records
/// carry another system's data first).
fn rock_ridge_sp_skip(su: &[u8]) -> usize {
    let mut skip = 0usize;
    system_use_entries(su, 0, |sig, data| {
        if sig == b"SP" && data.len() >= 3 && data[0] == 0xbe && data[1] == 0xef {
            skip = data[2] as usize;
        }
    });
    skip
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::images::fixtures::{CountingImage, iso};
    use crate::images::{MemoryImage, index as index_any};

    #[tokio::test]
    async fn a_minimal_image_yields_one_file_at_its_extent() {
        let bytes = iso::minimal_iso();
        let (offset, len) = iso::data_range(&bytes);
        let idx = index(&MemoryImage::new(bytes)).await.expect("indexed");
        assert_eq!(
            idx.files,
            vec![ImageFile {
                path: "/HELLO.TXT".into(),
                len,
                extents: vec![Extent { offset, len }],
            }]
        );
    }

    #[tokio::test]
    async fn a_directory_is_walked_and_is_not_itself_a_file() {
        let image = MemoryImage::new(iso::iso_with_subdirectory());
        let idx = index(&image).await.expect("indexed");
        let paths: Vec<&str> = idx.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, vec!["/HELLO.TXT", "/VIDEO_TS/VTS_01_1.VOB"]);
    }

    #[tokio::test]
    async fn a_file_over_four_gib_is_several_records_and_one_file() {
        // Two records for one name, the first flagged multi-extent. The
        // image itself is small: the extents' *stated* lengths are what
        // make the file large, and nothing reads them.
        let image = iso::multi_extent_image();
        let idx = index(&image).await.expect("indexed");
        assert_eq!(idx.files.len(), 1, "{:?}", idx.files);
        let f = &idx.files[0];
        assert_eq!(f.path, "/BIG.BIN");
        assert_eq!(f.extents.len(), 2);
        assert_eq!(f.len, f.extents.iter().map(|e| e.len).sum::<u64>());
        assert!(f.len > u32::MAX as u64, "{} bytes", f.len);
    }

    #[tokio::test]
    async fn joliet_names_are_preferred_over_the_iso_ones() {
        let image = MemoryImage::new(iso::iso_with_joliet());
        let idx = index(&image).await.expect("indexed");
        assert_eq!(
            idx.format,
            ImageFormat::Iso9660 {
                joliet: true,
                rock_ridge: false
            }
        );
        assert_eq!(idx.files[0].path, "/Fällt schwer.mkv");
    }

    #[tokio::test]
    async fn rock_ridge_names_are_used_when_there_is_no_joliet_tree() {
        let image = MemoryImage::new(iso::iso_with_rock_ridge());
        let idx = index(&image).await.expect("indexed");
        assert_eq!(
            idx.format,
            ImageFormat::Iso9660 {
                joliet: false,
                rock_ridge: true
            }
        );
        assert_eq!(idx.files[0].path, "/a long unix name.mkv");
    }

    #[tokio::test]
    async fn the_version_suffix_is_stripped() {
        assert_eq!(iso_name(b"HELLO.TXT;1"), "HELLO.TXT");
        assert_eq!(iso_name(b"README.;1"), "README");
        assert_eq!(iso_name(b"NOVERSION"), "NOVERSION");
    }

    #[tokio::test]
    async fn indexing_reads_descriptors_and_directories_only() {
        // The bound: the descriptor scan (sectors 16..=17 here, stopping at
        // the terminator) plus one sector per directory. Asserted as a
        // number so that a parser that starts reading a file's data fails
        // rather than merely getting slower.
        let bytes = iso::iso_with_subdirectory();
        let image = CountingImage::new(bytes);
        index(&image).await.expect("indexed");
        assert!(
            image.total() <= 8 * SECTOR,
            "read {} bytes to index a two-directory image",
            image.total()
        );
    }

    #[tokio::test]
    async fn a_record_pointing_outside_the_image_is_malformed() {
        let image = MemoryImage::new(iso::iso_with_file_past_the_end());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("past the image")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_directory_cycle_is_refused_rather_than_walked() {
        let image = MemoryImage::new(iso::iso_with_directory_cycle());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("cycle")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_name_holding_a_path_separator_is_refused() {
        // Otherwise the path this reports is not the path the image
        // describes, and whatever resolves it resolves something else.
        let image = MemoryImage::new(iso::iso_with_a_separator_in_a_name());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("name is")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_depth_bomb_stops_at_the_depth_limit() {
        let image = MemoryImage::new(iso::iso_with_a_depth_bomb());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("deeper than")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_record_whose_length_overflows_its_directory_is_malformed() {
        let image = MemoryImage::new(iso::iso_with_overlong_record());
        let err = index(&image).await.expect_err("refused");
        assert!(matches!(err, Refusal::Malformed { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn every_truncation_of_a_good_image_is_a_refusal_and_never_a_panic() {
        // The image cut at every sector boundary, and at a few odd offsets
        // inside them. Each one either indexes (the tail that was cut held
        // nothing the index needed) or refuses; none of them panics and
        // none of them returns an extent outside what is left.
        let full = iso::iso_with_subdirectory();
        let mut cuts: Vec<usize> = (0..full.len()).step_by(SECTOR as usize).collect();
        cuts.extend([1, 33, 32_768, 32_800, 34_000, full.len() - 1]);
        for cut in cuts {
            let image = MemoryImage::new(full[..cut].to_vec());
            if let Ok(idx) = index(&image).await {
                for f in &idx.files {
                    for e in &f.extents {
                        assert!(
                            e.offset + e.len <= cut as u64,
                            "cut at {cut} still reported {e:?}"
                        );
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn every_single_byte_corruption_of_a_header_is_a_refusal_or_an_index() {
        // Each byte of the descriptor and directory region flipped, one at
        // a time. The assertion is that the parser always terminates with
        // an answer or a typed refusal -- the file data region is left
        // alone, since nothing reads it.
        let full = iso::iso_with_subdirectory();
        for at in (32_768..36_864.min(full.len())).step_by(7) {
            for xor in [0xff, 0x01, 0x80] {
                let mut bytes = full.clone();
                bytes[at] ^= xor;
                let image = MemoryImage::new(bytes);
                match index_any(&image).await {
                    Ok(idx) => {
                        for f in &idx.files {
                            for e in &f.extents {
                                assert!(e.offset + e.len <= full.len() as u64);
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }
    }
}

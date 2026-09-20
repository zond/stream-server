//! UDF (ECMA-167 / OSTA UDF), the half of this module that matters for
//! Blu-ray.
//!
//! A DVD-Video image is a bridge disc and can be read through its ISO 9660
//! tree; a Blu-ray image has **no ISO 9660 tree at all** and is UDF 2.50
//! only, so without this there is no BD image support of any kind.
//!
//! The walk, each step reading one or two sectors:
//!
//! 1. The **anchor volume descriptor pointer** at sector 256 (and, as the
//!    standard allows, at the last sector and 256 before it), which gives
//!    the extent of the main volume descriptor sequence.
//! 2. The **volume descriptor sequence**: the partition descriptor (where
//!    the partition starts on the medium and how long it is) and the
//!    logical volume descriptor (the logical block size, the UDF revision
//!    in its domain identifier, the partition maps, and a `long_ad`
//!    pointing at the file set descriptor).
//! 3. The **file set descriptor**, which points at the root directory's
//!    ICB.
//! 4. **File entries** (tag 261) and **extended file entries** (tag 266).
//!    A directory's data is a run of file identifier descriptors (tag 257),
//!    each naming a child and pointing at its ICB. A file's allocation
//!    descriptors are its extents.
//!
//! Every descriptor carries a tag with a checksum over the tag itself and a
//! CRC over the descriptor's body. Both are checked: they are small, they
//! are the format's own, and they are exactly the "header checksum" case
//! `docs/translated-sources.md` §2.2.4 keeps.
//!
//! What is refused rather than guessed at, each naming itself:
//!
//! * **Virtual, sparable and metadata partition maps** (type 2). A metadata
//!   partition -- which UDF 2.50, and therefore some Blu-ray images, uses
//!   -- remaps logical blocks through a metadata file, so every extent this
//!   parser computed would be the wrong range of the image. That is the one
//!   thing worse than refusing.
//! * **Extended allocation descriptors**, **ICB strategies other than 4**,
//!   and **indirect entries**.
//! * **Unrecorded (sparse) extents**: a hole reads as zeros, which are not
//!   bytes of the image and cannot be a range of it.
//! * A **file set descriptor sequence that continues** into another extent.
//!
//! Short, long and inline (embedded) allocation descriptors are supported;
//! inline data is a real byte range of the image -- the bytes sit inside the
//! file entry -- so it is an extent like any other.

use super::{
    Budget, Extent, ImageFile, ImageFormat, ImageIndex, ImageReader, Refusal, SECTOR,
    extents_within, le_u16, le_u32, le_u64, trim_to_len,
};

const FORMAT: &str = "UDF";

/// Where the standard puts the anchor.
const ANCHOR_SECTOR: u64 = 256;

/// Descriptor tag identifiers used here (ECMA-167 3/7.2.1 and 4/7.2.1).
const TAG_PRIMARY_VOLUME: u16 = 1;
const TAG_ANCHOR: u16 = 2;
const TAG_PARTITION: u16 = 5;
const TAG_LOGICAL_VOLUME: u16 = 6;
const TAG_TERMINATING: u16 = 8;
const TAG_FILE_SET: u16 = 256;
const TAG_FILE_IDENTIFIER: u16 = 257;
const TAG_ALLOCATION_EXTENT: u16 = 258;
const TAG_INDIRECT_ENTRY: u16 = 259;
const TAG_FILE_ENTRY: u16 = 261;
const TAG_EXTENDED_FILE_ENTRY: u16 = 266;

/// ICB file types (ECMA-167 4/14.6.6).
const FILE_TYPE_DIRECTORY: u8 = 4;
const FILE_TYPE_REGULAR: u8 = 5;

/// The only ICB strategy this reads. Strategy 4 is one direct entry, which
/// is what every UDF writer produces; 4096 is a chained pair and would need
/// following, and the others are not written at all.
const ICB_STRATEGY_DIRECT: u16 = 4;

/// How many descriptors of the volume descriptor sequence are read before
/// the sequence is called malformed.
const MAX_VDS_DESCRIPTORS: u64 = 64;

/// How many allocation-descriptor continuation blocks one file may chain.
/// A short_ad addresses just under a gibibyte, so a 40 GiB film is forty
/// descriptors and needs none of these at all.
const MAX_AD_CONTINUATIONS: usize = 64;

/// How deep the directory tree is walked, and how many files an image may
/// hold, on the same grounds as the ISO 9660 walk's limits.
const MAX_DEPTH: usize = 64;
const MAX_FILES: usize = 65_536;

/// Read the image's UDF index.
pub async fn index(reader: &dyn ImageReader) -> Result<ImageIndex, Refusal> {
    let budget = Budget::new(reader, FORMAT);
    let mut volume = Volume::open(budget).await?;
    let root = volume.file_set_root().await?;
    let mut files = Vec::new();
    volume
        .walk(&root, "", 0, &mut Vec::new(), &mut files)
        .await?;

    let image_len = volume.budget.image_len();
    for file in &files {
        extents_within(&file.extents, image_len)
            .map_err(|detail| volume.budget.malformed(detail))?;
    }
    Ok(ImageIndex {
        format: ImageFormat::Udf {
            revision: volume.revision,
        },
        files,
    })
}

/// One partition, as the descriptors state it: where it starts on the
/// medium and how many logical blocks long it is.
struct Partition {
    start: u32,
    blocks: u32,
}

/// A `long_ad`: a length, a logical block, and which partition the block is
/// in.
#[derive(Clone, Copy)]
struct LongAd {
    len: u32,
    block: u32,
    partition: u16,
}

impl LongAd {
    fn parse(b: &[u8], at: usize) -> Option<Self> {
        Some(Self {
            len: le_u32(b, at)?,
            block: le_u32(b, at + 4)?,
            partition: le_u16(b, at + 8)?,
        })
    }

    /// The low 30 bits are the byte length; the top two are the extent's
    /// kind (see [`ExtentKind`]).
    fn bytes(&self) -> u64 {
        (self.len & 0x3fff_ffff) as u64
    }

    fn kind(&self) -> ExtentKind {
        ExtentKind::of(self.len)
    }
}

/// What an allocation descriptor's top two length bits say the extent is.
#[derive(PartialEq, Eq, Clone, Copy)]
enum ExtentKind {
    /// Recorded and allocated: real bytes of the image.
    Recorded,
    /// Allocated but not recorded, or neither: a hole that reads as zeros.
    /// Not a range of the image, so a file holding one is refused.
    Unrecorded,
    /// The extent holds the *next* run of allocation descriptors, not data.
    Continuation,
}

impl ExtentKind {
    fn of(raw: u32) -> Self {
        match raw >> 30 {
            0 => Self::Recorded,
            3 => Self::Continuation,
            _ => Self::Unrecorded,
        }
    }
}

struct Volume<'a> {
    budget: Budget<'a>,
    block_size: u64,
    /// Indexed by a `long_ad`'s partition reference number.
    partitions: Vec<Partition>,
    /// The file set descriptor's location, as stated by the logical volume
    /// descriptor.
    file_set: LongAd,
    revision: Option<u16>,
}

impl<'a> Volume<'a> {
    /// Anchor, then volume descriptor sequence, then everything the walk
    /// needs to turn a logical block into an offset in the image.
    async fn open(mut budget: Budget<'a>) -> Result<Volume<'a>, Refusal> {
        let vds = find_anchor(&mut budget).await?;

        let mut partition_descriptors: Vec<(u16, Partition)> = Vec::new();
        let mut logical: Option<LogicalVolume> = None;
        let mut saw_any = false;

        let sectors = (vds.0 as u64).div_ceil(SECTOR).min(MAX_VDS_DESCRIPTORS);
        for i in 0..sectors {
            let sector = vds.1 as u64 + i;
            let d = budget.read_sector(sector).await?;
            let Some(ident) = check_tag(&d, &budget)? else {
                // A zeroed sector ends the sequence; a sequence whose
                // stated length runs past what was written is ordinary.
                break;
            };
            saw_any = true;
            match ident {
                TAG_TERMINATING => break,
                TAG_PARTITION => {
                    let number = le_u16(&d, 22).expect("a sector is 2048 bytes");
                    let start = le_u32(&d, 188).expect("a sector is 2048 bytes");
                    let blocks = le_u32(&d, 192).expect("a sector is 2048 bytes");
                    partition_descriptors.push((number, Partition { start, blocks }));
                }
                TAG_LOGICAL_VOLUME => {
                    if logical.is_none() {
                        logical = Some(parse_logical_volume(&d, &budget)?);
                    }
                }
                TAG_PRIMARY_VOLUME => {}
                _ => {}
            }
        }

        if !saw_any {
            return Err(Refusal::NotAnImage {
                detail: format!(
                    "the UDF anchor at sector {ANCHOR_SECTOR} points at sector {} where there is no volume descriptor",
                    vds.1
                ),
            });
        }
        let Some(logical) = logical else {
            return Err(budget
                .malformed("the volume descriptor sequence holds no logical volume descriptor"));
        };

        // The maps turn a `long_ad`'s partition *reference* number into a
        // partition *number*, which the partition descriptors are keyed by.
        let mut partitions = Vec::new();
        for number in &logical.map_to_partition {
            let Some((_, p)) = partition_descriptors.iter().find(|(n, _)| n == number) else {
                return Err(budget.malformed(format!(
                    "a partition map names partition {number}, which has no partition descriptor"
                )));
            };
            partitions.push(Partition {
                start: p.start,
                blocks: p.blocks,
            });
        }
        if partitions.is_empty() {
            return Err(budget.malformed("the logical volume has no partition map"));
        }

        Ok(Volume {
            budget,
            block_size: logical.block_size,
            partitions,
            file_set: logical.file_set,
            revision: logical.revision,
        })
    }

    /// Where a partition-relative logical block sits in the image.
    fn offset_of(&self, partition: u16, block: u32) -> Result<u64, Refusal> {
        let p = self.partitions.get(partition as usize).ok_or_else(|| {
            self.budget.malformed(format!(
                "a descriptor names partition reference {partition}, and the volume has {}",
                self.partitions.len()
            ))
        })?;
        if block >= p.blocks {
            return Err(self.budget.malformed(format!(
                "a descriptor names block {block} of a partition that is {} blocks long",
                p.blocks
            )));
        }
        (p.start as u64)
            .checked_add(block as u64)
            .and_then(|b| b.checked_mul(self.block_size))
            .ok_or_else(|| {
                self.budget
                    .malformed(format!("block {block} of partition {partition} overflows"))
            })
    }

    /// The file set descriptor, and the root directory's ICB in it.
    async fn file_set_root(&mut self) -> Result<LongAd, Refusal> {
        let offset = self.offset_of(self.file_set.partition, self.file_set.block)?;
        let d = self.budget.read_exact(offset, 512).await?;
        match check_tag(&d, &self.budget)? {
            Some(TAG_FILE_SET) => {}
            other => {
                return Err(self.budget.malformed(format!(
                    "the logical volume points at a descriptor with tag {:?} where the file set descriptor should be",
                    other
                )));
            }
        }
        // A file set sequence that continues elsewhere holds files this
        // walk would not see, so it is named rather than silently halved.
        let next = LongAd::parse(&d, 448).expect("the descriptor is 512 bytes");
        if next.bytes() != 0 {
            return Err(self.budget.unsupported(
                "a file set descriptor sequence that continues into a second extent",
            ));
        }
        Ok(LongAd::parse(&d, 400).expect("the descriptor is 512 bytes"))
    }

    /// Read one ICB's file entry.
    async fn file_entry(&mut self, icb: LongAd) -> Result<FileEntry, Refusal> {
        if icb.kind() != ExtentKind::Recorded || icb.bytes() == 0 {
            return Err(self
                .budget
                .malformed("an ICB points at an extent that holds nothing"));
        }
        let offset = self.offset_of(icb.partition, icb.block)?;
        let d = self
            .budget
            .read_exact(offset, self.block_size as usize)
            .await?;
        let ident = check_tag(&d, &self.budget)?;
        let extended = match ident {
            Some(TAG_FILE_ENTRY) => false,
            Some(TAG_EXTENDED_FILE_ENTRY) => true,
            Some(TAG_INDIRECT_ENTRY) => {
                return Err(self.budget.unsupported("indirect ICB entries"));
            }
            other => {
                return Err(self.budget.malformed(format!(
                    "expected a file entry and found a descriptor with tag {other:?}"
                )));
            }
        };

        // ICB tag: 20 bytes at offset 16.
        let strategy = le_u16(&d, 20).expect("a block is at least 2048 bytes");
        if strategy != ICB_STRATEGY_DIRECT {
            return Err(self
                .budget
                .unsupported(format!("ICB strategy type {strategy}")));
        }
        let file_type = d[27];
        let flags = le_u16(&d, 34).expect("a block is at least 2048 bytes");
        let info_len = le_u64(&d, 56).expect("a block is at least 2048 bytes");

        let (len_ea_at, len_ad_at, fixed) = if extended {
            (208usize, 212usize, 216usize)
        } else {
            (168usize, 172usize, 176usize)
        };
        let len_ea = le_u32(&d, len_ea_at).expect("a block is at least 2048 bytes") as usize;
        let len_ad = le_u32(&d, len_ad_at).expect("a block is at least 2048 bytes") as usize;
        let ad_at = fixed.checked_add(len_ea).ok_or_else(|| {
            self.budget
                .malformed("a file entry's attribute length overflows")
        })?;
        let ad_end = ad_at.checked_add(len_ad).ok_or_else(|| {
            self.budget
                .malformed("a file entry's descriptor length overflows")
        })?;
        if ad_end > d.len() {
            return Err(self.budget.malformed(format!(
                "a file entry's allocation descriptors end at {ad_end}, past its {}-byte block",
                d.len()
            )));
        }

        let extents = match flags & 0x07 {
            // Short allocation descriptors: block numbers in this file
            // entry's own partition.
            0 => {
                self.allocation_descriptors(&d[ad_at..ad_end], 8, icb.partition)
                    .await?
            }
            1 => {
                self.allocation_descriptors(&d[ad_at..ad_end], 16, icb.partition)
                    .await?
            }
            2 => {
                return Err(self.budget.unsupported("extended allocation descriptors"));
            }
            // Inline: the file's bytes are in the file entry itself, which
            // is a real range of the image and needs no descriptor at all.
            3 => vec![Extent {
                offset: offset + ad_at as u64,
                len: len_ad as u64,
            }],
            other => {
                return Err(self
                    .budget
                    .malformed(format!("allocation descriptor type {other}")));
            }
        };

        Ok(FileEntry {
            file_type,
            info_len,
            extents,
        })
    }

    /// Turn a run of allocation descriptors into extents, following a
    /// continuation extent (an allocation extent descriptor) when the
    /// descriptors do not fit in the file entry.
    async fn allocation_descriptors(
        &mut self,
        first: &[u8],
        stride: usize,
        home: u16,
    ) -> Result<Vec<Extent>, Refusal> {
        let mut out = Vec::new();
        let mut run = first.to_vec();
        let mut followed = 0usize;

        loop {
            let mut next: Option<LongAd> = None;
            let mut pos = 0usize;
            while pos + stride <= run.len() {
                let ad = if stride == 8 {
                    LongAd {
                        len: le_u32(&run, pos).expect("checked above"),
                        block: le_u32(&run, pos + 4).expect("checked above"),
                        partition: home,
                    }
                } else {
                    LongAd::parse(&run, pos).expect("checked above")
                };
                pos += stride;
                match ad.kind() {
                    ExtentKind::Recorded => {
                        if ad.bytes() == 0 {
                            continue;
                        }
                        out.push(Extent {
                            offset: self.offset_of(ad.partition, ad.block)?,
                            len: ad.bytes(),
                        });
                    }
                    ExtentKind::Unrecorded => {
                        // A hole reads as zeros, and zeros are not
                        // anywhere in the image. Serving the file would
                        // mean making bytes up.
                        return Err(self.budget.unsupported(
                            "a sparse file (an extent that is allocated but not recorded)",
                        ));
                    }
                    ExtentKind::Continuation => {
                        next = Some(ad);
                        break;
                    }
                }
                if out.len() > MAX_FILES {
                    return Err(self
                        .budget
                        .malformed("a file entry has an implausible number of extents"));
                }
            }

            let Some(next) = next else { break };
            followed += 1;
            if followed > MAX_AD_CONTINUATIONS {
                return Err(self.budget.malformed(format!(
                    "a file's allocation descriptors continue through more than {MAX_AD_CONTINUATIONS} extents"
                )));
            }
            let offset = self.offset_of(next.partition, next.block)?;
            let len = (next.bytes() as usize).min(self.block_size as usize);
            if len < 24 {
                return Err(self
                    .budget
                    .malformed("an allocation extent is too short to hold a descriptor"));
            }
            let d = self.budget.read_exact(offset, len).await?;
            if check_tag(&d, &self.budget)? != Some(TAG_ALLOCATION_EXTENT) {
                return Err(self
                    .budget
                    .malformed("an allocation descriptor continuation has the wrong tag"));
            }
            let l_ad = le_u32(&d, 20).expect("at least 24 bytes") as usize;
            let end = 24usize
                .checked_add(l_ad)
                .filter(|e| *e <= d.len())
                .ok_or_else(|| {
                    self.budget
                        .malformed("an allocation extent's descriptor length runs past its block")
                })?;
            run = d[24..end].to_vec();
        }
        Ok(out)
    }

    /// Walk a directory's file identifier descriptors, recursing into
    /// subdirectories and collecting regular files.
    ///
    /// Boxed for the same reason the ISO 9660 walk is: an `async fn` cannot
    /// name its own future. `path_icbs` is the chain of ICBs from the root,
    /// which is what makes a directory pointing at its own ancestor a
    /// refusal instead of a loop.
    fn walk<'b>(
        &'b mut self,
        icb: &'b LongAd,
        path: &'b str,
        depth: usize,
        path_icbs: &'b mut Vec<(u16, u32)>,
        files: &'b mut Vec<ImageFile>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Refusal>> + Send + 'b>> {
        Box::pin(async move {
            if depth > MAX_DEPTH {
                return Err(self.budget.malformed(format!(
                    "the directory tree is deeper than {MAX_DEPTH} levels at `{path}`"
                )));
            }
            let here = (icb.partition, icb.block);
            if path_icbs.contains(&here) {
                return Err(self.budget.malformed(format!(
                    "directory `{path}` points at block {} of partition {}, which is already on the path from the root: a cycle",
                    icb.block, icb.partition
                )));
            }
            path_icbs.push(here);

            let entry = self.file_entry(*icb).await?;
            if entry.file_type != FILE_TYPE_DIRECTORY {
                return Err(self.budget.malformed(format!(
                    "`{path}` is named as a directory and its file entry says type {}",
                    entry.file_type
                )));
            }
            let bytes = self.read_extents(&entry).await?;

            let mut children: Vec<(LongAd, String)> = Vec::new();
            let mut pos = 0usize;
            while pos + 38 <= bytes.len() {
                let fid = &bytes[pos..];
                match check_tag(fid, &self.budget)? {
                    Some(TAG_FILE_IDENTIFIER) => {}
                    // An all-zero tag is unwritten space at the end of the
                    // directory's last block, not a descriptor.
                    None => break,
                    _ => {
                        return Err(self.budget.malformed(format!(
                            "`{path}` holds something that is not a file identifier"
                        )));
                    }
                }
                let characteristics = fid[18];
                let len_fi = fid[19] as usize;
                let len_iu = le_u16(fid, 36).expect("at least 38 bytes") as usize;
                let total = 38usize
                    .checked_add(len_iu)
                    .and_then(|t| t.checked_add(len_fi))
                    .ok_or_else(|| {
                        self.budget
                            .malformed("a file identifier's length overflows")
                    })?;
                let padded = total.next_multiple_of(4);
                if total > fid.len() {
                    return Err(self.budget.malformed(format!(
                        "a file identifier in `{path}` claims {total} bytes and {} are left",
                        fid.len()
                    )));
                }
                let child_icb = LongAd::parse(fid, 20).expect("at least 38 bytes");
                let name_bytes = &fid[38 + len_iu..total];
                pos += padded.max(1);

                // Bit 3 is the parent entry (`..`), bit 2 a deleted one.
                if characteristics & 0x08 != 0 || characteristics & 0x04 != 0 {
                    continue;
                }
                let Some(name) = cs0_name(name_bytes) else {
                    return Err(self.budget.malformed(format!(
                        "a file identifier in `{path}` has a name this parser cannot decode"
                    )));
                };
                if name.is_empty()
                    || name == "."
                    || name == ".."
                    || name.contains('/')
                    || name.contains('\0')
                {
                    return Err(self
                        .budget
                        .malformed(format!("a file identifier's name is `{name}`")));
                }
                let child_path = format!("{path}/{name}");

                if characteristics & 0x02 != 0 {
                    children.push((child_icb, child_path));
                    continue;
                }

                let child = self.file_entry(child_icb).await?;
                // Anything that is not a directory and not a regular file
                // -- a symbolic link, a device node, a named stream
                // directory -- is not a file whose bytes a player can be
                // handed, and is left out the way a directory is.
                if child.file_type != FILE_TYPE_REGULAR {
                    continue;
                }
                let mut extents = child.extents;
                let covered = trim_to_len(&mut extents, child.info_len);
                if covered < child.info_len {
                    return Err(self.budget.malformed(format!(
                        "`{child_path}` says it is {} bytes and its extents cover {covered}",
                        child.info_len
                    )));
                }
                files.push(ImageFile {
                    path: child_path,
                    len: child.info_len,
                    extents,
                });
                if files.len() > MAX_FILES {
                    return Err(self
                        .budget
                        .malformed(format!("the image holds more than {MAX_FILES} files")));
                }
            }
            drop(bytes);

            for (child_icb, child_path) in &children {
                self.walk(child_icb, child_path, depth + 1, path_icbs, files)
                    .await?;
            }
            path_icbs.pop();
            Ok(())
        })
    }

    /// A directory's own bytes, which are the only file-entry data this
    /// parser ever reads -- and they are an index, not content.
    async fn read_extents(&mut self, entry: &FileEntry) -> Result<Vec<u8>, Refusal> {
        let mut out = Vec::new();
        let mut left = entry.info_len;
        for e in &entry.extents {
            if left == 0 {
                break;
            }
            let want = e.len.min(left);
            let want = usize::try_from(want).map_err(|_| {
                self.budget
                    .malformed("a directory extent is implausibly long")
            })?;
            out.extend_from_slice(&self.budget.read_exact(e.offset, want).await?);
            left -= want as u64;
        }
        if left != 0 {
            return Err(self.budget.malformed(format!(
                "a directory says it is {} bytes and its extents cover {}",
                entry.info_len,
                entry.info_len - left
            )));
        }
        Ok(out)
    }
}

struct FileEntry {
    file_type: u8,
    info_len: u64,
    extents: Vec<Extent>,
}

/// The logical volume descriptor, reduced to what the walk needs.
struct LogicalVolume {
    block_size: u64,
    file_set: LongAd,
    /// Partition number per partition reference number, in map order.
    map_to_partition: Vec<u16>,
    revision: Option<u16>,
}

fn parse_logical_volume(d: &[u8], budget: &Budget<'_>) -> Result<LogicalVolume, Refusal> {
    let block_size = le_u32(d, 212).expect("a sector is 2048 bytes") as u64;
    if block_size == 0 || !block_size.is_power_of_two() || !(512..=65_536).contains(&block_size) {
        return Err(budget.malformed(format!(
            "logical block size {block_size} is not a power of two between 512 and 65536"
        )));
    }
    // The domain identifier's suffix carries the UDF revision as a
    // little-endian BCD-ish u16: 0x0250 is UDF 2.50, the Blu-ray one.
    let domain = &d[216..248];
    let revision = if domain[1..20] == *b"*OSTA UDF Compliant" {
        le_u16(domain, 24)
    } else {
        None
    };
    let file_set = LongAd::parse(d, 248).expect("a sector is 2048 bytes");

    let map_count = le_u32(d, 268).expect("a sector is 2048 bytes") as usize;
    let mut map_to_partition = Vec::new();
    let mut pos = 440usize;
    for _ in 0..map_count {
        let Some(head) = d.get(pos..pos + 2) else {
            return Err(budget.malformed("the partition map table runs past the descriptor"));
        };
        let (kind, len) = (head[0], head[1] as usize);
        if len < 2 || pos + len > d.len() {
            return Err(budget.malformed(format!("a partition map claims {len} bytes")));
        }
        match kind {
            1 => {
                let number = le_u16(d, pos + 4)
                    .ok_or_else(|| budget.malformed("a type 1 partition map is too short"))?;
                map_to_partition.push(number);
            }
            2 => {
                // The identifier says which kind: every one of them remaps
                // blocks through another structure, so an extent computed
                // without it would be the wrong bytes of the image.
                let name = d
                    .get(pos + 4..pos + 36)
                    .map(|regid| {
                        String::from_utf8_lossy(&regid[1..])
                            .trim_end_matches('\0')
                            .trim()
                            .to_string()
                    })
                    .unwrap_or_default();
                return Err(budget.unsupported(format!(
                    "a type 2 partition map (`{name}`), which remaps logical blocks"
                )));
            }
            other => {
                return Err(budget.malformed(format!("partition map type {other}")));
            }
        }
        pos += len;
    }
    Ok(LogicalVolume {
        block_size,
        file_set,
        map_to_partition,
        revision,
    })
}

/// Find the anchor and return the main volume descriptor sequence's extent
/// as `(length in bytes, first sector)`.
///
/// The standard puts an anchor at sector 256, at the last sector, and at
/// 256 before the last; an image cut from a disc may have any of the three.
async fn find_anchor(budget: &mut Budget<'_>) -> Result<(u32, u32), Refusal> {
    let sectors = budget.image_len() / SECTOR;
    let mut candidates = vec![ANCHOR_SECTOR];
    if sectors > 0 {
        candidates.push(sectors - 1);
        if sectors > ANCHOR_SECTOR {
            candidates.push(sectors - 1 - ANCHOR_SECTOR);
        }
    }
    for sector in candidates {
        if (sector + 1) * SECTOR > budget.image_len() {
            continue;
        }
        let d = budget.read_sector(sector).await?;
        if check_tag(&d, budget)? != Some(TAG_ANCHOR) {
            continue;
        }
        let len = le_u32(&d, 16).expect("a sector is 2048 bytes");
        let location = le_u32(&d, 20).expect("a sector is 2048 bytes");
        if len == 0 {
            // The main sequence is empty; the reserve sequence is at 24.
            let len = le_u32(&d, 24).expect("a sector is 2048 bytes");
            let location = le_u32(&d, 28).expect("a sector is 2048 bytes");
            if len == 0 {
                return Err(budget.malformed("the UDF anchor names no volume descriptor sequence"));
            }
            return Ok((len, location));
        }
        return Ok((len, location));
    }
    Err(Refusal::NotAnImage {
        detail: format!("no UDF anchor volume descriptor pointer at sector {ANCHOR_SECTOR}"),
    })
}

/// Verify a descriptor tag and return its identifier.
///
/// `None` means the tag is all zeros, which is an unwritten sector and ends
/// a sequence rather than failing it. A tag whose checksum or CRC is wrong
/// is a refusal: those are the format's own two-hundred-byte integrity
/// checks, they cost nothing, and a descriptor that fails one is a
/// descriptor whose offsets must not be believed.
fn check_tag(d: &[u8], budget: &Budget<'_>) -> Result<Option<u16>, Refusal> {
    let Some(tag) = d.get(..16) else {
        return Err(budget.malformed("a descriptor is shorter than its tag"));
    };
    if tag.iter().all(|&b| b == 0) {
        return Ok(None);
    }
    let ident = le_u16(tag, 0).expect("16 bytes");
    let sum: u32 = tag[..4].iter().map(|&b| b as u32).sum::<u32>()
        + tag[5..16].iter().map(|&b| b as u32).sum::<u32>();
    if (sum & 0xff) as u8 != tag[4] {
        return Err(budget.malformed(format!(
            "a descriptor with tag {ident} fails its own tag checksum"
        )));
    }
    let crc = le_u16(tag, 8).expect("16 bytes");
    let crc_len = le_u16(tag, 10).expect("16 bytes") as usize;
    let Some(body) = d.get(16..16 + crc_len) else {
        return Err(budget.malformed(format!(
            "a descriptor with tag {ident} says its body is {crc_len} bytes and {} are there",
            d.len().saturating_sub(16)
        )));
    };
    if crc_itu_t(body) != crc {
        return Err(budget.malformed(format!("a descriptor with tag {ident} fails its own CRC")));
    }
    Ok(Some(ident))
}

/// CRC-ITU-T (polynomial `x^16 + x^12 + x^5 + 1`, zero seed), which is the
/// CRC every ECMA-167 descriptor tag carries.
pub(crate) fn crc_itu_t(bytes: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in bytes {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// OSTA CS0: a name whose first byte says how the rest is encoded -- 8 for
/// one byte per character (Latin-1), 16 for UCS-2 big-endian. An empty name
/// is the parent entry's and is not decoded here.
fn cs0_name(raw: &[u8]) -> Option<String> {
    match raw.split_first() {
        None => Some(String::new()),
        Some((8, rest)) => Some(rest.iter().map(|&b| b as char).collect()),
        Some((16, rest)) => {
            let units = rest
                .as_chunks::<2>()
                .0
                .iter()
                .map(|c| u16::from_be_bytes(*c));
            Some(
                char::decode_utf16(units)
                    .map(|c| c.unwrap_or(char::REPLACEMENT_CHARACTER))
                    .collect(),
            )
        }
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::images::MemoryImage;
    use crate::images::fixtures::{CountingImage, udf as fx};

    #[tokio::test]
    async fn a_minimal_udf_image_yields_its_one_file() {
        let bytes = fx::minimal_udf();
        let (offset, len) = fx::data_range(&bytes);
        let idx = index(&MemoryImage::new(bytes)).await.expect("indexed");
        assert_eq!(
            idx.format,
            ImageFormat::Udf {
                revision: Some(0x0250)
            }
        );
        assert_eq!(
            idx.files,
            vec![ImageFile {
                path: "/MOVIE.BIN".into(),
                len,
                extents: vec![Extent { offset, len }],
            }]
        );
    }

    #[tokio::test]
    async fn a_directory_is_walked_and_a_file_can_have_several_extents() {
        let image = MemoryImage::new(fx::udf_with_subdirectory());
        let idx = index(&image).await.expect("indexed");
        let paths: Vec<&str> = idx.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, vec!["/BDMV/STREAM/00000.m2ts"]);
        assert_eq!(idx.files[0].extents.len(), 2);
        assert_eq!(
            idx.files[0].len,
            idx.files[0].extents.iter().map(|e| e.len).sum::<u64>()
        );
    }

    #[tokio::test]
    async fn an_extended_file_entry_is_read_like_a_file_entry() {
        let image = MemoryImage::new(fx::udf_with_extended_file_entry());
        let idx = index(&image).await.expect("indexed");
        assert_eq!(idx.files[0].path, "/MOVIE.BIN");
        assert_eq!(idx.files[0].extents.len(), 1);
    }

    #[tokio::test]
    async fn long_allocation_descriptors_are_read() {
        // Asserted as the exact extent: a parser reading these with the
        // short descriptor's eight-byte stride still finds *an* extent,
        // just the wrong one.
        let image = MemoryImage::new(fx::udf_with_long_ads());
        let idx = index(&image).await.expect("indexed");
        assert_eq!(
            idx.files[0].extents,
            vec![Extent {
                offset: fx::Builder::block_offset(fx::FILE_DATA_BLOCK),
                len: fx::FILE_LEN,
            }]
        );
    }

    #[tokio::test]
    async fn a_long_allocation_descriptor_names_its_own_partition() {
        // The partition reference is the one field a short descriptor
        // does not have, and the only way to tell the two strides apart:
        // the first eight bytes of a long descriptor are a short one.
        let image = MemoryImage::new(fx::udf_with_a_long_ad_in_another_partition());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("partition reference 1")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_name_holding_a_path_separator_is_refused() {
        let image = MemoryImage::new(fx::udf_with_a_separator_in_a_name());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("name is")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_depth_bomb_stops_at_the_depth_limit() {
        let image = MemoryImage::new(fx::udf_with_a_depth_bomb());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("deeper than")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn inline_data_is_an_extent_inside_the_file_entry() {
        let bytes = fx::udf_with_inline_file();
        let expected = fx::inline_data_range(&bytes);
        let idx = index(&MemoryImage::new(bytes)).await.expect("indexed");
        assert_eq!(
            idx.files[0].extents,
            vec![Extent {
                offset: expected.0,
                len: expected.1
            }]
        );
    }

    #[tokio::test]
    async fn the_last_extent_is_trimmed_to_the_stated_length() {
        // The allocation descriptor is a whole block; the file entry says
        // the file is shorter. Serving the block's tail would append
        // padding to the film.
        let image = MemoryImage::new(fx::udf_with_padded_last_extent());
        let idx = index(&image).await.expect("indexed");
        assert_eq!(idx.files[0].len, 300);
        assert_eq!(idx.files[0].extents[0].len, 300);
    }

    #[tokio::test]
    async fn a_metadata_partition_map_is_refused_by_name() {
        let image = MemoryImage::new(fx::udf_with_metadata_partition());
        let err = index(&image).await.expect_err("refused");
        let Refusal::Unsupported { what, .. } = &err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(what.contains("Metadata"), "{what}");
    }

    #[tokio::test]
    async fn a_sparse_file_is_refused_rather_than_served_as_zeros() {
        let image = MemoryImage::new(fx::udf_with_sparse_file());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Unsupported { what, .. } if what.contains("sparse")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn an_unsupported_icb_strategy_is_named() {
        let image = MemoryImage::new(fx::udf_with_strategy_4096());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Unsupported { what, .. } if what.contains("4096")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_corrupted_descriptor_fails_its_own_crc() {
        let image = MemoryImage::new(fx::udf_with_corrupted_file_entry());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("CRC")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_directory_cycle_is_refused() {
        let image = MemoryImage::new(fx::udf_with_directory_cycle());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("cycle")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_block_outside_its_partition_is_refused() {
        let image = MemoryImage::new(fx::udf_with_block_past_the_partition());
        let err = index(&image).await.expect_err("refused");
        assert!(
            matches!(&err, Refusal::Malformed { detail, .. } if detail.contains("blocks long")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn indexing_a_udf_image_reads_descriptors_only() {
        let bytes = fx::minimal_udf();
        let (offset, len) = fx::data_range(&bytes);
        let image = CountingImage::new(bytes);
        index(&image).await.expect("indexed");
        assert!(
            !image.read_any_of(offset, len),
            "the index read file data: {:?}",
            image.reads()
        );
        // Anchor, four volume descriptors, the file set descriptor, two
        // file entries and one directory block: nine sectors' worth, and
        // the anchor search may read two more.
        assert!(
            image.total() <= 16 * SECTOR,
            "read {} bytes to index a one-file image",
            image.total()
        );
    }

    #[tokio::test]
    async fn the_crc_matches_the_standard_check_value() {
        // ECMA-167 4/7.2.6 gives this vector for CRC-ITU-T over `123456789`.
        assert_eq!(crc_itu_t(b"123456789"), 0x31c3);
    }

    #[test]
    fn cs0_decodes_both_encodings_and_refuses_the_rest() {
        assert_eq!(cs0_name(b"\x08HELLO").as_deref(), Some("HELLO"));
        assert_eq!(
            cs0_name(&[16, 0x00, 0x41, 0x00, 0xe4]).as_deref(),
            Some("Aä")
        );
        assert_eq!(cs0_name(&[7, 0x41]), None);
    }

    #[tokio::test]
    async fn every_truncation_of_a_good_image_is_a_refusal_and_never_a_panic() {
        let full = fx::udf_with_subdirectory();
        let mut cuts: Vec<usize> = (0..full.len()).step_by(SECTOR as usize / 2).collect();
        cuts.push(full.len() - 1);
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
    async fn every_single_byte_corruption_is_a_refusal_or_an_index() {
        let full = fx::udf_with_subdirectory();
        for at in (0..full.len()).step_by(37) {
            for xor in [0xff, 0x01] {
                let mut bytes = full.clone();
                bytes[at] ^= xor;
                let image = MemoryImage::new(bytes);
                if let Ok(idx) = index(&image).await {
                    for f in &idx.files {
                        for e in &f.extents {
                            assert!(e.offset + e.len <= full.len() as u64);
                        }
                    }
                }
            }
        }
    }
}

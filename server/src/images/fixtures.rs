//! Images built byte by byte for the tests.
//!
//! Nothing binary is committed: every fixture here is assembled from the
//! field layouts in the standards, which is also what makes the hostile
//! cases possible at all -- a cycle, a record that points past the end, a
//! descriptor whose CRC no longer matches are all one line each, and no
//! tool will write them for you.
//!
//! The readers here are the other half of the bargain the module makes.
//! [`CountingImage`] records every range that was read so a test can assert
//! that the file's own bytes were never among them, and [`SparseImage`]
//! reports a length far larger than the bytes it holds, which is how a
//! multi-gibibyte file is tested in a few kilobytes.

use super::{ImageReader, SECTOR};
use async_trait::async_trait;
use std::io;
use std::sync::Mutex;

/// An image that records what was read from it.
pub struct CountingImage {
    bytes: Vec<u8>,
    reads: Mutex<Vec<(u64, u64)>>,
}

impl CountingImage {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            reads: Mutex::new(Vec::new()),
        }
    }

    pub fn reads(&self) -> Vec<(u64, u64)> {
        self.reads.lock().expect("not poisoned").clone()
    }

    pub fn total(&self) -> u64 {
        self.reads().iter().map(|(_, l)| l).sum()
    }

    /// Whether any read overlapped `[offset, offset + len)`. The assertion
    /// that matters: an index that touched the film's bytes is not an
    /// index.
    pub fn read_any_of(&self, offset: u64, len: u64) -> bool {
        self.reads()
            .iter()
            .any(|(o, l)| *o < offset + len && offset < o + l)
    }
}

#[async_trait]
impl ImageReader for CountingImage {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let start = offset as usize;
        if start >= self.bytes.len() {
            return Ok(0);
        }
        let n = buf.len().min(self.bytes.len() - start);
        buf[..n].copy_from_slice(&self.bytes[start..start + n]);
        self.reads
            .lock()
            .expect("not poisoned")
            .push((offset, n as u64));
        Ok(n)
    }
}

/// An image whose stated length is `len` and whose first bytes are `head`;
/// everything after that reads as zeros. A 8 GiB image in 64 KiB.
pub struct SparseImage {
    pub head: Vec<u8>,
    pub len: u64,
}

#[async_trait]
impl ImageReader for SparseImage {
    fn len(&self) -> u64 {
        self.len
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if offset >= self.len {
            return Ok(0);
        }
        let n = buf.len().min((self.len - offset) as usize);
        buf[..n].fill(0);
        let start = offset as usize;
        if start < self.head.len() {
            let from_head = n.min(self.head.len() - start);
            buf[..from_head].copy_from_slice(&self.head[start..start + from_head]);
        }
        Ok(n)
    }
}

/// An image whose reads always fail, for the difference between "this is
/// not an image" and "these bytes are not available".
pub struct FailingImage {
    pub len: u64,
}

#[async_trait]
impl ImageReader for FailingImage {
    fn len(&self) -> u64 {
        self.len
    }

    async fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::TimedOut, "no bytes today"))
    }
}

fn put_u16(b: &mut [u8], at: usize, v: u16) {
    b[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

fn put_u32(b: &mut [u8], at: usize, v: u32) {
    b[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn put_u64(b: &mut [u8], at: usize, v: u64) {
    b[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

/// ISO 9660 images.
pub mod iso {
    use super::*;

    /// Where the file's data goes in the one-file images, and how long it
    /// is. Not a whole sector, so a parser that rounded up would be caught.
    pub const DATA_SECTOR: u64 = 19;
    pub const DATA_LEN: u64 = 1234;

    fn image(sectors: usize) -> Vec<u8> {
        vec![0u8; sectors * SECTOR as usize]
    }

    fn at(image: &mut [u8], sector: u64) -> &mut [u8] {
        let start = (sector * SECTOR) as usize;
        &mut image[start..start + SECTOR as usize]
    }

    /// A both-endian 32-bit field: little first, then big.
    fn both_u32(b: &mut [u8], at: usize, v: u32) {
        b[at..at + 4].copy_from_slice(&v.to_le_bytes());
        b[at + 4..at + 8].copy_from_slice(&v.to_be_bytes());
    }

    fn both_u16(b: &mut [u8], at: usize, v: u16) {
        b[at..at + 2].copy_from_slice(&v.to_le_bytes());
        b[at + 2..at + 4].copy_from_slice(&v.to_be_bytes());
    }

    /// One directory record. `name` is the raw name bytes (`\0` for `.`,
    /// `\x01` for `..`), `system_use` whatever Rock Ridge data follows.
    pub fn record(name: &[u8], lba: u32, len: u32, flags: u8, system_use: &[u8]) -> Vec<u8> {
        let name_len = name.len();
        let su_start = 33 + name_len + usize::from(name_len.is_multiple_of(2));
        let mut total = su_start + system_use.len();
        if total % 2 == 1 {
            total += 1;
        }
        let mut rec = vec![0u8; total];
        rec[0] = u8::try_from(total).expect("a test record fits in a byte");
        both_u32(&mut rec, 2, lba);
        both_u32(&mut rec, 10, len);
        rec[25] = flags;
        both_u16(&mut rec, 28, 1);
        rec[32] = u8::try_from(name_len).expect("a test name fits in a byte");
        rec[33..33 + name_len].copy_from_slice(name);
        rec[su_start..su_start + system_use.len()].copy_from_slice(system_use);
        rec
    }

    /// A volume descriptor: type, `CD001`, version, the logical block size
    /// and a root directory record.
    fn volume_descriptor(kind: u8, root: &[u8], escape: Option<&[u8]>) -> Vec<u8> {
        let mut d = vec![0u8; SECTOR as usize];
        d[0] = kind;
        d[1..6].copy_from_slice(b"CD001");
        d[6] = 1;
        if let Some(escape) = escape {
            d[88..88 + escape.len()].copy_from_slice(escape);
        }
        both_u32(&mut d, 80, 64);
        both_u16(&mut d, 120, 1);
        both_u16(&mut d, 124, 1);
        both_u16(&mut d, 128, SECTOR as u16);
        d[156..156 + root.len()].copy_from_slice(root);
        d
    }

    fn terminator() -> Vec<u8> {
        let mut d = vec![0u8; SECTOR as usize];
        d[0] = 255;
        d[1..6].copy_from_slice(b"CD001");
        d[6] = 1;
        d
    }

    fn directory(records: &[Vec<u8>]) -> Vec<u8> {
        let mut d = vec![0u8; SECTOR as usize];
        let mut pos = 0;
        for r in records {
            d[pos..pos + r.len()].copy_from_slice(r);
            pos += r.len();
        }
        d
    }

    /// The `.` and `..` records every directory starts with.
    fn dots(lba: u32, parent: u32) -> Vec<Vec<u8>> {
        vec![
            record(&[0x00], lba, SECTOR as u32, 0x02, &[]),
            record(&[0x01], parent, SECTOR as u32, 0x02, &[]),
        ]
    }

    /// Primary volume descriptor, terminator, a root directory holding one
    /// file, and that file's data.
    pub fn minimal_iso() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut records = dots(18, 18);
        records.push(record(
            b"HELLO.TXT;1",
            DATA_SECTOR as u32,
            DATA_LEN as u32,
            0,
            &[],
        ));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        let data = (DATA_SECTOR * SECTOR) as usize;
        image[data..data + DATA_LEN as usize].fill(b'A');
        image
    }

    /// Where [`minimal_iso`]'s file data is.
    pub fn data_range(_image: &[u8]) -> (u64, u64) {
        (DATA_SECTOR * SECTOR, DATA_LEN)
    }

    /// A root with one file and one directory, the directory holding one
    /// file of its own.
    pub fn iso_with_subdirectory() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());

        let mut records = dots(18, 18);
        records.push(record(b"HELLO.TXT;1", 19, DATA_LEN as u32, 0, &[]));
        records.push(record(b"VIDEO_TS", 20, SECTOR as u32, 0x02, &[]));
        at(&mut image, 18).copy_from_slice(&directory(&records));

        let mut sub = dots(20, 18);
        sub.push(record(b"VTS_01_1.VOB;1", 21, 4096, 0, &[]));
        at(&mut image, 20).copy_from_slice(&directory(&sub));
        image
    }

    /// One name, two records, the first flagged multi-extent: a file over
    /// 4 GiB. The image reports eight gibibytes and holds twenty-four
    /// sectors, which is exactly the point -- nothing reads the data.
    pub fn multi_extent_image() -> SparseImage {
        let mut head = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut head, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut head, 17).copy_from_slice(&terminator());

        // 0xFFFF_F800 is the largest length that is a whole number of
        // sectors, which is how a writer splits a big file.
        let first_len: u32 = 0xffff_f800;
        let mut records = dots(18, 18);
        records.push(record(b"BIG.BIN;1", 32, first_len, 0x80, &[]));
        records.push(record(
            b"BIG.BIN;1",
            32 + first_len / SECTOR as u32,
            1_000_000,
            0,
            &[],
        ));
        at(&mut head, 18).copy_from_slice(&directory(&records));
        SparseImage {
            head,
            len: 8 * 1024 * 1024 * 1024,
        }
    }

    /// A primary tree and a Joliet supplementary tree over the same data.
    pub fn iso_with_joliet() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 21, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        let joliet_root = record(&[0x00], 20, SECTOR as u32, 0x02, &[]);
        at(&mut image, 17).copy_from_slice(&volume_descriptor(
            2,
            &joliet_root,
            Some(&[0x25, 0x2f, 0x45]),
        ));
        at(&mut image, 18).copy_from_slice(&terminator());

        // The same file's extent, under two names, in two trees.
        let mut primary = dots(21, 21);
        primary.push(record(b"FALLT_SC.MKV;1", 19, DATA_LEN as u32, 0, &[]));
        at(&mut image, 21).copy_from_slice(&directory(&primary));

        let ucs2: Vec<u8> = "Fällt schwer.mkv;1"
            .encode_utf16()
            .flat_map(|u| u.to_be_bytes())
            .collect();
        let mut joliet = dots(20, 20);
        joliet.push(record(&ucs2, 19, DATA_LEN as u32, 0, &[]));
        at(&mut image, 20).copy_from_slice(&directory(&joliet));
        image
    }

    /// A primary tree whose records carry Rock Ridge `NM` names, and whose
    /// `.` record carries the `SP` entry.
    pub fn iso_with_rock_ridge() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());

        let sp = [b'S', b'P', 7, 1, 0xbe, 0xef, 0];
        let mut records = vec![
            record(&[0x00], 18, SECTOR as u32, 0x02, &sp),
            record(&[0x01], 18, SECTOR as u32, 0x02, &[]),
        ];
        let name = b"a long unix name.mkv";
        let mut nm = vec![b'N', b'M', (5 + name.len()) as u8, 1, 0];
        nm.extend_from_slice(name);
        records.push(record(b"A_LONG_U.MKV;1", 19, DATA_LEN as u32, 0, &nm));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        image
    }

    /// A file record whose extent is past the end of the image.
    pub fn iso_with_file_past_the_end() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut records = dots(18, 18);
        records.push(record(b"GONE.BIN;1", 10_000, 4096, 0, &[]));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        image
    }

    /// A subdirectory whose extent is the directory that holds it.
    pub fn iso_with_directory_cycle() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut records = dots(18, 18);
        records.push(record(b"LOOP", 18, SECTOR as u32, 0x02, &[]));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        image
    }

    /// A record whose name holds a path separator: a name that would
    /// build a path the image does not describe.
    pub fn iso_with_a_separator_in_a_name() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut records = dots(18, 18);
        records.push(record(b"../../ETC/PASSWD;1", 19, DATA_LEN as u32, 0, &[]));
        at(&mut image, 18).copy_from_slice(&directory(&records));
        image
    }

    /// A chain of directories deeper than the walk's limit, each one a
    /// sector of its own so that nothing is a cycle.
    pub fn iso_with_a_depth_bomb() -> Vec<u8> {
        let levels = 100u32;
        let mut image = image(18 + levels as usize + 2);
        let root = record(&[0x00], 18, SECTOR as u32, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        for level in 0..levels {
            let here = 18 + level;
            let mut records = dots(here, here.saturating_sub(1).max(18));
            records.push(record(b"D", here + 1, SECTOR as u32, 0x02, &[]));
            at(&mut image, here as u64).copy_from_slice(&directory(&records));
        }
        image
    }

    /// A record claiming more bytes than are left in its directory.
    pub fn iso_with_overlong_record() -> Vec<u8> {
        let mut image = image(24);
        let root = record(&[0x00], 18, 100, 0x02, &[]);
        at(&mut image, 16).copy_from_slice(&volume_descriptor(1, &root, None));
        at(&mut image, 17).copy_from_slice(&terminator());
        let mut rec = record(b"BIG.BIN;1", 19, 4096, 0, &[]);
        rec[0] = 200;
        let dir = at(&mut image, 18);
        dir[..rec.len()].copy_from_slice(&rec);
        image
    }
}

/// UDF images.
pub mod udf {
    use super::*;
    use crate::images::udf::crc_itu_t;

    pub const SECTORS: u64 = 320;
    /// The main volume descriptor sequence.
    pub const VDS_SECTOR: u64 = 32;
    pub const ANCHOR_SECTOR: u64 = 256;
    /// Where the one partition starts, in sectors.
    pub const PARTITION_START: u32 = 64;
    pub const PARTITION_BLOCKS: u32 = 240;

    /// Partition blocks, by what lives in them.
    pub const FSD_BLOCK: u32 = 0;
    pub const ROOT_FE_BLOCK: u32 = 1;
    pub const ROOT_DIR_BLOCK: u32 = 2;
    pub const FILE_FE_BLOCK: u32 = 3;
    pub const FILE_DATA_BLOCK: u32 = 4;

    pub const FILE_LEN: u64 = 1234;

    /// An allocation descriptor as a test writes it.
    pub enum Ad {
        /// `kind` is the top two bits of the length field: 0 recorded,
        /// 1 allocated but not recorded, 3 a continuation.
        Short { kind: u32, len: u32, block: u32 },
        /// `part` is the partition *reference* number, which only a long
        /// descriptor carries -- a short one is always in its file entry's
        /// own partition.
        Long {
            kind: u32,
            len: u32,
            block: u32,
            part: u16,
        },
    }

    pub struct Builder {
        pub image: Vec<u8>,
    }

    impl Default for Builder {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Builder {
        pub fn new() -> Self {
            Self {
                image: vec![0u8; (SECTORS * SECTOR) as usize],
            }
        }

        fn sector(&mut self, sector: u64) -> &mut [u8] {
            let start = (sector * SECTOR) as usize;
            &mut self.image[start..start + SECTOR as usize]
        }

        pub fn block_offset(block: u32) -> u64 {
            (PARTITION_START as u64 + block as u64) * SECTOR
        }

        /// Write a descriptor and seal its tag: the CRC over the body, then
        /// the checksum over the tag.
        fn seal(&mut self, sector: u64, ident: u16, location: u32, body_len: usize) {
            let start = (sector * SECTOR) as usize;
            let d = &mut self.image[start..start + SECTOR as usize];
            put_u16(d, 0, ident);
            put_u16(d, 2, 2);
            put_u16(d, 6, 1);
            let crc = crc_itu_t(&d[16..16 + body_len]);
            put_u16(d, 8, crc);
            put_u16(
                d,
                10,
                u16::try_from(body_len).expect("a test body is short"),
            );
            put_u32(d, 12, location);
            d[4] = 0;
            let sum: u32 = d[..4].iter().map(|&b| b as u32).sum::<u32>()
                + d[5..16].iter().map(|&b| b as u32).sum::<u32>();
            d[4] = (sum & 0xff) as u8;
        }

        /// The volume recognition sequence at sector 16. Not read by the
        /// parser -- the anchor is what it looks for -- but a real image
        /// has it and the bridge fixture needs the sectors accounted for.
        pub fn vrs(&mut self) -> &mut Self {
            for (i, id) in [b"BEA01", b"NSR03", b"TEA01"].iter().enumerate() {
                let d = self.sector(16 + i as u64);
                d[0] = 0;
                d[1..6].copy_from_slice(*id);
                d[6] = 1;
            }
            self
        }

        pub fn anchor(&mut self, vds_sectors: u32) -> &mut Self {
            {
                let d = self.sector(ANCHOR_SECTOR);
                put_u32(d, 16, vds_sectors * SECTOR as u32);
                put_u32(d, 20, VDS_SECTOR as u32);
            }
            self.seal(ANCHOR_SECTOR, 2, ANCHOR_SECTOR as u32, 496);
            self
        }

        pub fn primary_volume(&mut self) -> &mut Self {
            self.seal(VDS_SECTOR, 1, VDS_SECTOR as u32, 496);
            self
        }

        pub fn partition(&mut self, start: u32, blocks: u32) -> &mut Self {
            {
                let d = self.sector(VDS_SECTOR + 1);
                put_u16(d, 22, 0);
                put_u32(d, 188, start);
                put_u32(d, 192, blocks);
            }
            self.seal(VDS_SECTOR + 1, 5, VDS_SECTOR as u32 + 1, 496);
            self
        }

        /// The logical volume descriptor, with a type 1 partition map.
        pub fn logical_volume(&mut self, revision: u16, fsd_block: u32) -> &mut Self {
            {
                let d = self.sector(VDS_SECTOR + 2);
                put_u32(d, 212, SECTOR as u32);
                d[217..217 + 19].copy_from_slice(b"*OSTA UDF Compliant");
                put_u16(d, 216 + 24, revision);
                // LogicalVolumeContentsUse: the file set descriptor's
                // long_ad.
                put_u32(d, 248, SECTOR as u32);
                put_u32(d, 252, fsd_block);
                put_u16(d, 256, 0);
                put_u32(d, 264, 6);
                put_u32(d, 268, 1);
                d[440] = 1;
                d[441] = 6;
                put_u16(d, 442, 1);
                put_u16(d, 444, 0);
            }
            self.seal(VDS_SECTOR + 2, 6, VDS_SECTOR as u32 + 2, 430);
            self
        }

        /// A logical volume descriptor whose one partition map is a type 2
        /// map of the named kind.
        pub fn logical_volume_with_type2_map(&mut self, name: &str) -> &mut Self {
            {
                let d = self.sector(VDS_SECTOR + 2);
                put_u32(d, 212, SECTOR as u32);
                d[217..217 + 19].copy_from_slice(b"*OSTA UDF Compliant");
                put_u16(d, 216 + 24, 0x0250);
                put_u32(d, 248, SECTOR as u32);
                put_u32(d, 252, FSD_BLOCK);
                put_u32(d, 264, 64);
                put_u32(d, 268, 1);
                d[440] = 2;
                d[441] = 64;
                d[445..445 + name.len()].copy_from_slice(name.as_bytes());
            }
            self.seal(VDS_SECTOR + 2, 6, VDS_SECTOR as u32 + 2, 488);
            self
        }

        pub fn terminator(&mut self) -> &mut Self {
            self.seal(VDS_SECTOR + 3, 8, VDS_SECTOR as u32 + 3, 496);
            self
        }

        pub fn file_set(&mut self, root_block: u32) -> &mut Self {
            let sector = PARTITION_START as u64 + FSD_BLOCK as u64;
            {
                let d = self.sector(sector);
                put_u32(d, 400, SECTOR as u32);
                put_u32(d, 404, root_block);
                put_u16(d, 408, 0);
            }
            self.seal(sector, 256, FSD_BLOCK, 496);
            self
        }

        /// A file entry (tag 261) or extended file entry (tag 266).
        #[allow(clippy::too_many_arguments)]
        pub fn file_entry(
            &mut self,
            block: u32,
            extended: bool,
            file_type: u8,
            strategy: u16,
            ad_type: u16,
            info_len: u64,
            ads: &[Ad],
            inline: &[u8],
        ) -> &mut Self {
            let sector = PARTITION_START as u64 + block as u64;
            let (len_ad_at, fixed) = if extended { (212, 216) } else { (172, 176) };
            let mut descriptors = Vec::new();
            for ad in ads {
                match ad {
                    Ad::Short { kind, len, block } => {
                        let mut b = [0u8; 8];
                        put_u32(&mut b, 0, (kind << 30) | len);
                        put_u32(&mut b, 4, *block);
                        descriptors.extend_from_slice(&b);
                    }
                    Ad::Long {
                        kind,
                        len,
                        block,
                        part,
                    } => {
                        let mut b = [0u8; 16];
                        put_u32(&mut b, 0, (kind << 30) | len);
                        put_u32(&mut b, 4, *block);
                        put_u16(&mut b, 8, *part);
                        descriptors.extend_from_slice(&b);
                    }
                }
            }
            if ad_type == 3 {
                descriptors = inline.to_vec();
            }
            {
                let d = self.sector(sector);
                put_u16(d, 20, strategy);
                d[27] = file_type;
                put_u16(d, 34, ad_type);
                put_u64(d, 56, info_len);
                put_u32(d, len_ad_at, descriptors.len() as u32);
                d[fixed..fixed + descriptors.len()].copy_from_slice(&descriptors);
            }
            let ident = if extended { 266 } else { 261 };
            self.seal(sector, ident, block, fixed + descriptors.len() - 16);
            self
        }

        /// A directory's file identifier descriptors: the parent entry and
        /// then `(name, is_dir, icb block)` for each child.
        pub fn directory(
            &mut self,
            block: u32,
            parent: u32,
            children: &[(&str, bool, u32)],
        ) -> u64 {
            let sector = PARTITION_START as u64 + block as u64;
            let mut fids: Vec<Vec<u8>> = vec![fid(&[], 0x0a, parent)];
            for (name, is_dir, icb) in children {
                let mut raw = vec![8u8];
                raw.extend_from_slice(name.as_bytes());
                fids.push(fid(&raw, if *is_dir { 0x02 } else { 0x00 }, *icb));
            }
            let mut pos = 0usize;
            for bytes in &fids {
                let start = (sector * SECTOR) as usize + pos;
                self.image[start..start + bytes.len()].copy_from_slice(bytes);
                // Each descriptor seals its own tag in place.
                let crc_len = bytes.len() - 16;
                let d = &mut self.image[start..start + bytes.len()];
                put_u16(d, 0, 257);
                put_u16(d, 2, 2);
                put_u16(d, 6, 1);
                let crc = crc_itu_t(&d[16..16 + crc_len]);
                put_u16(d, 8, crc);
                put_u16(d, 10, crc_len as u16);
                put_u32(d, 12, block);
                d[4] = 0;
                let sum: u32 = d[..4].iter().map(|&b| b as u32).sum::<u32>()
                    + d[5..16].iter().map(|&b| b as u32).sum::<u32>();
                d[4] = (sum & 0xff) as u8;
                pos += bytes.len();
            }
            pos as u64
        }

        pub fn finish(self) -> Vec<u8> {
            self.image
        }
    }

    /// One file identifier descriptor, tag left unsealed, padded to four
    /// bytes as the standard requires.
    fn fid(name: &[u8], characteristics: u8, icb_block: u32) -> Vec<u8> {
        let total = 38 + name.len();
        let padded = total.next_multiple_of(4);
        let mut b = vec![0u8; padded];
        put_u16(&mut b, 16, 1);
        b[18] = characteristics;
        b[19] = u8::try_from(name.len()).expect("a test name fits in a byte");
        put_u32(&mut b, 20, SECTOR as u32);
        put_u32(&mut b, 24, icb_block);
        put_u16(&mut b, 28, 0);
        put_u16(&mut b, 36, 0);
        b[38..38 + name.len()].copy_from_slice(name);
        b
    }

    /// The volume descriptors every fixture here shares.
    fn volume() -> Builder {
        let mut b = Builder::new();
        b.vrs()
            .anchor(4)
            .primary_volume()
            .partition(PARTITION_START, PARTITION_BLOCKS)
            .logical_volume(0x0250, FSD_BLOCK)
            .terminator()
            .file_set(ROOT_FE_BLOCK);
        b
    }

    /// Descriptors, a root directory with one file, and the file's data.
    pub fn minimal_udf() -> Vec<u8> {
        let mut b = volume();
        let dir_len = b.directory(
            ROOT_DIR_BLOCK,
            ROOT_FE_BLOCK,
            &[("MOVIE.BIN", false, FILE_FE_BLOCK)],
        );
        b.file_entry(
            ROOT_FE_BLOCK,
            false,
            4,
            4,
            0,
            dir_len,
            &[Ad::Short {
                kind: 0,
                len: SECTOR as u32,
                block: ROOT_DIR_BLOCK,
            }],
            &[],
        );
        b.file_entry(
            FILE_FE_BLOCK,
            false,
            5,
            4,
            0,
            FILE_LEN,
            &[Ad::Short {
                kind: 0,
                len: FILE_LEN as u32,
                block: FILE_DATA_BLOCK,
            }],
            &[],
        );
        let mut image = b.finish();
        let data = Builder::block_offset(FILE_DATA_BLOCK) as usize;
        image[data..data + FILE_LEN as usize].fill(b'M');
        image
    }

    /// Where [`minimal_udf`]'s file data is.
    pub fn data_range(_image: &[u8]) -> (u64, u64) {
        (Builder::block_offset(FILE_DATA_BLOCK), FILE_LEN)
    }

    /// `/BDMV/STREAM/00000.m2ts`, in two extents, the Blu-ray shape.
    pub fn udf_with_subdirectory() -> Vec<u8> {
        let mut b = volume();
        let root_len = b.directory(ROOT_DIR_BLOCK, ROOT_FE_BLOCK, &[("BDMV", true, 5)]);
        b.file_entry(
            ROOT_FE_BLOCK,
            false,
            4,
            4,
            0,
            root_len,
            &[Ad::Short {
                kind: 0,
                len: SECTOR as u32,
                block: ROOT_DIR_BLOCK,
            }],
            &[],
        );
        let bdmv_len = b.directory(6, ROOT_FE_BLOCK, &[("STREAM", true, 7)]);
        b.file_entry(
            5,
            false,
            4,
            4,
            0,
            bdmv_len,
            &[Ad::Short {
                kind: 0,
                len: SECTOR as u32,
                block: 6,
            }],
            &[],
        );
        let stream_len = b.directory(8, 5, &[("00000.m2ts", false, 9)]);
        b.file_entry(
            7,
            false,
            4,
            4,
            0,
            stream_len,
            &[Ad::Short {
                kind: 0,
                len: SECTOR as u32,
                block: 8,
            }],
            &[],
        );
        b.file_entry(
            9,
            false,
            5,
            4,
            0,
            SECTOR + 1000,
            &[
                Ad::Short {
                    kind: 0,
                    len: SECTOR as u32,
                    block: 10,
                },
                Ad::Short {
                    kind: 0,
                    len: 1000,
                    block: 11,
                },
            ],
            &[],
        );
        b.finish()
    }

    /// The same one-file image with the tag 266 file entry, whose fields
    /// sit at different offsets.
    pub fn udf_with_extended_file_entry() -> Vec<u8> {
        one_file_image(|b| {
            b.file_entry(
                FILE_FE_BLOCK,
                true,
                5,
                4,
                0,
                FILE_LEN,
                &[Ad::Short {
                    kind: 0,
                    len: FILE_LEN as u32,
                    block: FILE_DATA_BLOCK,
                }],
                &[],
            );
        })
    }

    pub fn udf_with_long_ads() -> Vec<u8> {
        one_file_image(|b| {
            b.file_entry(
                FILE_FE_BLOCK,
                false,
                5,
                4,
                1,
                FILE_LEN,
                &[Ad::Long {
                    kind: 0,
                    len: FILE_LEN as u32,
                    block: FILE_DATA_BLOCK,
                    part: 0,
                }],
                &[],
            );
        })
    }

    /// Inline (embedded) data: the file's bytes are inside its file entry.
    pub fn udf_with_inline_file() -> Vec<u8> {
        one_file_image(|b| {
            b.file_entry(
                FILE_FE_BLOCK,
                false,
                5,
                4,
                3,
                20,
                &[],
                b"twenty bytes inline!",
            );
        })
    }

    /// Where [`udf_with_inline_file`]'s data is: inside the file entry,
    /// after its fixed part.
    pub fn inline_data_range(_image: &[u8]) -> (u64, u64) {
        (Builder::block_offset(FILE_FE_BLOCK) + 176, 20)
    }

    /// A whole block allocated for a 300-byte file.
    pub fn udf_with_padded_last_extent() -> Vec<u8> {
        one_file_image(|b| {
            b.file_entry(
                FILE_FE_BLOCK,
                false,
                5,
                4,
                0,
                300,
                &[Ad::Short {
                    kind: 0,
                    len: SECTOR as u32,
                    block: FILE_DATA_BLOCK,
                }],
                &[],
            );
        })
    }

    /// An extent that is allocated but not recorded: a hole.
    pub fn udf_with_sparse_file() -> Vec<u8> {
        one_file_image(|b| {
            b.file_entry(
                FILE_FE_BLOCK,
                false,
                5,
                4,
                0,
                SECTOR * 2,
                &[
                    Ad::Short {
                        kind: 0,
                        len: SECTOR as u32,
                        block: FILE_DATA_BLOCK,
                    },
                    Ad::Short {
                        kind: 1,
                        len: SECTOR as u32,
                        block: 0,
                    },
                ],
                &[],
            );
        })
    }

    pub fn udf_with_strategy_4096() -> Vec<u8> {
        one_file_image(|b| {
            b.file_entry(
                FILE_FE_BLOCK,
                false,
                5,
                4096,
                0,
                FILE_LEN,
                &[Ad::Short {
                    kind: 0,
                    len: FILE_LEN as u32,
                    block: FILE_DATA_BLOCK,
                }],
                &[],
            );
        })
    }

    /// A file whose extent is outside the partition.
    pub fn udf_with_block_past_the_partition() -> Vec<u8> {
        one_file_image(|b| {
            b.file_entry(
                FILE_FE_BLOCK,
                false,
                5,
                4,
                0,
                FILE_LEN,
                &[Ad::Short {
                    kind: 0,
                    len: FILE_LEN as u32,
                    block: PARTITION_BLOCKS + 1000,
                }],
                &[],
            );
        })
    }

    /// A good image with one byte of a file entry flipped after its tag
    /// was sealed.
    pub fn udf_with_corrupted_file_entry() -> Vec<u8> {
        let mut image = minimal_udf();
        let at = Builder::block_offset(FILE_FE_BLOCK) as usize + 56;
        image[at] ^= 0xff;
        image
    }

    /// A subdirectory whose ICB is the root's own.
    pub fn udf_with_directory_cycle() -> Vec<u8> {
        let mut b = volume();
        let dir_len = b.directory(
            ROOT_DIR_BLOCK,
            ROOT_FE_BLOCK,
            &[("LOOP", true, ROOT_FE_BLOCK)],
        );
        b.file_entry(
            ROOT_FE_BLOCK,
            false,
            4,
            4,
            0,
            dir_len,
            &[Ad::Short {
                kind: 0,
                len: SECTOR as u32,
                block: ROOT_DIR_BLOCK,
            }],
            &[],
        );
        b.finish()
    }

    /// A chain of directories deeper than the walk's limit, two blocks
    /// per level and no block used twice, so it is a depth bomb and not a
    /// cycle.
    pub fn udf_with_a_depth_bomb() -> Vec<u8> {
        let mut b = volume();
        let levels = 90u32;
        let mut fe = ROOT_FE_BLOCK;
        let mut dir = ROOT_DIR_BLOCK;
        for level in 0..levels {
            let next_fe = 5 + level * 2;
            let next_dir = next_fe + 1;
            let dir_len = b.directory(dir, fe, &[("D", true, next_fe)]);
            b.file_entry(
                fe,
                false,
                4,
                4,
                0,
                dir_len,
                &[Ad::Short {
                    kind: 0,
                    len: SECTOR as u32,
                    block: dir,
                }],
                &[],
            );
            fe = next_fe;
            dir = next_dir;
        }
        b.finish()
    }

    /// A long allocation descriptor naming a partition the volume does
    /// not have. Only a parser that reads the descriptor's own sixteen
    /// bytes sees the partition field at all.
    pub fn udf_with_a_long_ad_in_another_partition() -> Vec<u8> {
        one_file_image(|b| {
            b.file_entry(
                FILE_FE_BLOCK,
                false,
                5,
                4,
                1,
                FILE_LEN,
                &[Ad::Long {
                    kind: 0,
                    len: FILE_LEN as u32,
                    block: FILE_DATA_BLOCK,
                    part: 1,
                }],
                &[],
            );
        })
    }

    /// A file identifier whose name holds a path separator.
    pub fn udf_with_a_separator_in_a_name() -> Vec<u8> {
        let mut b = volume();
        let dir_len = b.directory(
            ROOT_DIR_BLOCK,
            ROOT_FE_BLOCK,
            &[("../../etc/passwd", false, FILE_FE_BLOCK)],
        );
        b.file_entry(
            ROOT_FE_BLOCK,
            false,
            4,
            4,
            0,
            dir_len,
            &[Ad::Short {
                kind: 0,
                len: SECTOR as u32,
                block: ROOT_DIR_BLOCK,
            }],
            &[],
        );
        b.finish()
    }

    /// A logical volume whose one partition map remaps blocks.
    pub fn udf_with_metadata_partition() -> Vec<u8> {
        let mut b = Builder::new();
        b.vrs()
            .anchor(4)
            .primary_volume()
            .partition(PARTITION_START, PARTITION_BLOCKS)
            .logical_volume_with_type2_map("*UDF Metadata Partition")
            .terminator();
        b.finish()
    }

    /// An image that is both ISO 9660 and UDF, which is what a DVD-Video
    /// disc is.
    pub fn bridge_image() -> Vec<u8> {
        let mut image = minimal_udf();
        let iso = super::iso::minimal_iso();
        // The 9660 descriptors and tree sit where the volume recognition
        // sequence was; a real bridge disc shares the area the same way.
        image[(16 * SECTOR) as usize..iso.len()].copy_from_slice(&iso[(16 * SECTOR) as usize..]);
        image
    }

    /// The volume descriptors, a root directory of one file, and the file
    /// entry the caller writes.
    fn one_file_image(file: impl FnOnce(&mut Builder)) -> Vec<u8> {
        let mut b = volume();
        let dir_len = b.directory(
            ROOT_DIR_BLOCK,
            ROOT_FE_BLOCK,
            &[("MOVIE.BIN", false, FILE_FE_BLOCK)],
        );
        b.file_entry(
            ROOT_FE_BLOCK,
            false,
            4,
            4,
            0,
            dir_len,
            &[Ad::Short {
                kind: 0,
                len: SECTOR as u32,
                block: ROOT_DIR_BLOCK,
            }],
            &[],
        );
        file(&mut b);
        b.finish()
    }
}

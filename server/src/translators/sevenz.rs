//! 7z, read as an index and never as a decoder.
//!
//! The walk is the crate's own: [`Archive::read`] over the blocking shim
//! ([`IndexReader`]), which costs the 32-byte signature header at the
//! front and the packed header at the back -- 7z puts its index at the
//! end, and the signature header says where. Nothing in between is read,
//! which is the whole claim, and the tests assert it by range.
//!
//! **In practice a 7z of a film is LZMA2 and is refused.** 7-Zip
//! compresses by default, and a release that wants its film served by
//! range ships it in a RAR, so the honest answer here is usually a
//! sentence the player shows. The translator is kept because it is cheap,
//! because the refusal is the *right* refusal (which method, on which
//! member, and told apart from a solid block and from encryption), and
//! because a store-method 7z -- `7z a -mx0`, which is what someone who
//! wanted a container rather than compression writes -- is served by
//! range like any other stored member.
//!
//! ## Where a stored member's bytes are
//!
//! A block's packed data begins at
//!
//! ```text
//! SIGNATURE_HEADER_SIZE + pack_pos + pack_stream_offsets[block_first_pack_stream_index[block]]
//! ```
//!
//! which is the crate's own arithmetic -- `ArchiveReader::
//! build_decode_stack` seeks exactly there before handing a block to a
//! decoder, and `Archive::pack_pos` documents itself as being for
//! "calculating byte offsets when streaming uncompressed (COPY) content".
//!
//! When the block's single coder is COPY its output *is* those packed
//! bytes, so the files in it lie end to end in that order, and the one
//! asked for starts after the ones before it in the same block. That sum
//! is the only part of this not read straight out of a header, and it is
//! what [`members_of`] carries along as it walks.
//!
//! Everything else -- LZMA, LZMA2, PPMd, a filter chain, AES -- is a
//! [`Refusal`], and which one is decided in [`body_of`].

use super::{
    Body, Budget, Index, IndexReader, Member, Refusal, Translator, direct_extent, le_u32, le_u64,
};
use crate::sources::ByteSource;
use async_trait::async_trait;
use sevenz_rust2::{Archive, Block, EncoderMethod, Error as SevenZError, Password};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// The 7z signature header: the magic, the format version, that header's
/// own CRC, and where the index at the end of the file is.
const SIGNATURE_HEADER_BYTES: usize = 32;
const SIGNATURE: &[u8] = &[b'7', b'z', 0xBC, 0xAF, 0x27, 0x1C];

/// What the format is called in the sentences a refusal is made of.
const FORMAT: &str = "7z";

pub struct SevenZ;

#[async_trait]
impl Translator for SevenZ {
    fn format(&self) -> &'static str {
        FORMAT
    }

    async fn index(&self, sources: &[Arc<dyn ByteSource>]) -> Result<Index, Refusal> {
        let source = match sources {
            [] => return Err(Refusal::Malformed(format!("no {FORMAT} to read"))),
            [source] => source.clone(),
            // `.7z.001`, `.7z.002`, ... is one file cut into pieces rather
            // than a set of archives: every piece but the first is a
            // headless slab of bytes, and there is nothing to index until
            // they are joined back into one. The crate does not join them
            // and neither does this -- joining them is fetching them all,
            // which is the thing this design exists to stop.
            several => {
                return Err(Refusal::Malformed(format!(
                    "a 7z is one file, and {} were given: a multi-part set (.7z.001, .7z.002, \
                     ...) is one file cut up, and has to be joined before any of it can be read",
                    several.len()
                )));
            }
        };
        // The signature header, read here rather than left to the crate:
        // see `check_signature_header` for the thing it is checked for
        // that the crate would otherwise answer by reading a mebibyte one
        // byte at a time.
        let mut budget = Budget::new(source.as_ref(), FORMAT);
        check_signature_header(&mut budget).await?;
        let archive = read_archive(source.clone()).await?;
        Ok(Index {
            members: members_of(&archive, source.as_ref()),
        })
    }
}

/// The 32 bytes at the front, checked before the crate is handed the file.
///
/// Most of this the crate would catch by itself. One thing it would not:
/// when the start header's CRC field is zero **and** the twenty bytes
/// after it are zero, `Archive::read` falls back to guessing where the end
/// header is, by reading one byte at a time backwards over the last
/// mebibyte of the file. Over a piece store or a proxied entity that is a
/// million round trips for a file that is broken anyway, so the shape is
/// refused here instead.
///
/// And one thing it would report unhelpfully: the first part of a
/// **multi-part** set carries the signature but keeps its index in the
/// last part, so the offset it states is past its own end. That is worth
/// a sentence of its own, because it is the one refusal here a viewer can
/// act on.
async fn check_signature_header(budget: &mut Budget<'_>) -> Result<(), Refusal> {
    let total = budget.source_len();
    if total < SIGNATURE_HEADER_BYTES as u64 {
        return Err(budget.malformed(format!("a {total}-byte file is too short to be a 7z")));
    }
    let head = budget.read_exact(0, SIGNATURE_HEADER_BYTES).await?;
    if &head[..SIGNATURE.len()] != SIGNATURE {
        return Err(budget.malformed("this file does not begin with a 7z signature"));
    }
    if head[6] != 0 {
        return Err(budget.malformed(format!(
            "this 7z states format version {}.{}, which this server does not read",
            head[6], head[7]
        )));
    }
    let start_header_crc = le_u32(&head, 8).expect("thirty-two bytes were read");
    if start_header_crc == 0 && head[12..].iter().all(|byte| *byte == 0) {
        return Err(budget.malformed(
            "this 7z's start header is blank, so nothing in it says where its index is",
        ));
    }
    let offset = le_u64(&head, 12).expect("thirty-two bytes were read");
    let size = le_u64(&head, 20).expect("thirty-two bytes were read");
    let end = (SIGNATURE_HEADER_BYTES as u64)
        .checked_add(offset)
        .and_then(|at| at.checked_add(size));
    if end.is_none_or(|end| end > total) {
        return Err(budget.malformed(
            "this 7z keeps its index past its own end, so it is the first part of a multi-part \
             set (.7z.001, .7z.002, ...) or a truncated file; either way it is not one archive \
             this server can read",
        ));
    }
    Ok(())
}

/// The archive's own index, parsed on the blocking pool through the shim.
async fn read_archive(source: Arc<dyn ByteSource>) -> Result<Archive, Refusal> {
    let described = source.describe();
    let mut reader = IndexReader::new(source);
    let tally = reader.tally();
    let parsed =
        tokio::task::spawn_blocking(move || Archive::read(&mut reader, &Password::empty()))
            .await
            .map_err(|error| {
                Refusal::Malformed(format!("{FORMAT}: reading {described}: {error}"))
            })?;
    tracing::debug!(
        archive = %described,
        bytes = tally.load(Ordering::Relaxed),
        "read a 7z index"
    );
    parsed.map_err(|error| match error {
        // The index itself is AES: the crate asks for a password at the
        // first coder, before a file is named. There is no password UX,
        // so this is where it stops (`docs/translated-sources.md` §6).
        SevenZError::PasswordRequired | SevenZError::MaybeBadPassword(_) => Refusal::Encrypted,
        SevenZError::Io(io, _) if IndexReader::is_over_budget(&io) => {
            Refusal::Malformed(format!("{FORMAT}: {described}: {io}"))
        }
        SevenZError::Io(io, _) => {
            Refusal::Malformed(format!("{FORMAT}: {described} could not be read: {io}"))
        }
        SevenZError::BadSignature(_) | SevenZError::UnsupportedVersion { .. } => {
            Refusal::Malformed(format!("{FORMAT}: {described} is not a 7z"))
        }
        // An index packed with a method this build has no decoder for.
        // The archive is well formed and this server cannot open it,
        // which is what `Unsupported` is for.
        SevenZError::UnsupportedCompressionMethod(method) => Refusal::Unsupported {
            format: FORMAT,
            what: format!("an index packed with {method}"),
        },
        other => Refusal::Malformed(format!("{FORMAT}: {described}: {other}")),
    })
}

/// Every file in the archive that has bytes of its own, as a [`Member`].
///
/// One pass over [`Archive::files`], carrying two running figures per
/// block: how many of its files have been seen (which is what says
/// whether a block holds several, and so whether it is solid) and how
/// many of their bytes have gone before (which is where the next one
/// starts). The file list is in block order and the crate builds the
/// stream map from the same walk, so a file's place in its block is its
/// place in this walk.
fn members_of(archive: &Archive, source: &dyn ByteSource) -> Vec<Member> {
    // Where each block's packed bytes begin, worked out once.
    let offsets: Vec<Option<u64>> = (0..archive.blocks.len())
        .map(|block| block_offset(archive, block))
        .collect();
    // How many files each block holds, counted before the walk because a
    // block's first file has to know about its last.
    let mut files_in_block = vec![0usize; archive.blocks.len()];
    for (at, file) in archive.files.iter().enumerate() {
        if let Some(block) = block_of(archive, at, file.has_stream)
            && let Some(count) = files_in_block.get_mut(block)
        {
            *count += 1;
        }
    }
    let mut within = vec![0u64; archive.blocks.len()];
    let mut members = Vec::new();
    for (at, file) in archive.files.iter().enumerate() {
        // A directory has no bytes, and an anti-item is a note that a
        // file was deleted -- neither is something a player can ask for.
        if file.is_directory || file.is_anti_item {
            continue;
        }
        let name = member_name(&file.name);
        if !file.has_stream {
            // An empty file: no block, no bytes, and `MemberView` makes a
            // zero-length body of it.
            members.push(Member {
                name,
                len: 0,
                body: Body::Direct(Vec::new()),
            });
            continue;
        }
        let body = match block_of(archive, at, true) {
            None => Body::Opaque(Refusal::Malformed(format!(
                "{FORMAT}: {name} has bytes and is in no block"
            ))),
            Some(block) => {
                let offset_in_block = within.get(block).copied().unwrap_or(0);
                if let Some(running) = within.get_mut(block) {
                    *running = running.saturating_add(file.size);
                }
                body_of(
                    archive,
                    block,
                    offsets.get(block).copied().flatten(),
                    files_in_block.get(block).copied().unwrap_or(0),
                    offset_in_block,
                    file.size,
                    &name,
                    source,
                )
            }
        };
        members.push(Member {
            name,
            len: file.size,
            body,
        });
    }
    members
}

/// The block file `at` is in, for a file that has one.
fn block_of(archive: &Archive, at: usize, has_stream: bool) -> Option<usize> {
    has_stream
        .then(|| {
            archive
                .stream_map
                .file_block_index
                .get(at)
                .copied()
                .flatten()
        })
        .flatten()
}

/// Where `block`'s packed bytes begin in the file, or `None` when the
/// numbers the header states do not add up inside a `u64`.
///
/// **The arithmetic this module exists for**, and the crate's own: see
/// `ArchiveReader::build_decode_stack`, which seeks exactly here before
/// handing a block to a decoder.
fn block_offset(archive: &Archive, block: usize) -> Option<u64> {
    let first = *archive
        .stream_map
        .block_first_pack_stream_index()
        .get(block)?;
    let offset = *archive.stream_map.pack_stream_offsets().get(first)?;
    sevenz_rust2::SIGNATURE_HEADER_SIZE
        .checked_add(archive.pack_pos())?
        .checked_add(offset)
}

/// Where a member's bytes are, or why they cannot be pointed at.
///
/// The three refusals are tried in the order of how much of the archive
/// each is about: encryption is the block's whatever else it does,
/// solidity says the file cannot be *entered* even by a decoder that
/// started at the block, and the method is the last and narrowest thing
/// left to say.
#[allow(clippy::too_many_arguments)]
fn body_of(
    archive: &Archive,
    block_at: usize,
    offset: Option<u64>,
    files_in_block: usize,
    offset_in_block: u64,
    len: u64,
    name: &str,
    source: &dyn ByteSource,
) -> Body {
    let Some(block) = archive.blocks.get(block_at) else {
        return Body::Opaque(Refusal::Malformed(format!(
            "{FORMAT}: {name} names block {block_at} of {}",
            archive.blocks.len()
        )));
    };
    if block
        .coders
        .iter()
        .any(|coder| coder.encoder_method_id() == EncoderMethod::ID_AES256_SHA256)
    {
        return Body::Opaque(Refusal::Encrypted);
    }
    let stored =
        matches!(&block.coders[..], [only] if only.encoder_method_id() == EncoderMethod::ID_COPY);
    if !stored {
        // A block holding several files cannot be entered at one of them
        // whatever its method: reaching the second file means decoding the
        // first. That is what 7-Zip calls solid, and it is the more useful
        // half of the truth -- "compressed" would be true too, and would
        // not say that even a decoder could not start here.
        if files_in_block > 1 {
            return Body::Opaque(Refusal::Solid);
        }
        return Body::Opaque(Refusal::Compressed {
            format: FORMAT,
            method: method_names(block),
        });
    }
    // A COPY block's packed bytes *are* its unpacked bytes. A block whose
    // two sizes differ is a header contradicting itself, and this is the
    // one arithmetic check the offset below rests on.
    let packed = archive
        .stream_map
        .block_first_pack_stream_index()
        .get(block_at)
        .and_then(|first| archive.pack_sizes().get(*first))
        .copied();
    if packed != Some(block.get_unpack_size()) {
        return Body::Opaque(Refusal::Malformed(format!(
            "{FORMAT}: {name} is stored, but its block unpacks {} bytes out of {}",
            block.get_unpack_size(),
            packed.map_or_else(
                || "no packed stream at all".to_string(),
                |size| size.to_string()
            ),
        )));
    }
    let Some(offset) = offset.and_then(|offset| offset.checked_add(offset_in_block)) else {
        return Body::Opaque(Refusal::Malformed(format!(
            "{FORMAT}: {name} is at an offset that does not fit in a u64"
        )));
    };
    match direct_extent(source, FORMAT, name, offset, len) {
        Ok(extents) => Body::Direct(extents),
        // A member whose stated range is outside the file is refused as
        // that member and not as the archive: the player asked for one
        // file, and the rest of the archive is still readable.
        Err(refusal) => Body::Opaque(refusal),
    }
}

/// What a block's coders are called, for the sentence the player shows:
/// the chain in the order the header lists it, because a filter in front
/// of a compressor is as much of a reason as the compressor.
fn method_names(block: &Block) -> String {
    let names: Vec<String> = block
        .coders
        .iter()
        .map(|coder| {
            EncoderMethod::by_id(coder.encoder_method_id()).map_or_else(
                || {
                    let id: String = coder
                        .encoder_method_id()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect();
                    format!("method {id}")
                },
                |method| method.name().to_string(),
            )
        })
        .collect();
    if names.is_empty() {
        "no method at all".to_string()
    } else {
        names.join(" + ")
    }
}

/// The name as the route matches it: `/`-separated, no leading slash. A
/// 7z written on Windows stores `\`.
fn member_name(name: &str) -> String {
    name.replace('\\', "/").trim_start_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::Extent;
    use crate::sources::testing::{CountingSource, MemorySource};
    use sevenz_rust2::encoder_options::AesEncoderOptions;
    use sevenz_rust2::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, SourceReader};
    use std::io::Cursor;

    /// Mildly incompressible, and different at every offset, so a member
    /// served from the wrong place in the archive is not the same bytes.
    fn payload(len: usize) -> Vec<u8> {
        (0..len)
            .map(|at| (at.wrapping_mul(31) % 251) as u8)
            .collect()
    }

    /// How a fixture packs its entries.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Packing {
        /// One block per entry (`7z a -ms=off`).
        PerEntry,
        /// Every entry in one block, which is what 7-Zip does by default
        /// and calls solid.
        OneBlock,
    }

    /// A 7z built in memory: `methods` is the coder chain the content goes
    /// through, `packing` says whether the entries share a block.
    fn archive(
        methods: Vec<EncoderConfiguration>,
        encrypt_header: bool,
        packing: Packing,
        entries: &[(&str, &[u8])],
    ) -> Vec<u8> {
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).expect("a 7z writer");
        writer.set_content_methods(methods);
        writer.set_encrypt_header(encrypt_header);
        match packing {
            Packing::OneBlock => {
                writer
                    .push_archive_entries(
                        entries
                            .iter()
                            .map(|(name, _)| ArchiveEntry::new_file(name))
                            .collect(),
                        entries
                            .iter()
                            .map(|(_, data)| SourceReader::new(Cursor::new(data.to_vec())))
                            .collect(),
                    )
                    .expect("one block");
            }
            Packing::PerEntry => {
                for (name, data) in entries {
                    writer
                        .push_archive_entry(
                            ArchiveEntry::new_file(name),
                            Some(Cursor::new(data.to_vec())),
                        )
                        .expect("an entry");
                }
            }
        }
        writer.finish().expect("a finished 7z").into_inner()
    }

    fn copy_methods() -> Vec<EncoderConfiguration> {
        vec![EncoderConfiguration::new(EncoderMethod::COPY)]
    }

    fn lzma2_methods() -> Vec<EncoderConfiguration> {
        vec![EncoderConfiguration::new(EncoderMethod::LZMA2)]
    }

    /// COPY, and then AES over it: an archive whose *members* are
    /// encrypted but whose index is readable.
    fn encrypted_methods() -> Vec<EncoderConfiguration> {
        vec![
            EncoderConfiguration::new(EncoderMethod::COPY),
            AesEncoderOptions::new(Password::new("hunter2")).into(),
        ]
    }

    /// A store-method 7z, one block per entry: the shape this translator
    /// exists to serve.
    fn stored(entries: &[(&str, &[u8])]) -> Vec<u8> {
        archive(copy_methods(), false, Packing::PerEntry, entries)
    }

    fn sources(bytes: Vec<u8>) -> Vec<Arc<dyn ByteSource>> {
        vec![Arc::new(MemorySource::new("fixture.7z", bytes))]
    }

    async fn index(bytes: Vec<u8>) -> Index {
        SevenZ.index(&sources(bytes)).await.expect("indexed")
    }

    fn extents_of(member: &Member) -> &[Extent] {
        let Body::Direct(extents) = &member.body else {
            panic!("{} is not direct: {:?}", member.name, member.body);
        };
        extents
    }

    /// The bytes `extents` point at, read out of the archive.
    fn gather(bytes: &[u8], extents: &[Extent]) -> Vec<u8> {
        extents
            .iter()
            .flat_map(|extent| {
                assert_eq!(extent.source, 0, "a 7z is one file");
                bytes[extent.offset as usize..(extent.offset + extent.len) as usize].to_vec()
            })
            .collect()
    }

    /// A stored member is one range of the archive, and it is the range
    /// its bytes really are at -- which is the whole of what this module
    /// claims, checked against the bytes and not against the sketch that
    /// produced the arithmetic.
    #[tokio::test]
    async fn a_stored_member_is_the_range_of_the_archive_its_bytes_are_at() {
        let film = payload(64 * 1024);
        let bytes = stored(&[("notes.txt", b"scene notes"), ("videos/film.bin", &film)]);
        let index = index(bytes.clone()).await;
        assert_eq!(index.members.len(), 2);

        let notes = index.find("notes.txt").expect("the notes").1;
        assert_eq!(notes.len, 11);
        assert_eq!(gather(&bytes, extents_of(notes)), b"scene notes");

        let member = index.find("videos/film.bin").expect("the film").1;
        assert_eq!(member.len, film.len() as u64);
        let extents = extents_of(member);
        assert_eq!(extents.len(), 1, "a stored 7z member is one run");
        assert_eq!(gather(&bytes, extents), film);
        // And the offset is the crate's own arithmetic, spelled out: the
        // signature header, the pack area, and this block's place in it.
        let parsed = Archive::read(&mut Cursor::new(&bytes), &Password::empty()).expect("read");
        let at = parsed
            .files
            .iter()
            .position(|f| f.name == "videos/film.bin")
            .unwrap();
        let block = parsed.stream_map.file_block_index[at].expect("a block");
        let first = parsed.stream_map.block_first_pack_stream_index()[block];
        assert_eq!(
            extents[0].offset,
            32 + parsed.pack_pos() + parsed.stream_map.pack_stream_offsets()[first]
        );
    }

    /// **Several files in one COPY block are each at a stated offset**:
    /// the block's output is its packed bytes, so they lie end to end and
    /// the one asked for starts after the sum of the ones before it. This
    /// is the case the design sketch called out, and it is served rather
    /// than refused -- nothing has to be decoded to reach it.
    #[tokio::test]
    async fn stored_members_sharing_a_block_start_after_the_ones_before_them() {
        let members: [(&str, &[u8]); 3] = [
            ("a.bin", &payload(1000)),
            ("b.bin", &payload(2000)[..2000]),
            ("c.bin", &payload(3000)[..3000]),
        ];
        let owned: Vec<(String, Vec<u8>)> = members
            .iter()
            .map(|(name, data)| (name.to_string(), data.to_vec()))
            .collect();
        let bytes = archive(copy_methods(), false, Packing::OneBlock, &members);
        // The fixture is what it says it is: one block holding all three.
        let parsed = Archive::read(&mut Cursor::new(&bytes), &Password::empty()).expect("read");
        assert_eq!(parsed.blocks.len(), 1, "one block");
        assert!(parsed.is_solid, "the crate calls a shared block solid");

        let index = index(bytes.clone()).await;
        assert_eq!(index.members.len(), 3);
        let mut expected = 0u64;
        for (name, data) in &owned {
            let member = index.find(name).expect("a member").1;
            let extents = extents_of(member);
            assert_eq!(extents.len(), 1);
            assert_eq!(gather(&bytes, extents), *data, "{name}");
            // Consecutive, in the order the archive lists them.
            if expected == 0 {
                expected = extents[0].offset;
            }
            assert_eq!(extents[0].offset, expected, "{name}");
            expected += data.len() as u64;
        }
    }

    /// **The index reads the two ends and not the film**, by range: the
    /// signature header at the front and the packed index at the back,
    /// with nothing of the member's own bytes touched and nothing opened.
    #[tokio::test]
    async fn indexing_reads_the_two_ends_and_not_the_film() {
        let film = payload(256 * 1024);
        let bytes = stored(&[("notes.txt", b"scene notes"), ("videos/film.bin", &film)]);
        let counting = Arc::new(CountingSource::new(Arc::new(MemorySource::new(
            "fixture.7z",
            bytes.clone(),
        ))));
        let counts = counting.counts();
        let index = SevenZ
            .index(&[counting.clone() as Arc<dyn ByteSource>])
            .await
            .expect("indexed");
        let extents = extents_of(index.find("videos/film.bin").expect("the film").1);
        assert_eq!(gather(&bytes, extents), film);

        assert!(
            !counts.read_any_of(extents[0].offset, extents[0].len),
            "the index read the member's own data: {:?}",
            counts.ranges()
        );
        // The signature header, the start header, and the index at the
        // end. Nothing near a film.
        const BOUND: u64 = 1024;
        assert!(
            counts.read_at_bytes() <= BOUND,
            "{} bytes read against a bound of {BOUND}",
            counts.read_at_bytes()
        );
        assert_eq!(counts.opens(), 0, "an index is read_ats");
    }

    /// A compressed member is refused as compressed, naming the chain the
    /// header states.
    #[tokio::test]
    async fn a_compressed_member_is_refused_naming_its_method() {
        let bytes = archive(
            lzma2_methods(),
            false,
            Packing::PerEntry,
            &[("videos/film.bin", &payload(64 * 1024))],
        );
        let index = index(bytes).await;
        assert_eq!(
            index.members[0].body,
            Body::Opaque(Refusal::Compressed {
                format: "7z",
                method: "LZMA2".into(),
            })
        );
    }

    /// A block holding several files that are not stored is refused as
    /// **solid** rather than as compressed: reaching the second file means
    /// decoding the first, which is the more useful half of the truth.
    #[tokio::test]
    async fn a_shared_compressed_block_is_refused_as_solid() {
        let first = payload(4096);
        let second = payload(8192);
        let bytes = archive(
            lzma2_methods(),
            false,
            Packing::OneBlock,
            &[("a.bin", &first), ("videos/film.bin", &second)],
        );
        let index = index(bytes).await;
        assert_eq!(index.members.len(), 2);
        for member in &index.members {
            assert_eq!(member.body, Body::Opaque(Refusal::Solid), "{}", member.name);
        }
    }

    /// An encrypted member is refused as encrypted even though its block
    /// is COPY underneath and its range could be worked out: there is no
    /// password UX, and a wrong guess is ciphertext to the player.
    #[tokio::test]
    async fn an_encrypted_member_is_refused_even_though_it_could_be_mapped() {
        let bytes = archive(
            encrypted_methods(),
            false,
            Packing::PerEntry,
            &[("videos/film.bin", &payload(4096))],
        );
        let index = index(bytes).await;
        assert_eq!(index.members[0].body, Body::Opaque(Refusal::Encrypted));
    }

    /// An encrypted index is refused before any member is named: the
    /// crate asks for a password at the first coder, and there is none to
    /// give it.
    #[tokio::test]
    async fn an_encrypted_index_is_refused_before_any_member_is_named() {
        let bytes = archive(
            encrypted_methods(),
            true,
            Packing::PerEntry,
            &[("videos/film.bin", &payload(4096))],
        );
        assert_eq!(
            SevenZ.index(&sources(bytes)).await.unwrap_err(),
            Refusal::Encrypted
        );
    }

    /// The first part of a `.7z.001` set carries the signature and keeps
    /// its index in the last part, so it is refused as what it is --
    /// naming the shape, because that is the one thing a viewer can act
    /// on.
    #[tokio::test]
    async fn the_first_part_of_a_multi_part_set_is_refused_naming_why() {
        let bytes = stored(&[("videos/film.bin", &payload(64 * 1024))]);
        let first_part = bytes[..bytes.len() / 2].to_vec();
        let refusal = SevenZ
            .index(&sources(first_part))
            .await
            .expect_err("half a 7z is not a 7z");
        let Refusal::Malformed(detail) = &refusal else {
            panic!("{refusal:?}");
        };
        assert!(detail.contains("multi-part"), "{detail}");
    }

    /// And a create that hands over several parts is refused the same
    /// way: this server reads one file, and joining the parts would be
    /// fetching all of them.
    #[tokio::test]
    async fn several_parts_at_once_are_refused_as_one_file_cut_up() {
        let bytes = stored(&[("videos/film.bin", &payload(4096))]);
        let two: Vec<Arc<dyn ByteSource>> = vec![
            Arc::new(MemorySource::new("film.7z.001", bytes.clone())),
            Arc::new(MemorySource::new("film.7z.002", bytes)),
        ];
        let refusal = SevenZ.index(&two).await.expect_err("one file");
        let Refusal::Malformed(detail) = &refusal else {
            panic!("{refusal:?}");
        };
        assert!(detail.contains(".7z.001"), "{detail}");
    }

    /// A file that is not a 7z at all, and one whose start header is
    /// blank -- the shape that would otherwise send the crate reading a
    /// mebibyte backwards a byte at a time.
    #[tokio::test]
    async fn a_file_that_is_not_a_7z_is_refused_without_a_search() {
        for bytes in [
            b"not a 7z at all, not even nearly one".to_vec(),
            b"7z".to_vec(),
            {
                let mut blank = SIGNATURE.to_vec();
                blank.resize(1024, 0);
                blank
            },
        ] {
            let counting = Arc::new(CountingSource::new(Arc::new(MemorySource::new(
                "fixture.7z",
                bytes,
            ))));
            let counts = counting.counts();
            let refusal = SevenZ
                .index(&[counting.clone() as Arc<dyn ByteSource>])
                .await
                .expect_err("not a 7z");
            assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
            assert!(
                counts.read_at_bytes() <= 32,
                "{} bytes read to say it is not a 7z",
                counts.read_at_bytes()
            );
        }
    }

    /// Directories have no bytes and are not members; an empty file has
    /// none either, and is a member of no length rather than a refusal.
    #[tokio::test]
    async fn directories_are_not_members_and_an_empty_file_is_an_empty_member() {
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).expect("a 7z writer");
        writer.set_content_methods(copy_methods());
        writer.set_encrypt_header(false);
        writer
            .push_archive_entry::<Cursor<Vec<u8>>>(ArchiveEntry::new_directory("videos"), None)
            .expect("a directory");
        writer
            .push_archive_entry::<Cursor<Vec<u8>>>(ArchiveEntry::new_file("empty.txt"), None)
            .expect("an empty file");
        writer
            .push_archive_entry(
                ArchiveEntry::new_file("videos/film.bin"),
                Some(Cursor::new(payload(4096))),
            )
            .expect("the film");
        let bytes = writer.finish().expect("a finished 7z").into_inner();

        let index = index(bytes.clone()).await;
        assert!(
            index.find("videos").is_none(),
            "a directory is not a member"
        );
        let empty = index.find("empty.txt").expect("the empty file").1;
        assert_eq!(empty.len, 0);
        assert_eq!(empty.body, Body::Direct(Vec::new()));
        let film = index.find("videos/film.bin").expect("the film").1;
        assert_eq!(gather(&bytes, extents_of(film)), payload(4096));
    }

    /// A name written on Windows is matched the way the route spells it.
    #[test]
    fn a_windows_name_is_slash_separated() {
        assert_eq!(member_name("videos\\film.bin"), "videos/film.bin");
        assert_eq!(member_name("/videos/film.bin"), "videos/film.bin");
    }
}

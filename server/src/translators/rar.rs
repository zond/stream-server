//! RAR, read as an index and never as a decoder -- and read as a **set**,
//! because a film in a RAR is a film across `.part1.rar`, `.part2.rar`, ...
//! or `.rar`, `.r00`, `.r01`, ... far more often than in one file.
//!
//! The walk is `unrar-rs`'s own: [`RarArchive::parse_volume_facts`] over a
//! blocking shim of each volume ([`IndexReader`]), which reads the volume's
//! headers and seeks past every member's data by its stated size -- so a
//! volume costs its headers, whatever the film inside it weighs. The facts
//! of every volume go into a [`StoredLayoutBuilder`], which is the part of
//! the format no header states: a split part's offset *within its member*
//! is the sum of the earlier parts' sizes, known only once every earlier
//! volume has been read. What comes out is, per member, the physical
//! `(volume, offset, len)` of each part -- which is an [`Extent`] list --
//! or the reason it cannot be one.
//!
//! What is refused here that the crate could serve: an **encrypted stored
//! member**, whose bytes the crate can map and decrypt by range, is
//! `Encrypted` all the same -- there is no password UX, and a wrong guess
//! is ciphertext to the player. And what is served here that the crate
//! would not: a member whose only checksum is BLAKE2sp, or none, which the
//! crate marks ineligible because *it* cannot verify such a member out of
//! order. Verification is the fetcher's (`docs/translated-sources.md`
//! §2.2.4): the bytes are piece-checked or they are what the origin served,
//! and a checksum over a member is a read of the whole member.
//!
//! LICENSING: `unrar-rs` is GPL-3.0-or-later, and this module is what the
//! `rar` cargo feature turns on. See `server/Cargo.toml` and AGENTS.md.

use super::{Body, Index, IndexReader, Member, Refusal, Translator};
use crate::sources::{ByteSource, Extent};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use unrar_rs::{
    IneligibilityReason, MalformedReason, MemberEligibility, RarArchive, RarError, RarVolumeFacts,
    StoredLayoutBuilder, StoredMember,
};

/// The hand-built archives the tests read, shared with the integration
/// tests by path. Declared here rather than inside `tests` because a
/// `#[path]` is resolved through the module's directory, and an inline
/// module's directory does not exist on disk for `..` to walk through.
#[cfg(test)]
#[path = "../../tests/support/rar_fixtures.rs"]
mod fixtures;

pub struct Rar;

#[async_trait]
impl Translator for Rar {
    fn format(&self) -> &'static str {
        "rar"
    }

    async fn index(&self, sources: &[Arc<dyn ByteSource>]) -> Result<Index, Refusal> {
        if sources.is_empty() {
            return Err(Refusal::Malformed("no rar to read".to_string()));
        }
        let mut builder: Option<StoredLayoutBuilder> = None;
        // The method each member was packed with, off its first part: the
        // layout reports *that* a member is compressed and the sentence
        // the player sees wants to say how.
        let mut methods: HashMap<String, u8> = HashMap::new();
        for (volume, source) in sources.iter().enumerate() {
            // Encrypted headers never get this far: the parse answers
            // `EncryptedArchive` at the encryption header, before any
            // member is named, and `volume_facts` turns that into the
            // refusal. (The walk is the physical header chain, never the
            // Quick Open cache the crate warns is bound to nothing.)
            let facts = volume_facts(source.clone()).await?;
            let described = source.describe();
            if facts.is_solid {
                return Err(Refusal::Solid);
            }
            // A volume that states its number states which position in the
            // set it is: given in another, the layout would be built from
            // the wrong prefix sums and every extent after the swap would
            // be a different part of the film.
            if let Some(stated) = facts.volume_number
                && stated as usize != volume
            {
                return Err(Refusal::Malformed(format!(
                    "rar: {described} is volume {} of its set, and was given as volume {}",
                    stated + 1,
                    volume + 1
                )));
            }
            if volume + 1 == sources.len() && facts.more_volumes {
                return Err(Refusal::Malformed(format!(
                    "rar: the set continues after {described} (volume {}), and volume {} was \
                     not given",
                    volume + 1,
                    volume + 2
                )));
            }
            let builder =
                builder.get_or_insert_with(|| StoredLayoutBuilder::new(facts.archive_format()));
            builder
                .add_volume(volume as u32, &facts)
                .map_err(|error| Refusal::Malformed(format!("rar: {described}: {error}")))?;
            for member in &facts.members {
                methods
                    .entry(member.name.clone())
                    .or_insert(member.compression_method);
            }
        }
        let builder = builder.expect("at least one volume was added");
        let mut members = Vec::new();
        for stored in builder.members() {
            if let Some(member) = member_of(stored, sources, &methods)? {
                members.push(member);
            }
        }
        Ok(Index { members })
    }

    fn volumes(&self, named: &str, siblings: &[String]) -> Result<Vec<String>, Refusal> {
        volume_set(named, siblings)
    }
}

/// One volume's headers, read on the blocking pool through the shim.
async fn volume_facts(source: Arc<dyn ByteSource>) -> Result<RarVolumeFacts, Refusal> {
    let described = source.describe();
    let reader = IndexReader::new(source);
    let tally = reader.tally();
    let parsed = tokio::task::spawn_blocking(move || RarArchive::parse_volume_facts(reader, None))
        .await
        .map_err(|error| Refusal::Malformed(format!("rar: reading {described}: {error}")))?;
    tracing::debug!(
        volume = %described,
        bytes = tally.load(Ordering::Relaxed),
        "read a rar volume's headers"
    );
    parsed.map_err(|error| match error {
        RarError::EncryptedArchive => Refusal::Encrypted,
        RarError::Io(io) if IndexReader::is_over_budget(&io) => {
            Refusal::Malformed(format!("rar: {described}: {io}"))
        }
        RarError::Io(io) => Refusal::Malformed(format!("rar: {described} could not be read: {io}")),
        RarError::InvalidSignature | RarError::UnsupportedFormat { .. } => {
            Refusal::Malformed(format!("rar: {described} is not a rar"))
        }
        other => Refusal::Malformed(format!("rar: {described}: {other}")),
    })
}

/// A member of the layout as a [`Member`], `None` for an entry that has no
/// bytes to serve (a directory, a link).
fn member_of(
    stored: &StoredMember,
    sources: &[Arc<dyn ByteSource>],
    methods: &HashMap<String, u8>,
) -> Result<Option<Member>, Refusal> {
    let name = member_name(&stored.name);
    let body = match stored.eligibility {
        // Direct, or ineligible for a reason that is the crate's and not
        // ours: it will not route a member it cannot checksum, and this
        // server checksums nothing (see the module doc).
        MemberEligibility::DirectEligible
        | MemberEligibility::Ineligible(
            IneligibilityReason::Blake2OnlyNoCrc32 | IneligibilityReason::NoChecksum,
        ) => direct(stored, &name, sources),
        MemberEligibility::ProvisionallyDirect => Body::Opaque(Refusal::Malformed(format!(
            "rar: {name} continues in a volume that was not given"
        ))),
        MemberEligibility::EncryptedStore(_) => Body::Opaque(Refusal::Encrypted),
        MemberEligibility::Ineligible(reason) => match reason {
            IneligibilityReason::Compressed { .. } => Body::Opaque(Refusal::Compressed {
                format: "rar",
                method: method_name(methods.get(&stored.name).copied().unwrap_or(u8::MAX)),
            }),
            IneligibilityReason::Encrypted => Body::Opaque(Refusal::Encrypted),
            IneligibilityReason::Solid => Body::Opaque(Refusal::Solid),
            IneligibilityReason::Directory | IneligibilityReason::Redirection => return Ok(None),
            IneligibilityReason::MalformedChain(reason) => {
                Body::Opaque(Refusal::Malformed(chain_sentence(&name, reason)))
            }
            IneligibilityReason::Blake2OnlyNoCrc32 | IneligibilityReason::NoChecksum => {
                unreachable!("matched above")
            }
        },
    };
    let len = match &body {
        Body::Direct(extents) => extents.iter().map(|extent| extent.len).sum(),
        Body::Opaque(_) => stored.unpacked_size.unwrap_or(0),
    };
    Ok(Some(Member { name, len, body }))
}

/// The member's parts as extents of the volumes they are in, each checked
/// against its volume's length -- a part a header puts outside its volume
/// is a malformed member, told at the index and not as a read error in
/// the middle of a film.
///
/// Only for a member the layout classified over a **complete** chain
/// (every eligibility this is called for is decided after the chain has
/// closed and its sizes have been checked), so the parts are in volume
/// order and each has its logical offset.
fn direct(stored: &StoredMember, name: &str, sources: &[Arc<dyn ByteSource>]) -> Body {
    let mut extents = Vec::with_capacity(stored.parts.len());
    for part in &stored.parts {
        let volume = part.volume as usize;
        let Some(source) = sources.get(volume) else {
            return Body::Opaque(Refusal::Malformed(format!(
                "rar: {name} has a part in volume {}, and only {} were given",
                volume + 1,
                sources.len()
            )));
        };
        let end = part.data_offset.saturating_add(part.data_size);
        if end > source.len() {
            return Body::Opaque(Refusal::Malformed(format!(
                "rar: {name} claims {}..{end} of a {}-byte volume",
                part.data_offset,
                source.len()
            )));
        }
        extents.push(Extent {
            source: volume,
            offset: part.data_offset,
            len: part.data_size,
        });
    }
    Body::Direct(extents)
}

/// The name as the route matches it: `/`-separated, no leading slash. A
/// RAR4 written on Windows stores `\`.
fn member_name(name: &str) -> String {
    name.replace('\\', "/").trim_start_matches('/').to_string()
}

/// What a compression method is called, for the sentence the player shows.
fn method_name(method: u8) -> String {
    match method {
        0 => "store".to_string(),
        1 => "fastest".to_string(),
        2 => "fast".to_string(),
        3 => "normal".to_string(),
        4 => "good".to_string(),
        5 => "best".to_string(),
        other => format!("method {other}"),
    }
}

/// One sentence for each way a member's split chain contradicts itself.
fn chain_sentence(name: &str, reason: MalformedReason) -> String {
    let detail = match reason {
        MalformedReason::DuplicatePartInVolume { volume } => {
            format!("volume {} holds two parts of it", volume + 1)
        }
        MalformedReason::OverlappingParts { volume } => {
            format!(
                "its bytes in volume {} overlap another member's",
                volume + 1
            )
        }
        MalformedReason::ContinuationInFirstVolume => {
            "the first volume holds a continuation of it, so an earlier volume is missing"
                .to_string()
        }
        MalformedReason::UnexpectedChainStart { volume } => {
            format!("volume {} starts it over", volume + 1)
        }
        MalformedReason::UnexpectedChainEnd { volume } => {
            format!("volume {} ends it early", volume + 1)
        }
        MalformedReason::MissingPartInAddedVolume { volume } => {
            format!(
                "volume {} should hold a part of it and does not",
                volume + 1
            )
        }
        MalformedReason::MissingUnpackedSize => "no header states its size".to_string(),
        MalformedReason::InconsistentUnpackedSize { first, second } => {
            format!("its headers disagree about its size ({first} and {second})")
        }
        MalformedReason::MixedEncryption { volume } => {
            format!(
                "volume {} encrypts it differently from the rest",
                volume + 1
            )
        }
        MalformedReason::SizeMismatch {
            packed_total,
            unpacked_size,
        } => format!(
            "its parts hold {packed_total} bytes of a {unpacked_size}-byte file, so a volume is \
             missing or not the right one"
        ),
        MalformedReason::ExceedsDeclaredSize {
            packed_total,
            unpacked_size,
        } => format!("its parts hold {packed_total} bytes of a {unpacked_size}-byte file"),
    };
    format!("rar: {name}: {detail}")
}

/// The volumes of the set `named` belongs to, among `siblings`, in order --
/// by the two naming rules and nothing else (`docs/translated-sources.md`
/// §6):
///
/// * `name.partN.rar`, ascending `N`, which may be zero-padded;
/// * `name.rar`, then `name.r00`, `name.r01`, ...
///
/// A file named neither way is one volume. A run with a gap is a set
/// missing a volume, refused **naming it** -- what the layout would
/// otherwise report as a chain whose sizes do not add up, which is true
/// and unhelpful.
pub fn volume_set(named: &str, siblings: &[String]) -> Result<Vec<String>, Refusal> {
    // The named file is a volume of its own set whether or not the list it
    // came with mentions it.
    let mut with_named;
    let siblings = if siblings.iter().any(|sibling| sibling == named) {
        siblings
    } else {
        with_named = siblings.to_vec();
        with_named.push(named.to_string());
        &with_named
    };
    let lower = named.to_ascii_lowercase();
    if let Some((stem, digits)) = part_number(named, &lower) {
        return part_set(stem, digits.len(), siblings);
    }
    if let Some(stem) = lower.strip_suffix(".rar") {
        let stem = &named[..stem.len()];
        return old_set(stem, named, siblings);
    }
    if let Some(stem) = old_continuation(&lower) {
        let stem = &named[..stem.len()];
        let first = siblings
            .iter()
            .find(|candidate| {
                candidate.len() == stem.len() + 4
                    && candidate.starts_with(stem)
                    && candidate[stem.len()..].eq_ignore_ascii_case(".rar")
            })
            .ok_or_else(|| {
                Refusal::Malformed(format!(
                    "rar: {named} is a continuation volume, and the set's first volume {stem}.rar \
                     is not beside it"
                ))
            })?;
        return old_set(stem, first, siblings);
    }
    Ok(vec![named.to_string()])
}

/// `(stem, digits)` of a `stem.partN.rar` name, on the lowercased spelling.
fn part_number<'a>(named: &'a str, lower: &'a str) -> Option<(&'a str, &'a str)> {
    let stem_and_number = lower.strip_suffix(".rar")?;
    let at = stem_and_number.rfind(".part")?;
    let digits = &stem_and_number[at + ".part".len()..];
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((
        &named[..at],
        &named[at + ".part".len()..at + ".part".len() + digits.len()],
    ))
}

/// `stem` of a `stem.rNN` name, on the lowercased spelling.
fn old_continuation(lower: &str) -> Option<&str> {
    let at = lower.len().checked_sub(4)?;
    let suffix = &lower[at..];
    (suffix.starts_with(".r") && suffix[2..].bytes().all(|byte| byte.is_ascii_digit()))
        .then_some(&lower[..at])
}

/// Every `stem.partN.rar` among `siblings`, by `N`, with no gap.
fn part_set(stem: &str, width: usize, siblings: &[String]) -> Result<Vec<String>, Refusal> {
    let mut parts: Vec<(u64, &String)> = siblings
        .iter()
        .filter_map(|candidate| {
            let lower = candidate.to_ascii_lowercase();
            let (candidate_stem, digits) = part_number(candidate, &lower)?;
            (candidate_stem == stem)
                .then(|| digits.parse::<u64>().ok().map(|number| (number, candidate)))
                .flatten()
        })
        .collect();
    parts.sort_by_key(|(number, _)| *number);
    parts.dedup_by_key(|(number, _)| *number);
    let first = parts.first().map(|(number, _)| *number).unwrap_or(1);
    let mut names = Vec::with_capacity(parts.len());
    for (expected, (number, name)) in (first..).zip(&parts) {
        if *number != expected {
            return Err(Refusal::Malformed(format!(
                "rar: the set is missing {stem}.part{expected:0width$}.rar"
            )));
        }
        names.push((*name).clone());
    }
    Ok(names)
}

/// `first` (the `.rar`), then every `stem.rNN` among `siblings`, by `NN`,
/// with no gap.
fn old_set(stem: &str, first: &str, siblings: &[String]) -> Result<Vec<String>, Refusal> {
    let mut rest: Vec<(u64, &String)> = siblings
        .iter()
        .filter_map(|candidate| {
            let lower = candidate.to_ascii_lowercase();
            let candidate_stem = old_continuation(&lower)?;
            (candidate.len() == stem.len() + 4
                && candidate.starts_with(stem)
                && candidate_stem.len() == stem.len())
            .then(|| {
                lower[stem.len() + 2..]
                    .parse::<u64>()
                    .ok()
                    .map(|number| (number, candidate))
            })
            .flatten()
        })
        .collect();
    rest.sort_by_key(|(number, _)| *number);
    rest.dedup_by_key(|(number, _)| *number);
    let mut names = vec![first.to_string()];
    for (expected, (number, name)) in (0u64..).zip(&rest) {
        if *number != expected {
            return Err(Refusal::Malformed(format!(
                "rar: the set is missing {stem}.r{expected:02}"
            )));
        }
        names.push((*name).clone());
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::testing::{CountingSource, MemorySource};

    use super::fixtures::{self, Hash, Method, Rar5Options, payload};

    fn source(name: &str, bytes: Vec<u8>) -> Arc<dyn ByteSource> {
        Arc::new(MemorySource::new(name, bytes))
    }

    fn sources(bytes: Vec<u8>) -> Vec<Arc<dyn ByteSource>> {
        vec![source("fixture.rar", bytes)]
    }

    async fn index(volumes: Vec<Vec<u8>>) -> Index {
        let sources: Vec<Arc<dyn ByteSource>> = volumes
            .into_iter()
            .enumerate()
            .map(|(at, bytes)| source(&format!("fixture.part{}.rar", at + 1), bytes))
            .collect();
        Rar.index(&sources).await.expect("indexed")
    }

    /// The bytes `extents` point at, read out of `volumes`.
    fn gather(volumes: &[Vec<u8>], extents: &[Extent]) -> Vec<u8> {
        extents
            .iter()
            .flat_map(|extent| {
                volumes[extent.source]
                    [extent.offset as usize..(extent.offset + extent.len) as usize]
                    .to_vec()
            })
            .collect()
    }

    fn extents_of(member: &Member) -> &[Extent] {
        let Body::Direct(extents) = &member.body else {
            panic!("{} is not direct: {:?}", member.name, member.body);
        };
        extents
    }

    /// A stored member of a single volume is one range of that volume, and
    /// it is the range its bytes really are at -- in both generations of
    /// the format.
    #[tokio::test]
    async fn a_stored_member_is_the_range_of_the_volume_its_bytes_are_at() {
        let data = payload(4096);
        for archive in [
            fixtures::rar5_stored(&[("notes.txt", b"small"), ("movie.mkv", &data)]),
            fixtures::rar4_stored(&[("notes.txt", b"small"), ("movie.mkv", &data)]),
        ] {
            let index = Rar.index(&sources(archive.clone())).await.expect("indexed");
            assert_eq!(index.members.len(), 2);
            let member = &index.members[1];
            assert_eq!(member.name, "movie.mkv");
            assert_eq!(member.len, data.len() as u64);
            let extents = extents_of(member);
            assert_eq!(extents.len(), 1);
            assert_eq!(gather(&[archive], extents), data);
        }
    }

    /// **A set is the ordinary case.** A member split across three
    /// volumes is three extents, one per volume, in order, which together
    /// are the member -- RAR5 and RAR4 alike.
    #[tokio::test]
    async fn a_member_across_three_volumes_is_three_extents_in_volume_order() {
        let data = payload(100_000);
        let members = [
            ("readme.nfo", &b"scene notes"[..]),
            ("movie.mkv", &data[..]),
        ];
        for volumes in [
            fixtures::rar5_volumes(&members, 40_000),
            fixtures::rar4_volumes(&members, 40_000, true),
        ] {
            assert_eq!(volumes.len(), 3, "the fixture splits into three");
            let index = index(volumes.clone()).await;
            let movie = index.find("movie.mkv").expect("the film").1;
            assert_eq!(movie.len, data.len() as u64);
            let extents = extents_of(movie);
            assert_eq!(
                extents
                    .iter()
                    .map(|extent| extent.source)
                    .collect::<Vec<_>>(),
                vec![0, 1, 2]
            );
            assert_eq!(gather(&volumes, extents), data);
            // And the small member before it is whole inside the first.
            let nfo = index.find("readme.nfo").expect("the nfo").1;
            assert_eq!(gather(&volumes, extents_of(nfo)), b"scene notes");
        }
    }

    /// **The index reads the headers and not the film**, per volume and by
    /// range: no read overlapped a part's data, the shim's reads are
    /// `read_at`s (nothing opened), and each volume cost under a stated
    /// bound -- so the set costs at most that bound times its volumes.
    #[tokio::test]
    async fn indexing_a_set_reads_each_volumes_headers_and_nothing_else() {
        let data = payload(300_000);
        let volumes = fixtures::rar5_volumes(&[("movie.mkv", &data)], 100_000);
        let counting: Vec<Arc<CountingSource>> = volumes
            .iter()
            .enumerate()
            .map(|(at, bytes)| {
                Arc::new(CountingSource::new(source(
                    &format!("fixture.part{}.rar", at + 1),
                    bytes.clone(),
                )))
            })
            .collect();
        let sources: Vec<Arc<dyn ByteSource>> = counting
            .iter()
            .map(|source| source.clone() as Arc<dyn ByteSource>)
            .collect();
        let index = Rar.index(&sources).await.expect("indexed");
        let extents = extents_of(&index.members[0]);
        assert_eq!(extents.len(), 3);

        // The stated bound per volume: the signature, a main header, one
        // file header with its name, and an end header. Nothing near it.
        const VOLUME_BOUND: u64 = 256;
        for (volume, source) in counting.iter().enumerate() {
            let counts = source.counts();
            let extent = extents[volume];
            assert!(
                !counts.read_any_of(extent.offset, extent.len),
                "volume {volume}: the index read the member's own data: {:?}",
                counts.ranges()
            );
            assert!(
                counts.read_at_bytes() <= VOLUME_BOUND,
                "volume {volume}: {} bytes read against a bound of {VOLUME_BOUND}",
                counts.read_at_bytes()
            );
            assert_eq!(counts.opens(), 0, "an index is read_ats");
        }
        let total: u64 = counting.iter().map(|source| source.counts().bytes()).sum();
        assert!(total <= VOLUME_BOUND * volumes.len() as u64);
    }

    /// A compressed member is refused as compressed, naming the method the
    /// way `rar` names it.
    #[tokio::test]
    async fn a_compressed_member_is_refused_as_compressed() {
        let archive = fixtures::rar5_archive(
            &[("movie.mkv", &payload(4096))],
            &Rar5Options {
                method: Method::Normal,
                ..Default::default()
            },
        );
        let index = Rar.index(&sources(archive)).await.expect("indexed");
        assert_eq!(
            index.members[0].body,
            Body::Opaque(Refusal::Compressed {
                format: "rar",
                method: "normal".into()
            })
        );
        let rar4 = fixtures::rar4_archive(&[("movie.mkv", &payload(4096))], Method::Normal, 0);
        let index = Rar.index(&sources(rar4)).await.expect("indexed");
        assert!(
            matches!(
                index.members[0].body,
                Body::Opaque(Refusal::Compressed { .. })
            ),
            "{:?}",
            index.members[0].body
        );
    }

    /// A solid archive is refused whole, before a member is looked at.
    #[tokio::test]
    async fn a_solid_archive_is_refused_whole() {
        let members = [("a.mkv", &payload(64)[..]), ("b.mkv", &payload(64)[..])];
        let rar5 = fixtures::rar5_archive(
            &members,
            &Rar5Options {
                solid: true,
                method: Method::Normal,
                ..Default::default()
            },
        );
        assert_eq!(Rar.index(&sources(rar5)).await.unwrap_err(), Refusal::Solid);
        let rar4 = fixtures::rar4_solid(&members);
        assert_eq!(Rar.index(&sources(rar4)).await.unwrap_err(), Refusal::Solid);
    }

    /// Encrypted headers are `Encrypted` before any member is named, in
    /// both generations.
    #[tokio::test]
    async fn encrypted_headers_are_refused_before_any_member_is_named() {
        for archive in [
            fixtures::rar5_header_encrypted(),
            fixtures::rar4_header_encrypted(),
        ] {
            assert_eq!(
                Rar.index(&sources(archive)).await.unwrap_err(),
                Refusal::Encrypted
            );
        }
    }

    /// An encrypted *stored* member -- which the crate can map to ranges,
    /// and could decrypt by range -- is refused all the same: there is no
    /// password UX, and a wrong guess is ciphertext to the player.
    #[tokio::test]
    async fn an_encrypted_stored_member_is_refused_even_though_it_could_be_mapped() {
        let archive = fixtures::rar5_archive(
            &[("movie.mkv", &payload(4096))],
            &Rar5Options {
                encrypted_members: true,
                ..Default::default()
            },
        );
        let index = Rar.index(&sources(archive)).await.expect("indexed");
        assert_eq!(index.members[0].body, Body::Opaque(Refusal::Encrypted));
    }

    /// A member whose only checksum is BLAKE2sp is served: the crate will
    /// not route what it cannot verify out of order, and this server does
    /// not verify at all -- the fetcher does.
    #[tokio::test]
    async fn a_member_with_only_a_blake2_checksum_is_served_because_nothing_here_verifies() {
        let data = payload(4096);
        let archive = fixtures::rar5_archive(
            &[("movie.mkv", &data)],
            &Rar5Options {
                hash: Hash::Blake2,
                ..Default::default()
            },
        );
        let index = Rar.index(&sources(archive.clone())).await.expect("indexed");
        assert_eq!(gather(&[archive], extents_of(&index.members[0])), data);
    }

    /// A member with no checksum at all is served for the same reason.
    #[tokio::test]
    async fn a_member_with_no_checksum_is_served_too() {
        let data = payload(4096);
        let archive = fixtures::rar5_archive(
            &[("movie.mkv", &data)],
            &Rar5Options {
                hash: Hash::None,
                ..Default::default()
            },
        );
        let index = Rar.index(&sources(archive.clone())).await.expect("indexed");
        assert_eq!(gather(&[archive], extents_of(&index.members[0])), data);
    }

    /// Directories are not members: they have no bytes, and a player that
    /// asked for one by name would be asking for nothing.
    #[tokio::test]
    async fn directories_are_not_members() {
        let archive =
            fixtures::rar5_stored(&[("videos/", b""), ("videos/movie.mkv", &payload(64))]);
        let index = Rar.index(&sources(archive)).await.expect("indexed");
        assert_eq!(index.members.len(), 1);
        assert_eq!(index.members[0].name, "videos/movie.mkv");
    }

    /// A member carrying the solid bit on its own -- with the archive not
    /// declared solid -- is refused as solid, by itself.
    #[tokio::test]
    async fn a_member_marked_solid_on_its_own_is_refused_as_solid() {
        let archive = fixtures::rar5_archive(
            &[("a.mkv", &payload(64)), ("b.mkv", &payload(64))],
            &Rar5Options {
                solid_members: true,
                ..Default::default()
            },
        );
        let index = Rar.index(&sources(archive)).await.expect("indexed");
        assert!(matches!(index.members[0].body, Body::Direct(_)));
        assert_eq!(index.members[1].body, Body::Opaque(Refusal::Solid));
    }

    /// A RAR4 member under one of the pre-AES ciphers is encryption the
    /// crate cannot even map, and is `Encrypted` like the rest.
    #[tokio::test]
    async fn a_member_under_a_legacy_cipher_is_refused_as_encrypted() {
        let archive = fixtures::rar4_legacy_encrypted(&[("movie.mkv", &payload(64))]);
        let index = Rar.index(&sources(archive)).await.expect("indexed");
        assert_eq!(index.members[0].body, Body::Opaque(Refusal::Encrypted));
    }

    /// A set that states no volume numbers anywhere -- an old `.rar`/`.r00`
    /// set -- is judged by its chains: a middle volume left out is a
    /// member whose parts do not add up, and a set given from its second
    /// volume is a continuation with nothing before it. Both are said as
    /// sentences about the member.
    #[tokio::test]
    async fn an_unnumbered_set_with_a_hole_is_told_by_its_chain() {
        let data = payload(100_000);
        let volumes = fixtures::rar4_volumes_unnumbered(&[("movie.mkv", &data)], 40_000);
        assert_eq!(volumes.len(), 3);

        let gapped: Vec<Arc<dyn ByteSource>> = vec![
            source("movie.rar", volumes[0].clone()),
            source("movie.r01", volumes[2].clone()),
        ];
        let index = Rar.index(&gapped).await.expect("indexed");
        let Body::Opaque(Refusal::Malformed(detail)) = &index.members[0].body else {
            panic!("{:?}", index.members[0].body);
        };
        assert!(detail.contains("a volume is missing"), "{detail}");

        let headless: Vec<Arc<dyn ByteSource>> = vec![
            source("movie.r00", volumes[1].clone()),
            source("movie.r01", volumes[2].clone()),
        ];
        let index = Rar.index(&headless).await.expect("indexed");
        let Body::Opaque(Refusal::Malformed(detail)) = &index.members[0].body else {
            panic!("{:?}", index.members[0].body);
        };
        assert!(detail.contains("earlier volume is missing"), "{detail}");
    }

    /// A member that continues past a volume which claims to be the last
    /// is a chain still open when every volume has been added, and is
    /// malformed for it -- the case the layout reports as provisional.
    #[tokio::test]
    async fn a_chain_left_open_by_the_last_volume_is_malformed() {
        let data = payload(100_000);
        let mut first = fixtures::rar5_volumes(&[("movie.mkv", &data)], 40_000).remove(0);
        // Rewrite the end header: no more volumes, says the volume whose
        // one member continues into the next.
        let end = fixtures::rar5_end_header(true);
        assert!(first.ends_with(&end));
        first.truncate(first.len() - end.len());
        first.extend_from_slice(&fixtures::rar5_end_header(false));
        let index = Rar.index(&sources(first)).await.expect("indexed");
        let Body::Opaque(Refusal::Malformed(detail)) = &index.members[0].body else {
            panic!("{:?}", index.members[0].body);
        };
        assert!(
            detail.contains("continues in a volume that was not given"),
            "{detail}"
        );
    }

    /// A set given without its last volume is refused naming the volume
    /// it wanted, and a set given with its volumes out of order is refused
    /// as the wrong set -- either would otherwise be a film with a hole.
    #[tokio::test]
    async fn a_set_missing_a_volume_or_given_out_of_order_is_malformed() {
        let data = payload(100_000);
        let volumes = fixtures::rar5_volumes(&[("movie.mkv", &data)], 40_000);

        let truncated: Vec<Arc<dyn ByteSource>> = vec![
            source("fixture.part1.rar", volumes[0].clone()),
            source("fixture.part2.rar", volumes[1].clone()),
        ];
        let refusal = Rar.index(&truncated).await.unwrap_err();
        let Refusal::Malformed(detail) = &refusal else {
            panic!("{refusal:?}");
        };
        assert!(detail.contains("volume 3 was not given"), "{detail}");

        let swapped: Vec<Arc<dyn ByteSource>> = vec![
            source("fixture.part1.rar", volumes[0].clone()),
            source("fixture.part3.rar", volumes[2].clone()),
            source("fixture.part2.rar", volumes[1].clone()),
        ];
        let refusal = Rar.index(&swapped).await.unwrap_err();
        let Refusal::Malformed(detail) = &refusal else {
            panic!("{refusal:?}");
        };
        assert!(
            detail.contains("fixture.part3.rar is volume 3 of its set, and was given as volume 2"),
            "{detail}"
        );

        // The middle volume left out: the last one says which it is.
        let gapped: Vec<Arc<dyn ByteSource>> = vec![
            source("fixture.part1.rar", volumes[0].clone()),
            source("fixture.part3.rar", volumes[2].clone()),
        ];
        let refusal = Rar.index(&gapped).await.unwrap_err();
        assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
    }

    /// Bytes that are not a RAR are refused by saying so.
    #[tokio::test]
    async fn bytes_that_are_not_a_rar_are_refused() {
        let refusal = Rar
            .index(&sources(b"not a rar at all, not even close".to_vec()))
            .await
            .unwrap_err();
        assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
        // A signature and then garbage: a truncated header, not a panic.
        let mut broken = vec![0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];
        broken.extend_from_slice(&[0xFF; 64]);
        let refusal = Rar.index(&sources(broken)).await.unwrap_err();
        assert!(matches!(refusal, Refusal::Malformed(_)), "{refusal:?}");
    }

    /// A member whose header puts its bytes outside the volume is refused
    /// as that member, at the index.
    #[tokio::test]
    async fn a_part_outside_its_volume_is_malformed() {
        let data = payload(4096);
        let mut archive = fixtures::rar5_stored(&[("movie.mkv", &data)]);
        // Cut the volume short of the member's data: the header still
        // claims 4096 bytes follow it.
        archive.truncate(archive.len() - 2048);
        let index = Rar.index(&sources(archive)).await.expect("indexed");
        assert!(
            matches!(index.members[0].body, Body::Opaque(Refusal::Malformed(_))),
            "{:?}",
            index.members[0].body
        );
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    /// `name.partN.rar`: every part with that stem, by `N`, whichever of
    /// them was named, zero-padded or not, and nobody else's set.
    #[test]
    fn part_numbered_volumes_are_found_by_stem_and_number() {
        let siblings = names(&[
            "Film.2024.part3.rar",
            "Film.2024.nfo",
            "Film.2024.part1.rar",
            "Other.part1.rar",
            "Film.2024.part2.rar",
            "Sample/Film.2024.sample.mkv",
        ]);
        let set = volume_set("Film.2024.part2.rar", &siblings).unwrap();
        assert_eq!(
            set,
            names(&[
                "Film.2024.part1.rar",
                "Film.2024.part2.rar",
                "Film.2024.part3.rar"
            ])
        );
        // Zero-padded, with a tenth volume that sorts after the ninth and
        // not after the first.
        let padded: Vec<String> = (1..=10)
            .rev()
            .map(|n| format!("x.part{n:02}.rar"))
            .collect();
        let set = volume_set("x.part07.rar", &padded).unwrap();
        assert_eq!(set.len(), 10);
        assert_eq!(set[9], "x.part10.rar");
        // Upper-case suffixes are the same rule.
        let set = volume_set("Y.PART1.RAR", &names(&["Y.PART2.RAR", "Y.PART1.RAR"])).unwrap();
        assert_eq!(set, names(&["Y.PART1.RAR", "Y.PART2.RAR"]));
    }

    /// `name.rar`, `name.r00`, `name.r01`: the `.rar` first, then the rest
    /// by number, whichever of them was named.
    #[test]
    fn old_numbered_volumes_are_found_from_the_rar_and_from_a_continuation() {
        let siblings = names(&["show.r01", "show.sfv", "show.rar", "show.r00", "other.r00"]);
        assert_eq!(
            volume_set("show.rar", &siblings).unwrap(),
            names(&["show.rar", "show.r00", "show.r01"])
        );
        assert_eq!(
            volume_set("show.r01", &siblings).unwrap(),
            names(&["show.rar", "show.r00", "show.r01"])
        );
        // A continuation whose `.rar` is not there has no first volume.
        let refusal = volume_set("show.r00", &names(&["show.r00", "show.r01"])).unwrap_err();
        assert!(refusal.to_string().contains("show.rar"), "{refusal}");
    }

    /// A gap in either run is a missing volume, named.
    #[test]
    fn a_gap_in_the_run_names_the_missing_volume() {
        let refusal = volume_set(
            "f.part1.rar",
            &names(&["f.part1.rar", "f.part3.rar", "f.part4.rar"]),
        )
        .unwrap_err();
        assert!(refusal.to_string().contains("f.part2.rar"), "{refusal}");
        let refusal =
            volume_set("f.part01.rar", &names(&["f.part01.rar", "f.part03.rar"])).unwrap_err();
        assert!(refusal.to_string().contains("f.part02.rar"), "{refusal}");
        let refusal = volume_set("f.rar", &names(&["f.rar", "f.r00", "f.r02"])).unwrap_err();
        assert!(refusal.to_string().contains("f.r01"), "{refusal}");
    }

    /// A name under neither rule, or one with no siblings, is one volume.
    #[test]
    fn a_name_under_neither_rule_is_one_volume() {
        assert_eq!(
            volume_set("archive.RAR", &names(&["archive.RAR", "notes.txt"])).unwrap(),
            names(&["archive.RAR"])
        );
        assert_eq!(
            volume_set(
                "odd.name.v2.rar",
                &names(&["odd.name.v2.rar", "odd.name.v2.r0"])
            )
            .unwrap(),
            names(&["odd.name.v2.rar"])
        );
        assert_eq!(
            volume_set("a.part1.rar", &[]).unwrap(),
            names(&["a.part1.rar"])
        );
    }

    #[test]
    fn a_method_is_named_for_the_sentence_the_player_shows() {
        assert_eq!(method_name(3), "normal");
        assert_eq!(method_name(0), "store");
        assert_eq!(method_name(9), "method 9");
    }

    #[test]
    fn a_windows_name_is_matched_with_slashes() {
        assert_eq!(member_name("Season 1\\E01.mkv"), "Season 1/E01.mkv");
        assert_eq!(member_name("/abs/x.mkv"), "abs/x.mkv");
    }

    /// **The fixtures are archives a real reader accepts.** Where `unrar`
    /// is installed, every stored fixture -- single volumes and sets under
    /// both naming rules, both generations -- passes its test mode, which
    /// checks every header and every checksum. Without it the test says
    /// so and passes: the rest of this module is asserted against the
    /// crate's own parser either way.
    #[test]
    fn the_stored_fixtures_pass_unrar_where_it_is_installed() {
        let unrar = match std::process::Command::new("unrar").arg("-inul").output() {
            Ok(_) => "unrar",
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("skipping: no unrar on this machine");
                return;
            }
            Err(error) => panic!("{error}"),
        };
        let data = payload(100_000);
        let members = [
            ("readme.nfo", &b"scene notes"[..]),
            ("movie.mkv", &data[..]),
        ];
        let dir = tempfile::tempdir().unwrap();
        let mut sets: Vec<(String, Vec<Vec<u8>>)> = Vec::new();
        sets.push(("single5.rar".into(), vec![fixtures::rar5_stored(&members)]));
        sets.push(("single4.rar".into(), vec![fixtures::rar4_stored(&members)]));
        let rar5 = fixtures::rar5_volumes(&members, 40_000);
        let rar4_new = fixtures::rar4_volumes(&members, 40_000, true);
        let rar4_old = fixtures::rar4_volumes(&members, 40_000, false);
        for (stem, volumes, names) in [
            (
                "five",
                rar5.clone(),
                fixtures::part_names("five", rar5.len()),
            ),
            (
                "four",
                rar4_new.clone(),
                fixtures::part_names("four", rar4_new.len()),
            ),
            (
                "old",
                rar4_old.clone(),
                fixtures::old_names("old", rar4_old.len()),
            ),
        ] {
            let _ = stem;
            for (name, bytes) in names.iter().zip(&volumes) {
                std::fs::write(dir.path().join(name), bytes).unwrap();
            }
            sets.push((names[0].clone(), Vec::new()));
        }
        for (first, volumes) in &sets {
            if let Some(bytes) = volumes.first() {
                std::fs::write(dir.path().join(first), bytes).unwrap();
            }
            let status = std::process::Command::new(unrar)
                .arg("t")
                .arg("-inul")
                .arg(dir.path().join(first))
                .status()
                .unwrap();
            assert!(status.success(), "unrar t {first} answered {status}");
        }
    }
}

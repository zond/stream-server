//! Disc images -- ISO 9660 and UDF -- as a translator: [`crate::images`]
//! behind a [`ByteSource`].
//!
//! The parsers are the images module's own and nothing here adds to them.
//! What this file is, is the seam `docs/translated-sources.md` §5 step 6
//! asked for: a [`ByteSource`] read as an [`ImageReader`] (the same two
//! calls, `len` and `read_at`, under other names), each [`ImageFile`] --
//! a path, a length and the image's own byte ranges that hold it --
//! restated as a [`Member`] whose extents name source 0, since the image's
//! extents are image-absolute already and the image is the one source, and
//! the images module's typed [`images::Refusal`] said in the words the
//! route already maps to HTTP.
//!
//! **The read budget is the images module's.** Both parsers read through
//! `images::Budget` -- 8 MiB per image, counted, and never a byte of a
//! file's data -- so this translator does not wrap the source in a
//! `translators::Budget` of its own: two counters over one walk would be
//! two places for the bound to be wrong. The tests here count through
//! `CountingSource` instead and assert, by range, that indexing a 9660
//! image and a UDF image touched neither file's data.
//!
//! A DVD-Video image is a bridge disc and indexes through its 9660 tree. A
//! Blu-ray image is UDF 2.50 with no 9660 tree at all, and its metadata
//! partition map is what the UDF parser refuses by name today: that
//! arrives here as [`Refusal::Unsupported`] and at the player as a `415`
//! whose sentence names the map -- the honest answer until the next UDF
//! increment, and not a wrong extent served with confidence.

use super::{Body, Index, Member, Refusal, Translator, only_source};
use crate::images::{self, ImageFile, ImageReader};
use crate::sources::{ByteSource, Extent};
use async_trait::async_trait;
use std::io;
use std::sync::Arc;

/// What this translator calls itself in a refusal: not "iso", because the
/// image behind the `/iso` prefix may be UDF with no ISO 9660 in it.
const FORMAT: &str = "disc image";

pub struct Iso;

#[async_trait]
impl Translator for Iso {
    fn format(&self) -> &'static str {
        FORMAT
    }

    async fn index(&self, sources: &[Arc<dyn ByteSource>]) -> Result<Index, Refusal> {
        let source = only_source(sources, FORMAT)?;
        let image = SourceImage(source.as_ref());
        let indexed = images::index(&image)
            .await
            .map_err(|refusal| translate(refusal, &source.describe()))?;
        tracing::debug!(
            image = %source.describe(),
            format = %indexed.format,
            files = indexed.files.len(),
            "indexed a disc image"
        );
        Ok(Index {
            members: indexed.files.into_iter().map(member).collect(),
        })
    }
}

/// A [`ByteSource`] as the images module reads one. Borrowed, because an
/// index is one call and the source outlives it.
struct SourceImage<'a>(&'a dyn ByteSource);

#[async_trait]
impl ImageReader for SourceImage<'_> {
    fn len(&self) -> u64 {
        self.0.len()
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read_at(offset, buf).await
    }
}

/// One file of the image as a member: the path without its leading `/`,
/// because the route matches the URL's wildcard against a relative name,
/// and the image's extents as extents of source 0.
fn member(file: ImageFile) -> Member {
    let extents = file
        .extents
        .iter()
        .map(|extent| Extent {
            source: 0,
            offset: extent.offset,
            len: extent.len,
        })
        .collect();
    Member {
        name: file.path.trim_start_matches('/').to_string(),
        len: file.len,
        body: Body::Direct(extents),
    }
}

/// The images module's refusal in the translators' words, and so in the
/// route's status codes (`routes::archive::refusal_response`):
/// `Unsupported` and `Encrypted` are `415` -- a well-formed image this
/// server will not serve by range -- `Malformed` and `NotAnImage` are `422`
/// -- the bytes are not what the prefix said they were -- and `Unreadable`
/// is the read error, worded the way `Budget::read_exact` words one for
/// every other translator: the source's short name and the error, never
/// its URL.
fn translate(refusal: images::Refusal, image: &str) -> Refusal {
    match refusal {
        images::Refusal::Unsupported { format, what } => Refusal::Unsupported {
            format: image_kind(format),
            what,
        },
        images::Refusal::Encrypted { .. } => Refusal::Encrypted,
        images::Refusal::Malformed { format, detail } => {
            Refusal::Malformed(format!("{format}: {detail}"))
        }
        images::Refusal::NotAnImage { detail } => {
            Refusal::Malformed(format!("not a disc image: {detail}"))
        }
        images::Refusal::Unreadable { detail } => {
            Refusal::Malformed(format!("{image} could not be read: {detail}"))
        }
    }
}

/// "UDF image" rather than "UDF": the images module names the filesystem,
/// and the sentence [`Refusal::Unsupported`] makes wants the thing the
/// viewer has, which is an image of it.
fn image_kind(format: &'static str) -> &'static str {
    match format {
        "UDF" => "UDF image",
        "ISO 9660" => "ISO 9660 image",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::images::fixtures::{iso, udf};
    use crate::sources::ReadHint;
    use crate::sources::testing::{CountingSource, MemorySource};

    fn sources(bytes: Vec<u8>) -> Vec<Arc<dyn ByteSource>> {
        vec![Arc::new(MemorySource::new("fixture.iso", bytes))]
    }

    /// The one direct member of an image, checked against the image: its
    /// extents are the image's own bytes, at the range the fixture wrote
    /// the file to.
    fn the_one_member(image: &[u8], index: &Index, name: &str, data: (u64, u64)) {
        assert_eq!(index.members.len(), 1, "{:?}", index.members);
        let member = &index.members[0];
        assert_eq!(member.name, name);
        assert_eq!(member.len, data.1);
        let Body::Direct(extents) = &member.body else {
            panic!("an image file is direct: {:?}", member.body);
        };
        assert_eq!(
            extents,
            &vec![Extent {
                source: 0,
                offset: data.0,
                len: data.1
            }]
        );
        let first = image[data.0 as usize];
        assert!(
            image[data.0 as usize..(data.0 + data.1) as usize]
                .iter()
                .all(|byte| *byte == first),
            "the extent is not the fixture's fill"
        );
    }

    /// An ISO 9660 file is a member named without the leading `/`, one
    /// extent of source 0 at the image's own offset.
    #[tokio::test]
    async fn a_9660_file_is_a_direct_member_at_the_images_own_range() {
        let image = iso::minimal_iso();
        let index = Iso.index(&sources(image.clone())).await.expect("indexed");
        the_one_member(&image, &index, "HELLO.TXT", iso::data_range(&image));
    }

    /// The same through the UDF parser, which a Blu-ray image reaches
    /// with no 9660 tree in front of it.
    #[tokio::test]
    async fn a_udf_file_is_a_direct_member_at_the_images_own_range() {
        let image = udf::minimal_udf();
        let index = Iso.index(&sources(image.clone())).await.expect("indexed");
        the_one_member(&image, &index, "MOVIE.BIN", udf::data_range(&image));
    }

    /// A file in a directory is named by its path, and a file written in
    /// pieces is a member of several extents whose lengths sum to its
    /// length -- the Blu-ray shape, `BDMV/STREAM/00000.m2ts`.
    #[tokio::test]
    async fn a_nested_udf_file_keeps_its_path_and_its_extents() {
        let index = Iso
            .index(&sources(udf::udf_with_subdirectory()))
            .await
            .expect("indexed");
        let (_, member) = index
            .find("BDMV/STREAM/00000.m2ts")
            .unwrap_or_else(|| panic!("named by its path: {:?}", index.members));
        let Body::Direct(extents) = &member.body else {
            panic!("direct");
        };
        assert_eq!(extents.len(), 2, "{extents:?}");
        assert_eq!(
            extents.iter().map(|extent| extent.len).sum::<u64>(),
            member.len
        );
        assert!(extents.iter().all(|extent| extent.source == 0));
    }

    /// **The index reads the descriptors and the tree, and not the film.**
    /// By range, through the repository's own counting source rather
    /// than the images module's, so what is measured is what the route
    /// will do to a torrent or a proxied entity -- and for both formats,
    /// since they are two parsers. Nothing is opened: an index is
    /// `read_at`s, and it stays inside the images module's budget.
    #[tokio::test]
    async fn indexing_reads_no_file_data_from_either_format() {
        for (image, data) in [
            (iso::minimal_iso(), iso::data_range(&[])),
            (udf::minimal_udf(), udf::data_range(&[])),
        ] {
            let counting = Arc::new(CountingSource::new(Arc::new(MemorySource::new(
                "fixture.iso",
                image,
            ))));
            let counts = counting.counts();
            let index = Iso
                .index(&[counting.clone() as Arc<dyn ByteSource>])
                .await
                .expect("indexed");
            assert_eq!(index.members.len(), 1);
            assert!(
                !counts.read_any_of(data.0, data.1),
                "the index read the file's own data: {:?}",
                counts.ranges()
            );
            assert_eq!(counts.opens(), 0, "an index is read_ats");
            assert!(
                counts.bytes() <= images::INDEX_READ_BUDGET,
                "{} bytes for an index",
                counts.bytes()
            );
        }
    }

    /// What a Blu-ray image answers today: the UDF parser names the
    /// metadata partition map it will not remap through, and that is a
    /// `415` whose sentence says so, not a `422` and not a wrong extent.
    #[tokio::test]
    async fn a_metadata_partition_map_is_unsupported_by_name() {
        let refusal = Iso
            .index(&sources(udf::udf_with_metadata_partition()))
            .await
            .expect_err("refused");
        let Refusal::Unsupported { format, what } = &refusal else {
            panic!("expected Unsupported, got {refusal:?}");
        };
        assert_eq!(*format, "UDF image");
        assert!(what.contains("partition map"), "{what}");
        assert_eq!(refusal.kind(), "unsupported");
        let sentence = refusal.to_string();
        assert!(sentence.starts_with("this UDF image uses"), "{sentence}");
    }

    /// Bytes with neither format's descriptors are a malformed container
    /// -- the `422` -- and the sentence says what was looked for.
    #[tokio::test]
    async fn bytes_that_are_no_image_are_malformed() {
        let refusal = Iso
            .index(&sources(vec![0u8; 600 * 1024]))
            .await
            .expect_err("refused");
        let Refusal::Malformed(detail) = &refusal else {
            panic!("expected Malformed, got {refusal:?}");
        };
        assert!(detail.starts_with("not a disc image"), "{detail}");
        assert!(detail.contains("CD001"), "{detail}");
    }

    /// A source that answers every read with an error.
    struct Failing;

    #[async_trait]
    impl ByteSource for Failing {
        fn len(&self) -> u64 {
            1 << 20
        }

        fn describe(&self) -> String {
            "torrent 0123abcd/broken.iso".to_string()
        }

        async fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("the piece never came"))
        }

        async fn open(
            &self,
            _offset: u64,
            _hint: ReadHint,
        ) -> io::Result<Box<dyn crate::sources::SeekableReader>> {
            Err(io::Error::other("the piece never came"))
        }
    }

    /// A read error is the read error, naming the source the way every
    /// other translator does -- its short name, never a URL -- and the
    /// error's own words.
    #[tokio::test]
    async fn a_read_error_names_the_source_and_the_error() {
        let refusal = Iso
            .index(&[Arc::new(Failing) as Arc<dyn ByteSource>])
            .await
            .expect_err("refused");
        let Refusal::Malformed(detail) = &refusal else {
            panic!("expected Malformed, got {refusal:?}");
        };
        assert!(
            detail.starts_with("torrent 0123abcd/broken.iso could not be read"),
            "{detail}"
        );
        assert!(detail.contains("the piece never came"), "{detail}");
    }

    /// The whole mapping table, in one place: which side of `415`/`422`
    /// each of the images module's refusals lands on.
    #[test]
    fn every_image_refusal_has_a_translator_refusal() {
        assert_eq!(
            translate(images::Refusal::Encrypted { format: "UDF" }, "fixture.iso"),
            Refusal::Encrypted
        );
        assert!(matches!(
            translate(
                images::Refusal::Unsupported {
                    format: "ISO 9660",
                    what: "extended attribute records".into()
                },
                "fixture.iso"
            ),
            Refusal::Unsupported {
                format: "ISO 9660 image",
                ..
            }
        ));
        assert!(matches!(
            translate(
                images::Refusal::Malformed {
                    format: "UDF",
                    detail: "a tag checksum does not match".into()
                },
                "fixture.iso"
            ),
            Refusal::Malformed(detail) if detail == "UDF: a tag checksum does not match"
        ));
        assert!(matches!(
            translate(
                images::Refusal::NotAnImage {
                    detail: "no CD001".into()
                },
                "fixture.iso"
            ),
            Refusal::Malformed(detail) if detail == "not a disc image: no CD001"
        ));
        assert!(matches!(
            translate(
                images::Refusal::Unreadable {
                    detail: "read at 32768 failed".into()
                },
                "fixture.iso"
            ),
            Refusal::Malformed(detail) if detail == "fixture.iso could not be read: read at 32768 failed"
        ));
    }

    /// A disc image is one file: two URLs under `/iso/create` are refused
    /// before a byte is read.
    #[tokio::test]
    async fn two_sources_are_refused_as_not_one_image() {
        let two: Vec<Arc<dyn ByteSource>> = vec![
            Arc::new(MemorySource::new("a.iso", iso::minimal_iso())),
            Arc::new(MemorySource::new("b.iso", iso::minimal_iso())),
        ];
        let refusal = Iso.index(&two).await.expect_err("refused");
        assert!(
            matches!(&refusal, Refusal::Malformed(detail) if detail.contains("one file")),
            "{refusal:?}"
        );
    }
}

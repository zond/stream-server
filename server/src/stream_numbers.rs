//! What this server holds of the stream a player is inside, asked with the
//! URL the player is playing.
//!
//! A client's playback panel wants two rows of numbers about the stream on
//! screen: what our cache holds around the playhead, and -- for a torrent --
//! what we have committed for sharing and what the session has moved. Both
//! rows are readings of a store, and there are two stores: the piece store
//! keyed by info hash ([`enginefs::piece_store`]) and the proxy cache keyed
//! by entity ([`crate::proxy_cache`]). This module is the one interface over
//! them.
//!
//! # One call, and the URL is what dispatches it
//!
//! The client already holds exactly one thing that names the stream: the URL
//! it handed its player. So that is the whole of the question, and the shape
//! of the URL is what decides which store answers it -- a torrent stream is
//! `/{infoHash}/{fileIdx}` (or `/stream/{infoHash}/{fileIdx}`), a proxied one
//! is `/proxy/?d=...`, and both of those shapes are this crate's own routes.
//! `{fileIdx}` includes `-1`, the auto-select this server's own stream and
//! stats routes resolve with the `f=` filters: see [`StreamFile`].
//! Each implementation recognises its own and no other, which is why a URL
//! **neither** recognises is not an error: it is a stream this server does not
//! hold -- a file:// path, another server's URL, an addon's direct link the
//! player fetched itself -- and the honest answer is no rows.
//!
//! # The trait carries no state, and that is the point
//!
//! [`StreamStore`] is an interface, not a layer. Both stores are already
//! correctly keyed by something this server maintains for other reasons, and
//! each implementation answers from a live reading of its own -- a listing of
//! the piece directories, a listing of the chunk directories -- and remembers
//! nothing between calls. A route with bookkeeping of its own would have been
//! a third thing to keep true, and the readings it cached would be claims
//! about a past by the time anybody read them.
//!
//! Nothing here is persisted, for the same reason. **The transfer totals in
//! particular are this session's**, librqbit's own per-torrent counters,
//! which begin at zero when a torrent is added to this process. Conventional
//! BitTorrent clients keep a ratio per torrent across restarts; doing that
//! here would mean storing counters we then have to keep true, and a stored
//! counter read back as an observation is the bug this codebase has shipped
//! more than once. So the ratio is labelled for what it is and the numbers
//! start where the process did.
//!
//! # What absence means
//!
//! Every `None` here is "there is no such number", never "the number is
//! zero", because a client draws no row for the first and a misleading zero
//! for the second:
//!
//! * **no [`StreamNumbers`] at all** -- this server is not holding that
//!   stream;
//! * **no [`StreamNumbers::window`]** -- no retention policy is bounding the
//!   stream (the budget covers it, or none has been published yet), or no
//!   reader has been anywhere inside it in this process. Where nothing is
//!   bounding a stream, what is on the disk is not a window: it is whatever
//!   has been fetched and not yet given back, which is a different
//!   quantity, and one row cannot honestly carry both;
//! * **no [`StreamNumbers::sharing`]** -- a proxied response is not seeded.
//!   There is no swarm, so there is no committed set and no ratio, and the
//!   row is absent rather than a line of zeroes;
//! * **no [`Sharing::committed_bytes`]** -- a torrent with no policy has
//!   promised nothing, whatever it announces;
//! * **no [`Sharing::transfer`]** -- the backend has no counters to read
//!   for this torrent, which for librqbit is any torrent that is not live:
//!   paused, still checking, stopped for space, in error. A torrent that
//!   has moved gigabytes and then paused has not moved nothing, so the
//!   three numbers go absent together rather than reading as a session
//!   that has shared nothing. A [`Sharing`] with neither half is no
//!   sharing row at all.

use enginefs::EngineFS;
use enginefs::backend::TorrentHandle;
use enginefs::retention::CacheWindow;
use url::Url;

use crate::proxy_cache::ProxyCache;
use crate::state::AppState;

/// What one playing stream's stores hold, right now.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamNumbers {
    /// What is on the disk for this stream, split at the playhead. See the
    /// module docs for what its absence means.
    pub window: Option<CacheWindow>,
    /// The sharing row: torrents only.
    pub sharing: Option<Sharing>,
}

/// What a torrent stream has committed and moved.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sharing {
    /// Bytes advertised and promised never to be reclaimed -- the retention
    /// policy's committed set. `None` where no policy is installed.
    pub committed_bytes: Option<u64>,
    /// What the torrent has moved this session, or `None` where the backend
    /// has no counters to read: see [`Transfer`].
    pub transfer: Option<Transfer>,
    /// How many pieces this engine's retention passes asked the backend to
    /// forget and were refused, over the life of the engine.
    ///
    /// **Zero is the only healthy value, and it is here because it was
    /// invisible.** A refusal is librqbit keeping a piece an open stream's
    /// lookahead still covers and this policy's window no longer does: the
    /// disk cannot come back under budget while that stream lives, and
    /// every tick spends a `drop_pieces` to be refused again. It and
    /// [`Transfer::wasted_bytes`] are the two numbers that made a phone
    /// fetching 1.6 GB to play a hundred megabytes legible, and they stay
    /// for that reason.
    ///
    /// `None` for a proxied response, which has no engine and no passes.
    pub refused_reclaims: Option<usize>,
}

/// What a torrent has moved over the connection **in this session**, and
/// the ratio of the two.
///
/// One value rather than three fields beside the committed set, because the
/// three stand or fall together: librqbit keeps these counters in a
/// torrent's live state, so a torrent that is paused, still checking,
/// stopped for space or in error has none to read -- and a torrent that has
/// moved gigabytes and then paused has not moved nothing. The absence is
/// the whole group's, and a client draws no transfer row for it rather than
/// three zeroes that say the opposite.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transfer {
    /// Bytes this torrent has fetched from peers **in this session**.
    pub downloaded_bytes: u64,
    /// Bytes fetched that never became a piece we kept: `fetched` less
    /// librqbit's `downloaded_and_checked_bytes`.
    ///
    /// A few per cent is the endgame duplicating the last pieces of a
    /// download and is normal. A multiple of what was played is a stream
    /// fetching what its own retention pass is deleting, which is the bug
    /// this figure exists to show.
    pub wasted_bytes: u64,
    /// Bytes this torrent has sent to peers **in this session**.
    pub uploaded_bytes: u64,
    /// Uploaded over downloaded, the form every BitTorrent client shows.
    ///
    /// `None` when nothing has been downloaded: a ratio against zero is not
    /// `0.00`, it is undefined, and a client should say so rather than
    /// report a session that has uploaded something as having shared
    /// nothing.
    pub ratio: Option<f64>,
}

impl Sharing {
    /// The sharing row for a torrent whose policy has committed
    /// `committed_bytes` and whose backend reports `transfer`, or `None`
    /// when it can say neither: a row with nothing in it is a row a client
    /// should not draw, and this server saying "no sharing numbers for this
    /// stream" is exactly as true of that torrent as it is of a proxied
    /// response.
    fn of(
        committed_bytes: Option<u64>,
        transfer: Option<enginefs::backend::TransferTotals>,
        refused_reclaims: Option<usize>,
    ) -> Option<Self> {
        let transfer = transfer.map(|transfer| Transfer {
            downloaded_bytes: transfer.fetched,
            wasted_bytes: transfer.wasted(),
            uploaded_bytes: transfer.uploaded,
            ratio: (transfer.fetched > 0)
                .then(|| transfer.uploaded as f64 / transfer.fetched as f64),
        });
        (committed_bytes.is_some() || transfer.is_some() || refused_reclaims.is_some()).then_some(
            Self {
                committed_bytes,
                transfer,
                refused_reclaims,
            },
        )
    }
}

/// A store that can say what it holds of the stream a URL names.
///
/// Implemented once per store, over the URL shape that store's own route
/// defines. It has no state of its own: see the module docs.
#[async_trait::async_trait]
pub trait StreamStore {
    /// The numbers for the stream `url` names, or `None` when this store is
    /// not holding it -- which includes every URL whose shape is not this
    /// store's.
    async fn stream_numbers(&self, url: &Url) -> Option<StreamNumbers>;
}

/// The numbers for the stream `url` names, from whichever store holds it.
///
/// The one call a client makes. `url` is the URL it handed its player,
/// absolute or just the path and query of one -- only the path and the query
/// decide anything here.
pub async fn stream_numbers(state: &AppState, url: &str) -> Option<StreamNumbers> {
    let url = parse(url)?;
    match state.engine.stream_numbers(&url).await {
        Some(numbers) => Some(numbers),
        None => state.proxy_cache.stream_numbers(&url).await,
    }
}

/// A player's URL, absolute or relative.
///
/// A client that stores the path it built rather than the whole URL is
/// asking about the same stream, and the host it would have to invent to be
/// allowed to ask decides nothing: both implementations read the path and
/// the query and nothing else.
fn parse(url: &str) -> Option<Url> {
    match Url::parse(url) {
        Ok(url) => Some(url),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            Url::options().base_url(Some(&base())).parse(url).ok()
        }
        Err(_) => None,
    }
}

fn base() -> Url {
    Url::parse("http://stream-server.invalid/").expect("a literal base URL parses")
}

#[async_trait::async_trait]
impl StreamStore for EngineFS {
    /// `/{infoHash}/{fileIdx}` and its `/stream/` alias -- the two paths
    /// [`crate::stream_routes`] serves a torrent's bytes on.
    ///
    /// `None` for a hash no engine exists for, and for a `-1` that names
    /// no file of it (see [`StreamFile`], which is where `-1` is resolved).
    /// **A peek**: it creates no engine and starts no magnet add, and it
    /// does not count as a poll, so a panel asking every second cannot keep
    /// a torrent out of the idle sweep by looking at it.
    async fn stream_numbers(&self, url: &Url) -> Option<StreamNumbers> {
        let (info_hash, file) = torrent_stream(url)?;
        let file_idx = file.resolve(self, &info_hash).await?;
        let numbers = self.torrent_stream_numbers(&info_hash, file_idx).await?;
        Some(StreamNumbers {
            window: numbers.window,
            sharing: Sharing::of(
                numbers.committed_bytes,
                numbers.transfer,
                Some(numbers.refused_reclaims),
            ),
        })
    }
}

#[async_trait::async_trait]
impl StreamStore for ProxyCache {
    /// `/proxy/?d=...` and the Core path format, read through the same
    /// [`crate::routes::proxy::requested`] the route itself reads them with,
    /// so "which stream is this" is decided in one place.
    ///
    /// Never a [`StreamNumbers::sharing`]: a proxied response is not seeded.
    async fn stream_numbers(&self, url: &Url) -> Option<StreamNumbers> {
        let target = proxied_target(url)?;
        // Off the reactor: the window is counted from a listing of the
        // entity's chunk directories.
        let retention = self.retention().clone();
        let window = tokio::task::spawn_blocking(move || retention.window(target.as_str()))
            .await
            .ok()??;
        Some(StreamNumbers {
            window: Some(window),
            sharing: None,
        })
    }
}

/// The origin a `/proxy` URL names, or `None` for a path of any other shape.
///
/// Read through [`crate::routes::proxy::requested`], the same function the
/// route itself reads its target with, so "which stream is this" is decided
/// in one place: `/proxy` and `/proxy/` are the query format, anything under
/// `/proxy/` the Core path format, exactly as the router splits them.
fn proxied_target(url: &Url) -> Option<Url> {
    let rest = match url.path() {
        "/proxy" | "/proxy/" => None,
        path => Some(path.strip_prefix("/proxy/")?),
    };
    let (_, target) = crate::routes::proxy::requested(rest, url.query())?;
    Some(target)
}

/// The info hash and the file a torrent stream URL names, or `None` for a
/// path of any other shape.
///
/// The hash is spelled as the routes and the registry spell it: forty hex
/// digits, matched case-insensitively and answered in lower case, which is
/// how the engine registry is keyed.
fn torrent_stream(url: &Url) -> Option<(String, StreamFile)> {
    let mut segments: Vec<&str> = url.path_segments()?.collect();
    if segments.first() == Some(&"stream") {
        segments.remove(0);
    }
    let [info_hash, file_idx] = segments[..] else {
        return None;
    };
    if !crate::routes::engine::is_info_hash(info_hash) {
        return None;
    }
    let file = StreamFile::parse(file_idx, || {
        crate::routes::compat::query_values(url.query(), "f")
    })?;
    Some((info_hash.to_lowercase(), file))
}

/// The file half of a torrent stream URL: `{fileIdx}`.
///
/// **`-1` is a file index like any other here, because it is one on the
/// route this dispatch mirrors.** `/{infoHash}/-1` is the documented "pick
/// the file yourself" -- `routes::compat::resolve_file_idx`, the largest
/// video narrowed by the `f=` filters -- and the stream route, the archive
/// route and the sibling control route `/{infoHash}/-1/stats.json` all
/// resolve it that way. A client whose player URL is one of those is
/// playing a file this server is holding, and answering "no rows" for it
/// while `stats.json` answers about the same stream would be this
/// dispatch disagreeing with the route it claims to mirror.
#[derive(Debug, Clone, PartialEq, Eq)]
enum StreamFile {
    /// An index, which names its file with no list to consult.
    Index(usize),
    /// `-1`, with the `f=` filters that narrow it -- the same values
    /// `PlaybackQuery` reads off a stream request and `get_file_stats` off
    /// a stats one.
    Auto(Vec<String>),
}

impl StreamFile {
    /// The `{fileIdx}` segment, with `filters` read only where they are
    /// wanted -- `-1` is the only spelling that consults them.
    fn parse(file_idx: &str, filters: impl FnOnce() -> Vec<String>) -> Option<Self> {
        match file_idx {
            "-1" => Some(Self::Auto(filters())),
            index => Some(Self::Index(index.parse().ok()?)),
        }
    }

    /// Which file of `info_hash` this is, or `None` when it cannot be
    /// decided: no engine exists (so there is no file list, and no stream
    /// either), or the filters match nothing playable.
    ///
    /// **A peek**, like everything else on this path: it looks the engine
    /// up without creating one and without touching its idle clock. An
    /// index needs no lookup at all, so the ordinary URL costs nothing
    /// here; only `-1` asks for the file list, which is the same list the
    /// stream route resolves it against.
    async fn resolve(&self, engines: &EngineFS, info_hash: &str) -> Option<usize> {
        let filters = match self {
            Self::Index(index) => return Some(*index),
            Self::Auto(filters) => filters,
        };
        let engine = engines.peek_engine(info_hash).await?;
        let files = engine.handle.get_files().await;
        let candidates: Vec<_> = files
            .iter()
            .enumerate()
            .map(|(index, file)| crate::routes::compat::FileCandidate {
                index,
                name: file.name.clone(),
                length: file.length,
            })
            .collect();
        Self::auto(&candidates, filters)
    }

    /// The auto-select itself: the route's own function, so the file a
    /// panel is told about is the file the player is being served.
    fn auto(
        candidates: &[crate::routes::compat::FileCandidate],
        filters: &[String],
    ) -> Option<usize> {
        crate::routes::compat::resolve_file_idx("-1", candidates, filters).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_torrent_stream_url_names_its_hash_and_file_in_both_spellings() {
        let hash = "a".repeat(40);
        for url in [
            format!("http://127.0.0.1:11470/{hash}/3"),
            format!("http://127.0.0.1:11470/stream/{hash}/3"),
            format!("/{hash}/3"),
            // The query a player's URL carries is not part of the name.
            format!("http://127.0.0.1:11470/{hash}/3?tr=udp%3A%2F%2Ftracker"),
        ] {
            assert_eq!(
                torrent_stream(&parse(&url).expect("the URL parses")),
                Some((hash.clone(), StreamFile::Index(3))),
                "{url}"
            );
        }
    }

    /// The hash is answered in the case the engine registry is keyed in, or
    /// a client that spelled its URL in upper case gets no rows for a
    /// torrent this server is holding.
    #[test]
    fn an_upper_case_hash_names_the_same_torrent() {
        let url = parse(&format!("http://127.0.0.1:11470/{}/0", "AB".repeat(20)))
            .expect("the URL parses");
        assert_eq!(
            torrent_stream(&url),
            Some(("ab".repeat(20), StreamFile::Index(0)))
        );
    }

    /// **`-1` is a file index this server serves, so it is one this
    /// dispatch answers about.**
    ///
    /// `/{infoHash}/-1?f=...` is the documented auto-select: the stream
    /// route plays it, and `/{infoHash}/-1/stats.json` reports on it. A
    /// client holding that URL is holding the URL of a stream that is
    /// playing, and the panel it is drawing must get the numbers for the
    /// file the player is actually being served -- the same file, picked by
    /// the same function, from the same filters.
    #[test]
    fn the_auto_select_is_a_stream_url_and_the_filters_pick_its_file() {
        let hash = "a".repeat(40);
        let url = parse(&format!(
            "http://127.0.0.1:11470/{hash}/-1?f=%2FS01E02%2Fi&tr=udp%3A%2F%2Ftracker"
        ))
        .expect("the URL parses");
        assert_eq!(
            torrent_stream(&url),
            Some((
                hash.clone(),
                StreamFile::Auto(vec!["/S01E02/i".to_string()])
            )),
            "the `f=` filters come with it, decoded, and nothing else does"
        );

        // And they pick the file the stream route would have played: the
        // season pack's second episode, not the largest file in it.
        let season = |name: &str, index: usize, length: u64| crate::routes::compat::FileCandidate {
            index,
            name: name.to_string(),
            length,
        };
        let pack = [
            season("Show.S01E01.mkv", 0, 900),
            season("Show.S01E02.mkv", 1, 100),
            season("Show.S01E03.mkv", 2, 800),
        ];
        assert_eq!(
            StreamFile::auto(&pack, &["/S01E02/i".to_string()]),
            Some(1),
            "the filter names the episode; without it the biggest file wins"
        );
        assert_eq!(StreamFile::auto(&pack, &[]), Some(0));
    }

    #[test]
    fn a_proxy_url_names_the_origin_the_route_would_have_fetched() {
        let query =
            parse("http://127.0.0.1:11470/proxy/?d=https%3A%2F%2Forigin.example%2Ffilm.mkv&p=abc")
                .expect("the URL parses");
        assert_eq!(
            proxied_target(&query).map(|url| url.to_string()),
            Some("https://origin.example/film.mkv".to_string()),
            "the query format, with the player token left out of the target"
        );

        // The Core path format: the segment before the first slash is the
        // proxy's own parameters, the rest is the file on the origin, and
        // the proxy URL's own query belongs to the target.
        let path = parse(
            "http://127.0.0.1:11470/proxy/d=https%3A%2F%2Forigin.example/dir/film.mkv?token=abc",
        )
        .expect("the URL parses");
        assert_eq!(
            proxied_target(&path).map(|url| url.to_string()),
            Some("https://origin.example/dir/film.mkv?token=abc".to_string())
        );
    }

    #[test]
    fn a_path_that_is_not_a_proxy_url_names_no_origin() {
        for url in [
            "http://127.0.0.1:11470/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/0",
            // The close route is not the stream.
            "http://127.0.0.1:11470/proxy-streams/abc/close",
            // A `/proxy` URL naming no target at all.
            "http://127.0.0.1:11470/proxy/?p=abc",
            // The Core parameter format under another of this server's
            // routes. It parses as a target perfectly well; what makes it
            // not a proxied stream is the path it is under.
            "http://127.0.0.1:11470/ftp/d=https%3A%2F%2Forigin.example/film.mkv",
        ] {
            assert_eq!(
                proxied_target(&parse(url).expect("the URL parses")),
                None,
                "{url}"
            );
        }
    }

    /// The wire shape a client parses: camelCase, like every other response
    /// of this server, and an absent row spelled `null` rather than left
    /// out, so a client that reads a missing key as "the server is old"
    /// still sees the difference between "no rows" and "zero".
    #[test]
    fn the_wire_shape_is_camel_case_with_absences_spelled_null() {
        let numbers = StreamNumbers {
            window: Some(CacheWindow {
                behind_bytes: 1_288_490_188,
                ahead_bytes: 356_515_840,
            }),
            sharing: Sharing::of(
                Some(859_832_320),
                Some(enginefs::backend::TransferTotals {
                    fetched: 4_800,
                    verified: 4_400,
                    uploaded: 2_100,
                }),
                Some(3),
            ),
        };
        assert_eq!(
            serde_json::to_value(numbers).expect("it serializes"),
            serde_json::json!({
                "window": { "behindBytes": 1_288_490_188u64, "aheadBytes": 356_515_840u64 },
                "sharing": {
                    "committedBytes": 859_832_320u64,
                    "transfer": {
                        "downloadedBytes": 4_800,
                        "wastedBytes": 400,
                        "uploadedBytes": 2_100,
                        "ratio": 2_100.0 / 4_800.0,
                    },
                    "refusedReclaims": 3,
                },
            })
        );

        let nothing = StreamNumbers {
            window: None,
            sharing: None,
        };
        assert_eq!(
            serde_json::to_value(nothing).expect("it serializes"),
            serde_json::json!({ "window": null, "sharing": null })
        );
    }

    /// **A ratio against nothing downloaded is not `0.00`.**
    ///
    /// A session that has uploaded bytes and downloaded none -- a torrent
    /// resumed onto a complete file and seeded from it -- has an undefined
    /// ratio, and reporting it as zero tells a viewer they have shared
    /// nothing while they are sharing.
    #[test]
    fn a_ratio_against_nothing_downloaded_is_absent_rather_than_zero() {
        use enginefs::backend::TransferTotals;

        let seeding = Sharing::of(
            None,
            Some(TransferTotals {
                fetched: 0,
                verified: 0,
                uploaded: 2_100,
            }),
            Some(0),
        )
        .expect("a torrent that has moved bytes has a sharing row");
        let seeding = seeding.transfer.expect("its counters were readable");
        assert_eq!(seeding.ratio, None);
        assert_eq!(seeding.uploaded_bytes, 2_100);

        let both = Sharing::of(
            Some(820),
            Some(TransferTotals {
                fetched: 4_800,
                verified: 4_400,
                uploaded: 2_100,
            }),
            Some(0),
        )
        .expect("a sharing row");
        assert_eq!(both.committed_bytes, Some(820));
        assert_eq!(
            both.transfer.expect("its counters were readable").ratio,
            Some(2_100.0 / 4_800.0)
        );
    }

    /// **A torrent whose counters cannot be read has moved what it has
    /// moved, and this must not say it has moved nothing.**
    ///
    /// librqbit keeps them in a torrent's live state, so a torrent that is
    /// paused, still checking, stopped for space or in error has none to
    /// read -- and every one of those is reachable with a panel up: the
    /// fastresume check at the start of every stream, and the idle,
    /// background and free-space arms of the reconciler. Three zeroes there
    /// would tell a viewer their session has shared nothing.
    #[test]
    fn a_torrent_whose_counters_cannot_be_read_reports_no_transfer_at_all() {
        let paused = Sharing::of(Some(820), None, None).expect("its policy still committed bytes");
        assert_eq!(paused.committed_bytes, Some(820));
        assert_eq!(
            paused.transfer, None,
            "absent, and never a zeroed transfer row"
        );

        assert_eq!(
            serde_json::to_value(paused).expect("it serializes"),
            serde_json::json!({
                "committedBytes": 820,
                "transfer": null,
                "refusedReclaims": null
            })
        );

        assert_eq!(
            Sharing::of(None, None, None),
            None,
            "and with no committed set either there is no sharing row to draw"
        );
    }

    #[test]
    fn a_path_that_is_not_a_torrent_stream_names_nothing() {
        for url in [
            // Not a hash.
            "http://127.0.0.1:11470/notahash/0",
            // A hash, but the file index is not one.
            "http://127.0.0.1:11470/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/last",
            // Nor is any other negative one: `-1` is the route's only
            // auto-select, and `resolve_file_idx` refuses the rest.
            "http://127.0.0.1:11470/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/-2",
            // The stats route of the same torrent is not the stream.
            "http://127.0.0.1:11470/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/0/stats.json",
            "http://127.0.0.1:11470/proxy/?d=https%3A%2F%2Forigin.example%2Ffilm.mkv",
            "http://127.0.0.1:11470/",
        ] {
            assert_eq!(
                torrent_stream(&parse(url).expect("the URL parses")),
                None,
                "{url}"
            );
        }
    }
}

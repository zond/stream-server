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
//!   the cleaner has not yet aged out, which is a different quantity, and one
//!   row cannot honestly carry both;
//! * **no [`StreamNumbers::sharing`]** -- a proxied response is not seeded.
//!   There is no swarm, so there is no committed set and no ratio, and the
//!   row is absent rather than a line of zeroes;
//! * **no [`Sharing::committed_bytes`]** -- a torrent with no policy has
//!   promised nothing, whatever it announces.

use enginefs::EngineFS;
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
    /// Bytes this torrent has fetched from peers **in this session**.
    pub downloaded_bytes: u64,
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
    /// `committed_bytes` and which has moved `transfer` this session.
    fn of(committed_bytes: Option<u64>, transfer: enginefs::backend::TransferTotals) -> Self {
        Self {
            committed_bytes,
            downloaded_bytes: transfer.fetched,
            uploaded_bytes: transfer.uploaded,
            ratio: (transfer.fetched > 0)
                .then(|| transfer.uploaded as f64 / transfer.fetched as f64),
        }
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
    /// `None` for a hash no engine exists for. **A peek**: it creates no
    /// engine and starts no magnet add, and it does not count as a poll, so
    /// a panel asking every second cannot keep a torrent out of the idle
    /// sweep by looking at it.
    async fn stream_numbers(&self, url: &Url) -> Option<StreamNumbers> {
        let (info_hash, file_idx) = torrent_stream(url)?;
        let numbers = self.torrent_stream_numbers(&info_hash, file_idx).await?;
        Some(StreamNumbers {
            window: numbers.window,
            sharing: Some(Sharing::of(numbers.committed_bytes, numbers.transfer)),
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

/// The info hash and file index a torrent stream URL names, or `None` for a
/// path of any other shape.
///
/// The hash is spelled as the routes and the registry spell it: forty hex
/// digits, matched case-insensitively and answered in lower case, which is
/// how the engine registry is keyed.
fn torrent_stream(url: &Url) -> Option<(String, usize)> {
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
    Some((info_hash.to_lowercase(), file_idx.parse().ok()?))
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
                Some((hash.clone(), 3)),
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
        assert_eq!(torrent_stream(&url), Some(("ab".repeat(20), 0)));
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
            sharing: Some(Sharing::of(
                Some(859_832_320),
                enginefs::backend::TransferTotals {
                    fetched: 4_800,
                    uploaded: 2_100,
                },
            )),
        };
        assert_eq!(
            serde_json::to_value(numbers).expect("it serializes"),
            serde_json::json!({
                "window": { "behindBytes": 1_288_490_188u64, "aheadBytes": 356_515_840u64 },
                "sharing": {
                    "committedBytes": 859_832_320u64,
                    "downloadedBytes": 4_800,
                    "uploadedBytes": 2_100,
                    "ratio": 2_100.0 / 4_800.0,
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
            TransferTotals {
                fetched: 0,
                uploaded: 2_100,
            },
        );
        assert_eq!(seeding.ratio, None);
        assert_eq!(seeding.uploaded_bytes, 2_100);

        let both = Sharing::of(
            Some(820),
            TransferTotals {
                fetched: 4_800,
                uploaded: 2_100,
            },
        );
        assert_eq!(both.ratio, Some(2_100.0 / 4_800.0));
        assert_eq!(both.committed_bytes, Some(820));
    }

    #[test]
    fn a_path_that_is_not_a_torrent_stream_names_nothing() {
        for url in [
            // Not a hash.
            "http://127.0.0.1:11470/notahash/0",
            // A hash, but the file index is not one.
            "http://127.0.0.1:11470/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/last",
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

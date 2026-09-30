//! **The id registry**: what each [`MediaId`] names, and what resolving it
//! found.
//!
//! `register` parses and records -- no I/O, so it answers at once and the
//! app has its id before it knows whether to wait for a magnet's metadata.
//! `resolve` does the work and keeps its answer on the entry, so the next
//! `resolve` and every `open_reader` after it are a lookup. A resolution
//! that has gone bad under the entry (a Drive grant found dead) is made
//! again at the next ask rather than kept.
//!
//! The entries live in a [`Sessions`] map, the one the archive sessions
//! and the Drive opens live in: at most [`MEDIA_ID_CAP`] of them, the least
//! recently used entry **nobody holds** evicted when a new one takes the
//! map over, and no clock. An open reader holds a lease on its entry
//! (`super::reader`), so an id being read is never the one let go; an id
//! nobody reads is, once enough newer ones are registered, and resolving it
//! then is [`Refusal::UnknownId`] -- which costs the app one `register`.

use super::reader::{self, MediaReader, Source};
use super::{GrantSupplier, MediaId, MediaSpec, PlayToken, Refusal, Resolved};
use crate::routes::compat;
use crate::sources::{ByteSource, DriveSource, Play, ProxySource, TorrentSource};
use crate::state::AppState;
use crate::stream_numbers::{StreamFile, StreamNumbers};
use crate::translators::session::{Lease, Sessions};
use enginefs::backend::TorrentHandle;
use enginefs::backend::priorities::BufferProfile;
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

/// How many ids the registry holds before a new one evicts the least
/// recently used id that no reader holds.
///
/// A backstop, as [`crate::translators::session::SESSION_CAP`] is for the
/// archive sessions: an entry is a parsed URL and, once resolved, a source
/// that holds no bytes of its own, and the app registers what it opens, so
/// sixty-four distinct titles opened with none of them read in between is
/// not a viewing pattern.
pub const MEDIA_ID_CAP: usize = 64;

/// Every id this server has issued and not let go.
#[derive(Clone)]
pub(crate) struct Registry {
    entries: Sessions<Entry>,
}

/// One id: what it names, parsed at registration, and what resolving it
/// found, once something has.
pub(crate) struct Entry {
    /// The parse, or the refusal it ended in: a URL whose shape is none of
    /// this server's is registered all the same (registration does not
    /// fail) and every resolve of it answers why.
    target: Result<Target, Refusal>,
    /// What `resolve` found, kept. The lock is what makes two resolves of
    /// one id one piece of work: the second waits and finds the first's
    /// answer.
    resolution: tokio::sync::Mutex<Option<Arc<Resolution>>>,
    /// The viewer's read-ahead choice for this id, as the last
    /// `open_reader` with a play or [`Registry::set_buffer`] stated it. A
    /// reader applies it at its next reopen.
    pub(super) buffer: std::sync::Mutex<Option<BufferProfile>>,
}

/// What a spec names, read by the routes' own parsers.
enum Target {
    Torrent {
        /// Lowercase.
        info_hash: String,
        file: StreamFile,
        /// The URL's query, for the `tr=` trackers an add is made with
        /// (`compat::get_or_create_engine` reads them lazily, as the route
        /// does).
        query: Option<String>,
    },
    Proxy {
        /// The origin URL `d=` named (and, in the Core path format, the
        /// path after it).
        target: Url,
        /// `h=`, relayed to the origin as `/proxy` relays them.
        request_headers: BTreeMap<String, String>,
        /// `r=Content-Type:...`, which a player would be told.
        content_type: Option<String>,
        /// The registered URL's path and query: what a player is handed,
        /// on this server, for an origin nothing here can seek.
        path_and_query: String,
    },
    Drive {
        file_id: String,
        name: Option<String>,
        grant: GrantSupplier,
    },
}

/// What resolving an id found.
pub(crate) enum Resolution {
    Torrent {
        /// Lowercase.
        info_hash: String,
        file_idx: usize,
        name: String,
        len: u64,
    },
    /// An origin that serves ranges, probed.
    Http {
        source: Arc<ProxySource>,
        target: Url,
        name: String,
        content_type: String,
    },
    /// An origin that answered a ranged probe with the whole entity.
    WillNotRange {
        target: Url,
        proxy_url: String,
        name: String,
        content_type: String,
    },
    Drive {
        source: Arc<DriveSource>,
        media_url: Url,
        name: String,
    },
}

impl Resolution {
    /// Whether this answer has gone bad since it was found, and the next
    /// ask should find another: a Drive source whose grant has died holds a
    /// credential that can only fail, and the grant supplier may hold a
    /// new one.
    fn is_stale(&self) -> bool {
        match self {
            Self::Drive { source, .. } => source.needs_pairing_again(),
            Self::Torrent { .. } | Self::Http { .. } | Self::WillNotRange { .. } => false,
        }
    }

    fn resolved(&self) -> Resolved {
        match self {
            Self::Torrent { name, len, .. } => Resolved {
                name: name.clone(),
                content_type: crate::routes::stream::content_type_for_name(name).to_string(),
                len: *len,
                member: None,
                in_process: true,
                proxy_url: None,
            },
            Self::Http {
                source,
                name,
                content_type,
                ..
            } => Resolved {
                name: name.clone(),
                content_type: content_type.clone(),
                len: source.len(),
                member: None,
                in_process: true,
                proxy_url: None,
            },
            Self::WillNotRange {
                proxy_url,
                name,
                content_type,
                ..
            } => Resolved {
                name: name.clone(),
                content_type: content_type.clone(),
                len: 0,
                member: None,
                in_process: false,
                proxy_url: Some(proxy_url.clone()),
            },
            Self::Drive { source, name, .. } => Resolved {
                name: name.clone(),
                content_type: source.content_type().to_string(),
                len: source.len(),
                member: None,
                in_process: true,
                proxy_url: None,
            },
        }
    }
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            entries: Sessions::new(MEDIA_ID_CAP),
        }
    }

    /// Parse `spec` and record it under a fresh id. No I/O.
    pub(crate) fn register(&self, spec: MediaSpec) -> anyhow::Result<MediaId> {
        let id = MediaId::random()?;
        let target = match spec {
            MediaSpec::StreamingUrl(url) => parse(&url),
            MediaSpec::Drive {
                file_id,
                name,
                grant,
            } => Ok(Target::Drive {
                file_id,
                name,
                grant,
            }),
        };
        drop(self.entries.insert(
            id.as_str().to_string(),
            Entry {
                target,
                resolution: tokio::sync::Mutex::new(None),
                buffer: std::sync::Mutex::new(None),
            },
        ));
        Ok(id)
    }

    fn entry(&self, id: &MediaId) -> Result<Lease<Entry>, Refusal> {
        self.entries.get(id.as_str()).ok_or(Refusal::UnknownId)
    }

    /// Resolve `id`, or answer what resolving it found before.
    pub(crate) async fn resolve(
        &self,
        state: &AppState,
        id: &MediaId,
    ) -> Result<Resolved, Refusal> {
        let entry = self.entry(id)?;
        Ok(entry.resolution(state).await?.resolved())
    }

    /// Record the viewer's read-ahead choice for `id`; a reader open on it
    /// applies it at its next reopen.
    pub(crate) fn set_buffer(&self, id: &MediaId, buffer: BufferProfile) -> Result<(), Refusal> {
        let entry = self.entry(id)?;
        *entry
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(buffer);
        Ok(())
    }

    /// What `id` resolved to, if it has been and nothing is resolving it
    /// now. **A peek**: it resolves nothing and waits for nothing, so the
    /// questions asked by id -- a panel's numbers, a player's reports --
    /// never become the I/O `resolve` is.
    fn peek(&self, id: &MediaId) -> Option<Arc<Resolution>> {
        let entry = self.entries.get(id.as_str())?;
        entry.resolution.try_lock().ok()?.clone()
    }

    /// [`crate::stream_numbers::stream_numbers`] for what `id` resolved to.
    pub(crate) async fn stream_numbers(
        &self,
        state: &AppState,
        id: &MediaId,
    ) -> Option<StreamNumbers> {
        match &*self.peek(id)? {
            Resolution::Torrent {
                info_hash,
                file_idx,
                ..
            } => {
                crate::stream_numbers::stream_numbers(state, &format!("/{info_hash}/{file_idx}"))
                    .await
            }
            Resolution::Http { target, .. } | Resolution::WillNotRange { target, .. } => {
                crate::stream_numbers::proxied_numbers(&state.proxy_cache, target.clone()).await
            }
            Resolution::Drive { media_url, .. } => {
                crate::stream_numbers::proxied_numbers(&state.proxy_cache, media_url.clone()).await
            }
        }
    }

    /// The torrent file `id` resolved to, for the reports a player makes
    /// about one (`note_duration`, `note_player_opened`,
    /// `note_player_stalled`). `None` for anything else, which those reports
    /// are not about, and for an id not resolved yet.
    pub(crate) fn torrent_file(&self, id: &MediaId) -> Option<(String, usize)> {
        match &*self.peek(id)? {
            Resolution::Torrent {
                info_hash,
                file_idx,
                ..
            } => Some((info_hash.clone(), *file_idx)),
            _ => None,
        }
    }

    /// A reader over what `id` names, resolving it first if nothing has.
    /// With a play, its reads are the viewer's playback; without one they
    /// are an aside, which moves no play session and shares nothing.
    ///
    /// **The stream is registered here**, before this returns, and not at
    /// the first read: the reconciler is told a read is about to start
    /// while there is still time for it to start a torrent an earlier pass
    /// stopped, and a viewer's play session is where the viewer is from the
    /// moment their player opened.
    pub(crate) async fn open_reader(
        &self,
        state: &AppState,
        id: &MediaId,
        play: Option<PlayToken>,
    ) -> Result<MediaReader, Refusal> {
        let entry = self.entry(id)?;
        let resolution = entry.resolution(state).await?;
        if let Some(play) = &play {
            *entry
                .buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(play.buffer);
        }
        let source = match &*resolution {
            Resolution::Torrent {
                info_hash,
                file_idx,
                name,
                ..
            } => {
                // Found or added again: the idle sweep may have removed the
                // engine since the resolve, and the stream route asks on
                // every request for the same reason.
                if let Ok(Target::Torrent { query, .. }) = &entry.target {
                    compat::get_or_create_engine(&state.engine, info_hash, query.as_deref())
                        .await
                        .map_err(|error| Refusal::TorrentUnavailable(error.client_message()))?;
                }
                let source = match play {
                    Some(play) => {
                        TorrentSource::played(
                            state,
                            info_hash,
                            *file_idx,
                            Play {
                                token: play.token,
                                buffer: play.buffer,
                                // Today's rule, decided here and never by
                                // the app: archive playback shares nothing.
                                shares: !crate::routes::stream::played_through_a_translator(name),
                            },
                        )
                        .await
                    }
                    None => TorrentSource::aside(state.engine.clone(), info_hash, *file_idx).await,
                }
                .map_err(|error| Refusal::OpenFailed(error.to_string()))?;
                Source::Torrent(Box::new(source))
            }
            Resolution::Http { source, .. } => {
                if let Some(play) = &play {
                    // What a `p=` request through `/proxy` does: the
                    // viewer's player is on a proxied stream now, so its
                    // session is off every torrent file, and what the
                    // player reads is read ahead of.
                    state.engine.note_player(
                        &play.token,
                        enginefs::retention::sessions::Played::Elsewhere,
                    );
                    read_ahead(state, source);
                }
                Source::Shared(source.clone())
            }
            Resolution::WillNotRange { .. } => return Err(Refusal::NoRanges),
            Resolution::Drive { source, .. } => {
                if let Some(play) = &play {
                    state.engine.note_player(
                        &play.token,
                        enginefs::retention::sessions::Played::Elsewhere,
                    );
                }
                // Every `/drive/stream` request registers the file for
                // read-ahead, and so does every reader.
                if let Some(key_dir) = source.key_dir() {
                    state
                        .proxy_cache
                        .retention()
                        .note_source(key_dir, Arc::new(source.filling_source()));
                }
                Source::Shared(source.clone())
            }
        };
        reader::open(entry, source).await
    }
}

/// Register `source`'s entity for read-ahead, as `/proxy` does for a
/// request carrying `p=`: quiet, since the passes' fetches are not a
/// viewer's.
fn read_ahead(state: &AppState, source: &ProxySource) {
    if let Some(key_dir) = source.key_dir() {
        state
            .proxy_cache
            .retention()
            .note_source(key_dir, Arc::new(source.for_filling()));
    }
}

impl Entry {
    /// What this id resolves to: kept from before, or found now.
    async fn resolution(&self, state: &AppState) -> Result<Arc<Resolution>, Refusal> {
        let target = self.target.as_ref().map_err(Clone::clone)?;
        let mut held = self.resolution.lock().await;
        if let Some(resolution) = held.as_ref()
            && !resolution.is_stale()
        {
            return Ok(resolution.clone());
        }
        let resolution = Arc::new(resolve(state, target).await?);
        *held = Some(resolution.clone());
        Ok(resolution)
    }
}

/// Read a streaming URL by its shape, with the routes' own parsers:
/// `torrent_stream` is the torrent route's `/{infoHash}/{fileIdx}` (and
/// `/stream/...`), `-1` with its `f=` filters included; `requested` is
/// `/proxy`'s own reading of `d=`, `h=` and `r=` in both spellings.
fn parse(url: &Url) -> Result<Target, Refusal> {
    if let Some((info_hash, file)) = crate::stream_numbers::torrent_stream(url) {
        return Ok(Target::Torrent {
            info_hash,
            file,
            query: url.query().map(str::to_string),
        });
    }
    let proxy_rest = match url.path() {
        "/proxy" | "/proxy/" => Some(None),
        path => path.strip_prefix("/proxy/").map(Some),
    };
    if let Some(rest) = proxy_rest {
        let (params, target) =
            crate::routes::proxy::requested(rest, url.query()).ok_or(Refusal::UnrecognisedUrl)?;
        let content_type = params
            .response_headers()
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map(|(_, value)| value.clone());
        let path_and_query = match url.query() {
            Some(query) => format!("{}?{query}", url.path()),
            None => url.path().to_string(),
        };
        return Ok(Target::Proxy {
            target,
            request_headers: params.request_headers().clone(),
            content_type,
            path_and_query,
        });
    }
    match url.path_segments().and_then(|mut segments| segments.next()) {
        Some("rar" | "zip" | "7zip" | "tar" | "tgz" | "iso") => Err(Refusal::NotYet {
            what: "a file inside an archive",
        }),
        Some("ftp") => Err(Refusal::NotYet {
            what: "a file on an FTP server",
        }),
        _ => Err(Refusal::UnrecognisedUrl),
    }
}

/// The work `resolve` does, once per id: add or find the torrent and
/// choose its file; probe the origin; renew the Drive grant.
async fn resolve(state: &AppState, target: &Target) -> Result<Resolution, Refusal> {
    match target {
        Target::Torrent {
            info_hash,
            file,
            query,
        } => {
            let engine = compat::get_or_create_engine(&state.engine, info_hash, query.as_deref())
                .await
                .map_err(|error| {
                    tracing::warn!(info_hash, %error, "a media id's torrent could not be added");
                    Refusal::TorrentUnavailable(error.client_message())
                })?;
            let files = engine.handle.get_files().await;
            let candidates = compat::candidates(&files);
            let (requested, filters) = match file {
                StreamFile::Index(index) => (index.to_string(), &[][..]),
                StreamFile::Auto(filters) => ("-1".to_string(), filters.as_slice()),
            };
            let file_idx = compat::resolve_file_idx(&requested, &candidates, filters)
                .map_err(Refusal::NoSuchFile)?;
            let file = files
                .get(file_idx)
                .ok_or_else(|| Refusal::NoSuchFile("File not found".to_string()))?;
            Ok(Resolution::Torrent {
                info_hash: info_hash.clone(),
                file_idx,
                name: file.name.clone(),
                len: file.length,
            })
        }
        Target::Proxy {
            target,
            request_headers,
            content_type,
            path_and_query,
        } => {
            let name = link_name(target);
            match ProxySource::open(
                state.proxy_cache.clone(),
                state.http_addr,
                target.clone(),
                request_headers.clone(),
            )
            .await
            {
                Ok(source) => Ok(Resolution::Http {
                    content_type: content_type
                        .clone()
                        .unwrap_or_else(|| source.content_type().to_string()),
                    source: Arc::new(source),
                    target: target.clone(),
                    name,
                }),
                Err(crate::sources::proxy::ProxySourceError::WillNotRange) => {
                    Ok(Resolution::WillNotRange {
                        target: target.clone(),
                        proxy_url: format!("{}{path_and_query}", state.base_url),
                        content_type: content_type.clone().unwrap_or_else(|| {
                            mime_guess::from_path(&name)
                                .first_or_octet_stream()
                                .to_string()
                        }),
                        name,
                    })
                }
                Err(error) => Err(Refusal::of_probe(error)),
            }
        }
        Target::Drive {
            file_id,
            name,
            grant,
        } => {
            let endpoints = state.drive.as_ref().ok_or(Refusal::NoPairingService)?;
            let refresh_token = grant().ok_or(Refusal::NoGrant)?;
            let pairing = endpoints.pairing(file_id, &refresh_token);
            let media_url = pairing.media_url().map_err(Refusal::of_drive)?;
            let source = DriveSource::open(state, pairing)
                .await
                .map_err(Refusal::of_drive)?;
            let name = name
                .clone()
                .unwrap_or_else(|| "Google Drive file".to_string());
            tracing::info!(file = %file_id, length = source.len(), "a media id's Drive file resolved");
            Ok(Resolution::Drive {
                source: Arc::new(source.named(name.clone())),
                media_url,
                name,
            })
        }
    }
}

/// What to call a link: its last path segment, decoded, or its host.
fn link_name(target: &Url) -> String {
    target
        .path_segments()
        .and_then(|mut segments| segments.rfind(|segment| !segment.is_empty()))
        .map(|segment| {
            urlencoding::decode(segment)
                .map(|decoded| decoded.into_owned())
                .unwrap_or_else(|_| segment.to_string())
        })
        .or_else(|| target.host_str().map(str::to_string))
        .unwrap_or_else(|| "link".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(url: &str) -> Result<Target, Refusal> {
        parse(&Url::parse(url).expect("a literal URL"))
    }

    #[test]
    fn a_torrent_url_is_read_as_the_stream_route_reads_it() {
        let hash = "AB".repeat(20);
        let Ok(Target::Torrent {
            info_hash,
            file,
            query,
        }) = parsed(&format!(
            "http://127.0.0.1:1/{hash}/-1?tr=udp%3A%2F%2Fone&f=film"
        ))
        else {
            panic!("not a torrent");
        };
        assert_eq!(info_hash, hash.to_lowercase());
        assert_eq!(file, StreamFile::Auto(vec!["film".to_string()]));
        assert_eq!(query.as_deref(), Some("tr=udp%3A%2F%2Fone&f=film"));
    }

    #[test]
    fn a_proxy_url_is_read_as_the_proxy_route_reads_it() {
        let Ok(Target::Proxy {
            target,
            request_headers,
            content_type,
            path_and_query,
        }) = parsed(
            "http://127.0.0.1:1/proxy/?d=https%3A%2F%2Fcdn.example%2Fa%2FThe%2520Film.mkv\
             &h=Referer%3Ahttps%3A%2F%2Fsite&r=Content-Type%3Avideo%2Fwebm",
        )
        else {
            panic!("not a proxy URL");
        };
        assert_eq!(target.as_str(), "https://cdn.example/a/The%20Film.mkv");
        assert_eq!(
            request_headers.get("Referer").map(String::as_str),
            Some("https://site")
        );
        assert_eq!(content_type.as_deref(), Some("video/webm"));
        assert!(path_and_query.starts_with("/proxy/?d="));
        assert_eq!(link_name(&target), "The Film.mkv");
    }

    /// Recognised, and refused for now: the archive and FTP forms are a
    /// later step, and saying so beats calling them unknown.
    #[test]
    fn an_archive_create_and_an_ftp_url_are_not_yet_playable_by_id() {
        for url in [
            "http://127.0.0.1:1/rar/create?lz=abc",
            "http://127.0.0.1:1/zip/create/key",
            "http://127.0.0.1:1/ftp/ftp%3A%2F%2Fhost%2Ffilm.mkv",
        ] {
            assert_eq!(
                parsed(url).err().map(|refusal| refusal.kind()),
                Some("notYet"),
                "{url}"
            );
        }
        assert_eq!(
            parsed("http://127.0.0.1:1/settings")
                .err()
                .map(|refusal| refusal.kind()),
            Some("unrecognisedUrl")
        );
    }
}

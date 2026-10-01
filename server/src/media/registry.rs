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
use super::sniff::{self, Sniffed};
use super::{GrantSupplier, MediaId, MediaSpec, PlayToken, Refusal, Resolved};
use crate::routes::archive::{self, ArchiveCreateRequest, Format};
use crate::routes::compat;
use crate::sources::held::HeldSource;
use crate::sources::local::{LocalFile, LocalSource};
use crate::sources::{ByteSource, DriveSource, Play, ProxySource, ReadHint, TorrentSource};
use crate::state::AppState;
use crate::stream_numbers::{StreamFile, StreamNumbers};
use crate::translators::session::{Lease, SessionSources, Sessions, TranslatedSession};
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
    /// How long `resolve` waits for a file's head before answering it
    /// unsniffed: [`sniff::SNIFF_BOUND`], or what a test set
    /// ([`Registry::set_sniff_bound`]). Milliseconds.
    sniff_bound: Arc<std::sync::atomic::AtomicU64>,
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
    /// The viewer's read-ahead choice for this id, as
    /// [`Registry::set_buffer`] last stated it; `None` until it has. It
    /// outranks a [`PlayToken`]'s buffer, which is only the initial value:
    /// a reader opens with this one when there is one, and applies it at
    /// every reopen.
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
    /// A file on this device, not opened until `resolve`.
    Local {
        file: LocalFile,
        name: Option<String>,
    },
    /// A member of a container, by the archive routes' own URLs: a
    /// `/{fmt}/create` (the container behind links) or a
    /// `/{fmt}/stream/{key}[/member]` (the `torrent:` form, or a session a
    /// create already made).
    Member {
        format: Format,
        container: Container,
        /// The member the URL names, or `None` for the one the create's
        /// rule picks (`routes::archive::chosen_member`).
        member: Option<String>,
        /// `fileIdx` and `fileMustInclude` (or `f`) of a `/stream/{key}`
        /// with no member, as the route's redirect reads them.
        file_idx: Option<usize>,
        file_must_include: Vec<String>,
    },
}

/// Where a member's container session comes from.
#[derive(Clone)]
pub(crate) enum Container {
    /// A `/{fmt}/create`, its `lz` payload parsed as the route parses it,
    /// under the key the URL names (`/create/{key}`) or one of the id's own.
    Create {
        key: Option<String>,
        payload: ArchiveCreateRequest,
    },
    /// A session key: `torrent:<info hash>/<path>`, which is indexed on
    /// first use, or one a create made.
    Key(String),
}

/// What resolving an id found.
///
/// A plain file's `sniffed` says whether its head was read and found to be
/// no container's (`super::sniff`); one whose head was a container's is a
/// [`Resolution::Member`] instead.
pub(crate) enum Resolution {
    Torrent {
        /// Lowercase.
        info_hash: String,
        file_idx: usize,
        name: String,
        len: u64,
        sniffed: bool,
    },
    /// An origin that serves ranges, probed.
    Http {
        source: Arc<ProxySource>,
        target: Url,
        name: String,
        content_type: String,
        sniffed: bool,
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
        sniffed: bool,
    },
    /// A pinned download of the link or the Drive file, whole on the disk:
    /// read off it, with no origin asked -- what plays it offline.
    Held {
        source: Arc<HeldSource>,
        /// What the proxy cache files the entity's reads under: the link,
        /// or the Drive file's media URL.
        target: Url,
        name: String,
        sniffed: bool,
    },
    /// A file on this device, open.
    Local {
        source: Arc<LocalSource>,
        sniffed: bool,
    },
    /// A member of a container. The session is found again at each open
    /// ([`member_session`]), not kept here: a session no reader leases is
    /// the archive map's to let go when the viewer moves on, and made again
    /// from this -- a key, and for a link-borne container its create.
    Member {
        format: Format,
        /// The session's key in the archive map.
        key: String,
        /// The create that makes the session again, for a container behind
        /// links; `None` for the `torrent:` form, whose key is enough.
        create: Option<ArchiveCreateRequest>,
        /// The member's path inside its container.
        name: String,
        len: u64,
        /// The torrent the container is in and its volumes' file indices,
        /// in set order; `None` for a container behind links.
        torrent: Option<(String, Vec<usize>)>,
        /// The link, Drive file or file on this device the container was
        /// found in by its first bytes, which makes the session again;
        /// `None` for an archive URL and for a torrent's file, whose key
        /// is enough.
        sniffed_in: Option<Sniffed>,
    },
}

impl Resolution {
    /// Whether this answer has gone bad since it was found, and the next
    /// ask should find another: a Drive source whose grant has died holds a
    /// credential that can only fail, and the grant supplier may hold a
    /// new one.
    ///
    /// A held download is stale once it is no longer pinned: its bytes are
    /// no longer kept, and the origin has to be asked again.
    fn is_stale(&self, state: &AppState) -> bool {
        match self {
            Self::Drive { source, .. } => source.needs_pairing_again(),
            Self::Held { source, .. } => !state.proxy_cache.retention().is_pinned(source.key_dir()),
            Self::Member {
                sniffed_in: Some(sniffed),
                ..
            } => sniffed.is_stale(state),
            Self::Torrent { .. }
            | Self::Http { .. }
            | Self::WillNotRange { .. }
            | Self::Member { .. }
            | Self::Local { .. } => false,
        }
    }

    fn resolved(&self) -> Resolved {
        match self {
            Self::Torrent {
                name, len, sniffed, ..
            } => Resolved {
                name: name.clone(),
                content_type: crate::routes::stream::content_type_for_name(name).to_string(),
                len: *len,
                member: None,
                in_process: true,
                proxy_url: None,
                sniffed: *sniffed,
            },
            Self::Http {
                source,
                name,
                content_type,
                sniffed,
                ..
            } => Resolved {
                name: name.clone(),
                content_type: content_type.clone(),
                len: source.len(),
                member: None,
                in_process: true,
                proxy_url: None,
                sniffed: *sniffed,
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
                // Nothing here can read its head without reading it all.
                sniffed: false,
            },
            Self::Held {
                source,
                name,
                sniffed,
                ..
            } => Resolved {
                name: name.clone(),
                content_type: source.content_type().to_string(),
                len: source.len(),
                member: None,
                in_process: true,
                proxy_url: None,
                sniffed: *sniffed,
            },
            Self::Drive {
                source,
                name,
                sniffed,
                ..
            } => Resolved {
                name: name.clone(),
                content_type: source.content_type().to_string(),
                len: source.len(),
                member: None,
                in_process: true,
                proxy_url: None,
                sniffed: *sniffed,
            },
            Self::Local { source, sniffed } => Resolved {
                name: source.name().to_string(),
                content_type: source.content_type().to_string(),
                len: source.len(),
                member: None,
                in_process: true,
                proxy_url: None,
                sniffed: *sniffed,
            },
            Self::Member { name, len, .. } => Resolved {
                name: name.clone(),
                content_type: mime_guess::from_path(name)
                    .first_or_octet_stream()
                    .to_string(),
                len: *len,
                member: Some(super::MemberInfo {
                    name: name.clone(),
                    len: *len,
                }),
                in_process: true,
                proxy_url: None,
                // The URL named the container, or its head did.
                sniffed: true,
            },
        }
    }

    /// The torrent files a member lies in, when it lies in a torrent: its
    /// one container file, or every volume of its multi-volume set. What
    /// the film's duration is told to, for the member's draw alone
    /// ([`enginefs::EngineFS::on_set_duration`]): a container's length over
    /// an episode's duration, or a volume's, is no rate of anything, so no
    /// file's stream takes one from it.
    fn member_files(&self) -> Option<(String, Vec<usize>)> {
        match self {
            Self::Member {
                torrent: Some((info_hash, files)),
                ..
            } if !files.is_empty() => Some((info_hash.clone(), files.clone())),
            _ => None,
        }
    }

    /// The one torrent file a member's container is, when it is one: what
    /// its play session is on, and what a player's reports about playing
    /// are about. `None` for a set and for a container behind links.
    fn container_file(&self) -> Option<(String, usize)> {
        match self {
            Self::Member {
                torrent: Some((info_hash, files)),
                ..
            } => match files.as_slice() {
                [file_idx] => Some((info_hash.clone(), *file_idx)),
                _ => None,
            },
            _ => None,
        }
    }
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            entries: Sessions::new(MEDIA_ID_CAP),
            sniff_bound: Arc::new(std::sync::atomic::AtomicU64::new(
                sniff::SNIFF_BOUND.as_millis() as u64,
            )),
        }
    }

    /// How long `resolve` waits for a file's head before it answers the
    /// file unsniffed.
    pub(crate) fn sniff_bound(&self) -> std::time::Duration {
        std::time::Duration::from_millis(
            self.sniff_bound.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Wait `bound` for a head instead: a test's, so a head that never
    /// comes is proven not to hold a resolve without spending the real
    /// bound.
    pub(crate) fn set_sniff_bound(&self, bound: std::time::Duration) {
        self.sniff_bound.store(
            bound.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
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
            MediaSpec::Local { file, name } => Ok(Target::Local { file, name }),
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
            Resolution::Http { target, .. }
            | Resolution::WillNotRange { target, .. }
            | Resolution::Held { target, .. } => {
                crate::stream_numbers::proxied_numbers(&state.proxy_cache, target.clone()).await
            }
            Resolution::Drive { media_url, .. } => {
                crate::stream_numbers::proxied_numbers(&state.proxy_cache, media_url.clone()).await
            }
            // Nothing is fetched or shared: no numbers to show.
            Resolution::Local { .. } => None,
            resolution @ Resolution::Member { .. } => {
                let (info_hash, file_idx) = resolution.container_file()?;
                crate::stream_numbers::stream_numbers(state, &format!("/{info_hash}/{file_idx}"))
                    .await
            }
        }
    }

    /// Whether the entity `id` resolved to has a source registered for
    /// read-ahead: a probe for the test that a played reader turns it on.
    pub(crate) fn read_ahead_registered(&self, state: &AppState, id: &MediaId) -> bool {
        let Some(resolution) = self.peek(id) else {
            return false;
        };
        let registered = |dir: Option<std::path::PathBuf>| {
            dir.is_some_and(|dir| state.proxy_cache.retention().has_source(&dir))
        };
        match &*resolution {
            Resolution::Http { source, .. } => registered(source.key_dir()),
            Resolution::Drive { source, .. } => registered(source.key_dir()),
            Resolution::Member { key, .. } => {
                state
                    .translated_archives
                    .get(key)
                    .is_some_and(|session| match session.sources() {
                        SessionSources::Held(sources) => {
                            sources.iter().any(|source| registered(source.key_dir()))
                        }
                        SessionSources::Torrent { .. } | SessionSources::Kept(_) => false,
                    })
            }
            _ => false,
        }
    }

    /// The torrent file `id` resolved to, for the reports a player makes
    /// about one (`note_duration`, `note_player_opened`,
    /// `note_player_stalled`). `None` for anything else, which those reports
    /// are not about, and for an id not resolved yet.
    ///
    /// For a member of a single-file container in a torrent, that file --
    /// though not for the film's duration, which is the member's and goes
    /// to [`Self::member_files`].
    pub(crate) fn torrent_file(&self, id: &MediaId) -> Option<(String, usize)> {
        match &*self.peek(id)? {
            Resolution::Torrent {
                info_hash,
                file_idx,
                ..
            } => Some((info_hash.clone(), *file_idx)),
            resolution @ Resolution::Member { .. } => resolution.container_file(),
            _ => None,
        }
    }

    /// The torrent files the member `id` resolved to lies in -- its one
    /// container file, or its set's volumes -- for the film's duration
    /// (`note_media_duration`), which they take for the member's draw
    /// alone. `None` for anything else, and for an id not resolved yet.
    pub(crate) fn member_files(&self, id: &MediaId) -> Option<(String, Vec<usize>)> {
        self.peek(id)?.member_files()
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
        let (entry, source) = self.open_source(state, id, play).await?;
        reader::open(entry, source).await
    }

    /// A lease on `id`'s entry, so it is not evicted while it is held: what
    /// a published cast holds for as long as it is published
    /// (`crate::cast`), as an open reader does.
    pub(crate) fn lease(&self, id: &MediaId) -> Result<Lease<Entry>, Refusal> {
        self.entry(id)
    }

    /// The source a reader over `id` reads, resolving it first if nothing
    /// has, with the lease on its entry: [`Self::open_reader`]'s open
    /// without the reader task, for a caller that reads it on the runtime
    /// itself -- a cast body (`crate::cast`). The same play rules, and the
    /// stream registered by the source's first open, ended by its drop.
    pub(crate) async fn open_source(
        &self,
        state: &AppState,
        id: &MediaId,
        play: Option<PlayToken>,
    ) -> Result<(Lease<Entry>, Source), Refusal> {
        let entry = self.entry(id)?;
        let resolution = entry.resolution(state).await?;
        // `set_buffer` outranks the token's buffer, which is only where a
        // playback starts when nobody has said otherwise for this id.
        let set_buffer = *entry
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
                    torrent_engine(state, info_hash, query.as_deref()).await?;
                }
                let source = match play {
                    Some(play) => {
                        TorrentSource::played(
                            state,
                            info_hash,
                            *file_idx,
                            Play {
                                token: play.token,
                                buffer: set_buffer.unwrap_or(play.buffer),
                                // Decided here and never by the app: a
                                // container played as itself shares
                                // nothing. Its member played through an id
                                // is the member path below, which does.
                                shares: !crate::routes::stream::played_through_a_translator(name),
                                member: None,
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
            Resolution::Held { source, .. } => {
                if let Some(play) = &play {
                    state.engine.note_player(
                        &play.token,
                        enginefs::retention::sessions::Played::Elsewhere,
                    );
                }
                // No read-ahead: every byte is here already.
                Source::Shared(source.clone())
            }
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
            Resolution::Local { source, .. } => {
                // The viewer's player is on a file of this device's now,
                // which is no torrent file: its session goes to
                // `Elsewhere`, as a `p=` request through `/proxy` puts it,
                // so a torrent it played before is left and goes slack.
                // Nothing is read ahead: every byte is here.
                if let Some(play) = &play {
                    state.engine.note_player(
                        &play.token,
                        enginefs::retention::sessions::Played::Elsewhere,
                    );
                }
                Source::Shared(source.clone())
            }
            Resolution::Member {
                format,
                key,
                create,
                name,
                sniffed_in,
                ..
            } => {
                let session =
                    member_session(state, *format, key, create.as_ref(), sniffed_in.as_ref())
                        .await?;
                open_member(state, session, name, play, set_buffer).await?
            }
        };
        Ok((entry, source))
    }
}

/// The session a member's container is in, leased: found under its key, or
/// made again -- indexed off the torrent's file for the `torrent:` form, the
/// create run again for a container behind links, the file a sniff found it
/// in indexed again.
async fn member_session(
    state: &AppState,
    format: Format,
    key: &str,
    create: Option<&ArchiveCreateRequest>,
    sniffed_in: Option<&Sniffed>,
) -> Result<Lease<TranslatedSession>, Refusal> {
    if let Some(sniffed) = sniffed_in {
        if let Some(session) = state.translated_archives.get(key) {
            return Ok(session);
        }
        return sniffed.index_again(state, format, key).await;
    }
    let translator = archive::translator_for(format).map_err(Refusal::of_session)?;
    if let Some(payload) = create {
        if let Some(session) = state.translated_archives.get(key) {
            return Ok(session);
        }
        return archive::create_session(state, translator.as_ref(), key.to_string(), payload)
            .await
            .map(|(session, _)| session)
            .map_err(Refusal::of_session);
    }
    archive::session_for(state, translator.as_ref(), key)
        .await
        .map_err(Refusal::of_session)
}

/// A reader's source over the member `name` of `session`'s container
/// (`docs/design/media-pipeline.md` §2.8), holding the session's lease for
/// the reader's life.
///
/// **What it shares.** With a play, a member of a **single-file container
/// in a torrent** is the viewer's playback of that file: its source is
/// played, with the member's byte extent, so the play session is on the
/// container file, draws, and draws inside the member. A member of a
/// **multi-volume set** is played across it: every volume is opened
/// played, sharing, with the member's bytes in each, so the play session is
/// on the set -- one thing played, which the reader crossing a volume
/// boundary does not move -- and the set draws once, over the member's
/// bytes in all of its volumes. A container **behind links** is a proxied entity: the session goes off
/// every torrent file and what the player reads is read ahead of, as for a
/// `/proxy` id. Without a play every volume is an aside, and nothing moves.
async fn open_member(
    state: &AppState,
    session: Lease<TranslatedSession>,
    name: &str,
    play: Option<PlayToken>,
    set_buffer: Option<BufferProfile>,
) -> Result<Source, Refusal> {
    let member = session
        .member(name)
        .ok_or_else(|| Refusal::NoSuchFile(format!("the archive holds no member {name}")))?
        .clone();
    let crate::translators::Body::Direct(extents) = &member.body else {
        let crate::translators::Body::Opaque(refusal) = member.body else {
            unreachable!("a body is direct or opaque")
        };
        return Err(Refusal::Translated(refusal));
    };
    let mut sources: Vec<Arc<dyn ByteSource>> = Vec::new();
    let mut played: Vec<Arc<TorrentSource>> = Vec::new();
    match session.sources() {
        SessionSources::Held(held) => {
            if let Some(play) = &play {
                state.engine.note_player(
                    &play.token,
                    enginefs::retention::sessions::Played::Elsewhere,
                );
                for source in held {
                    read_ahead(state, source);
                }
            }
            sources.extend(
                held.iter()
                    .map(|source| source.clone() as Arc<dyn ByteSource>),
            );
        }
        SessionSources::Kept(kept) => {
            // On this device already: the player is off every torrent
            // file, as for the file played as itself, and nothing is read
            // ahead of.
            if let Some(play) = &play {
                state.engine.note_player(
                    &play.token,
                    enginefs::retention::sessions::Played::Elsewhere,
                );
            }
            sources.extend(kept.iter().cloned());
        }
        SessionSources::Torrent {
            info_hash, paths, ..
        } => {
            let names = TorrentSource::file_names(&state.engine, info_hash)
                .await
                .map_err(|error| Refusal::OpenFailed(error.to_string()))?;
            let files = paths
                .iter()
                .map(|path| {
                    names.iter().position(|name| name == path).ok_or_else(|| {
                        Refusal::NoSuchFile(format!("the torrent holds no file {path}"))
                    })
                })
                .collect::<Result<Vec<usize>, Refusal>>()?;
            // Where the film is, and what its session draws inside: its
            // extent in the one file, or its bytes in every volume of a set.
            let extent = match files.as_slice() {
                [_] => extent_of(extents).map(crate::sources::MemberExtent::In),
                _ => volumes_of(extents, &files).map(crate::sources::MemberExtent::Across),
            };
            for file_idx in files {
                match &play {
                    Some(play) => {
                        let source = Arc::new(
                            TorrentSource::played(
                                state,
                                info_hash,
                                file_idx,
                                Play {
                                    token: play.token.clone(),
                                    buffer: set_buffer.unwrap_or(play.buffer),
                                    shares: true,
                                    member: extent.clone(),
                                },
                            )
                            .await
                            .map_err(|error| Refusal::OpenFailed(error.to_string()))?,
                        );
                        played.push(source.clone());
                        sources.push(source);
                    }
                    None => sources.push(Arc::new(
                        TorrentSource::aside(state.engine.clone(), info_hash, file_idx)
                            .await
                            .map_err(|error| Refusal::OpenFailed(error.to_string()))?,
                    )),
                }
            }
        }
    }
    let view = session
        .view(&member, sources)
        .map_err(|error| Refusal::OpenFailed(error.to_string()))?;
    // The viewer's play session moves, and the stream registers, before
    // the reader is handed over, as a torrent id's does: a view opens its
    // sources lazily, at its first read, so the first extent's is opened
    // here.
    if let Some(first) = extents.first()
        && let Some(source) = played.get(first.source)
    {
        source
            .open(first.offset, ReadHint::REST)
            .await
            .map_err(|error| reader::refusal_of(&error))?;
    }
    Ok(Source::Member {
        view,
        played,
        _session: session,
    })
}

/// The bytes of its one file a member occupies, first to last: the hull of
/// its extents, which for a stored member is the one run it is.
fn extent_of(extents: &[crate::sources::Extent]) -> Option<std::ops::Range<u64>> {
    let start = extents.iter().map(|extent| extent.offset).min()?;
    let end = extents
        .iter()
        .map(|extent| extent.offset + extent.len)
        .max()?;
    (start < end).then_some(start..end)
}

/// The bytes of each volume a member occupies, in the member's order: the
/// hull of its extents in each file `sources` names (`files[source]`), one
/// volume per file. `None` for a member with no bytes.
fn volumes_of(
    extents: &[crate::sources::Extent],
    files: &[usize],
) -> Option<Vec<enginefs::retention::sessions::Volume>> {
    let mut volumes: Vec<enginefs::retention::sessions::Volume> = Vec::new();
    for extent in extents.iter().filter(|extent| extent.len > 0) {
        let file_idx = *files.get(extent.source)?;
        let bytes = extent.offset..extent.offset + extent.len;
        match volumes
            .iter_mut()
            .find(|volume| volume.file_idx == file_idx)
        {
            Some(volume) => {
                volume.member =
                    volume.member.start.min(bytes.start)..volume.member.end.max(bytes.end);
            }
            None => volumes.push(enginefs::retention::sessions::Volume {
                file_idx,
                member: bytes,
            }),
        }
    }
    (!volumes.is_empty()).then_some(volumes)
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
            && !resolution.is_stale(state)
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
    let prefix = url.path_segments().and_then(|mut segments| segments.next());
    if let Some(format) = prefix.and_then(Format::of_prefix) {
        return parse_member(url, format);
    }
    match prefix {
        Some("ftp") => Err(Refusal::NotYet {
            what: "a file on an FTP server",
        }),
        _ => Err(Refusal::UnrecognisedUrl),
    }
}

/// An archive route's URL: `/{fmt}/create[/{key}]?lz=...` as the route
/// reads its payload, or `/{fmt}/stream/{key}[/member]` (and
/// `/{fmt}/stream?key=&file=`) as the stream routes read theirs.
fn parse_member(url: &Url, format: Format) -> Result<Target, Refusal> {
    let segments: Vec<String> = url
        .path_segments()
        .into_iter()
        .flatten()
        .skip(1)
        .map(|segment| {
            urlencoding::decode(segment)
                .map(|decoded| decoded.into_owned())
                .unwrap_or_else(|_| segment.to_string())
        })
        .collect();
    let query = |name: &str| {
        url.query_pairs()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.into_owned())
    };
    match segments.first().map(String::as_str) {
        Some("create") => {
            let payload = archive::parse_create_request(query("lz"), &axum::body::Bytes::new())
                .map_err(Refusal::BadRequest)?;
            Ok(Target::Member {
                format,
                container: Container::Create {
                    key: segments.get(1).filter(|key| !key.is_empty()).cloned(),
                    payload,
                },
                member: None,
                file_idx: None,
                file_must_include: Vec::new(),
            })
        }
        Some("stream") => {
            let (key, member) = match segments.get(1).filter(|key| !key.is_empty()) {
                Some(key) => {
                    let member = segments[2..].join("/");
                    (key.clone(), (!member.is_empty()).then_some(member))
                }
                None => (
                    query("key").ok_or(Refusal::UnrecognisedUrl)?,
                    query("file").filter(|file| !file.is_empty()),
                ),
            };
            let selection = archive::MemberSelection {
                file_idx: query("fileIdx").and_then(|idx| idx.parse().ok()),
                file_must_include: query("fileMustInclude").or_else(|| query("f")),
            };
            Ok(Target::Member {
                format,
                container: Container::Key(key),
                member,
                file_idx: selection.file_idx,
                file_must_include: selection.file_must_include(),
            })
        }
        _ => Err(Refusal::UnrecognisedUrl),
    }
}

/// A media id's torrent, found or added (with the URL's `tr=`), or why
/// not as the refusal a client switches on ([`Refusal::of_magnet_add`]):
/// wait for a swarm that has not answered, stop on one the backend refused.
async fn torrent_engine(
    state: &AppState,
    info_hash: &str,
    query: Option<&str>,
) -> Result<Arc<enginefs::engine::Engine<enginefs::backend::librqbit::LibrqbitHandle>>, Refusal> {
    compat::get_or_create_engine(&state.engine, info_hash, query)
        .await
        .map_err(|error| {
            tracing::warn!(info_hash, %error, "a media id's torrent could not be added");
            Refusal::of_magnet_add(&error)
        })
}

/// The work `resolve` does, once per id: add or find the torrent and
/// choose its file; probe the origin; renew the Drive grant; index a
/// member's container and find the member in it.
async fn resolve(state: &AppState, target: &Target) -> Result<Resolution, Refusal> {
    match target {
        Target::Torrent {
            info_hash,
            file,
            query,
        } => {
            let engine = torrent_engine(state, info_hash, query.as_deref()).await?;
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
            let plain = |sniffed| Resolution::Torrent {
                info_hash: info_hash.clone(),
                file_idx,
                name: file.name.clone(),
                len: file.length,
                sniffed,
            };
            // The container sniff: the file's head off the store, or
            // fetched within the bound -- and a head not here in time is
            // the file as itself, so the player can start on it.
            let Some(head) =
                sniff::torrent_head(state, info_hash, file_idx, state.media.sniff_bound()).await
            else {
                return Ok(plain(false));
            };
            let formats = sniff::formats(&head);
            if formats.is_empty() {
                return Ok(plain(true));
            }
            // By the file's name, as the `torrent:` form is made: a RAR's
            // other volumes are its siblings by the naming rules.
            let (format, key, session) =
                sniff::torrent_session(state, info_hash, &file.name, &formats).await?;
            let name = chosen_member(&session, None, &[])?;
            let (len, torrent) = member_in(state, &session, &name).await?;
            tracing::info!(
                info_hash,
                file_idx,
                ?format,
                "a media id's torrent file is a container, by its head"
            );
            Ok(Resolution::Member {
                format,
                key,
                create: None,
                name,
                len,
                torrent,
                sniffed_in: None,
            })
        }
        Target::Proxy {
            target,
            request_headers,
            content_type,
            path_and_query,
        } => {
            let name = link_name(target);
            // A finished download of this link first, and off the disk: it
            // plays without the origin, which is what a download is for.
            let pin = crate::proxy_downloads::ProxyPinKey::Url {
                target: target.to_string(),
                headers: request_headers.clone(),
            };
            if let Some((source, pinned_name)) =
                crate::proxy_downloads::held_download(state, &pin).await
            {
                let source = Arc::new(source);
                return match sniffed_file(state, Sniffed::Held(source.clone())).await? {
                    Sniff::Member(member) => Ok(member),
                    Sniff::Plain { sniffed } => Ok(Resolution::Held {
                        source,
                        target: target.clone(),
                        name: pinned_name,
                        sniffed,
                    }),
                };
            }
            match ProxySource::open(
                state.proxy_cache.clone(),
                state.http_addr,
                target.clone(),
                request_headers.clone(),
            )
            .await
            {
                Ok(source) => {
                    let source = Arc::new(source);
                    match sniffed_file(state, Sniffed::Link(source.clone())).await? {
                        Sniff::Member(member) => Ok(member),
                        Sniff::Plain { sniffed } => Ok(Resolution::Http {
                            content_type: content_type
                                .clone()
                                .unwrap_or_else(|| source.content_type().to_string()),
                            source,
                            target: target.clone(),
                            name,
                            sniffed,
                        }),
                    }
                }
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
            // A finished download first, off the disk and before the grant
            // is asked for: it plays on a device with no network, as
            // `open_drive_file` lets it (`complete_drive_download`).
            let pin = crate::proxy_downloads::ProxyPinKey::Drive {
                file_id: file_id.clone(),
            };
            if let Some((source, pinned_name)) =
                crate::proxy_downloads::held_download(state, &pin).await
            {
                let target = endpoints
                    .pairing(file_id, "")
                    .media_url()
                    .map_err(Refusal::of_drive)?;
                let source = Arc::new(source);
                return match sniffed_file(state, Sniffed::Held(source.clone())).await? {
                    Sniff::Member(member) => Ok(member),
                    Sniff::Plain { sniffed } => Ok(Resolution::Held {
                        source,
                        target,
                        name: name.clone().unwrap_or(pinned_name),
                        sniffed,
                    }),
                };
            }
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
            let source = Arc::new(source.named(name.clone()));
            match sniffed_file(state, Sniffed::Drive(source.clone())).await? {
                Sniff::Member(member) => Ok(member),
                Sniff::Plain { sniffed } => Ok(Resolution::Drive {
                    source,
                    media_url,
                    name,
                    sniffed,
                }),
            }
        }
        Target::Local { file, name } => {
            // The I/O `register` did not do: open it, prove it seeks, read
            // its length.
            let name = name
                .clone()
                .or_else(|| file.file_name())
                .unwrap_or_else(|| "a file on this device".to_string());
            let source = Arc::new(
                LocalSource::open(file, name)
                    .await
                    .map_err(Refusal::of_local)?,
            );
            match sniffed_file(state, Sniffed::Local(source.clone())).await? {
                Sniff::Member(member) => Ok(member),
                Sniff::Plain { sniffed } => Ok(Resolution::Local { source, sniffed }),
            }
        }
        Target::Member {
            format,
            container,
            member,
            file_idx,
            file_must_include,
        } => {
            let (key, create) = match container {
                Container::Create { key, payload } => (
                    key.clone()
                        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                    Some(payload.clone()),
                ),
                Container::Key(key) => (key.clone(), None),
            };
            let session = member_session(state, *format, &key, create.as_ref(), None).await?;
            let name = match member {
                Some(name) => name.clone(),
                None => chosen_member(&session, *file_idx, file_must_include)?,
            };
            let (len, torrent) = member_in(state, &session, &name).await?;
            Ok(Resolution::Member {
                format: *format,
                key,
                create,
                name,
                len,
                torrent,
                sniffed_in: None,
            })
        }
    }
}

/// The member `session`'s container plays with none named: the rule
/// `/create` picks by (`routes::archive::chosen_member`).
fn chosen_member(
    session: &TranslatedSession,
    file_idx: Option<usize>,
    file_must_include: &[String],
) -> Result<String, Refusal> {
    archive::chosen_member(session, file_idx, file_must_include)
        .map_err(Refusal::of_session)?
        .ok_or_else(|| Refusal::NoSuchFile("the archive holds no member to play".to_string()))
}

/// The member `name` of `session`'s container, refused if it cannot be
/// served by range: its length, and the torrent the container is in with
/// its volumes' file indices, when it is in one.
async fn member_in(
    state: &AppState,
    session: &TranslatedSession,
    name: &str,
) -> Result<(u64, Option<(String, Vec<usize>)>), Refusal> {
    let found = session
        .member(name)
        .ok_or_else(|| Refusal::NoSuchFile(format!("the archive holds no member {name}")))?;
    if let crate::translators::Body::Opaque(refusal) = &found.body {
        return Err(Refusal::Translated(refusal.clone()));
    }
    let torrent = match session.sources() {
        SessionSources::Held(_) | SessionSources::Kept(_) => None,
        SessionSources::Torrent {
            info_hash, paths, ..
        } => {
            let names = TorrentSource::file_names(&state.engine, info_hash)
                .await
                .map_err(|error| Refusal::NoSuchFile(error.to_string()))?;
            let files = paths
                .iter()
                .map(|path| names.iter().position(|name| name == path))
                .collect::<Option<Vec<usize>>>()
                .ok_or_else(|| {
                    Refusal::NoSuchFile("a volume of the archive left its torrent".into())
                })?;
            Some((info_hash.to_lowercase(), files))
        }
    };
    Ok((found.len, torrent))
}

/// What a sniff of a file that is not a torrent's found.
enum Sniff {
    /// The container it holds, as the member an archive URL would name.
    Member(Resolution),
    /// No container: `sniffed` is whether its head was read at all.
    Plain { sniffed: bool },
}

/// Read the head of the link, Drive file or file on this device
/// `sniffed` names, within the bound, and on a container's signature
/// answer its member -- or the first translator's refusal when none of
/// those it names indexes it.
async fn sniffed_file(state: &AppState, sniffed: Sniffed) -> Result<Sniff, Refusal> {
    let source = sniffed.source();
    let Some(head) = sniff::head_of(source.as_ref(), state.media.sniff_bound()).await else {
        return Ok(Sniff::Plain { sniffed: false });
    };
    let formats = sniff::formats(&head);
    if formats.is_empty() {
        return Ok(Sniff::Plain { sniffed: true });
    }
    // One volume: a set behind links or Drive needs its volume list
    // stated, and nothing supplies one yet -- the first volume alone is
    // what the translator is handed, and its refusal is the answer.
    let (format, key, session) = sniffed.index(state, &formats).await?;
    let name = chosen_member(&session, None, &[])?;
    let (len, torrent) = member_in(state, &session, &name).await?;
    tracing::info!(
        source = %source.describe(),
        ?format,
        "a media id's file is a container, by its head"
    );
    Ok(Sniff::Member(Resolution::Member {
        format,
        key,
        create: None,
        name,
        len,
        torrent,
        sniffed_in: Some(sniffed),
    }))
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

/// **Pins and unpins by id** (`docs/design/media-pipeline.md` §2.6): the
/// dispatch from what an id names to the two pin paths there are -- the
/// piece store's (`routes::downloads::pin_download`) for a torrent file,
/// the proxy cache's (`proxy_downloads::pin_url`, `pin_drive`) for a link
/// or a Drive file. A member pins its container's files, every volume of a
/// set; a file on this device has nothing to download.
impl Registry {
    /// Pin what `id` names as an offline download, and answer its rows:
    /// one, or one per volume for a member of a set.
    pub(crate) async fn pin(
        &self,
        state: &AppState,
        id: &MediaId,
    ) -> Result<Vec<crate::DownloadInfo>, super::PinError> {
        use super::PinError;
        let entry = self.entry(id).map_err(PinError::Refused)?;
        let target = entry
            .target
            .as_ref()
            .map_err(|refusal| PinError::Refused(refusal.clone()))?;
        match target {
            Target::Local { .. } => Err(PinError::NothingToDownload),
            Target::Drive {
                file_id,
                name,
                grant,
            } => {
                // The grant is asked for here, as a resolve asks for it; a
                // file already whole on the disk needs none.
                let token = grant();
                let dir = crate::proxy_downloads::pin_drive(
                    state,
                    file_id,
                    token.as_deref(),
                    name.clone(),
                )
                .await
                .map_err(PinError::Proxy)?;
                let row = crate::routes::downloads::proxy_row(state, &dir)
                    .await
                    .map_err(PinError::Proxy)?;
                Ok(vec![row])
            }
            Target::Proxy {
                target,
                request_headers,
                ..
            } => Ok(vec![
                pin_link(state, target, request_headers, Some(link_name(target))).await?,
            ]),
            Target::Torrent { query, .. } => {
                let resolution = entry.resolution(state).await.map_err(PinError::Refused)?;
                let trackers = compat::parse_trackers(query.as_deref());
                let Resolution::Torrent {
                    info_hash,
                    file_idx,
                    ..
                } = &*resolution
                else {
                    // A file whose head is a container's resolves to its
                    // member, and pins what a member pins: every volume.
                    let mut rows = Vec::new();
                    for volume in member_volumes(state, &resolution).await? {
                        let Volume::Torrent {
                            info_hash,
                            file_idx,
                        } = volume
                        else {
                            unreachable!("a torrent's container is in the torrent");
                        };
                        rows.push(
                            crate::routes::downloads::pin_download(
                                state,
                                &info_hash,
                                file_idx,
                                trackers.clone(),
                            )
                            .await
                            .map_err(PinError::Torrent)?,
                        );
                    }
                    return Ok(rows);
                };
                let row =
                    crate::routes::downloads::pin_download(state, info_hash, *file_idx, trackers)
                        .await
                        .map_err(PinError::Torrent)?;
                Ok(vec![row])
            }
            Target::Member { .. } => {
                let resolution = entry.resolution(state).await.map_err(PinError::Refused)?;
                let mut rows = Vec::new();
                for volume in member_volumes(state, &resolution).await? {
                    rows.push(match volume {
                        Volume::Torrent {
                            info_hash,
                            file_idx,
                        } => crate::routes::downloads::pin_download(
                            state,
                            &info_hash,
                            file_idx,
                            Vec::new(),
                        )
                        .await
                        .map_err(PinError::Torrent)?,
                        Volume::Link(url) => {
                            pin_link(state, &url, &BTreeMap::new(), Some(link_name(&url))).await?
                        }
                    });
                }
                Ok(rows)
            }
        }
    }

    /// Drop the pin on what `id` names, with `delete_files` its bytes too:
    /// [`crate::ServerHandle::unpin_download`] or
    /// [`crate::ServerHandle::unpin_proxy_download`] for each file it pinned,
    /// the outcomes joined -- `unpinned` if any pin went, `deleted_files` if
    /// any bytes did.
    pub(crate) async fn unpin(
        &self,
        state: &AppState,
        id: &MediaId,
        delete_files: bool,
    ) -> Result<enginefs::UnpinOutcome, super::PinError> {
        use super::PinError;
        let entry = self.entry(id).map_err(PinError::Refused)?;
        let target = entry
            .target
            .as_ref()
            .map_err(|refusal| PinError::Refused(refusal.clone()))?;
        let proxy = |key: crate::proxy_downloads::ProxyPinKey| async move {
            let Some(name) = crate::routes::downloads::proxy_download_key(state, &key) else {
                return Err(PinError::Proxy(
                    crate::proxy_downloads::ProxyPinError::Unkeyable(
                        "nothing this server can key, so nothing is pinned under it",
                    ),
                ));
            };
            Ok(crate::routes::downloads::unpin_proxy_download(state, &name, delete_files).await)
        };
        let torrent = |info_hash: String, file_idx: usize| async move {
            crate::routes::downloads::unpin_download(state, &info_hash, file_idx, delete_files)
                .await
                .map_err(PinError::Torrent)
        };
        match target {
            Target::Local { .. } => Err(PinError::NothingToDownload),
            Target::Drive { file_id, .. } => {
                proxy(crate::proxy_downloads::ProxyPinKey::Drive {
                    file_id: file_id.clone(),
                })
                .await
            }
            Target::Proxy {
                target,
                request_headers,
                ..
            } => {
                proxy(crate::proxy_downloads::ProxyPinKey::Url {
                    target: target.to_string(),
                    headers: request_headers.clone(),
                })
                .await
            }
            // An index needs no resolve: an unpin must not add a torrent
            // the session does not hold (a dormant pin's) to learn it --
            // unless a resolve already found the file a container's, whose
            // pin was every volume.
            Target::Torrent {
                info_hash,
                file: StreamFile::Index(file_idx),
                ..
            } if !matches!(
                entry
                    .resolution
                    .try_lock()
                    .ok()
                    .and_then(|held| held.clone())
                    .as_deref(),
                Some(Resolution::Member { .. })
            ) =>
            {
                torrent(info_hash.clone(), *file_idx).await
            }
            Target::Torrent { .. } | Target::Member { .. } => {
                let resolution = entry.resolution(state).await.map_err(PinError::Refused)?;
                let volumes = match &*resolution {
                    Resolution::Torrent {
                        info_hash,
                        file_idx,
                        ..
                    } => vec![Volume::Torrent {
                        info_hash: info_hash.clone(),
                        file_idx: *file_idx,
                    }],
                    resolution => member_volumes(state, resolution).await?,
                };
                let mut joined = enginefs::UnpinOutcome {
                    unpinned: false,
                    deleted_files: false,
                };
                for volume in volumes {
                    let outcome = match volume {
                        Volume::Torrent {
                            info_hash,
                            file_idx,
                        } => torrent(info_hash, file_idx).await?,
                        Volume::Link(url) => {
                            proxy(crate::proxy_downloads::ProxyPinKey::Url {
                                target: url.to_string(),
                                headers: BTreeMap::new(),
                            })
                            .await?
                        }
                    };
                    joined.unpinned |= outcome.unpinned;
                    joined.deleted_files |= outcome.deleted_files;
                }
                Ok(joined)
            }
        }
    }
}

/// One file a member's container is made of, as a pin names it.
enum Volume {
    /// A file of the torrent the container is in.
    Torrent { info_hash: String, file_idx: usize },
    /// A link the container is behind, fetched with no `h=` headers, as an
    /// archive `/create` fetches its volumes.
    Link(Url),
}

/// Every file `resolution`'s container is made of, in set order: a
/// member's pin is its container's, all of it -- a member is ranges of
/// those files and nothing else holds its bytes.
async fn member_volumes(
    state: &AppState,
    resolution: &Resolution,
) -> Result<Vec<Volume>, super::PinError> {
    let Resolution::Member {
        format,
        key,
        create,
        torrent,
        sniffed_in,
        ..
    } = resolution
    else {
        unreachable!("only a member has volumes");
    };
    if let Some((info_hash, files)) = torrent {
        return Ok(files
            .iter()
            .map(|file_idx| Volume::Torrent {
                info_hash: info_hash.clone(),
                file_idx: *file_idx,
            })
            .collect());
    }
    // Behind links: the create's own list when the id carries one, else the
    // session's sources.
    if let Some(payload) = create {
        return payload
            .urls
            .iter()
            .map(|url| {
                Url::parse(url).map(Volume::Link).map_err(|_| {
                    super::PinError::Refused(Refusal::BadRequest(
                        "a volume of the archive is not a web address".to_string(),
                    ))
                })
            })
            .collect();
    }
    let session = member_session(state, *format, key, None, sniffed_in.as_ref())
        .await
        .map_err(super::PinError::Refused)?;
    Ok(match session.sources() {
        SessionSources::Held(sources) => sources
            .iter()
            .map(|source| Volume::Link(source.url().clone()))
            .collect(),
        // On this device already: nothing to download.
        SessionSources::Kept(_) => return Err(super::PinError::NothingToDownload),
        SessionSources::Torrent { .. } => {
            unreachable!("a member in a torrent resolves with its torrent's files")
        }
    })
}

/// Pin one link as a download, and answer its row.
async fn pin_link(
    state: &AppState,
    target: &Url,
    headers: &BTreeMap<String, String>,
    name: Option<String>,
) -> Result<crate::DownloadInfo, super::PinError> {
    let dir = crate::proxy_downloads::pin_url(state, target.as_str(), headers.clone(), name)
        .await
        .map_err(super::PinError::Proxy)?;
    crate::routes::downloads::proxy_row(state, &dir)
        .await
        .map_err(super::PinError::Proxy)
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

    /// Recognised, and refused for now: the FTP form is a later step, and
    /// saying so beats calling it unknown.
    #[test]
    fn an_ftp_url_is_not_yet_playable_by_id() {
        assert_eq!(
            parsed("http://127.0.0.1:1/ftp/ftp%3A%2F%2Fhost%2Ffilm.mkv")
                .err()
                .map(|refusal| refusal.kind()),
            Some("notYet")
        );
        assert_eq!(
            parsed("http://127.0.0.1:1/settings")
                .err()
                .map(|refusal| refusal.kind()),
            Some("unrecognisedUrl")
        );
    }

    /// **A player's reports about a member are about its container file**
    /// -- the one torrent file a single-file container is, where the play
    /// session draws and the film's duration divides the member's extent
    /// -- and about nothing for a set (`a_sets_duration_is_its_volumes`) or
    /// a container behind links.
    #[test]
    fn a_members_reports_are_its_single_container_files() {
        let registry = Registry::new();
        let member = |torrent: Option<(String, Vec<usize>)>| Resolution::Member {
            format: Format::Rar,
            key: "key".to_string(),
            create: None,
            name: "film.mkv".to_string(),
            len: 1,
            torrent,
            sniffed_in: None,
        };
        let hash = "ab".repeat(20);
        for (torrent, expected) in [
            (Some((hash.clone(), vec![3])), Some((hash.clone(), 3))),
            (Some((hash.clone(), vec![3, 4, 5])), None),
            (None, None),
        ] {
            let id = registry
                .register(MediaSpec::StreamingUrl(
                    Url::parse("http://127.0.0.1:1/rar/stream/key/film.mkv").expect("a URL"),
                ))
                .expect("registered");
            let entry = registry.entry(&id).expect("the entry");
            *entry.resolution.try_lock().expect("nobody resolving") =
                Some(Arc::new(member(torrent)));
            drop(entry);
            assert_eq!(registry.torrent_file(&id), expected);
        }
    }

    /// **The film's duration for a member is told to the files it lies
    /// in, for the member's draw alone** (`ServerHandle::note_media_duration`):
    /// every volume of a set, the one file of a single-file container --
    /// whose stream would otherwise take a container's length over an
    /// episode's duration as its rate -- and nothing for a container behind
    /// links.
    #[test]
    fn a_members_duration_is_its_files() {
        let registry = Registry::new();
        let hash = "ab".repeat(20);
        for (torrent, expected) in [
            (
                Some((hash.clone(), vec![3, 4, 5])),
                Some((hash.clone(), vec![3, 4, 5])),
            ),
            (Some((hash.clone(), vec![3])), Some((hash.clone(), vec![3]))),
            (None, None),
        ] {
            let id = registry
                .register(MediaSpec::StreamingUrl(
                    Url::parse("http://127.0.0.1:1/rar/stream/key/film.mkv").expect("a URL"),
                ))
                .expect("registered");
            let entry = registry.entry(&id).expect("the entry");
            *entry.resolution.try_lock().expect("nobody resolving") =
                Some(Arc::new(Resolution::Member {
                    format: Format::Rar,
                    key: "key".to_string(),
                    create: None,
                    name: "film.mkv".to_string(),
                    len: 1,
                    torrent,
                    sniffed_in: None,
                }));
            drop(entry);
            assert_eq!(registry.member_files(&id), expected);
        }
    }

    /// **An archive URL is read as its route reads it**: a `/create`'s
    /// `lz` payload by the route's own decoder, which refuses a payload it
    /// cannot decode or a create with none as the route's `400` does; the
    /// `torrent:` form's key and member path decoded as the stream route's
    /// path extractor decodes them; and a `/stream/{key}` with no member
    /// keeps the redirect's `fileIdx` and `f` for the rule to pick with.
    #[test]
    fn an_archive_url_is_read_as_its_route_reads_it() {
        let lz = lz_str::compress_to_encoded_uri_component(
            r#"{"urls":["https://cdn.example/a.part1.rar","https://cdn.example/a.part2.rar"],"fileIdx":1}"#,
        );
        let Ok(Target::Member {
            format: Format::Rar,
            container: Container::Create { key: None, payload },
            member: None,
            ..
        }) = parsed(&format!("http://127.0.0.1:1/rar/create?lz={lz}"))
        else {
            panic!("not a create");
        };
        assert_eq!(payload.urls.len(), 2);
        assert_eq!(payload.file_idx, Some(1));
        for url in [
            "http://127.0.0.1:1/rar/create?lz=abc",
            "http://127.0.0.1:1/zip/create/key",
        ] {
            assert_eq!(
                parsed(url).err().map(|refusal| refusal.kind()),
                Some("badRequest"),
                "{url}"
            );
        }

        let hash = "ab".repeat(20);
        let Ok(Target::Member {
            format: Format::Zip,
            container: Container::Key(key),
            member,
            ..
        }) = parsed(&format!(
            "http://127.0.0.1:1/zip/stream/torrent:{hash}%2FRelease%20One%2Ffixture.zip/videos/the%20film.mkv"
        ))
        else {
            panic!("not a member");
        };
        assert_eq!(key, format!("torrent:{hash}/Release One/fixture.zip"));
        assert_eq!(member.as_deref(), Some("videos/the film.mkv"));

        let Ok(Target::Member {
            member: None,
            file_idx,
            file_must_include,
            ..
        }) = parsed(&format!(
            "http://127.0.0.1:1/iso/stream/torrent:{hash}%2Fdisc.iso?fileIdx=2&f=main,feature"
        ))
        else {
            panic!("not a member");
        };
        assert_eq!(file_idx, Some(2));
        assert_eq!(file_must_include, ["main", "feature"]);
    }
}

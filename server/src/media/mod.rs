//! **Playable things by id, and a blocking reader over any of them**
//! (`docs/design/media-pipeline.md` §2.3, §2.4).
//!
//! The app names what it wants played by an opaque id this server issued
//! ([`MediaId`]), never by a path or a URL a string from outside could
//! forge, and reads it through a [`MediaReader`] whose calls block -- what
//! mpv's `stream_cb` callbacks and a JNI export are. Two halves:
//!
//! * [`registry`](crate::media::registry) -- [`MediaSpec`](crate::media::MediaSpec) in, [`MediaId`](crate::media::MediaId) out, with no I/O
//!   (`Registry::register`); the work -- adding the torrent,
//!   probing the origin, renewing the Drive grant -- is `resolve`'s, and
//!   its answer is kept on the entry. The entries are a
//!   [`Sessions`](crate::translators::session::Sessions) map: a count cap
//!   and least-recently-used eviction of entries nobody holds, and no
//!   clock. An open reader holds a lease on its entry, so an id being
//!   read is never the one evicted.
//! * [`reader`](crate::media::reader) -- the reader task.
//!
//! # Who owns what
//!
//! **One runtime task per open reader owns the reader** and everything a
//! reader holds: the byte source, its file handle, the stream registration
//! and the play session's move (a torrent's `TorrentStream`), and the
//! lease on the registry entry. The foreign side -- the thread mpv calls
//! in on -- owns a [`MediaReader`]: a command sender, a cancellation token,
//! a length and the runtime's handle. Nothing else.
//!
//! * **Nothing a foreign thread drops can spawn.** A torrent source's end
//!   (`StreamLifecycleGuard::notify_end`, an aside's `on_stream_end`) calls
//!   `tokio::spawn`, which panics off a runtime. Dropping a
//!   [`MediaReader`] drops a sender; the task sees its channel end and
//!   drops the reader where it lives, on the runtime.
//! * **A cancel overtakes the read it interrupts.** It is not a queued
//!   command -- a queued command waits behind the read it is meant to
//!   interrupt. [`MediaReader::cancel`] trips a token, which does not
//!   block; the task races every read, seek and reopen against it and
//!   answers the one in flight with [`std::io::ErrorKind::Interrupted`],
//!   and every later command the same, at once. Dropping the in-flight read
//!   is what wakes one parked on a piece nobody has: it is what a closed
//!   socket does to an HTTP body.
//! * **A runtime that goes away is an error, never a hang.** Its tasks are
//!   dropped with it, a reply sender with each, and the foreign thread's
//!   wait for the reply returns an error; a command sent afterwards finds
//!   the channel closed.
//! * **Neither side holds the `ServerHandle`.** An embedder stops the
//!   server by waiting for its last `Arc` of the handle, so a reader holding
//!   one would hold the stop up for as long as a player held the stream.
//!   The task holds the pieces of `AppState` it needs; the reader holds
//!   the runtime's `Handle`.

pub mod reader;
pub mod registry;

pub use reader::{Canceller, Command, MediaReader};
pub use registry::MEDIA_ID_CAP;

use crate::sources::drive::DriveError;
use crate::sources::local::LocalError;
pub use crate::sources::local::LocalFile;
use crate::sources::proxy::ProxySourceError;
use enginefs::backend::priorities::BufferProfile;
use std::sync::Arc;
use url::Url;

/// Where a Google Drive file's grant comes from, asked each time the file
/// is resolved: the refresh token, or `None` when this device holds no
/// link to the account any more. The server keeps no grant of its own
/// beyond an open source's; the embedder does (xtremio's
/// `ServerState::drive_grant`).
pub type GrantSupplier = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// What the app hands the server to play. Parsed, never fetched.
pub enum MediaSpec {
    /// A URL stremio-core built (`streaming_url`) on this server: a
    /// torrent (`/{infoHash}/{fileIdx}`, `-1` and `f=` and `tr=`
    /// included), `/proxy` in either spelling. Read by the routes' own
    /// parsers, so the core stays the one place these URLs are *built*
    /// and this server the one place they are *read*. An archive
    /// `/{fmt}/create` and `/ftp` are recognised and refused as
    /// [`Refusal::NotYet`] for now.
    StreamingUrl(Url),
    /// A Google Drive file, and where its grant comes from.
    Drive {
        /// Drive's id for the file. Not a secret: it is the cache key.
        file_id: String,
        /// What to call it; Drive's bytes carry no name.
        name: Option<String>,
        /// The refresh token, asked for at each resolve.
        grant: GrantSupplier,
    },
    /// A file on this device: a path, or an fd the app hands over (on
    /// Android, `ParcelFileDescriptor.detachFd()`). **Only ever named
    /// here**, through [`crate::ServerHandle::register`]: no HTTP route
    /// resolves an id and nothing deserializes a `MediaSpec`, so no URL
    /// and no request body can name a file on this device. Resolving it
    /// opens it, proves it can seek -- a pipe is refused, never streamed
    /// forward -- and reads its length.
    Local {
        /// The file.
        file: LocalFile,
        /// What to call it. A path's file name when `None`; an fd has
        /// no name of its own, so the app should say. Its extension is
        /// where the content type comes from.
        name: Option<String>,
    },
}

/// An id [`crate::ServerHandle::register`] issued: 128 random bits, hex.
/// Nothing in it says what it names, no HTTP route resolves one, and a
/// restart forgets every one of them -- the app registers at open.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct MediaId(String);

impl MediaId {
    /// A fresh id: 16 bytes from the OS's generator, as the control
    /// token is drawn (`auth.rs`).
    pub(crate) fn random() -> anyhow::Result<Self> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes)
            .map_err(|err| anyhow::anyhow!("failed to draw random bytes for a media id: {err}"))?;
        Ok(Self(hex::encode(bytes)))
    }

    /// The id as the string it travels as.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for MediaId {
    /// An id the app kept as a string, handed back. An id this server did
    /// not issue is simply one it holds nothing under.
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for MediaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The viewer's player behind a reader: the token `p=` carries today
/// (`<viewer>.<screen>`) and its read-ahead choice. Whether the file
/// shares is decided here, not by the app: a torrent file shares unless it
/// is an archive or a disc image, whose playback shares nothing.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PlayToken {
    /// `<viewer>.<screen>`, as `p=` carries it.
    pub token: String,
    /// The read-ahead choice for this playback; settable later with
    /// [`crate::ServerHandle::set_buffer`].
    pub buffer: BufferProfile,
}

/// The member a container resolved to. Nothing resolves to one yet (the
/// sniff is step E of the design); the field is here so the answer's shape
/// does not change when something does.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberInfo {
    /// The member's path inside its container.
    pub name: String,
    /// Its length.
    pub len: u64,
}

/// What resolution found: enough to show, open, cast and pin.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resolved {
    /// What to call it: the torrent file's name, the last segment of a
    /// link, or the name a Drive file was registered with.
    pub name: String,
    pub content_type: String,
    /// Its length. `0` when [`Self::in_process`] is false: the origin
    /// answered a ranged probe with the whole entity, and its length was
    /// not what was being asked.
    pub len: u64,
    /// The member a container resolved to, when it was one. Always `None`
    /// until the sniff moves here (the design's step E).
    pub member: Option<MemberInfo>,
    /// `false` only for an HTTP origin that will not serve ranges: nothing
    /// in this process can seek it, so a player has to be handed
    /// [`Self::proxy_url`] and read it forward itself. Not a refusal.
    pub in_process: bool,
    /// The absolute `/proxy` URL on this server to hand a player when
    /// [`Self::in_process`] is false, and `None` otherwise.
    pub proxy_url: Option<String>,
}

/// Why an id cannot be resolved or read. Each carries the sentence a
/// player can show, and [`Self::kind`] is what a client switches on: the
/// `{refused, message}` shape the archive routes answer with a `415`,
/// `422` or `501` (`docs/design/translated-sources.md`), carried typed
/// over FFI instead of as a status. It serializes as that same object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// No entry under this id: never issued, or evicted at the cap.
    UnknownId,
    /// A URL whose shape is none of this server's routes.
    UnrecognisedUrl,
    /// A shape this server serves over HTTP that cannot be played by id
    /// yet: an archive `/create`, `/ftp`.
    NotYet {
        /// What it is, as a sentence's subject ("an archive member").
        what: &'static str,
    },
    /// What a container says about a member it holds (`415`/`422` on the
    /// archive routes).
    Translated(crate::translators::Refusal),
    /// An origin that will not serve ranges, where ranges are the only way
    /// in: a member inside one, or a reader asked of one (`501 noRanges`).
    /// Resolving such a link is *not* this: it is `in_process: false`.
    NoRanges,
    /// A build with no reader for the format (`501 noReader`).
    NoReader(String),
    /// The torrent could not be added or found (a metadata timeout, the
    /// backend refusing it). The engine's own non-leaking sentence.
    TorrentUnavailable(String),
    /// The torrent has no file by that index, or `-1` matched nothing.
    NoSuchFile(String),
    /// The origin answered, and not with the resource.
    OriginRefused(String),
    /// The origin could not be reached.
    Unreachable(String),
    /// This server was configured with no Drive pairing service.
    NoPairingService,
    /// The grant supplier has no grant: this device is not linked.
    NoGrant,
    /// The Drive grant is gone; only a new pairing brings it back.
    PairAgain,
    /// Drive, or the service that keeps the device linked, would not
    /// serve the file.
    DriveUnreadable(String),
    /// The disk gate refused: the volume is short even with the caches'
    /// slack given back (the stream route's `507`).
    InsufficientDiskSpace,
    /// The file could not be opened for reading.
    OpenFailed(String),
    /// A file on this device that reads only forwards (a pipe, as a cloud
    /// provider may hand out): playing needs a seek, so it is refused
    /// rather than streamed forward.
    NotSeekable,
    /// The server has stopped, or is stopping.
    ServerStopped,
}

impl Refusal {
    /// The short name a client switches on. Stable: it is part of the
    /// contract, as the archive routes' `refused` values are.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::UnknownId => "unknownId",
            Self::UnrecognisedUrl => "unrecognisedUrl",
            Self::NotYet { .. } => "notYet",
            Self::Translated(refusal) => refusal.kind(),
            Self::NoRanges => "noRanges",
            Self::NoReader(_) => "noReader",
            Self::TorrentUnavailable(_) => "torrentUnavailable",
            Self::NoSuchFile(_) => "noSuchFile",
            Self::OriginRefused(_) => "originRefused",
            Self::Unreachable(_) => "unreachable",
            Self::NoPairingService => "noPairingService",
            Self::NoGrant => "noGrant",
            Self::PairAgain => "pairAgain",
            Self::DriveUnreadable(_) => "driveUnreadable",
            Self::InsufficientDiskSpace => "insufficientDiskSpace",
            Self::OpenFailed(_) => "openFailed",
            Self::NotSeekable => "notSeekable",
            Self::ServerStopped => "serverStopped",
        }
    }

    /// A probe's failure, as the sentence `ProxySourceError` already has
    /// for it. `WillNotRange` is not handled here: resolving such a link
    /// succeeds with `in_process: false`.
    pub(crate) fn of_probe(error: ProxySourceError) -> Self {
        match error {
            ProxySourceError::WillNotRange => Self::NoRanges,
            ProxySourceError::Origin(_) => Self::OriginRefused(error.to_string()),
            ProxySourceError::Fetch(_) | ProxySourceError::Credentials(_) => {
                Self::Unreachable(error.to_string())
            }
        }
    }

    /// A local file's refusal: a pipe as itself, anything else a failed
    /// open with its sentence.
    pub(crate) fn of_local(error: LocalError) -> Self {
        match error {
            LocalError::NotSeekable => Self::NotSeekable,
            error => Self::OpenFailed(error.to_string()),
        }
    }

    /// A Drive open's failure, `pairAgain` kept as itself.
    pub(crate) fn of_drive(error: DriveError) -> Self {
        match error {
            DriveError::PairAgain => Self::PairAgain,
            error => Self::DriveUnreadable(error.to_string()),
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownId => f.write_str(
                "this server holds nothing under that id: it was never issued here, or was let go",
            ),
            Self::UnrecognisedUrl => {
                f.write_str("this server serves nothing at that URL, so it cannot play it")
            }
            Self::NotYet { what } => write!(f, "{what} cannot be played by id yet"),
            Self::Translated(refusal) => write!(f, "{refusal}"),
            Self::NoRanges => write!(f, "{}", ProxySourceError::WillNotRange),
            Self::NoReader(message)
            | Self::TorrentUnavailable(message)
            | Self::NoSuchFile(message)
            | Self::OriginRefused(message)
            | Self::Unreachable(message)
            | Self::DriveUnreadable(message)
            | Self::OpenFailed(message) => f.write_str(message),
            Self::NoPairingService => {
                write!(f, "{}", crate::DriveOpenError::NoPairingService)
            }
            Self::NoGrant => {
                f.write_str("this device is not linked to a Google account that can read that file")
            }
            Self::PairAgain => write!(f, "{}", DriveError::PairAgain),
            Self::InsufficientDiskSpace => {
                f.write_str(crate::routes::stream::INSUFFICIENT_DISK_SPACE_BODY)
            }
            Self::NotSeekable => write!(f, "{}", LocalError::NotSeekable),
            Self::ServerStopped => f.write_str("the server has stopped"),
        }
    }
}

impl std::error::Error for Refusal {}

impl serde::Serialize for Refusal {
    /// `{"refused": kind, "message": sentence}`, the archive routes' shape.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut object = serializer.serialize_struct("Refusal", 2)?;
        object.serialize_field("refused", self.kind())?;
        object.serialize_field("message", &self.to_string())?;
        object.end()
    }
}

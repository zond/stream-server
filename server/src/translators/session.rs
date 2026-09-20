//! An indexed container, in memory, leased: what a `/{fmt}/create` leaves
//! behind and every response body reads from.
//!
//! **It owns no file.** An archive session used to own a download under
//! `<cacheRoot>/.archives` and one extraction per member beside it, and
//! the sweep that took the session unlinked them. What is here instead is
//! the container's [`Index`] and the [`ByteSource`]s it was read from, so
//! what the sweep frees is memory and -- for a torrent -- the stream
//! registration the source holds. For a proxied URL it frees nothing at
//! all: the bytes are the proxy cache's, under its own retention owner,
//! exactly as if the player had fetched the file through `/proxy` itself.
//!
//! A re-index after a sweep is therefore a few small ranged reads, which
//! the proxy cache answers from disk and a torrent from its piece store.
//! That is the whole cost of forgetting one.

use super::{Body, Index, Member};
use crate::sources::{ByteSource, MemberView};
use std::io;
use std::sync::Arc;

/// One container, indexed: where its bytes come from, what is in it, and
/// which member the `/create` that made it chose.
pub struct TranslatedSession {
    /// What this was made from -- a URL, or a `torrent:<hash>/<path>` key.
    /// Two creates naming the same origin are the same session's business;
    /// one naming a different origin under a key already in use is
    /// refused, because every `/{fmt}/stream/{key}/...` after it would
    /// read a different archive (`routes::archive`).
    origin: String,
    /// Where the container's bytes come from -- see [`SessionSources`].
    sources: SessionSources,
    index: Index,
    /// The member `fileIdx`/`fileMustInclude` picked, if the create picked
    /// one: what `/stream/{key}` with no member path redirects to.
    selected: Option<usize>,
}

impl TranslatedSession {
    pub fn new(
        origin: impl Into<String>,
        sources: SessionSources,
        index: Index,
        selected: Option<usize>,
    ) -> Self {
        Self {
            origin: origin.into(),
            sources,
            index,
            selected,
        }
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn sources(&self) -> &SessionSources {
        &self.sources
    }

    /// The member this session was created for, if any.
    pub fn selected(&self) -> Option<&Member> {
        self.selected.and_then(|at| self.index.members.get(at))
    }

    /// The member called `name`.
    pub fn member(&self, name: &str) -> Option<&Member> {
        self.index.find(name).map(|(_, member)| member)
    }

    /// `member` as a file: the extents it is made of, over `sources`. An
    /// error here is a translator that computed an offset outside its
    /// source, which `MemberView::new` refuses to build rather than
    /// leaving for the middle of a film.
    pub fn view(
        &self,
        member: &Member,
        sources: Vec<Arc<dyn ByteSource>>,
    ) -> io::Result<MemberView> {
        let Body::Direct(extents) = &member.body else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not served by range", member.name),
            ));
        };
        MemberView::new(member.name.clone(), sources, extents.clone())
    }
}

/// Where a session's bytes come from, and -- the part that matters -- for
/// how long it holds the way in.
pub enum SessionSources {
    /// Held for the session's life. An HTTP entity read through the proxy
    /// cache owns nothing and registers nothing, so keeping the source is
    /// keeping a URL, a length and a validator.
    Held(Vec<Arc<dyn ByteSource>>),
    /// A file of a torrent, **opened per read and dropped with the body**.
    ///
    /// A `TorrentFileSource` registers a stream on its torrent for as long
    /// as it lives (`sources::torrent::TorrentMemberStream`), and that
    /// registration is what tells this server a player is reading: hold
    /// one for the session's ten idle minutes and the reconciler cannot
    /// stop a torrent the viewer left ten minutes ago. So the session
    /// keeps the *index*, which is what was expensive to read, and each
    /// body opens its own source -- which is a file lookup and a
    /// reconcile, not a fetch.
    Torrent { info_hash: String, path: String },
}

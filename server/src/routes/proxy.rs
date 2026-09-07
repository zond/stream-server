use crate::state::AppState;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header, response::Builder},
    response::{IntoResponse, Response},
    routing::any,
};
use dashmap::DashSet;
use futures_util::StreamExt;
use reqwest::{Client, Method};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{LazyLock, OnceLock};
use url::Url;

/// Lazily-built, process-wide reqwest client for the proxy route: the one
/// that **verifies** the origin's certificate, which is every request until
/// one fails.
///
/// `Client::builder().build()` can fail (e.g. if the TLS backend can't be
/// initialized), so building it once at startup-on-first-use and reusing it
/// avoids both a per-request `.unwrap()` panic and the cost of rebuilding a
/// client for every proxied request.
static HTTP_CLIENT: OnceLock<Option<Client>> = OnceLock::new();

/// The same client with verification off, built only if some host actually
/// needs it. See [`UNVERIFIED_ORIGINS`] for why it exists at all.
static INSECURE_HTTP_CLIENT: OnceLock<Option<Client>> = OnceLock::new();

/// Origins whose certificate this process could not verify -- `scheme`,
/// host and port, as [`Url::origin`] serializes them. The request that
/// discovered one is retried unverified; a later request for the same
/// origin goes straight to the unverified client, so a stream pays the
/// failed handshake once rather than once per range.
///
/// The whole origin, not the host: verification is a property of a TLS
/// endpoint, and a host serving a broken certificate on `:8443` says
/// nothing about the one it serves on `:443`. Keyed by host alone, a
/// failure at either turned verification off for both -- and for the
/// plain-HTTP `http://host` that is not even the same protocol.
///
/// This route was built with `danger_accept_invalid_certs(true)` from its
/// first commit, commented "Parity with rejectUnauthorized: false" -- it is
/// inherited from the closed-source `server.js` proxy this file was ported
/// from, not a response to any host we ever measured. Nothing in the history
/// names a host that needs it. Meanwhile the client that used to fetch a
/// remote stream was mpv; now it is this, so the flag stopped being about a
/// rarely-used route and became how every remote stream is fetched.
///
/// Verifying everything and letting the broken hosts fail would break
/// streams that play today, and we cannot say which ones. So: verify, and
/// when a certificate is the reason a fetch failed, retry that host once
/// without verification and remember it, at WARN, by name. Be plain about
/// what that is worth -- an on-path attacker can produce a certificate
/// error as easily as a misconfigured CDN can, so this stops nothing it
/// could not also trigger. What it buys is that the downgrade is per host,
/// visible in the log, and enumerable: today's blanket silence cannot tell
/// us which hosts to scope it to, and this can.
static UNVERIFIED_ORIGINS: LazyLock<DashSet<String>> = LazyLock::new(DashSet::new);

/// Every origin this process has downgraded, for a test to assert against.
///
/// [`UNVERIFIED_ORIGINS`] is the only record that a downgrade happened at
/// all: over plain HTTP the unverified client behaves identically to the
/// verified one, so marking the wrong endpoint is invisible from outside --
/// which is how a plain-HTTP host came to be marked at all. Exported
/// doc-hidden so the test that pins it can see what was written down.
#[doc(hidden)]
pub fn unverified_origins() -> Vec<String> {
    UNVERIFIED_ORIGINS
        .iter()
        .map(|origin| origin.clone())
        .collect()
}

/// How [`UNVERIFIED_ORIGINS`] is keyed: `https://host:port`, with a default
/// port left off, which is what [`Url::origin`] serializes.
fn origin_key(url: &Url) -> String {
    url.origin().ascii_serialization()
}

/// How many redirects one proxied fetch follows before giving up.
///
/// Ten, which is what reqwest's default policy allowed before this route
/// walked the chain itself: a chain that plays today keeps playing. (The
/// reference allows five.) The limit is also the loop detection -- a
/// redirect ring is a chain that never ends, and counting hops ends it.
const MAX_REDIRECTS: usize = 10;

/// The statuses that mean "the resource is over there": the five reqwest's
/// default policy follows, and the only ones this loop follows either.
///
/// It used to be every status in `300..400` carrying a `Location`, which is
/// what the reference tests (`result.status>=300&&result.status<400&&
/// result.headers.has("location")`). Measured, that followed `300`, `304`,
/// `305` and `306` as well, answering `200` with the body of wherever the
/// `Location` pointed where the client this route replaced relayed the
/// status untouched -- a `304 Not Modified` became a fetch, which is the
/// opposite of what it says.
///
/// `305 Use Proxy` is the one that makes this a security fix rather than a
/// tidy-up. It never named a new location for the resource; it named a
/// *proxy to send the request through*. Following it, with `h=` re-applied
/// on every hop (see the loop in [`proxy`]), let any origin that answers
/// `305` choose the host our credentialed request goes to. Browsers stopped
/// honouring it decades ago for exactly this reason.
const FOLLOWED_REDIRECTS: [StatusCode; 5] = [
    StatusCode::MOVED_PERMANENTLY,
    StatusCode::FOUND,
    StatusCode::SEE_OTHER,
    StatusCode::TEMPORARY_REDIRECT,
    StatusCode::PERMANENT_REDIRECT,
];

/// Where a response says to go next, resolved against the URL it came
/// *from* -- `None` when it is not a redirect this proxy follows.
///
/// Against the current URL, not the origin. The reference resolves
/// `Location` against `dest.href` with the path and fragment cut off, so a
/// relative `Location: seg/2.m3u8` lands at `/seg/2.m3u8` on the host root
/// instead of beside the resource that sent it.
///
/// A `Location` naming any scheme but `http`/`https` is not followed. This
/// route fetches whatever a caller names, so the one thing it must not do
/// is let an *origin* redirect it somewhere a caller could not have asked
/// for.
///
/// The target's scheme is not compared with the one it came from, so an
/// `https` hop may legitimately end at an `http` one -- what such a step
/// down costs is decided where the request is built, not here: the caller's
/// `h=` credentials stop travelling ([`CredentialChain`], which the loop in
/// [`proxy`] asks about each hop and [`CarriedParams`] about each line of a
/// playlist that continues the chain after it).
fn redirect_target(response: &reqwest::Response, from: &Url) -> Option<Url> {
    if !FOLLOWED_REDIRECTS.contains(&response.status()) {
        return None;
    }
    let location = response.headers().get(header::LOCATION)?.to_str().ok()?;
    let target = from.join(location).ok()?;
    matches!(target.scheme(), "http" | "https").then_some(target)
}

/// The `Location` of a redirect this proxy declined to follow, in the form
/// it is relayed in: absolute, so the client resolves it against the host
/// that wrote it.
///
/// A relative `Location` is relative to the URL it came *from*, and the
/// client reading our response never saw that URL -- it asked this server,
/// so `Location: /elsewhere` resolves against `http://127.0.0.1:11470` and
/// points the player back at us, at a path we do not serve. Resolving it
/// here makes no request, so the reason not to rewrite it into a `/proxy/`
/// link is untouched: naming where an origin pointed is not going there.
///
/// A value that is already absolute is relayed byte for byte rather than
/// round-tripped through [`Url`], which would normalise a spelling that is
/// the origin's to choose. That includes one naming a scheme this proxy
/// will not fetch -- an `ftp://` `Location` is exactly the diagnostic the
/// relay was added to preserve, and it needs nothing resolved. So does a
/// value we cannot read as text or cannot resolve at all: whatever the
/// origin wrote is better than nothing.
fn relayed_location(location: &HeaderValue, from: &Url) -> HeaderValue {
    let Ok(written) = location.to_str() else {
        return location.clone();
    };
    if Url::parse(written).is_ok() {
        return location.clone();
    }
    from.join(written)
        .ok()
        .and_then(|absolute| HeaderValue::from_str(absolute.as_str()).ok())
        .unwrap_or_else(|| location.clone())
}

/// Whether a `206`'s `Content-Range` says the part it carries is the whole
/// entity -- `bytes 0-<len-1>/<len>`.
///
/// Which is not a corner: a player that opens a stream with
/// `Range: bytes=0-` to find out whether the origin is seekable gets a
/// `206` back with the entire body in it, and for a playlist that body is
/// one we must still rewrite. A `206` that carries a *part* is a fragment
/// of a playlist, and there is nothing coherent to do with a rewritten
/// fragment: its length is not the length the range promised, and the
/// lines at its edges are cut.
fn covers_the_whole_entity(content_range: &str) -> bool {
    let Some((range, total)) = content_range
        .trim()
        .strip_prefix("bytes ")
        .and_then(|range| range.split_once('/'))
    else {
        return false;
    };
    let Some((first, last)) = range.split_once('-') else {
        return false;
    };
    let (Ok(first), Ok(last), Ok(total)) = (
        first.trim().parse::<u64>(),
        last.trim().parse::<u64>(),
        total.trim().parse::<u64>(),
    ) else {
        return false;
    };
    // `total - 1` rather than `last + 1`, so an origin claiming the last
    // byte is `u64::MAX` is a `false` and not an overflow panic.
    first == 0 && total.checked_sub(1) == Some(last)
}

/// Whether a URL's path names a playlist by its extension.
///
/// Case-insensitively, which the reference is not: `path.extname()` against
/// a list of two lowercase literals does not see `/live/master.M3U8` at
/// all, and only its content-type arm catches one. There is no reason to
/// inherit that -- a filename's case is the origin's spelling, not a
/// statement about the format.
fn names_a_playlist(url: &Url) -> bool {
    let path = url.path().to_ascii_lowercase();
    path.ends_with(".m3u8") || path.ends_with(".m3u")
}

/// The content type the caller *forced* with `r=Content-Type`, folded to
/// lower case -- `None` when it sent none, which is every request but the
/// HLS one stremio-core builds.
///
/// Kept apart from the origin's own value rather than merged over it,
/// because the two are not interchangeable evidence about what this body
/// is. `r=` is addon metadata describing the resource the caller meant;
/// the origin's header describes the bytes that actually arrived. Merged,
/// `r=` *shadowed* the origin, and the classification in [`proxy`] then
/// read a forced `video/mp4` as proof that a genuine
/// `application/x-mpegURL` playlist was not a playlist. Split, `r=` can
/// only add to the verdict -- which is the property the reference has (its
/// two arms are OR'd) and the one the merge lost.
///
/// Folded because the spelling that matters most is not lower case: Apple
/// writes `application/x-mpegURL`, that is what stremio-core sends and what
/// this repo's README uses, and a case-sensitive `contains("mpegurl")` sees
/// none of it.
fn forced_content_type(response_header_overrides: &BTreeMap<String, String>) -> Option<String> {
    response_header_overrides
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.to_ascii_lowercase())
}

/// What the *origin* labelled its own body with, folded the same way --
/// the empty string when it said nothing, which neither forces the
/// playlist path nor vetoes it.
fn origin_content_type(res_headers: &HeaderMap) -> String {
    res_headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase()
}

/// Whether a content type says plainly that this body is *not* a playlist,
/// whatever the URL naming it was called.
///
/// `video/*`, `image/*` and `audio/*` outside the mpegurl family: bodies
/// that would be run through a line rewriter, stripped of their framing and
/// handed to a player as text. `audio/x-mpegurl` and `audio/mpegurl` are
/// the classic `.m3u` spellings, so the family is asked for first and
/// vetoes nothing.
///
/// A type this does not recognise vetoes nothing either, and that is the
/// point of naming a set rather than a complement:
/// `application/octet-stream` is what an indifferent origin labels
/// everything, including the playlist an extension-less edge URL serves,
/// and it says nothing at all. Only a type that positively describes some
/// other medium is allowed to overrule the URL.
///
/// The parameters are cut off first: `video/mp4; charset=binary` is still
/// `video/mp4`.
fn cannot_be_a_playlist(content_type: &str) -> bool {
    let essence = content_type.split(';').next().unwrap_or_default().trim();
    if essence.contains("mpegurl") {
        return false;
    }
    ["video/", "audio/", "image/"]
        .iter()
        .any(|medium| essence.starts_with(medium))
}

fn http_client() -> Option<&'static Client> {
    HTTP_CLIENT
        .get_or_init(|| {
            Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| tracing::error!("Failed to build proxy HTTP client: {e}"))
                .ok()
        })
        .as_ref()
}

fn insecure_http_client() -> Option<&'static Client> {
    INSECURE_HTTP_CLIENT
        .get_or_init(|| {
            Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .danger_accept_invalid_certs(true)
                .build()
                .map_err(|e| tracing::error!("Failed to build unverified proxy HTTP client: {e}"))
                .ok()
        })
        .as_ref()
}

/// Whether a failed fetch failed *because the peer's certificate would not
/// verify*, which is the only failure the unverified retry can help with --
/// a refused connection or a DNS miss must stay an error.
///
/// **Matched by type, never by prose.** This used to be a case-insensitive
/// search for the word "certificate" over every `Display` in the chain, and
/// reqwest writes the request URL into that text: measured with no TLS
/// anywhere in the picture, one request for
/// `http://127.0.0.1:1/certificate-of-authenticity.mkv` -- a refused
/// connection -- put `127.0.0.1` into [`UNVERIFIED_ORIGINS`] for the life of
/// the process. A filename could turn certificate verification off.
///
/// The error that actually says so is a
/// `rustls::Error::InvalidCertificate`, which covers every reason a chain
/// can be rejected (unknown issuer, expired, not valid for the name) and is
/// exactly the set `danger_accept_invalid_certs` waives. Measured, it sits
/// under reqwest's connect error inside **two** nested `io::Error`s -- and
/// `source()` alone walks straight past it, because `io::Error::source`
/// delegates to the inner error's source instead of yielding the inner
/// error itself. So every node is asked for its `get_ref` as well as its
/// `source`.
///
/// The rustls type comes from `tokio_rustls`, which re-exports the same
/// `rustls` reqwest links (one entry in the lockfile, and the whole
/// workspace is on one TLS backend on purpose). If that ever stopped being
/// true the downcast would simply stop matching, and the failure would be
/// a stream that is not retried rather than a host quietly downgraded.
fn is_certificate_error(error: &(dyn std::error::Error + 'static)) -> bool {
    use tokio_rustls::rustls;

    // Both links out of each node are followed, and the walk is bounded
    // rather than trusting an error chain not to loop.
    let mut pending: Vec<&(dyn std::error::Error + 'static)> = vec![error];
    let mut visited = 0;
    while let Some(error) = pending.pop() {
        visited += 1;
        if visited > 32 {
            break;
        }
        if matches!(
            error.downcast_ref::<rustls::Error>(),
            Some(rustls::Error::InvalidCertificate(_))
        ) {
            return true;
        }
        if let Some(inner) = error
            .downcast_ref::<std::io::Error>()
            .and_then(|error| error.get_ref())
        {
            pending.push(inner);
        }
        if let Some(source) = error.source() {
            pending.push(source);
        }
    }
    false
}

/// The header names `r=` may not set, because they describe how *this*
/// response is framed rather than what it contains -- and framing it is
/// hyper's business, decided from the body actually being written.
///
/// They are the same three the origin's own values are never relayed
/// under, for the same reason, and `r=` is if anything the more dangerous
/// source: it is addon metadata, and an addon that says
/// `r=Content-Length:1` in front of a two-gigabyte film panics the
/// connection task in a debug build ("payload claims content-length of ...,
/// custom content-length header claims 1") and, in a release build, leaves
/// the player waiting for bytes that will never come.
const UNFRAMEABLE_RESPONSE_HEADERS: [&str; 3] =
    ["content-length", "transfer-encoding", "connection"];

/// Applies the `r=` custom response headers to a response builder,
/// validating each name/value pair first so that a malicious or malformed
/// header (e.g. containing a newline) can never poison the builder's
/// internal error state. Invalid pairs -- and every name in
/// [`UNFRAMEABLE_RESPONSE_HEADERS`] -- are skipped and logged at debug
/// level rather than propagated.
///
/// They **replace**, which is the whole of what `r=` is for. Appending
/// them, as this did, left the origin's own header in place beside the
/// override and a client reading the first of two `content-type`s got the
/// origin's -- which is exactly the value stremio-core sends `r=` to
/// correct. `Builder::header` appends; `HeaderMap::insert` takes the name
/// over entirely, which is why the map is reached through `headers_mut`
/// rather than the builder's own method. A builder already in an error
/// state has no map to reach, and skipping is right there too: it is about
/// to become a 502 (see [`finalize_response`]).
fn apply_custom_response_headers(
    mut builder: Builder,
    custom_response_headers: &BTreeMap<String, String>,
) -> Builder {
    for (name, value) in custom_response_headers {
        match (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            (Ok(header_name), Ok(header_value)) => {
                if UNFRAMEABLE_RESPONSE_HEADERS.contains(&header_name.as_str()) {
                    tracing::debug!(
                        name = %header_name,
                        value = %value,
                        "Skipping a framing header from r= proxy param: this response is \
                         framed by the body being written, not by the addon"
                    );
                    continue;
                }
                if let Some(headers) = builder.headers_mut() {
                    headers.insert(header_name, header_value);
                }
            }
            _ => {
                tracing::debug!(
                    name = %name,
                    value = %value,
                    "Skipping invalid custom response header from r= proxy param"
                );
            }
        }
    }
    builder
}

/// The `h=` custom request headers as a header map, validated the same way
/// and for the same reason as the response ones.
///
/// A map rather than a series of `RequestBuilder::header` calls because
/// those *append*: an addon's `h=User-Agent:...` used to be sent alongside
/// the player's own, two `user-agent` headers on one request, and which of
/// them the origin honoured was its business. `RequestBuilder::headers`
/// replaces the name outright, which is what an override means.
fn custom_request_headers(overrides: &BTreeMap<String, String>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in overrides {
        match (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            (Ok(header_name), Ok(header_value)) => {
                headers.insert(header_name, header_value);
            }
            _ => {
                tracing::debug!(
                    name = %name,
                    value = %value,
                    "Skipping invalid custom request header from h= proxy param"
                );
            }
        }
    }
    headers
}

/// The `h=` names that are credentials rather than description: the three
/// reqwest's own redirect policy calls sensitive and strips across hosts
/// (`remove_sensitive_headers`).
///
/// Nothing else this route sends can be one. The player's own headers are
/// a fixed allow-list (see `build_request`) with no credential in it, so
/// `h=` is the only way a secret reaches an origin at all.
const CREDENTIAL_REQUEST_HEADERS: [&str; 3] = ["authorization", "cookie", "proxy-authorization"];

/// The same header map with every name in [`CREDENTIAL_REQUEST_HEADERS`]
/// left out -- what a hop that no longer deserves the caller's secret is
/// built with.
///
/// The rest of `h=` still goes: a `User-Agent`, a `Referer` or an
/// addon's own `X-` header describes the request and is not a secret to
/// spend, and dropping those would break the stream for no gain.
///
/// [`ProxyParams::carried`] does the same filtering to the `h=` it writes
/// into a rewritten playlist line, where the names are the caller's own
/// strings rather than a header map's.
fn without_credentials(headers: &HeaderMap) -> HeaderMap {
    headers
        .iter()
        .filter(|(name, _)| !CREDENTIAL_REQUEST_HEADERS.contains(&name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// The chain a request for the caller's resource has taken so far, and the
/// **one predicate** that decides whether the next request in it may carry
/// [`CREDENTIAL_REQUEST_HEADERS`].
///
/// There are two requests that decision has to be made for, and they are
/// made in different places: the next hop of the redirect loop in
/// [`proxy`], and the request a *player* makes for a line this route wrote
/// into a rewritten playlist (see [`CarriedParams`], which asks this about
/// every line). A rewritten line is as much a hop of this chain as a `302`
/// is -- it is written by us, fetched automatically, and re-arms `h=` from
/// whatever we wrote into it -- so the two must answer the same, and they
/// answer the same by both calling [`CredentialChain::may_carry_to`].
///
/// **The rule it encodes**, which is the whole of the guarantee -- the
/// credentials leave the origin the caller named only over `https`:
///
/// * an **`https` chain** -- one every URL of which, the caller's own
///   included, is `https` -- carries them to any `https` target, across
///   hosts. That is a deliberate trade, made against reqwest's
///   `remove_sensitive_headers`, and it is what buys back the CDN-to-edge
///   `403` (see the loop in [`proxy`]);
/// * **any other chain** carries them to the origin the caller named in
///   `d=` when the caller named it in the clear, and to nowhere else --
///   whatever the target's scheme, at any depth. Such a caller published
///   the credential on that origin itself, by naming it, so a request
///   pointing back there publishes nothing that is not published already,
///   and an authenticated plain-`http` stream would lose every segment
///   without it. Anywhere else is a fresh disclosure somebody other than
///   the caller chose: what named it was a `Location` or a playlist line
///   that crossed a wire anyone on the path could read and rewrite.
///
/// Two things follow from the second arm that the rule needs, and neither
/// is a case of its own. A chain that began at an `https` URL and stepped
/// down has published nothing and has nowhere to spend anything: it carries
/// the credentials to no target at all, an `https` one included, because an
/// `https` URL named over a wire that was read is not the caller's `https`
/// origin talking. And a chain that began in the clear does not reach the
/// same host's TLS port either -- `https://a.example` is not the origin
/// `http://a.example`, which is the same answer the loop gives a `302` from
/// cleartext to `https`.
///
/// **Why this is one type and not two conditions that agree.** It is the
/// fourth round on this rule. Each of the first three fixed a real
/// asymmetry between the loop and the rewriter -- the loop dropping the
/// credential on a cleartext hop while the rewriter re-armed it one line
/// later; the rewriter's cleartext exception keyed on the scheme, so any
/// cleartext host got it; then keyed on the origin the playlist came
/// *from*, so a cleartext `302` disarmed the caller's own host and armed
/// the redirect target -- and each time the two conditions were left as
/// two. The fourth was the direction nobody had just tested: the loop
/// refused a cleartext chain's `https` hop and the rewriter allowed the
/// same chain's `https` line. Measured, a caller naming `http://A` with
/// `h=Authorization:Bearer s3cret` and `h=Cookie:session=abc` got back a
/// playlist naming `https://C`, armed; C logged both when the line was
/// fetched the way a player fetches one; and C's own playlist -- an `https`
/// chain of its own by then -- armed `https://D`, which logged them too.
/// Two hosts the caller never named, from a chain the loop would not have
/// carried one hop of. Two implementations of one policy agree only by
/// discipline, and discipline has now failed four times.
///
/// `Url::origin` is the comparison because it is the value a rewritten line
/// is written with (`d=` is an origin), and because it says the things a
/// host comparison here has to say: `a.example` and `cdn.a.example` are
/// different hosts; two ports on one host are different listeners -- one on
/// `:8080` is not the one the credential was handed to, and on a shared
/// host it is often not even the same party's; and a default port spelled
/// out is neither.
#[derive(Clone)]
struct CredentialChain {
    /// The origin the caller named in `d=`, when the caller named a
    /// cleartext one: the single origin this chain has already published
    /// the credentials to, and so the only one a chain that is not all
    /// `https` may hand them to. `None` for a chain that began at an
    /// `https` URL, which has published nothing.
    spent_in_the_clear: Option<url::Origin>,
    /// Whether every URL this chain has visited, the caller's own included,
    /// was `https`.
    all_tls: bool,
}

impl CredentialChain {
    /// The chain as the caller named it, before a hop has been taken. The
    /// caller's own URL is already part of it: naming an `http://` target
    /// is the caller spending the credential on that origin, and naming an
    /// `https` one is what an `https` chain is.
    fn named_by(caller: &Url) -> Self {
        let over_tls = caller.scheme() == "https";
        Self {
            spent_in_the_clear: (!over_tls).then(|| caller.origin()),
            all_tls: over_tls,
        }
    }

    /// Whether a request for `target` -- the next hop of the redirect loop,
    /// or the fetch a player will make for a rewritten line -- may carry
    /// [`CREDENTIAL_REQUEST_HEADERS`]. The rest of `h=` travels either way
    /// (see [`without_credentials`]).
    fn may_carry_to(&self, target: &Url) -> bool {
        if self.all_tls {
            target.scheme() == "https"
        } else {
            self.spent_in_the_clear
                .as_ref()
                .is_some_and(|spent_on| *spent_on == target.origin())
        }
    }

    /// Records a hop the chain has taken. Only a chain every URL of which
    /// is `https` is an `https` chain, and nothing turns one back into one.
    fn stepped_to(&mut self, hop: &Url) {
        self.all_tls &= hop.scheme() == "https";
    }
}

/// Finishes building a response, turning a builder error (which can no
/// longer happen for headers we control, but is handled defensively for any
/// other builder failure) into a 502 instead of panicking via `.unwrap()`.
fn finalize_response(builder: Builder, body: axum::body::Body) -> Response {
    match builder.body(body) {
        Ok(resp) => resp.into_response(),
        Err(e) => {
            tracing::error!("Failed to build proxy response: {e}");
            (StatusCode::BAD_GATEWAY, "Proxy response error").into_response()
        }
    }
}

/// The proxy's own parameters, in whichever URL shape carried them: the
/// target (`d=`), the request headers to send with it (`h=`), the response
/// headers to send back (`r=`), and the client's name for the player
/// reading the stream (`p=`).
///
/// One parser for both shapes. The Core format spells them in the path
/// segment before the target's path, the query format in the request's own
/// query, and until this struct existed only the Core format could express
/// `h=`/`r=` at all -- which meant the playlist rewrite could not carry an
/// authenticated playlist's headers into the segments it named. Measured:
/// the playlist fetched `200`, every segment `403`, the origin logging
/// `auth=[]`.
///
/// `BTreeMap` rather than `HashMap` because the order the headers come back
/// out in is written into every line of every rewritten playlist, and a
/// body that differs between two identical requests is not something to
/// hand a caching player.
#[derive(Debug, Default, PartialEq, Eq)]
struct ProxyParams {
    /// The target URL, as `d=` spelled it. In the Core format this is the
    /// origin, and the request path is appended to it.
    target: String,
    /// `h=Name:Value` -- sent to the origin, replacing whatever we would
    /// otherwise have forwarded under that name.
    request_headers: BTreeMap<String, String>,
    /// `r=Name:Value` -- sent back to the player, replacing whatever the
    /// origin said under that name.
    response_headers: BTreeMap<String, String>,
    /// `p=<token>` -- never sent to the origin. See [`crate::proxy_streams`].
    player_token: Option<String>,
}

impl ProxyParams {
    /// Reads the four parameters out of one `application/x-www-form-
    /// urlencoded` string, whether it came off the path segment or the
    /// query. Anything else in it belongs to the target and is ignored
    /// here.
    fn parse(query: &str) -> Self {
        let mut params = Self::default();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "d" => params.target = value.into_owned(),
                // Header format "Name:Value", for the request and the
                // response respectively.
                "h" => {
                    if let Some((name, value)) = value.split_once(':') {
                        params
                            .request_headers
                            .insert(name.trim().to_string(), value.trim().to_string());
                    }
                }
                "r" => {
                    if let Some((name, value)) = value.split_once(':') {
                        params
                            .response_headers
                            .insert(name.trim().to_string(), value.trim().to_string());
                    }
                }
                "p" if !value.is_empty() => params.player_token = Some(value.into_owned()),
                _ => {}
            }
        }
        params
    }

    /// Everything a rewritten playlist line has to carry besides its own
    /// target, spelled as query parameters: `h=` and `p=`.
    ///
    /// `h=` because a segment of an authenticated stream needs the same
    /// authorization the playlist needed -- without it the playlist fetched
    /// `200` and every segment `403`. `p=` because the segment read has to
    /// belong to the same player as the playlist that named it: closing an
    /// HLS player has to break the read that is actually in flight, and
    /// that is a segment, never the playlist. `p=` has no counterpart in
    /// the reference at all -- it is this fork's, and a reader diffing
    /// against `server.js` will not find it there.
    ///
    /// **`r=` is deliberately not here.** It is a response-header override
    /// for *the resource the caller named*, and the caller named a
    /// playlist: stremio-core sends `r=Content-Type:application/x-mpegurl`
    /// for an HLS stream, so copying it onto every line labelled the
    /// segments and the AES keys as playlists too. mpv was handed MPEG-TS
    /// under `application/x-mpegurl` and a 16-byte key under it as well.
    ///
    /// The reference does copy it, on every same-origin line and every
    /// absolute path, because its virtual root is the caller's whole opts
    /// string -- and there it compounds: it classifies a response by the
    /// content type it has already merged `r=` into, so a segment fetched
    /// through such a line is itself called a playlist and run through the
    /// line rewriter. We ask the caller's forced type too, and for a good
    /// reason (see [`forced_content_type`]); what keeps the same thing from
    /// happening here is exactly this -- `r=` is not on the line, so a
    /// segment inherits no label. Its own cross-origin branch drops `r=`
    /// (`newOpts` has only `d` and `h`), which is the half worth keeping.
    ///
    /// **Nor is all of `h=` on every line.** Which lines
    /// [`CREDENTIAL_REQUEST_HEADERS`] may be written into is not this
    /// function's decision at all: `chain` is the chain the playlist
    /// arrived over -- what the caller named, and what has happened to it
    /// since -- and every line asks [`CredentialChain::may_carry_to`] about
    /// its own target, which is the same call the redirect loop makes about
    /// its next hop. Both spellings are built here because the target is
    /// not known until [`proxied_uri`] has resolved the line.
    fn carried(&self, chain: &CredentialChain) -> CarriedParams {
        let mut all = String::new();
        let mut uncredentialed = String::new();
        for (name, value) in &self.request_headers {
            let parameter = format!("&h={}", urlencoding::encode(&format!("{name}:{value}")));
            // `h=` names are the caller's spelling, not a `HeaderMap`'s, so
            // the comparison has to do the lowercasing the header map would
            // have done: `h=AUTHORIZATION:...` is the same credential.
            if !CREDENTIAL_REQUEST_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
                uncredentialed.push_str(&parameter);
            }
            all.push_str(&parameter);
        }
        if let Some(token) = &self.player_token {
            let parameter = format!("&p={}", urlencoding::encode(token));
            all.push_str(&parameter);
            uncredentialed.push_str(&parameter);
        }
        CarriedParams {
            credentialed: all,
            uncredentialed,
            chain: chain.clone(),
        }
    }
}

/// What a rewritten playlist line carries, in the two spellings a line may
/// need. [`ProxyParams::carried`] builds both; the line's own target picks
/// between them, by asking [`CredentialChain`] -- the same predicate, and
/// the same call, the redirect loop makes about its next hop.
///
/// **A rewritten line is a hop of the same chain** -- the last one this
/// route has any say over. What it writes into the line is what the player
/// hands straight back to us as its own `h=`, and what we then spend on
/// whatever that line named, without a caller ever having decided to. That
/// is why the rule cannot live here: a rule enforced on the lines and not
/// in the loop, or the other way round, holds for one request and leaks on
/// the next, and it has done so in all four directions the two could
/// disagree in (see [`CredentialChain`] for the four). What is left here is
/// the two strings and where a line's target is asked about.
///
/// Without a rule on the lines the loop's guard was one line deep.
/// Measured: an `https` origin serving a playlist that names
/// `http://…/seg-0.ts` had the caller's `Authorization` and `Cookie`
/// written into the segment's URL, and the player -- which fetches segments
/// by itself, that being what a playlist is for -- delivered them to the
/// cleartext origin the loop's guard exists to keep them from. The same one
/// line defeated the guard end to end for a chain that *had* downgraded:
/// the playlist hop was correctly asked with no credential, and the
/// playlist it returned re-armed `h=` for every segment.
///
/// The cleartext half is the caller's origin and not the *playlist's*,
/// which is how it shipped once and is a different origin the moment a
/// cleartext `302` is in the chain. Measured: a caller naming
/// `http://A/live/master.m3u8` with `h=Authorization:Bearer s3cret` was
/// redirected to `http://B/edge/master.m3u8`, whose playlist named
/// `http://A/back-on-a.ts` -- and the line home to A was written with no
/// `h=` at all, A logging `authorization=None`, so every segment of an
/// authenticated stream `403`s, while B's own lines were armed and handed
/// the credential to a host the caller never named. Keyed on `d=`'s own
/// origin, which is what [`CredentialChain`] holds, both halves come out
/// right.
///
/// **A chain that is not all `https` arms the origin the caller named in the
/// clear and no other, at any depth, whatever a line's scheme** -- and a
/// chain that began at `https` and stepped down arms nothing at all, having
/// named nothing in the clear to spend on. A line to A is fetched as
/// `d=http://A`, so the origin *it* may arm is A again, and nothing else in
/// its playlist -- an `https` line included -- is written with a credential
/// for anything further on to inherit. That bound is that chain's alone: an
/// armed `https` line on an `https` chain is a fresh
/// request whose own redirects and own playlist may carry the credential
/// onward to further `https` hosts, and nothing counts those --
/// `MAX_REDIRECTS` bounds the hops inside one request, and a rewritten line
/// is not a hop of the request that wrote it but a new one the player
/// makes. Every one of those hosts is reached over TLS, which is the whole
/// of what the one rule promises.
struct CarriedParams {
    /// What a line the credentials may travel on carries.
    credentialed: String,
    /// What every other line carries: the same, with
    /// [`CREDENTIAL_REQUEST_HEADERS`] left out.
    uncredentialed: String,
    /// The chain the playlist arrived over, which is what every line is
    /// judged against -- and the only thing here that decides.
    chain: CredentialChain,
}

impl CarriedParams {
    /// The spelling `target` has earned, which is [`CredentialChain`]'s
    /// answer and nothing of this type's own.
    fn for_target(&self, target: &Url) -> &str {
        if self.chain.may_carry_to(target) {
            &self.credentialed
        } else {
            &self.uncredentialed
        }
    }

    /// The same parameters whatever a line names -- what the line tests
    /// below are written against, since they are about the rewriting and
    /// not about the credential rule. Both spellings are one string, so
    /// the chain cannot change the answer; it is here because the type has
    /// one.
    #[cfg(test)]
    fn everywhere(carried: &str) -> Self {
        Self {
            credentialed: carried.to_string(),
            uncredentialed: carried.to_string(),
            // Any chain: with one string for both answers, what this
            // says cannot be observed.
            chain: CredentialChain::named_by(
                &Url::parse("https://carried.invalid/").expect("a literal URL"),
            ),
        }
    }
}

/// Every shape `/proxy` answers, at absolute paths and merged rather than
/// nested under the prefix.
///
/// `nest("/proxy", ...)` cannot express all three. It registers the prefix
/// itself plus a `{*tail}` wildcard beneath it, and a wildcard matches at
/// least one character -- so `/proxy/` matches neither and is a router-level
/// `404` before any handler runs. That is exactly the URL the query format
/// has, and back when a playlist rewrite wrote that format into every line,
/// an HLS stream fetched through the proxy handed the player a playlist
/// whose every segment 404ed. Rewritten lines are in the path format now
/// (see [`proxied_uri`]), but the query format is still read: callers
/// write it.
pub fn router() -> Router<AppState> {
    Router::new()
        // The original JS uses /proxy/:opts/:pathname*
        // We can use a wildcard capturing the whole path.
        .route("/proxy/{*rest}", any(proxy_handler))
        // The query format, with or without the trailing slash.
        .route("/proxy", any(proxy_root_handler))
        .route("/proxy/", any(proxy_root_handler))
}

/// `/proxy/?d=<url>`: the whole target in the query, nothing in the path.
///
/// The one format the wildcard above cannot express. Its query carries the
/// same proxy parameters the Core format carries in its path segment
/// (`d=`, `h=`, `r=`, `p=`), read by the same parser, because the whole
/// target in one parameter has to be able to say everything a target named
/// by path can.
pub async fn proxy_root_handler(
    State(state): State<AppState>,
    raw_query: axum::extract::RawQuery,
    headers: HeaderMap,
    method: Method,
) -> impl IntoResponse {
    proxy(state, None, raw_query.0, headers, method).await
}

/// The Core path format, read from the URI rather than from the router's
/// capture, because the capture is percent-*decoded*.
///
/// Both [`Path`] and `RawPathParams` decode what the wildcard matched, and
/// the target's path is not ours to decode: `%2F` became a path separator,
/// `%3F` began a query and everything from a `%23` on was read as a
/// fragment and lost. Measured end to end, `https://host/a%2Fb/film.mkv`
/// reached the origin as `GET /a/b/film.mkv` -- a signed link whose path
/// segment carries a base64 signature gets a 403, and a file named with a
/// `#` gets a 404. The URI's own path is the target as it came off the wire, so
/// what the caller encoded is what the origin is asked for. It also means
/// the `d=`/`h=`/`r=` segment is decoded exactly once, by
/// `form_urlencoded` -- a header value carrying a `%` or a `&` used to be
/// decoded twice and lose its meaning.
pub async fn proxy_handler(
    State(state): State<AppState>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    headers: HeaderMap,
    method: Method,
) -> impl IntoResponse {
    // The route this handler serves is `/proxy/{*rest}`, so the prefix is
    // always there; an empty rest could only come of a router change, and it
    // answers 400 the way any unparseable target does.
    let rest = uri.path().strip_prefix("/proxy/").unwrap_or_default();
    proxy(state, Some(rest.to_string()), raw_query, headers, method).await
}

/// `rest` is what the path held after `/proxy/`, and *that is what decides
/// the format*: `None` -- nothing in the path -- is the query format
/// (`/proxy/?d=<url>`), anything else is the Core path format
/// (`/proxy/d=<origin>&h=.../<path>`).
///
/// It used to be decided by asking whether the request's query had a `d`
/// parameter, which is a name the target URL may own too. A Core-format
/// request for `/proxy/d=<encoded>/film.mkv?d=1&t=2` took `d="1"` as the
/// whole target, failed to parse it and answered `400 Invalid target URL`;
/// worse, a `d` value that happened to parse as a URL would have been
/// fetched *instead of* the target the caller named. The path shape cannot
/// be spoofed by the target's own query, so the path shape decides.
async fn proxy(
    state: AppState,
    rest: Option<String>,
    raw_query: Option<String>,
    headers: HeaderMap,
    method: Method,
) -> Response {
    // Porting the logic from express_805.js
    // Format 1: ?d=URL (standard)
    // Format 2: /<query_params>/<path> (Core) where query_params contains d=ORIGIN&h=HEADER&r=RESPONSE_HEADER

    let is_path_format = rest.is_some();
    let params = match &rest {
        None => ProxyParams::parse(raw_query.as_deref().unwrap_or_default()),
        Some(rest) => {
            // The Core path format: /proxy/d=...&h=.../path/to/file. The
            // segment before the first slash is the proxy's own parameters,
            // everything after it is the target's path.
            let (query_seg, path_seg) = match rest.split_once('/') {
                Some((q, p)) => (q, p),
                None => (rest.as_str(), ""),
            };
            let mut params = ProxyParams::parse(query_seg);
            if params.target.is_empty() {
                // Fallback: assume whole rest is the URL (legacy/simple proxy)
                params.target = rest.clone();
            } else if !path_seg.is_empty() {
                // `d=` is the origin, the rest of the path is the file on it.
                if !params.target.ends_with('/') {
                    params.target.push('/');
                }
                params.target.push_str(path_seg);
            }
            params
        }
    };
    // The client's name for the player this stream is for, if it minted one
    // (`p=`). It is ours, not the target's: it never travels to the origin,
    // and it is what `POST /proxy-streams/{token}/close` addresses. See
    // [`crate::proxy_streams`].
    let player_token = params.player_token.clone();

    let mut url = match Url::parse(&params.target) {
        Ok(u) => u,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid target URL").into_response(),
    };

    if is_path_format && let Some(q) = raw_query {
        url.set_query(Some(&q));
    }

    // A token that has been closed is not given another stream. ffmpeg
    // reconnects through the URL it already has -- token and all -- so
    // without this the close is a stutter rather than an end: measured,
    // three closes on one live reader produced three fresh origin fetches
    // at the offsets the closes interrupted. `410 Gone` because that is
    // exactly what happened: this stream was here and was deliberately
    // ended. A `404` would read as a target that never existed and send a
    // client looking for a typo in its URL. The check comes before the
    // fetch, so a refusal costs the origin nothing -- but it is only the
    // cheap half of the refusal: the fetch below takes as long as the origin
    // takes, and the token can be retired while it does. The registration
    // asks again (see the `attach` below).
    if let Some(token) = player_token.as_deref()
        && state.proxy_streams.is_closed(token)
    {
        tracing::debug!(
            token = %token,
            "refusing a proxied read for a player token that was closed"
        );
        return (
            StatusCode::GONE,
            "This player's stream was closed by its client",
        )
            .into_response();
    }

    let custom_request_headers = custom_request_headers(&params.request_headers);
    let uncredentialed_request_headers = without_credentials(&custom_request_headers);
    let build_request = |client: &Client, url: &Url, carry_credentials: bool| {
        let mut req_builder = client.request(method.clone(), url.clone());

        // What the player asked for, forwarded as it asked for it.
        // `connection` and `transfer-encoding` are deliberately absent: both
        // describe the framing of one hop, and this is a new hop -- reqwest
        // frames its own request, and a `transfer-encoding: chunked` copied
        // from a bodyless player request describes a body that is not there.
        let allowed_req_headers = [
            "accept",
            "accept-language",
            "range",
            "if-range",
            "user-agent",
        ];

        for name in allowed_req_headers {
            if let Some(value) = headers.get(name) {
                req_builder = req_builder.header(name, value);
            }
        }

        // `accept-encoding` is answered here rather than forwarded. This
        // client has no gzip/brotli/deflate feature, so it decodes nothing,
        // and a playlist arrives as bytes we cannot rewrite -- while the
        // player's own `accept-encoding: gzip` invited exactly that. Asking
        // for `identity` says what we can actually take. An origin that
        // compresses anyway is still relayed honestly: `content-encoding`
        // travels back with the body it describes (see the relayed-body
        // headers below).
        req_builder = req_builder.header(header::ACCEPT_ENCODING, "identity");

        // The `h=` overrides last, and replacing rather than adding to what
        // the player sent: an override that leaves the original in place
        // is not one. Every hop of a redirect chain is built through here,
        // so every hop gets them (see the loop below) -- minus the
        // credentials once the chain has stepped down to cleartext.
        req_builder = req_builder.headers(if carry_credentials {
            custom_request_headers.clone()
        } else {
            uncredentialed_request_headers.clone()
        });
        req_builder
    };

    // The redirect chain is walked here, one hop at a time, rather than
    // left to reqwest -- and the reason is `h=`. reqwest's default policy
    // strips `Authorization`, `Cookie` and `Proxy-Authorization` on any
    // cross-host *or cross-port* redirect, which is exactly the shape of an
    // authenticated stream behind a CDN that hands off to an edge: the
    // playlist fetched `200` and everything it named `403`, with the origin
    // logging no credential at all. The reference's answer is structural --
    // `redirect: "manual"`, its own loop, and
    // `opts.h.forEach(headers.set(...))` re-applied on every hop -- and this
    // is that: each hop is built by `build_request`, so each hop carries the
    // headers the caller asked for.
    //
    // Say plainly what that costs, because it was chosen and not
    // overlooked: reqwest's policy calls those three headers sensitive and
    // drops them across hosts (`remove_sensitive_headers`) precisely so a
    // redirect cannot walk a credential to a host the caller never named,
    // and re-applying `h=` per hop gives that protection up. What is left
    // holding the line is the set of statuses we follow
    // ([`FOLLOWED_REDIRECTS`]) and the hop bound ([`MAX_REDIRECTS`]): the
    // credential travels only where the *resource* moved, only a bounded
    // number of times, and never to a host an origin nominated as a proxy
    // to route us through. The header is the addon's and it goes where the
    // origin sent the resource; that is the trade the caller made by naming
    // a header for a stream, and the alternative is the `403`.
    //
    // **One thing that trade does not cover is the wire going cleartext.**
    // [`redirect_target`] takes any `http` or `https` target without
    // comparing it to the scheme it came from, and a `Location` that
    // arrived in the clear is not something the origin can be said to have
    // written. A `302` from `https` to `http` would have the caller's
    // `Authorization` -- or `Cookie` -- re-applied on a hop anyone on the
    // path can read: not "the credential goes where the resource went" but
    // the origin choosing to publish it, and no `403` is avoided by
    // obliging. A `302` sent *over* cleartext named its target in the clear
    // too, so whoever could read the credential could also have chosen who
    // receives it next -- which is why even a target on the very host that
    // redirected us is not one this hop can trust the answer about, and why
    // an upgrade back to `https` brings nothing back.
    //
    // Which hops carry them is therefore not decided here. It is decided by
    // [`CredentialChain::may_carry_to`], which this loop asks about every
    // hop and which is asked again, unchanged, about every line of a
    // playlist the chain comes back with. What it answers is one rule, and
    // it is the whole of the guarantee: **the credentials leave the origin
    // the caller named only over `https`.** An `https` chain carries them
    // across `https` hosts, which is the trade that buys the `403` above
    // back; any other chain spends them on `url` when the caller named
    // `url` in the clear -- and by naming it chose to publish them there --
    // and on nobody else, at any depth, an `https` target included. A chain
    // that started `https` and stepped down has published nothing and so
    // spends nothing anywhere. The rest of `h=` still travels either way
    // (see [`without_credentials`]).
    //
    // "Every hop" has to include the ones this route does not make itself.
    // A playlist is rewritten line by line into `/proxy/` URLs the player
    // then fetches, and each of those lines is written with `h=` on it --
    // so a chain that ended here is continued, credentials and all, by the
    // very next request the player makes. That the two agree is no longer
    // something this comment asks of whoever edits the other one: they are
    // the same call ([`CarriedParams::for_target`] is the other caller),
    // because for four rounds agreement was a discipline, and four times
    // the discipline failed.
    //
    // The method is kept across hops, as the reference keeps it. A `303`
    // asks for a `GET` and a browser would give it one, but this route is
    // reached with a `GET`, a `HEAD` or an `OPTIONS` from a player and
    // never with a body, so there is nothing for the distinction to change.
    let mut fetched_url = url.clone();
    let mut hops = 0usize;
    // What the caller named, and what has happened to the chain since.
    // Nothing else in this loop decides what a request carries.
    let mut chain = CredentialChain::named_by(&url);
    let response = loop {
        // Asked per hop, and the one question there is to ask: the same
        // call decides every line of a playlist this chain returns.
        let carry_credentials = chain.may_carry_to(&fetched_url);
        // The origin is fetched verified unless a previous request for this
        // endpoint failed on its certificate (see [`UNVERIFIED_ORIGINS`]).
        // Asked per hop, which it could not be while reqwest owned the
        // chain: a downgrade recorded for a redirect *target* used to be
        // invisible here, so such a chain paid its failed handshake again
        // on every request rather than once.
        let known_unverified = UNVERIFIED_ORIGINS.contains(&origin_key(&fetched_url));
        let client = match if known_unverified {
            insecure_http_client()
        } else {
            http_client()
        } {
            Some(c) => c,
            None => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Proxy client unavailable",
                )
                    .into_response();
            }
        };

        let response = match build_request(client, &fetched_url, carry_credentials)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) if !known_unverified && is_certificate_error(&e) => {
                // The endpoint whose handshake failed, which is this hop and
                // no other -- walking the chain ourselves is what makes that
                // simply true. It used to be inferred from a task-local the
                // redirect policy wrote, because reqwest attributes a
                // connect failure to the URL the request *started* at: an
                // `https` -> `https` chain permanently downgraded the *good*
                // host and left the bad one verified.
                //
                // Only a TLS endpoint has a certificate to waive. Nothing
                // else can produce this error, so this is a guard rather
                // than a case: if it ever fires, the honest answer is to
                // fail rather than downgrade a guess.
                if fetched_url.scheme() != "https" {
                    tracing::warn!(
                        url = %fetched_url,
                        error = %e,
                        "a certificate failed verification, but not at an https URL we can \
                         name; not retrying"
                    );
                    return (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e))
                        .into_response();
                }
                // One retry, for this endpoint only, and say so once per
                // process.
                let downgraded = origin_key(&fetched_url);
                tracing::warn!(
                    origin = %downgraded,
                    error = %e,
                    "certificate verification failed; retrying this origin unverified for \
                     the life of the process"
                );
                UNVERIFIED_ORIGINS.insert(downgraded);
                let Some(client) = insecure_http_client() else {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Proxy client unavailable",
                    )
                        .into_response();
                };
                match build_request(client, &fetched_url, carry_credentials)
                    .send()
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        return (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e))
                            .into_response();
                    }
                }
            }
            Err(e) => {
                return (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e)).into_response();
            }
        };

        let Some(location) = redirect_target(&response, &fetched_url) else {
            break response;
        };
        if hops >= MAX_REDIRECTS {
            tracing::warn!(url = %url, "too many redirects; giving up");
            return (StatusCode::BAD_GATEWAY, "Proxy error: too many redirects").into_response();
        }
        chain.stepped_to(&location);
        if carry_credentials && !chain.may_carry_to(&location) {
            // At WARN only when there was something to drop, and by header
            // name rather than value: a stream that now `403`s has to be
            // diagnosable, and a chain with no credential in it is not an
            // event. Once per hop that loses them rather than once per
            // chain, since a chain that is not all `https` may step back
            // onto the origin the caller named and be carrying them again.
            let dropped: Vec<&str> = custom_request_headers
                .keys()
                .map(|name| name.as_str())
                .filter(|name| CREDENTIAL_REQUEST_HEADERS.contains(name))
                .collect();
            if !dropped.is_empty() {
                tracing::warn!(
                    from = %fetched_url,
                    to = %location,
                    headers = ?dropped,
                    "a redirect leaves what this chain may spend the caller's h= \
                     credentials on; not carrying them onto it"
                );
            }
        }
        hops += 1;
        tracing::debug!(from = %fetched_url, to = %location, "following a proxied redirect");
        fetched_url = location;
    };

    // `fetched_url` is where the body actually came from, `url` where the
    // caller pointed us. A playlist's relative lines are relative to the URL
    // it *arrived* at: rewriting against the URL we asked for sends every
    // segment back to the host that redirected us, and to its directory,
    // which for a CDN-to-edge `302` -- the ordinary HLS deployment -- is
    // every segment of every stream served that way.
    let status = response.status();
    let res_headers = response.headers().clone();

    // The caller's forced type and the origin's own, asked separately: see
    // the three arms below, and [`forced_content_type`] for why merging
    // them was the bug.
    let forced_content_type = forced_content_type(&params.response_headers);
    let origin_content_type = origin_content_type(&res_headers);
    // Both URLs are asked, because either one alone has a blind spot. The
    // URL the *caller* named is the one an HLS player knows it asked for,
    // and it is the only evidence left when a redirect lands on an
    // extension-less URL an indifferent origin labels
    // `application/octet-stream` -- testing the fetched path alone stopped
    // rewriting that stream at all. The URL the body *came from* is the one
    // that catches the other direction, a caller naming an extension-less
    // URL that redirects to a `.m3u8`. The reference tests only the
    // pre-redirect path (its `dest` is the router's, untouched by the
    // redirect loop) and leans on its content-type arm for the rest.
    //
    // But a name is only evidence, and the body gets a veto: a URL that
    // ends `.m3u8` and answers with an MP4 is an MP4. Measured -- a caller
    // naming `/s/index.m3u8`, the origin redirecting to `/movie.mp4` and
    // serving 39,998 bytes of `video/mp4` -- the response lost its
    // `Content-Length`, claimed `Accept-Ranges: none`, dropped
    // `Content-Range`, `ETag` and `Last-Modified`, turned a `206` into a
    // `200`, and ran the video through the line rewriter; ffmpeg then
    // failed on it. **The reference has the same weakness and we are
    // deliberately not keeping it**: its `path.extname(dest.pathname)` is
    // the pre-redirect, caller-named path, so nothing there stops a named
    // `.m3u8` that serves a film. See [`cannot_be_a_playlist`] for what
    // counts as a veto.
    //
    // Only the *origin's* type vetoes, because only the origin has seen the
    // bytes. What a caller forces with `r=Content-Type` may add the
    // playlist verdict and may never take it away -- which is exactly what
    // the reference's OR of two arms buys, and what merging `r=` into one
    // effective type here threw away. Measured: an origin serving
    // `application/x-mpegURL` and a caller sending
    // `r=Content-Type:video/mp4` -- a perfectly ordinary thing for an addon
    // to say about the stream it describes -- had the playlist relayed
    // verbatim, so the player then fetched every segment straight from the
    // origin, without the `h=` those segments needed and without the `p=` a
    // close is addressed by. An empty `r=Content-Type:` did the same, by
    // shadowing the origin's type with nothing at all. `r=` is an escape
    // hatch *into* the rewrite; it was acting as an escape hatch out of it.
    let is_playlist = origin_content_type.contains("mpegurl")
        || forced_content_type
            .as_deref()
            .is_some_and(|forced| forced.contains("mpegurl"))
        || ((names_a_playlist(&url) || names_a_playlist(&fetched_url))
            && !cannot_be_a_playlist(&origin_content_type));

    // A body under a content coding we cannot decode is a body we must not
    // rewrite: the lines are not text yet. We relay it whole instead --
    // its segment URLs then point straight at the origin, which loses the
    // `h=` request headers, so say so rather than serving the player a
    // rewritten playlist made of compressed bytes.
    let content_encoding = res_headers
        .get(header::CONTENT_ENCODING)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let encoded_body =
        !content_encoding.is_empty() && !content_encoding.eq_ignore_ascii_case("identity");
    // Only a body that is actually a playlist is rewritten as one, and a
    // status code is half of what says so. A 404's error page served at a
    // `.m3u8` URL was being rewritten line by line and handed back as a
    // playlist of fabricated proxy URLs -- an origin's "Not found" became a
    // segment list. It falls through to the plain relay, which is what it
    // always should have been.
    // A rewritten body replaces the origin's, so the response has to be one
    // that *is* the whole body. `status.is_success()` was not that test: a
    // `206` passed it, and a rewritten fragment of a playlist is a body
    // whose length is not the length the range promised and whose edge
    // lines are cut in half. A `206` that carries the whole entity is
    // different, and it is not a corner -- it is what an origin answers the
    // `Range: bytes=0-` a player opens a stream with -- so it is rewritten
    // and answered as the `200` it has become. The reference guards none of
    // this; it rewrites a `206` and relays its `Content-Range` beside a body
    // that no longer matches it.
    let whole_body = status == StatusCode::OK
        || (status == StatusCode::PARTIAL_CONTENT
            && res_headers
                .get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(covers_the_whole_entity));
    // Whether the body this response describes is one we would replace.
    // Not the same question as whether we are writing one now: a `HEAD`
    // has no body to rewrite, so it is not rewritten -- but it still
    // *describes* the resource a `GET` would be answered with, and that is
    // a rewritten playlist.
    //
    // Answering it from the relay branch instead had `HEAD` and `GET`
    // disagree about the same URL: measured, the `HEAD` advertised the
    // origin's `Content-Length: 67` and `Accept-Ranges: bytes` while the
    // `GET` returned 199 chunked bytes and `Accept-Ranges: none`, so a
    // player that sized the resource and then sent `Range: bytes=0-66` got
    // a `200` carrying 199. Framing headers for a body we would not serve
    // are worse than none: every field below is now decided by what the
    // resource *is*, and only the body itself by the method.
    let rewritable_body = is_playlist && !encoded_body && whole_body;
    let rewriting_playlist = rewritable_body && method != Method::HEAD;
    if is_playlist && encoded_body {
        tracing::warn!(
            content_encoding = %content_encoding,
            url = %fetched_url,
            "relaying a compressed playlist unrewritten; its segments will bypass the proxy"
        );
    }
    if is_playlist && !whole_body && status.is_success() && method != Method::HEAD {
        tracing::warn!(
            status = %status,
            url = %fetched_url,
            "relaying part of a playlist unrewritten; its segments will bypass the proxy"
        );
    }

    // A rewritten playlist is the whole resource however it was asked for,
    // so it is answered `200` even when the origin said `206` -- and a
    // `HEAD` for it says the same, since what it describes is that same
    // response.
    let mut res_builder = Response::builder().status(if rewritable_body {
        StatusCode::OK
    } else {
        status
    });

    // What the origin said about the *resource*: true of whatever we send
    // back, because none of it describes the bytes on this hop.
    let resource_res_headers = ["content-type", "server", "date"];
    for name in resource_res_headers {
        if let Some(value) = res_headers.get(name) {
            res_builder = res_builder.header(name, value);
        }
    }

    // A `3xx` still here is one the loop above declined to follow -- a
    // status outside [`FOLLOWED_REDIRECTS`], a `Location` naming a scheme
    // this proxy will not fetch, or none at all -- and it is relayed with
    // its status. Without its `Location` it was relayed with nothing else:
    // measured, a player got `302 Found`, the CORS headers and
    // `content-length: 0`, which makes an unfollowable redirect and a
    // headerless one the same dead end, and neither one diagnosable.
    //
    // Not rewritten into a proxy URL of our own: we are declining to follow
    // it, and handing the player a `/proxy/` link to the same place would
    // be making the request anyway with extra steps -- for a `305` that is
    // the whole of what must not happen. Absolutised, though, because a
    // relative `Location` relayed as written is resolved by the client
    // against *this* server. See [`relayed_location`].
    if status.is_redirection()
        && let Some(location) = res_headers.get(header::LOCATION)
    {
        res_builder =
            res_builder.header(header::LOCATION, relayed_location(location, &fetched_url));
    }

    // What the origin said about *its own body*: only true of a body we hand
    // on byte for byte. A rewritten playlist is a different body, and the
    // origin's framing copied onto it is a lie hyper catches -- with
    // `Content-Length` the connection task panics ("payload claims
    // content-length of 180, custom content-length header claims 82"), with
    // `Transfer-Encoding: chunked` it closes having written nothing, and only
    // a close-delimited origin survived by accident. That was every proxied
    // HLS stream failing to play. `Accept-Ranges`, `Content-Range`, `ETag`
    // and `Last-Modified` go with it: they all describe the entity at the
    // origin, and a client that acted on them -- ranging into the rewritten
    // playlist, or caching it under the origin's tag -- would be acting on
    // the wrong bytes. `content-encoding` belongs to the same set and for
    // the same reason -- it names the coding of *these* bytes, and dropping
    // it (as this route used to) hands the player gzip labelled as identity.
    // `connection` and `transfer-encoding` are relayed in neither branch:
    // framing this response is hyper's job, not the origin's.
    let relayed_body_res_headers = [
        "accept-ranges",
        "content-encoding",
        "content-length",
        "content-range",
        "last-modified",
        "etag",
    ];
    if rewritable_body {
        // And ranging into a body we wrote is ranging into the wrong
        // entity, so say so rather than leave a player to infer it from a
        // missing header. The reference sets this too
        // (`responseHeaders["accept-ranges"]="none"`), which is the one
        // thing it does with a rewritten playlist's headers that we did
        // not.
        //
        // A `HEAD` takes this branch as well, and so answers with no length
        // and no claim to ranges: the only honest thing to say about a body
        // whose length is not known until it has been written.
        res_builder = res_builder.header(header::ACCEPT_RANGES, "none");
    } else {
        for name in relayed_body_res_headers {
            if let Some(value) = res_headers.get(name) {
                res_builder = res_builder.header(name, value);
            }
        }
    }

    // Apply custom response headers (Core format), validated so a malformed
    // r= param can never poison the response builder.
    res_builder = apply_custom_response_headers(res_builder, &params.response_headers);

    // CORS headers
    res_builder = res_builder
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS")
        .header(header::ACCESS_CONTROL_ALLOW_HEADERS, "*");

    if rewriting_playlist {
        // The playlist is rewritten as it arrives, a line at a time, and
        // handed to hyper as a stream. Nothing measures it: the whole of
        // the framing trouble this route has had came of buffering the body
        // so a `Content-Length` could be declared for it, and a body hyper
        // frames from what is actually written cannot disagree with its own
        // headers.
        //
        // It is read through the registry, exactly as a media body is, and
        // that is the point: a live-HLS player refreshing its playlist
        // against an origin that has stopped answering is a read wedged in
        // here, and this branch used to return before `attach` ever ran --
        // so the one read this feature exists for was the one read it could
        // not reach. Streaming it makes the close plainer still: the
        // registry's stream *is* the body now, so a close breaks the
        // player's read directly rather than a drain it is waiting behind.
        let Some(chunks) = state
            .proxy_streams
            .attach(player_token.clone(), response.bytes_stream())
        else {
            tracing::debug!(
                token = player_token.as_deref().unwrap_or_default(),
                "a player token was closed while its playlist was being fetched"
            );
            return (
                StatusCode::GONE,
                "This player's stream was closed by its client",
            )
                .into_response();
        };
        // Lines resolve against `fetched_url`, where the playlist came
        // from; which of them may be written with the credentials in `h=`
        // is a question for `chain`, which knows what the *caller* named
        // and what the hops since have cost. The two URLs part company at a
        // redirect, and only the first of them says where a line resolves.
        let carried = params.carried(&chain);
        let rewritten = rewritten_playlist_body(chunks, fetched_url, carried);
        return finalize_response(res_builder, axum::body::Body::from_stream(rewritten));
    }

    // Registered under the client's token, so the client can end this exact
    // read rather than waiting out a timeout meant for a slow swarm -- or
    // refused, if the close landed while the origin was still thinking. Same
    // `410` and for the same reason as the check above: the stream was asked
    // for, and its client ended it before a byte of it arrived.
    let Some(stream) = state
        .proxy_streams
        .attach(player_token.clone(), response.bytes_stream())
    else {
        tracing::debug!(
            token = player_token.as_deref().unwrap_or_default(),
            "a player token was closed while its origin was being fetched"
        );
        return (
            StatusCode::GONE,
            "This player's stream was closed by its client",
        )
            .into_response();
    };
    finalize_response(res_builder, axum::body::Body::from_stream(stream))
}

/// `POST /proxy-streams/{token}/close`: end every proxied stream the client
/// marked with `token`, and say how many that was.
///
/// It also retires the token, which is the half that makes it stick: the
/// closed reads break, and any later `/proxy` request bearing the same `p=`
/// is answered `410 Gone` instead of being given a fresh stream. Without
/// that, ffmpeg's `reconnect=1` re-fetches through the URL it already has
/// and playback carries on.
///
/// A **control** route -- bearer token, loopback listener, and never on the
/// LAN media listener, which serves no control route at all: the ability to
/// cut another device's playback is not something to hand the network. The
/// same operation is [`crate::ServerHandle::close_proxy_streams`], through
/// this same function, so an embedder needs no HTTP client for it.
pub async fn close_proxy_streams(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    let closed = state.proxy_streams.close(&token);
    tracing::debug!(closed, "closing proxied streams by player token");
    Json(serde_json::json!({ "closed": closed }))
}

/// One line of a rewritten playlist, or the line unchanged when there is
/// nothing in it to rewrite. `carried` is [`ProxyParams::carried`] -- the
/// `h=`/`p=` the playlist's own URL arrived with, which every line it names
/// needs too: a segment of an authenticated stream needs the same
/// authorization the playlist needed, and closing an HLS player has to
/// break the read in flight, which is a segment. What a segment does *not*
/// need is the caller's `r=`; see [`ProxyParams::carried`]. It is
/// [`CarriedParams`] rather than a string because the credentials in `h=`
/// depend on where the line points -- the choice is made in
/// [`proxied_uri`], which is where the target is known.
///
/// **Every line that names a resource is rewritten, relative ones
/// included.** The reference leaves those alone, correctly for itself: a
/// relative line is resolved by the player against the URL the player asked
/// for, which under the path format already points back through the proxy.
/// Two things stop us inheriting that. A redirect moves the directory the
/// lines are relative to and the player cannot know -- the URL it asked for
/// is the CDN's, the one the playlist came from is the edge's, and `base`
/// here is the second (see
/// `a_playlist_reached_through_a_redirect_is_rewritten_against_the_edge`).
/// And the URL the player asked for carries the caller's `r=`, so a line
/// left alone would re-acquire on the segment the very label this rewrite
/// exists to keep off it. Resolving every line costs bytes in the playlist
/// and buys both.
///
/// A line is one of three things and the reference reads them the same way:
/// a tag (`#…`), where only a `URI="…"` attribute names a resource; an
/// empty line; or a URI. Where we differ is in *finding* the URI inside a
/// tag. The reference matches `URI="([^"]+)"` and splices the result back
/// with `line.replace(uri[1], …)`, which replaces the first occurrence of
/// that *substring* anywhere in the line -- so
/// `#EXT-X-KEY:METHOD=AES-128,IV=0xabc,URI="0xabc"` rewrites the IV and
/// leaves the key URI alone. The value is spliced by index here, so it is
/// the attribute that moves and nothing else.
///
/// Like the reference, only the first `URI="…"` on a line is rewritten. No
/// HLS tag carries two, but say so rather than leave it looking exhaustive.
fn rewrite_line<'a>(line: &'a str, base: &Url, carried: &CarriedParams) -> Cow<'a, str> {
    const URI_ATTRIBUTE: &str = "URI=\"";

    if line.starts_with('#') {
        let Some(start) = line.find(URI_ATTRIBUTE) else {
            return Cow::Borrowed(line);
        };
        let value = start + URI_ATTRIBUTE.len();
        let Some(end) = line[value..].find('"').map(|end| value + end) else {
            return Cow::Borrowed(line);
        };
        match proxied_uri(&line[value..end], base, carried) {
            Some(proxied) => Cow::Owned(format!("{}{proxied}{}", &line[..value], &line[end..])),
            None => Cow::Borrowed(line),
        }
    } else if line.is_empty() {
        Cow::Borrowed(line)
    } else {
        match proxied_uri(line, base, carried) {
            Some(proxied) => Cow::Owned(proxied),
            None => Cow::Borrowed(line),
        }
    }
}

/// The path one URI named by a playlist takes back through this proxy:
/// `/proxy/d=<origin>&h=…&p=…/<path on that origin>[?<query>]`, with `uri`
/// resolved against `base` -- the URL the playlist itself came from.
///
/// **The path format, not the `/proxy/?d=<whole url>` query format this
/// used to write**, and that is the load-bearing half of the port. A
/// query-format URL has no directory. A media playlist named by a master
/// one is rewritten like everything else, so the player fetches it at
/// `/proxy/?d=…media.m3u8` -- and then resolves *its* relative lines
/// against that, where `seg-0.ts` becomes `/proxy/seg-0.ts` and 404s at
/// our own router before it ever becomes a request to the origin. The path
/// format mirrors the origin's path structure underneath the proxy's
/// mount, so a nested playlist's own relative lines land back here at the
/// right origin. It is what the reference builds its `virtualRoot` for,
/// and the reason its rewritten lines have no query format to be written
/// in.
///
/// All four line forms -- absolute URL, absolute path, protocol-relative
/// and relative -- go through this one call, because [`Url::join`] already
/// distinguishes them. The reference spells out three branches and gets two
/// of them wrong: it tests for an absolute URL with
/// `startsWith("http://")`, so `HTTP://host/…` falls through to its
/// absolute-path branch untouched, and `//host/path` hits that branch too
/// and is mangled into `/proxy/<opts>/host/path`. Our own `contains("://")`
/// test had the mirror-image fault, reading a relative line whose query
/// carries `?u=http://x` as absolute.
///
/// `None` when the line does not resolve to an `http(s)` URL at all -- a
/// `data:` URI, or something that is not a URL. Such a line is left exactly
/// as the origin wrote it, since there is nothing this proxy could fetch
/// for it.
fn proxied_uri(uri: &str, base: &Url, carried: &CarriedParams) -> Option<String> {
    // A blank URI names nothing. [`Url::join`] disagrees -- it strips the
    // whitespace and hands back `base` itself -- so a line of spaces, or an
    // `URI=""`, would otherwise be rewritten into a proxy URL for the
    // playlist that contains it. (The reference's `URI="([^"]+)"` cannot
    // match an empty one, and a blank line falls out of its URI branch
    // untouched; this is the same answer for both.)
    if uri.trim().is_empty() {
        return None;
    }
    let target = base.join(uri).ok()?;
    if !matches!(target.scheme(), "http" | "https") {
        return None;
    }
    // This is where the line's target -- and so its scheme and its origin
    // -- is finally known, which makes it where the caller's credentials
    // are either written into the line or left out of it.
    let carried = carried.for_target(&target);
    // `d=` is the bare origin and the path rides in the URL's own path,
    // which is the invariant the path format's handler depends on: it
    // *appends* the request path to `d=`. (The reference instead
    // *replaces* `d=`'s pathname with the request path, which comes to the
    // same thing only because its `d=` is always a bare origin too.)
    let mut proxied = format!(
        "/proxy/d={}{carried}{}",
        urlencoding::encode(&target.origin().ascii_serialization()),
        target.path()
    );
    // The target's own query travels in the rewritten URL's query, where
    // this route reads it back off the wire and puts it on the origin
    // request -- a signed CDN URL is a path plus a token, and it is the
    // token that makes it fetchable. (The reference writes the query into
    // the line too, and then drops it on the next hop: it assigns
    // `dest.search = req.search || ""`, and nothing in its server ever sets
    // `req.search`.)
    if let Some(query) = target.query() {
        proxied.push('?');
        proxied.push_str(query);
    }
    Some(proxied)
}

/// The longest line the rewriter will hold before deciding the body it is
/// reading is not line-oriented after all.
///
/// A streaming rewrite has to keep the bytes since the last `\n` until a
/// `\n` arrives to complete them, and nothing about a response guarantees
/// one ever does: a `.m3u8` URL that answers with a megabyte of MPEG-TS is
/// enough to make that buffer the whole body. A playlist line is a tag or a
/// URI, so 64 KiB is orders of magnitude more than any real one; past it,
/// the bytes are handed on as they came and the rest of that line with
/// them.
const LONGEST_REWRITABLE_LINE: usize = 64 * 1024;

/// A playlist rewriter that takes the body a chunk at a time.
///
/// The reference streams too, through a `stream.Transform` that keeps the
/// tail after the last separator in a `partialLine` and prepends it to the
/// next chunk; this is that, and the reason for it is the same. Buffering
/// the whole body to measure it was where our framing bugs came from: the
/// rewritten length had to be declared, so a `Content-Length` had to be
/// written, and a `206` or a `HEAD` measured the wrong thing. A body handed
/// to hyper as a stream is framed by hyper from what it actually writes.
///
/// **Line endings are preserved per line**, which is better than the
/// reference and cheaper. It detects the ending once, from the first chunk
/// that contains one, and re-emits that for the whole body -- and its
/// detection has three faults worth naming so nobody ports them back: with
/// both characters present and `\n` first it returns the literal `"\n\r"`,
/// an ending that does not exist; it scans the whole buffered chunk, so a
/// stray `\r` anywhere in a large first chunk mis-detects the body; and
/// with no terminator in the first chunk at all it splits on `null`, which
/// JavaScript coerces to the string `"null"`. Splitting on `\n` and putting
/// back whatever `\r` the line already carried needs none of that, and a
/// body with mixed endings comes out as it went in. A lone `\r` is not a
/// line ending here; the HLS specification says lines end `\n` or `\r\n`.
struct PlaylistRewriter {
    /// The URL the playlist came from -- what its lines are relative to.
    base: Url,
    /// [`ProxyParams::carried`]: the `h=`/`p=` every line it writes
    /// carries, in the two spellings a line may have earned.
    carried: CarriedParams,
    /// Bytes since the last `\n`, waiting for the one that completes them.
    pending: Vec<u8>,
    /// Set when [`pending`](Self::pending) outgrew
    /// [`LONGEST_REWRITABLE_LINE`] and its head has already gone out
    /// unrewritten: the rest of that one line follows it verbatim.
    passing_through: bool,
}

impl PlaylistRewriter {
    fn new(base: Url, carried: CarriedParams) -> Self {
        Self {
            base,
            carried,
            pending: Vec::new(),
            passing_through: false,
        }
    }

    /// Every line `chunk` completes, rewritten; the rest is held for the
    /// chunk that completes it.
    fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        self.pending.extend_from_slice(chunk);
        let mut rewritten = Vec::new();
        while let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=end).collect();
            self.write_line(&line[..end], &mut rewritten);
            rewritten.push(b'\n');
        }
        if self.passing_through || self.pending.len() > LONGEST_REWRITABLE_LINE {
            self.passing_through = true;
            rewritten.append(&mut self.pending);
        }
        rewritten
    }

    /// What is left when the origin's body ends: the tail after the last
    /// `\n`, rewritten, with **no** terminator added.
    ///
    /// Which is how the presence or absence of a final newline survives the
    /// rewrite -- `body.lines()`, which this replaced, could not tell
    /// `"a\nb"` from `"a\nb\n"` and invented one for both. The reference's
    /// `flush` does the same thing for the same reason.
    fn finish(&mut self) -> Vec<u8> {
        let pending = std::mem::take(&mut self.pending);
        let mut rewritten = Vec::new();
        self.write_line(&pending, &mut rewritten);
        rewritten
    }

    fn write_line(&mut self, line: &[u8], rewritten: &mut Vec<u8>) {
        if self.passing_through {
            rewritten.extend_from_slice(line);
            self.passing_through = false;
            return;
        }
        let (line, carriage_return) = match line.strip_suffix(b"\r") {
            Some(line) => (line, true),
            None => (line, false),
        };
        match std::str::from_utf8(line) {
            Ok(text) => rewritten
                .extend_from_slice(rewrite_line(text, &self.base, &self.carried).as_bytes()),
            // A playlist is UTF-8 by specification, so a line that is not
            // holds no URI to rewrite. It is passed on as it came rather
            // than through `from_utf8_lossy`, which this used to do to the
            // whole body: replacing bytes we cannot read with U+FFFD
            // corrupts them on their way to a player that might have
            // understood them.
            Err(_) => rewritten.extend_from_slice(line),
        }
        if carriage_return {
            rewritten.push(b'\r');
        }
    }
}

/// The origin's playlist as a stream of rewritten chunks.
///
/// Nothing here measures anything, which is the point: the response is
/// framed by hyper from the bytes actually written. The reference reaches
/// the same place by hand, deleting `content-length` and forcing
/// `transfer-encoding: chunked` -- a header we must not set ourselves, and
/// do not need to.
fn rewritten_playlist_body<S, C, E>(
    chunks: S,
    base: Url,
    carried: CarriedParams,
) -> impl futures_util::Stream<Item = Result<Vec<u8>, E>>
where
    S: futures_util::Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
{
    futures_util::stream::unfold(
        Some((chunks, PlaylistRewriter::new(base, carried))),
        |state| async move {
            let (mut chunks, mut rewriter) = state?;
            loop {
                match chunks.next().await {
                    Some(Ok(chunk)) => {
                        let rewritten = rewriter.push(chunk.as_ref());
                        // A chunk that completes no line has nothing to
                        // send yet; an empty one on the wire would be a
                        // frame that says nothing.
                        if rewritten.is_empty() {
                            continue;
                        }
                        return Some((Ok(rewritten), Some((chunks, rewriter))));
                    }
                    // The read failed part-way. The error ends the body,
                    // which is what a player has to see: half a playlist
                    // delivered cleanly would parse as a stream that stops.
                    Some(Err(error)) => return Some((Err(error), None)),
                    None => {
                        let tail = rewriter.finish();
                        return (!tail.is_empty()).then_some((Ok(tail), None));
                    }
                }
            }
        },
    )
}

/// A whole playlist through the rewriter in one call, for the tests that
/// are about the lines rather than about the chunking.
#[cfg(test)]
fn rewrite_playlist(body: &str, base: &Url, carried: &str) -> String {
    rewrite_playlist_carrying(body, base, CarriedParams::everywhere(carried))
}

/// The same, for the tests that are about which lines carry the caller's
/// credentials and which do not.
#[cfg(test)]
fn rewrite_playlist_carrying(body: &str, base: &Url, carried: CarriedParams) -> String {
    let mut rewriter = PlaylistRewriter::new(base.clone(), carried);
    let mut rewritten = rewriter.push(body.as_bytes());
    rewritten.append(&mut rewriter.finish());
    String::from_utf8(rewritten).expect("text in, text out")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Url {
        Url::parse("http://example.com/streams/master.m3u8").unwrap()
    }

    /// A rewritten line, spelled the way the path format spells it: the
    /// proxy's mount, the target's origin in `d=`, the `h=`/`p=` the
    /// request carried, and then the target's own path and query.
    fn proxied(target: &str) -> String {
        proxied_with(target, "")
    }

    fn proxied_with(target: &str, carried: &str) -> String {
        let target = Url::parse(target).expect("a test names a target it can parse");
        let mut proxied = format!(
            "/proxy/d={}{carried}{}",
            urlencoding::encode(&target.origin().ascii_serialization()),
            target.path()
        );
        if let Some(query) = target.query() {
            proxied.push('?');
            proxied.push_str(query);
        }
        proxied
    }

    #[test]
    fn relative_segment_is_joined_against_base_and_wrapped() {
        let body = "seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://example.com/streams/seg-0.ts"))
        );
    }

    #[test]
    fn absolute_http_line_is_wrapped_without_double_joining() {
        let body = "http://cdn.example.org/other/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://cdn.example.org/other/seg-0.ts"))
        );
    }

    #[test]
    fn absolute_https_line_is_wrapped_without_double_joining() {
        let body = "https://cdn.example.org/other/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("https://cdn.example.org/other/seg-0.ts"))
        );
    }

    #[test]
    fn ext_x_media_uri_is_rewritten_and_other_attributes_are_preserved() {
        let body = concat!(
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",",
            "URI=\"audio/en.m3u8\",DEFAULT=YES,AUTOSELECT=YES\n"
        );
        let rewritten = rewrite_playlist(body, &base(), "");
        let expected = format!(
            concat!(
                "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",",
                "URI=\"{}\",DEFAULT=YES,AUTOSELECT=YES\n"
            ),
            proxied("http://example.com/streams/audio/en.m3u8")
        );
        assert_eq!(rewritten, expected);
    }

    #[test]
    fn ext_x_media_with_absolute_uri_is_wrapped_without_double_joining() {
        let body = concat!(
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",",
            "URI=\"https://cdn.example.org/audio/en.m3u8\"\n"
        );
        let rewritten = rewrite_playlist(body, &base(), "");
        let expected = format!(
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"aud\",NAME=\"English\",URI=\"{}\"\n",
            proxied("https://cdn.example.org/audio/en.m3u8")
        );
        assert_eq!(rewritten, expected);
    }

    /// Pins current behavior: EXT-X-KEY's URI is rewritten through the same
    /// generic `URI="..."` handling as EXT-X-MEDIA, so encryption key
    /// fetches ARE proxied (not left pointing at the origin directly). If
    /// that's ever intentionally changed, update this test alongside it.
    #[test]
    fn ext_x_key_uri_is_proxied_like_other_uri_attributes() {
        let body = "#EXT-X-KEY:METHOD=AES-128,URI=\"key/enc.key\",IV=0x0123456789abcdef\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        let expected = format!(
            "#EXT-X-KEY:METHOD=AES-128,URI=\"{}\",IV=0x0123456789abcdef\n",
            proxied("http://example.com/streams/key/enc.key")
        );
        assert_eq!(rewritten, expected);
    }

    #[test]
    fn root_relative_path_resolves_against_origin() {
        let body = "/videos/seg-0.ts\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://example.com/videos/seg-0.ts"))
        );
    }

    /// A protocol-relative line, which the reference's `startsWith("/")`
    /// branch mangles into `/proxy/<opts>/host/path` -- a path on the
    /// playlist's own origin, named after the host that was meant to serve
    /// it. [`Url::join`] knows the form, so it costs us nothing to get
    /// right.
    #[test]
    fn a_protocol_relative_line_keeps_the_host_it_names() {
        let rewritten = rewrite_playlist("//cdn.example.org/other/seg-0.ts\n", &base(), "");
        assert_eq!(
            rewritten,
            format!("{}\n", proxied("http://cdn.example.org/other/seg-0.ts"))
        );
    }

    /// A relative line whose *query* contains a scheme. The absolute test
    /// used to be `line.contains("://")`, which read this as an absolute
    /// URL and handed `Url::parse` a relative path.
    #[test]
    fn a_query_that_looks_like_a_url_does_not_make_the_line_absolute() {
        let rewritten = rewrite_playlist("seg-0.ts?u=http://origin/x\n", &base(), "");
        assert_eq!(
            rewritten,
            format!(
                "{}\n",
                proxied("http://example.com/streams/seg-0.ts?u=http://origin/x")
            )
        );
    }

    /// The signed URL's whole point: the token in the query is what makes
    /// the segment fetchable, so it has to survive into the line we write
    /// and be there again when the player comes back through it.
    #[test]
    fn a_segment_s_own_query_survives_the_rewrite() {
        let rewritten = rewrite_playlist("seg-0.ts?token=abc&e=1700\n", &base(), "&p=one");
        assert_eq!(
            rewritten,
            format!(
                "{}\n",
                proxied_with(
                    "http://example.com/streams/seg-0.ts?token=abc&e=1700",
                    "&p=one"
                )
            )
        );
    }

    /// The whole reason the lines are written in the path format: a media
    /// playlist named by a master one keeps its directory under the proxy's
    /// mount, so the relative lines *it* contains resolve to a URL this
    /// route serves. Under the query format they resolved to
    /// `/proxy/seg-0.ts` and 404ed at our own router.
    #[test]
    fn a_nested_playlist_keeps_a_directory_for_its_own_relative_lines() {
        let rewritten = rewrite_playlist("v/720p/media.m3u8\n", &base(), "&p=one");
        let line = rewritten.trim_end();
        let (directory, _) = line.rsplit_once('/').expect("a path format line has one");
        assert_eq!(
            format!("{directory}/seg-0.ts"),
            proxied_with("http://example.com/streams/v/720p/seg-0.ts", "&p=one"),
            "what the player will resolve `seg-0.ts` to is the segment beside the media playlist"
        );
    }

    /// The reference splices a rewritten `URI="…"` back with
    /// `line.replace(uri[1], …)`, which replaces the first occurrence of
    /// that *substring* anywhere in the line. Here the IV happens to equal
    /// the URI, so the reference rewrites the IV and leaves the key alone.
    /// The value is spliced by index, so only the attribute moves.
    #[test]
    fn a_tag_attribute_that_repeats_the_uri_is_left_where_it_is() {
        let body = "#EXT-X-KEY:METHOD=AES-128,IV=0xabc,URI=\"0xabc\"\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(
            rewritten,
            format!(
                "#EXT-X-KEY:METHOD=AES-128,IV=0xabc,URI=\"{}\"\n",
                proxied("http://example.com/streams/0xabc")
            )
        );
    }

    /// A blank line, and a tag attribute that names nothing. `Url::join`
    /// strips the whitespace and hands back the base, so both used to be
    /// rewritten into a proxy URL for the playlist itself -- a segment
    /// list in which the playlist is one of its own segments.
    #[test]
    fn a_line_that_names_nothing_is_not_turned_into_the_playlist_s_own_url() {
        assert_eq!(rewrite_playlist("   \n", &base(), ""), "   \n");
        let empty_attribute = "#EXT-X-KEY:METHOD=NONE,URI=\"\"\n";
        assert_eq!(
            rewrite_playlist(empty_attribute, &base(), ""),
            empty_attribute
        );
    }

    /// A line this proxy could not fetch anything for is a line to leave
    /// alone rather than to invent a `d=` for.
    #[test]
    fn a_line_that_is_not_an_http_url_is_left_as_the_origin_wrote_it() {
        let body = "#EXT-X-KEY:METHOD=AES-128,URI=\"data:text/plain;base64,AAAA\"\n";
        assert_eq!(rewrite_playlist(body, &base(), ""), body);
    }

    /// The player's token travels into every line the rewrite writes, so a
    /// segment fetch belongs to the same player as the playlist that named
    /// it -- otherwise closing an HLS player would close its playlist read
    /// and leave the segment in flight, which is the read that matters.
    #[test]
    fn the_player_token_is_carried_into_every_rewritten_line() {
        let body = "#EXT-X-KEY:METHOD=AES-128,URI=\"key/enc.key\"\nseg-0.ts\n";
        let params = ProxyParams::parse("d=whatever&p=player+one");
        let rewritten = rewrite_playlist_carrying(
            body,
            &base(),
            params.carried(&CredentialChain::named_by(&base())),
        );
        let expected = format!(
            "#EXT-X-KEY:METHOD=AES-128,URI=\"{}\"\n{}\n",
            proxied_with("http://example.com/streams/key/enc.key", "&p=player%20one"),
            proxied_with("http://example.com/streams/seg-0.ts", "&p=player%20one")
        );
        assert_eq!(rewritten, expected);
    }

    /// The body arriving in chunks that fall wherever the network puts
    /// them, including inside a URI and between the `\r` and the `\n`. What
    /// comes out is what the whole body would have produced -- that is the
    /// whole contract of a streaming rewrite.
    #[test]
    fn a_body_split_across_chunks_rewrites_to_the_same_bytes() {
        let body = "#EXTM3U\r\n#EXTINF:10,\r\nseg-0.ts\r\nhttps://cdn.example.org/s/1.ts\r\n";
        let whole = rewrite_playlist(body, &base(), "&p=one");

        for split in 1..body.len() {
            let mut rewriter = PlaylistRewriter::new(base(), CarriedParams::everywhere("&p=one"));
            let mut streamed = rewriter.push(&body.as_bytes()[..split]);
            streamed.append(&mut rewriter.push(&body.as_bytes()[split..]));
            streamed.append(&mut rewriter.finish());
            assert_eq!(
                String::from_utf8(streamed).expect("text in, text out"),
                whole,
                "split at {split}"
            );
        }
    }

    /// Line endings survive per line, `\r\n` and `\n` alike, and a body that
    /// ended without one still does. `body.lines()`, which this replaced,
    /// turned a CRLF playlist into an LF one and invented a final newline
    /// for a body that had none.
    #[test]
    fn line_endings_come_out_the_way_they_went_in() {
        assert_eq!(
            rewrite_playlist("#EXTM3U\r\n#EXT-X-ENDLIST\r\n", &base(), ""),
            "#EXTM3U\r\n#EXT-X-ENDLIST\r\n"
        );
        assert_eq!(
            rewrite_playlist("#EXTM3U\n#EXT-X-ENDLIST", &base(), ""),
            "#EXTM3U\n#EXT-X-ENDLIST",
            "no terminator invented for a body that ended without one"
        );
        assert_eq!(
            rewrite_playlist("#EXTM3U\r\n#EXTINF:10,\nseg-0.ts\r\n", &base(), ""),
            format!(
                "#EXTM3U\r\n#EXTINF:10,\n{}\r\n",
                proxied("http://example.com/streams/seg-0.ts")
            ),
            "mixed endings are the origin's business, not something to normalise"
        );
    }

    /// A line that is not UTF-8 holds no URI, and replacing the bytes we
    /// cannot read with U+FFFD -- which `from_utf8_lossy` over the whole
    /// body used to do -- corrupts them on their way to a player that might
    /// have understood them.
    #[test]
    fn a_line_that_is_not_text_is_passed_on_as_it_came() {
        let mut rewriter = PlaylistRewriter::new(base(), CarriedParams::everywhere(""));
        let mut out = rewriter.push(b"#EXTM3U\n\xff\xfe not text\nseg-0.ts\n");
        out.append(&mut rewriter.finish());

        let mut expected = b"#EXTM3U\n\xff\xfe not text\n".to_vec();
        expected.extend_from_slice(proxied("http://example.com/streams/seg-0.ts").as_bytes());
        expected.push(b'\n');
        assert_eq!(out, expected, "the bytes as the origin wrote them");
    }

    /// A body that is not line-oriented at all -- a `.m3u8` URL answering
    /// with megabytes of MPEG-TS -- must not be held in memory waiting for
    /// a newline that never comes. Past
    /// [`LONGEST_REWRITABLE_LINE`] the bytes are handed on as they came,
    /// and the rest of that line with them.
    #[test]
    fn a_line_too_long_to_be_one_is_handed_on_rather_than_held() {
        let mut rewriter = PlaylistRewriter::new(base(), CarriedParams::everywhere(""));
        let overlong = vec![b'x'; LONGEST_REWRITABLE_LINE + 1];
        assert_eq!(
            rewriter.push(&overlong),
            overlong,
            "nothing is held back once the line cannot be one"
        );
        let mut out = rewriter.push(b"more of it\nseg-0.ts\n");
        out.append(&mut rewriter.finish());
        assert_eq!(
            String::from_utf8(out).expect("text in, text out"),
            format!(
                "more of it\n{}\n",
                proxied("http://example.com/streams/seg-0.ts")
            ),
            "the rest of that line follows it verbatim, and the next line is a line again"
        );
    }

    /// Both URL shapes carry the same four parameters, and one parser reads
    /// them: the Core format spells them in its path segment, the query
    /// format -- the one the rewrite writes -- in the query.
    #[test]
    fn both_url_shapes_are_read_by_the_same_parser() {
        let expected = ProxyParams {
            target: "http://example.com/film.mkv".to_string(),
            request_headers: BTreeMap::from([(
                "Authorization".to_string(),
                "Bearer x:y".to_string(),
            )]),
            response_headers: BTreeMap::from([(
                "Content-Type".to_string(),
                "video/mp4".to_string(),
            )]),
            player_token: Some("player one".to_string()),
        };
        let query = "d=http%3A%2F%2Fexample.com%2Ffilm.mkv\
                     &h=Authorization%3ABearer%20x%3Ay\
                     &r=Content-Type%3Avideo%2Fmp4\
                     &p=player%20one";
        assert_eq!(ProxyParams::parse(query), expected);
        // A header value's own colons belong to the value, and a `%`-encoded
        // separator is decoded exactly once.
        assert_eq!(
            expected
                .request_headers
                .get("Authorization")
                .map(String::as_str),
            Some("Bearer x:y")
        );
    }

    /// The whole point of [`ProxyParams::carried`]: a segment fetched
    /// through a rewritten line is asked for with the headers the
    /// playlist's own URL carried. Without this an authenticated HLS stream
    /// served its playlist and 403ed every segment.
    ///
    /// And `r=` stays behind. The playlist is what the caller labelled; a
    /// segment carrying that label is MPEG-TS announced as a playlist.
    #[test]
    fn the_headers_are_carried_into_every_rewritten_line_too() {
        let params = ProxyParams::parse(
            "d=whatever&h=Authorization%3ABearer+abc&r=Content-Type%3Avideo%2Fmp4&p=one",
        );
        let rewritten = rewrite_playlist_carrying(
            "seg-0.ts\n",
            &base(),
            params.carried(&CredentialChain::named_by(&base())),
        );
        assert_eq!(
            rewritten,
            format!(
                "{}\n",
                proxied_with(
                    "http://example.com/streams/seg-0.ts",
                    "&h=Authorization%3ABearer%20abc&p=one"
                )
            )
        );
    }

    /// The one rule, one hop further on: the credentials leave the origin
    /// the caller named only over `https`, so a line that steps down from
    /// an `https` playlist to an `http` target is written without them and
    /// one that stays on `https` keeps them.
    ///
    /// A rewritten line is where the loop's guard was being defeated. The
    /// player fetches what the playlist names, by itself, so an
    /// `Authorization` written into an `http` line is an `Authorization`
    /// delivered in the clear -- to an origin the caller never named, on a
    /// line the origin chose.
    #[test]
    fn a_line_that_steps_down_to_cleartext_is_written_without_the_credentials() {
        let secure = Url::parse("https://example.com/streams/master.m3u8").expect("a base URL");
        let params = ProxyParams::parse(
            "d=whatever&h=Authorization%3ABearer+abc&h=Cookie%3Asession%3Dxyz\
             &h=User-Agent%3Aaddon%2F1&p=one",
        );
        let rewritten = rewrite_playlist_carrying(
            "https://cdn.example.org/s/1.ts\nhttp://plain.example.org/s/2.ts\n",
            &secure,
            params.carried(&CredentialChain::named_by(&secure)),
        );
        assert_eq!(
            rewritten,
            format!(
                "{}\n{}\n",
                proxied_with(
                    "https://cdn.example.org/s/1.ts",
                    "&h=Authorization%3ABearer%20abc&h=Cookie%3Asession%3Dxyz\
                     &h=User-Agent%3Aaddon%2F1&p=one"
                ),
                proxied_with(
                    "http://plain.example.org/s/2.ts",
                    "&h=User-Agent%3Aaddon%2F1&p=one"
                )
            ),
            "the credentials stay on the TLS line; the rest of h= goes on both"
        );
    }

    /// And once the chain has stepped off `https`, no line gets them back
    /// -- including one naming `https`. A playlist fetched over cleartext
    /// was told what to name in the clear too, so an `https` line in it is
    /// not the caller's `https` origin talking; and the cleartext host that
    /// served it is not the origin the caller spent the credential on, so
    /// its own segments are no more armed than anyone else's.
    #[test]
    fn a_playlist_reached_over_cleartext_arms_no_line_with_the_credentials() {
        // What the caller named -- `https`, so no cleartext line is its
        // origin -- and `base()`, the cleartext host a redirect landed on,
        // is where the playlist actually came from.
        let requested = Url::parse("https://secure.example.net/master.m3u8").expect("a caller URL");
        let params =
            ProxyParams::parse("d=whatever&h=Authorization%3ABearer+abc&h=User-Agent%3Aaddon%2F1");
        // The chain: named over `https`, landed on cleartext, so nothing
        // it names is the origin the credential was spent on.
        let mut chain = CredentialChain::named_by(&requested);
        chain.stepped_to(&base());
        let rewritten = rewrite_playlist_carrying(
            "https://cdn.example.org/s/1.ts\nseg-0.ts\n",
            &base(),
            params.carried(&chain),
        );
        assert_eq!(
            rewritten,
            format!(
                "{}\n{}\n",
                proxied_with(
                    "https://cdn.example.org/s/1.ts",
                    "&h=User-Agent%3Aaddon%2F1"
                ),
                proxied_with(
                    "http://example.com/streams/seg-0.ts",
                    "&h=User-Agent%3Aaddon%2F1"
                )
            )
        );
    }

    /// The exception a cleartext chain earns is one origin's, not the
    /// scheme's, and not the scheme's in the other direction either. A
    /// caller that names an `http://` target has spent the credential on
    /// exactly that origin, and lines naming it are the only lines of such
    /// a playlist that may carry it -- at whatever depth, since every line
    /// is a fresh request and re-arms `h=` from what it was written with.
    ///
    /// The `https` row is the fourth round of this bug and the reason the
    /// decision is one predicate now. It used to read `armed`, justified as
    /// "the same trade the redirect loop makes" -- which was the opposite
    /// of what the loop did: the loop refuses a cleartext chain's `https`
    /// hop, and the rewriter allowed the same chain's `https` line. What
    /// that cost is in [`CredentialChain`], measured.
    ///
    /// Two decisions are pinned here rather than only argued in
    /// [`CarriedParams`]: a subdomain is a different host, and a different
    /// port is a different listener. Both are what [`Url::origin`] already
    /// says, which is also what `d=` is written with -- and the default
    /// port spelled out is *not* a different listener.
    #[test]
    fn a_cleartext_line_carries_the_credentials_only_to_the_origin_the_caller_named() {
        let params =
            ProxyParams::parse("d=whatever&h=Authorization%3ABearer+abc&h=User-Agent%3Aaddon%2F1");
        // `base()` is `http://example.com/streams/master.m3u8`, and here it
        // is what the caller named as well as where the playlist came from.
        let carried = params.carried(&CredentialChain::named_by(&base()));
        let armed = "&h=Authorization%3ABearer%20abc&h=User-Agent%3Aaddon%2F1";
        let unarmed = "&h=User-Agent%3Aaddon%2F1";
        for (target, expected, why) in [
            (
                "http://example.com/streams/seg-0.ts",
                armed,
                "the origin the caller named, which already has it",
            ),
            (
                "http://example.com:80/seg-0.ts",
                armed,
                "the same origin with its default port spelled out",
            ),
            (
                "http://example.com:8080/seg-0.ts",
                unarmed,
                "another port is another listener, and may be another party",
            ),
            (
                "http://cdn.example.com/seg-0.ts",
                unarmed,
                "a subdomain is a different host",
            ),
            (
                "http://example.org/seg-0.ts",
                unarmed,
                "and a different host plainly is",
            ),
            (
                "https://cdn.example.org/seg-0.ts",
                unarmed,
                "and an https line is not an exception to that: this chain \
                 is not an https chain, and the redirect loop refuses the \
                 same target for the same reason",
            ),
        ] {
            let target = Url::parse(target).expect("a target URL");
            assert_eq!(carried.for_target(&target), expected, "{target}: {why}");
        }
    }

    /// Which origin is armed follows the caller's URL, not the body's. A
    /// cleartext `302` makes those two different origins, and keyed on the
    /// body's this had it exactly backwards: the line home to the host the
    /// caller named and authenticated to lost the credential, and the
    /// redirect target the caller had never named gained it.
    #[test]
    fn a_cleartext_redirect_does_not_move_which_origin_a_line_may_be_armed_for() {
        // The caller named A; a cleartext `302` took the fetch to B, so the
        // playlist's own lines are B's and the credential is A's.
        let named = Url::parse("http://a.example/live/master.m3u8").expect("the caller's URL");
        let landed = Url::parse("http://b.example/edge/master.m3u8").expect("the redirect target");
        let params =
            ProxyParams::parse("d=whatever&h=Authorization%3ABearer+abc&h=User-Agent%3Aaddon%2F1");
        // The hop to B carried no credential, which does not un-spend it
        // on the origin the caller named.
        let mut chain = CredentialChain::named_by(&named);
        chain.stepped_to(&landed);
        let carried = params.carried(&chain);
        let armed = "&h=Authorization%3ABearer%20abc&h=User-Agent%3Aaddon%2F1";
        let unarmed = "&h=User-Agent%3Aaddon%2F1";
        for (target, expected, why) in [
            (
                "http://a.example/back-on-a.ts",
                armed,
                "the caller named A and authenticated to it; its segments still play",
            ),
            (
                landed.join("seg-0.ts").expect("a relative line").as_str(),
                unarmed,
                "while B, which only the redirect named, gets nothing",
            ),
            (
                "https://cdn.example.org/seg-0.ts",
                unarmed,
                "and the chain is off https, so no https line is armed either",
            ),
        ] {
            let target = Url::parse(target).expect("a target URL");
            assert_eq!(carried.for_target(&target), expected, "{target}: {why}");
        }
    }

    /// The two callers of the predicate, asked the same question and made
    /// to give the same answer: for every shape of chain and every shape of
    /// target, what the redirect loop would put on its next hop
    /// ([`CredentialChain::may_carry_to`], which is the call the loop makes)
    /// is what the rewriter puts on a line naming the same target.
    ///
    /// This is the test for the *defect*, not for the rule -- the rule is
    /// pinned by the tests around this one. Four rounds running, the loop
    /// and the rewriter each implemented the policy and each round they
    /// agreed in every direction anyone had just tested and disagreed in one
    /// nobody had. They cannot now: `for_target` is `may_carry_to` and
    /// nothing else. Should someone give the rewriter a condition of its
    /// own again, this fails in whichever direction they got wrong, rather
    /// than a fifth round finding it in the field.
    #[test]
    fn the_loop_and_the_rewriter_answer_alike_for_every_chain_and_target() {
        let params =
            ProxyParams::parse("d=whatever&h=Authorization%3ABearer+abc&h=User-Agent%3Aaddon%2F1");
        let armed = "&h=Authorization%3ABearer%20abc&h=User-Agent%3Aaddon%2F1";
        let unarmed = "&h=User-Agent%3Aaddon%2F1";
        let url = |spelling: &str| Url::parse(spelling).expect("a URL");
        // Each chain as the loop builds it: what the caller named, then the
        // hops it has taken.
        let chain = |named: &str, hops: &[&str]| {
            let mut chain = CredentialChain::named_by(&url(named));
            for hop in hops {
                chain.stepped_to(&url(hop));
            }
            chain
        };
        for (chain, what) in [
            (chain("http://a.example/m.m3u8", &[]), "a cleartext caller"),
            (
                chain("http://a.example/m.m3u8", &["http://b.example/m.m3u8"]),
                "a cleartext caller after a cleartext redirect",
            ),
            (
                chain("http://a.example/m.m3u8", &["https://c.example/m.m3u8"]),
                "a cleartext caller after a redirect that stepped up",
            ),
            (chain("https://a.example/m.m3u8", &[]), "an https caller"),
            (
                chain("https://a.example/m.m3u8", &["https://c.example/m.m3u8"]),
                "an https caller after an https redirect",
            ),
            (
                chain("https://a.example/m.m3u8", &["http://b.example/m.m3u8"]),
                "an https caller after a redirect that stepped down",
            ),
            (
                chain(
                    "https://a.example/m.m3u8",
                    &["http://b.example/m.m3u8", "https://c.example/m.m3u8"],
                ),
                "an https caller that stepped down and back up",
            ),
        ] {
            let carried = params.carried(&chain);
            for target in [
                "http://a.example/seg.ts",
                "https://a.example/seg.ts",
                "http://a.example:8080/seg.ts",
                "http://b.example/seg.ts",
                "https://c.example/seg.ts",
            ] {
                let target = url(target);
                // What the loop asks before it builds the hop.
                let hop = chain.may_carry_to(&target);
                assert_eq!(
                    carried.for_target(&target),
                    if hop { armed } else { unarmed },
                    "{what}, target {target}: the loop would{} carry the \
                     credentials, so a line naming it must{} be armed",
                    if hop { "" } else { " not" },
                    if hop { "" } else { " not" },
                );
            }
        }
    }

    /// However the caller spelled the name. `h=` values are the caller's
    /// text, not a header map's, so nothing has lowercased them for us.
    #[test]
    fn a_credential_named_in_any_case_is_still_one() {
        let secure = Url::parse("https://example.com/streams/master.m3u8").expect("a base URL");
        let params = ProxyParams::parse("d=whatever&h=AUTHORIZATION%3ABearer+abc");
        assert_eq!(
            params
                .carried(&CredentialChain::named_by(&secure))
                .for_target(&base()),
            ""
        );
    }

    /// Several of them, and the order is the same every time: the rewritten
    /// playlist is a body a player may cache and re-fetch, and two spellings
    /// of the same playlist would be two.
    #[test]
    fn carried_parameters_come_out_in_a_stable_order() {
        let params = ProxyParams::parse("d=whatever&h=B%3A2&h=A%3A1&r=Y%3Ayes&r=X%3Ano&p=t");
        let carried = params.carried(&CredentialChain::named_by(&base()));
        let https = Url::parse("https://example.com/s/1.ts").expect("an https target");
        assert_eq!(carried.for_target(&https), "&h=A%3A1&h=B%3A2&p=t");
        assert_eq!(carried.for_target(&base()), "&h=A%3A1&h=B%3A2&p=t");
    }

    #[test]
    fn comment_and_blank_lines_are_left_unchanged() {
        let body = "#EXTM3U\n#EXT-X-VERSION:3\n\n#EXT-X-TARGETDURATION:10\n";
        let rewritten = rewrite_playlist(body, &base(), "");
        assert_eq!(rewritten, body);
    }

    /// `r=` is an override: the origin's value for that name goes, rather
    /// than the two of them being sent together for the client to choose
    /// between -- and a client reading the first of two `content-type`s
    /// read the origin's, which is the value `r=` exists to correct.
    #[test]
    fn a_custom_response_header_replaces_the_origin_s_own() {
        let builder = Response::builder()
            .status(200)
            .header("content-type", "application/octet-stream");
        let overrides = BTreeMap::from([("Content-Type".to_string(), "video/mp4".to_string())]);
        let response = finalize_response(
            apply_custom_response_headers(builder, &overrides),
            axum::body::Body::empty(),
        );

        assert_eq!(
            response.headers().get_all("content-type").iter().count(),
            1,
            "one value, not the override queued behind the origin's"
        );
        assert_eq!(response.headers().get("content-type").unwrap(), "video/mp4");
    }

    /// The same for `h=`, where the collision is with what the player sent.
    #[test]
    fn a_custom_request_header_replaces_what_the_player_sent() {
        let overrides = BTreeMap::from([("User-Agent".to_string(), "addon/1".to_string())]);
        let mut request = HeaderMap::new();
        request.insert("user-agent", HeaderValue::from_static("mpv/0.41"));
        // What `RequestBuilder::headers` does with the map this builds.
        for (name, value) in custom_request_headers(&overrides) {
            request.insert(name.expect("a named header"), value);
        }
        assert_eq!(request.get_all("user-agent").iter().count(), 1);
        assert_eq!(request.get("user-agent").unwrap(), "addon/1");
    }

    /// `r=` names a header on the resource, not on the hop. The three that
    /// frame the hop are dropped from it exactly as they are dropped from
    /// the origin's own headers -- an addon that says its film is one byte
    /// long panics hyper in debug and hangs the player in release.
    #[test]
    fn a_custom_response_header_cannot_reframe_the_response() {
        let overrides = BTreeMap::from([
            ("Content-Length".to_string(), "1".to_string()),
            ("Transfer-Encoding".to_string(), "chunked".to_string()),
            ("Connection".to_string(), "close".to_string()),
            ("Content-Type".to_string(), "video/mp4".to_string()),
        ]);
        let response = finalize_response(
            apply_custom_response_headers(Response::builder().status(200), &overrides),
            axum::body::Body::empty(),
        );

        for name in UNFRAMEABLE_RESPONSE_HEADERS {
            assert!(
                !response.headers().contains_key(name),
                "{name} frames the response, and r= does not get to say"
            );
        }
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "video/mp4",
            "and the header r= exists for still arrives"
        );
    }

    #[test]
    fn apply_custom_response_headers_skips_invalid_name_and_value() {
        let mut headers = BTreeMap::new();
        // Valid pair: should be applied.
        headers.insert("X-Proxy-Ok".to_string(), "yes".to_string());
        // Invalid value: embedded CR/LF must never reach the header map.
        headers.insert(
            "X-Evil".to_string(),
            "bad\r\nInjected-Header: true".to_string(),
        );
        // Invalid name: space is not a legal header-name character.
        headers.insert("Bad Name".to_string(), "value".to_string());

        let builder = apply_custom_response_headers(Response::builder().status(200), &headers);
        let response = builder.body(axum::body::Body::empty()).unwrap();

        assert_eq!(response.headers().get("x-proxy-ok").unwrap(), "yes");
        assert!(response.headers().get("x-evil").is_none());
        assert!(!response.headers().contains_key("injected-header"));
    }

    #[test]
    fn malicious_r_header_value_does_not_panic_and_yields_a_response() {
        // Simulates parsing r=X-Evil:bad%0d%0aInjected:1 from the proxy URL:
        // once percent-decoded and split on ':', the value carries a raw
        // newline. Feeding this straight into a response builder (the old
        // `.header(name, value)` + `.unwrap()` code path) would poison the
        // builder and panic at `.body()`. The validated path must not.
        let mut custom_response_headers = BTreeMap::new();
        custom_response_headers.insert("X-Evil".to_string(), "bad\r\nInjected: true".to_string());

        let builder = apply_custom_response_headers(
            Response::builder().status(200),
            &custom_response_headers,
        );
        let response = finalize_response(builder, axum::body::Body::empty());

        // No panic occurred (we got here), and the handler degrades to a
        // clean response rather than crashing the whole process.
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("x-evil").is_none());
    }

    #[test]
    fn finalize_response_returns_502_on_builder_error_instead_of_panicking() {
        // Bypass our own validation to force the underlying http builder
        // into an error state, the way an unvalidated header ingest used to.
        let builder = Response::builder()
            .status(200)
            .header("Bad Header Name\r\n", "value");

        let response = finalize_response(builder, axum::body::Body::empty());

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    /// The two `206`s that mean different things. The first is what an
    /// origin answers a player's opening `Range: bytes=0-` with: the whole
    /// entity, which is a playlist to rewrite. The second is a fragment,
    /// and there is no rewriting a fragment.
    #[test]
    fn only_a_206_that_carries_the_whole_entity_is_a_body_to_rewrite() {
        assert!(covers_the_whole_entity("bytes 0-179/180"));
        assert!(!covers_the_whole_entity("bytes 10-40/180"));
        assert!(!covers_the_whole_entity("bytes 0-178/180"));
        // An origin that will not say how long the entity is has not said
        // this is all of it.
        assert!(!covers_the_whole_entity("bytes 0-179/*"));
        assert!(!covers_the_whole_entity("bytes */180"));
        assert!(!covers_the_whole_entity("items 0-179/180"));
        assert!(!covers_the_whole_entity(""));
    }

    /// The four forms a `Location` we are not following can take. Only the
    /// relative ones are resolved; the rest are the origin's own bytes,
    /// including the `ftp://` this relay exists to preserve as a
    /// diagnostic.
    #[test]
    fn only_a_relative_location_is_resolved_before_it_is_relayed() {
        let from = Url::parse("https://cdn.example.com/cdn/2024/film.mkv").unwrap();
        let relayed = |written: &str| {
            relayed_location(&HeaderValue::from_str(written).unwrap(), &from)
                .to_str()
                .expect("the fixture values are all text")
                .to_string()
        };

        assert_eq!(relayed("/elsewhere"), "https://cdn.example.com/elsewhere");
        assert_eq!(
            relayed("v2/film.mkv"),
            "https://cdn.example.com/cdn/2024/v2/film.mkv",
            "beside the resource that sent it, which is what relative means here"
        );
        assert_eq!(
            relayed("//edge.example.org/film.mkv"),
            "https://edge.example.org/film.mkv",
            "a protocol-relative value takes the scheme of the hop it came from"
        );
        assert_eq!(
            relayed("http://edge.example.org/film.mkv"),
            "http://edge.example.org/film.mkv",
            "an absolute value is the origin's spelling and is not normalised"
        );
        assert_eq!(
            relayed("ftp://files.example.com/film.mkv"),
            "ftp://files.example.com/film.mkv",
            "including one naming a scheme we would never fetch"
        );
    }

    #[test]
    fn both_clients_build_successfully() {
        assert!(http_client().is_some());
        assert!(insecure_http_client().is_some());
    }

    /// The retry is for a certificate and nothing else: a refused connection
    /// or a DNS miss must not be retried unverified, because verification is
    /// not what stopped it.
    ///
    /// The second URL is the measured reproduction of what reading the
    /// chain's prose cost. There is no TLS anywhere here -- plain HTTP to a
    /// port nothing listens on -- and the old substring search found the
    /// word "certificate" in reqwest's own "error sending request for url
    /// (...)", so the *filename* marked `127.0.0.1` unverified for the life
    /// of the process.
    #[test]
    fn only_a_certificate_failure_is_read_as_one() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let refused = |url: &'static str| {
            rt.block_on(async move {
                reqwest::Client::new()
                    .get(url)
                    .send()
                    .await
                    .expect_err("nothing is listening there")
            })
        };
        assert!(!is_certificate_error(&refused("http://127.0.0.1:1/")));
        assert!(!is_certificate_error(&refused(
            "http://127.0.0.1:1/certificate-of-authenticity.mkv"
        )));
    }

    /// The shape the real failure arrives in, measured against a self-signed
    /// origin: reqwest's connect error, an `io::Error`, a second
    /// `io::Error`, and only then the rustls one. `source()` alone stops at
    /// the first of the two, because `io::Error::source` yields the inner
    /// error's source rather than the inner error.
    #[test]
    fn a_rustls_failure_is_found_through_the_nested_io_errors_it_arrives_in() {
        use tokio_rustls::rustls;

        let rustls_error =
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        let nested = std::io::Error::other(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            rustls_error,
        ));
        assert!(is_certificate_error(&nested));

        // A TLS failure that verification would not have prevented is not
        // one to waive it for.
        let unrelated = std::io::Error::other(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            rustls::Error::DecryptError,
        ));
        assert!(!is_certificate_error(&unrelated));
    }
}

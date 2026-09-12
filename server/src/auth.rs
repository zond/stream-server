//! Bearer-token protection for the control API.
//!
//! The HTTP surface is split in two (see `build_router`): the *media* routes
//! that hand bytes to a player are open, because players (mpv, a Chromecast
//! receiver) fetch plain URLs and cannot attach headers; every other route is
//! *control* API and must carry `Authorization: Bearer <token>`, generated
//! per launch (see [`ServerAuth`]). The token is only ever accepted from
//! that header -- never
//! from the query string, so it does not end up in access logs or in URLs a
//! client hands to a third party.

use axum::{
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use subtle::ConstantTimeEq;

use crate::state::AppState;

/// How the control routes authenticate. Media routes are always open.
///
/// One variant, and every server has a token. There were two more --
/// `Token(String)`, a token the caller chose, and `Disabled`, which opened
/// every control route -- and both existed for the deleted daemon's command
/// line (`--token`, `--no-auth`). An embedder has no command line: it reads
/// the generated token off [`crate::ServerHandle::auth_token`] and attaches
/// it, so a token it could pick itself buys nothing, and an open control API
/// on a machine that also runs a browser is a hole no embedder asked for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ServerAuth {
    /// A fresh random token for this launch (32 random bytes, hex).
    /// Embedders read it from [`crate::ServerHandle::auth_token`].
    #[default]
    Generated,
}

impl ServerAuth {
    /// The token this launch requires.
    pub(crate) fn resolve(&self) -> anyhow::Result<String> {
        match self {
            Self::Generated => generate_token(),
        }
    }
}

fn generate_token() -> anyhow::Result<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|err| anyhow::anyhow!("failed to draw random bytes for the auth token: {err}"))?;
    Ok(hex::encode(bytes))
}

/// Fixed 401 body; the response never says *why* the credentials failed.
pub const UNAUTHORIZED_BODY: &str = "unauthorized";

/// The credentials of an `Authorization: Bearer <token>` header, if present.
fn bearer_token(req: &Request) -> Option<&str> {
    let value = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.trim().split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim())
        .filter(|token| !token.is_empty())
}

/// Constant-time comparison (for equal lengths; a length mismatch is a plain
/// `false`, which reveals nothing an attacker does not already know about a
/// fixed-length hex token).
fn token_matches(expected: &str, presented: &str) -> bool {
    expected.as_bytes().ct_eq(presented.as_bytes()).into()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        UNAUTHORIZED_BODY,
    )
        .into_response()
}

/// `axum::middleware::from_fn_with_state` layer for the control router: lets
/// the request through when it carries the right bearer token, 401s
/// otherwise.
pub(crate) async fn require_bearer(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    // A state with no token used to mean "authentication is off, let
    // everyone in" -- `ServerAuth::Disabled`, which is gone. `run` always
    // sets one now, so this arm is only reachable from an `AppState` nobody
    // filled in, and refusing is the safe way to be wrong about that: a bug
    // in the wiring must not silently open the control API.
    let Some(expected) = state.auth_token.as_deref() else {
        return unauthorized();
    };
    match bearer_token(&req) {
        Some(presented) if token_matches(expected, presented) => next.run(req).await,
        _ => unauthorized(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with_authorization(value: &str) -> Request {
        Request::builder()
            .uri("/heartbeat")
            .header(header::AUTHORIZATION, value)
            .body(axum::body::Body::empty())
            .unwrap()
    }

    #[test]
    fn generated_tokens_are_32_random_bytes_as_hex() {
        let a = ServerAuth::Generated.resolve().unwrap();
        let b = ServerAuth::Generated.resolve().unwrap();
        assert_eq!(a.len(), 64);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "two launches must not share a token");
    }

    #[test]
    fn bearer_token_parses_the_scheme_case_insensitively() {
        let req = request_with_authorization("Bearer abc");
        assert_eq!(bearer_token(&req), Some("abc"));
        let req = request_with_authorization("bearer  abc ");
        assert_eq!(bearer_token(&req), Some("abc"));
    }

    #[test]
    fn bearer_token_rejects_other_schemes_missing_headers_and_empty_tokens() {
        let req = request_with_authorization("Basic abc");
        assert_eq!(bearer_token(&req), None);
        let req = request_with_authorization("Bearer ");
        assert_eq!(bearer_token(&req), None);
        let req = request_with_authorization("abc");
        assert_eq!(bearer_token(&req), None);
        let req = Request::builder()
            .uri("/heartbeat")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(bearer_token(&req), None);
    }

    #[test]
    fn token_matches_requires_exact_equality() {
        assert!(token_matches("abc", "abc"));
        assert!(!token_matches("abc", "abd"));
        assert!(!token_matches("abc", "abcd"));
        assert!(!token_matches("abc", ""));
    }
}

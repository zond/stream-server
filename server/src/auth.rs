//! Bearer-token protection for the control API.
//!
//! The HTTP surface is split in two (see `build_router`): the *media* routes
//! that hand bytes to a player are open, because players (mpv, a Chromecast
//! receiver) fetch plain URLs and cannot attach headers; every other route is
//! *control* API and must carry `Authorization: Bearer <token>`, where the
//! token is generated per launch (or supplied, or disabled) via
//! [`ServerAuth`]. The token is only ever accepted from that header -- never
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
#[derive(Clone, Default, PartialEq, Eq)]
pub enum ServerAuth {
    /// A fresh random token for this launch (32 random bytes, hex). The
    /// default for both [`crate::ServerConfig::embedded`] and
    /// [`crate::ServerConfig::binary_default`]; embedders read it from
    /// [`crate::ServerHandle::auth_token`], the binary prints it to stdout at
    /// startup (never to the log).
    #[default]
    Generated,
    /// Exactly this token (must not be empty).
    Token(String),
    /// No authentication at all: every route is open (`--no-auth`).
    Disabled,
}

impl std::fmt::Debug for ServerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Generated => f.write_str("Generated"),
            // Never print the secret via `{:?}` on a `ServerConfig`.
            Self::Token(_) => f.write_str("Token(<redacted>)"),
            Self::Disabled => f.write_str("Disabled"),
        }
    }
}

impl ServerAuth {
    /// The token this launch requires, or `None` when authentication is off.
    pub(crate) fn resolve(&self) -> anyhow::Result<Option<String>> {
        match self {
            Self::Generated => generate_token().map(Some),
            Self::Token(token) => {
                anyhow::ensure!(!token.is_empty(), "ServerAuth::Token must not be empty");
                Ok(Some(token.clone()))
            }
            Self::Disabled => Ok(None),
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
/// the request through when no token is configured (`ServerAuth::Disabled`)
/// or when it carries the right bearer token, 401s otherwise.
pub(crate) async fn require_bearer(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected) = state.auth_token.as_deref() else {
        return next.run(req).await;
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
        let a = ServerAuth::Generated.resolve().unwrap().unwrap();
        let b = ServerAuth::Generated.resolve().unwrap().unwrap();
        assert_eq!(a.len(), 64);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "two launches must not share a token");
    }

    #[test]
    fn explicit_token_is_used_verbatim_and_must_not_be_empty() {
        assert_eq!(
            ServerAuth::Token("secret".into()).resolve().unwrap(),
            Some("secret".to_string())
        );
        assert!(ServerAuth::Token(String::new()).resolve().is_err());
    }

    #[test]
    fn disabled_resolves_to_no_token() {
        assert_eq!(ServerAuth::Disabled.resolve().unwrap(), None);
    }

    #[test]
    fn debug_output_redacts_the_token() {
        assert_eq!(
            format!("{:?}", ServerAuth::Token("secret".into())),
            "Token(<redacted>)"
        );
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

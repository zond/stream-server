use crate::routes::compat;
use crate::state::AppState;
use axum::{
    Router,
    body::Body,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct FtpQuery {
    pub lz: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FtpStreamBody {
    pub ftp_url: String,
}

pub fn router() -> Router<AppState> {
    Router::new().route("/{filename}", get(stream_ftp))
}

async fn stream_ftp(Path(filename): Path<String>, Query(params): Query<FtpQuery>) -> Response {
    let lz_data = match params.lz {
        Some(lz) => lz,
        None => return (StatusCode::BAD_REQUEST, "Missing lz parameter").into_response(),
    };

    // Decompress lz-string (returns Vec<u16>)
    let utf16_data = match lz_str::decompress_from_encoded_uri_component(&lz_data) {
        Some(s) => s,
        None => return (StatusCode::BAD_REQUEST, "Failed to decompress lz data").into_response(),
    };
    let json_str = match String::from_utf16(&utf16_data) {
        Ok(s) => s,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid UTF-16 in lz data").into_response(),
    };

    let body: FtpStreamBody = match serde_json::from_str(&json_str) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to parse FTP body: {}", e),
            )
                .into_response();
        }
    };

    // Only what the route is named for. The URL goes to a spawned `curl`,
    // which speaks every scheme there is, and this route is open to any
    // loopback caller -- on Android, every app on the device. Without this
    // gate, `file:///<data dir>/settings.json` streamed the proxy password
    // to whoever asked, and any URL curl knows a scheme for was fetched on
    // the caller's behalf. HTTP(S) is refused too: a caller with an HTTP
    // URL has `/proxy`, which is built to be handed one.
    let args = match curl_args(&body.ftp_url) {
        Ok(args) => args,
        Err(refusal) => return (StatusCode::BAD_REQUEST, refusal).into_response(),
    };
    stream_via_curl(args, &filename).await
}

/// The argument vector `curl` is spawned with for `url`, or why it is not
/// spawned at all.
///
/// The scheme is matched by allow-list, not by refusing what is known to be
/// dangerous: curl's scheme table is long (`file`, `gopher`, `dict`,
/// `smb`, ...) and every entry not named here is a capability handed to an
/// unauthenticated caller. The `--` before the URL is the second lock: it
/// ends option parsing, so a URL that passed the scheme check can never be
/// read as a flag whatever follows it -- the check stands on the scheme, not
/// on the first byte, and the two do not depend on each other.
fn curl_args(url: &str) -> Result<Vec<String>, &'static str> {
    let scheme = url.split_once("://").map(|(scheme, _)| scheme);
    match scheme {
        Some(scheme)
            if scheme.eq_ignore_ascii_case("ftp") || scheme.eq_ignore_ascii_case("ftps") =>
        {
            Ok(vec!["-s".into(), "-L".into(), "--".into(), url.to_string()])
        }
        _ => Err("ftpUrl must be an ftp:// or ftps:// URL"),
    }
}

/// Stream `curl`'s stdout for the argument vector [`curl_args`] built.
async fn stream_via_curl(args: Vec<String>, filename: &str) -> Response {
    use tokio_util::io::ReaderStream;

    // curl is typically available on Linux/macOS, less so on Windows
    let mut cmd = tokio::process::Command::new("curl");
    cmd.args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            // On Windows, curl might not be available
            let msg = if cfg!(windows) {
                format!("FTP streaming requires curl to be installed: {}", e)
            } else {
                format!("Failed to spawn curl: {}", e)
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
        }
    };

    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => return (StatusCode::INTERNAL_SERVER_ERROR, "No stdout").into_response(),
    };

    let stream = ReaderStream::new(stdout);

    let content_type = mime_guess::from_path(filename)
        .first_or_octet_stream()
        .to_string();

    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .header(
            axum::http::header::CONTENT_DISPOSITION,
            compat::content_disposition_inline(filename),
        )
        .header("transferMode.dlna.org", compat::DLNA_TRANSFER_MODE)
        .header("contentFeatures.dlna.org", compat::DLNA_CONTENT_FEATURES)
        .body(Body::from_stream(stream))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the route is named for goes through; nothing else does. Every
    /// scheme curl knows and this list does not is a capability handed to
    /// an unauthenticated loopback caller -- `file://` read the settings
    /// file, and the proxy password in it, to any app on the device.
    #[test]
    fn only_ftp_and_ftps_reach_curl() {
        let ftp = curl_args("ftp://host/dir/movie.mkv").unwrap();
        assert_eq!(ftp, ["-s", "-L", "--", "ftp://host/dir/movie.mkv"]);
        assert!(
            curl_args("FTPS://host/movie.mkv").is_ok(),
            "the scheme is case-insensitive"
        );

        assert!(curl_args("file:///data/data/app/files/settings.json").is_err());
        assert!(curl_args("http://example.com/movie.mkv").is_err());
        assert!(curl_args("https://example.com/movie.mkv").is_err());
        assert!(curl_args("gopher://host/1").is_err());
        assert!(curl_args("host/movie.mkv").is_err(), "no scheme is not ftp");
        assert!(
            curl_args("ftp:host").is_err(),
            "and nor is a scheme without a host part"
        );
    }

    /// The scheme check refuses a URL that begins with `-`, and the `--`
    /// would stop it being read as a flag even if it did not: the URL is
    /// always the argument after the terminator, so no byte of it is ever
    /// parsed as an option.
    #[test]
    fn the_url_is_never_where_an_option_could_be() {
        assert!(curl_args("-o/tmp/owned").is_err());
        assert!(curl_args("--config=/etc/curlrc").is_err());
        let args = curl_args("ftp://-host/x").unwrap();
        assert_eq!(args[args.len() - 2], "--");
        assert_eq!(args.last().unwrap(), "ftp://-host/x");
    }

    fn lz(url: &str) -> Option<String> {
        let json = serde_json::json!({ "ftpUrl": url }).to_string();
        Some(lz_str::compress_to_encoded_uri_component(&json))
    }

    /// The refusal reaches the wire as a 400 before anything is spawned:
    /// the handler has no state, so it is called as the router would.
    #[tokio::test]
    async fn a_non_ftp_url_is_refused_at_the_route() {
        for url in [
            "file:///data/data/app/files/settings.json",
            "http://example.com/movie.mkv",
        ] {
            let response = stream_ftp(
                Path("movie.mkv".to_string()),
                Query(FtpQuery { lz: lz(url) }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{url}");
        }
    }
}

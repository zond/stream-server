//! The HTTPS listener, and the one place it is started from.
//!
//! stremio-core's remote-HTTPS feature works like this: the client asks
//! `GET /get-https?authKey=…&ipAddress=…`, the server fetches a certificate
//! for that address from Stremio's API, writes it to disk and answers with a
//! `port`, and the client builds `https://<domain>:<port>` from the answer
//! and shows it as the address other devices can use. So the port in that
//! answer has to be a port a TLS handshake will succeed on, at the moment
//! the answer is given.
//!
//! It used to be neither. The route answered the plain-HTTP port,
//! unconditionally, and the only code that bound an HTTPS socket was a block
//! in `run` that looked for the PEM files once at boot -- so the first
//! `/get-https` of a fresh install wrote the certificate and pointed TLS at
//! a port speaking HTTP, and after a restart the listener existed on its own
//! port while the route still named the other one. The embedded
//! configuration, with no HTTPS address at all, wrote a private key to disk
//! that nothing would ever serve.
//!
//! [`HttpsListener`] is the same shape as `lan_media::LanMedia`: a control
//! block on `AppState` that `run` uses to start the listener at boot when the
//! certificate is already on disk, and that the route uses to (re)start it
//! the moment a new certificate has been written -- and whose bound address
//! is what the route answers with. No configured address means no listener
//! and a clear refusal, never a wrong port.

use crate::state::AppState;
use anyhow::Context;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Put `contents` at `path` whole or not at all: written beside it, then
/// renamed over it. A plain write truncates first, so a kill between the
/// truncate and the write left a PEM that would not load, and the listener
/// with it.
async fn write_whole(path: &Path, contents: &str) -> anyhow::Result<()> {
    let mut staged = path.as_os_str().to_owned();
    staged.push(".tmp");
    let staged = PathBuf::from(staged);
    tokio::fs::write(&staged, contents)
        .await
        .with_context(|| format!("failed to write {}", staged.display()))?;
    tokio::fs::rename(&staged, path)
        .await
        .with_context(|| format!("failed to put {} in place", path.display()))
}

/// The HTTPS listener's control block, held by [`AppState`].
pub struct HttpsListener {
    /// Where the listener binds, from `ServerConfig::https_addr`. `None`
    /// means the embedder configured no HTTPS at all and the listener can
    /// never run -- `/get-https` then refuses rather than pretend.
    configured_addr: Option<SocketAddr>,
    /// The certificate and key `/get-https` writes and the listener serves,
    /// in the config dir.
    cert_path: PathBuf,
    key_path: PathBuf,
    /// The running listener, if any. One mutex serialises start and stop,
    /// so a boot-time start and a `/get-https` cannot both bind.
    running: tokio::sync::Mutex<Option<Running>>,
}

struct Running {
    bound: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl HttpsListener {
    pub fn new(configured_addr: Option<SocketAddr>, config_dir: &Path) -> Self {
        Self {
            configured_addr,
            cert_path: config_dir.join("https-cert.pem"),
            key_path: config_dir.join("https-key.pem"),
            running: tokio::sync::Mutex::new(None),
        }
    }

    /// The address a listener would bind, whether or not one is running.
    pub fn configured_addr(&self) -> Option<SocketAddr> {
        self.configured_addr
    }

    /// Whether a certificate and key are on disk to serve.
    pub fn certificate_present(&self) -> bool {
        self.cert_path.exists() && self.key_path.exists()
    }

    /// The address the listener is bound to right now, or `None` when it is
    /// not running. With a configured port of 0 this is the OS-assigned
    /// port, which is why the answer comes from the listener and never from
    /// the configuration.
    pub async fn bound_addr(&self) -> Option<SocketAddr> {
        self.running
            .lock()
            .await
            .as_ref()
            .map(|running| running.bound)
    }

    /// Write a certificate and key to disk and serve them: the listener is
    /// started, or restarted so the new certificate is the one presented,
    /// and its bound address returned. This is what `/get-https` does with
    /// the certificate it fetched, and what the answer it gives is built
    /// from.
    ///
    /// Refused before anything is written when no HTTPS address is
    /// configured: a key on disk that nothing serves is a liability, not a
    /// feature half done.
    pub async fn install_certificate(
        &self,
        state: &AppState,
        cert_pem: &str,
        key_pem: &str,
    ) -> anyhow::Result<SocketAddr> {
        anyhow::ensure!(
            self.configured_addr.is_some(),
            "this server has no HTTPS listener configured (ServerConfig::https_addr is unset), \
             so there is nothing to serve a certificate on"
        );
        let mut running = self.running.lock().await;
        write_whole(&self.cert_path, cert_pem).await?;
        write_whole(&self.key_path, key_pem).await?;
        tracing::info!(dir = %self.cert_path.parent().unwrap_or(&self.cert_path).display(), "Saved HTTPS certificate");
        if let Some(previous) = running.take() {
            // A listener presenting the old certificate has to go; the
            // acceptor's config is fixed at bind. Aborting closes the socket
            // and the port frees at once; a response already streaming
            // finishes on its own connection task, as with the LAN listener.
            previous.task.abort();
            let _ = previous.task.await;
            tracing::info!(bound = %previous.bound, "HTTPS listener stopped for a new certificate");
        }
        let bound = self.bind_and_serve(state).await?;
        let addr = bound.bound;
        *running = Some(bound);
        Ok(addr)
    }

    /// Start the listener at boot when a certificate from an earlier
    /// `/get-https` is already on disk -- the answer that call gave names
    /// this listener's port, and a client that kept the URL expects it back
    /// after a restart. Nothing to do when no address is configured or no
    /// certificate is present; a certificate that will not load or a port
    /// that will not bind is an error for the caller, which `run` logs and
    /// serves on without.
    pub async fn start_if_certificate_present(
        &self,
        state: &AppState,
    ) -> anyhow::Result<Option<SocketAddr>> {
        let Some(addr) = self.configured_addr else {
            return Ok(None);
        };
        if !self.certificate_present() {
            tracing::info!(
                %addr,
                "no HTTPS certificate on disk yet; the HTTPS listener starts when /get-https fetches one"
            );
            return Ok(None);
        }
        let mut running = self.running.lock().await;
        if let Some(running) = running.as_ref() {
            return Ok(Some(running.bound));
        }
        let bound = self.bind_and_serve(state).await?;
        let addr = bound.bound;
        *running = Some(bound);
        Ok(Some(addr))
    }

    /// Stop the listener; a no-op when it is not running. For shutdown.
    pub async fn stop(&self) {
        if let Some(running) = self.running.lock().await.take() {
            running.task.abort();
            let _ = running.task.await;
        }
    }

    /// Bind the configured address with the certificate on disk and serve
    /// the full router on it -- the same router the plain listener serves,
    /// control routes behind the same bearer middleware. The caller holds
    /// the `running` lock.
    async fn bind_and_serve(&self, state: &AppState) -> anyhow::Result<Running> {
        let addr = self
            .configured_addr
            .context("no HTTPS address is configured")?;
        let config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&self.cert_path, &self.key_path)
                .await
                .context("the HTTPS certificate or key on disk would not load")?;
        // A std listener first, so the bound address is known before the
        // serve future exists -- the configured port may be 0.
        let listener = std::net::TcpListener::bind(addr)
            .with_context(|| format!("failed to bind the HTTPS listener on {addr}"))?;
        listener
            .set_nonblocking(true)
            .context("failed to make the HTTPS listener non-blocking")?;
        let bound = listener.local_addr()?;
        let app = crate::build_router(state.clone());
        let server = axum_server::from_tcp_rustls(listener, config)
            .context("failed to take over the HTTPS listener socket")?;
        let task = tokio::spawn(async move {
            if let Err(error) = server
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
            {
                tracing::error!(%error, "HTTPS listener failed");
            }
        });
        tracing::info!(%bound, "HTTPS listener started");
        Ok(Running { bound, task })
    }
}

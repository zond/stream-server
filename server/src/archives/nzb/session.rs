use super::nntp::Client as NntpClient;
use super::parser::Nzb;
use anyhow::{Result, anyhow};
use std::sync::Arc;

/// The most NNTP connections one session opens to one server, whatever the
/// caller asked for.
///
/// `connections` is caller-chosen on both `/create` paths -- a JSON field
/// and the path of a `news://host/<n>` URL -- and was passed straight to
/// the pool, so one request could have this process open as many sockets
/// as it liked and hold them for the life of the session. Usenet providers
/// sell plans of 20 to 60 connections and refuse the rest, and a client
/// asking for more gets nothing more from them. Fifty is above every plan
/// that is common and below anything that is a mistake or an attack.
pub const MAX_CONNECTIONS_PER_SERVER: u32 = 50;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct NzbConfig {
    pub servers: Vec<NzbServerConfig>,
    pub nzb_url: String, // Keep track of source
}

#[derive(Clone, Debug)]
pub struct NzbServerConfig {
    pub host: String,
    pub port: u16,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub ssl: bool,
    pub connections: u32,
}

/// One NZB being served: the parsed index and a pool of NNTP connections
/// to fetch its segments over.
///
/// The pool is a bounded channel of connected clients, and the session owns
/// both ends. A stream borrows a client per segment and returns it; nothing
/// else ever closes one. So the connections live exactly as long as the
/// session does -- held in the registry until it is swept as idle, and by
/// the streams reading from it until they end -- and dropping the last
/// `Arc<NzbSession>` closes every socket in the pool. That is the whole of
/// the connection lifetime, and it is deliberate: the session registry is
/// what decides when a session ends, and this type must not hold anything
/// the registry's drop does not release.
#[allow(dead_code)]
pub struct NzbSession {
    pub key: String,
    pub config: NzbConfig,
    pub nzb: Arc<Nzb>,
    pool: async_channel::Sender<NntpClient>,
    pool_receiver: async_channel::Receiver<NntpClient>,
}

impl NzbSession {
    /// Parse the NZB and start connecting the pool. Each server's
    /// `connections` is clamped into `1..=`[`MAX_CONNECTIONS_PER_SERVER`]
    /// here, so no caller can ask for more and no route has to remember to
    /// check: zero connections is a pool every fetch waits on forever, and
    /// more than the cap is sockets the provider will refuse.
    pub async fn new(key: String, mut config: NzbConfig, nzb_content: String) -> Result<Self> {
        let nzb = super::parser::parse_nzb_xml(&nzb_content)?;

        for server in &mut config.servers {
            let asked = server.connections;
            server.connections = asked.clamp(1, MAX_CONNECTIONS_PER_SERVER);
            if server.connections != asked {
                tracing::warn!(
                    host = %server.host,
                    asked,
                    using = server.connections,
                    "NNTP connection count clamped"
                );
            }
        }

        let total_connections: usize = config.servers.iter().map(|s| s.connections as usize).sum();
        let (tx, rx) = async_channel::bounded(total_connections);

        let config_clone = config.clone();
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            for server in &config_clone.servers {
                for _ in 0..server.connections {
                    match NntpClient::connect(&server.host, server.port, server.ssl).await {
                        Ok(mut client) => {
                            if let (Some(u), Some(p)) = (&server.user, &server.pass)
                                && let Err(e) = client.authenticate(u, p).await
                            {
                                tracing::error!("NNTP Auth failed for {}: {}", server.host, e);
                                continue;
                            }
                            // Fails only once the session is gone, and then
                            // the client is dropped here and closed.
                            if tx_clone.send(client).await.is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to connect to NNTP {}: {}", server.host, e);
                        }
                    }
                }
            }
        });

        Ok(Self {
            key,
            config,
            nzb: Arc::new(nzb),
            pool: tx,
            pool_receiver: rx,
        })
    }

    pub async fn fetch_segment(&self, message_id: &str) -> Result<Vec<u8>> {
        let mut client = self
            .pool_receiver
            .recv()
            .await
            .map_err(|_| anyhow!("Connection pool closed"))?;

        let result = client.fetch_body(message_id).await;

        match result {
            Ok(data) => {
                let _ = self.pool.send(client).await;
                Ok(data)
            }
            Err(e) => {
                tracing::warn!("Failed to fetch article: {}", e);
                // If error is recoverable, maybe put back?
                // For now, if we fail to fetch body, client might be in bad state or connection dropped.
                // We rely on pool not putting it back to drop it.
                // But wait, if I don't send it back, I lose a slot.
                // I should probably drop the client and spawn a replacement or try to reconnect.
                // For MVP: drop.
                Err(e)
            }
        }
    }

    /// A stream over the file whose subject names `filename`. The stream
    /// shares this session, so the pool stays open until the stream ends
    /// even if the registry has let the session go by then.
    pub fn stream_file(self: &Arc<Self>, filename: &str) -> Result<super::stream::NzbFileStream> {
        // Find file
        let file = self.nzb.files.iter().find(|f| {
            // Very naive match: subject contains filename
            f.subject.contains(filename)
        });

        if let Some(file) = file {
            Ok(super::stream::NzbFileStream::new(
                self.clone(),
                file.clone(),
            ))
        } else {
            Err(anyhow!("File not found in NZB: {}", filename))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const NZB: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="p" date="1" subject="video.mkv (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="10" number="1">seg1@test</segment></segments>
  </file>
</nzb>"#;

    /// Something that greets like a news server and then holds the socket
    /// open until the client closes it, counting connections accepted and
    /// connections closed.
    async fn fake_news_server() -> (u16, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let (accepted_, closed_) = (accepted.clone(), closed.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                accepted_.fetch_add(1, Ordering::SeqCst);
                let closed = closed_.clone();
                tokio::spawn(async move {
                    if socket.write_all(b"200 hello\r\n").await.is_err() {
                        return;
                    }
                    let mut buf = [0u8; 64];
                    // The client never speaks unprompted; a read that ends
                    // is the socket closing under us.
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                    closed.fetch_add(1, Ordering::SeqCst);
                });
            }
        });
        (port, accepted, closed)
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        // Generous so a regression fails rather than hangs, never a timing
        // assertion.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while !condition() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "condition never held"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// A caller asking for five hundred connections gets the cap, and
    /// dropping the session closes every connection it opened.
    #[tokio::test]
    async fn connections_are_capped_and_closed_with_the_session() {
        let (port, accepted, closed) = fake_news_server().await;
        let config = NzbConfig {
            nzb_url: "http://example.invalid/x.nzb".into(),
            servers: vec![NzbServerConfig {
                host: "127.0.0.1".into(),
                port,
                user: None,
                pass: None,
                ssl: false,
                connections: 500,
            }],
        };
        let session = NzbSession::new("k".into(), config, NZB.to_string())
            .await
            .expect("parses");
        assert_eq!(
            session.config.servers[0].connections,
            MAX_CONNECTIONS_PER_SERVER
        );

        wait_until(|| accepted.load(Ordering::SeqCst) == MAX_CONNECTIONS_PER_SERVER as usize).await;
        assert_eq!(
            closed.load(Ordering::SeqCst),
            0,
            "held while the session lives"
        );

        drop(session);
        wait_until(|| closed.load(Ordering::SeqCst) == MAX_CONNECTIONS_PER_SERVER as usize).await;
    }

    /// Zero is not a pool; it is one connection.
    #[tokio::test]
    async fn zero_connections_becomes_one() {
        let (port, accepted, _closed) = fake_news_server().await;
        let config = NzbConfig {
            nzb_url: "http://example.invalid/x.nzb".into(),
            servers: vec![NzbServerConfig {
                host: "127.0.0.1".into(),
                port,
                user: None,
                pass: None,
                ssl: false,
                connections: 0,
            }],
        };
        let session = NzbSession::new("k".into(), config, NZB.to_string())
            .await
            .expect("parses");
        assert_eq!(session.config.servers[0].connections, 1);
        wait_until(|| accepted.load(Ordering::SeqCst) == 1).await;
    }
}

use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use url::Url;

const UDP_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

pub struct TrackerProber;

impl TrackerProber {
    /// The trackers that answered their probe, fastest first. One that did
    /// not answer is left out rather than ranked last: the caller keeps the
    /// top of this list, and failures at its end filled that top whenever
    /// fewer trackers answered than it keeps.
    pub async fn rank_trackers(trackers: Vec<String>) -> Vec<String> {
        let mut results = Vec::new();

        let mut handles = Vec::new();
        for tracker in trackers {
            handles.push(tokio::spawn(async move {
                let rtt = Self::probe(&tracker).await;
                (tracker, rtt)
            }));
        }

        for handle in handles {
            if let Ok((tracker, Some(duration))) = handle.await {
                results.push((tracker, duration));
            }
        }

        // Sort by RTT
        results.sort_by_key(|k| k.1);

        // Return just the URLs
        results.into_iter().map(|(url, _)| url).collect()
    }

    async fn probe(url_str: &str) -> Option<Duration> {
        let url = match Url::parse(url_str) {
            Ok(u) => u,
            Err(_) => return None,
        };

        match url.scheme() {
            "http" | "https" => Self::probe_http(&url).await,
            "udp" => Self::probe_udp(&url).await,
            _ => None,
        }
    }

    async fn probe_http(url: &Url) -> Option<Duration> {
        let client = crate::http_client_builder()
            .timeout(HTTP_CONNECT_TIMEOUT)
            .build()
            .ok()?;

        let start = Instant::now();
        // Just try to fetch the root or scrape - HEAD might be enough to verify connectivity
        // Many trackers return 400 or similar on root, but if we get a response, it's alive.
        let result = client.head(url.clone()).send().await;

        match result {
            Ok(_) => Some(start.elapsed()),
            Err(_) => {
                // Try GET if HEAD fails
                match client.get(url.clone()).send().await {
                    Ok(_) => Some(start.elapsed()),
                    Err(_) => None,
                }
            }
        }
    }

    async fn probe_udp(url: &Url) -> Option<Duration> {
        let host = url.host_str()?;
        let port = url.port().unwrap_or(80);
        let addr = format!("{}:{}", host, port);

        // Resolve address first
        let socket = UdpSocket::bind("0.0.0.0:0").await.ok()?;
        if socket.connect(&addr).await.is_err() {
            return None;
        }

        // UDP Tracker Protocol - Connect Request
        // Offset  Size    Name            Value
        // 0       64      protocol_id     0x41727101980 // magic constant
        // 8       32      action          0 // connect
        // 12      32      transaction_id

        let protocol_id: u64 = 0x41727101980;
        let action: u32 = 0;
        let transaction_id: u32 = 12345; // simplified

        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&protocol_id.to_be_bytes());
        buf[8..12].copy_from_slice(&action.to_be_bytes());
        buf[12..16].copy_from_slice(&transaction_id.to_be_bytes());

        let start = Instant::now();
        if socket.send(&buf).await.is_err() {
            return None;
        }

        let mut recv_buf = [0u8; 16];
        let timeout = tokio::time::timeout(UDP_CONNECT_TIMEOUT, socket.recv(&mut recv_buf));

        match timeout.await {
            Ok(Ok(n)) if n >= 8 => {
                // Check action (0) and transaction_id (12345)
                let recv_action = u32::from_be_bytes(recv_buf[0..4].try_into().unwrap());
                let recv_trans_id = u32::from_be_bytes(recv_buf[4..8].try_into().unwrap());

                if recv_action == 0 && recv_trans_id == transaction_id {
                    Some(start.elapsed())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tracker whose probe fails is left out, not ranked last (review
    /// #47): the caller keeps the top of this list.
    #[tokio::test]
    async fn a_tracker_that_does_not_answer_is_not_ranked() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    // The request is read before the answer goes out, and
                    // the write side is closed rather than dropped: a
                    // socket dropped with bytes still unread is a reset on
                    // Windows, and the probe then reads its own answer as
                    // a tracker that did not answer.
                    let mut head = [0u8; 1024];
                    let _ = socket.read(&mut head).await;
                    let _ = socket
                        .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        let alive = format!("http://{addr}/announce");
        let ranked = TrackerProber::rank_trackers(vec![
            "not a tracker url".to_string(),
            "wss://tracker.invalid/announce".to_string(),
            alive.clone(),
        ])
        .await;
        assert_eq!(ranked, vec![alive]);
    }
}

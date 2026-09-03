//! One clear report on whether the mainline DHT works on this network.
//!
//! ## Why this exists
//!
//! On a real Android phone (motorola edge 60 pro, Android 16, mobile data)
//! every DHT bootstrap host failed for a whole 28-minute session, and
//! librqbit said so on every retry:
//!
//! ```text
//! WARN librqbit_dht::dht: error in bootstrap: no successful lookups, 0 errors retry_in=1.7s addr="router.bittorrent.com:6881"
//! ```
//!
//! That line comes from `bootstrap_hostname_with_backoff`
//! (`crates/dht/src/dht.rs`), whose `backon` retry notifier logs at WARN on
//! *every* attempt, with a delay that grows to ~120 s and a total budget of
//! 24 hours -- so a DHT-hostile network produces hundreds of identical
//! warnings and no conclusion. Worse, the message is misleading:
//! `bootstrap_hostname` runs the v4 and v6 lookups and returns `v4.or(v6)`,
//! and `Result::or` *discards* the v4 error whenever v4 failed. The bootstrap
//! hosts have no `AAAA` records, so the v6 branch starts with an empty
//! address set and returns `NoSuccessfulLookups { errors: 0 }` -- the "0
//! errors" is that empty v6 set, not evidence that nothing was tried over v4.
//!
//! None of it is caused by our configuration: the DHT binds its own
//! dual-stack UDP socket on its own port (`DhtState::with_config`), entirely
//! independent of the torrent listen port, so `TorrentListenPort::Ephemeral`
//! cannot affect it. It is the network dropping the DHT's UDP.
//!
//! ## What this module does instead
//!
//! Sample the routing table on a timer and report the *conclusion* once:
//! INFO when the DHT comes up, WARN once when it has demonstrably not come up
//! within [`BOOTSTRAP_GRACE`], INFO if it recovers after that. Every other
//! sample is a debug line. The per-retry librqbit warnings are turned down to
//! their own level in `DEFAULT_LOG_FILTER` (`crate::DEFAULT_LOG_FILTER`),
//! since this is now the thing that reports the state.
//!
//! The same state is served to clients as `dht` on `/stats.json` and as
//! [`crate::ServerHandle::dht_status`], so a client can say "DHT unavailable,
//! using trackers only" rather than pretending peer discovery is healthy.

use std::{sync::Arc, time::Duration};

use enginefs::EngineFS;
use enginefs::backend::DhtStatus;

/// How long a cold DHT is given before it is reported as unavailable.
/// Generous on purpose: a bootstrap round-trip over a slow mobile link plus
/// librqbit's own backoff can take tens of seconds, and a false "DHT
/// unavailable" is worse than a late true one.
pub const BOOTSTRAP_GRACE: Duration = Duration::from_secs(90);

/// How often the routing table is sampled. Two routing-table length reads,
/// so the interval is about report latency, not cost.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30);

/// The one-off conclusions worth a log line above debug.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DhtReport {
    /// The DHT has nodes: peer discovery is fully working.
    Bootstrapped,
    /// [`BOOTSTRAP_GRACE`] passed with an empty routing table. Reported once.
    Unavailable,
    /// It came up after [`DhtReport::Unavailable`] was reported, so that
    /// warning no longer holds.
    Recovered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Nothing reported yet, still inside the grace window.
    Waiting,
    /// [`DhtReport::Bootstrapped`] or [`DhtReport::Recovered`] was reported;
    /// nothing more to say for the rest of the session.
    Up,
    /// [`DhtReport::Unavailable`] was reported; only a recovery is news.
    Down,
    /// No DHT is running at all, so there is nothing to report ever.
    Disabled,
}

/// Turns a stream of [`DhtStatus`] samples into at most a couple of log-worthy
/// conclusions. Pure: `observe` takes the time since the first sample rather
/// than reading a clock, so the transitions are testable without sleeping.
#[derive(Debug)]
pub struct DhtHealthReporter {
    phase: Phase,
}

impl Default for DhtHealthReporter {
    fn default() -> Self {
        Self {
            phase: Phase::Waiting,
        }
    }
}

impl DhtHealthReporter {
    /// Fold in one sample taken `since_start` after the reporter began, and
    /// say what (if anything) is worth reporting above debug level.
    pub fn observe(&mut self, since_start: Duration, status: DhtStatus) -> Option<DhtReport> {
        if !status.enabled {
            // A backend with no DHT is a deliberate configuration, not a
            // network problem: latch it so a later sample cannot warn.
            self.phase = Phase::Disabled;
            return None;
        }
        match (self.phase, status.is_usable()) {
            (Phase::Disabled, _) | (Phase::Up, _) => None,
            (Phase::Waiting, true) => {
                self.phase = Phase::Up;
                Some(DhtReport::Bootstrapped)
            }
            (Phase::Waiting, false) if since_start >= BOOTSTRAP_GRACE => {
                self.phase = Phase::Down;
                Some(DhtReport::Unavailable)
            }
            (Phase::Waiting, false) => None,
            (Phase::Down, true) => {
                self.phase = Phase::Up;
                Some(DhtReport::Recovered)
            }
            (Phase::Down, false) => None,
        }
    }
}

/// Sample `engine`'s DHT forever, logging the conclusions from
/// [`DhtHealthReporter`] and everything else at debug.
pub fn start(engine: Arc<EngineFS>) -> tokio::task::JoinHandle<()> {
    super::logging::spawn_logged("dht-health", async move {
        let mut reporter = DhtHealthReporter::default();
        let start = tokio::time::Instant::now();
        let mut ticker = tokio::time::interval(POLL_INTERVAL);
        loop {
            // The first tick completes immediately, so the first sample is
            // taken at `since_start == 0` and cannot trip the grace window.
            ticker.tick().await;
            let status = engine.dht_status();
            let since_start = start.elapsed();
            match reporter.observe(since_start, status) {
                Some(DhtReport::Bootstrapped) => tracing::info!(
                    nodes = status.nodes,
                    nodes_v6 = status.nodes_v6,
                    after_secs = since_start.as_secs(),
                    "DHT bootstrapped; peer discovery uses the DHT and trackers"
                ),
                Some(DhtReport::Unavailable) => tracing::warn!(
                    after_secs = since_start.as_secs(),
                    "DHT did not bootstrap on this network; peer discovery is trackers-only. \
                     Torrents with working trackers are unaffected. This is normally the \
                     network dropping the DHT's UDP (carrier-grade NAT, a firewalled mobile \
                     APN, a captive portal), not a server fault. Retries continue in the \
                     background and are logged at debug; this warning is not repeated"
                ),
                Some(DhtReport::Recovered) => tracing::info!(
                    nodes = status.nodes,
                    nodes_v6 = status.nodes_v6,
                    after_secs = since_start.as_secs(),
                    "DHT bootstrapped after all; the earlier DHT-unavailable warning no \
                     longer applies"
                ),
                None => tracing::debug!(
                    enabled = status.enabled,
                    nodes = status.nodes,
                    nodes_v6 = status.nodes_v6,
                    ever_bootstrapped = status.ever_bootstrapped,
                    "DHT sample"
                ),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usable(nodes: u64) -> DhtStatus {
        DhtStatus {
            enabled: true,
            nodes,
            nodes_v6: 0,
            ever_bootstrapped: nodes > 0,
        }
    }

    fn cold() -> DhtStatus {
        DhtStatus {
            enabled: true,
            ..DhtStatus::default()
        }
    }

    #[test]
    fn a_dht_that_comes_up_is_reported_once_and_then_never_again() {
        let mut reporter = DhtHealthReporter::default();
        assert_eq!(reporter.observe(Duration::ZERO, cold()), None);
        assert_eq!(
            reporter.observe(Duration::from_secs(30), usable(8)),
            Some(DhtReport::Bootstrapped)
        );
        for secs in [60, 90, 600, 3600] {
            assert_eq!(
                reporter.observe(Duration::from_secs(secs), usable(8)),
                None,
                "{secs}s"
            );
        }
        // Even an empty routing table later is not news: it bootstrapped.
        assert_eq!(reporter.observe(Duration::from_secs(7200), cold()), None);
    }

    /// The whole point: half an hour of a dead DHT is ONE warning, not one
    /// per librqbit bootstrap retry.
    #[test]
    fn an_unreachable_dht_warns_once_not_every_sample() {
        let mut reporter = DhtHealthReporter::default();
        let mut reports = Vec::new();
        // 28 minutes of samples at the real poll interval -- the length of
        // the field session that prompted this.
        let samples = (28 * 60) / POLL_INTERVAL.as_secs();
        for tick in 0..samples {
            let since_start = POLL_INTERVAL * tick as u32;
            if let Some(report) = reporter.observe(since_start, cold()) {
                reports.push((since_start, report));
            }
        }
        assert_eq!(
            reports,
            vec![(BOOTSTRAP_GRACE, DhtReport::Unavailable)],
            "exactly one report, at the end of the grace window"
        );
    }

    #[test]
    fn nothing_is_reported_before_the_grace_window_expires() {
        let mut reporter = DhtHealthReporter::default();
        assert_eq!(
            reporter.observe(BOOTSTRAP_GRACE - Duration::from_secs(1), cold()),
            None
        );
        assert_eq!(
            reporter.observe(BOOTSTRAP_GRACE, cold()),
            Some(DhtReport::Unavailable)
        );
    }

    #[test]
    fn a_recovery_retracts_the_warning_once() {
        let mut reporter = DhtHealthReporter::default();
        assert_eq!(
            reporter.observe(BOOTSTRAP_GRACE, cold()),
            Some(DhtReport::Unavailable)
        );
        assert_eq!(reporter.observe(Duration::from_secs(120), cold()), None);
        assert_eq!(
            reporter.observe(Duration::from_secs(150), usable(3)),
            Some(DhtReport::Recovered)
        );
        assert_eq!(reporter.observe(Duration::from_secs(180), usable(3)), None);
        assert_eq!(reporter.observe(Duration::from_secs(210), cold()), None);
    }

    #[test]
    fn a_backend_without_a_dht_never_reports_anything() {
        let mut reporter = DhtHealthReporter::default();
        for secs in [0, 90, 600, 3600] {
            assert_eq!(
                reporter.observe(Duration::from_secs(secs), DhtStatus::default()),
                None,
                "{secs}s"
            );
        }
    }
}

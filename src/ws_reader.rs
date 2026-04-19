//! Phase 2: WebSocket reader and multi-relay supervisor.
//!
//! Owns the connection lifecycle, manages per-relay failure state,
//! handles failover per DESIGN.md §3 ("relay supervisor"), and pushes
//! raw binary frames into a bounded mpsc channel for the decoder task.
//!
//! # Architecture
//!
//! Single tokio task (`supervisor`) drives an outer loop:
//!
//! 1. Pick a relay using the §3 selection algorithm.
//! 2. If everyone is in cooldown, sleep until the soonest expiry.
//! 3. `run_one_connection` — connect via proto-blue-ws, then drain
//!    frames into the channel until the connection ends or shutdown
//!    fires.
//! 4. On disconnect, record the failure (possibly setting cooldown if
//!    the per-relay threshold was hit), back off, repeat.
//!
//! # Why we wrap `proto-blue-ws::recv` with a timeout
//!
//! `WebSocketKeepAlive::recv` runs an internal unbounded reconnect
//! loop pinned to a single URL: any reconnectable error (TCP RST,
//! heartbeat timeout, etc.) is silently retried with proto-blue-ws's
//! own backoff. Only a clean Close (`Ok(None)`) or a non-reconnectable
//! error surfaces. That breaks failover: a relay whose TCP keeps
//! refusing would trap the supervisor inside `recv` forever, and we'd
//! never get a chance to pick a fallback.
//!
//! `read_timeout` bounds that worst case. If `recv` doesn't return a
//! frame within the window, we drop the inner client and surrender
//! back to the outer loop, which records a failure and runs the
//! selection algorithm again. ATProto firehose traffic is continuous
//! (~1500 events/sec), so any silence longer than `read_timeout` is
//! genuinely degraded operation and should be treated as a failure.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use proto_blue_ws::{WebSocketKeepAlive, WebSocketKeepAliveOpts};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::RelayConfig;

/// Cap on the rolling reconnect-history ring buffer. At normal rates
/// (a handful of reconnects per hour) this is wildly over-provisioned;
/// the cap exists only to bound memory under pathological reconnect
/// loops.
const RECONNECT_RING_CAP: usize = 10_000;
const RECONNECT_HOUR: Duration = Duration::from_secs(3600);

/// Tunables that production callers shouldn't normally override but
/// tests do. Kept off `RelayConfig` so the user-facing TOML schema
/// stays focused on operational dials.
#[derive(Debug, Clone)]
pub struct WsReaderOptions {
    /// Capacity of the frame channel between the supervisor and
    /// downstream consumers (decoder task in Phase 3).
    pub frame_buffer: usize,

    /// How long a relay must be connected and producing frames before
    /// its failure counter is reset to zero (§3 "60s clean operation").
    pub clean_window: Duration,

    /// How many frames must arrive within `clean_window` before reset
    /// (§3 "≥10 events successfully published"). Phase 2 counts
    /// frames-from-WS as a proxy until the publisher exists.
    pub clean_frames: u64,

    /// Heartbeat interval handed to proto-blue-ws.
    pub heartbeat_interval_ms: u64,

    /// Outer read deadline. See module docs for why this exists.
    pub read_timeout: Duration,
}

impl Default for WsReaderOptions {
    fn default() -> Self {
        Self {
            frame_buffer: 1024,
            clean_window: Duration::from_secs(60),
            clean_frames: 10,
            heartbeat_interval_ms: 10_000,
            read_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
struct RelayState {
    url: String,
    consecutive_failures: u32,
    last_failed_at: Option<Instant>,
    cooldown_until: Option<Instant>,
    /// Phase 2 only tracks this — Phase 4 (cursor module) is what
    /// actually persists it. Kept here so failover preserves the
    /// per-relay last-known sequence across switches.
    last_cursor: Option<u64>,
}

impl RelayState {
    fn new(url: String) -> Self {
        Self {
            url,
            consecutive_failures: 0,
            last_failed_at: None,
            cooldown_until: None,
            last_cursor: None,
        }
    }

    fn in_cooldown(&self, now: Instant) -> bool {
        matches!(self.cooldown_until, Some(t) if t > now)
    }
}

/// Caller-visible snapshot of one relay's state.
#[derive(Debug, Clone)]
pub struct RelayStateSnapshot {
    pub url: String,
    pub consecutive_failures: u32,
    pub cooldown_until: Option<Instant>,
    pub last_cursor: Option<u64>,
}

/// Counters surfaced to the caller (and ultimately to `metrics.rs`).
#[derive(Debug, Clone)]
pub struct ReconnectMetrics {
    pub active_relay: Option<String>,
    pub total_reconnects_since_start: u64,
    pub reconnects_last_hour: u64,
}

#[derive(Debug)]
struct SharedState {
    relays: Vec<RelayState>,
    primary_index: usize,
    active_index: Option<usize>,
    total_reconnects: u64,
    reconnect_history: VecDeque<Instant>,
}

impl SharedState {
    fn from_config(cfg: &RelayConfig) -> Self {
        let mut relays = vec![RelayState::new(cfg.url.clone())];
        relays.extend(cfg.fallbacks.iter().cloned().map(RelayState::new));
        Self {
            relays,
            primary_index: 0,
            active_index: None,
            total_reconnects: 0,
            reconnect_history: VecDeque::new(),
        }
    }

    /// DESIGN.md §3 selection algorithm.
    fn pick_relay(&self, now: Instant) -> usize {
        if !self.relays[self.primary_index].in_cooldown(now) {
            return self.primary_index;
        }
        for (i, r) in self.relays.iter().enumerate() {
            if i != self.primary_index && !r.in_cooldown(now) {
                return i;
            }
        }
        // Everyone in cooldown — pick the soonest-expiring.
        self.relays
            .iter()
            .enumerate()
            .min_by_key(|(_, r)| r.cooldown_until)
            .map(|(i, _)| i)
            .unwrap_or(self.primary_index)
    }

    fn record_reconnect(&mut self, now: Instant) {
        self.total_reconnects = self.total_reconnects.saturating_add(1);
        self.reconnect_history.push_back(now);
        self.trim_history(now);
    }

    fn trim_history(&mut self, now: Instant) {
        if let Some(cutoff) = now.checked_sub(RECONNECT_HOUR) {
            while matches!(self.reconnect_history.front(), Some(&t) if t < cutoff) {
                self.reconnect_history.pop_front();
            }
        }
        while self.reconnect_history.len() > RECONNECT_RING_CAP {
            self.reconnect_history.pop_front();
        }
    }

    fn record_failure(
        &mut self,
        idx: usize,
        now: Instant,
        threshold: u32,
        cooldown: Duration,
    ) {
        let r = &mut self.relays[idx];
        r.consecutive_failures = r.consecutive_failures.saturating_add(1);
        r.last_failed_at = Some(now);
        if r.consecutive_failures >= threshold {
            r.cooldown_until = Some(now + cooldown);
        }
    }

    fn reset_failures(&mut self, idx: usize) {
        let r = &mut self.relays[idx];
        r.consecutive_failures = 0;
        r.cooldown_until = None;
    }

    fn metrics(&mut self, now: Instant) -> ReconnectMetrics {
        self.trim_history(now);
        ReconnectMetrics {
            active_relay: self.active_index.map(|i| self.relays[i].url.clone()),
            total_reconnects_since_start: self.total_reconnects,
            reconnects_last_hour: self.reconnect_history.len() as u64,
        }
    }

    fn snapshot(&self) -> Vec<RelayStateSnapshot> {
        self.relays
            .iter()
            .map(|r| RelayStateSnapshot {
                url: r.url.clone(),
                consecutive_failures: r.consecutive_failures,
                cooldown_until: r.cooldown_until,
                last_cursor: r.last_cursor,
            })
            .collect()
    }
}

/// Caller-facing handle to the spawned supervisor task.
pub struct WsReader {
    frames_rx: mpsc::Receiver<Vec<u8>>,
    task: JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
    state: Arc<Mutex<SharedState>>,
}

impl WsReader {
    /// Receive the next raw frame. Returns `None` once the supervisor
    /// has exited and the channel is drained.
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.frames_rx.recv().await
    }

    pub fn metrics(&self) -> ReconnectMetrics {
        self.state.lock().unwrap().metrics(Instant::now())
    }

    pub fn relay_states(&self) -> Vec<RelayStateSnapshot> {
        self.state.lock().unwrap().snapshot()
    }

    /// Signal shutdown and wait for the supervisor task to exit.
    /// The active connection is dropped without a close handshake
    /// (TCP RST is acceptable per DESIGN.md §3 shutdown ordering).
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

pub fn spawn(cfg: RelayConfig, opts: WsReaderOptions) -> WsReader {
    let (frames_tx, frames_rx) = mpsc::channel(opts.frame_buffer);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state = Arc::new(Mutex::new(SharedState::from_config(&cfg)));
    let task = tokio::spawn(supervisor(
        cfg,
        opts,
        frames_tx,
        shutdown_rx,
        state.clone(),
    ));
    WsReader {
        frames_rx,
        task,
        shutdown_tx,
        state,
    }
}

async fn supervisor(
    cfg: RelayConfig,
    opts: WsReaderOptions,
    frames_tx: mpsc::Sender<Vec<u8>>,
    mut shutdown_rx: watch::Receiver<bool>,
    state: Arc<Mutex<SharedState>>,
) {
    let initial_backoff = Duration::from_millis(cfg.reconnect_initial_delay_ms);
    let max_backoff = Duration::from_millis(cfg.reconnect_max_delay_ms);
    let cooldown = Duration::from_secs(cfg.failover_cooldown_seconds);
    let inner_max_secs = (cfg.reconnect_max_delay_ms / 1000).max(1);
    let mut backoff = initial_backoff;

    'outer: loop {
        if *shutdown_rx.borrow() {
            break;
        }

        let now = Instant::now();
        let (idx, url) = {
            let mut s = state.lock().unwrap();
            let i = s.pick_relay(now);
            s.active_index = Some(i);
            (i, s.relays[i].url.clone())
        };

        // If everyone is in cooldown, the picked relay is the
        // soonest-expiring one — sleep until that point first.
        let cooldown_remaining = state.lock().unwrap().relays[idx]
            .cooldown_until
            .and_then(|t| t.checked_duration_since(now));
        if let Some(d) = cooldown_remaining.filter(|d| *d > Duration::ZERO) {
            debug!(
                relay = %url,
                sleep_ms = d.as_millis() as u64,
                "all relays in cooldown; sleeping until soonest expiry"
            );
            tokio::select! {
                _ = tokio::time::sleep(d) => {}
                _ = shutdown_rx.changed() => break 'outer,
            }
        }

        let outcome = run_one_connection(
            &url,
            &opts,
            &state,
            idx,
            &frames_tx,
            &mut shutdown_rx,
            inner_max_secs,
        )
        .await;

        match outcome {
            ConnectionOutcome::Shutdown | ConnectionOutcome::DownstreamGone => {
                break 'outer;
            }
            ref failure => {
                let (consecutive, metrics) = {
                    let mut s = state.lock().unwrap();
                    s.record_failure(
                        idx,
                        Instant::now(),
                        cfg.failover_threshold,
                        cooldown,
                    );
                    let consecutive = s.relays[idx].consecutive_failures;
                    let metrics = s.metrics(Instant::now());
                    (consecutive, metrics)
                };
                info!(
                    target: "horizon_firehose::metrics",
                    event_type = "relay_disconnected",
                    relay = %url,
                    reason = ?failure,
                    consecutive_failures = consecutive,
                    total_reconnects_since_start = metrics.total_reconnects_since_start,
                    reconnects_last_hour = metrics.reconnects_last_hour,
                    "relay disconnected; will reattempt"
                );

                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = shutdown_rx.changed() => break 'outer,
                }
                backoff = backoff.saturating_mul(2).min(max_backoff);
                continue;
            }
        }
    }

    info!("ws_reader supervisor exiting");
}

#[derive(Debug)]
enum ConnectionOutcome {
    Shutdown,
    DownstreamGone,
    ConnectFailed,
    ReadTimeout,
    CleanlyClosed,
    Error,
}

async fn run_one_connection(
    url: &str,
    opts: &WsReaderOptions,
    state: &Arc<Mutex<SharedState>>,
    idx: usize,
    frames_tx: &mpsc::Sender<Vec<u8>>,
    shutdown_rx: &mut watch::Receiver<bool>,
    inner_max_secs: u64,
) -> ConnectionOutcome {
    info!(relay = %url, "connecting to relay");

    let mut ws = WebSocketKeepAlive::new(
        url,
        WebSocketKeepAliveOpts {
            max_reconnect_seconds: inner_max_secs,
            heartbeat_interval_ms: opts.heartbeat_interval_ms,
        },
    );

    let connect_res = tokio::select! {
        biased;
        _ = shutdown_rx.changed() => return ConnectionOutcome::Shutdown,
        r = ws.connect() => r,
    };
    if let Err(e) = connect_res {
        warn!(relay = %url, error = %e, "initial connect failed");
        return ConnectionOutcome::ConnectFailed;
    }

    let metrics = {
        let mut s = state.lock().unwrap();
        s.record_reconnect(Instant::now());
        s.metrics(Instant::now())
    };
    info!(
        target: "horizon_firehose::metrics",
        event_type = "relay_connected",
        relay = %url,
        total_reconnects_since_start = metrics.total_reconnects_since_start,
        reconnects_last_hour = metrics.reconnects_last_hour,
        "relay connected"
    );

    let connected_at = Instant::now();
    let mut frames_since: u64 = 0;
    let mut failures_reset = false;

    loop {
        let result = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => return ConnectionOutcome::Shutdown,
            r = tokio::time::timeout(opts.read_timeout, ws.recv()) => r,
        };

        let data = match result {
            Err(_) => {
                warn!(
                    relay = %url,
                    timeout_secs = opts.read_timeout.as_secs(),
                    "no data within read_timeout; treating as failure"
                );
                return ConnectionOutcome::ReadTimeout;
            }
            Ok(Ok(Some(data))) => data,
            Ok(Ok(None)) => {
                info!(relay = %url, "server closed connection cleanly");
                return ConnectionOutcome::CleanlyClosed;
            }
            Ok(Err(e)) => {
                warn!(relay = %url, error = %e, "ws recv error");
                return ConnectionOutcome::Error;
            }
        };

        frames_since += 1;
        if !failures_reset
            && frames_since >= opts.clean_frames
            && connected_at.elapsed() >= opts.clean_window
        {
            let mut s = state.lock().unwrap();
            if s.relays[idx].consecutive_failures > 0 {
                debug!(
                    relay = %s.relays[idx].url,
                    "resetting failure counter after clean operation"
                );
            }
            s.reset_failures(idx);
            failures_reset = true;
        }

        let send_res = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => return ConnectionOutcome::Shutdown,
            r = frames_tx.send(data) => r,
        };
        if send_res.is_err() {
            return ConnectionOutcome::DownstreamGone;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    // ─── pure-logic unit tests on SharedState ──────────────────────────

    fn relay_cfg(primary: &str, fallbacks: &[&str]) -> RelayConfig {
        RelayConfig {
            url: primary.to_string(),
            fallbacks: fallbacks.iter().map(|s| s.to_string()).collect(),
            reconnect_initial_delay_ms: 50,
            reconnect_max_delay_ms: 200,
            failover_threshold: 2,
            failover_cooldown_seconds: 1,
            tls_extra_ca_file: String::new(),
        }
    }

    #[test]
    fn pick_relay_returns_primary_when_not_in_cooldown() {
        let s = SharedState::from_config(&relay_cfg("ws://p", &["ws://a", "ws://b"]));
        assert_eq!(s.pick_relay(Instant::now()), 0);
    }

    #[test]
    fn pick_relay_skips_primary_in_cooldown() {
        let mut s = SharedState::from_config(&relay_cfg("ws://p", &["ws://a", "ws://b"]));
        s.relays[0].cooldown_until = Some(Instant::now() + Duration::from_secs(60));
        let pick = s.pick_relay(Instant::now());
        assert_eq!(pick, 1, "should pick first fallback");
    }

    #[test]
    fn pick_relay_returns_to_primary_after_cooldown_expires() {
        let mut s = SharedState::from_config(&relay_cfg("ws://p", &["ws://a"]));
        s.relays[0].cooldown_until = Some(Instant::now() - Duration::from_millis(1));
        // Cooldown is in the past — primary is no longer "in cooldown".
        assert_eq!(s.pick_relay(Instant::now()), 0);
    }

    #[test]
    fn pick_relay_picks_soonest_expiring_when_all_in_cooldown() {
        let mut s = SharedState::from_config(&relay_cfg("ws://p", &["ws://a", "ws://b"]));
        let now = Instant::now();
        s.relays[0].cooldown_until = Some(now + Duration::from_secs(30));
        s.relays[1].cooldown_until = Some(now + Duration::from_secs(60));
        s.relays[2].cooldown_until = Some(now + Duration::from_secs(10)); // soonest
        assert_eq!(s.pick_relay(now), 2);
    }

    #[test]
    fn record_failure_sets_cooldown_at_threshold() {
        let mut s = SharedState::from_config(&relay_cfg("ws://p", &[]));
        let now = Instant::now();
        s.record_failure(0, now, 3, Duration::from_secs(10));
        assert_eq!(s.relays[0].consecutive_failures, 1);
        assert!(s.relays[0].cooldown_until.is_none());
        s.record_failure(0, now, 3, Duration::from_secs(10));
        assert!(s.relays[0].cooldown_until.is_none());
        s.record_failure(0, now, 3, Duration::from_secs(10));
        assert_eq!(s.relays[0].consecutive_failures, 3);
        assert!(s.relays[0].cooldown_until.is_some());
    }

    #[test]
    fn reset_failures_clears_counter_and_cooldown() {
        let mut s = SharedState::from_config(&relay_cfg("ws://p", &[]));
        s.record_failure(0, Instant::now(), 1, Duration::from_secs(10));
        assert!(s.relays[0].cooldown_until.is_some());
        s.reset_failures(0);
        assert_eq!(s.relays[0].consecutive_failures, 0);
        assert!(s.relays[0].cooldown_until.is_none());
    }

    #[test]
    fn record_reconnect_trims_history_beyond_one_hour() {
        let mut s = SharedState::from_config(&relay_cfg("ws://p", &[]));
        let now = Instant::now();
        // Insert an old reconnect (>1h ago) directly, then record a new
        // one — the old entry should be trimmed.
        if let Some(old) = now.checked_sub(Duration::from_secs(3700)) {
            s.reconnect_history.push_back(old);
        }
        s.total_reconnects = 1;
        s.record_reconnect(now);
        assert_eq!(s.reconnect_history.len(), 1, "old entry should be trimmed");
        assert_eq!(s.total_reconnects, 2);
    }

    // ─── end-to-end tests with mock WebSocket servers ──────────────────

    fn test_options() -> WsReaderOptions {
        WsReaderOptions {
            frame_buffer: 16,
            clean_window: Duration::from_millis(100),
            clean_frames: 2,
            heartbeat_interval_ms: 60_000,
            // Long enough that no test trips it accidentally.
            read_timeout: Duration::from_secs(10),
        }
    }

    /// Mock WebSocket server. Each `behaviors[n]` controls connection N.
    /// If `behaviors` runs out, the last entry is reused.
    #[derive(Clone)]
    enum Behavior {
        /// Accept and immediately close the connection.
        CloseImmediately,
        /// Send these frames once, then hold the connection open until
        /// the client disconnects.
        SendThenHold(Vec<Vec<u8>>),
        /// Stream frames forever at `interval` until the client
        /// disconnects.
        StreamForever {
            interval: Duration,
            payload: Vec<u8>,
        },
    }

    struct MockServer {
        url: String,
        accepts: Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }

    impl MockServer {
        async fn shutdown(self) {
            self.task.abort();
            let _ = self.task.await;
        }
    }

    async fn start_mock(behaviors: Vec<Behavior>) -> MockServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let accepts = Arc::new(AtomicUsize::new(0));
        let accepts_c = accepts.clone();
        let behaviors = Arc::new(behaviors);
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let n = accepts_c.fetch_add(1, Ordering::SeqCst);
                let behavior = behaviors
                    .get(n)
                    .cloned()
                    .or_else(|| behaviors.last().cloned())
                    .unwrap_or(Behavior::CloseImmediately);
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_async(stream).await else { return };
                    match behavior {
                        Behavior::CloseImmediately => {
                            let _ = ws.close(None).await;
                        }
                        Behavior::SendThenHold(frames) => {
                            for f in frames {
                                if ws.send(Message::Binary(f.into())).await.is_err() {
                                    return;
                                }
                            }
                            while ws.next().await.is_some() {}
                        }
                        Behavior::StreamForever { interval, payload } => loop {
                            if ws
                                .send(Message::Binary(payload.clone().into()))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            tokio::time::sleep(interval).await;
                        },
                    }
                });
            }
        });
        MockServer { url, accepts, task }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connects_and_receives_frames() {
        let server = start_mock(vec![Behavior::SendThenHold(vec![
            b"hello".to_vec(),
            b"world".to_vec(),
        ])])
        .await;
        let cfg = relay_cfg(&server.url, &[]);
        let mut reader = spawn(cfg, test_options());

        let f1 = tokio::time::timeout(Duration::from_secs(3), reader.recv())
            .await
            .expect("recv timed out")
            .expect("channel closed");
        let f2 = tokio::time::timeout(Duration::from_secs(3), reader.recv())
            .await
            .expect("recv timed out")
            .expect("channel closed");
        assert_eq!(f1, b"hello");
        assert_eq!(f2, b"world");

        let metrics = reader.metrics();
        assert_eq!(metrics.total_reconnects_since_start, 1);
        assert_eq!(metrics.active_relay.as_deref(), Some(server.url.as_str()));

        reader.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnects_after_disconnection() {
        // First connection closes immediately; second delivers a frame.
        let server = start_mock(vec![
            Behavior::CloseImmediately,
            Behavior::SendThenHold(vec![b"after-reconnect".to_vec()]),
        ])
        .await;
        let cfg = relay_cfg(&server.url, &[]);
        let mut reader = spawn(cfg, test_options());

        let frame = tokio::time::timeout(Duration::from_secs(3), reader.recv())
            .await
            .expect("recv timed out")
            .expect("channel closed");
        assert_eq!(frame, b"after-reconnect");

        // Server should have accepted at least 2 connections.
        assert!(
            server.accepts.load(Ordering::SeqCst) >= 2,
            "expected ≥2 accepts, got {}",
            server.accepts.load(Ordering::SeqCst)
        );

        let metrics = reader.metrics();
        assert!(metrics.total_reconnects_since_start >= 2);

        reader.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fails_over_to_fallback_after_threshold() {
        // Primary always closes immediately; fallback delivers frames.
        let primary = start_mock(vec![Behavior::CloseImmediately]).await;
        let fallback = start_mock(vec![Behavior::SendThenHold(vec![
            b"from-fallback".to_vec(),
        ])])
        .await;

        let cfg = relay_cfg(&primary.url, &[fallback.url.as_str()]);
        let mut reader = spawn(cfg, test_options());

        let frame = tokio::time::timeout(Duration::from_secs(5), reader.recv())
            .await
            .expect("recv timed out")
            .expect("channel closed");
        assert_eq!(frame, b"from-fallback");

        // Primary should have been hit at least failover_threshold times
        // before we gave up and switched.
        assert!(
            primary.accepts.load(Ordering::SeqCst) >= 2,
            "primary accepts = {}",
            primary.accepts.load(Ordering::SeqCst)
        );
        assert!(fallback.accepts.load(Ordering::SeqCst) >= 1);

        let states = reader.relay_states();
        assert_eq!(states[0].url, primary.url);
        assert!(
            states[0].cooldown_until.is_some(),
            "primary should be in cooldown after threshold failures"
        );
        assert_eq!(states[1].url, fallback.url);

        reader.shutdown().await;
        primary.shutdown().await;
        fallback.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn skips_relays_in_cooldown() {
        // Both servers are broken initially. We just want to confirm
        // that once primary is in cooldown, the supervisor *picks* the
        // fallback rather than re-trying primary.
        let primary = start_mock(vec![Behavior::CloseImmediately]).await;
        let fallback = start_mock(vec![Behavior::CloseImmediately]).await;

        // Tighten cooldown so we don't loop too long. cfg.failover_threshold
        // is 2, cooldown 1s. After primary's 2 failures it cools down for
        // 1s; fallback should get attempted in that window.
        let cfg = relay_cfg(&primary.url, &[fallback.url.as_str()]);
        let reader = spawn(cfg, test_options());

        // Give the supervisor enough time to fail primary twice and try
        // the fallback at least once.
        tokio::time::sleep(Duration::from_millis(800)).await;

        assert!(
            fallback.accepts.load(Ordering::SeqCst) >= 1,
            "fallback should have been attempted while primary was in cooldown; \
             primary={}, fallback={}",
            primary.accepts.load(Ordering::SeqCst),
            fallback.accepts.load(Ordering::SeqCst)
        );

        reader.shutdown().await;
        primary.shutdown().await;
        fallback.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn returns_to_primary_after_cooldown_expires() {
        // Primary closes the first 3 connections, then accepts and
        // streams. Fallback also closes immediately. cfg cooldown is 1s.
        // Sequence: primary fails 2× (cooldown), fallback fails 2×
        // (cooldown), wait for primary cooldown to expire, primary
        // reattempted, eventually delivers frames.
        let primary = start_mock(vec![
            Behavior::CloseImmediately,
            Behavior::CloseImmediately,
            Behavior::CloseImmediately,
            Behavior::StreamForever {
                interval: Duration::from_millis(20),
                payload: b"primary-recovered".to_vec(),
            },
        ])
        .await;
        let fallback = start_mock(vec![Behavior::CloseImmediately]).await;

        let cfg = relay_cfg(&primary.url, &[fallback.url.as_str()]);
        let mut reader = spawn(cfg, test_options());

        let frame = tokio::time::timeout(Duration::from_secs(8), reader.recv())
            .await
            .expect("recv timed out")
            .expect("channel closed");
        assert_eq!(frame, b"primary-recovered");

        // We should have ended up back on primary.
        assert_eq!(reader.metrics().active_relay.as_deref(), Some(primary.url.as_str()));

        reader.shutdown().await;
        primary.shutdown().await;
        fallback.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resets_failure_counter_after_clean_operation_window() {
        // One disconnect to bump consecutive_failures, then a stable
        // stream that should reset it.
        let server = start_mock(vec![
            Behavior::CloseImmediately,
            Behavior::StreamForever {
                interval: Duration::from_millis(10),
                payload: b"x".to_vec(),
            },
        ])
        .await;
        let cfg = relay_cfg(&server.url, &[]);
        let opts = WsReaderOptions {
            clean_window: Duration::from_millis(100),
            clean_frames: 2,
            ..test_options()
        };
        let mut reader = spawn(cfg, opts);

        // Drain frames while the clean window elapses.
        let deadline = Instant::now() + Duration::from_millis(400);
        while Instant::now() < deadline {
            let _ = tokio::time::timeout(Duration::from_millis(50), reader.recv()).await;
        }

        let states = reader.relay_states();
        assert_eq!(
            states[0].consecutive_failures, 0,
            "failure counter should have been reset after clean operation"
        );
        assert!(states[0].cooldown_until.is_none());

        reader.shutdown().await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_returns_quickly_without_close_handshake() {
        // Stream frames; shutdown is signalled mid-stream. We assert the
        // task exits quickly (well under any plausible close-handshake
        // round-trip) — proving we abort the connection rather than
        // waiting for handshake.
        let server = start_mock(vec![Behavior::StreamForever {
            interval: Duration::from_millis(5),
            payload: b"f".to_vec(),
        }])
        .await;
        let cfg = relay_cfg(&server.url, &[]);
        let mut reader = spawn(cfg, test_options());

        // Wait for at least one frame so we know we're connected.
        let _ = tokio::time::timeout(Duration::from_secs(2), reader.recv())
            .await
            .expect("recv timed out");

        let start = Instant::now();
        reader.shutdown().await;
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "shutdown took {elapsed:?} — too long; should be near-instant"
        );

        server.shutdown().await;
    }
}

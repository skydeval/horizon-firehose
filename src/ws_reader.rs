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
//! # Historical note on the silent-relay / runaway-reconnect problem
//!
//! Pre-Phase-8.6, `WebSocketKeepAlive::recv` retried reconnectable
//! errors indefinitely and never surfaced silence on a
//! TCP-accepting-then-not-sending relay. Our supervisor wrapped
//! `recv` in an external `tokio::time::timeout(60s)` and tracked
//! outer-loop failures manually to implement failover.
//!
//! **Closed by upstream as of proto-blue 0.2.4** (commit
//! `9bc4e51`, Doll): `WebSocketKeepAliveOpts` now takes a
//! `per_recv_timeout_ms` (bounds silence, triggers an internal
//! reconnect) and `max_reconnect_attempts` (caps internal retries,
//! surfaces `WsError::ReconnectExhausted { attempts }` when hit).
//! Our supervisor matches on that variant and rotates to the
//! next relay in the fallback list. Both features are native; no
//! external wrapper needed. See
//! <https://github.com/dollspace-gay/proto-blue/issues/3> — closed
//! by upstream, integrated in horizon-firehose phase 8.6.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use proto_blue_ws::{
    TungsteniteConnector, WebSocketConnector, WebSocketKeepAlive, WebSocketKeepAliveOpts,
};
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::RelayConfig;
use crate::cursor::Cursors;
use crate::metrics::Metrics;

/// One raw WebSocket frame plus the relay URL it came from. The decoder
/// stamps each decoded event with this so the publisher (and the
/// per-relay cursor tracker) knows which relay's sequence number
/// advanced.
pub type Frame = (Vec<u8>, String);

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

    /// Upper bound on silence from a TCP-accepting-then-not-sending
    /// relay. Passed to `WebSocketKeepAliveOpts::per_recv_timeout_ms`.
    /// ATProto firehose is continuous (~1500 events/sec) so 60 s
    /// silence is degraded operation by definition — the SDK
    /// reconnects internally when this trips.
    pub per_recv_timeout: Duration,

    /// Cap on consecutive failed reconnects before the SDK surfaces
    /// `WsError::ReconnectExhausted`. Passed to
    /// `WebSocketKeepAliveOpts::max_reconnect_attempts`. When hit,
    /// the supervisor rotates to the next relay in the fallback
    /// list.
    pub max_reconnect_attempts: u32,

    /// Phase 8.7: optional custom `rustls::ClientConfig` (system
    /// roots + any `relay.tls_extra_ca_file` CAs). `None` — the
    /// common case — uses proto-blue-ws's default `native-tls`
    /// path against system roots. `Some(...)` routes every connect
    /// through `TungsteniteConnector::with_rustls_config` so the
    /// extra CAs take effect at the TLS handshake.
    pub tls_config: Option<Arc<rustls::ClientConfig>>,
}

impl Default for WsReaderOptions {
    fn default() -> Self {
        Self {
            frame_buffer: 1024,
            clean_window: Duration::from_secs(60),
            clean_frames: 10,
            heartbeat_interval_ms: 10_000,
            per_recv_timeout: Duration::from_secs(60),
            max_reconnect_attempts: 5,
            tls_config: None,
        }
    }
}

/// Build the WebSocket connector once per supervisor lifetime. When
/// `tls_config` is `Some`, the connector carries that
/// `rustls::ClientConfig` so every `connect()` routes through
/// `connect_async_tls_with_config(Connector::Rustls(...))`. With
/// `None`, we use the default `TungsteniteConnector`, which in turn
/// uses `tokio_tungstenite::connect_async` — that path is backed by
/// `native-tls`, not rustls, so the rustls crate never touches a
/// handshake in the common-case deployment.
fn build_connector(tls: Option<&Arc<rustls::ClientConfig>>) -> Arc<dyn WebSocketConnector> {
    match tls {
        Some(config) => Arc::new(TungsteniteConnector::new().with_rustls_config(config.clone())),
        None => Arc::new(TungsteniteConnector::new()),
    }
}

#[derive(Debug, Clone)]
struct RelayState {
    url: String,
    consecutive_failures: u32,
    last_failed_at: Option<Instant>,
    cooldown_until: Option<Instant>,
}

impl RelayState {
    fn new(url: String) -> Self {
        Self {
            url,
            consecutive_failures: 0,
            last_failed_at: None,
            cooldown_until: None,
        }
    }

    fn in_cooldown(&self, now: Instant) -> bool {
        matches!(self.cooldown_until, Some(t) if t > now)
    }
}

/// Caller-visible snapshot of one relay's state. Cursor tracking lives
/// in [`crate::cursor::Cursors`] now; it's shared across ws_reader and
/// publisher so there's no per-relay cursor stored on these snapshots.
#[derive(Debug, Clone)]
pub struct RelayStateSnapshot {
    pub url: String,
    pub consecutive_failures: u32,
    pub cooldown_until: Option<Instant>,
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
    /// Phase 8.5 follow-up finding 2.3: set by `reset_failures` when
    /// the clean-operation threshold is reached mid-connection;
    /// observed (and cleared) by the supervisor loop to reset the
    /// local `backoff` variable. Without this, a succeed → disconnect
    /// cycle would carry the pre-reset compounded backoff forward
    /// even though `consecutive_failures` had gone back to zero.
    clean_since_last_backoff: bool,
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
            clean_since_last_backoff: false,
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

    fn record_failure(&mut self, idx: usize, now: Instant, threshold: u32, cooldown: Duration) {
        let r = &mut self.relays[idx];
        r.consecutive_failures = r.consecutive_failures.saturating_add(1);
        r.last_failed_at = Some(now);
        if r.consecutive_failures >= threshold {
            r.cooldown_until = Some(now + cooldown);
        }
    }

    /// Phase 8.6: SDK surfaced `WsError::ReconnectExhausted` —
    /// the inner client already tried `attempts` reconnects. That's
    /// enough signal to treat this relay as bad *right now* without
    /// needing `failover_threshold` more outer-loop failures to
    /// reach cooldown. Push the relay straight into cooldown so the
    /// next `pick_relay` call picks a fallback.
    fn force_cooldown(&mut self, idx: usize, now: Instant, cooldown: Duration) {
        let r = &mut self.relays[idx];
        r.consecutive_failures = r.consecutive_failures.saturating_add(1);
        r.last_failed_at = Some(now);
        r.cooldown_until = Some(now + cooldown);
    }

    fn reset_failures(&mut self, idx: usize) {
        let r = &mut self.relays[idx];
        r.consecutive_failures = 0;
        r.cooldown_until = None;
        // Phase 8.5 follow-up finding 2.3: a clean-operation reset
        // must also signal the supervisor to reset its local
        // `backoff` variable. Otherwise the next disconnect would
        // sleep for the compounded value from the pre-reset
        // failures, even though we've proven the relay is healthy.
        self.clean_since_last_backoff = true;
    }

    /// Read-and-clear the clean-operation flag. The supervisor calls
    /// this after each `run_one_connection` returns; if true, it
    /// resets its local backoff to the initial value.
    fn take_clean_flag(&mut self) -> bool {
        std::mem::replace(&mut self.clean_since_last_backoff, false)
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
            })
            .collect()
    }
}

/// Caller-facing handle to the spawned supervisor task.
///
/// When built via [`spawn`] the handle also owns an internal
/// `shutdown_tx` so [`shutdown`][Self::shutdown] can flip the signal.
/// When built via [`spawn_with_cursors`] the caller owns the shutdown
/// sender externally — the handle is purely a receiver + task-join
/// — so [`shutdown`][Self::shutdown] only awaits the task (the
/// shutdown signal must be raised by the caller).
pub struct WsReader {
    frames_rx: mpsc::Receiver<Frame>,
    pub(crate) task: JoinHandle<()>,
    shutdown_tx: Option<watch::Sender<bool>>,
    state: Arc<Mutex<SharedState>>,
    /// A weak reference to the frames-channel sender, kept so the
    /// metrics emitter can sample `ws_to_decoder` depth without
    /// preventing channel close during shutdown (`mpsc::WeakSender`
    /// doesn't count toward keep-alive).
    frames_weak: mpsc::WeakSender<Frame>,
    /// Shared `Notify` so the decoder's circuit breaker can ask us
    /// to drop the current connection. See [`WsReaderControl`].
    force_reconnect: Arc<Notify>,
}

/// Cloneable accessor for ws_reader's supervisor state. Produced by
/// [`WsReader::state_reader`] *before* `WsReader` is consumed by the
/// decoder task, so `main` (and the metrics emitter) can still read
/// reconnect stats later without threading a handle through the
/// pipeline.
#[derive(Clone)]
pub struct WsStateReader {
    state: Arc<Mutex<SharedState>>,
}

/// Snapshot shape convenient for the metrics emitter.
///
/// `total_reconnects_since_start` lives on the shared [`Metrics`]
/// struct instead (single source of truth for the periodic emitter),
/// so only the currently-active relay and the rolling hour gauge
/// come through this type.
#[derive(Debug, Clone, Default)]
pub struct WsStateSnapshot {
    pub active_relay: Option<String>,
    pub reconnects_last_hour: u64,
}

impl WsStateReader {
    pub async fn snapshot(&self, now: Instant) -> WsStateSnapshot {
        let mut s = self.state.lock().unwrap();
        let m = s.metrics(now);
        WsStateSnapshot {
            active_relay: m.active_relay,
            reconnects_last_hour: m.reconnects_last_hour,
        }
    }
}

/// Cloneable control handle for the ws_reader supervisor. Exposes
/// `trigger_force_reconnect` so downstream tasks can ask the
/// supervisor to drop the current connection and run its failover
/// selection algorithm — used by the decoder's circuit breaker
/// (Phase 8.5 review finding 3.4) when consecutive decode failures
/// indicate the current relay is sending garbage.
///
/// Decoupled from `WsStateReader` because read access is a different
/// capability from the ability to force a reconnect. Keeping them
/// separate means tests that just want to observe don't accidentally
/// get a trigger.
#[derive(Clone)]
pub struct WsReaderControl {
    force_reconnect: Arc<Notify>,
}

impl WsReaderControl {
    /// Ask the supervisor to abort the current connection and re-run
    /// relay selection. The supervisor observes this in its inner
    /// `select!` and treats it as if the connection dropped — the
    /// failing relay's failure counter increments as normal, so
    /// repeated forced reconnects will push it into cooldown and
    /// promote a fallback.
    pub fn trigger_force_reconnect(&self) {
        self.force_reconnect.notify_waiters();
    }
}

impl WsReader {
    /// Receive the next raw frame together with the relay URL it came
    /// from. Returns `None` once the supervisor has exited and the
    /// channel is drained.
    pub async fn recv(&mut self) -> Option<Frame> {
        self.frames_rx.recv().await
    }

    pub fn metrics(&self) -> ReconnectMetrics {
        self.state.lock().unwrap().metrics(Instant::now())
    }

    pub fn relay_states(&self) -> Vec<RelayStateSnapshot> {
        self.state.lock().unwrap().snapshot()
    }

    /// Clone the supervisor state so `main` can read reconnect stats
    /// after this `WsReader` has been moved into the decoder task.
    pub fn state_reader(&self) -> WsStateReader {
        WsStateReader {
            state: self.state.clone(),
        }
    }

    /// WeakSender for the frames channel. Used by the metrics emitter
    /// to sample `ws_to_decoder` depth without keeping the channel
    /// alive past the ws_reader's exit.
    pub fn frames_weak(&self) -> mpsc::WeakSender<Frame> {
        self.frames_weak.clone()
    }

    /// Control handle that can trigger a forced reconnect. Used by
    /// the decoder's circuit breaker (Phase 8.5 review finding 3.4).
    pub fn control(&self) -> WsReaderControl {
        WsReaderControl {
            force_reconnect: self.force_reconnect.clone(),
        }
    }

    /// Signal shutdown (if the handle owns its shutdown sender) and
    /// wait for the supervisor task to exit. The active connection is
    /// dropped without a close handshake — TCP RST is acceptable per
    /// DESIGN.md §3 shutdown ordering.
    pub async fn shutdown(self) {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(true);
        }
        let _ = self.task.await;
    }
}

/// Spawn the supervisor with no saved cursors and an internal
/// shutdown-watch channel. Used by phase 2 tests that predate the
/// Phase 4 cursor tracker and the Phase 5 global-shutdown wiring.
pub fn spawn(cfg: RelayConfig, opts: WsReaderOptions) -> WsReader {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    spawn_internal(
        cfg,
        opts,
        Cursors::new(),
        shutdown_rx,
        Some(shutdown_tx),
        Arc::new(Metrics::default()),
    )
}

/// Spawn the supervisor against an **external** shutdown watch and a
/// shared [`Metrics`] struct. Used by the Phase 5 coordinator in
/// `main`: one global shutdown signal fans out to every task, and the
/// coordinator drives the DESIGN.md §3 shutdown cascade by flipping
/// that single watch. The Phase 6 [`Metrics`] accumulates reconnect
/// counts and history.
///
/// `cursors` is shared with the publisher. On each connect/reconnect,
/// the supervisor reads the current cursor for the chosen relay and
/// appends `?cursor=N` so the relay resumes from our last-confirmed
/// XADD.
pub fn spawn_with_cursors(
    cfg: RelayConfig,
    opts: WsReaderOptions,
    cursors: Cursors,
    shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> WsReader {
    spawn_internal(cfg, opts, cursors, shutdown_rx, None, metrics)
}

fn spawn_internal(
    cfg: RelayConfig,
    opts: WsReaderOptions,
    cursors: Cursors,
    shutdown_rx: watch::Receiver<bool>,
    shutdown_tx: Option<watch::Sender<bool>>,
    metrics: Arc<Metrics>,
) -> WsReader {
    let (frames_tx, frames_rx) = mpsc::channel(opts.frame_buffer);
    let frames_weak = frames_tx.downgrade();
    let state = Arc::new(Mutex::new(SharedState::from_config(&cfg)));
    let force_reconnect = Arc::new(Notify::new());
    let task = crate::spawn_instrumented(
        "ws_reader_supervisor",
        supervisor(
            cfg,
            opts,
            frames_tx,
            shutdown_rx,
            state.clone(),
            cursors,
            metrics,
            force_reconnect.clone(),
        ),
    );
    WsReader {
        frames_rx,
        task,
        shutdown_tx,
        state,
        frames_weak,
        force_reconnect,
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervisor(
    cfg: RelayConfig,
    opts: WsReaderOptions,
    frames_tx: mpsc::Sender<Frame>,
    mut shutdown_rx: watch::Receiver<bool>,
    state: Arc<Mutex<SharedState>>,
    cursors: Cursors,
    metrics: Arc<Metrics>,
    force_reconnect: Arc<Notify>,
) {
    let initial_backoff = Duration::from_millis(cfg.reconnect_initial_delay_ms);
    let max_backoff = Duration::from_millis(cfg.reconnect_max_delay_ms);
    let cooldown = Duration::from_secs(cfg.failover_cooldown_seconds);
    let inner_max_secs = (cfg.reconnect_max_delay_ms / 1000).max(1);
    let mut backoff = initial_backoff;

    // Built once per supervisor lifetime — clones are cheap
    // (`Arc<dyn WebSocketConnector>`) and each reconnect hands the
    // same underlying connector to a fresh `WebSocketKeepAlive`.
    let connector = build_connector(opts.tls_config.as_ref());

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

        // Read the saved cursor (if any) for this relay and build the
        // connection URL with a `?cursor=N` query param so the relay
        // resumes from our last-confirmed position. Cursors are shared
        // with the publisher, which advances them on XADD success —
        // so any reconnect mid-stream picks up the latest.
        let connect_url = match cursors.get(&url).await {
            Some(seq) => format!("{}{}cursor={seq}", url, url_separator(&url)),
            None => url.clone(),
        };

        let outcome = run_one_connection(
            &connect_url,
            &url,
            &opts,
            &state,
            idx,
            &frames_tx,
            &mut shutdown_rx,
            inner_max_secs,
            &metrics,
            &force_reconnect,
            connector.clone(),
        )
        .await;

        match outcome {
            ConnectionOutcome::Shutdown | ConnectionOutcome::DownstreamGone => {
                break 'outer;
            }
            ref failure => {
                // Phase 8.5 follow-up finding 2.3: if clean
                // operation was achieved at any point during this
                // connection, the supervisor's local `backoff`
                // must reset to `initial_backoff` before the next
                // disconnect grows it. Otherwise a long-running
                // healthy session that eventually disconnects would
                // sleep for the pre-reset compounded value.
                //
                // Phase 8.6: if the outcome is `ReconnectExhausted`,
                // the SDK already tried its inner budget of reconnects
                // on this URL. Push the relay straight into cooldown
                // instead of waiting for another `failover_threshold`
                // outer failures — the evidence is already in.
                let force_cooldown =
                    matches!(outcome, ConnectionOutcome::ReconnectExhausted { .. });
                let (consecutive, reconnect_metrics, had_clean_op) = {
                    let mut s = state.lock().unwrap();
                    let had_clean_op = s.take_clean_flag();
                    if force_cooldown {
                        s.force_cooldown(idx, Instant::now(), cooldown);
                    } else {
                        s.record_failure(idx, Instant::now(), cfg.failover_threshold, cooldown);
                    }
                    let consecutive = s.relays[idx].consecutive_failures;
                    let rm = s.metrics(Instant::now());
                    (consecutive, rm, had_clean_op)
                };
                if had_clean_op {
                    backoff = initial_backoff;
                }
                info!(
                    target: "horizon_firehose::metrics",
                    event_type = "relay_disconnected",
                    relay = %url,
                    reason = ?failure,
                    consecutive_failures = consecutive,
                    total_reconnects_since_start = reconnect_metrics.total_reconnects_since_start,
                    reconnects_last_hour = reconnect_metrics.reconnects_last_hour,
                    backoff_ms = backoff.as_millis() as u64,
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
    /// Phase 8.6: SDK surfaced `WsError::ReconnectExhausted` after
    /// its internal reconnect budget (`max_reconnect_attempts`) was
    /// exhausted. Treated as a terminal failure for this relay;
    /// supervisor rotates to the next fallback. The carried
    /// `attempts` count is read via `{failure:?}` in the
    /// `relay_disconnected` structured log line — dead-code
    /// analysis doesn't see that usage.
    #[allow(dead_code)]
    ReconnectExhausted {
        attempts: u32,
    },
    CleanlyClosed,
    Error,
    /// Decoder asked us to reconnect because consecutive decode
    /// failures indicate garbage from this relay. Treated as a
    /// failure for cooldown/failover purposes — repeated forces
    /// push the relay into cooldown and promote a fallback.
    ForcedReconnect,
}

/// Drive one WebSocket connection attempt. `connect_url` may include
/// a `?cursor=N` resume param (the WebSocket client sees this);
/// `relay_label` is the canonical URL used for cursor lookups, log
/// fields, and per-frame stamping.
#[allow(clippy::too_many_arguments)]
async fn run_one_connection(
    connect_url: &str,
    relay_label: &str,
    opts: &WsReaderOptions,
    state: &Arc<Mutex<SharedState>>,
    idx: usize,
    frames_tx: &mpsc::Sender<Frame>,
    shutdown_rx: &mut watch::Receiver<bool>,
    inner_max_secs: u64,
    metrics: &Metrics,
    force_reconnect: &Notify,
    connector: Arc<dyn WebSocketConnector>,
) -> ConnectionOutcome {
    // `connect_url` is `relay_label + ?cursor=N` — both components are
    // userinfo-free (validator rejects userinfo-bearing relay URLs in
    // config.rs, and the cursor is a numeric seq), so the log output
    // carries no credentials. Sanitizer here is defense-in-depth per
    // Phase 8.5 review finding 4.4.
    info!(
        relay = %crate::config::sanitize_ws_url(relay_label),
        connect_url = %crate::config::sanitize_ws_url(connect_url),
        "connecting to relay"
    );

    let mut ws = WebSocketKeepAlive::with_connector(
        connect_url,
        WebSocketKeepAliveOpts {
            max_reconnect_seconds: inner_max_secs,
            heartbeat_interval_ms: opts.heartbeat_interval_ms,
            // Phase 8.6: SDK-native silence bound. Replaces our old
            // `tokio::time::timeout(read_timeout, ws.recv())` wrapper.
            per_recv_timeout_ms: Some(opts.per_recv_timeout.as_millis() as u64),
            // Phase 8.6: SDK-native reconnect cap. When hit, recv
            // returns `WsError::ReconnectExhausted` and we rotate to
            // the next fallback relay.
            max_reconnect_attempts: Some(opts.max_reconnect_attempts),
        },
        // Phase 8.7: hand proto-blue-ws our connector so custom
        // `rustls::ClientConfig` (from `tls_extra_ca_file`) — if any
        // — is honoured at handshake time. For the common case
        // (empty `tls_extra_ca_file`), this is a default
        // `TungsteniteConnector` and TLS goes through native-tls
        // unchanged.
        connector,
    );

    let connect_res = tokio::select! {
        biased;
        _ = shutdown_rx.changed() => return ConnectionOutcome::Shutdown,
        r = ws.connect() => r,
    };
    if let Err(e) = connect_res {
        warn!(relay = %relay_label, error = %e, "initial connect failed");
        return ConnectionOutcome::ConnectFailed;
    }

    let now = Instant::now();
    let reconnect_metrics = {
        let mut s = state.lock().unwrap();
        s.record_reconnect(now);
        s.metrics(now)
    };
    // Mirror the reconnect into the shared Phase 6 Metrics so the
    // periodic emitter's `relay_reconnects_in_window` /
    // `total_reconnects_since_start` / `reconnects_last_hour` all
    // come from the same source. Supervisor's internal counters stay
    // for structured log lines above.
    metrics.record_reconnect(now).await;

    info!(
        target: "horizon_firehose::metrics",
        event_type = "relay_connected",
        relay = %relay_label,
        total_reconnects_since_start = reconnect_metrics.total_reconnects_since_start,
        reconnects_last_hour = reconnect_metrics.reconnects_last_hour,
        "relay connected"
    );

    let connected_at = Instant::now();
    let mut frames_since: u64 = 0;
    let mut failures_reset = false;

    loop {
        // Phase 8.6: no more `tokio::time::timeout(opts.read_timeout,
        // ws.recv())` wrapper. proto-blue-ws bounds silence itself
        // via `per_recv_timeout_ms` (set above) and triggers an
        // internal reconnect; if that reconnect loop exhausts the
        // configured cap, `recv` surfaces `WsError::ReconnectExhausted`
        // and we rotate to the next fallback relay.
        let result = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => return ConnectionOutcome::Shutdown,
            _ = force_reconnect.notified() => {
                info!(
                    relay = %relay_label,
                    "forced reconnect requested by downstream (decoder circuit breaker)"
                );
                return ConnectionOutcome::ForcedReconnect;
            }
            r = ws.recv() => r,
        };

        let data = match result {
            Ok(Some(data)) => data,
            Ok(None) => {
                info!(relay = %relay_label, "server closed connection cleanly");
                return ConnectionOutcome::CleanlyClosed;
            }
            Err(proto_blue_ws::WsError::ReconnectExhausted { attempts }) => {
                warn!(
                    target: "horizon_firehose::metrics",
                    event_type = "ws_reconnect_exhausted",
                    relay = %relay_label,
                    attempts,
                    "SDK exhausted reconnect budget on this relay; rotating to fallback"
                );
                return ConnectionOutcome::ReconnectExhausted { attempts };
            }
            Err(e) => {
                warn!(relay = %relay_label, error = %e, "ws recv error");
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
            r = frames_tx.send((data, relay_label.to_string())) => r,
        };
        if send_res.is_err() {
            return ConnectionOutcome::DownstreamGone;
        }
    }
}

/// Pick the separator that keeps an existing query string valid. If
/// the URL already has a `?`, we append with `&`; otherwise we start
/// the query with `?`. Assumes `cursor` isn't already in the URL —
/// the config validator rejects relay URLs that aren't clean.
fn url_separator(url: &str) -> char {
    if url.contains('?') { '&' } else { '?' }
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

    // ─── Phase 8.5 follow-up finding 2.3: backoff reset signal ─────

    #[test]
    fn reset_failures_signals_clean_since_last_backoff() {
        let mut s = SharedState::from_config(&relay_cfg("ws://p", &[]));
        assert!(
            !s.clean_since_last_backoff,
            "fresh state starts with flag false"
        );
        s.reset_failures(0);
        assert!(
            s.clean_since_last_backoff,
            "reset_failures must set the backoff-reset signal"
        );
    }

    #[test]
    fn take_clean_flag_is_one_shot() {
        let mut s = SharedState::from_config(&relay_cfg("ws://p", &[]));
        s.reset_failures(0);
        assert!(s.take_clean_flag(), "first take returns true");
        assert!(
            !s.take_clean_flag(),
            "second take returns false — flag is cleared by take"
        );
        assert!(!s.clean_since_last_backoff);
    }

    #[test]
    fn backoff_reset_scenario_fail_fail_succeed_fail() {
        // Simulates: fail → fail → (succeed long enough to reset) → fail.
        // After the clean operation, the next failure's backoff sleep
        // should be `initial_backoff`, not the compounded value.
        let mut s = SharedState::from_config(&relay_cfg("ws://p", &[]));
        let now = Instant::now();

        // Fail x2.
        s.record_failure(0, now, 5, Duration::from_secs(10));
        s.record_failure(0, now, 5, Duration::from_secs(10));
        assert_eq!(s.relays[0].consecutive_failures, 2);

        // Clean operation happens; reset_failures sets the flag.
        s.reset_failures(0);
        assert_eq!(s.relays[0].consecutive_failures, 0);
        assert!(s.clean_since_last_backoff);

        // Supervisor picks up the flag, resets its local backoff.
        let reset = s.take_clean_flag();
        assert!(reset);

        // Next failure.
        s.record_failure(0, now, 5, Duration::from_secs(10));
        assert_eq!(s.relays[0].consecutive_failures, 1);
        // The supervisor's backoff would now grow from
        // `initial_backoff`, not from the pre-reset compounded value.
        // Observable in the structured log line `backoff_ms` —
        // unit-test level we just verify the flag contract.
        assert!(
            !s.clean_since_last_backoff,
            "flag was consumed by take_clean_flag; failure doesn't re-set it"
        );
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
            // Long enough that no test trips it accidentally — the
            // SDK bounds silence internally via `per_recv_timeout_ms`
            // now (Phase 8.6), so we want this well above any test's
            // run duration.
            per_recv_timeout: Duration::from_secs(10),
            // Phase 8.6: cap reconnects per connection. Test mocks
            // either accept immediately or close, so we shouldn't
            // hit this; small cap to fail fast if something goes
            // sideways.
            max_reconnect_attempts: 3,
            // Tests run against plaintext ws:// mocks — no rustls
            // involvement.
            tls_config: None,
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
                    let Ok(mut ws) = accept_async(stream).await else {
                        return;
                    };
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
        assert_eq!(f1.0, b"hello");
        assert_eq!(f1.1, server.url);
        assert_eq!(f2.0, b"world");
        assert_eq!(f2.1, server.url);

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
        assert_eq!(frame.0, b"after-reconnect");
        assert_eq!(frame.1, server.url);

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
        assert_eq!(frame.0, b"from-fallback");
        assert_eq!(frame.1, fallback.url);

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
        assert_eq!(frame.0, b"primary-recovered");
        assert_eq!(frame.1, primary.url);

        // We should have ended up back on primary.
        assert_eq!(
            reader.metrics().active_relay.as_deref(),
            Some(primary.url.as_str())
        );

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

    // ─── Phase 8.6: SDK `ReconnectExhausted` → force-cooldown + failover ──

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnect_exhausted_forces_cooldown_and_rotates_to_fallback() {
        // Phase 8.6: the SDK's `ReconnectExhausted` fires after N
        // consecutive failed *connect* attempts (successful connects
        // reset the counter). To trigger it we need the primary to
        // accept the initial connection then become unreachable.
        //
        // Strategy: spawn primary, let the supervisor connect, then
        // shut the primary's listener down. The established
        // connection dies (or silently stalls; `per_recv_timeout`
        // surfaces the stall), the SDK tries to reconnect, fails
        // `max_reconnect_attempts` times against the now-dead port,
        // returns `ReconnectExhausted`. Our supervisor pushes the
        // primary into cooldown and rotates to the fallback.
        let primary = start_mock(vec![Behavior::SendThenHold(vec![])]).await;
        let fallback = start_mock(vec![Behavior::SendThenHold(vec![
            b"from-fallback".to_vec(),
        ])])
        .await;

        let mut cfg = relay_cfg(&primary.url, &[fallback.url.as_str()]);
        cfg.reconnect_initial_delay_ms = 10;
        cfg.reconnect_max_delay_ms = 100;

        let primary_url = primary.url.clone();
        let fallback_url = fallback.url.clone();

        // Very small per_recv_timeout + small max_reconnect_attempts
        // so the test completes in < 5 s once the primary dies.
        let opts = WsReaderOptions {
            per_recv_timeout: Duration::from_millis(100),
            max_reconnect_attempts: 2,
            ..test_options()
        };
        let mut reader = spawn(cfg, opts);

        // Give the supervisor a moment to hit the primary's TCP
        // accept, then shut the listener down so subsequent reconnect
        // attempts fail with connection-refused (`reconnects`
        // accumulates toward the cap).
        tokio::time::sleep(Duration::from_millis(250)).await;
        primary.shutdown().await;

        let frame = tokio::time::timeout(Duration::from_secs(10), reader.recv())
            .await
            .expect("never got a frame from the fallback")
            .expect("channel closed");
        assert_eq!(frame.0, b"from-fallback");
        assert_eq!(frame.1, fallback_url);

        // Primary must be in cooldown — the Phase 8.6 `force_cooldown`
        // branch runs on any `ReconnectExhausted` outcome.
        let states = reader.relay_states();
        assert_eq!(states[0].url, primary_url);
        assert!(
            states[0].cooldown_until.is_some(),
            "primary should be in cooldown after ReconnectExhausted — got {states:?}"
        );
        assert_eq!(states[1].url, fallback_url);

        reader.shutdown().await;
        fallback.shutdown().await;
    }
}

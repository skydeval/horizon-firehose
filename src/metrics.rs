//! Phase 6: structured metrics.
//!
//! A single [`Metrics`] struct holds atomic monotonic counters that
//! every task increments on the hot path. A 10-second emitter task
//! samples those counters, diffs against the previous sample, and
//! emits a `periodic_metrics` tracing event matching the DESIGN.md §4
//! schema. Gauges (channel depths, cursor ages, oldest stream entry
//! age) are sampled at emission time from their live sources —
//! [`mpsc::WeakSender`] for channels, [`crate::cursor::Cursors`] for
//! cursor ages, and [`crate::backend::StreamBackend::oldest_event_timestamp`]
//! for the stream's oldest entry.
//!
//! # Why monotonic totals + delta at emission, not swap-reset
//!
//! Two common shapes for "X-in-window" counters:
//!
//! 1. `AtomicU64::swap(0, …)` to atomically read-and-reset. One
//!    counter per metric, but the shutdown_metrics event has to be
//!    computed separately (you'd need a second cumulative counter
//!    anyway) — so the "one counter" saving is illusory.
//!
//! 2. Monotonic total + emitter-side delta. One counter per metric,
//!    the emitter holds a `last_sample` snapshot and subtracts. The
//!    shutdown path reads the totals directly without any bookkeeping.
//!
//! Phase 6 uses (2). Fewer atomic writes on the hot path (no extra
//! "total" store per event) and one canonical source for shutdown
//! reporting. The emitter's local state is the cost.
//!
//! # Shutdown model
//!
//! The emitter observes the global shutdown watch directly — it is
//! not in the pipeline cascade (nothing downstream cares if it's
//! running) so there's no channel-close signal to rely on. On
//! shutdown it simply exits; [`crate::main`] emits the final
//! `shutdown_metrics` event from the cumulative totals after the
//! pipeline cascade has drained.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::backend::StreamBackend;
use crate::cursor::Cursors;
use crate::ws_reader::WsStateReader;

/// Counters and a small reconnect-history ring buffer shared across
/// the pipeline. Every field is lock-free except the ring buffer,
/// which is contended only by the ws_reader task (single writer).
#[derive(Debug, Default)]
pub struct Metrics {
    /// Total successful XADDs since startup. DESIGN.md §4
    /// `total_events_published`.
    pub events_total: AtomicU64,
    /// Total JSON bytes XADDed to the stream.
    pub bytes_total: AtomicU64,
    /// Total CBOR/CAR decode failures. DESIGN.md §4
    /// `total_decode_errors` on shutdown, `decode_errors_in_window`
    /// per period.
    pub decode_errors_total: AtomicU64,
    /// Total Redis XADD/SET failures observed by the publisher's
    /// retry loop. DESIGN.md §4 `total_redis_errors`.
    pub redis_errors_total: AtomicU64,
    /// Total records with a `$type` we couldn't classify. Phase 6
    /// exposes the metric but leaves the counter at 0 — actual
    /// classification wants a lexicon registry and lands in a later
    /// phase.
    pub unknown_type_count_total: AtomicU64,
    /// Total events exceeding `max_event_size_bytes`.
    pub oversize_events_total: AtomicU64,
    /// Total firehose frames the decoder explicitly *skipped* (e.g.
    /// `#sync`). Phase 3 finding: these still advance the cursor but
    /// produce no Redis entry; distinct from decode errors.
    pub skipped_frames_total: AtomicU64,
    /// Total firehose frames carrying a type name we don't know.
    /// Phase 3 finding: kept separate from `decode_errors_total` so
    /// operators can alert on "ATProto added a frame type" without
    /// alerting on every corrupted CBOR frame.
    pub unknown_frame_types_total: AtomicU64,

    /// Total reconnects observed by the ws_reader supervisor. Also
    /// reported on the supervisor's own structured log lines, but
    /// we mirror here so the metrics emitter has a single source of
    /// truth.
    pub reconnects_total: AtomicU64,

    /// Phase 8.5 review finding 3.5: counts panics the decoder task
    /// caught via `std::panic::catch_unwind`. Stack overflows are
    /// not catchable (SIGABRT), so this counts everything *else*
    /// that went wrong during decode: arithmetic panics, slice
    /// out-of-bounds, unexpected unwraps in dependencies.
    pub decoder_panics_total: AtomicU64,

    /// Phase 8.5 review finding 3.4: times the decoder's consecutive
    /// error threshold was reached and it asked the supervisor for a
    /// failover. Proxy for "relay is sending garbage" — alert on any
    /// sustained increase.
    pub decoder_circuit_opens_total: AtomicU64,

    /// Rolling reconnect history (for the `reconnects_last_hour`
    /// gauge) owned by the metrics struct. Written by ws_reader on
    /// each reconnect, read by the emitter on each tick. Capped at
    /// `RECONNECT_RING_CAP` to bound memory under pathological
    /// reconnect loops.
    pub reconnect_history: Mutex<std::collections::VecDeque<Instant>>,
}

const RECONNECT_RING_CAP: usize = 10_000;
const RECONNECT_HOUR: Duration = Duration::from_secs(3600);

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn record_reconnect(&self, now: Instant) {
        self.reconnects_total.fetch_add(1, Ordering::Relaxed);
        let mut h = self.reconnect_history.lock().await;
        h.push_back(now);
        if let Some(cutoff) = now.checked_sub(RECONNECT_HOUR) {
            while matches!(h.front(), Some(&t) if t < cutoff) {
                h.pop_front();
            }
        }
        while h.len() > RECONNECT_RING_CAP {
            h.pop_front();
        }
    }

    pub async fn reconnects_last_hour(&self, now: Instant) -> u64 {
        let mut h = self.reconnect_history.lock().await;
        if let Some(cutoff) = now.checked_sub(RECONNECT_HOUR) {
            while matches!(h.front(), Some(&t) if t < cutoff) {
                h.pop_front();
            }
        }
        h.len() as u64
    }

    fn load_snapshot(&self) -> CumulativeSnapshot {
        CumulativeSnapshot {
            events: self.events_total.load(Ordering::Relaxed),
            bytes: self.bytes_total.load(Ordering::Relaxed),
            decode_errors: self.decode_errors_total.load(Ordering::Relaxed),
            redis_errors: self.redis_errors_total.load(Ordering::Relaxed),
            unknown_type_count: self.unknown_type_count_total.load(Ordering::Relaxed),
            oversize_events: self.oversize_events_total.load(Ordering::Relaxed),
            skipped_frames: self.skipped_frames_total.load(Ordering::Relaxed),
            unknown_frame_types: self.unknown_frame_types_total.load(Ordering::Relaxed),
            reconnects: self.reconnects_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct CumulativeSnapshot {
    events: u64,
    bytes: u64,
    decode_errors: u64,
    redis_errors: u64,
    unknown_type_count: u64,
    oversize_events: u64,
    skipped_frames: u64,
    unknown_frame_types: u64,
    reconnects: u64,
}

impl CumulativeSnapshot {
    fn delta(&self, prev: &Self) -> Self {
        Self {
            events: self.events.saturating_sub(prev.events),
            bytes: self.bytes.saturating_sub(prev.bytes),
            decode_errors: self.decode_errors.saturating_sub(prev.decode_errors),
            redis_errors: self.redis_errors.saturating_sub(prev.redis_errors),
            unknown_type_count: self
                .unknown_type_count
                .saturating_sub(prev.unknown_type_count),
            oversize_events: self.oversize_events.saturating_sub(prev.oversize_events),
            skipped_frames: self.skipped_frames.saturating_sub(prev.skipped_frames),
            unknown_frame_types: self
                .unknown_frame_types
                .saturating_sub(prev.unknown_frame_types),
            reconnects: self.reconnects.saturating_sub(prev.reconnects),
        }
    }
}

/// WeakSender references for every bounded channel between tasks. The
/// emitter upgrades each one briefly to read capacity, then drops the
/// temporary Sender — so the metrics task does not keep channels open
/// after the producer has gone. Using `WeakSender` avoids a subtle
/// shutdown bug: if the emitter held a strong `Sender` clone, channel
/// close would only fire once the emitter exited, defeating the
/// cascade ordering in DESIGN.md §3.
#[derive(Clone)]
pub struct ChannelGauges {
    pub ws_to_decoder: mpsc::WeakSender<crate::ws_reader::Frame>,
    pub decoder_to_router: mpsc::WeakSender<crate::publisher::PublishOp>,
    pub router_to_publisher: mpsc::WeakSender<crate::publisher::PublishOp>,
}

impl ChannelGauges {
    /// Sample depth (messages buffered) for each channel. Returns
    /// `None` for a channel whose producer has already dropped its
    /// sender — at which point "depth" is meaningless: the channel
    /// is being drained, not written to.
    pub fn sample(&self) -> ChannelDepths {
        ChannelDepths {
            ws_to_decoder: depth(&self.ws_to_decoder),
            decoder_to_router: depth(&self.decoder_to_router),
            router_to_publisher: depth(&self.router_to_publisher),
        }
    }
}

fn depth<T>(w: &mpsc::WeakSender<T>) -> Option<u64> {
    w.upgrade()
        .map(|s| (s.max_capacity() - s.capacity()) as u64)
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct ChannelDepths {
    pub ws_to_decoder: Option<u64>,
    pub decoder_to_router: Option<u64>,
    pub router_to_publisher: Option<u64>,
}

/// Handle to the spawned metrics emitter task.
pub struct EmitterHandle {
    pub(crate) task: JoinHandle<()>,
}

impl EmitterHandle {
    pub async fn join(self) {
        let _ = self.task.await;
    }
}

/// Spawn the periodic metrics emitter.
///
/// Every `interval` tick: loads the cumulative counter snapshot, diffs
/// against the previous tick, samples the channel and cursor gauges,
/// queries the backend for the oldest stream entry age, and emits a
/// single `periodic_metrics` tracing event.
///
/// Exits on `shutdown_rx.changed()` — the emitter is *not* in the
/// pipeline cascade, so it does not wait for any channel to close.
#[allow(clippy::too_many_arguments)]
pub fn spawn_emitter<B: StreamBackend>(
    metrics: Arc<Metrics>,
    channels: ChannelGauges,
    cursors: Cursors,
    ws_state: WsStateReader,
    backend: Arc<B>,
    stream_key: String,
    started_at: Instant,
    interval: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) -> EmitterHandle {
    let task = crate::spawn_instrumented("metrics_emitter", async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately; skip it so we don't emit
        // a "just started, everything's zero" event on top of
        // startup_metrics.
        ticker.tick().await;

        let mut prev = metrics.load_snapshot();
        let mut ticks: u64 = 0;

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    debug!(ticks, "metrics emitter shutting down");
                    break;
                }
                _ = ticker.tick() => {
                    let now = Instant::now();
                    let current = metrics.load_snapshot();
                    let delta = current.delta(&prev);

                    let depths = channels.sample();
                    let cursor_ages = cursors.age_seconds(now).await;
                    let ws_snapshot = ws_state.snapshot(now).await;
                    // Phase 8.5 follow-up finding 4.6: distinguish
                    // three states for `oldest_event_age_seconds`:
                    //   (a) Redis reachable, stream has entries →
                    //       real age in seconds.
                    //   (b) Redis reachable, stream is empty →
                    //       `oldest_event_age_seconds = null`,
                    //       `redis_healthy = true`.
                    //   (c) Redis unreachable / XRANGE errored →
                    //       `oldest_event_age_seconds = null`,
                    //       `redis_healthy = false`.
                    // The old code reported `0` in all three cases,
                    // so alerts on "oldest_event_age_seconds > 300"
                    // couldn't fire during Redis outages. Operators
                    // should alert on `redis_healthy = false`
                    // instead for that class of incident.
                    let (oldest_age, redis_healthy) =
                        match backend.oldest_event_timestamp(&stream_key).await {
                            Ok(Some(ts)) => {
                                let age =
                                    (chrono::Utc::now() - ts).num_seconds().max(0) as u64;
                                (Some(age), true)
                            }
                            Ok(None) => (None, true),
                            Err(_) => (None, false),
                        };
                    let oldest_json = serde_json::to_string(&oldest_age).unwrap_or_default();

                    info!(
                        target: "horizon_firehose::metrics",
                        event_type = "periodic_metrics",
                        window_seconds = interval.as_secs(),
                        events_in_window = delta.events,
                        events_per_sec_in_window = (delta.events as f64)
                            / interval.as_secs_f64(),
                        bytes_in_window = delta.bytes,
                        decode_errors_in_window = delta.decode_errors,
                        redis_errors_in_window = delta.redis_errors,
                        relay_reconnects_in_window = delta.reconnects,
                        total_reconnects_since_start = current.reconnects,
                        reconnects_last_hour = ws_snapshot.reconnects_last_hour,
                        unknown_type_count_in_window = delta.unknown_type_count,
                        oversize_events_in_window = delta.oversize_events,
                        skipped_frames_in_window = delta.skipped_frames,
                        unknown_frame_types_in_window = delta.unknown_frame_types,
                        active_relay = ws_snapshot.active_relay.as_deref().unwrap_or(""),
                        channel_depths = %serde_json::to_string(&depths).unwrap_or_default(),
                        cursor_ages_seconds = %serde_json::to_string(&cursor_ages).unwrap_or_default(),
                        oldest_event_age_seconds = %oldest_json,
                        redis_healthy = redis_healthy,
                        uptime_seconds = started_at.elapsed().as_secs(),
                        "periodic_metrics"
                    );

                    prev = current;
                    ticks += 1;
                }
            }
        }
    });
    EmitterHandle { task }
}

/// Cumulative totals for the `shutdown_metrics` event. `main` builds
/// this at the end of the shutdown cascade from the same counters the
/// emitter samples.
#[derive(Debug, Clone, Copy)]
pub struct ShutdownTotals {
    pub total_events_published: u64,
    pub total_decode_errors: u64,
    pub total_redis_errors: u64,
    pub total_reconnects: u64,
    pub total_skipped_frames: u64,
    pub total_oversize_events: u64,
    pub total_unknown_frame_types: u64,
}

impl Metrics {
    pub fn shutdown_totals(&self) -> ShutdownTotals {
        let s = self.load_snapshot();
        ShutdownTotals {
            total_events_published: s.events,
            total_decode_errors: s.decode_errors,
            total_redis_errors: s.redis_errors,
            total_reconnects: s.reconnects,
            total_skipped_frames: s.skipped_frames,
            total_oversize_events: s.oversize_events,
            total_unknown_frame_types: s.unknown_frame_types,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn counters_start_at_zero() {
        let m = Metrics::default();
        let s = m.load_snapshot();
        assert_eq!(s.events, 0);
        assert_eq!(s.reconnects, 0);
    }

    #[test]
    fn counters_increment_and_snapshot_is_consistent() {
        let m = Metrics::default();
        m.events_total.fetch_add(42, Ordering::Relaxed);
        m.bytes_total.fetch_add(1000, Ordering::Relaxed);
        m.decode_errors_total.fetch_add(3, Ordering::Relaxed);
        let s = m.load_snapshot();
        assert_eq!(s.events, 42);
        assert_eq!(s.bytes, 1000);
        assert_eq!(s.decode_errors, 3);
    }

    #[test]
    fn delta_computes_per_window_from_two_snapshots() {
        let prev = CumulativeSnapshot {
            events: 100,
            bytes: 10_000,
            ..Default::default()
        };
        let curr = CumulativeSnapshot {
            events: 250,
            bytes: 20_000,
            ..prev
        };
        let d = curr.delta(&prev);
        assert_eq!(d.events, 150);
        assert_eq!(d.bytes, 10_000);
    }

    #[test]
    fn delta_never_underflows_on_out_of_order_samples() {
        // Theoretically impossible (totals are monotonic) but the
        // saturating_sub guards against any atomic-memory-model
        // surprise so the emitter can't produce a billion-sized
        // spurious delta.
        let prev = CumulativeSnapshot {
            events: 100,
            ..Default::default()
        };
        let curr = CumulativeSnapshot::default(); // all 0
        let d = curr.delta(&prev);
        assert_eq!(d.events, 0);
    }

    #[tokio::test]
    async fn reconnect_history_trims_past_one_hour() {
        let m = Metrics::default();
        // Record a reconnect "two hours ago" by reaching past the API.
        let old = Instant::now() - Duration::from_secs(7200);
        {
            let mut h = m.reconnect_history.lock().await;
            h.push_back(old);
        }
        assert_eq!(m.reconnect_history.lock().await.len(), 1);

        let now = Instant::now();
        m.record_reconnect(now).await;
        // Record triggers trim; the old entry should be gone.
        let hour = m.reconnects_last_hour(now).await;
        assert_eq!(hour, 1, "only the recent reconnect should remain");
    }

    #[tokio::test]
    async fn reconnect_history_ring_cap_bounds_memory() {
        let m = Metrics::default();
        let base = Instant::now();
        for i in 0..(RECONNECT_RING_CAP + 500) {
            // Clamp to recent window so time-trimming doesn't
            // intervene; we want to see the len cap kick in.
            m.record_reconnect(base + Duration::from_millis(i as u64))
                .await;
        }
        assert_eq!(m.reconnect_history.lock().await.len(), RECONNECT_RING_CAP);
    }
}

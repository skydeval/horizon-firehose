//! Phase 4: Redis stream publisher.
//!
//! Takes filtered `(Event, seq)` pairs from the router, serializes each
//! to JSON, XADDs to Redis with MAXLEN trimming, and advances the
//! cursor tracker on confirmed success.
//!
//! # Cursor-advance invariant (DESIGN.md §3)
//!
//! The cursor is **only** advanced after a confirmed XADD. A crash or
//! panic between "decoded an event" and "Redis acknowledged the XADD"
//! must not advance the cursor — otherwise restart would skip an event
//! that never made it downstream. This is the load-bearing line in
//! Phase 4 and the reason for the retry loop below.
//!
//! # Retry policy (DESIGN.md §3 "Redis disconnect")
//!
//! On Redis error:
//! - Exponential backoff: start at `retry_initial`, double each attempt,
//!   cap at `retry_max` (30 s per §3).
//! - Never drop the event, never advance the cursor. Events pile up in
//!   the input channel, backpressuring the decoder → ws_reader →
//!   eventually the relay (slow-reader disconnect), which is the
//!   correct global behaviour.
//! - A WARN is emitted every `retry_warn_interval` with total downtime
//!   so operators can alert without being spammed per-retry.
//! - Shutdown (`shutdown_rx` flip) breaks out of the retry loop
//!   immediately and the task exits with the current stats. The
//!   outstanding event is lost from the publisher's POV but the cursor
//!   is still correct — it reflects only XADDed events.
//!
//! # Oversize policy (DESIGN.md §3 "Oversized events")
//!
//! If the serialised JSON exceeds `max_event_size_bytes`:
//! - `skip_with_log`: WARN with repo+seq+size, increment counter, drop.
//!   Cursor does **advance** past the event — we know what seq it was,
//!   the operator has decided this event isn't worth delivering, and
//!   *not* advancing would leave us permanently stuck on replay.
//! - `fail_hard`: ERROR, stop the task, return an error from the run
//!   loop. The main-task coordinator propagates it and exits non-zero.
//!
//! # Replay guard (2026-10-22)
//!
//! Before anything else, an event whose seq is at or below its
//! relay's published cursor is dropped and counted rather than
//! XADDed; a long run of such drops is handled as a relay sequence
//! reset per `on_stale_cursor`. See [`ReplayGuard`].

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::backend::{BackendError, StreamBackend};
use crate::config::{OversizePolicy, StaleCursorPolicy};
use crate::cursor::{Cursors, cursor_key};
use crate::decoder::PolicyExit;
use crate::event::Event;
use crate::metrics::Metrics;

/// In-band pipeline message — the single item type on every channel
/// between decoder / router / publisher. Phase 8.5 review finding 1.1:
/// previously the decoder advanced the shared in-memory cursor
/// directly whenever a `#sync` frame arrived. That moved the cursor
/// past buffered commits still in flight, violating the DESIGN.md §3
/// "cursor = last XADDed seq" invariant and silently losing events
/// on hard crashes or graceful-shutdown-with-Redis-down. The fix:
/// route skip-advances as an in-band [`PublishOp::Skip`] through the
/// same channel that carries events. The publisher — already the
/// XADD-success-advances-cursor writer — becomes the *sole* cursor
/// writer, so advance ordering matches XADD ordering by construction.
#[derive(Debug)]
pub enum PublishOp {
    /// Publish `event` to Redis, advance the cursor on XADD success.
    Publish(Event, u64),
    /// Advance the cursor without publishing anything. Used for
    /// frames the decoder classified as `Skipped` (e.g. `#sync`) —
    /// they advance the firehose position but have no Redis
    /// representation.
    Skip { relay: String, seq: u64 },
}

/// Where the replay guard sends a policy exit: the same coordinator
/// channel the decoder uses for `on_stale_cursor = "exit"`, plus the
/// global shutdown watch.
#[derive(Debug, Clone)]
pub struct PolicyExitSink {
    pub policy_exit_tx: mpsc::UnboundedSender<PolicyExit>,
    pub shutdown_tx: watch::Sender<bool>,
}

#[derive(Debug, Clone)]
pub struct PublisherOptions {
    pub stream_key: String,
    pub max_stream_len: u64,
    pub max_event_size_bytes: u64,
    pub on_oversize: OversizePolicy,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    /// How often to re-log a warning while Redis is down.
    pub retry_warn_interval: Duration,
    /// Replay guard: how long a run of seq ≤ cursor drops may last,
    /// with no event advancing that relay's cursor, before the relay
    /// is treated as having reset its sequence. Config:
    /// `cursor.relay_reset_after_seconds`.
    pub relay_reset_after: Duration,
    /// Replay guard: what to do on a suspected relay sequence reset.
    /// Config: `cursor.on_stale_cursor`.
    pub on_stale_cursor: StaleCursorPolicy,
    /// Replay guard: where an `Exit` goes. `None` (unit tests) logs
    /// the reset and keeps dropping.
    pub policy_exit: Option<PolicyExitSink>,
}

impl PublisherOptions {
    /// Default retry knobs per DESIGN.md §3 "Redis disconnect".
    pub fn with_defaults(
        stream_key: impl Into<String>,
        max_stream_len: u64,
        max_event_size_bytes: u64,
        on_oversize: OversizePolicy,
    ) -> Self {
        Self {
            stream_key: stream_key.into(),
            max_stream_len,
            max_event_size_bytes,
            on_oversize,
            retry_initial: Duration::from_millis(500),
            retry_max: Duration::from_secs(30),
            retry_warn_interval: Duration::from_secs(30),
            relay_reset_after: Duration::from_secs(600),
            on_stale_cursor: StaleCursorPolicy::LiveTip,
            policy_exit: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum PublisherError {
    #[error(
        "oversize event with seq={seq}, size={size} bytes exceeded max {max} — policy fail_hard"
    )]
    OversizeFailHard { seq: u64, size: u64, max: u64 },

    #[error("event serialization failed for seq={seq}: {source}")]
    Serialize {
        seq: u64,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PublisherStats {
    pub events_in: u64,
    pub events_published: u64,
    pub events_oversize_skipped: u64,
    pub redis_errors: u64,
    pub total_retry_wait: Duration,
    /// Phase 8.5 review finding 1.1: `PublishOp::Skip` ops processed.
    /// These advanced the cursor without XADDing anything — e.g.
    /// `#sync` frames the decoder forwarded.
    pub skip_advances: u64,
    /// 2026-10-22 replay guard: events not XADDed because their seq
    /// was at or below the cursor already published for their relay.
    pub replayed_dropped: u64,
    /// Phase 8.5 follow-up finding 4.10: true when `join`/`shutdown`
    /// surfaced a `JoinError` (the task panicked). In that case the
    /// numeric fields above are `Default` — the actual pre-panic
    /// counts are lost with the task's local state. Main's outer
    /// coordinator still detects the panic via the handle's
    /// `JoinHandle` in its `select!`, so fault reporting + non-zero
    /// exit still work; this flag is for `shutdown_metrics`
    /// accuracy so operators don't misread "zero events published"
    /// as a clean but silent session.
    pub task_panicked: bool,
}

/// Handle to the spawned publisher task.
pub struct PublisherHandle {
    pub(crate) task: JoinHandle<Result<PublisherStats, PublisherError>>,
    shutdown_tx: watch::Sender<bool>,
}

impl PublisherHandle {
    /// Wait for the publisher task to exit on its own, **without**
    /// signalling shutdown. Use this on the clean-exit path: the
    /// caller has (or will) drop the input sender, the task will
    /// drain any pending events, any mid-flight retries will resolve
    /// via eventual Redis recovery, and the task exits naturally when
    /// `input.recv()` returns `None`.
    pub async fn join(self) -> Result<PublisherStats, PublisherError> {
        // Keep `shutdown_tx` alive through the await. Dropping it
        // would cause `watch::Receiver::changed()` inside the retry
        // loop to return `Err(_)` — which `tokio::select!` treats as
        // a ready branch and exits the retry prematurely, leaking
        // an unpublished event just like an explicit shutdown would.
        let _keepalive = self.shutdown_tx;
        match self.task.await {
            Ok(r) => r,
            Err(_join_err) => Ok(PublisherStats {
                task_panicked: true,
                ..PublisherStats::default()
            }),
        }
    }

    /// Signal shutdown (preempting any retry loop) and wait for the
    /// task to exit. Use this when the caller wants an immediate
    /// bounded-time exit even if Redis is currently unreachable — the
    /// retry loop will break mid-sleep and the task will return its
    /// current stats. Any event that was in flight when shutdown fired
    /// has **not** been published, and the cursor is correspondingly
    /// not advanced past it (DESIGN.md §3 invariant).
    pub async fn shutdown(self) -> Result<PublisherStats, PublisherError> {
        let _ = self.shutdown_tx.send(true);
        match self.task.await {
            Ok(r) => r,
            Err(_join_err) => Ok(PublisherStats {
                task_panicked: true,
                ..PublisherStats::default()
            }),
        }
    }

    /// Phase 8.5 follow-up finding 2.2: await the task up to
    /// `budget`, then force-shutdown via the signal. Used by the
    /// main coordinator's shutdown cascade to guarantee the final
    /// cursor flush has time to run within the outer
    /// `SHUTDOWN_BUDGET`. If the budget elapses, the retry loop
    /// observes the shutdown watch, exits, and the task completes;
    /// the returned stats reflect events actually published before
    /// the forced exit.
    pub async fn join_with_budget(
        self,
        budget: Duration,
    ) -> Result<PublisherStats, PublisherError> {
        let Self {
            mut task,
            shutdown_tx,
        } = self;
        tokio::select! {
            r = &mut task => {
                // Completed on its own before the budget fired.
                // Keep the sender alive; the retry loop's select
                // treats `changed()` on a dropped sender as a ready
                // branch and would exit prematurely otherwise.
                let _keepalive = shutdown_tx;
                match r {
                    Ok(res) => res,
                    Err(_join_err) => Ok(PublisherStats {
                        task_panicked: true,
                        ..PublisherStats::default()
                    }),
                }
            }
            _ = tokio::time::sleep(budget) => {
                // Budget elapsed — force the retry loop out.
                let _ = shutdown_tx.send(true);
                match task.await {
                    Ok(res) => res,
                    Err(_join_err) => Ok(PublisherStats {
                        task_panicked: true,
                        ..PublisherStats::default()
                    }),
                }
            }
        }
    }
}

pub fn spawn<B: StreamBackend>(
    backend: Arc<B>,
    cursors: Cursors,
    opts: PublisherOptions,
    input: mpsc::Receiver<PublishOp>,
    metrics: Arc<Metrics>,
) -> PublisherHandle {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = crate::spawn_instrumented(
        "publisher",
        run(backend, cursors, opts, input, shutdown_rx, metrics),
    );
    PublisherHandle { task, shutdown_tx }
}

pub async fn run<B: StreamBackend>(
    backend: Arc<B>,
    cursors: Cursors,
    opts: PublisherOptions,
    mut input: mpsc::Receiver<PublishOp>,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> Result<PublisherStats, PublisherError> {
    let mut stats = PublisherStats::default();
    let mut guard = ReplayGuard::default();

    loop {
        // Drain-first policy: `biased;` with `input` checked before
        // the shutdown signal means pending events are published
        // before we honour an explicit shutdown. This matches the
        // DESIGN.md §3 ordering — "Publisher task: drain its input
        // channel, XADD all remaining events to Redis" — and is how
        // tests can rely on `drop(tx); handle.shutdown()` publishing
        // every queued event before exiting. Mid-retry shutdown is a
        // separate concern, handled inside `xadd_with_retry` below.
        let op = tokio::select! {
            biased;
            recv = input.recv() => match recv {
                Some(op) => op,
                None => {
                    debug!(?stats, "publisher: input channel closed");
                    break;
                }
            },
            _ = shutdown_rx.changed() => {
                debug!(?stats, "publisher: shutdown signalled with empty channel");
                break;
            }
        };
        let (event, seq) = match op {
            PublishOp::Publish(ev, seq) => (ev, seq),
            // Skip-advance in-band: process in channel order, so a
            // commit queued before a `#sync` is guaranteed to have
            // been XADDed (or skipped with oversize) before we touch
            // the cursor for the sync. The publisher is now the sole
            // writer to the in-memory cursor.
            PublishOp::Skip { relay, seq } => {
                let before = cursors.get(&relay).await;
                cursors.advance(&relay, seq).await;
                if before.is_none_or(|c| seq > c) {
                    // A forward skip is progress: any replay-drop run
                    // on this relay is over.
                    guard.end_run(&relay);
                }
                stats.skip_advances += 1;
                continue;
            }
        };
        stats.events_in += 1;

        let relay = event.relay().to_string();

        // 2026-10-22 replay guard: never XADD a seq this relay has
        // already published. See `ReplayGuard`.
        match guard.check(&cursors, &relay, seq, &opts, &metrics).await {
            GuardVerdict::Publish => {}
            GuardVerdict::Drop => {
                stats.replayed_dropped += 1;
                continue;
            }
        }

        // Serialise once: we need both the size check and the bytes we
        // eventually hand to XADD.
        let event_type = event.type_name();
        let data = match event.to_json_bytes() {
            Ok(bytes) => bytes,
            Err(source) => {
                // Serialisation *shouldn't* ever fail — our JSON types
                // are all well-formed — but don't panic the task.
                error!(seq, error = %source, "event serialization failed; dropping");
                return Err(PublisherError::Serialize { seq, source });
            }
        };

        // Oversize check (DESIGN.md §3 "Oversized events").
        let size = data.len() as u64;
        if size > opts.max_event_size_bytes {
            match opts.on_oversize {
                OversizePolicy::SkipWithLog => {
                    stats.events_oversize_skipped += 1;
                    metrics
                        .oversize_events_total
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target: "horizon_firehose::metrics",
                        event_type = "oversize_event_skipped",
                        repo = debug_repo(&event),
                        seq,
                        size,
                        max = opts.max_event_size_bytes,
                        "dropping oversize event per on_oversize=skip_with_log"
                    );
                    // Advance the cursor past this skipped event.
                    //
                    // If we *didn't* advance, the same frame would
                    // replay forever: relay re-sends on reconnect, we
                    // drop for oversize again, cursor never moves,
                    // repeat. Advancing means the oversize event is
                    // permanently lost (by design — it exceeded our
                    // size cap and operator chose `skip_with_log`)
                    // but downstream processing continues past it.
                    cursors.advance(&relay, seq).await;
                    continue;
                }
                OversizePolicy::FailHard => {
                    metrics
                        .oversize_events_total
                        .fetch_add(1, Ordering::Relaxed);
                    error!(
                        repo = debug_repo(&event),
                        seq,
                        size,
                        max = opts.max_event_size_bytes,
                        "oversize event; on_oversize=fail_hard"
                    );
                    return Err(PublisherError::OversizeFailHard {
                        seq,
                        size,
                        max: opts.max_event_size_bytes,
                    });
                }
            }
        }

        // XADD with indefinite retry. Breaks on shutdown; otherwise
        // loops forever. On success: advance the cursor, fall through
        // to the next event.
        let xadd_result = xadd_with_retry(
            backend.as_ref(),
            &opts,
            &mut stats,
            event_type,
            &data,
            seq,
            &mut shutdown_rx,
            &metrics,
        )
        .await;

        match xadd_result {
            XaddOutcome::Ok => {
                cursors.advance(&relay, seq).await;
                stats.events_published += 1;
                metrics.events_total.fetch_add(1, Ordering::Relaxed);
                metrics.bytes_total.fetch_add(size, Ordering::Relaxed);
            }
            XaddOutcome::ShuttingDown => {
                debug!(?stats, "publisher: shutdown during retry");
                break;
            }
        }
    }

    Ok(stats)
}

/// What the replay guard decided for one event.
#[derive(Debug, PartialEq, Eq)]
enum GuardVerdict {
    Publish,
    Drop,
}

/// One unbroken run of replay drops on a relay.
#[derive(Debug)]
struct DropRun {
    started: Instant,
    first_seq: u64,
    last_seq: u64,
    count: u64,
    cursor: u64,
    /// The reset policy already fired for this run (Exit, or Exit
    /// with no sink). Prevents re-firing on every later drop.
    reset_handled: bool,
}

/// 2026-10-22 replay guard (defence in depth behind the per-dial
/// cursor fix in `ws_reader::connect_url_fn`).
///
/// # Rule
///
/// An event whose `seq` is **at or below** the cursor already
/// published for its relay is not XADDed. The cursor is "last
/// successfully XADDed (or skip-advanced) seq", so such an event is
/// one this relay already delivered downstream. Equal counts as
/// published. Seq order is per relay URL; the comparison is against
/// that relay's own cursor only. A relay with no cursor yet (fresh
/// start, live tip) is never guarded.
///
/// Drops are counted in `replayed_events_dropped_total` and logged
/// once per burst: a WARN when a run of drops starts and an INFO
/// summary when the next event advances the cursor.
///
/// Where legitimate drops come from: after a reconnect, frames from
/// the old connection still sitting in the pipeline channels (up to
/// ~3 k) are published first, then the new connection re-sends the
/// same seqs from the cursor it was given. Those duplicates are
/// dropped here instead of re-XADDed.
///
/// The publisher is the sole cursor writer, so the read-compare here
/// is race-free.
///
/// # Relay sequence reset
///
/// A relay that restarts its numbering (a rebuilt relay instance)
/// sends live events with seqs below our cursor. Dropping those
/// forever would be a silent outage. So if a drop run lasts
/// `relay_reset_after` (config `cursor.relay_reset_after_seconds`,
/// default 600 s) with no event advancing the cursor, the guard
/// treats it as a reset, counts `relay_seq_resets_total`, and applies
/// `on_stale_cursor` — the same knob as a relay rejecting our cursor:
///
/// - `exit`: log ERROR with the remediation, send a policy exit to the
///   coordinator and flip global shutdown — the same path as the
///   decoder's `OutdatedCursor` exit. The process exits non-zero after
///   the normal drain and cursor flush. A restart resumes from the
///   saved cursor; if the relay really did reset, the drops resume
///   and the process exits again every `relay_reset_after`, a visible
///   crash loop until the operator deletes the cursor key.
/// - `live_tip`: log WARN, move the relay's cursor back to just below
///   the current seq ([`Cursors::rebase`]) and publish from there on.
///   Events dropped during the window are lost, matching what
///   `live_tip` already means for a rejected cursor.
///
/// A false positive needs ≥ `relay_reset_after` of drops with no
/// advance, i.e. a relay that ignores `?cursor=` and replays more
/// than that much history. `exit` then restarts the process (which
/// re-dials with the saved cursor); `live_tip` re-publishes the rest
/// of that replay as duplicates. Both are recoverable; dropping
/// forever is not.
#[derive(Debug, Default)]
struct ReplayGuard {
    runs: std::collections::HashMap<String, DropRun>,
}

impl ReplayGuard {
    async fn check(
        &mut self,
        cursors: &Cursors,
        relay: &str,
        seq: u64,
        opts: &PublisherOptions,
        metrics: &Metrics,
    ) -> GuardVerdict {
        let cursor = match cursors.get(relay).await {
            Some(c) if seq <= c => c,
            _ => {
                self.end_run(relay);
                return GuardVerdict::Publish;
            }
        };

        metrics
            .replayed_events_dropped_total
            .fetch_add(1, Ordering::Relaxed);
        let run = self.runs.entry(relay.to_string()).or_insert_with(|| {
            warn!(
                target: "horizon_firehose::metrics",
                event_type = "replay_drop_started",
                relay = %relay,
                first_dropped_seq = seq,
                cursor,
                "relay sent events at or below the published cursor; dropping them \
                 (not XADDed) until it passes the cursor"
            );
            DropRun {
                started: Instant::now(),
                first_seq: seq,
                last_seq: seq,
                count: 0,
                cursor,
                reset_handled: false,
            }
        });
        run.last_seq = seq;
        run.count += 1;

        if run.reset_handled || run.started.elapsed() < opts.relay_reset_after {
            return GuardVerdict::Drop;
        }

        // Drops have run for `relay_reset_after` with no advance:
        // treat as a relay sequence reset.
        metrics
            .relay_seq_resets_total
            .fetch_add(1, Ordering::Relaxed);
        let key = cursor_key(relay);
        match opts.on_stale_cursor {
            StaleCursorPolicy::LiveTip => {
                warn!(
                    target: "horizon_firehose::metrics",
                    event_type = "relay_seq_reset",
                    relay = %relay,
                    policy = "on_stale_cursor=live_tip",
                    seq,
                    cursor = run.cursor,
                    dropped = run.count,
                    first_dropped_seq = run.first_seq,
                    drop_secs = run.started.elapsed().as_secs(),
                    "relay sequence reset suspected; accepting the relay's new numbering \
                     (cursor moves back; events dropped during the window are lost)"
                );
                self.runs.remove(relay);
                cursors.rebase(relay, seq.saturating_sub(1)).await;
                GuardVerdict::Publish
            }
            StaleCursorPolicy::Exit => {
                run.reset_handled = true;
                let reason = format!(
                    "on_stale_cursor=exit after relay sequence reset suspected on {relay}: \
                     {} events at or below cursor {} over {}s (seq {}..={}). If the relay \
                     really reset, delete the cursor key to resume from its live tip: \
                     redis-cli -u \"$REDIS_URL\" DEL {key}",
                    run.count,
                    run.cursor,
                    run.started.elapsed().as_secs(),
                    run.first_seq,
                    run.last_seq,
                );
                error!(
                    target: "horizon_firehose::metrics",
                    event_type = "policy_exit",
                    relay = %relay,
                    policy = "on_stale_cursor=exit",
                    reason = %reason,
                    "relay sequence reset suspected; initiating shutdown"
                );
                if let Some(sink) = &opts.policy_exit {
                    let _ = sink.policy_exit_tx.send(PolicyExit {
                        task: "publisher",
                        reason,
                    });
                    let _ = sink.shutdown_tx.send(true);
                }
                // Keep draining (and dropping) rather than breaking:
                // the shutdown cascade closes our input, and the
                // coordinator turns the policy exit into a non-zero
                // exit after the final cursor flush.
                GuardVerdict::Drop
            }
        }
    }

    /// The relay advanced its cursor: close any drop run with a
    /// one-line summary.
    fn end_run(&mut self, relay: &str) {
        if let Some(run) = self.runs.remove(relay) {
            info!(
                target: "horizon_firehose::metrics",
                event_type = "replay_drop_ended",
                relay = %relay,
                dropped = run.count,
                first_dropped_seq = run.first_seq,
                last_dropped_seq = run.last_seq,
                cursor = run.cursor,
                drop_secs = run.started.elapsed().as_secs_f64(),
                "relay passed the published cursor; replay drops ended"
            );
        }
    }
}

enum XaddOutcome {
    Ok,
    ShuttingDown,
}

#[allow(clippy::too_many_arguments)]
async fn xadd_with_retry<B: StreamBackend>(
    backend: &B,
    opts: &PublisherOptions,
    stats: &mut PublisherStats,
    event_type: &str,
    data: &[u8],
    seq: u64,
    shutdown_rx: &mut watch::Receiver<bool>,
    metrics: &Metrics,
) -> XaddOutcome {
    let mut backoff = opts.retry_initial;
    let mut outage_start: Option<Instant> = None;
    let mut last_warn: Option<Instant> = None;

    loop {
        match backend
            .xadd(&opts.stream_key, opts.max_stream_len, event_type, data, seq)
            .await
        {
            Ok(_id) => return XaddOutcome::Ok,
            Err(err) => {
                stats.redis_errors += 1;
                metrics.redis_errors_total.fetch_add(1, Ordering::Relaxed);
                let now = Instant::now();
                outage_start.get_or_insert(now);

                let downtime = outage_start
                    .map(|s| now.duration_since(s))
                    .unwrap_or_default();
                let should_warn = match last_warn {
                    None => true,
                    Some(t) => now.duration_since(t) >= opts.retry_warn_interval,
                };
                if should_warn {
                    log_redis_outage(&err, seq, downtime, backoff);
                    last_warn = Some(now);
                }
            }
        }

        // Sleep with shutdown responsiveness. `biased;` with shutdown
        // first (the mirror image of the outer drain-first select) so
        // an operator-initiated shutdown during a Redis outage exits
        // immediately instead of waiting out the current backoff.
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => return XaddOutcome::ShuttingDown,
            _ = tokio::time::sleep(backoff) => {}
        }

        stats.total_retry_wait = stats.total_retry_wait.saturating_add(backoff);
        backoff = backoff.saturating_mul(2).min(opts.retry_max);
    }
}

fn log_redis_outage(err: &BackendError, seq: u64, downtime: Duration, next_backoff: Duration) {
    warn!(
        target: "horizon_firehose::metrics",
        event_type = "redis_outage",
        seq,
        downtime_secs = downtime.as_secs_f64(),
        next_backoff_ms = next_backoff.as_millis() as u64,
        error = %err,
        "Redis XADD failed; retrying indefinitely (cursor will not advance until success)"
    );
}

fn debug_repo(ev: &Event) -> String {
    match ev {
        Event::Commit(c) => c.repo.clone(),
        Event::Identity(e) => e.did.clone(),
        Event::Account(e) => e.did.clone(),
        Event::Handle(e) => e.did.clone(),
        Event::Tombstone(e) => e.did.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use tokio::time::timeout;

    use crate::backend::{FailMode, InMemoryBackend};
    use crate::event::{CommitEvent, IdentityEvent, Operation};

    const RELAY: &str = "wss://relay.test/xrpc/com.atproto.sync.subscribeRepos";

    fn tiny_commit(seq_hint: u64) -> Event {
        Event::Commit(CommitEvent {
            repo: format!("did:plc:{seq_hint}"),
            commit: "bafycommit".into(),
            rev: "3k2la".into(),
            since: None,
            ops: vec![Operation {
                action: "create".into(),
                path: "app.bsky.feed.post/abc".into(),
                cid: Some("bafyrecord".into()),
                record: Some(json!({"$type": "app.bsky.feed.post", "text": "hi"})),
            }],
            time: "2026-04-19T00:00:00.000Z".into(),
            relay: RELAY.into(),
        })
    }

    fn opts(max_len: u64, max_bytes: u64, policy: OversizePolicy) -> PublisherOptions {
        PublisherOptions {
            stream_key: "firehose:events".into(),
            max_stream_len: max_len,
            max_event_size_bytes: max_bytes,
            on_oversize: policy,
            // Tight retry so tests don't wait seconds.
            retry_initial: Duration::from_millis(10),
            retry_max: Duration::from_millis(40),
            retry_warn_interval: Duration::from_millis(50),
            relay_reset_after: Duration::from_secs(600),
            on_stale_cursor: StaleCursorPolicy::LiveTip,
            policy_exit: None,
        }
    }

    fn m() -> Arc<Metrics> {
        Arc::new(Metrics::default())
    }

    #[tokio::test]
    async fn publishes_event_and_advances_cursor_on_success() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(1000, 1 << 20, OversizePolicy::SkipWithLog),
            rx,
            m(),
        );

        tx.send(PublishOp::Publish(tiny_commit(1), 100))
            .await
            .unwrap();
        tx.send(PublishOp::Publish(tiny_commit(2), 101))
            .await
            .unwrap();
        drop(tx);

        let stats = timeout(Duration::from_secs(2), handle.join())
            .await
            .expect("join timed out")
            .expect("ok stats");
        assert_eq!(stats.events_published, 2);
        assert_eq!(stats.events_in, 2);
        assert_eq!(c.get(RELAY).await, Some(101));
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn maxlen_trimming_keeps_stream_bounded() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        let (tx, rx) = mpsc::channel(64);
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(3, 1 << 20, OversizePolicy::SkipWithLog),
            rx,
            m(),
        );

        for i in 0..20u64 {
            tx.send(PublishOp::Publish(tiny_commit(i), 1000 + i))
                .await
                .unwrap();
        }
        drop(tx);

        let stats = handle.join().await.unwrap();
        assert_eq!(stats.events_published, 20);
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 3);
        // Cursor always reflects the latest successfully-published seq.
        assert_eq!(c.get(RELAY).await, Some(1019));
    }

    #[tokio::test]
    async fn oversize_skip_with_log_drops_and_still_advances_cursor() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        let (tx, rx) = mpsc::channel(4);
        // Tiny max so every event is oversize.
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(1000, 10, OversizePolicy::SkipWithLog),
            rx,
            m(),
        );

        tx.send(PublishOp::Publish(tiny_commit(1), 50))
            .await
            .unwrap();
        tx.send(PublishOp::Publish(tiny_commit(2), 51))
            .await
            .unwrap();
        drop(tx);

        let stats = handle.join().await.unwrap();
        assert_eq!(stats.events_oversize_skipped, 2);
        assert_eq!(stats.events_published, 0);
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 0);
        // Cursor still advances past the skipped seqs so replay doesn't
        // loop forever.
        assert_eq!(c.get(RELAY).await, Some(51));
    }

    #[tokio::test]
    async fn oversize_fail_hard_returns_error() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        let (tx, rx) = mpsc::channel(4);
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(1000, 10, OversizePolicy::FailHard),
            rx,
            m(),
        );
        tx.send(PublishOp::Publish(tiny_commit(1), 50))
            .await
            .unwrap();
        // drop(tx) unnecessary — task returns Err before reading more.

        let result = timeout(Duration::from_secs(2), handle.join())
            .await
            .expect("join timed out");
        match result {
            Err(PublisherError::OversizeFailHard { seq, .. }) => assert_eq!(seq, 50),
            other => panic!("expected OversizeFailHard, got {other:?}"),
        }
        // No XADD occurred, no cursor advance.
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 0);
        assert_eq!(c.get(RELAY).await, None);
    }

    #[tokio::test]
    async fn retries_transient_redis_errors_then_succeeds() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        // Fail the first 3 XADDs, then succeed.
        b.set_fail_mode(FailMode::FailNext(3)).await;

        let (tx, rx) = mpsc::channel(4);
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(1000, 1 << 20, OversizePolicy::SkipWithLog),
            rx,
            m(),
        );
        tx.send(PublishOp::Publish(tiny_commit(1), 77))
            .await
            .unwrap();
        drop(tx);

        let stats = timeout(Duration::from_secs(5), handle.join())
            .await
            .expect("join timed out")
            .unwrap();
        assert_eq!(stats.events_published, 1);
        assert_eq!(stats.redis_errors, 3);
        assert!(stats.total_retry_wait > Duration::from_millis(0));
        assert_eq!(c.get(RELAY).await, Some(77));
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn cursor_never_advances_on_redis_failure_without_eventual_success() {
        // Publisher is trying indefinitely; we shut it down mid-retry.
        // Cursor must remain None.
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        // 1000 failures in the queue — way more than our test window.
        b.set_fail_mode(FailMode::FailNext(1000)).await;

        let (tx, rx) = mpsc::channel(4);
        let handle = spawn(
            b.clone(),
            c.clone(),
            // Make retry backoff tight so we accumulate errors quickly.
            PublisherOptions {
                retry_initial: Duration::from_millis(5),
                retry_max: Duration::from_millis(5),
                ..opts(1000, 1 << 20, OversizePolicy::SkipWithLog)
            },
            rx,
            m(),
        );
        tx.send(PublishOp::Publish(tiny_commit(1), 9999))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Shut down while the publisher is still retrying.
        let stats = handle.shutdown().await.unwrap();
        assert_eq!(stats.events_published, 0);
        assert!(stats.redis_errors > 0);
        // This is the load-bearing assertion of Phase 4: cursor
        // never advances past an event that didn't make it to Redis.
        assert_eq!(c.get(RELAY).await, None);
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn identity_events_publish_and_advance_per_relay() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        let (tx, rx) = mpsc::channel(4);
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(1000, 1 << 20, OversizePolicy::SkipWithLog),
            rx,
            m(),
        );

        let id_ev = Event::Identity(IdentityEvent {
            did: "did:plc:xyz".into(),
            handle: Some("alice.test".into()),
            relay: RELAY.into(),
        });
        tx.send(PublishOp::Publish(id_ev, 42)).await.unwrap();
        drop(tx);

        handle.join().await.unwrap();
        assert_eq!(c.get(RELAY).await, Some(42));
        let entries = b.stream_entries("firehose:events").await;
        assert_eq!(entries.len(), 1);
        // Verify the stream entry's `type` field is the string
        // "identity" per DESIGN.md §4.
        let (_id, _ts, fields) = &entries[0];
        let type_field = fields
            .iter()
            .find(|(k, _)| k == "type")
            .map(|(_, v)| v.clone())
            .unwrap();
        assert_eq!(type_field, b"identity");
    }

    // ─── Phase 8.5: finding 1.1 — SkipAdvance timing invariant ────

    #[tokio::test]
    async fn join_with_budget_forces_shutdown_on_elapse() {
        // Phase 8.5 follow-up finding 2.2: when the publisher is
        // stuck in the retry loop on an unreachable Redis, the
        // inner budget must fire and force the task to exit —
        // otherwise the outer `SHUTDOWN_BUDGET` would consume all
        // 30s here and starve the final cursor flush.
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        b.set_fail_mode(FailMode::FailNext(10_000)).await;
        let (tx, rx) = mpsc::channel::<PublishOp>(4);
        let handle = spawn(
            b.clone(),
            c.clone(),
            PublisherOptions {
                retry_initial: Duration::from_millis(5),
                retry_max: Duration::from_millis(20),
                ..opts(1000, 1 << 20, OversizePolicy::SkipWithLog)
            },
            rx,
            m(),
        );
        tx.send(PublishOp::Publish(tiny_commit(1), 42))
            .await
            .unwrap();

        // 150ms budget is far shorter than the retry loop's infinite
        // patience. Must return within that window.
        let started = std::time::Instant::now();
        let stats = timeout(
            Duration::from_millis(500),
            handle.join_with_budget(Duration::from_millis(150)),
        )
        .await
        .expect("join_with_budget must respect its own budget")
        .unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(400),
            "join_with_budget should have returned near its 150ms budget, not {elapsed:?}"
        );
        assert_eq!(stats.events_published, 0);
        // Cursor must NOT have advanced — the invariant holds even
        // under budget-triggered force-exit.
        assert_eq!(c.get(RELAY).await, None);
    }

    #[tokio::test]
    async fn task_panicked_flag_is_set_when_task_panics() {
        // Phase 8.5 follow-up finding 4.10: a panicking publisher
        // task should surface as `stats.task_panicked = true`
        // rather than zero stats silently replacing the real ones.
        // We can't ergonomically force the publisher to panic from
        // outside — simulate by synthesising a PublisherHandle
        // whose task IS a panicking spawn.
        let (shutdown_tx, _rx) = tokio::sync::watch::channel(false);
        let task: JoinHandle<Result<PublisherStats, PublisherError>> = tokio::spawn(async {
            panic!("synthetic publisher panic");
        });
        let handle = PublisherHandle { task, shutdown_tx };
        let stats = handle.join().await.unwrap();
        assert!(
            stats.task_panicked,
            "join must report task_panicked=true for panicked task"
        );
        assert_eq!(stats.events_published, 0);
    }

    #[tokio::test]
    async fn skip_op_advances_cursor_without_xadd() {
        // Phase 8.5 finding 1.1: `PublishOp::Skip` advances the
        // cursor the same way a published event would, but writes
        // nothing to the stream.
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        let (tx, rx) = mpsc::channel(4);
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(1000, 1 << 20, OversizePolicy::SkipWithLog),
            rx,
            m(),
        );

        tx.send(PublishOp::Skip {
            relay: RELAY.to_string(),
            seq: 77,
        })
        .await
        .unwrap();
        drop(tx);

        let stats = handle.join().await.unwrap();
        assert_eq!(stats.events_published, 0);
        assert_eq!(stats.skip_advances, 1);
        assert_eq!(c.get(RELAY).await, Some(77));
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 0);
    }

    /// The load-bearing test for Phase 8.5 review finding 1.1. The
    /// DESIGN.md §3 invariant — "cursor = last successfully XADDed
    /// seq" — held only if the publisher is the sole cursor writer.
    /// We verify that invariant in the presence of interleaved
    /// commit / skip / commit frames AND a blocked backend: the
    /// cursor must never move past a seq whose predecessor is still
    /// pending XADD.
    #[tokio::test]
    async fn skip_never_advances_cursor_past_pending_commits() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        // Block every XADD. The publisher will enter its retry
        // loop on the first commit and never drain the channel.
        b.set_fail_mode(FailMode::FailNext(10_000)).await;

        let (tx, rx) = mpsc::channel::<PublishOp>(16);
        let handle = spawn(
            b.clone(),
            c.clone(),
            PublisherOptions {
                retry_initial: Duration::from_millis(5),
                retry_max: Duration::from_millis(20),
                ..opts(1000, 1 << 20, OversizePolicy::SkipWithLog)
            },
            rx,
            m(),
        );

        // Queue commit@100, skip@101, commit@102.
        tx.send(PublishOp::Publish(tiny_commit(1), 100))
            .await
            .unwrap();
        tx.send(PublishOp::Skip {
            relay: RELAY.to_string(),
            seq: 101,
        })
        .await
        .unwrap();
        tx.send(PublishOp::Publish(tiny_commit(2), 102))
            .await
            .unwrap();

        // Give the publisher time to pick up commit@100 and start
        // retrying.
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Cursor must still be None. The pre-fix bug was: decoder
        // advanced to 101 on skip even though commit@100 hadn't
        // been XADDed yet. Now the skip is behind @100 in the
        // channel and the publisher processes in order.
        assert_eq!(
            c.get(RELAY).await,
            None,
            "cursor advanced past pending commit — \
             Phase 8.5 review finding 1.1 regression"
        );

        // Force shutdown; the task exits with commit@100 unpublished.
        let stats = handle.shutdown().await.unwrap();
        assert_eq!(stats.events_published, 0);
        assert_eq!(
            stats.skip_advances, 0,
            "publisher must not have processed the skip while blocked on 100"
        );
        assert_eq!(c.get(RELAY).await, None);
    }

    #[tokio::test]
    async fn skip_advances_cursor_in_channel_order_after_commit_succeeds() {
        // Mirror of the test above, but with a healthy backend.
        // commit@100 → skip@101 → commit@102 all process; the final
        // cursor must be 102, and intermediate observations must
        // show the cursor advancing monotonically through 100, 101,
        // 102 (i.e. the skip is processed between the commits, not
        // before).
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        let (tx, rx) = mpsc::channel::<PublishOp>(16);
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(1000, 1 << 20, OversizePolicy::SkipWithLog),
            rx,
            m(),
        );

        tx.send(PublishOp::Publish(tiny_commit(1), 100))
            .await
            .unwrap();
        tx.send(PublishOp::Skip {
            relay: RELAY.to_string(),
            seq: 101,
        })
        .await
        .unwrap();
        tx.send(PublishOp::Publish(tiny_commit(2), 102))
            .await
            .unwrap();
        drop(tx);

        let stats = handle.join().await.unwrap();
        assert_eq!(stats.events_published, 2);
        assert_eq!(stats.skip_advances, 1);
        assert_eq!(c.get(RELAY).await, Some(102));
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 2);
    }

    // ─── 2026-10-22 replay guard ──────────────────────────────────────

    fn commit_on(relay: &str, n: u64) -> Event {
        let Event::Commit(mut c) = tiny_commit(n) else {
            unreachable!()
        };
        c.relay = relay.into();
        Event::Commit(c)
    }

    async fn send_seqs(tx: &mpsc::Sender<PublishOp>, seqs: &[u64]) {
        for &s in seqs {
            tx.send(PublishOp::Publish(tiny_commit(s), s))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn events_at_or_below_saved_cursor_are_dropped_and_counted() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        // As if loaded from Redis at startup, or published earlier.
        c.advance(RELAY, 100).await;
        let metrics = m();
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(1000, 1 << 20, OversizePolicy::SkipWithLog),
            rx,
            metrics.clone(),
        );

        // 98, 99: replayed history. 100: equal = already published.
        // 101, 102: new.
        send_seqs(&tx, &[98, 99, 100, 101, 102]).await;
        drop(tx);

        let stats = handle.join().await.unwrap();
        assert_eq!(stats.events_in, 5);
        assert_eq!(stats.replayed_dropped, 3);
        assert_eq!(stats.events_published, 2);
        assert_eq!(
            metrics
                .replayed_events_dropped_total
                .load(Ordering::Relaxed),
            3
        );
        assert_eq!(metrics.relay_seq_resets_total.load(Ordering::Relaxed), 0);
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 2);
        let seqs: Vec<_> = b
            .stream_entries("firehose:events")
            .await
            .into_iter()
            .map(|(_, _, fields)| {
                fields
                    .into_iter()
                    .find(|(k, _)| k == "seq")
                    .map(|(_, v)| String::from_utf8(v).unwrap())
                    .unwrap()
            })
            .collect();
        assert_eq!(seqs, vec!["101", "102"]);
        assert_eq!(c.get(RELAY).await, Some(102));
    }

    #[tokio::test]
    async fn replay_guard_is_per_relay() {
        // A high cursor on one relay must not drop another relay's
        // (independently numbered) events.
        const OTHER: &str = "wss://other.test/xrpc/com.atproto.sync.subscribeRepos";
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        c.advance(RELAY, 1_000_000).await;
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn(
            b.clone(),
            c.clone(),
            opts(1000, 1 << 20, OversizePolicy::SkipWithLog),
            rx,
            m(),
        );
        tx.send(PublishOp::Publish(commit_on(OTHER, 1), 5))
            .await
            .unwrap();
        tx.send(PublishOp::Publish(commit_on(RELAY, 2), 5))
            .await
            .unwrap();
        drop(tx);

        let stats = handle.join().await.unwrap();
        assert_eq!(stats.events_published, 1);
        assert_eq!(stats.replayed_dropped, 1);
        assert_eq!(c.get(OTHER).await, Some(5));
        assert_eq!(c.get(RELAY).await, Some(1_000_000));
    }

    #[tokio::test]
    async fn short_drop_runs_separated_by_progress_never_count_as_reset() {
        // Two separate overlap bursts, each well under the threshold,
        // with time passing between them: the clock restarts when the
        // relay advances, so neither is a reset.
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        c.advance(RELAY, 100).await;
        let metrics = m();
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn(
            b.clone(),
            c.clone(),
            PublisherOptions {
                relay_reset_after: Duration::from_millis(100),
                on_stale_cursor: StaleCursorPolicy::Exit,
                ..opts(1000, 1 << 20, OversizePolicy::SkipWithLog)
            },
            rx,
            metrics.clone(),
        );
        send_seqs(&tx, &[50]).await;
        tokio::time::sleep(Duration::from_millis(70)).await;
        send_seqs(&tx, &[101]).await;
        tokio::time::sleep(Duration::from_millis(70)).await;
        send_seqs(&tx, &[60, 102]).await;
        drop(tx);

        let stats = handle.join().await.unwrap();
        assert_eq!(stats.replayed_dropped, 2);
        assert_eq!(stats.events_published, 2);
        assert_eq!(metrics.relay_seq_resets_total.load(Ordering::Relaxed), 0);
        assert_eq!(c.get(RELAY).await, Some(102));
    }

    #[tokio::test]
    async fn relay_reset_under_live_tip_rebases_cursor_and_resumes_publishing() {
        // A relay that restarted its numbering at 1 while our cursor
        // is 1_000_000. After `relay_reset_after` of drops with no
        // advance, live_tip accepts the new numbering.
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        c.advance(RELAY, 1_000_000).await;
        let metrics = m();
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn(
            b.clone(),
            c.clone(),
            PublisherOptions {
                relay_reset_after: Duration::from_millis(50),
                on_stale_cursor: StaleCursorPolicy::LiveTip,
                ..opts(1000, 1 << 20, OversizePolicy::SkipWithLog)
            },
            rx,
            metrics.clone(),
        );
        send_seqs(&tx, &[1, 2]).await;
        tokio::time::sleep(Duration::from_millis(80)).await;
        send_seqs(&tx, &[3, 4]).await;
        drop(tx);

        let stats = handle.join().await.unwrap();
        assert_eq!(stats.replayed_dropped, 2, "1 and 2 fell inside the window");
        assert_eq!(
            stats.events_published, 2,
            "3 triggers the rebase, 4 follows"
        );
        assert_eq!(metrics.relay_seq_resets_total.load(Ordering::Relaxed), 1);
        assert_eq!(c.get(RELAY).await, Some(4));
    }

    #[tokio::test]
    async fn relay_reset_under_exit_raises_one_policy_exit_and_keeps_dropping() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        c.advance(RELAY, 1_000_000).await;
        let metrics = m();
        let (exit_tx, mut exit_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (tx, rx) = mpsc::channel(16);
        let handle = spawn(
            b.clone(),
            c.clone(),
            PublisherOptions {
                relay_reset_after: Duration::from_millis(50),
                on_stale_cursor: StaleCursorPolicy::Exit,
                policy_exit: Some(PolicyExitSink {
                    policy_exit_tx: exit_tx,
                    shutdown_tx,
                }),
                ..opts(1000, 1 << 20, OversizePolicy::SkipWithLog)
            },
            rx,
            metrics.clone(),
        );
        send_seqs(&tx, &[1]).await;
        tokio::time::sleep(Duration::from_millis(80)).await;
        send_seqs(&tx, &[2, 3]).await;
        drop(tx);

        let stats = handle.join().await.unwrap();
        // Nothing below the cursor is ever published under `exit`,
        // and the cursor never moves back.
        assert_eq!(stats.events_published, 0);
        assert_eq!(stats.replayed_dropped, 3);
        assert_eq!(b.xlen("firehose:events").await.unwrap(), 0);
        assert_eq!(c.get(RELAY).await, Some(1_000_000));
        assert_eq!(metrics.relay_seq_resets_total.load(Ordering::Relaxed), 1);

        let exit = exit_rx.try_recv().expect("policy exit raised");
        assert_eq!(exit.task, "publisher");
        assert!(
            exit.reason.contains("on_stale_cursor=exit"),
            "{}",
            exit.reason
        );
        assert!(
            exit.reason.contains("DEL firehose:cursor:"),
            "{}",
            exit.reason
        );
        assert!(exit_rx.try_recv().is_err(), "exit must fire once per run");
        assert!(*shutdown_rx.borrow(), "global shutdown flipped");
    }
}

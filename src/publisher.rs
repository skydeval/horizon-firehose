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

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use crate::backend::{BackendError, StreamBackend};
use crate::config::OversizePolicy;
use crate::cursor::Cursors;
use crate::event::Event;
use crate::metrics::Metrics;

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
        }
    }
}

#[derive(Debug, Error)]
pub enum PublisherError {
    #[error("oversize event with seq={seq}, size={size} bytes exceeded max {max} — policy fail_hard")]
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
            Err(_join_err) => Ok(PublisherStats::default()),
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
            Err(_join_err) => Ok(PublisherStats::default()),
        }
    }
}

pub fn spawn<B: StreamBackend>(
    backend: Arc<B>,
    cursors: Cursors,
    opts: PublisherOptions,
    input: mpsc::Receiver<(Event, u64)>,
    metrics: Arc<Metrics>,
) -> PublisherHandle {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run(backend, cursors, opts, input, shutdown_rx, metrics));
    PublisherHandle { task, shutdown_tx }
}

pub async fn run<B: StreamBackend>(
    backend: Arc<B>,
    cursors: Cursors,
    opts: PublisherOptions,
    mut input: mpsc::Receiver<(Event, u64)>,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> Result<PublisherStats, PublisherError> {
    let mut stats = PublisherStats::default();

    loop {
        // Drain-first policy: `biased;` with `input` checked before
        // the shutdown signal means pending events are published
        // before we honour an explicit shutdown. This matches the
        // DESIGN.md §3 ordering — "Publisher task: drain its input
        // channel, XADD all remaining events to Redis" — and is how
        // tests can rely on `drop(tx); handle.shutdown()` publishing
        // every queued event before exiting. Mid-retry shutdown is a
        // separate concern, handled inside `xadd_with_retry` below.
        let (event, seq) = tokio::select! {
            biased;
            recv = input.recv() => match recv {
                Some(pair) => pair,
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
        stats.events_in += 1;

        // Serialise once: we need both the size check and the bytes we
        // eventually hand to XADD.
        let event_type = event.type_name();
        let relay = event.relay().to_string();
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
                    metrics.oversize_events_total.fetch_add(1, Ordering::Relaxed);
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
                    metrics.oversize_events_total.fetch_add(1, Ordering::Relaxed);
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

                let downtime = outage_start.map(|s| now.duration_since(s)).unwrap_or_default();
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

        tx.send((tiny_commit(1), 100)).await.unwrap();
        tx.send((tiny_commit(2), 101)).await.unwrap();
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
            tx.send((tiny_commit(i), 1000 + i)).await.unwrap();
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

        tx.send((tiny_commit(1), 50)).await.unwrap();
        tx.send((tiny_commit(2), 51)).await.unwrap();
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
        tx.send((tiny_commit(1), 50)).await.unwrap();
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
        tx.send((tiny_commit(1), 77)).await.unwrap();
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
        tx.send((tiny_commit(1), 9999)).await.unwrap();

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
        tx.send((id_ev, 42)).await.unwrap();
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
}

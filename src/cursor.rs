//! Phase 4: per-relay cursor tracker and Redis persister.
//!
//! DESIGN.md §3 spec:
//! - In-memory tracker maps `relay_url → last_successfully_xadded_seq`.
//! - `advance(relay, seq)` only advances *forward* — cursors never go
//!   backwards, so a stale event (reordering, failover, manual replay)
//!   can't rewind our published position.
//! - Persister wakes every `save_interval_seconds`, writes each cursor
//!   to `firehose:cursor:{base64url(relay_url)}`.
//! - On startup, callers `load_initial` to hydrate the in-memory map
//!   from any previously-persisted values.
//! - On shutdown, callers `persist_all` once more so the final cursor
//!   reflects every successful XADD (DESIGN.md §3 shutdown ordering).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use tokio::sync::RwLock;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::backend::{BackendError, StreamBackend};

/// Thread-safe per-relay cursor tracker. Cheaply cloneable — the
/// internal storage is an `Arc<RwLock>`, so every clone sees the same
/// state.
#[derive(Clone, Default, Debug)]
pub struct Cursors {
    inner: Arc<RwLock<HashMap<String, u64>>>,
}

impl Cursors {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the cursor for `relay` to `seq` if and only if `seq` is
    /// strictly greater than the current value. Called by the publisher
    /// on XADD success.
    ///
    /// Returns the new value (same as old if the advance was rejected).
    pub async fn advance(&self, relay: &str, seq: u64) -> u64 {
        let mut map = self.inner.write().await;
        let entry = map.entry(relay.to_string()).or_insert(0);
        if seq > *entry {
            *entry = seq;
        }
        *entry
    }

    /// Read the current cursor for `relay`. `None` if we've never seen
    /// (or loaded) a value for it.
    pub async fn get(&self, relay: &str) -> Option<u64> {
        self.inner.read().await.get(relay).copied()
    }

    /// Snapshot all known cursors as `(relay_url, seq)` pairs. Order is
    /// arbitrary (HashMap iteration).
    pub async fn snapshot(&self) -> Vec<(String, u64)> {
        self.inner
            .read()
            .await
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Load cursors from Redis for every configured relay. Missing keys
    /// (no prior cursor for that relay) leave the in-memory map empty
    /// for that relay, which the ws_reader interprets as "resume from
    /// live tip" per DESIGN.md §3.
    pub async fn load_initial<B: StreamBackend>(
        &self,
        backend: &B,
        relays: &[String],
    ) -> Result<(), BackendError> {
        for relay in relays {
            let key = cursor_key(relay);
            match backend.get_cursor(&key).await? {
                Some(seq) => {
                    info!(relay = %relay, seq, key = %key, "loaded cursor on startup");
                    self.inner.write().await.insert(relay.clone(), seq);
                }
                None => {
                    debug!(relay = %relay, key = %key, "no prior cursor; will resume from live tip");
                }
            }
        }
        Ok(())
    }

    /// Persist every known cursor to Redis. Used both by the periodic
    /// persister task and once more on shutdown.
    ///
    /// Errors on individual writes are logged and counted — we do not
    /// abort mid-flush, because a single transient failure shouldn't
    /// suppress the persistence of the other cursors in a multi-relay
    /// deployment.
    pub async fn persist_all<B: StreamBackend>(&self, backend: &B) -> PersistOutcome {
        let snap = self.snapshot().await;
        let mut outcome = PersistOutcome::default();
        for (relay, seq) in snap {
            let key = cursor_key(&relay);
            match backend.set_cursor(&key, seq).await {
                Ok(()) => outcome.written += 1,
                Err(err) => {
                    outcome.failed += 1;
                    warn!(
                        relay = %relay,
                        key = %key,
                        seq,
                        error = %err,
                        "cursor persist failed; will retry on next tick"
                    );
                }
            }
        }
        outcome
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PersistOutcome {
    pub written: u64,
    pub failed: u64,
}

/// Compute the Redis key a given relay's cursor is stored at. Using
/// base64url-no-pad keeps the key stable for unusual relay URLs (ports,
/// query strings, non-ASCII hosts) without collision risk.
pub fn cursor_key(relay_url: &str) -> String {
    format!(
        "firehose:cursor:{}",
        URL_SAFE_NO_PAD.encode(relay_url.as_bytes())
    )
}

/// Handle to a spawned persister task.
pub struct PersisterHandle {
    task: JoinHandle<PersisterStats>,
    shutdown_tx: watch::Sender<bool>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PersisterStats {
    pub ticks: u64,
    pub writes_successful: u64,
    pub writes_failed: u64,
}

impl PersisterHandle {
    /// Signal shutdown, await the task, return its cumulative stats.
    pub async fn shutdown(self) -> PersisterStats {
        let _ = self.shutdown_tx.send(true);
        self.task.await.unwrap_or_default()
    }
}

/// Spawn the periodic cursor persister. Wakes every `interval`, writes
/// each cursor to Redis, logs any failures.
///
/// Does *not* perform the final-on-shutdown flush — that's the caller's
/// job per DESIGN.md §3 shutdown ordering, because the publisher must
/// drain *first* so the final snapshot reflects every XADDed event.
pub fn spawn_persister<B: StreamBackend>(
    cursors: Cursors,
    backend: Arc<B>,
    interval: Duration,
) -> PersisterHandle {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut stats = PersisterStats::default();
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately; skip it so we don't hammer
        // Redis during startup before any XADD has succeeded.
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = ticker.tick() => {
                    let outcome = cursors.persist_all(backend.as_ref()).await;
                    stats.ticks += 1;
                    stats.writes_successful += outcome.written;
                    stats.writes_failed += outcome.failed;
                }
            }
        }

        stats
    });

    PersisterHandle { task, shutdown_tx }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{FailMode, InMemoryBackend};

    const R_A: &str = "wss://relay-a.test/xrpc/com.atproto.sync.subscribeRepos";
    const R_B: &str = "wss://relay-b.test/xrpc/com.atproto.sync.subscribeRepos";

    #[tokio::test]
    async fn cursor_key_matches_base64url_format() {
        let key = cursor_key("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos");
        assert!(key.starts_with("firehose:cursor:"));
        // URL-safe no-pad: no `=`, no `+`, no `/`.
        let suffix = &key["firehose:cursor:".len()..];
        assert!(!suffix.contains('='));
        assert!(!suffix.contains('+'));
        assert!(!suffix.contains('/'));
        // Round-trips.
        let decoded = URL_SAFE_NO_PAD.decode(suffix).unwrap();
        assert_eq!(
            decoded,
            b"wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
        );
    }

    #[tokio::test]
    async fn cursor_key_handles_nonstandard_port_and_query_string() {
        // DESIGN.md §5 "Cursor keys for relay URLs with non-standard
        // ports are correctly stored, retrieved, and cleaned up".
        let a = cursor_key("wss://example.com:9443/xrpc/com.atproto.sync.subscribeRepos");
        let b = cursor_key("wss://example.com/xrpc/com.atproto.sync.subscribeRepos?since=10");
        assert_ne!(a, b);
        for key in [&a, &b] {
            let suffix = &key["firehose:cursor:".len()..];
            URL_SAFE_NO_PAD.decode(suffix).expect("decodes cleanly");
        }
    }

    #[tokio::test]
    async fn advance_moves_forward_only() {
        let c = Cursors::new();
        assert_eq!(c.advance(R_A, 10).await, 10);
        assert_eq!(c.advance(R_A, 20).await, 20);
        // Stale sequence is ignored.
        assert_eq!(c.advance(R_A, 15).await, 20);
        assert_eq!(c.get(R_A).await, Some(20));
    }

    #[tokio::test]
    async fn advance_isolated_per_relay() {
        let c = Cursors::new();
        c.advance(R_A, 100).await;
        c.advance(R_B, 7).await;
        assert_eq!(c.get(R_A).await, Some(100));
        assert_eq!(c.get(R_B).await, Some(7));
    }

    #[tokio::test]
    async fn concurrent_advances_are_safe_and_monotonic() {
        use std::sync::Arc as StdArc;
        let c = StdArc::new(Cursors::new());
        let mut handles = Vec::new();
        // 8 tasks each advance by a unique non-overlapping range.
        for task in 0u64..8 {
            let c = c.clone();
            handles.push(tokio::spawn(async move {
                let base = task * 1000;
                for i in 0..100 {
                    c.advance(R_A, base + i).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Highest advance wins: task 7 base=7000, last i=99 → 7099.
        assert_eq!(c.get(R_A).await, Some(7099));
    }

    #[tokio::test]
    async fn load_initial_reads_from_backend() {
        let b = InMemoryBackend::new();
        b.seed_cursor(&cursor_key(R_A), 12345).await;
        // R_B is deliberately not seeded.
        let c = Cursors::new();
        c.load_initial(&b, &[R_A.to_string(), R_B.to_string()])
            .await
            .unwrap();
        assert_eq!(c.get(R_A).await, Some(12345));
        assert_eq!(c.get(R_B).await, None);
    }

    #[tokio::test]
    async fn persist_all_writes_each_cursor_at_correct_key() {
        let b = InMemoryBackend::new();
        let c = Cursors::new();
        c.advance(R_A, 111).await;
        c.advance(R_B, 222).await;
        let out = c.persist_all(&b).await;
        assert_eq!(out.written, 2);
        assert_eq!(out.failed, 0);

        let kv = b.kv_snapshot().await;
        assert_eq!(kv.get(&cursor_key(R_A)).unwrap(), b"111");
        assert_eq!(kv.get(&cursor_key(R_B)).unwrap(), b"222");
    }

    #[tokio::test]
    async fn persist_all_counts_failures_but_keeps_going() {
        let b = InMemoryBackend::new();
        let c = Cursors::new();
        c.advance(R_A, 111).await;
        c.advance(R_B, 222).await;
        // Fail the first write, succeed the second.
        b.set_fail_mode(FailMode::FailNext(1)).await;
        let out = c.persist_all(&b).await;
        assert_eq!(out.written, 1);
        assert_eq!(out.failed, 1);
    }

    #[tokio::test]
    async fn persister_ticks_and_shuts_down_cleanly() {
        let b = Arc::new(InMemoryBackend::new());
        let c = Cursors::new();
        c.advance(R_A, 42).await;

        let handle = spawn_persister(c.clone(), b.clone(), Duration::from_millis(20));
        // Let a couple of ticks fire.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let stats = handle.shutdown().await;
        assert!(stats.ticks >= 2, "expected ≥2 ticks, got {}", stats.ticks);
        // The cursor should be in Redis.
        assert_eq!(b.get_cursor(&cursor_key(R_A)).await.unwrap(), Some(42));
    }
}

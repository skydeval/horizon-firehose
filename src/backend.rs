//! Storage backend abstraction for the publisher and cursor modules.
//!
//! The real implementation ([`RedisBackend`]) wraps `redis::aio::ConnectionManager`.
//! The in-memory implementation ([`InMemoryBackend`]) is what the unit and
//! integration tests exercise — it reproduces the specific Redis behaviours
//! Phase 4 depends on (XADD with MAXLEN approximate trimming, per-key
//! cursor GET/SET, controllable failure injection) without requiring a
//! real server or Docker.
//!
//! # Fidelity of the in-memory fake
//!
//! The fake implements the behaviours we rely on from DESIGN.md §§3–4:
//!
//! - **XADD with `MAXLEN ~`**. The trim is approximate: we keep the
//!   stream at or below `max_len` entries in the fake by popping oldest
//!   entries once the stream exceeds the threshold. Real Redis
//!   approximation can leave the stream slightly over because it only
//!   trims whole radix-tree nodes; we do the strict version, which is a
//!   superset (anything the real backend trims, we also trim). Tests
//!   that check "stream length stays bounded" are safe. Tests that
//!   require *exact* approximation counts are not — there aren't any,
//!   but flagging it here so it doesn't surprise a future reader.
//!   TODO(phase-11-smoke): verify approximate trim against real Redis.
//!
//! - **XADD returns entry IDs**. Real Redis returns `<ms>-<seq>` IDs;
//!   the fake returns `<n>-0` where `n` is a monotonic counter. Our
//!   publisher never round-trips the ID (we only care about success/
//!   failure), so this difference is invisible. TODO(phase-11-smoke):
//!   confirm the real-server ID format if we ever persist it.
//!
//! - **Connection loss / retry**. The fake exposes [`InMemoryBackend::set_fail_mode`]
//!   so a test can force the next N operations to fail, then succeed,
//!   matching the shape of a transient Redis outage. Real Redis errors
//!   will look different on the wire (timeout vs. RESP error vs. TCP
//!   reset) but the publisher treats them uniformly as "retry with
//!   backoff", so the fake's synthetic errors cover the same code path.
//!
//! - **Cursor GET/SET**. Plain key/value SET and GET. The fake stores
//!   the value as a u64; real Redis stores the ASCII decimal string.
//!   The real backend parses on GET, which is the narrow path we need
//!   to verify. TODO(phase-11-smoke): confirm SET/GET round-trip on the
//!   real server.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use thiserror::Error;
use tokio::sync::Mutex;

/// Errors returned by [`StreamBackend`] implementations. One type for
/// every backend so the publisher can uniformly apply its retry policy.
#[derive(Debug, Error)]
pub enum BackendError {
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// Used by the fake to simulate transient failures without dragging
    /// a full `RedisError` constructor into test code.
    #[error("injected failure: {0}")]
    Injected(String),
}

/// The small surface area the publisher and cursor modules need. Kept
/// deliberately narrow — adding methods here should require a DESIGN.md
/// note, because anything we depend on must be faked faithfully.
///
/// The trait uses AFIT (`async fn` in trait) rather than the
/// `async_trait` crate. Callers are generic over `B: StreamBackend`
/// (not `dyn StreamBackend`), which sidesteps the dyn-safety limitation
/// and produces nicer error messages at the cost of per-impl monomorph.
pub trait StreamBackend: Send + Sync + 'static {
    /// XADD `stream_key` with MAXLEN-approximate trimming at `max_len`.
    /// Fields are `type` / `data` / `seq` per DESIGN.md §4 Redis stream
    /// format. Returns the assigned stream entry ID on success.
    fn xadd(
        &self,
        stream_key: &str,
        max_len: u64,
        event_type: &str,
        data: &[u8],
        seq: u64,
    ) -> impl std::future::Future<Output = Result<String, BackendError>> + Send;

    /// SET `key` = decimal string of `seq`.
    fn set_cursor(
        &self,
        key: &str,
        seq: u64,
    ) -> impl std::future::Future<Output = Result<(), BackendError>> + Send;

    /// GET `key`, parse as u64. Returns `Ok(None)` if the key doesn't
    /// exist.
    fn get_cursor(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = Result<Option<u64>, BackendError>> + Send;

    /// XLEN `stream_key`. Used in tests to assert trimming behaviour;
    /// not called on the hot path.
    fn xlen(
        &self,
        stream_key: &str,
    ) -> impl std::future::Future<Output = Result<u64, BackendError>> + Send;

    /// Timestamp of the oldest entry in `stream_key`, or `Ok(None)` if
    /// the stream is empty. Real Redis stream IDs are
    /// `"<milliseconds>-<seq>"`; we parse the milliseconds half.
    /// Used by the metrics emitter for the `oldest_event_age_seconds`
    /// gauge in DESIGN.md §4.
    fn oldest_event_timestamp(
        &self,
        stream_key: &str,
    ) -> impl std::future::Future<
        Output = Result<Option<chrono::DateTime<chrono::Utc>>, BackendError>,
    > + Send;
}

// ─── Real Redis backend ────────────────────────────────────────────────

/// Production backend. Uses `redis::aio::ConnectionManager` for
/// automatic reconnection; the publisher's retry loop handles the
/// window between a command failing and the manager re-establishing
/// the connection.
#[derive(Clone)]
pub struct RedisBackend {
    conn: Arc<Mutex<ConnectionManager>>,
}

impl RedisBackend {
    pub async fn connect(url: &str) -> Result<Self, BackendError> {
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Connect with indefinite exponential backoff, checking `shutdown`
    /// between attempts so SIGINT/SIGTERM still exits cleanly while
    /// Redis is unreachable at startup. DESIGN.md §3 "Redis disconnect"
    /// — we never exit on Redis outage; we wait it out.
    ///
    /// Returns `Ok(None)` if shutdown fired before connection
    /// succeeded. Returns `Ok(Some(backend))` on success.
    pub async fn connect_with_retry(
        url: &str,
        initial_backoff: std::time::Duration,
        max_backoff: std::time::Duration,
        warn_interval: std::time::Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<Option<Self>, BackendError> {
        let mut backoff = initial_backoff;
        let started = std::time::Instant::now();
        let mut last_warn: Option<std::time::Instant> = None;
        let mut attempt: u64 = 0;

        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }
            attempt += 1;
            match Self::connect(url).await {
                Ok(b) => {
                    if attempt > 1 {
                        tracing::info!(
                            attempts = attempt,
                            total_wait_secs = started.elapsed().as_secs_f64(),
                            "Redis connection established after retries"
                        );
                    }
                    return Ok(Some(b));
                }
                Err(err) => {
                    let now = std::time::Instant::now();
                    let warn_due = match last_warn {
                        None => true,
                        Some(t) => now.duration_since(t) >= warn_interval,
                    };
                    if warn_due {
                        tracing::warn!(
                            target: "horizon_firehose::metrics",
                            event_type = "redis_startup_retry",
                            attempt,
                            downtime_secs = started.elapsed().as_secs_f64(),
                            next_backoff_ms = backoff.as_millis() as u64,
                            error = %err,
                            "Redis not reachable at startup; retrying"
                        );
                        last_warn = Some(now);
                    }
                }
            }

            tokio::select! {
                biased;
                _ = shutdown.changed() => return Ok(None),
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff.saturating_mul(2).min(max_backoff);
        }
    }
}

impl StreamBackend for RedisBackend {
    async fn xadd(
        &self,
        stream_key: &str,
        max_len: u64,
        event_type: &str,
        data: &[u8],
        seq: u64,
    ) -> Result<String, BackendError> {
        let seq_str = seq.to_string();
        let items: &[(&str, &[u8])] = &[
            ("type", event_type.as_bytes()),
            ("data", data),
            ("seq", seq_str.as_bytes()),
        ];
        let id: String = self
            .conn
            .lock()
            .await
            .xadd_maxlen(
                stream_key,
                redis::streams::StreamMaxlen::Approx(max_len as usize),
                "*",
                items,
            )
            .await?;
        Ok(id)
    }

    async fn set_cursor(&self, key: &str, seq: u64) -> Result<(), BackendError> {
        let _: () = self.conn.lock().await.set(key, seq.to_string()).await?;
        Ok(())
    }

    async fn get_cursor(&self, key: &str) -> Result<Option<u64>, BackendError> {
        let raw: Option<String> = self.conn.lock().await.get(key).await?;
        Ok(raw.and_then(|s| s.parse::<u64>().ok()))
    }

    async fn xlen(&self, stream_key: &str) -> Result<u64, BackendError> {
        let n: u64 = self.conn.lock().await.xlen(stream_key).await?;
        Ok(n)
    }

    async fn oldest_event_timestamp(
        &self,
        stream_key: &str,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, BackendError> {
        // XRANGE <key> - + COUNT 1 → the one oldest entry.
        // Response shape: Vec<StreamId> with one element containing an
        // `id` string "<ms>-<seq>".
        let reply: redis::streams::StreamRangeReply = self
            .conn
            .lock()
            .await
            .xrange_count(stream_key, "-", "+", 1)
            .await?;
        Ok(reply.ids.first().and_then(|entry| parse_stream_id_ms(&entry.id)))
    }
}

/// Parse the `<ms>-<seq>` ID form Redis streams use into a UTC
/// timestamp. Returns `None` on malformed input — `oldest_event_age`
/// is diagnostic, not load-bearing, so we degrade silently rather
/// than erroring.
fn parse_stream_id_ms(id: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let (ms_part, _seq_part) = id.split_once('-')?;
    let ms: i64 = ms_part.parse().ok()?;
    chrono::DateTime::from_timestamp_millis(ms)
}

// ─── In-memory fake ────────────────────────────────────────────────────

/// How the fake should respond to the next operation. `AlwaysOk` is the
/// default. Test code flips this to drive retry/recovery branches.
#[derive(Debug, Clone, Default)]
pub enum FailMode {
    #[default]
    AlwaysOk,
    /// Fail the next `n` operations (of any kind), then flip back to
    /// `AlwaysOk`. Used to simulate transient outages.
    FailNext(usize),
}

/// One stream entry: id, insertion timestamp, and ordered list of
/// (field, value) pairs. Real Redis preserves field insertion order
/// (Vec matches that) and embeds the ms timestamp in the id; we
/// carry it as a separate field so tests that pre-seed or reach into
/// the fake don't have to parse "<ms>-<seq>".
type StreamEntry = (String, chrono::DateTime<chrono::Utc>, Vec<(String, Vec<u8>)>);

#[derive(Debug, Default)]
struct InMemoryState {
    /// Stream entries, oldest-first.
    streams: HashMap<String, VecDeque<StreamEntry>>,
    /// Plain key/value store for cursor keys.
    kv: HashMap<String, Vec<u8>>,
    /// Monotonic counter for synthetic entry IDs.
    next_id: u64,
    /// Controllable failure injection.
    fail_mode: FailMode,
}

/// Faithful-enough Redis stand-in. See the module docs for fidelity
/// caveats.
#[derive(Clone, Default)]
pub struct InMemoryBackend {
    inner: Arc<Mutex<InMemoryState>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pre-seed a cursor value. Useful for startup-resume tests.
    pub async fn seed_cursor(&self, key: &str, seq: u64) {
        self.inner
            .lock()
            .await
            .kv
            .insert(key.to_string(), seq.to_string().into_bytes());
    }

    pub async fn set_fail_mode(&self, mode: FailMode) {
        self.inner.lock().await.fail_mode = mode;
    }

    /// Read all entries from a stream in insertion order — used to
    /// assert on published content.
    pub async fn stream_entries(&self, stream_key: &str) -> Vec<StreamEntry> {
        self.inner
            .lock()
            .await
            .streams
            .get(stream_key)
            .cloned()
            .map(|v| v.into_iter().collect())
            .unwrap_or_default()
    }

    pub async fn kv_snapshot(&self) -> HashMap<String, Vec<u8>> {
        self.inner.lock().await.kv.clone()
    }

    /// Consume a failure token if one is pending. Returns `Err` if the
    /// caller should pretend this operation failed.
    fn maybe_inject_failure(state: &mut InMemoryState, op: &str) -> Result<(), BackendError> {
        if let FailMode::FailNext(n) = state.fail_mode.clone() {
            if n > 0 {
                state.fail_mode = if n == 1 {
                    FailMode::AlwaysOk
                } else {
                    FailMode::FailNext(n - 1)
                };
                return Err(BackendError::Injected(format!(
                    "simulated failure for op={op}"
                )));
            }
        }
        Ok(())
    }
}

impl StreamBackend for InMemoryBackend {
    async fn xadd(
        &self,
        stream_key: &str,
        max_len: u64,
        event_type: &str,
        data: &[u8],
        seq: u64,
    ) -> Result<String, BackendError> {
        let mut state = self.inner.lock().await;
        Self::maybe_inject_failure(&mut state, "xadd")?;

        let id = {
            state.next_id += 1;
            format!("{}-0", state.next_id)
        };

        let fields = vec![
            ("type".to_string(), event_type.as_bytes().to_vec()),
            ("data".to_string(), data.to_vec()),
            ("seq".to_string(), seq.to_string().into_bytes()),
        ];

        let inserted_at = chrono::Utc::now();
        let entries = state.streams.entry(stream_key.to_string()).or_default();
        entries.push_back((id.clone(), inserted_at, fields));

        // MAXLEN ~ trim. Real Redis may leave the stream slightly over
        // because it trims only whole radix-tree nodes; we do strict
        // trimming, which is always <= the real-Redis length for the
        // same input sequence. See module docs.
        while entries.len() as u64 > max_len {
            entries.pop_front();
        }

        Ok(id)
    }

    async fn set_cursor(&self, key: &str, seq: u64) -> Result<(), BackendError> {
        let mut state = self.inner.lock().await;
        Self::maybe_inject_failure(&mut state, "set_cursor")?;
        state.kv.insert(key.to_string(), seq.to_string().into_bytes());
        Ok(())
    }

    async fn get_cursor(&self, key: &str) -> Result<Option<u64>, BackendError> {
        let mut state = self.inner.lock().await;
        Self::maybe_inject_failure(&mut state, "get_cursor")?;
        Ok(state
            .kv
            .get(key)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|s| s.parse::<u64>().ok()))
    }

    async fn xlen(&self, stream_key: &str) -> Result<u64, BackendError> {
        // Deliberately not subject to `FailMode`: `xlen` is a pure
        // observation path used by tests to assert on the published
        // state *after* they've set up a failure scenario. If we let
        // injected failures consume an `xlen` call, the tests would
        // have to remember to reset FailMode before every assertion
        // — tedious and error-prone. Real Redis `XLEN` can still fail
        // at the wire layer, but we don't exercise it on the hot path.
        let state = self.inner.lock().await;
        Ok(state
            .streams
            .get(stream_key)
            .map(|v| v.len() as u64)
            .unwrap_or(0))
    }

    async fn oldest_event_timestamp(
        &self,
        stream_key: &str,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, BackendError> {
        // Same reasoning as `xlen` — observation path, not subject
        // to fail injection.
        let state = self.inner.lock().await;
        Ok(state
            .streams
            .get(stream_key)
            .and_then(|v| v.front().map(|(_, ts, _)| *ts)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn xadd_trims_to_max_len() {
        let b = InMemoryBackend::new();
        for i in 0..10u64 {
            b.xadd("k", 3, "commit", b"{}", i).await.unwrap();
        }
        assert_eq!(b.xlen("k").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn set_and_get_cursor_round_trips() {
        let b = InMemoryBackend::new();
        assert_eq!(b.get_cursor("c").await.unwrap(), None);
        b.set_cursor("c", 42).await.unwrap();
        assert_eq!(b.get_cursor("c").await.unwrap(), Some(42));
    }

    #[tokio::test]
    async fn fail_next_consumes_one_failure_per_op() {
        let b = InMemoryBackend::new();
        b.set_fail_mode(FailMode::FailNext(2)).await;
        assert!(b.xadd("k", 10, "t", b"d", 1).await.is_err());
        assert!(b.xadd("k", 10, "t", b"d", 2).await.is_err());
        assert!(b.xadd("k", 10, "t", b"d", 3).await.is_ok());
        assert_eq!(b.xlen("k").await.unwrap(), 1);
    }
}

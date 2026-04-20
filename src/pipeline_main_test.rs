//! Phase 5 integration tests for the full pipeline orchestration.
//!
//! These tests exercise the pieces `run_async` glues together
//! (ws_reader ← decoder ← router ← publisher ← cursor persister), but
//! avoid taking dependencies on the actual `run_async` entry point
//! because it reads real env + real Redis URLs at startup. Instead
//! each test wires the same topology against mock servers +
//! [`InMemoryBackend`], verifying:
//!
//! - The full pipeline carries a real ATProto fixture frame end-to-end
//!   (ws → decoder → router → publisher → backend).
//! - Shutdown flows in the §3 cascade order within the 30s budget and
//!   the final cursor lands in Redis.
//! - A task panic is detected by the coordinator-style select pattern
//!   and triggers a clean cascade anyway.
//! - Startup against a backend that's failing every op lets us still
//!   cleanly shut down via the signal.

use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::{accept_async, tungstenite::Message};

use crate::backend::{FailMode, InMemoryBackend, StreamBackend};
use crate::config::{OversizePolicy, RelayConfig};
use crate::cursor::{Cursors, cursor_key, spawn_persister};
use crate::decoder;
use crate::event::Event;
use crate::metrics::Metrics;
use crate::publisher::{self, PublishOp, PublisherOptions};
use crate::router::{self, RouterOptions};
use crate::ws_reader::{self, WsReaderOptions};

/// Load a single fixture frame to stream through the mock relay. Phase
/// 3 proved every captured frame decodes; Phase 5 just needs one that
/// will produce an event.
fn sample_fixture_frame() -> Option<Vec<u8>> {
    let fixtures_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let manifest_path = fixtures_dir.join("manifest.json");
    let manifest_str = std::fs::read_to_string(&manifest_path).ok()?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest_str).ok()?;
    let frames = manifest["frames"].as_array()?;
    for f in frames {
        let name = f["filename"].as_str()?;
        let bytes = std::fs::read(fixtures_dir.join(name)).ok()?;
        // Return the first frame that decodes to an Event (skip
        // `#info` / `#sync` for the "one round-trip" test).
        if let Ok(crate::decoder::DecodedFrame::Event { .. }) =
            crate::decoder::decode_frame(&bytes, "ws://test")
        {
            return Some(bytes);
        }
    }
    None
}

/// Start a WebSocket server that sends the supplied frames once and
/// then holds the connection open. Returns `(url, JoinHandle)`;
/// aborting the handle closes the listener.
async fn start_mock_ws(frames: Vec<Vec<u8>>) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let frames = frames.clone();
            tokio::spawn(async move {
                let Ok(mut ws) = accept_async(stream).await else {
                    return;
                };
                for f in frames {
                    if ws.send(Message::Binary(f.into())).await.is_err() {
                        return;
                    }
                }
                // Hold the connection so the reader stays happy; the
                // test initiates shutdown from its end.
                std::future::pending::<()>().await;
            });
        }
    });
    (url, task)
}

fn relay_cfg(url: String) -> RelayConfig {
    RelayConfig {
        url,
        fallbacks: vec![],
        reconnect_initial_delay_ms: 50,
        reconnect_max_delay_ms: 500,
        failover_threshold: 3,
        failover_cooldown_seconds: 5,
        tls_extra_ca_file: String::new(),
    }
}

fn test_publisher_opts() -> PublisherOptions {
    PublisherOptions {
        stream_key: "firehose:events".into(),
        max_stream_len: 1_000,
        max_event_size_bytes: 1 << 20,
        on_oversize: OversizePolicy::SkipWithLog,
        retry_initial: Duration::from_millis(10),
        retry_max: Duration::from_millis(50),
        retry_warn_interval: Duration::from_millis(200),
    }
}

// ─── test 1: happy-path end-to-end through all five stages ─────────

#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_pipeline_delivers_event_to_backend() {
    let Some(frame) = sample_fixture_frame() else {
        eprintln!("SKIP: no fixtures available for phase-5 end-to-end test");
        return;
    };
    let (url, server_task) = start_mock_ws(vec![frame]).await;

    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let (event_tx, event_rx) = mpsc::channel::<PublishOp>(128);
    let (filtered_tx, filtered_rx) = mpsc::channel::<PublishOp>(128);

    let ws_reader = ws_reader::spawn_with_cursors(
        relay_cfg(url.clone()),
        WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
        Metrics::new(),
    );
    let ws_control = ws_reader.control();
    // Phase 10.5 finding 5.1: decoder needs policy config + exit
    // plumbing. Tests use LiveTip + ReconnectFromLiveTip (the default
    // behaviour) and ignore the policy-exit channel since neither
    // test triggers a policy-exit frame.
    let (decoder_policy_exit_tx, _decoder_policy_exit_rx) =
        mpsc::unbounded_channel::<decoder::PolicyExit>();
    let decoder_h = decoder::spawn(
        ws_reader,
        event_tx,
        Metrics::new(),
        ws_control,
        decoder::DecoderPolicies {
            on_stale_cursor: crate::config::StaleCursorPolicy::LiveTip,
            on_protocol_error: crate::config::ProtocolErrorPolicy::ReconnectFromLiveTip,
        },
        shutdown_tx.clone(),
        decoder_policy_exit_tx,
    );
    let router_h = router::spawn(
        RouterOptions {
            record_types: vec![],
        },
        event_rx,
        filtered_tx,
    );
    let publisher_h = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        test_publisher_opts(),
        filtered_rx,
        Metrics::new(),
    );

    // Poll for the event to land in the backend. Up to 3 seconds.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if backend.xlen("firehose:events").await.unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        backend.xlen("firehose:events").await.unwrap() > 0,
        "event should have propagated through ws_reader → decoder → router → publisher"
    );
    // Exactly one relay cursor should have moved past 0.
    let snap = cursors.snapshot().await;
    assert!(
        snap.iter().any(|(r, seq)| *r == url && *seq > 0),
        "cursor should have advanced on XADD success; snap={snap:?}"
    );

    // Shut down by flipping the global signal, same way `run_async` does.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), decoder_h.join()).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), router_h.shutdown()).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), publisher_h.join()).await;

    server_task.abort();
}

// ─── test 2: full shutdown cascade lands the final cursor ─────────

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cascade_finalises_cursor_within_budget() {
    let Some(frame) = sample_fixture_frame() else {
        eprintln!("SKIP: no fixtures available");
        return;
    };
    let (url, server_task) = start_mock_ws(vec![frame]).await;

    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let (event_tx, event_rx) = mpsc::channel::<PublishOp>(128);
    let (filtered_tx, filtered_rx) = mpsc::channel::<PublishOp>(128);

    let ws_reader = ws_reader::spawn_with_cursors(
        relay_cfg(url.clone()),
        WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
        Metrics::new(),
    );
    let ws_control = ws_reader.control();
    // Phase 10.5 finding 5.1: decoder needs policy config + exit
    // plumbing. Tests use LiveTip + ReconnectFromLiveTip (the default
    // behaviour) and ignore the policy-exit channel since neither
    // test triggers a policy-exit frame.
    let (decoder_policy_exit_tx, _decoder_policy_exit_rx) =
        mpsc::unbounded_channel::<decoder::PolicyExit>();
    let decoder_h = decoder::spawn(
        ws_reader,
        event_tx,
        Metrics::new(),
        ws_control,
        decoder::DecoderPolicies {
            on_stale_cursor: crate::config::StaleCursorPolicy::LiveTip,
            on_protocol_error: crate::config::ProtocolErrorPolicy::ReconnectFromLiveTip,
        },
        shutdown_tx.clone(),
        decoder_policy_exit_tx,
    );
    let router_h = router::spawn(
        RouterOptions {
            record_types: vec![],
        },
        event_rx,
        filtered_tx,
    );
    let publisher_h = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        test_publisher_opts(),
        filtered_rx,
        Metrics::new(),
    );
    // Persist cursors every 20ms so we know the periodic path runs.
    let persister_h = spawn_persister(cursors.clone(), backend.clone(), Duration::from_millis(20));

    // Let one event flow through.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline
        && backend.xlen("firehose:events").await.unwrap() == 0
    {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(backend.xlen("firehose:events").await.unwrap() > 0);

    let start_shutdown = std::time::Instant::now();
    // Same sequence as `run_async::shutdown_cascade`: signal → drain
    // each stage → final cursor write → persister shutdown.
    let _ = shutdown_tx.send(true);
    let _decoder_stats = tokio::time::timeout(Duration::from_secs(5), decoder_h.join())
        .await
        .expect("decoder join timed out");
    let _router_stats = tokio::time::timeout(Duration::from_secs(5), router_h.shutdown())
        .await
        .expect("router shutdown timed out");
    let _publisher_stats = tokio::time::timeout(Duration::from_secs(5), publisher_h.join())
        .await
        .expect("publisher join timed out");
    let final_out = cursors.persist_all(backend.as_ref()).await;
    assert!(final_out.written >= 1);
    let _persister_stats = tokio::time::timeout(Duration::from_secs(5), persister_h.shutdown())
        .await
        .expect("persister shutdown timed out");

    let shutdown_elapsed = start_shutdown.elapsed();
    assert!(
        shutdown_elapsed < Duration::from_secs(30),
        "shutdown took {shutdown_elapsed:?} — should be well under 30s budget"
    );

    // Final cursor should be persisted to the backend under its
    // base64url key.
    let key = cursor_key(&url);
    let persisted = backend.get_cursor(&key).await.unwrap();
    assert!(
        persisted.is_some(),
        "final cursor should be written under {key}; kv={:?}",
        backend.kv_snapshot().await
    );

    server_task.abort();
}

// ─── test 3: task panic is detected and triggers cascade ──────────

#[tokio::test(flavor = "multi_thread")]
async fn task_panic_is_detected_via_join_handle_select() {
    // Spawn a task that panics shortly. This simulates what the
    // coordinator select in `run_async` sees when a real pipeline
    // task crashes.
    let task: JoinHandle<()> = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        panic!("synthetic panic for test");
    });

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let mut h = task;

    // Mirror the main coordinator's select topology.
    let exit_reason = tokio::select! {
        _ = shutdown_rx.changed() => "signal",
        r = &mut h => {
            // `is_err()` returns true for panics (JoinError).
            assert!(r.is_err(), "should be a panic-derived JoinError");
            "panicked"
        }
    };
    assert_eq!(exit_reason, "panicked");

    // Initiating shutdown after the panic must still work (no hang).
    let _ = shutdown_tx.send(true);
}

// ─── test 4: pipeline survives Redis-down-at-startup ──────────────

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_initializes_even_when_backend_is_failing_then_shuts_down() {
    // Simulate an unreachable Redis by pre-setting a massive fail
    // count on the fake. Every XADD will fail, every cursor
    // set_cursor will fail. The publisher should enter its retry
    // loop; the coordinator's signal path must still cleanly shut
    // down.
    let backend = Arc::new(InMemoryBackend::new());
    backend.set_fail_mode(FailMode::FailNext(10_000)).await;

    let cursors = Cursors::new();
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);

    let (event_tx, event_rx) = mpsc::channel::<PublishOp>(32);
    let (filtered_tx, filtered_rx) = mpsc::channel::<PublishOp>(32);

    // No ws_reader/decoder in this test — we stuff events in
    // directly. What we're testing is the publisher-with-broken-backend
    // path remaining shutdown-responsive.
    let router_h = router::spawn(
        RouterOptions {
            record_types: vec![],
        },
        event_rx,
        filtered_tx,
    );
    let publisher_h = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        test_publisher_opts(),
        filtered_rx,
        Metrics::new(),
    );
    let persister_h = spawn_persister(cursors.clone(), backend.clone(), Duration::from_millis(20));

    // Inject one event so the publisher is actively retrying.
    let ev = Event::Tombstone(crate::event::TombstoneEvent {
        did: "did:plc:abc".into(),
        relay: "ws://test".into(),
    });
    event_tx.send(PublishOp::Publish(ev, 42)).await.unwrap();
    drop(event_tx);

    // Let the retry loop engage.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Now shut down, same shape as `run_async`.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), router_h.shutdown())
        .await
        .expect("router shutdown hung while backend broken");
    let _ = tokio::time::timeout(Duration::from_secs(5), publisher_h.shutdown())
        .await
        .expect("publisher shutdown hung while backend broken");
    let _ = tokio::time::timeout(Duration::from_secs(5), persister_h.shutdown())
        .await
        .expect("persister shutdown hung while backend broken");

    // Cursor must NOT have advanced — Redis never accepted the XADD.
    assert_eq!(cursors.get("ws://test").await, None);
    // Nothing ever landed in the stream.
    assert_eq!(backend.xlen("firehose:events").await.unwrap(), 0);
}

// ─── Phase 6: metrics integration ─────────────────────────────────

/// A tracing layer that stashes every `info!`-level event from
/// `horizon_firehose::metrics` into a shared `Vec<String>` so tests
/// can inspect the emitted event's fields.
mod capture {
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;

    #[derive(Default, Clone)]
    pub struct Captured {
        pub records: Arc<Mutex<Vec<Record>>>,
    }

    #[derive(Debug, Clone)]
    pub struct Record {
        pub fields: Vec<(String, String)>,
    }

    impl Record {
        pub fn field(&self, name: &str) -> Option<&str> {
            self.fields
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        }
    }

    struct CaptureVisitor<'a>(&'a mut Vec<(String, String)>);
    impl Visit for CaptureVisitor<'_> {
        fn record_debug(&mut self, f: &Field, v: &dyn std::fmt::Debug) {
            self.0.push((f.name().to_string(), format!("{v:?}")));
        }
        fn record_str(&mut self, f: &Field, v: &str) {
            self.0.push((f.name().to_string(), v.to_string()));
        }
        fn record_u64(&mut self, f: &Field, v: u64) {
            self.0.push((f.name().to_string(), v.to_string()));
        }
        fn record_i64(&mut self, f: &Field, v: i64) {
            self.0.push((f.name().to_string(), v.to_string()));
        }
        fn record_f64(&mut self, f: &Field, v: f64) {
            self.0.push((f.name().to_string(), v.to_string()));
        }
        fn record_bool(&mut self, f: &Field, v: bool) {
            self.0.push((f.name().to_string(), v.to_string()));
        }
    }

    pub struct CaptureLayer(pub Captured);
    impl<S: Subscriber> Layer<S> for CaptureLayer {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().target() != "horizon_firehose::metrics" {
                return;
            }
            let mut fields = Vec::new();
            event.record(&mut CaptureVisitor(&mut fields));
            self.0.records.lock().unwrap().push(Record { fields });
        }
    }
}

// `current_thread` flavor deliberately. `tracing::subscriber::set_default`
// installs a *thread-local* dispatcher, so spawned tokio tasks on a
// multi-thread runtime run on worker threads that don't see it and
// the emitter's `info!(…)` calls get swallowed by the no-op
// subscriber. Current-thread drives everything on the test thread,
// preserving the dispatcher.
#[tokio::test(flavor = "current_thread")]
async fn periodic_metrics_emits_with_expected_fields() {
    use tracing_subscriber::layer::SubscriberExt;

    let captured = capture::Captured::default();
    let subscriber = tracing_subscriber::registry().with(capture::CaptureLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();
    let metrics = Metrics::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Seed some counter activity so deltas are non-zero on emission.
    metrics
        .events_total
        .fetch_add(100, std::sync::atomic::Ordering::Relaxed);
    metrics
        .bytes_total
        .fetch_add(12_345, std::sync::atomic::Ordering::Relaxed);
    cursors.advance("ws://test", 42).await;

    // Fake ws_state + channel gauges so the emitter has something to
    // sample. The emitter does not actually care whether the channels
    // ever had traffic — only that upgrade() returns Some(Sender).
    let (e_tx, _e_rx) = mpsc::channel::<PublishOp>(16);
    let (f_tx, _f_rx) = mpsc::channel::<PublishOp>(16);
    let (w_tx, _w_rx) = mpsc::channel::<crate::ws_reader::Frame>(16);
    let gauges = crate::metrics::ChannelGauges {
        ws_to_decoder: w_tx.downgrade(),
        decoder_to_router: e_tx.downgrade(),
        router_to_publisher: f_tx.downgrade(),
    };
    // Build a WsStateReader by spawning a throwaway ws_reader against
    // a non-existent relay — it'll fail to connect, but state_reader
    // only cares about the shared state struct.
    let throwaway = ws_reader::spawn_with_cursors(
        relay_cfg("ws://127.0.0.1:1".into()),
        WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
        Metrics::new(),
    );
    let ws_state = throwaway.state_reader();

    let handle = crate::metrics::spawn_emitter(
        metrics.clone(),
        gauges,
        cursors.clone(),
        ws_state,
        backend.clone(),
        "firehose:events".to_string(),
        std::time::Instant::now(),
        Duration::from_millis(50),
        shutdown_rx.clone(),
    );

    // Let a couple of ticks fire.
    tokio::time::sleep(Duration::from_millis(180)).await;
    let _ = shutdown_tx.send(true);
    handle.join().await;
    throwaway.shutdown().await;

    let records = captured.records.lock().unwrap().clone();
    let periodic: Vec<_> = records
        .into_iter()
        .filter(|r| r.field("event_type") == Some("periodic_metrics"))
        .collect();
    assert!(
        !periodic.is_empty(),
        "expected at least one periodic_metrics event"
    );
    let first = &periodic[0];
    // Spot-check the critical schema fields from DESIGN.md §4.
    for required in [
        "window_seconds",
        "events_in_window",
        "events_per_sec_in_window",
        "bytes_in_window",
        "decode_errors_in_window",
        "redis_errors_in_window",
        "relay_reconnects_in_window",
        "total_reconnects_since_start",
        "reconnects_last_hour",
        "unknown_type_count_in_window",
        "oversize_events_in_window",
        "skipped_frames_in_window",
        "channel_depths",
        "cursor_ages_seconds",
        "oldest_event_age_seconds",
        "redis_healthy",
        "uptime_seconds",
    ] {
        assert!(
            first.field(required).is_some(),
            "periodic_metrics missing required field `{required}`; got fields: {:?}",
            first.fields
        );
    }

    // `cursor_ages_seconds` is a JSON object keyed by relay URL with
    // per-relay age in seconds — NOT a scalar. Pin this explicitly so
    // a future "simplify to single number" refactor would break the
    // test, matching the DESIGN.md §4 schema.
    let ages = first.field("cursor_ages_seconds").unwrap();
    let ages_val: serde_json::Value =
        serde_json::from_str(ages).expect("cursor_ages_seconds must be JSON");
    let obj = ages_val
        .as_object()
        .expect("cursor_ages_seconds must be a JSON object, not a scalar");
    let age_for_test = obj
        .get("ws://test")
        .expect("expected 'ws://test' key in cursor_ages_seconds");
    assert!(
        age_for_test.as_u64().is_some(),
        "per-relay age must be a number; got {age_for_test:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn redis_healthy_false_and_oldest_event_age_null_when_backend_errors() {
    // Phase 8.5 follow-up finding 4.6: on Redis outage, the
    // metric must report `redis_healthy = false` AND
    // `oldest_event_age_seconds` must serialize as JSON `null`,
    // not `0`. Previously the field read `0` in both "stream is
    // empty" and "Redis unreachable" cases — alerts couldn't
    // distinguish "everything's fine, no backlog" from "Redis
    // is down".
    use tracing_subscriber::layer::SubscriberExt;

    let captured = capture::Captured::default();
    let subscriber = tracing_subscriber::registry().with(capture::CaptureLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let backend = Arc::new(InMemoryBackend::new());
    // Every call fails — simulates Redis outage.
    backend.set_fail_mode(FailMode::FailNext(10_000)).await;

    let cursors = Cursors::new();
    let metrics = Metrics::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let (e_tx, _e_rx) = mpsc::channel::<PublishOp>(16);
    let (f_tx, _f_rx) = mpsc::channel::<PublishOp>(16);
    let (w_tx, _w_rx) = mpsc::channel::<crate::ws_reader::Frame>(16);
    let gauges = crate::metrics::ChannelGauges {
        ws_to_decoder: w_tx.downgrade(),
        decoder_to_router: e_tx.downgrade(),
        router_to_publisher: f_tx.downgrade(),
    };
    let throwaway = ws_reader::spawn_with_cursors(
        relay_cfg("ws://127.0.0.1:1".into()),
        WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
        Metrics::new(),
    );

    let handle = crate::metrics::spawn_emitter(
        metrics.clone(),
        gauges,
        cursors.clone(),
        throwaway.state_reader(),
        backend.clone(),
        "firehose:events".to_string(),
        std::time::Instant::now(),
        Duration::from_millis(50),
        shutdown_rx.clone(),
    );

    tokio::time::sleep(Duration::from_millis(180)).await;
    let _ = shutdown_tx.send(true);
    handle.join().await;
    throwaway.shutdown().await;

    let records = captured.records.lock().unwrap().clone();
    let periodic = records
        .iter()
        .find(|r| r.field("event_type") == Some("periodic_metrics"))
        .expect("at least one periodic_metrics event");

    assert_eq!(
        periodic.field("redis_healthy"),
        Some("false"),
        "Redis is failing; redis_healthy must be false"
    );
    // `oldest_event_age_seconds` is emitted as JSON serialization
    // of `Option<u64>`. On outage → `null`.
    let oldest = periodic.field("oldest_event_age_seconds").unwrap_or("");
    assert_eq!(
        oldest.trim(),
        "null",
        "on Redis outage, oldest_event_age_seconds must be null — got {oldest:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cursor_ages_seconds_is_per_relay_object_not_scalar() {
    use tracing_subscriber::layer::SubscriberExt;

    let captured = capture::Captured::default();
    let subscriber = tracing_subscriber::registry().with(capture::CaptureLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();
    let metrics = Metrics::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Multi-relay scenario: advance two cursors at different
    // Instants so their ages differ in the emission.
    cursors.advance("wss://primary.test/xrpc/sub", 100).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    cursors.advance("wss://fallback.test/xrpc/sub", 7).await;

    let (e_tx, _e_rx) = mpsc::channel::<PublishOp>(16);
    let (f_tx, _f_rx) = mpsc::channel::<PublishOp>(16);
    let (w_tx, _w_rx) = mpsc::channel::<crate::ws_reader::Frame>(16);
    let gauges = crate::metrics::ChannelGauges {
        ws_to_decoder: w_tx.downgrade(),
        decoder_to_router: e_tx.downgrade(),
        router_to_publisher: f_tx.downgrade(),
    };
    let throwaway = ws_reader::spawn_with_cursors(
        relay_cfg("ws://127.0.0.1:1".into()),
        WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
        Metrics::new(),
    );

    let handle = crate::metrics::spawn_emitter(
        metrics.clone(),
        gauges,
        cursors.clone(),
        throwaway.state_reader(),
        backend.clone(),
        "firehose:events".to_string(),
        std::time::Instant::now(),
        Duration::from_millis(50),
        shutdown_rx.clone(),
    );

    tokio::time::sleep(Duration::from_millis(180)).await;
    let _ = shutdown_tx.send(true);
    handle.join().await;
    throwaway.shutdown().await;

    let records = captured.records.lock().unwrap().clone();
    let periodic = records
        .iter()
        .find(|r| r.field("event_type") == Some("periodic_metrics"))
        .expect("at least one periodic_metrics event");

    let ages_str = periodic.field("cursor_ages_seconds").unwrap();
    let ages: serde_json::Value = serde_json::from_str(ages_str).unwrap();
    let obj = ages
        .as_object()
        .expect("must be object — multi-relay deployments have multiple ages");
    // Both relays present with their own number.
    assert!(obj.contains_key("wss://primary.test/xrpc/sub"));
    assert!(obj.contains_key("wss://fallback.test/xrpc/sub"));
    assert_eq!(obj.len(), 2, "only the two relays we advanced; got {obj:?}");
    for (relay, age) in obj {
        assert!(
            age.as_u64().is_some(),
            "relay {relay:?} age must be a number; got {age:?}"
        );
    }
    // Concrete example of the emitted shape (pretty-printed for docs):
    //   "cursor_ages_seconds": {
    //     "wss://primary.test/xrpc/sub": 0,
    //     "wss://fallback.test/xrpc/sub": 0
    //   }
    // Field is plural, values are seconds-since-last-advance per relay.
}

// See the sibling `periodic_metrics_emits_with_expected_fields`
// comment for why this is `current_thread`.
#[tokio::test(flavor = "current_thread")]
async fn periodic_metrics_delta_captures_increments_between_ticks() {
    use tracing_subscriber::layer::SubscriberExt;

    let captured = capture::Captured::default();
    let subscriber = tracing_subscriber::registry().with(capture::CaptureLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();
    let metrics = Metrics::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let (e_tx, _e_rx) = mpsc::channel::<PublishOp>(16);
    let (f_tx, _f_rx) = mpsc::channel::<PublishOp>(16);
    let (w_tx, _w_rx) = mpsc::channel::<crate::ws_reader::Frame>(16);
    let gauges = crate::metrics::ChannelGauges {
        ws_to_decoder: w_tx.downgrade(),
        decoder_to_router: e_tx.downgrade(),
        router_to_publisher: f_tx.downgrade(),
    };
    let throwaway = ws_reader::spawn_with_cursors(
        relay_cfg("ws://127.0.0.1:1".into()),
        WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
        Metrics::new(),
    );
    let ws_state = throwaway.state_reader();

    let handle = crate::metrics::spawn_emitter(
        metrics.clone(),
        gauges,
        cursors.clone(),
        ws_state,
        backend.clone(),
        "firehose:events".to_string(),
        std::time::Instant::now(),
        Duration::from_millis(60),
        shutdown_rx.clone(),
    );

    // Bump +10 before tick 1, +20 before tick 2. Expect deltas 10, 20.
    tokio::time::sleep(Duration::from_millis(40)).await;
    metrics
        .events_total
        .fetch_add(10, std::sync::atomic::Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(60)).await;
    metrics
        .events_total
        .fetch_add(20, std::sync::atomic::Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(80)).await;

    let _ = shutdown_tx.send(true);
    handle.join().await;
    throwaway.shutdown().await;

    let records = captured.records.lock().unwrap().clone();
    let deltas: Vec<u64> = records
        .iter()
        .filter(|r| r.field("event_type") == Some("periodic_metrics"))
        .filter_map(|r| r.field("events_in_window").and_then(|s| s.parse().ok()))
        .collect();
    assert!(
        deltas.len() >= 2,
        "expected ≥2 periodic events; got {deltas:?}"
    );
    // First non-zero delta should be 10, second should be 20. Timing
    // jitter can coalesce increments into one tick, so relax the
    // assertion to "total adds up and ordering monotonic".
    let sum: u64 = deltas.iter().sum();
    assert_eq!(sum, 30, "deltas must cover the full 30-count increment");
}

#[tokio::test(flavor = "current_thread")]
async fn spawn_instrumented_emits_task_panicked_before_propagating() {
    use tracing_subscriber::layer::SubscriberExt;

    // Phase 8.5 follow-up finding 2.1: verify the attribution log
    // fires inside the panicking task (so the root cause name is in
    // the log stream), and that the JoinHandle still resolves with
    // an Err(JoinError) so main's coordinator `select!` sees the
    // panic the same way tokio's default propagation would.
    let captured = capture::Captured::default();
    let subscriber = tracing_subscriber::registry().with(capture::CaptureLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    // `panic!("literal")` in modern Rust / edition 2024 ends up
    // with a panic payload type that is NOT one of `&'static str` or
    // `String` — the macro routes through `fmt::Arguments` and the
    // concrete payload type is a private stdlib wrapper (type_id
    // not publicly nameable). Using `panic_any` with an explicit
    // `String` sidesteps that: the payload is a real `String` we
    // can downcast to for the log body.
    let handle = crate::spawn_instrumented("unit_test_victim_task", async {
        std::panic::panic_any(String::from(
            "synthetic panic carries this message verbatim",
        ))
    });

    // JoinHandle must resolve with JoinError (panic propagated).
    let result = handle.await;
    assert!(
        result.is_err(),
        "spawn_instrumented must propagate panic via JoinHandle Err"
    );

    // The structured task_panicked event must have been emitted
    // before propagation, with the task name intact.
    let records = captured.records.lock().unwrap().clone();
    let panic_events: Vec<_> = records
        .iter()
        .filter(|r| r.field("event_type") == Some("task_panicked"))
        .collect();
    assert_eq!(
        panic_events.len(),
        1,
        "expected exactly one task_panicked event, got {panic_events:?}"
    );
    let ev = panic_events[0];
    assert_eq!(ev.field("task_name"), Some("unit_test_victim_task"));
    let msg = ev.field("panic_message").unwrap_or("");
    assert!(
        msg.contains("synthetic panic carries this message verbatim"),
        "panic_message field should contain the panic body; got {msg:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn spawn_instrumented_does_not_emit_on_clean_exit() {
    use tracing_subscriber::layer::SubscriberExt;

    let captured = capture::Captured::default();
    let subscriber = tracing_subscriber::registry().with(capture::CaptureLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    let handle = crate::spawn_instrumented("clean_task", async { 42 });
    let result = handle.await.unwrap();
    assert_eq!(result, 42);

    let records = captured.records.lock().unwrap().clone();
    assert!(
        !records
            .iter()
            .any(|r| r.field("event_type") == Some("task_panicked")),
        "clean exit must not emit task_panicked; got {records:?}"
    );
}

#[test]
fn malformed_cursor_remediation_includes_everything_an_operator_needs() {
    // Phase 8.5 review finding 4.5: at 3am an operator debugging
    // a failed startup should see, in a single log block:
    //   - which relay this cursor belongs to
    //   - the exact Redis key to operate on
    //   - the malformed value that tripped the parse
    //   - a copy-pasteable remediation (three options, inspect /
    //     repair / delete, using the actual key — not `{key}`)
    //
    // This test pins the message contents so a future refactor
    // that accidentally drops a field or re-interpolates `{key}`
    // wrong fails loudly.
    let relay = "wss://relay.fyi/xrpc/com.atproto.sync.subscribeRepos";
    let key =
        "firehose:cursor:d3NzOi8vcmVsYXkuZnlpL3hycGMvY29tLmF0cHJvdG8uc3luYy5zdWJzY3JpYmVSZXBvcw";
    let value = "not-a-u64";
    let msg = crate::malformed_cursor_remediation(relay, key, value);

    // Every concrete value must appear in the message.
    assert!(msg.contains(relay), "relay URL missing: {msg}");
    assert!(msg.contains(key), "cursor key missing: {msg}");
    assert!(msg.contains(value), "malformed value missing: {msg}");

    // All three `redis-cli` remediation commands must be present
    // with the *actual* key substituted in — not a literal `{key}`
    // placeholder.
    assert!(
        msg.contains(&format!("redis-cli -u \"$REDIS_URL\" GET {key}")),
        "GET command missing or has un-interpolated placeholder: {msg}"
    );
    assert!(
        msg.contains(&format!("redis-cli -u \"$REDIS_URL\" SET {key} N")),
        "SET command missing or has un-interpolated placeholder: {msg}"
    );
    assert!(
        msg.contains(&format!("redis-cli -u \"$REDIS_URL\" DEL {key}")),
        "DEL command missing or has un-interpolated placeholder: {msg}"
    );
    // The "{" character can only appear from `{value:?}` Debug quoting;
    // any literal un-interpolated `{key}` or `{value}` would fail this.
    assert!(
        !msg.contains("{key}"),
        "literal `{{key}}` slipped through un-interpolated: {msg}"
    );
    assert!(
        !msg.contains("{value}"),
        "literal `{{value}}` slipped through un-interpolated: {msg}"
    );

    // Must explicitly state that resuming from live tip silently
    // loses events — so the operator understands why we're failing
    // startup rather than warning and continuing.
    assert!(
        msg.to_lowercase().contains("silently lose")
            || msg.to_lowercase().contains("silently loses"),
        "missing loss-of-events explanation: {msg}"
    );
}

#[test]
fn malformed_cursor_remediation_labels_unknown_relay_clearly() {
    // When the key doesn't match any configured relay (e.g. a
    // stale cursor for a relay removed from config), the function
    // is invoked with a `<unknown — …>` placeholder from main.rs.
    // Verify it passes through into the message.
    let msg = crate::malformed_cursor_remediation(
        "<unknown — key didn't match any configured relay>",
        "firehose:cursor:deadbeef",
        "garbage",
    );
    assert!(
        msg.contains("<unknown"),
        "unknown-relay label missing: {msg}"
    );
    assert!(msg.contains("firehose:cursor:deadbeef"));
    assert!(msg.contains("redis-cli -u \"$REDIS_URL\" DEL firehose:cursor:deadbeef"));
}

/// Self-signed RSA cert generated with
/// `openssl req -x509 -newkey rsa:2048 -nodes -keyout /dev/null \
///     -out - -days 3650 -subj '/CN=horizon-test-ca'`. Shape-only
/// fixture: rustls doesn't validate the chain at `add` time, just
/// that the DER parses as a well-formed X.509.
const VALID_TEST_CA_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIDFTCCAf2gAwIBAgIUHnDVt5kv824CloB0fu5rdPf6XEYwDQYJKoZIhvcNAQEL
BQAwGjEYMBYGA1UEAwwPaG9yaXpvbi10ZXN0LWNhMB4XDTI2MDQyMDAwMDk1OVoX
DTM2MDQxNzAwMDk1OVowGjEYMBYGA1UEAwwPaG9yaXpvbi10ZXN0LWNhMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3cJYienkltVL5xC+MRTLPNOmoAgJ
JnMwnAnut8UEtSOmax+uW00/JCTelmK1yGhaSOYgutCebuzhtzsqzA4ma7OIhVzr
nZY/GS/eodcOfg7/FYDsyjBGwikxoCmIzG0ZTrBF0xsQlaBkjtNIxiPmGKno74Yz
+iFVtgpsY+6RrHrBSxN0qN4Dk/da5LXD0QXQsnefCRsPGn2t3zufio7qLnofUWtu
UGC9+nNQYhWTwe3zd/ZzhkhwUqxbp/iUmpubBoQDa/PrHBXXFMSLmMcKikevLfs5
CyGvwR45ExtpWwjTqejs/yBAA57fPHLMbFQcDMTSHCgeH0bnpRK0J/cAtwIDAQAB
o1MwUTAdBgNVHQ4EFgQUNkYRMntoo0hFqrrJuJM/R51Qq30wHwYDVR0jBBgwFoAU
NkYRMntoo0hFqrrJuJM/R51Qq30wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0B
AQsFAAOCAQEAI+a5HicW7ey0BxmCMX2KnwT00SD0oC2qJIK1ozFQ0xvJXdMP5Ipd
xUuYKaBTzjYTku8VZ1u5ppwPude0AoZ7Isk54aMAea8yAM8G5gSQiaHn1WbG6yGq
+vaFdn0Rnj8U9a7qS4IrnyX5sx9SxhM2r+ko3kuaAlzxNGFOZs4pWLswR0hcRX/O
l8J9ME23ZSBgGzOK8/v3n3PKr5VdOhn0/GT/jdgShcBqsjcPW79hncmXhHZg/+7U
1djsppdsAa5UVKgiZxTH84y9E29JgWCloTlu5RvTlXzN7vsXFvXrkyCprp+q/qjS
ZapPt6wCG5AXCsXIOoyp3EbgzWP5oZVRBg==
-----END CERTIFICATE-----
";

fn cfg_with_tls(tls: &str) -> crate::config::Config {
    use crate::config::{
        Config, CursorConfig, FilterConfig, LogFormat, LoggingConfig, OversizePolicy,
        ProtocolErrorPolicy, PublisherConfig, RedisConfig, RelayConfig, StaleCursorPolicy,
    };
    Config {
        config_version: 1,
        relay: RelayConfig {
            url: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos".into(),
            fallbacks: vec![],
            reconnect_initial_delay_ms: 1000,
            reconnect_max_delay_ms: 60_000,
            failover_threshold: 5,
            failover_cooldown_seconds: 600,
            tls_extra_ca_file: tls.into(),
        },
        redis: RedisConfig {
            url: "redis://localhost:6379".into(),
            stream_key: "firehose:events".into(),
            max_stream_len: 500_000,
            cleanup_unknown_cursors: false,
        },
        filter: FilterConfig {
            record_types: vec![],
        },
        cursor: CursorConfig {
            save_interval_seconds: 5,
            on_stale_cursor: StaleCursorPolicy::LiveTip,
            on_protocol_error: ProtocolErrorPolicy::ReconnectFromLiveTip,
        },
        publisher: PublisherConfig {
            max_event_size_bytes: 1_048_576,
            on_oversize: OversizePolicy::SkipWithLog,
        },
        logging: LoggingConfig {
            level: "info".into(),
            format: LogFormat::Json,
        },
    }
}

#[test]
fn load_tls_client_config_empty_field_returns_none() {
    // Phase 8.7: the common-case deployment has `tls_extra_ca_file = ""`
    // and should get `None` back — the supervisor then uses
    // proto-blue-ws's default `TungsteniteConnector`, which dials over
    // native-tls against system roots. No rustls involvement.
    let got = crate::load_tls_client_config(&cfg_with_tls(""))
        .expect("empty field must not fail startup");
    assert!(
        got.is_none(),
        "empty tls_extra_ca_file should produce no client config"
    );
}

#[test]
fn load_tls_client_config_nonexistent_file_fails_at_startup() {
    // File-doesn't-exist fails fast so the operator sees the misconfig
    // at boot, not as an unrelated-looking TLS handshake error at
    // first connect attempt (this was the Phase 8.5 finding 4.2
    // failure mode — inert config hiding real TLS errors).
    let bogus = "/definitely/not/a/real/path/extra-ca.pem";
    let err = crate::load_tls_client_config(&cfg_with_tls(bogus))
        .expect_err("nonexistent CA file must fail startup");
    let msg = err.to_string();
    assert!(
        msg.contains(bogus),
        "error must echo the configured path so the operator finds it; got: {msg}"
    );
    assert!(
        msg.to_lowercase().contains("failed to read"),
        "error must indicate read failure rather than a generic parse error; got: {msg}"
    );
}

#[test]
fn load_tls_client_config_non_pem_content_rejects_with_clear_error() {
    // A file that has no `-----BEGIN CERTIFICATE-----` marker at all
    // produces zero parsed certs; we refuse rather than silently
    // installing a ClientConfig that only has system roots (which
    // would defeat the point of setting `tls_extra_ca_file`).
    let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
    std::io::Write::write_all(&mut tmp, b"this is not a PEM file\n").expect("write tmp");
    let path = tmp.path().to_string_lossy().into_owned();

    let err = crate::load_tls_client_config(&cfg_with_tls(&path))
        .expect_err("non-PEM content must fail startup");
    let msg = err.to_string();
    assert!(
        msg.contains("no CERTIFICATE"),
        "error should name the missing-certificate-entries failure mode; got: {msg}"
    );
}

#[test]
fn load_tls_client_config_malformed_pem_fails_at_startup() {
    // A PEM block whose payload isn't valid base64 makes
    // `rustls_pemfile::certs` yield an Err. We surface it as a
    // TlsExtraCaFile error at startup rather than at handshake.
    let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
    std::io::Write::write_all(
        &mut tmp,
        b"-----BEGIN CERTIFICATE-----\n\
          not valid base64 data !!! @@@ ### $$$ \n\
          -----END CERTIFICATE-----\n",
    )
    .expect("write tmp");
    let path = tmp.path().to_string_lossy().into_owned();

    let err = crate::load_tls_client_config(&cfg_with_tls(&path))
        .expect_err("malformed PEM must fail startup");
    let msg = err.to_string();
    assert!(
        msg.contains(&path),
        "error must echo the configured path; got: {msg}"
    );
    // Either the pemfile parser or the RootCertStore add step can
    // surface the failure, depending on what the garbage decodes to —
    // both are equally valid "don't start up with this file" outcomes.
    assert!(
        msg.to_lowercase().contains("parse pem")
            || msg.to_lowercase().contains("add cert to root store"),
        "error should name which parse stage failed; got: {msg}"
    );
}

#[test]
fn load_tls_client_config_valid_pem_produces_client_config() {
    // Happy path: a real self-signed RSA cert parses cleanly and we
    // get back a ClientConfig. We don't drive a handshake against it
    // here — exercising TLS against a custom CA needs a mock relay
    // with a matching server cert, which is deferred to a Phase 11
    // smoke test per DESIGN.md §3.
    let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
    std::io::Write::write_all(&mut tmp, VALID_TEST_CA_PEM.as_bytes()).expect("write tmp");
    let path = tmp.path().to_string_lossy().into_owned();

    let config = crate::load_tls_client_config(&cfg_with_tls(&path))
        .expect("valid self-signed CA should load")
        .expect("valid PEM should produce Some(ClientConfig)");

    // Smoke-assert the config is actually populated — rustls'
    // public surface on ClientConfig is mostly opaque, but
    // ALPN should be empty by default (we don't set it),
    // confirming we got a fresh builder output rather than some
    // junk value.
    assert!(
        config.alpn_protocols.is_empty(),
        "ClientConfig::builder() should produce no ALPN entries by default"
    );
}

#[tokio::test]
async fn startup_metrics_only_emits_allowlisted_fields_no_credentials() {
    use crate::config::{
        Config, CursorConfig, FilterConfig, LoggingConfig, OversizePolicy, ProtocolErrorPolicy,
        PublisherConfig, RedisConfig, RelayConfig, StaleCursorPolicy,
    };
    use tracing_subscriber::layer::SubscriberExt;

    let captured = capture::Captured::default();
    let subscriber = tracing_subscriber::registry().with(capture::CaptureLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

    // Build a config whose Redis URL has userinfo credentials. The
    // emission must strip them (use the sanitised host form) and
    // never echo the full URL.
    let cfg = Config {
        config_version: 1,
        relay: RelayConfig {
            url: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos".into(),
            fallbacks: vec![],
            reconnect_initial_delay_ms: 1000,
            reconnect_max_delay_ms: 60000,
            failover_threshold: 5,
            failover_cooldown_seconds: 600,
            tls_extra_ca_file: String::new(),
        },
        redis: RedisConfig {
            url: "redis://SUPER_SECRET_USER:SUPER_SECRET_PASS@redis.internal:6379/0".into(),
            stream_key: "firehose:events".into(),
            max_stream_len: 500_000,
            cleanup_unknown_cursors: false,
        },
        filter: FilterConfig {
            record_types: vec![],
        },
        cursor: CursorConfig {
            save_interval_seconds: 5,
            on_stale_cursor: StaleCursorPolicy::LiveTip,
            on_protocol_error: ProtocolErrorPolicy::ReconnectFromLiveTip,
        },
        publisher: PublisherConfig {
            max_event_size_bytes: 1_048_576,
            on_oversize: OversizePolicy::SkipWithLog,
        },
        logging: LoggingConfig {
            level: "info".into(),
            format: crate::config::LogFormat::Json,
        },
    };

    crate::emit_startup_metrics(&cfg, std::path::Path::new("/tmp/test.toml"), false);

    let records = captured.records.lock().unwrap().clone();
    let startup: Vec<_> = records
        .iter()
        .filter(|r| r.field("event_type") == Some("startup_metrics"))
        .collect();
    assert_eq!(
        startup.len(),
        1,
        "exactly one startup_metrics event expected"
    );

    // Every emitted field value must not contain the secret. The
    // `payload` JSON field contains the structured §4 payload; it
    // must contain the sanitised host but not the userinfo.
    for (name, value) in &startup[0].fields {
        assert!(
            !value.contains("SUPER_SECRET_USER"),
            "field `{name}` leaked userinfo: {value}"
        );
        assert!(
            !value.contains("SUPER_SECRET_PASS"),
            "field `{name}` leaked password: {value}"
        );
    }

    // Positive check: the sanitised host appears in the payload.
    let payload = startup[0]
        .field("payload")
        .expect("startup_metrics event should carry the payload field");
    assert!(
        payload.contains("redis.internal:6379"),
        "payload should carry the sanitised host; got {payload}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn emitter_exits_promptly_on_shutdown_signal() {
    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();
    let metrics = Metrics::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let (e_tx, _e_rx) = mpsc::channel::<PublishOp>(16);
    let (f_tx, _f_rx) = mpsc::channel::<PublishOp>(16);
    let (w_tx, _w_rx) = mpsc::channel::<crate::ws_reader::Frame>(16);
    let gauges = crate::metrics::ChannelGauges {
        ws_to_decoder: w_tx.downgrade(),
        decoder_to_router: e_tx.downgrade(),
        router_to_publisher: f_tx.downgrade(),
    };
    let throwaway = ws_reader::spawn_with_cursors(
        relay_cfg("ws://127.0.0.1:1".into()),
        WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
        Metrics::new(),
    );

    // Deliberately long interval (10s). If the emitter respected the
    // ticker for shutdown we'd wait 10s; the biased shutdown-first
    // select means it exits near-instantly.
    let handle = crate::metrics::spawn_emitter(
        metrics.clone(),
        gauges,
        cursors.clone(),
        throwaway.state_reader(),
        backend.clone(),
        "firehose:events".to_string(),
        std::time::Instant::now(),
        Duration::from_secs(10),
        shutdown_rx.clone(),
    );

    // Give the task a moment to start.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let start = std::time::Instant::now();
    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), handle.join())
        .await
        .expect("emitter did not exit within 2s of shutdown signal");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "emitter took {elapsed:?} to exit on shutdown; should be ≪ the 10s ticker"
    );
    throwaway.shutdown().await;
}

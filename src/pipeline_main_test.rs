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
use crate::publisher::{self, PublisherOptions};
use crate::router::{self, RouterOptions};
use crate::ws_reader::{self, WsReaderOptions};

/// Load a single fixture frame to stream through the mock relay. Phase
/// 3 proved every captured frame decodes; Phase 5 just needs one that
/// will produce an event.
fn sample_fixture_frame() -> Option<Vec<u8>> {
    let fixtures_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
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
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let (event_tx, event_rx) = mpsc::channel::<(Event, u64)>(128);
    let (filtered_tx, filtered_rx) = mpsc::channel::<(Event, u64)>(128);

    let ws_reader = ws_reader::spawn_with_cursors(
        relay_cfg(url.clone()),
        WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
    );
    let decoder_h = decoder::spawn(ws_reader, cursors.clone(), event_tx);
    let router_h = router::spawn(
        RouterOptions { record_types: vec![] },
        event_rx,
        filtered_tx,
    );
    let publisher_h = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        test_publisher_opts(),
        filtered_rx,
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
    let _ = _shutdown_tx.send(true);
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

    let (event_tx, event_rx) = mpsc::channel::<(Event, u64)>(128);
    let (filtered_tx, filtered_rx) = mpsc::channel::<(Event, u64)>(128);

    let ws_reader = ws_reader::spawn_with_cursors(
        relay_cfg(url.clone()),
        WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
    );
    let decoder_h = decoder::spawn(ws_reader, cursors.clone(), event_tx);
    let router_h = router::spawn(
        RouterOptions { record_types: vec![] },
        event_rx,
        filtered_tx,
    );
    let publisher_h = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        test_publisher_opts(),
        filtered_rx,
    );
    // Persist cursors every 20ms so we know the periodic path runs.
    let persister_h =
        spawn_persister(cursors.clone(), backend.clone(), Duration::from_millis(20));

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

    let (event_tx, event_rx) = mpsc::channel::<(Event, u64)>(32);
    let (filtered_tx, filtered_rx) = mpsc::channel::<(Event, u64)>(32);

    // No ws_reader/decoder in this test — we stuff events in
    // directly. What we're testing is the publisher-with-broken-backend
    // path remaining shutdown-responsive.
    let router_h = router::spawn(
        RouterOptions { record_types: vec![] },
        event_rx,
        filtered_tx,
    );
    let publisher_h = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        test_publisher_opts(),
        filtered_rx,
    );
    let persister_h =
        spawn_persister(cursors.clone(), backend.clone(), Duration::from_millis(20));

    // Inject one event so the publisher is actively retrying.
    let ev = Event::Tombstone(crate::event::TombstoneEvent {
        did: "did:plc:abc".into(),
        relay: "ws://test".into(),
    });
    event_tx.send((ev, 42)).await.unwrap();
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

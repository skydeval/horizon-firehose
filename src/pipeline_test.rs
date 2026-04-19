//! End-to-end integration test for the Phase 4 pipeline.
//!
//! Wires the real decoder + router + publisher + cursor persister
//! together against an [`InMemoryBackend`] and feeds captured WebSocket
//! fixtures through the whole thing. Asserts on the post-run state:
//!
//! - every `DecodedFrame::Event` that survived the filter appears in
//!   the Redis stream once,
//! - the in-memory cursor advanced to the highest published seq per
//!   relay,
//! - the persister wrote that cursor to Redis under the correct
//!   base64url key,
//! - MAXLEN trimming keeps the stream bounded when configured that way,
//! - oversize events respect the configured policy.
//!
//! Phase 5 wires the same modules together in `main.rs` for the real
//! runtime; this test exists so the wiring topology is validated
//! *before* Phase 5 starts moving pieces into the binary.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::backend::{InMemoryBackend, StreamBackend};
use crate::config::OversizePolicy;
use crate::cursor::{Cursors, cursor_key, spawn_persister};
use crate::decoder::{DecodedFrame, decode_frame};
use crate::event::Event;
use crate::publisher::{self, PublisherOptions};
use crate::router::{self, RouterOptions};

const RELAY: &str = "wss://relay.test/xrpc/com.atproto.sync.subscribeRepos";

/// Read the first `limit` fixture frames from `tests/fixtures/`.
///
/// Returns `None` if the corpus is missing (so CI environments without
/// it can skip cleanly rather than fail).
fn load_fixture_frames(limit: usize) -> Option<Vec<Vec<u8>>> {
    let fixtures_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let manifest_path = fixtures_dir.join("manifest.json");
    if !manifest_path.exists() {
        return None;
    }
    let manifest_str = std::fs::read_to_string(&manifest_path).ok()?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest_str).ok()?;
    let frames = manifest["frames"].as_array()?;
    let mut out = Vec::new();
    for f in frames.iter().take(limit) {
        let name = f["filename"].as_str()?;
        if let Ok(bytes) = std::fs::read(fixtures_dir.join(name)) {
            out.push(bytes);
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

/// Decode frames into `(Event, seq)` pairs. Frames that don't decode
/// cleanly or that the decoder classifies as non-republishable (`Info`,
/// `Skipped`, `OutdatedCursor`) are excluded, matching what Phase 5's
/// supervisor will feed into the router.
fn decode_all(frames: &[Vec<u8>]) -> Vec<(Event, u64)> {
    let mut out = Vec::new();
    for bytes in frames {
        if let Ok(DecodedFrame::Event { event, seq }) = decode_frame(bytes, RELAY) {
            out.push((event, seq));
        }
    }
    out
}

#[tokio::test]
async fn pipeline_publishes_every_fixture_event_and_advances_cursor() {
    let Some(frames) = load_fixture_frames(500) else {
        eprintln!("SKIP pipeline_test: no fixtures (run capture-fixtures)");
        return;
    };
    let decoded = decode_all(&frames);
    assert!(
        !decoded.is_empty(),
        "fixtures produced zero decodable events — suspicious"
    );
    let max_seq = decoded.iter().map(|(_, s)| *s).max().unwrap();

    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();

    // Set up the channels: decoder-out → router-in → publisher-in.
    let (router_in_tx, router_in_rx) = mpsc::channel::<(Event, u64)>(1024);
    let (pub_in_tx, pub_in_rx) = mpsc::channel::<(Event, u64)>(1024);

    // Empty filter → pass everything.
    let router_handle = router::spawn(
        RouterOptions { record_types: vec![] },
        router_in_rx,
        pub_in_tx,
    );
    let publisher_handle = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        PublisherOptions::with_defaults(
            "firehose:events",
            10_000,
            1 << 20,
            OversizePolicy::SkipWithLog,
        ),
        pub_in_rx,
    );

    // Feed decoded events into the pipeline.
    for pair in decoded.iter().cloned() {
        router_in_tx.send(pair).await.unwrap();
    }
    drop(router_in_tx);

    let router_stats = router_handle.shutdown().await;
    let pub_stats = publisher_handle.join().await.expect("publisher ok");

    assert_eq!(router_stats.events_in as usize, decoded.len());
    assert_eq!(router_stats.events_out as usize, decoded.len());
    assert_eq!(pub_stats.events_published as usize, decoded.len());

    assert_eq!(cursors.get(RELAY).await, Some(max_seq));
    assert_eq!(
        backend.xlen("firehose:events").await.unwrap() as usize,
        decoded.len(),
        "every decoded event should appear in the stream once"
    );
}

#[tokio::test]
async fn pipeline_with_filter_drops_non_matching_commits() {
    let Some(frames) = load_fixture_frames(500) else {
        eprintln!("SKIP pipeline_test (filter): no fixtures");
        return;
    };
    let decoded = decode_all(&frames);
    assert!(!decoded.is_empty());

    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();

    let (router_in_tx, router_in_rx) = mpsc::channel::<(Event, u64)>(1024);
    let (pub_in_tx, pub_in_rx) = mpsc::channel::<(Event, u64)>(1024);

    // Only pass app.bsky.feed.post. Commits without posts must be
    // dropped; identity/account/handle/tombstone must still pass.
    let router_handle = router::spawn(
        RouterOptions {
            record_types: vec!["app.bsky.feed.post".into()],
        },
        router_in_rx,
        pub_in_tx,
    );
    let publisher_handle = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        PublisherOptions::with_defaults(
            "firehose:events",
            10_000,
            1 << 20,
            OversizePolicy::SkipWithLog,
        ),
        pub_in_rx,
    );

    for pair in decoded.iter().cloned() {
        router_in_tx.send(pair).await.unwrap();
    }
    drop(router_in_tx);

    let router_stats = router_handle.shutdown().await;
    let pub_stats = publisher_handle.join().await.expect("publisher ok");

    assert_eq!(router_stats.events_in as usize, decoded.len());
    // Some non-matching events should be dropped.
    assert!(
        router_stats.events_dropped > 0,
        "expected filter to drop some events; in={} out={} dropped={}",
        router_stats.events_in,
        router_stats.events_out,
        router_stats.events_dropped
    );
    assert_eq!(router_stats.events_out, pub_stats.events_published);

    // Identity/account/handle/tombstone events should all have made
    // it through regardless of the filter.
    let non_commit_in: u64 = decoded
        .iter()
        .filter(|(e, _)| !matches!(e, Event::Commit(_)))
        .count() as u64;
    assert!(
        pub_stats.events_published >= non_commit_in,
        "non-commit events must never be filtered out; published={} non_commits={}",
        pub_stats.events_published,
        non_commit_in
    );
}

#[tokio::test]
async fn pipeline_maxlen_trims_but_cursor_reflects_all_publishes() {
    let Some(frames) = load_fixture_frames(200) else {
        eprintln!("SKIP pipeline_test (maxlen): no fixtures");
        return;
    };
    let decoded = decode_all(&frames);
    assert!(decoded.len() >= 10, "need at least 10 decodable events");
    let max_seq = decoded.iter().map(|(_, s)| *s).max().unwrap();

    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();
    const TRIM_LEN: u64 = 5;

    let (router_in_tx, router_in_rx) = mpsc::channel::<(Event, u64)>(1024);
    let (pub_in_tx, pub_in_rx) = mpsc::channel::<(Event, u64)>(1024);

    let router_handle = router::spawn(
        RouterOptions { record_types: vec![] },
        router_in_rx,
        pub_in_tx,
    );
    let publisher_handle = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        PublisherOptions::with_defaults(
            "firehose:events",
            TRIM_LEN,
            1 << 20,
            OversizePolicy::SkipWithLog,
        ),
        pub_in_rx,
    );

    for pair in decoded.iter().cloned() {
        router_in_tx.send(pair).await.unwrap();
    }
    drop(router_in_tx);

    let _ = router_handle.shutdown().await;
    let stats = publisher_handle.join().await.unwrap();
    assert_eq!(stats.events_published as usize, decoded.len());
    assert_eq!(
        backend.xlen("firehose:events").await.unwrap(),
        TRIM_LEN,
        "MAXLEN should bound the stream"
    );
    // Cursor advancement is *not* limited by trimming — it reflects
    // every XADD success, even ones whose entries got trimmed away.
    assert_eq!(cursors.get(RELAY).await, Some(max_seq));
}

#[tokio::test]
async fn cursor_persister_writes_under_base64url_key() {
    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();
    cursors.advance(RELAY, 4242).await;

    let persister = spawn_persister(cursors.clone(), backend.clone(), Duration::from_millis(20));
    // Let the persister tick a few times.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let stats = persister.shutdown().await;
    assert!(stats.ticks >= 2, "expected ≥2 ticks, got {}", stats.ticks);

    let key = cursor_key(RELAY);
    assert_eq!(backend.get_cursor(&key).await.unwrap(), Some(4242));

    // Round-trip via load_initial: a fresh Cursors should hydrate.
    let fresh = Cursors::new();
    fresh.load_initial(backend.as_ref(), &[RELAY.to_string()])
        .await
        .unwrap();
    assert_eq!(fresh.get(RELAY).await, Some(4242));
}

#[tokio::test]
async fn pipeline_recovers_from_transient_redis_outage_without_losing_events() {
    let Some(frames) = load_fixture_frames(20) else {
        eprintln!("SKIP pipeline_test (outage): no fixtures");
        return;
    };
    let decoded = decode_all(&frames);
    assert!(!decoded.is_empty());
    let max_seq = decoded.iter().map(|(_, s)| *s).max().unwrap();

    let backend = Arc::new(InMemoryBackend::new());
    let cursors = Cursors::new();

    // Inject several failures covering the first few XADDs. Retry
    // should ride through them without losing events or advancing the
    // cursor past a non-XADDed one.
    backend.set_fail_mode(crate::backend::FailMode::FailNext(4)).await;

    let (router_in_tx, router_in_rx) = mpsc::channel::<(Event, u64)>(1024);
    let (pub_in_tx, pub_in_rx) = mpsc::channel::<(Event, u64)>(1024);

    let router_handle = router::spawn(
        RouterOptions { record_types: vec![] },
        router_in_rx,
        pub_in_tx,
    );
    let publisher_handle = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        PublisherOptions {
            retry_initial: Duration::from_millis(5),
            retry_max: Duration::from_millis(20),
            ..PublisherOptions::with_defaults(
                "firehose:events",
                10_000,
                1 << 20,
                OversizePolicy::SkipWithLog,
            )
        },
        pub_in_rx,
    );

    for pair in decoded.iter().cloned() {
        router_in_tx.send(pair).await.unwrap();
    }
    drop(router_in_tx);

    let _ = router_handle.shutdown().await;
    let stats = publisher_handle.join().await.unwrap();

    assert!(stats.redis_errors >= 4, "expected ≥4 errors, got {}", stats.redis_errors);
    assert_eq!(stats.events_published as usize, decoded.len());
    assert_eq!(cursors.get(RELAY).await, Some(max_seq));
    assert_eq!(
        backend.xlen("firehose:events").await.unwrap() as usize,
        decoded.len(),
        "no events should be lost across the transient outage"
    );
}

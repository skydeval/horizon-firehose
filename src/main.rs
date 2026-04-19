//! horizon-firehose entry point.
//!
// The binary build never exercises the fake backend, the internal
// `ws_reader::spawn` variant, or the publisher's mid-retry
// `shutdown()` path — those are for unit tests. Suppressing
// dead_code only in the non-test build keeps those items alive for
// their test users without masking genuinely dead items in CI
// (where tests run).
#![cfg_attr(not(test), allow(dead_code))]

//! Phase 5 scope (per DESIGN.md §6): wire every module into a running
//! pipeline, coordinate DESIGN.md §3 shutdown ordering, detect task
//! panics, fail open on transient Redis outages at startup. What the
//! binary does now:
//!
//! 1. Load + validate config, initialise tracing, emit
//!    `startup_metrics`.
//! 2. Install a global shutdown watch driven by SIGINT/SIGTERM.
//! 3. Connect to Redis with indefinite exponential backoff, aborting
//!    cleanly if a shutdown signal fires during startup.
//! 4. Load any previously-persisted cursors so failover can resume
//!    from our last XADD, not live tip.
//! 5. Spawn five tasks — ws_reader, decoder, router, publisher,
//!    cursor persister — wired via bounded mpsc channels.
//! 6. Block until *either* a shutdown signal *or* a task exits
//!    unexpectedly (panic / unrecoverable error).
//! 7. Drive the §3 shutdown cascade: flip the global signal, await
//!    each task in order. Budget: 30 seconds total; force-exit
//!    non-zero on overrun.
//! 8. Flush cursors one last time so the final persisted value
//!    reflects every XADDed event (required by §3 ordering).
//! 9. Emit `shutdown_metrics`, exit zero on clean shutdown, non-zero
//!    on panic or budget overrun.

mod backend;
mod config;
mod cursor;
mod decoder;
mod error;
mod event;
mod metrics;
mod publisher;
mod router;
mod ws_reader;
#[cfg(test)]
mod pipeline_test;
#[cfg(test)]
mod pipeline_main_test;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::backend::{RedisBackend, StreamBackend};
use crate::config::{Config, LogFormat};
use crate::cursor::Cursors;
use crate::error::Error;

/// Compile-time consumer version, surfaced in `startup_metrics`.
const CONSUMER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// DESIGN.md §3 shutdown budget.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

/// Default channel buffer between pipeline tasks. Tunable later.
const CHANNEL_BUFFER: usize = 1024;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // Tracing may not be initialised yet (config could fail
            // before we set up subscribers), so emit on stderr too.
            eprintln!("fatal: {err}");
            error!(error = %err, "horizon-firehose exiting non-zero");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Error> {
    let path = Config::resolve_path();
    let cfg = Config::load(&path)?;
    validate_tls_extra_ca(&cfg)?;

    init_tracing(&cfg);
    emit_startup_metrics(&cfg, &path);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(run_async(cfg))
}

fn init_tracing(cfg: &Config) {
    let filter = EnvFilter::try_new(&cfg.logging.level)
        .unwrap_or_else(|_| EnvFilter::new("info"));

    match cfg.logging.format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .with_current_span(false)
                .with_span_list(false)
                .init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }
}

/// Check that `relay.tls_extra_ca_file` (if set) points at a readable
/// file before we start long-running work. Fails fast on typos.
///
/// **Limitation**: proto-blue-ws uses `tokio_tungstenite::connect_async`
/// under the hood, which takes no `ClientConfig`, so this CA bundle
/// is not currently plumbed into the actual TLS handshake. Phase 5
/// validates the file so operators know the path is wrong *at
/// startup* rather than discovering it only after proto-blue-ws
/// grows a `connect_async_tls_with_config` path. See DESIGN.md §3
/// "TLS verification" — the additive-CA config is accepted but
/// inert until upstream exposes the config hook.
///
/// See upstream feature request at
/// <https://github.com/dollspace-gay/proto-blue/issues/4> for
/// tracking. When proto-blue-ws exposes a rustls `ClientConfig`
/// hook, this validation step becomes the first half of actual CA
/// injection (load + parse the PEM → install as additive roots on
/// the `ClientConfig` passed to `connect_async_tls_with_config`).
fn validate_tls_extra_ca(cfg: &Config) -> Result<(), Error> {
    let path = cfg.relay.tls_extra_ca_file.trim();
    if path.is_empty() {
        return Ok(());
    }
    let p = std::path::PathBuf::from(path);
    match std::fs::metadata(&p) {
        Ok(_) => {
            warn!(
                path = %p.display(),
                "relay.tls_extra_ca_file is set but not plumbed through to proto-blue-ws; \
                 file is validated for existence, not loaded into the TLS stack yet"
            );
            Ok(())
        }
        Err(source) => Err(Error::TlsExtraCaFile { path: p, source }),
    }
}

pub(crate) fn emit_startup_metrics(cfg: &Config, path: &std::path::Path) {
    let payload = json!({
        "type": "startup_metrics",
        "config_version": cfg.config_version,
        "consumer_version": CONSUMER_VERSION,
        "relay_primary": cfg.relay.url,
        "relay_fallbacks": cfg.relay.fallbacks,
        "redis_url_host": cfg.redis_url_host(),
        "max_stream_len": cfg.redis.max_stream_len,
        "filter_nsid_count": cfg.filter.record_types.len(),
    });

    info!(
        target: "horizon_firehose::metrics",
        event_type = "startup_metrics",
        config_path = %path.display(),
        payload = %payload,
        "horizon-firehose starting"
    );
}

async fn run_async(cfg: Config) -> Result<(), Error> {
    let started_at = Instant::now();

    // Global shutdown signal: one watch, cloned everywhere.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(listen_for_signals(shutdown_tx.clone()));

    // Redis connect with indefinite retry. If a signal fires first,
    // we unwind cleanly without ever having started the pipeline.
    let backend = match RedisBackend::connect_with_retry(
        &cfg.redis.url,
        Duration::from_millis(500),
        Duration::from_secs(30),
        Duration::from_secs(30),
        shutdown_rx.clone(),
    )
    .await
    {
        Ok(Some(b)) => Arc::new(b),
        Ok(None) => {
            info!("shutdown signal received during Redis startup retry; exiting clean");
            return Ok(());
        }
        Err(err) => {
            return Err(Error::TaskFailure {
                task: "redis_startup",
                message: err.to_string(),
            });
        }
    };

    // Load persisted cursors so each relay resumes where we left off.
    // Best-effort: a transient Redis hiccup here logs WARN but doesn't
    // block startup — we'd just resume from live tip, which is the
    // documented §3 behaviour for "no prior cursor".
    let all_relays: Vec<String> = std::iter::once(cfg.relay.url.clone())
        .chain(cfg.relay.fallbacks.iter().cloned())
        .collect();
    let cursors = Cursors::new();
    if let Err(err) = cursors.load_initial(backend.as_ref(), &all_relays).await {
        warn!(error = %err, "failed to load cursors at startup; resuming from live tip");
    }

    // Shared metrics. Every task that produces counters holds an
    // `Arc<Metrics>`; the periodic emitter samples them on a ticker
    // and diffs against the previous sample to produce the
    // `*_in_window` fields from DESIGN.md §4.
    let metrics = metrics::Metrics::new();

    // Channels between pipeline stages. All bounded — backpressure
    // cascades upstream to the relay if a downstream stage stalls.
    let (event_tx, event_rx) = mpsc::channel::<(event::Event, u64)>(CHANNEL_BUFFER);
    let (filtered_tx, filtered_rx) = mpsc::channel::<(event::Event, u64)>(CHANNEL_BUFFER);
    // Downgrade the producer sides so the metrics emitter can sample
    // channel depth without keeping the channel alive past the
    // producer's exit — see metrics::ChannelGauges docs for why.
    let decoder_to_router_weak = event_tx.downgrade();
    let router_to_publisher_weak = filtered_tx.downgrade();

    // Spawn the pipeline. Order matters only so the channels line
    // up; the tasks all run concurrently.
    let ws_reader = ws_reader::spawn_with_cursors(
        cfg.relay.clone(),
        ws_reader::WsReaderOptions::default(),
        cursors.clone(),
        shutdown_rx.clone(),
        metrics.clone(),
    );
    // Capture the ws → decoder WeakSender and the state reader
    // *before* `ws_reader` is moved into `decoder::spawn`.
    let ws_to_decoder_weak = ws_reader.frames_weak();
    let ws_state = ws_reader.state_reader();

    let mut decoder_handle =
        decoder::spawn(ws_reader, cursors.clone(), event_tx, metrics.clone());

    let mut router_handle = router::spawn(
        router::RouterOptions {
            record_types: cfg.filter.record_types.clone(),
        },
        event_rx,
        filtered_tx,
    );

    let mut publisher_handle = publisher::spawn(
        backend.clone(),
        cursors.clone(),
        publisher::PublisherOptions::with_defaults(
            cfg.redis.stream_key.clone(),
            cfg.redis.max_stream_len,
            cfg.publisher.max_event_size_bytes,
            cfg.publisher.on_oversize,
        ),
        filtered_rx,
        metrics.clone(),
    );

    let mut persister_handle = cursor::spawn_persister(
        cursors.clone(),
        backend.clone(),
        Duration::from_secs(cfg.cursor.save_interval_seconds),
    );

    let metrics_emitter = metrics::spawn_emitter(
        metrics.clone(),
        metrics::ChannelGauges {
            ws_to_decoder: ws_to_decoder_weak,
            decoder_to_router: decoder_to_router_weak,
            router_to_publisher: router_to_publisher_weak,
        },
        cursors.clone(),
        ws_state,
        backend.clone(),
        cfg.redis.stream_key.clone(),
        started_at,
        Duration::from_secs(10),
        shutdown_rx.clone(),
    );

    info!(
        target: "horizon_firehose::metrics",
        event_type = "pipeline_started",
        relay_primary = %cfg.relay.url,
        channel_buffer = CHANNEL_BUFFER,
        "pipeline running"
    );

    // Wait for the first of: explicit shutdown signal, OR any pipeline
    // task exiting unexpectedly (panic / unrecoverable error / its
    // upstream channel vanishing).
    //
    // `JoinHandle<T>` is `Unpin`, so `&mut handle` is itself a
    // `Future`. Polling it in `select!` detects task completion —
    // normal exit *or* panic — without consuming the handle, so we
    // can still call `join()` on it in the cascade below to collect
    // each task's stats. Do **not** "simplify" by moving the handle
    // into the select arm: that would drop the handle after the first
    // poll and lose our ability to (a) distinguish panic vs clean
    // exit via `JoinError` in the error path, and (b) surface
    // per-task stats in the final `shutdown_metrics` event.
    let mut shutdown_rx_ev = shutdown_rx.clone();
    let exit_reason: ExitReason = tokio::select! {
        _ = shutdown_rx_ev.changed() => ExitReason::Signal,
        r = &mut decoder_handle.task => ExitReason::TaskEnded {
            name: "decoder",
            panicked: r.is_err(),
        },
        r = &mut router_handle.task => ExitReason::TaskEnded {
            name: "router",
            panicked: r.is_err(),
        },
        r = &mut publisher_handle.task => ExitReason::TaskEnded {
            name: "publisher",
            panicked: r.is_err(),
        },
        r = &mut persister_handle.task => ExitReason::TaskEnded {
            name: "cursor_persister",
            panicked: r.is_err(),
        },
    };

    let fault = match &exit_reason {
        ExitReason::Signal => None,
        ExitReason::TaskEnded { name, panicked } => {
            if *panicked {
                error!(task = name, "pipeline task panicked — initiating shutdown");
            } else {
                error!(task = name, "pipeline task exited unexpectedly — initiating shutdown");
            }
            Some(*name)
        }
    };

    // Flip the global shutdown so ws_reader bails immediately. The
    // rest of the cascade unfolds through channel closes. The
    // metrics emitter also observes this watch and exits.
    let _ = shutdown_tx.send(true);

    // Drive the §3 cascade under a 30s total budget. If we overrun,
    // force-exit non-zero — cursor accuracy is best-effort on overrun
    // but the orchestrator shouldn't hang.
    let shutdown_outcome = tokio::time::timeout(
        SHUTDOWN_BUDGET,
        shutdown_cascade(
            decoder_handle,
            router_handle,
            publisher_handle,
            persister_handle,
            metrics_emitter,
            cursors.clone(),
            backend.clone(),
        ),
    )
    .await;

    let shutdown_result = match shutdown_outcome {
        Ok(result) => result,
        Err(_timeout) => {
            error!(
                target: "horizon_firehose::metrics",
                event_type = "shutdown_budget_exceeded",
                budget_secs = SHUTDOWN_BUDGET.as_secs(),
                "graceful shutdown did not complete in budget; forcing exit"
            );
            return Err(Error::ShutdownBudgetExceeded);
        }
    };

    // `shutdown_metrics` payload per DESIGN.md §4. Totals come from
    // the same atomic counters the periodic emitter samples, so there
    // is exactly one source of truth for "how many events did we
    // publish this run".
    let totals = metrics.shutdown_totals();
    let final_cursor_per_relay: std::collections::HashMap<String, u64> =
        shutdown_result.final_cursors.iter().cloned().collect();
    info!(
        target: "horizon_firehose::metrics",
        event_type = "shutdown_metrics",
        uptime_seconds = started_at.elapsed().as_secs(),
        exit_reason = ?exit_reason,
        total_events_published = totals.total_events_published,
        total_decode_errors = totals.total_decode_errors,
        total_redis_errors = totals.total_redis_errors,
        total_reconnects = totals.total_reconnects,
        total_skipped_frames = totals.total_skipped_frames,
        total_oversize_events = totals.total_oversize_events,
        total_unknown_frame_types = totals.total_unknown_frame_types,
        final_cursor_per_relay = %serde_json::to_string(&final_cursor_per_relay).unwrap_or_default(),
        "shutdown complete"
    );

    // A task fault → non-zero exit so the orchestrator restarts us.
    if let Some(task) = fault {
        return Err(Error::TaskFailure {
            task,
            message: format!("task '{task}' exited before shutdown signal"),
        });
    }
    // A publisher `fail_hard` oversize error also counts as non-zero.
    if let Err(err) = shutdown_result.publisher {
        return Err(Error::TaskFailure {
            task: "publisher",
            message: err.to_string(),
        });
    }
    Ok(())
}

/// Aggregated shutdown result passed back to `run_async`. Only the
/// publisher's Result and the final cursor snapshot are surfaced —
/// the cumulative stats for decoder/router/publisher/persister live
/// on the shared [`metrics::Metrics`] and are read via
/// `metrics.shutdown_totals()` for the `shutdown_metrics` event.
#[derive(Debug)]
struct ShutdownResult {
    publisher: Result<publisher::PublisherStats, publisher::PublisherError>,
    final_cursors: Vec<(String, u64)>,
}

/// Drive the §3 cascade: drain each stage before draining the next.
/// Tasks have drain-first `biased;` selects, so signalling the global
/// watch doesn't stop them from publishing buffered events — channel
/// close is what drives exit, and channel close only happens when the
/// previous stage has finished draining. Exactly the §3 ordering.
#[allow(clippy::too_many_arguments)]
async fn shutdown_cascade<B: StreamBackend>(
    decoder_h: decoder::DecoderHandle,
    router_h: router::RouterHandle,
    publisher_h: publisher::PublisherHandle,
    persister_h: cursor::PersisterHandle,
    metrics_emitter_h: metrics::EmitterHandle,
    cursors: Cursors,
    backend: Arc<B>,
) -> ShutdownResult {
    // Decoder sees the ws_reader's frames channel close (ws_reader
    // bailed on shutdown signal), drains any buffered frames, closes
    // its event channel downstream.
    let _decoder_stats = decoder_h.join().await;
    // Router sees the event channel close, drains, closes filtered
    // channel.
    let _router_stats = router_h.shutdown().await;
    // Publisher sees the filtered channel close, drains (with Redis
    // retry as needed), closes. join() (not shutdown()) because we
    // want to let mid-flight retries resolve rather than abort them.
    let publisher_result = publisher_h.join().await;

    // One last cursor flush AFTER the publisher has drained — this
    // is the final "last XADDed seq" write required by §3. Doing it
    // here means the persister's periodic ticks can have missed the
    // last few advances without risk.
    let outcome = cursors.persist_all(backend.as_ref()).await;
    if outcome.failed > 0 {
        warn!(
            written = outcome.written,
            failed = outcome.failed,
            "final cursor flush had failures; proceeding to exit"
        );
    }
    let final_cursors = cursors.snapshot().await;

    // Stop the periodic persister now that we've done the final write.
    let _persister_stats = persister_h.shutdown().await;

    // Metrics emitter is *not* in the pipeline cascade — it observes
    // the global shutdown watch directly. By the time we get here
    // it's already exited (the `select!` above flipped the watch).
    // Just join so we don't leave a task orphaned.
    metrics_emitter_h.join().await;

    ShutdownResult {
        publisher: publisher_result,
        final_cursors,
    }
}

#[derive(Debug)]
enum ExitReason {
    Signal,
    TaskEnded {
        name: &'static str,
        panicked: bool,
    },
}

/// Listen for SIGINT/SIGTERM and flip the shared shutdown watch.
async fn listen_for_signals(tx: watch::Sender<bool>) {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!(error = %err, "failed to listen for ctrl-c");
        }
    };

    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(err) => {
                error!(error = %err, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!(signal = "SIGINT", "shutdown_initiated"),
        _ = term   => info!(signal = "SIGTERM", "shutdown_initiated"),
    }
    let _ = tx.send(true);
}

//! horizon-firehose entry point.
//!
//! Phase 1 scope (per DESIGN.md §6): load config, initialise tracing,
//! emit `startup_metrics`, wait for SIGINT/SIGTERM, emit a shutdown
//! event, exit zero. Subsequent phases bolt the actual pipeline tasks
//! (ws reader, decoder, publisher, cursor persister) onto this scaffold.

// Phase 1 only wires a subset of the config + error surface into `main`;
// the rest is consumed by later-phase modules. Anything still unused at
// the v1 cut should be deleted, not annotated.
//
// TODO: remove these `#[allow(dead_code)]` once the rustc ICE below is
// fixed *and* every field has a real consumer.
//
// rustc 1.95.0 ICEs while emitting a dead_code warning for these
// modules — `StyledBuffer::replace` in annotate-snippets panics with
// "slice index starts at N but ends at M" when --diagnostic-width is
// auto-detected as 0 (which cargo does under non-TTY invocations like
// WSL-from-Windows-bash). See:
//   https://github.com/rust-lang/rust/issues/154258
//   https://github.com/rust-lang/rust/pull/154914 (fix; not in 1.95.0)
#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod error;

use std::process::ExitCode;

use serde_json::json;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::config::{Config, LogFormat};
use crate::error::Error;

/// Compile-time consumer version, surfaced in `startup_metrics`.
const CONSUMER_VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // Tracing may not be initialised yet (config could fail before
            // we set up subscribers), so emit on stderr too.
            eprintln!("fatal: {err}");
            error!(error = %err, "horizon-firehose exiting non-zero");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Error> {
    let path = Config::resolve_path();
    let cfg = Config::load(&path)?;

    init_tracing(&cfg);
    emit_startup_metrics(&cfg, &path);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(idle_until_shutdown());

    info!(
        target: "horizon_firehose::metrics",
        event_type = "shutdown_metrics",
        uptime_seconds = 0,
        "shutdown complete"
    );
    Ok(())
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

/// Emit the startup_metrics event per DESIGN.md §4.
///
/// Only the explicit allowlist of fields is included. Credentials and
/// credential-adjacent values (passwords, full Redis URLs with userinfo)
/// are never emitted.
fn emit_startup_metrics(cfg: &Config, path: &std::path::Path) {
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

/// Phase 1 has no pipeline yet — we just block on shutdown signals so
/// the binary behaves like a real service for smoke testing.
async fn idle_until_shutdown() {
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
}

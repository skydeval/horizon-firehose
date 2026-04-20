//! horizon-firehose entry point.
//!
// The binary build never exercises the fake backend, the internal
// `ws_reader::spawn` variant, or the publisher's mid-retry
// `shutdown()` path — those are for unit tests. Suppressing
// dead_code only in the non-test build keeps those items alive for
// their test users without masking genuinely dead items in CI
// (where tests run).
#![cfg_attr(not(test), allow(dead_code))]
// `result_large_err` fires on every function that returns
// `Result<_, Error>` because `error::Error::ConfigLoad` wraps
// `figment::Error`, which is itself >200 bytes. Boxing figment
// errors is a large invasive refactor for a lint that targets
// hot-path performance — `Error` is only in the cold startup +
// shutdown paths where a 200-byte return size is irrelevant.
// The lint would be useful on per-event hot code; for this crate
// it's pure noise.
#![allow(clippy::result_large_err)]

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
#[cfg(test)]
mod golden_test;
mod metrics;
#[cfg(test)]
mod pipeline_main_test;
#[cfg(test)]
mod pipeline_test;
mod publisher;
mod router;
mod ws_reader;

use std::path::{Path, PathBuf};
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
    match handle_cli_args() {
        CliAction::Run => match run() {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                // Tracing may not be initialised yet (config could fail
                // before we set up subscribers), so emit on stderr too.
                eprintln!("fatal: {err}");
                error!(error = %err, "horizon-firehose exiting non-zero");
                ExitCode::FAILURE
            }
        },
        CliAction::Exit(code) => code,
    }
}

enum CliAction {
    Run,
    Exit(ExitCode),
}

/// Minimal CLI-arg handling. Phase 9 added this for two reasons:
///   1. `--version` lets the Dockerfile smoke test confirm the image
///      built and linked correctly without needing a real config.
///   2. `--help` points ops at DESIGN.md + config.example.toml when
///      someone runs the binary blind on a new host.
///
/// Anything beyond those two is rejected rather than silently ignored
/// — operator typos ("--cofig" etc.) should be loud, not ignored.
/// We intentionally don't wire clap in here; a two-flag CLI doesn't
/// justify the dep and `capture-fixtures` already uses clap if we
/// ever need richer parsing.
fn handle_cli_args() -> CliAction {
    let mut args = std::env::args().skip(1);
    let Some(arg) = args.next() else {
        return CliAction::Run;
    };
    match arg.as_str() {
        "--version" | "-V" => {
            println!("horizon-firehose {CONSUMER_VERSION}");
            CliAction::Exit(ExitCode::SUCCESS)
        }
        "--help" | "-h" => {
            print_help();
            CliAction::Exit(ExitCode::SUCCESS)
        }
        other => {
            eprintln!("horizon-firehose: unrecognised argument '{other}'");
            eprintln!("try `--help` for usage");
            CliAction::Exit(ExitCode::from(2))
        }
    }
}

fn print_help() {
    println!(
        "horizon-firehose {CONSUMER_VERSION}\n\
         \n\
         A Rust-based ATProto firehose consumer with CBOR/CAR decoding,\n\
         per-relay failover, and Redis stream publishing.\n\
         \n\
         USAGE:\n    \
             horizon-firehose [--version | --help]\n\
         \n\
         The binary takes no positional arguments. Configuration is loaded\n\
         from ./config.toml by default; override with the HF_CONFIG_PATH\n\
         environment variable. Individual fields can be overridden with\n\
         HORIZON_FIREHOSE_* env vars — see config.example.toml for the full\n\
         schema.\n\
         \n\
         See DESIGN.md for architecture and operational guidance."
    );
}

fn run() -> Result<(), Error> {
    // Phase 10.5 finding 1.2 (preventative): install the ring crypto
    // provider as rustls' process-wide default before any
    // `ClientConfig::builder()` call. Today rustls's `ring` feature
    // unification means `builder()` picks ring automatically, so this
    // is a no-op. The risk it heads off: a future transitive dep
    // could pull in `rustls/aws-lc-rs`, breaking feature-unification
    // uniqueness → `builder()` panics with "no crypto provider
    // installed" on the first TLS config build. Installing explicitly
    // turns that latent panic into either "install succeeds" (we
    // continue) or "install fails because one is already installed"
    // (we continue too; `.ok()` swallows the Err). Either way startup
    // proceeds deterministically.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let path = Config::resolve_path();
    let cfg = Config::load(&path)?;
    let tls_config = load_tls_client_config(&cfg)?;

    init_tracing(&cfg);
    emit_startup_metrics(&cfg, &path, tls_config.is_some());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(run_async(cfg, tls_config))
}

fn init_tracing(cfg: &Config) {
    let filter = EnvFilter::try_new(&cfg.logging.level).unwrap_or_else(|_| EnvFilter::new("info"));

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

/// Phase 8.7: load `relay.tls_extra_ca_file` into a
/// [`rustls::ClientConfig`] (additive to the OS trust store) so the
/// downstream [`proto_blue_ws::TungsteniteConnector::with_rustls_config`]
/// hook can enforce it during the TLS handshake. Returns:
///
/// - `Ok(None)` when the config field is empty — the default
///   `tokio-tungstenite` + `native-tls` path handles TLS against
///   system roots, no rustls involved.
/// - `Ok(Some(Arc<ClientConfig>))` when the file loaded cleanly —
///   system roots *plus* the CERTIFICATE entries from the PEM are
///   installed, so private CAs extend (not replace) public trust.
/// - `Err(Error::TlsExtraCaFile)` at the first failure point.
///   We intentionally fail startup rather than warn-and-continue —
///   Phase 8.5 finding 4.2's lesson was that a silently-ignored CA
///   turns into an unrelated-looking TLS error at handshake time,
///   which is exactly the shape of failure ops can't debug.
///
/// Errors are attributed to one of four points:
/// (a) the file can't be read, (b) the PEM parser couldn't tokenise
/// it, (c) it parsed but contained zero `CERTIFICATE` entries,
/// (d) `RootCertStore::add` rejected a cert (malformed DER inside a
/// well-formed PEM wrapper).
fn load_tls_client_config(cfg: &Config) -> Result<Option<Arc<rustls::ClientConfig>>, Error> {
    let path_str = cfg.relay.tls_extra_ca_file.trim();
    if path_str.is_empty() {
        return Ok(None);
    }
    let path = PathBuf::from(path_str);
    let config = build_rustls_config_from_pem(&path)?;
    Ok(Some(Arc::new(config)))
}

fn build_rustls_config_from_pem(path: &Path) -> Result<rustls::ClientConfig, Error> {
    let pem_bytes = std::fs::read(path).map_err(|e| Error::TlsExtraCaFile {
        path: path.to_path_buf(),
        reason: format!("failed to read: {e}"),
    })?;

    let mut roots = rustls::RootCertStore::empty();

    // System roots first. Individual cert failures inside the OS
    // trust store shouldn't refuse startup (some system stores carry
    // pre-expired or malformed intermediates) — log and proceed with
    // whatever loaded successfully.
    //
    // Phase 10.5 finding 1.3: we used to `let _ = roots.add(cert)`
    // for every cert and call it done. If every single one failed
    // (empty store, all malformed, all expired), the resulting
    // ClientConfig would trust ONLY the custom PEM — silently
    // turning an intended "system + extras" config into a
    // "private-CA only" config. That's a dangerous default for a
    // consumer talking to public relays: a compromised or misissued
    // private CA is suddenly enough to MITM everything. Track
    // success count explicitly and refuse startup if nothing loaded.
    let native = rustls_native_certs::load_native_certs();
    let native_error_count = native.errors.len();
    if native_error_count > 0 {
        warn!(
            error_count = native_error_count,
            "some OS trust-store certs failed to load; continuing with the subset that loaded"
        );
    }
    let native_attempted = native.certs.len();
    let mut native_added = 0usize;
    for cert in native.certs {
        if roots.add(cert).is_ok() {
            native_added += 1;
        }
    }
    if native_added == 0 {
        return Err(Error::TlsExtraCaFile {
            path: path.to_path_buf(),
            reason: format!(
                "zero system trust-store roots loaded successfully \
                 (attempted={native_attempted}, load_native_certs errors={native_error_count}). \
                 Refusing to build a ClientConfig that would trust only the custom PEM — \
                 that would silently downgrade the TLS posture to \"private-CA only\", \
                 where any misissuance in {path:?} becomes enough to MITM public relays. \
                 Fix the OS trust store, or if you genuinely want private-CA-only mode, \
                 that's a separate feature — not the default \"additive\" semantics."
            ),
        });
    }

    // Custom roots from PEM, additive to the system store above.
    // Uses the `PemObject` trait from `rustls-pki-types` (rustls
    // re-exports it as `rustls::pki_types`). Previously we leaned on
    // `rustls-pemfile`, but RUSTSEC-2025-0134 flagged that crate as
    // unmaintained — pki-types is the modern in-rustls-ecosystem
    // replacement and we already carry it transitively.
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    let custom: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem_bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::TlsExtraCaFile {
            path: path.to_path_buf(),
            reason: format!("failed to parse PEM: {e}"),
        })?;
    if custom.is_empty() {
        return Err(Error::TlsExtraCaFile {
            path: path.to_path_buf(),
            reason: "no CERTIFICATE entries found in PEM file".into(),
        });
    }
    let custom_count = custom.len();
    for cert in custom {
        roots.add(cert).map_err(|e| Error::TlsExtraCaFile {
            path: path.to_path_buf(),
            reason: format!("failed to add cert to root store: {e}"),
        })?;
    }

    info!(
        target: "horizon_firehose::metrics",
        event_type = "tls_extra_ca_loaded",
        path = %path.display(),
        custom_cert_count = custom_count,
        native_cert_count = native_added,
        "loaded tls_extra_ca_file additive to system roots"
    );

    Ok(rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

/// Phase 8.5 follow-up finding 2.1: spawn a tokio task, catch any
/// panic inside with `FutureExt::catch_unwind`, emit a structured
/// ERROR event that names the task before propagating, then
/// `resume_unwind` so the `JoinHandle` still resolves with
/// `Err(JoinError)` exactly as tokio's default panic propagation
/// would.
///
/// The attribution problem without this helper: when `ws_reader`
/// panics, its `frames_tx` drops, the decoder sees its input
/// channel close, and exits cleanly with `Ok(_)`. `main`'s
/// coordinator `select!` observes the decoder handle completing
/// first (because ws_reader's handle is owned by the decoder task),
/// records `panicked: false` for the decoder, and emits a misleading
/// "decoder exited unexpectedly" log. The `task_panicked` event
/// from `spawn_instrumented` fires *inside* ws_reader's task before
/// any of that cascade begins, so root cause is in the log stream
/// regardless of which handle the coordinator eventually observes.
pub(crate) fn spawn_instrumented<F, T>(name: &'static str, fut: F) -> tokio::task::JoinHandle<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tokio::spawn(instrumented_future(name, fut))
}

async fn instrumented_future<F, T>(name: &'static str, fut: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    use futures_util::FutureExt;
    match std::panic::AssertUnwindSafe(fut).catch_unwind().await {
        Ok(v) => v,
        Err(payload) => {
            // `&*payload` is an explicit `&(dyn Any + Send)`. Don't
            // change this to `&payload` (which would be
            // `&Box<dyn Any + Send>`) — investigated Phase 8.5
            // follow-up: auto-deref coercion goes through Box's own
            // `Any` impl, so `downcast_ref::<String>()` would ask
            // "is this a Box<dyn Any + Send>?" (yes) rather than
            // "is the inner value a String?" (also yes, but
            // invisible through the outer Box's TypeId). The
            // explicit deref exposes the trait object so the
            // downcast sees the real concrete type.
            let panic_message = panic_payload_to_string(&*payload);
            error!(
                target: "horizon_firehose::metrics",
                event_type = "task_panicked",
                task_name = %name,
                panic_message = %panic_message,
                "task '{}' panicked — propagating to supervisor",
                name
            );
            std::panic::resume_unwind(payload);
        }
    }
}

/// Best-effort string extraction from a `catch_unwind` panic payload.
/// Rust panics commonly carry either `&'static str` (literal panics)
/// or `String` (format-args panics). We try both. `futures-util`'s
/// `CatchUnwind` unwraps the `Box<dyn Any>` we receive, but the
/// interior type follows the same rules either way.
/// Caller must pass an explicitly-dereffed `&(dyn Any + Send)` —
/// `&*payload` where `payload: Box<dyn Any + Send>`. Passing
/// `&payload` directly lets method resolution find Box's own Any
/// impl, and every downcast returns `None` because it checks
/// against `TypeId::of::<Box<dyn Any + Send>>()` instead of the
/// inner concrete type. See the call site's comment.
fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    format!(
        "<panic payload not representable as str; type_id={:?}>",
        payload.type_id()
    )
}

/// Phase 8.5 review finding 4.5: build the concrete, copy-pasteable
/// remediation block for a malformed cursor at startup. Factored
/// here so the test suite can assert on its contents — 3am ops
/// regressions where "operator didn't see a DEL command" are a
/// silent-failure shape we want to guard against.
pub(crate) fn malformed_cursor_remediation(relay: &str, key: &str, value: &str) -> String {
    format!(
        "\n  \
         relay:             {relay}\n  \
         cursor key:        {key}\n  \
         malformed value:   {value:?}\n\n\
         Refusing to resume from live tip — that would silently lose every event\n\
         between the corrupted seq and current live tip. Operator action required.\n\n\
         Options (pick one, then restart):\n\
         \n  \
         1. Inspect the raw value yourself:\n       \
            redis-cli -u \"$REDIS_URL\" GET {key}\n\n  \
         2. Repair the cursor to a specific seq N you trust:\n       \
            redis-cli -u \"$REDIS_URL\" SET {key} N\n\n  \
         3. Delete the key to resume from live tip DELIBERATELY\n     \
            (accepts the loss of events between the corrupt seq and live tip):\n       \
            redis-cli -u \"$REDIS_URL\" DEL {key}\n"
    )
}

pub(crate) fn emit_startup_metrics(cfg: &Config, path: &std::path::Path, custom_ca_loaded: bool) {
    // Phase 8.5 review finding 4.4: belt-and-suspenders sanitization.
    // Config-level validation already rejects userinfo-bearing relay
    // URLs (see config::validate_ws_url), so these `sanitize_ws_url`
    // calls are redundant in the steady state — but cheap, and cover
    // any future validation regression before the leak reaches
    // persisted stream entries or log aggregation pipelines.
    let primary = crate::config::sanitize_ws_url(&cfg.relay.url);
    let fallbacks: Vec<String> = cfg
        .relay
        .fallbacks
        .iter()
        .map(|u| crate::config::sanitize_ws_url(u))
        .collect();
    let payload = json!({
        "type": "startup_metrics",
        "config_version": cfg.config_version,
        "consumer_version": CONSUMER_VERSION,
        "relay_primary": primary,
        "relay_fallbacks": fallbacks,
        "redis_url_host": cfg.redis_url_host(),
        "max_stream_len": cfg.redis.max_stream_len,
        "filter_nsid_count": cfg.filter.record_types.len(),
        "tls_custom_ca_loaded": custom_ca_loaded,
    });

    info!(
        target: "horizon_firehose::metrics",
        event_type = "startup_metrics",
        config_path = %path.display(),
        payload = %payload,
        "horizon-firehose starting"
    );
}

async fn run_async(
    cfg: Config,
    tls_config: Option<Arc<rustls::ClientConfig>>,
) -> Result<(), Error> {
    let started_at = Instant::now();

    // Global shutdown signal: one watch, cloned everywhere.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // Signal-listener runs for the lifetime of the process; a panic
    // here would be highly unusual but still attribute-worthy.
    spawn_instrumented("signal_listener", listen_for_signals(shutdown_tx.clone()));

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
        // Phase 8.5 review finding 4.5: distinguish "cursor value was
        // malformed" (corruption or typo in Redis — hard fail for
        // operator to fix) from "Redis transiently unreachable" (log
        // + resume from live tip, which is the documented §3
        // behaviour for "no prior cursor").
        match &err {
            backend::BackendError::MalformedCursor { key, value } => {
                let relay_for_key = all_relays
                    .iter()
                    .find(|r| cursor::cursor_key(r) == *key)
                    .cloned()
                    .unwrap_or_else(|| "<unknown — key didn't match any configured relay>".into());
                let remediation = malformed_cursor_remediation(&relay_for_key, key, value);
                error!(
                    relay = %relay_for_key,
                    cursor_key = %key,
                    malformed_value = %value,
                    "{}",
                    remediation
                );
                return Err(Error::TaskFailure {
                    task: "cursor_startup",
                    message: err.to_string(),
                });
            }
            _ => {
                warn!(error = %err, "failed to load cursors at startup; resuming from live tip");
            }
        }
    }

    // Shared metrics. Every task that produces counters holds an
    // `Arc<Metrics>`; the periodic emitter samples them on a ticker
    // and diffs against the previous sample to produce the
    // `*_in_window` fields from DESIGN.md §4.
    let metrics = metrics::Metrics::new();

    // Channels between pipeline stages. All bounded — backpressure
    // cascades upstream to the relay if a downstream stage stalls.
    // Channel item type is `PublishOp` end-to-end so skip-advances
    // flow in-band (Phase 8.5 review finding 1.1).
    let (event_tx, event_rx) = mpsc::channel::<publisher::PublishOp>(CHANNEL_BUFFER);
    let (filtered_tx, filtered_rx) = mpsc::channel::<publisher::PublishOp>(CHANNEL_BUFFER);
    // Downgrade the producer sides so the metrics emitter can sample
    // channel depth without keeping the channel alive past the
    // producer's exit — see metrics::ChannelGauges docs for why.
    let decoder_to_router_weak = event_tx.downgrade();
    let router_to_publisher_weak = filtered_tx.downgrade();

    // Spawn the pipeline. Order matters only so the channels line
    // up; the tasks all run concurrently.
    let ws_reader = ws_reader::spawn_with_cursors(
        cfg.relay.clone(),
        ws_reader::WsReaderOptions {
            tls_config: tls_config.clone(),
            ..ws_reader::WsReaderOptions::default()
        },
        cursors.clone(),
        shutdown_rx.clone(),
        metrics.clone(),
    );
    // Capture the ws → decoder WeakSender, state reader, and
    // control handle *before* `ws_reader` is moved into
    // `decoder::spawn`.
    let ws_to_decoder_weak = ws_reader.frames_weak();
    let ws_state = ws_reader.state_reader();
    let ws_control = ws_reader.control();

    // Phase 10.5 finding 5.1: the decoder consults
    // `cursor.on_stale_cursor` and `cursor.on_protocol_error`. When
    // a policy of `Exit` fires, the decoder pushes a reason onto
    // this channel and flips the global shutdown. We observe the
    // channel *after* the shutdown cascade so the process exits
    // non-zero with clear attribution, instead of the misleading
    // "decoder exited unexpectedly" the coordinator would otherwise log.
    let (policy_exit_tx, mut policy_exit_rx) = mpsc::unbounded_channel::<decoder::PolicyExit>();

    let mut decoder_handle = decoder::spawn(
        ws_reader,
        event_tx,
        metrics.clone(),
        ws_control,
        decoder::DecoderPolicies::from_cursor_config(&cfg.cursor),
        shutdown_tx.clone(),
        policy_exit_tx,
    );

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
        relay_primary = %crate::config::sanitize_ws_url(&cfg.relay.url),
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
                error!(
                    task = name,
                    "pipeline task exited unexpectedly — initiating shutdown"
                );
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
    // Phase 8.5 follow-up finding 4.10: surface whether the
    // publisher task panicked. If it did, the per-task stats in
    // `shutdown_result.publisher` are zeroed (the panic lost them)
    // but the shared-metrics totals are still accurate.
    let publisher_panicked = matches!(
        &shutdown_result.publisher,
        Ok(stats) if stats.task_panicked
    );
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
        publisher_task_panicked = publisher_panicked,
        final_cursor_per_relay = %serde_json::to_string(&final_cursor_per_relay).unwrap_or_default(),
        "shutdown complete"
    );

    // Phase 10.5 finding 5.1: policy-driven exits surface here after
    // the cascade has drained everything. Takes precedence over the
    // task-fault branch because an operator-configured
    // `on_stale_cursor=exit` IS a clean shutdown (not a bug) —
    // we just need the process to come out non-zero so the
    // orchestrator sees the operator's intent.
    if let Ok(policy_exit) = policy_exit_rx.try_recv() {
        return Err(Error::TaskFailure {
            task: "decoder",
            message: format!("policy-driven exit: {}", policy_exit.reason),
        });
    }

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
/// Phase 8.5 follow-up finding 2.2: inner budget for the publisher's
/// drain-and-retry phase, sized so the final cursor flush has
/// guaranteed time inside the outer `SHUTDOWN_BUDGET`.
const PUBLISHER_JOIN_BUDGET: Duration = Duration::from_secs(20);

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
    // retry as needed), closes. Phase 8.5 follow-up finding 2.2:
    // bound the join to `PUBLISHER_JOIN_BUDGET` so even a publisher
    // stuck in the Redis retry loop can't consume the whole
    // `SHUTDOWN_BUDGET` and starve the final cursor-flush step that
    // follows. On budget overrun, signal shutdown so the retry loop
    // exits on its own select — the in-flight event is lost from
    // the publisher's POV, but the cursor correspondingly doesn't
    // advance past it (DESIGN.md §3 invariant).
    let publisher_result = publisher_h.join_with_budget(PUBLISHER_JOIN_BUDGET).await;

    // Phase 8.5 follow-up finding 2.4: stop the periodic persister
    // *before* the final `persist_all`, not after. Previously a
    // persister tick could fire concurrently with the final flush;
    // the result is still consistent (cursor advances are monotonic
    // so both writes land) but the ordering is now cleaner and
    // `persist_all.outcome.failed` is no longer polluted by an
    // independent periodic write racing on the same key.
    let _persister_stats = persister_h.shutdown().await;

    // One last cursor flush after both the publisher has drained
    // AND the persister has stopped — sole-writer ordering.
    let outcome = cursors.persist_all(backend.as_ref()).await;
    if outcome.failed > 0 {
        warn!(
            written = outcome.written,
            failed = outcome.failed,
            "final cursor flush had failures; proceeding to exit"
        );
    }
    let final_cursors = cursors.snapshot().await;

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
    TaskEnded { name: &'static str, panicked: bool },
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

//! capture-fixtures — record raw firehose frames to disk.
//!
//! Connects to a single ATProto relay over WebSocket and writes each
//! received binary frame, untouched, to its own `.bin` file under the
//! output directory. A `manifest.json` tracks per-frame metadata
//! (filename, wall-clock timestamp, size) and is rewritten
//! incrementally so a crashed capture leaves usable data behind.
//!
//! This tool exists to break the chicken-and-egg problem from
//! DESIGN.md round-3 review F27: the Phase 3 decoder needs real frames
//! to test against, and we couldn't capture frames until Phase 2 gave
//! us a working WebSocket layer. The output of this binary becomes the
//! seed corpus for `tests/fixtures/`.
//!
//! Failover, multi-relay supervision, and clean-window resets do not
//! belong here — those live in `ws_reader::WsReader` for the
//! production pipeline. Capture is single-URL and best-effort.

use std::error::Error;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};
use clap::Parser;
use proto_blue_ws::{WebSocketKeepAlive, WebSocketKeepAliveOpts};
use serde::{Deserialize, Serialize};
use tokio::time::sleep_until;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "capture-fixtures",
    about = "Capture raw ATProto firehose frames to disk for decoder fixtures."
)]
struct Args {
    /// Relay WebSocket URL to subscribe to. Must include the firehose
    /// XRPC path — proto-blue-ws is a generic WS client and does not
    /// auto-route to `/xrpc/com.atproto.sync.subscribeRepos` the way
    /// the Python `atproto` library does. This default matches the
    /// `[relay].url` shipped in `config.example.toml` and documented
    /// in DESIGN.md §4 so production and capture use identical URL
    /// shapes.
    #[arg(
        long,
        default_value = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
    )]
    relay_url: String,

    /// Directory to write frames and manifest into. Created if missing.
    #[arg(long, default_value = "./tests/fixtures")]
    output_dir: PathBuf,

    /// Stop after capturing this many frames. Unlimited if unset.
    #[arg(long)]
    count: Option<u64>,

    /// Stop after this many seconds. Unlimited if unset.
    #[arg(long)]
    duration_secs: Option<u64>,

    /// Log a progress line every N frames.
    #[arg(long, default_value_t = 100)]
    progress_interval: u64,

    /// Rewrite manifest.json every N frames so a crashed capture
    /// leaves the most recent N-1 frames recoverable.
    #[arg(long, default_value_t = 100)]
    manifest_flush_interval: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct FrameEntry {
    filename: String,
    timestamp_ms: u128,
    size_bytes: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    capture_started_at: String,
    capture_ended_at: Option<String>,
    relay_url: String,
    frame_count: u64,
    frames: Vec<FrameEntry>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();
    if args.progress_interval == 0 || args.manifest_flush_interval == 0 {
        eprintln!("--progress-interval and --manifest-flush-interval must be > 0");
        return ExitCode::FAILURE;
    }

    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "capture-fixtures failed");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(&args.output_dir)?;
    let manifest_path = args.output_dir.join("manifest.json");

    let started_system = SystemTime::now();
    let started_iso = format_iso8601(started_system);
    info!(
        relay_url = %args.relay_url,
        output_dir = %args.output_dir.display(),
        count = ?args.count,
        duration_secs = ?args.duration_secs,
        "capture starting"
    );

    let mut ws = WebSocketKeepAlive::new(
        &args.relay_url,
        WebSocketKeepAliveOpts {
            max_reconnect_seconds: 60,
            heartbeat_interval_ms: 10_000,
        },
    );
    ws.connect()
        .await
        .map_err(|e| format!("initial connect to {} failed: {e}", args.relay_url))?;
    info!(relay_url = %args.relay_url, "connected");

    let mut manifest = Manifest {
        capture_started_at: started_iso,
        capture_ended_at: None,
        relay_url: args.relay_url.clone(),
        frame_count: 0,
        frames: Vec::new(),
    };
    write_manifest(&manifest_path, &manifest)?;

    let start_instant = Instant::now();
    let deadline = args
        .duration_secs
        .map(|s| tokio::time::Instant::now() + Duration::from_secs(s));

    let stop_reason = capture_loop(
        &mut ws,
        &mut manifest,
        &args,
        &manifest_path,
        start_instant,
        deadline,
    )
    .await?;

    manifest.capture_ended_at = Some(format_iso8601(SystemTime::now()));
    write_manifest(&manifest_path, &manifest)?;

    let elapsed = start_instant.elapsed();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        manifest.frame_count as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    info!(
        frames = manifest.frame_count,
        elapsed_secs = elapsed.as_secs_f64(),
        rate_per_sec = rate,
        stop_reason,
        manifest = %manifest_path.display(),
        "capture complete"
    );
    Ok(())
}

async fn capture_loop(
    ws: &mut WebSocketKeepAlive,
    manifest: &mut Manifest,
    args: &Args,
    manifest_path: &Path,
    start_instant: Instant,
    deadline: Option<tokio::time::Instant>,
) -> Result<&'static str, Box<dyn Error>> {
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        if let Some(c) = args.count {
            if manifest.frame_count >= c {
                return Ok("count_limit");
            }
        }

        let recv = ws.recv();
        let frame_result = match deadline {
            Some(d) => {
                tokio::select! {
                    biased;
                    res = &mut ctrl_c => {
                        if let Err(e) = res {
                            warn!(error = %e, "ctrl-c handler errored");
                        }
                        return Ok("ctrl_c");
                    }
                    _ = sleep_until(d) => return Ok("duration_limit"),
                    r = recv => r,
                }
            }
            None => {
                tokio::select! {
                    biased;
                    res = &mut ctrl_c => {
                        if let Err(e) = res {
                            warn!(error = %e, "ctrl-c handler errored");
                        }
                        return Ok("ctrl_c");
                    }
                    r = recv => r,
                }
            }
        };

        let frame = match frame_result {
            Ok(Some(bytes)) => bytes,
            // proto-blue-ws returns Ok(None) on a clean server-initiated
            // close. Its next `recv()` call will reconnect transparently.
            Ok(None) => {
                warn!("server closed connection; will rely on inner auto-reconnect");
                continue;
            }
            Err(e) => return Err(format!("recv error after {} frames: {e}", manifest.frame_count).into()),
        };

        let now = SystemTime::now();
        let ts_ms = now
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let idx = manifest.frame_count;
        let filename = format!("{ts_ms}_{idx:08}.bin");
        let frame_path = args.output_dir.join(&filename);
        let size = frame.len();
        fs::write(&frame_path, &frame)?;

        manifest.frames.push(FrameEntry {
            filename,
            timestamp_ms: ts_ms,
            size_bytes: size,
        });
        manifest.frame_count += 1;

        if manifest.frame_count % args.progress_interval == 0 {
            let elapsed = start_instant.elapsed();
            let rate = manifest.frame_count as f64 / elapsed.as_secs_f64().max(1e-6);
            info!(
                frames = manifest.frame_count,
                elapsed_secs = elapsed.as_secs_f64(),
                rate_per_sec = rate,
                "progress"
            );
        }

        if manifest.frame_count % args.manifest_flush_interval == 0 {
            if let Err(e) = write_manifest(manifest_path, manifest) {
                warn!(error = %e, "manifest flush failed; will retry on next interval");
            }
        }
    }
}

fn write_manifest(path: &Path, manifest: &Manifest) -> Result<(), Box<dyn Error>> {
    // Atomic rewrite via temp file → rename. Avoids leaving a half-written
    // manifest if the process crashes mid-write.
    let tmp = path.with_extension("json.tmp");
    {
        let f = File::create(&tmp)?;
        let mut w = BufWriter::new(f);
        serde_json::to_writer_pretty(&mut w, manifest)?;
        w.into_inner()?.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

fn format_iso8601(t: SystemTime) -> String {
    let dt: DateTime<Utc> = t.into();
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

//! Configuration loading, env-var overrides, and validation.
//!
//! Layering (lowest to highest precedence): defaults baked into the
//! struct definitions → TOML file on disk → environment variables
//! prefixed `HORIZON_FIREHOSE_` with `__` separating sections.
//!
//! Validation runs after merging. We reject impossible values eagerly so
//! the rest of the binary can treat the config as a trusted invariant.

use std::path::{Path, PathBuf};

use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result, SUPPORTED_CONFIG_VERSION};

/// Default location the loader checks if no path is supplied.
pub const DEFAULT_CONFIG_PATH: &str = "config.toml";

/// Env-var that overrides the config file path itself.
///
/// **Deliberately outside the `HORIZON_FIREHOSE_` namespace** — figment's
/// `Env::prefixed("HORIZON_FIREHOSE_").split("__")` would otherwise claim
/// any bare `HORIZON_FIREHOSE_CONFIG` as a top-level field `config`, and
/// `deny_unknown_fields` would then reject startup even though the
/// operator did nothing wrong (Phase 10.5 finding 5.2 caught this with
/// `env_var_overrides_with_config_path_env_also_set`). `HF_CONFIG_PATH`
/// lives in its own namespace so the two reading strategies coexist
/// cleanly: `main` reads this directly and passes the resolved file to
/// figment, which then merges `HORIZON_FIREHOSE_*` on top.
pub const CONFIG_PATH_ENV: &str = "HF_CONFIG_PATH";

/// Top-level config struct. Fields map 1:1 to the TOML schema in
/// DESIGN.md §4.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub config_version: u32,

    // `[relay]` and `[redis]` are required: silent defaults here would
    // point operators at the wrong relay or the wrong cache and only
    // surface as a connection failure at runtime. The other sections
    // below are tuning dials with universal defaults and may be
    // omitted. See DESIGN.md §4 "required vs. optional sections".
    pub relay: RelayConfig,
    pub redis: RedisConfig,

    #[serde(default)]
    pub filter: FilterConfig,

    #[serde(default)]
    pub cursor: CursorConfig,

    #[serde(default)]
    pub publisher: PublisherConfig,

    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RelayConfig {
    pub url: String,
    #[serde(default)]
    pub fallbacks: Vec<String>,
    pub reconnect_initial_delay_ms: u64,
    pub reconnect_max_delay_ms: u64,
    pub failover_threshold: u32,
    pub failover_cooldown_seconds: u64,
    #[serde(default)]
    pub tls_extra_ca_file: String,
}

// No `Default` impl: `[relay]` is required, and `config.example.toml`
// is the single documented source of recommended values.

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    pub url: String,
    pub stream_key: String,
    pub max_stream_len: u64,
    #[serde(default)]
    pub cleanup_unknown_cursors: bool,
}

// No `Default` impl: `[redis]` is required, same rationale as `[relay]`.

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FilterConfig {
    /// Empty = pass all record types through.
    #[serde(default)]
    pub record_types: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CursorConfig {
    pub save_interval_seconds: u64,
    pub on_stale_cursor: StaleCursorPolicy,
    pub on_protocol_error: ProtocolErrorPolicy,
}

impl Default for CursorConfig {
    fn default() -> Self {
        Self {
            save_interval_seconds: 5,
            on_stale_cursor: StaleCursorPolicy::LiveTip,
            on_protocol_error: ProtocolErrorPolicy::ReconnectFromLiveTip,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StaleCursorPolicy {
    LiveTip,
    Exit,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorPolicy {
    ReconnectFromLiveTip,
    Exit,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublisherConfig {
    pub max_event_size_bytes: u64,
    pub on_oversize: OversizePolicy,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            max_event_size_bytes: 1_048_576,
            on_oversize: OversizePolicy::SkipWithLog,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OversizePolicy {
    SkipWithLog,
    FailHard,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// Bare level or a full EnvFilter directive
    /// (e.g. `"info,horizon_firehose=debug"`).
    pub level: String,
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Json,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Json,
    Pretty,
}

impl Config {
    /// Resolve which config path to use, honouring `HF_CONFIG_PATH`
    /// or falling back to `./config.toml`.
    pub fn resolve_path() -> PathBuf {
        std::env::var(CONFIG_PATH_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_CONFIG_PATH))
    }

    /// Load + validate a config file. Env vars override TOML values.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(Error::ConfigNotFound(path.to_path_buf()));
        }
        Self::load_from_figment(Figment::new().merge(Toml::file(path)), Some(path))
    }

    fn load_from_figment(base: Figment, path: Option<&Path>) -> Result<Self> {
        let figment = base.merge(Env::prefixed("HORIZON_FIREHOSE_").split("__"));
        let cfg: Self = figment.extract().map_err(|source| Error::ConfigLoad {
            path: path.map(Path::to_path_buf).unwrap_or_default(),
            source,
        })?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Cross-field validation. Per-field shape checks are handled by serde
    /// (enum strings, integer parsing). Anything that requires looking at
    /// more than one field, or talking to the filesystem, lives here.
    pub fn validate(&self) -> Result<()> {
        if self.config_version > SUPPORTED_CONFIG_VERSION {
            return Err(Error::ConfigVersionTooNew {
                found: self.config_version,
                supported: SUPPORTED_CONFIG_VERSION,
            });
        }

        validate_ws_url("relay.url", &self.relay.url)?;
        for (i, fb) in self.relay.fallbacks.iter().enumerate() {
            validate_ws_url(&format!("relay.fallbacks[{i}]"), fb)?;
        }

        if self.relay.reconnect_initial_delay_ms == 0 {
            return Err(Error::ConfigValidation(
                "relay.reconnect_initial_delay_ms must be > 0".into(),
            ));
        }
        if self.relay.reconnect_max_delay_ms < self.relay.reconnect_initial_delay_ms {
            return Err(Error::ConfigValidation(format!(
                "relay.reconnect_max_delay_ms ({}) must be >= reconnect_initial_delay_ms ({})",
                self.relay.reconnect_max_delay_ms, self.relay.reconnect_initial_delay_ms,
            )));
        }
        if self.relay.failover_threshold == 0 {
            return Err(Error::ConfigValidation(
                "relay.failover_threshold must be > 0".into(),
            ));
        }

        validate_redis_url("redis.url", &self.redis.url)?;
        if self.redis.stream_key.is_empty() {
            return Err(Error::ConfigValidation(
                "redis.stream_key must not be empty".into(),
            ));
        }
        if self.redis.max_stream_len == 0 {
            return Err(Error::ConfigValidation(
                "redis.max_stream_len must be > 0".into(),
            ));
        }

        if self.cursor.save_interval_seconds == 0 {
            return Err(Error::ConfigValidation(
                "cursor.save_interval_seconds must be > 0".into(),
            ));
        }

        if self.publisher.max_event_size_bytes == 0 {
            return Err(Error::ConfigValidation(
                "publisher.max_event_size_bytes must be > 0".into(),
            ));
        }

        if self.logging.level.trim().is_empty() {
            return Err(Error::ConfigValidation(
                "logging.level must not be empty".into(),
            ));
        }

        Ok(())
    }

    /// Host:port form of the Redis URL with any `user:pass@` userinfo
    /// stripped — safe to log under `startup_metrics.redis_url_host`.
    pub fn redis_url_host(&self) -> String {
        sanitize_redis_host(&self.redis.url)
    }
}

fn validate_ws_url(field: &str, url: &str) -> Result<()> {
    if !(url.starts_with("ws://") || url.starts_with("wss://")) {
        return Err(Error::ConfigValidation(format!(
            "{field} ({url:?}) must start with ws:// or wss://"
        )));
    }
    // Phase 8.5 review finding 4.4 + 4.9: userinfo in the relay URL
    // (`wss://user:pass@host/…`) would land in every startup_metrics
    // log and every event's `_relay` field. Reject at config time —
    // ATProto firehose doesn't use URL-based auth in the first place,
    // so a userinfo-bearing URL is always a misconfiguration.
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let authority = after_scheme
        .split(&['/', '?', '#'][..])
        .next()
        .unwrap_or("");
    if authority.contains('@') {
        return Err(Error::ConfigValidation(format!(
            "{field} contains userinfo (`user@host` or `user:pass@host`); \
             ATProto relays don't use URL-based auth and committing credentials \
             to logs / event payloads is a leak risk. Remove the userinfo portion.",
        )));
    }
    // Phase 8.5 review finding 4.1: the comment previously claimed the
    // validator rejects pre-existing `cursor=` query params; it didn't.
    // Reject them now so the `?cursor=N` appended by ws_reader can't
    // produce a double-cursor URL with undefined relay behaviour.
    let query = url.split_once('?').map(|(_, q)| q).unwrap_or("");
    for param in query.split('&') {
        let name = param.split_once('=').map(|(n, _)| n).unwrap_or(param);
        if name.eq_ignore_ascii_case("cursor") {
            return Err(Error::ConfigValidation(format!(
                "{field} has a pre-existing `cursor=` query parameter — \
                 ws_reader appends its own `?cursor=N` on reconnect and would \
                 produce a double-cursor URL. Remove it from config; cursors \
                 are managed at runtime.",
            )));
        }
    }
    Ok(())
}

/// Strip userinfo from a `ws://` or `wss://` URL, preserving scheme +
/// host + port + path + query. Used everywhere a relay URL might land
/// in a log, a metric, or an event payload. Mirrors
/// [`sanitize_redis_host`] but keeps the path/query intact since the
/// XRPC endpoint and query params are part of the relay's identity
/// (cursor-key hashing, `_relay` event field, and the
/// `connect_url` log on reconnect all need them).
pub fn sanitize_ws_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some(at) = rest.rfind('@') else {
        return url.to_string();
    };
    // Userinfo only counts if the `@` is in the authority section —
    // i.e. before the first `/`, `?`, or `#`. An `@` inside a path
    // is legal and should be preserved.
    let first_path_char = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    if at >= first_path_char {
        return url.to_string();
    }
    format!("{scheme}://{}", &rest[at + 1..])
}

fn validate_redis_url(field: &str, url: &str) -> Result<()> {
    if !(url.starts_with("redis://") || url.starts_with("rediss://")) {
        return Err(Error::ConfigValidation(format!(
            "{field} ({url:?}) must start with redis:// or rediss://"
        )));
    }
    Ok(())
}

/// Strip scheme, userinfo, and path from a `redis://` URL, leaving
/// `host[:port]`. Used for log-safe identification.
fn sanitize_redis_host(url: &str) -> String {
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let after_userinfo = after_scheme
        .rsplit_once('@')
        .map(|(_, r)| r)
        .unwrap_or(after_scheme);
    let host_port = after_userinfo
        .split_once('/')
        .map(|(h, _)| h)
        .unwrap_or(after_userinfo);
    let host_port = host_port
        .split_once('?')
        .map(|(h, _)| h)
        .unwrap_or(host_port);
    host_port.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Jail;

    fn write_config(jail: &mut Jail, body: &str) -> PathBuf {
        let path = jail.directory().join("config.toml");
        std::fs::write(&path, body).unwrap();
        path
    }

    const MINIMAL_CONFIG: &str = r#"
config_version = 1

[relay]
url = "wss://bsky.network"
reconnect_initial_delay_ms = 1000
reconnect_max_delay_ms = 60000
failover_threshold = 5
failover_cooldown_seconds = 600

[redis]
url = "redis://localhost:6379"
stream_key = "firehose:events"
max_stream_len = 500000

[cursor]
save_interval_seconds = 5
on_stale_cursor = "live_tip"
on_protocol_error = "reconnect_from_live_tip"

[publisher]
max_event_size_bytes = 1048576
on_oversize = "skip_with_log"

[logging]
level = "info"
format = "json"
"#;

    #[test]
    fn loads_a_minimal_valid_config() {
        Jail::expect_with(|jail| {
            let path = write_config(jail, MINIMAL_CONFIG);
            let cfg = Config::load(&path).expect("load");
            assert_eq!(cfg.config_version, 1);
            assert_eq!(cfg.relay.url, "wss://bsky.network");
            assert_eq!(cfg.redis.stream_key, "firehose:events");
            assert_eq!(cfg.cursor.on_stale_cursor, StaleCursorPolicy::LiveTip);
            assert_eq!(cfg.publisher.on_oversize, OversizePolicy::SkipWithLog);
            assert_eq!(cfg.logging.format, LogFormat::Json);
            Ok(())
        });
    }

    #[test]
    fn missing_file_is_a_clear_error() {
        let path = PathBuf::from("definitely-not-a-real-path-12345.toml");
        let err = Config::load(&path).unwrap_err();
        assert!(matches!(err, Error::ConfigNotFound(_)));
    }

    #[test]
    fn rejects_config_missing_relay_section() {
        // Strip the entire `[relay]` block out of MINIMAL_CONFIG. With
        // no `Default` impl and no `#[serde(default)]` on
        // `Config::relay`, serde must produce a "missing field" error.
        Jail::expect_with(|jail| {
            let body = strip_section(MINIMAL_CONFIG, "[relay]");
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            match err {
                Error::ConfigLoad { source, .. } => {
                    let msg = source.to_string();
                    assert!(
                        msg.contains("relay"),
                        "expected error message to name `relay`, got: {msg}"
                    );
                }
                other => panic!("expected ConfigLoad, got {other:?}"),
            }
            Ok(())
        });
    }

    #[test]
    fn rejects_config_missing_redis_section() {
        Jail::expect_with(|jail| {
            let body = strip_section(MINIMAL_CONFIG, "[redis]");
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            match err {
                Error::ConfigLoad { source, .. } => {
                    let msg = source.to_string();
                    assert!(
                        msg.contains("redis"),
                        "expected error message to name `redis`, got: {msg}"
                    );
                }
                other => panic!("expected ConfigLoad, got {other:?}"),
            }
            Ok(())
        });
    }

    #[test]
    fn accepts_config_omitting_optional_sections() {
        // [filter], [cursor], [publisher], [logging] are tuning dials
        // with universal defaults — they may be omitted.
        let body = "\
config_version = 1

[relay]
url = \"wss://bsky.network\"
reconnect_initial_delay_ms = 1000
reconnect_max_delay_ms = 60000
failover_threshold = 5
failover_cooldown_seconds = 600

[redis]
url = \"redis://localhost:6379\"
stream_key = \"firehose:events\"
max_stream_len = 500000
";
        Jail::expect_with(|jail| {
            let path = write_config(jail, body);
            let cfg = Config::load(&path).expect("load");
            // Spot-check a couple of defaulted fields.
            assert_eq!(cfg.cursor.save_interval_seconds, 5);
            assert_eq!(cfg.publisher.max_event_size_bytes, 1_048_576);
            assert_eq!(cfg.logging.format, LogFormat::Json);
            assert!(cfg.filter.record_types.is_empty());
            Ok(())
        });
    }

    /// Remove an entire `[section]` block (header + all key/value
    /// lines that follow, up to the next `[` or EOF) from a TOML body.
    fn strip_section(body: &str, header: &str) -> String {
        let mut out = String::new();
        let mut skipping = false;
        for line in body.lines() {
            let trimmed = line.trim_start();
            if skipping {
                if trimmed.starts_with('[') {
                    skipping = false;
                } else {
                    continue;
                }
            }
            if trimmed == header {
                skipping = true;
                continue;
            }
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        Jail::expect_with(|jail| {
            let body = format!("{MINIMAL_CONFIG}\nbogus_field = 42\n");
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            assert!(matches!(err, Error::ConfigLoad { .. }));
            Ok(())
        });
    }

    #[test]
    fn rejects_future_config_version() {
        Jail::expect_with(|jail| {
            let body = MINIMAL_CONFIG.replace("config_version = 1", "config_version = 99");
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            match err {
                Error::ConfigVersionTooNew { found, supported } => {
                    assert_eq!(found, 99);
                    assert_eq!(supported, SUPPORTED_CONFIG_VERSION);
                }
                other => panic!("wrong error: {other:?}"),
            }
            Ok(())
        });
    }

    #[test]
    fn rejects_non_ws_relay_url() {
        Jail::expect_with(|jail| {
            let body = MINIMAL_CONFIG.replace("wss://bsky.network", "http://bsky.network");
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            assert!(matches!(err, Error::ConfigValidation(_)));
            Ok(())
        });
    }

    #[test]
    fn rejects_non_redis_url() {
        Jail::expect_with(|jail| {
            let body = MINIMAL_CONFIG.replace("redis://localhost:6379", "http://localhost:6379");
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            assert!(matches!(err, Error::ConfigValidation(_)));
            Ok(())
        });
    }

    #[test]
    fn rejects_max_delay_below_initial() {
        Jail::expect_with(|jail| {
            let body = MINIMAL_CONFIG.replace(
                "reconnect_max_delay_ms = 60000",
                "reconnect_max_delay_ms = 500",
            );
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            assert!(
                matches!(err, Error::ConfigValidation(s) if s.contains("reconnect_max_delay_ms"))
            );
            Ok(())
        });
    }

    #[test]
    fn rejects_zero_values_that_must_be_positive() {
        for (needle, replacement) in [
            (
                "reconnect_initial_delay_ms = 1000",
                "reconnect_initial_delay_ms = 0",
            ),
            ("failover_threshold = 5", "failover_threshold = 0"),
            ("max_stream_len = 500000", "max_stream_len = 0"),
            ("save_interval_seconds = 5", "save_interval_seconds = 0"),
            ("max_event_size_bytes = 1048576", "max_event_size_bytes = 0"),
        ] {
            Jail::expect_with(|jail| {
                let body = MINIMAL_CONFIG.replace(needle, replacement);
                let path = write_config(jail, &body);
                let err = Config::load(&path).unwrap_err();
                assert!(
                    matches!(err, Error::ConfigValidation(_)),
                    "expected ConfigValidation for {replacement}, got {err:?}",
                );
                Ok(())
            });
        }
    }

    #[test]
    fn env_var_overrides_nested_field() {
        Jail::expect_with(|jail| {
            let path = write_config(jail, MINIMAL_CONFIG);
            jail.set_env("HORIZON_FIREHOSE_REDIS__URL", "redis://override:6380");
            let cfg = Config::load(&path).unwrap();
            assert_eq!(cfg.redis.url, "redis://override:6380");
            Ok(())
        });
    }

    #[test]
    fn env_var_overrides_top_level_field() {
        Jail::expect_with(|jail| {
            let path = write_config(jail, MINIMAL_CONFIG);
            jail.set_env("HORIZON_FIREHOSE_CONFIG_VERSION", "1");
            let cfg = Config::load(&path).unwrap();
            assert_eq!(cfg.config_version, 1);
            Ok(())
        });
    }

    #[test]
    fn env_var_can_introduce_invalid_value() {
        Jail::expect_with(|jail| {
            let path = write_config(jail, MINIMAL_CONFIG);
            jail.set_env("HORIZON_FIREHOSE_RELAY__URL", "http://nope");
            let err = Config::load(&path).unwrap_err();
            assert!(matches!(err, Error::ConfigValidation(_)));
            Ok(())
        });
    }

    #[test]
    fn env_var_overrides_with_config_path_env_also_set() {
        // Phase 10.5 finding 5.2: the config-path selector used to live
        // at `HORIZON_FIREHOSE_CONFIG`, which collided with figment's
        // `Env::prefixed("HORIZON_FIREHOSE_").split("__")` — figment
        // would parse it as a top-level field `config` and
        // `deny_unknown_fields` would reject startup. The fix was to
        // move the selector to `HF_CONFIG_PATH` (different namespace).
        // This test pins that: with `HF_CONFIG_PATH` set, startup
        // must succeed.
        Jail::expect_with(|jail| {
            let path = write_config(jail, MINIMAL_CONFIG);
            jail.set_env(CONFIG_PATH_ENV, path.to_str().unwrap());
            let cfg = Config::load(&path).expect("should load when HF_CONFIG_PATH is set");
            assert_eq!(cfg.config_version, 1);
            Ok(())
        });
    }

    #[test]
    fn validates_fallback_relay_urls() {
        Jail::expect_with(|jail| {
            let body = MINIMAL_CONFIG.replace(
                "[relay]\nurl = \"wss://bsky.network\"",
                "[relay]\nurl = \"wss://bsky.network\"\nfallbacks = [\"wss://relay.fyi\", \"http://bad\"]",
            );
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            assert!(matches!(err, Error::ConfigValidation(s) if s.contains("fallbacks[1]")));
            Ok(())
        });
    }

    #[test]
    fn rejects_invalid_enum_strings() {
        Jail::expect_with(|jail| {
            let body = MINIMAL_CONFIG.replace(
                "on_stale_cursor = \"live_tip\"",
                "on_stale_cursor = \"yolo\"",
            );
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            assert!(matches!(err, Error::ConfigLoad { .. }));
            Ok(())
        });
    }

    #[test]
    fn rejects_userinfo_in_relay_url() {
        // Phase 8.5 review finding 4.4: userinfo would land in every
        // startup_metrics log and every event's `_relay` field.
        Jail::expect_with(|jail| {
            let body = MINIMAL_CONFIG.replace(
                "url = \"wss://bsky.network\"",
                "url = \"wss://user:pass@bsky.network\"",
            );
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            match err {
                Error::ConfigValidation(msg) => {
                    assert!(
                        msg.contains("userinfo"),
                        "want userinfo mention; got: {msg}"
                    );
                }
                other => panic!("expected ConfigValidation, got {other:?}"),
            }
            Ok(())
        });
    }

    #[test]
    fn rejects_userinfo_in_relay_fallback_url() {
        Jail::expect_with(|jail| {
            let body = MINIMAL_CONFIG.replace(
                "[relay]\nurl = \"wss://bsky.network\"",
                "[relay]\nurl = \"wss://bsky.network\"\nfallbacks = [\"wss://u@fb\"]",
            );
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            assert!(matches!(err, Error::ConfigValidation(s) if s.contains("userinfo")));
            Ok(())
        });
    }

    #[test]
    fn rejects_preexisting_cursor_query_param_in_relay_url() {
        // Phase 8.5 review finding 4.1: ws_reader appends `?cursor=N`
        // on reconnect; a pre-existing cursor param would produce a
        // double-cursor URL.
        Jail::expect_with(|jail| {
            let body = MINIMAL_CONFIG.replace(
                "url = \"wss://bsky.network\"",
                "url = \"wss://bsky.network/xrpc/sub?cursor=0\"",
            );
            let path = write_config(jail, &body);
            let err = Config::load(&path).unwrap_err();
            assert!(matches!(err, Error::ConfigValidation(s) if s.contains("cursor=")));
            Ok(())
        });
    }

    #[test]
    fn sanitize_ws_url_strips_userinfo_keeps_path_and_query() {
        assert_eq!(
            sanitize_ws_url("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"),
            "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
        );
        assert_eq!(
            sanitize_ws_url("wss://user:pass@host.example/xrpc/sub?x=1"),
            "wss://host.example/xrpc/sub?x=1"
        );
        assert_eq!(sanitize_ws_url("ws://u@host:9443/"), "ws://host:9443/");
        // `@` inside the path must NOT trip the sanitizer — path
        // `@` is legal per RFC 3986.
        assert_eq!(
            sanitize_ws_url("wss://host.example/path/with/@sign"),
            "wss://host.example/path/with/@sign"
        );
    }

    #[test]
    fn redis_url_host_strips_userinfo_and_path() {
        assert_eq!(
            sanitize_redis_host("redis://localhost:6379"),
            "localhost:6379"
        );
        assert_eq!(
            sanitize_redis_host("redis://user:pass@host.example:6380/0"),
            "host.example:6380",
        );
        assert_eq!(
            sanitize_redis_host("rediss://h@host.example:6380"),
            "host.example:6380",
        );
        assert_eq!(sanitize_redis_host("redis://host?timeout=1"), "host",);
    }

    #[test]
    fn example_config_file_loads() {
        // Our shipped config.example.toml must parse and validate.
        // Wrapped in `Jail` so it serialises with the other env-var
        // tests — `Config::load` merges process env vars, and other
        // tests' temporary `HORIZON_FIREHOSE_*` settings would
        // otherwise race in here when tests run in parallel.
        Jail::expect_with(|_jail| {
            let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
            let cfg = Config::load(&example).expect("example config loads");
            assert_eq!(cfg.config_version, SUPPORTED_CONFIG_VERSION);
            Ok(())
        });
    }
}

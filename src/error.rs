//! Top-level error types for horizon-firehose.
//!
//! Each subsystem (config, ws, decoder, publisher, cursor) feeds into
//! this enum so `main` has one thing to match on. Variants carry enough
//! context to be useful in a structured log line without re-wrapping.

use std::path::PathBuf;

use thiserror::Error;

/// Maximum config schema version this build understands.
pub const SUPPORTED_CONFIG_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum Error {
    #[error("config file not found at {0}")]
    ConfigNotFound(PathBuf),

    #[error("failed to load config from {path}: {source}")]
    ConfigLoad {
        path: PathBuf,
        #[source]
        source: figment::Error,
    },

    #[error(
        "config_version {found} is newer than the maximum supported version {supported}. \
         Either upgrade horizon-firehose or downgrade your config. \
         See DESIGN.md §4 'config version bump policy' for migration guidance."
    )]
    ConfigVersionTooNew { found: u32, supported: u32 },

    #[error("config validation failed: {0}")]
    ConfigValidation(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

//! # walastack-runtime
//!
//! Tokio-backed runtime integration for WalaStack.
//!
//! Provides helpers for tracing initialization and graceful-shutdown signal
//! handling. The full `#[walastack::main]` runtime macro lives in
//! `walastack-macros`; this crate provides the underlying helpers the macro
//! calls into.

use tracing_subscriber::{EnvFilter, fmt};

/// Initialize structured tracing with an env-filter-aware subscriber.
///
/// Reads `RUST_LOG` for the filter directive; falls back to `info` level if
/// the environment variable is unset or malformed.
///
/// Safe to call multiple times — only the first call installs a subscriber;
/// subsequent calls are no-ops.
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}

/// Wait for a graceful-shutdown signal — SIGINT (Ctrl+C) on all platforms,
/// SIGTERM additionally on Unix.
///
/// Returns when the first signal arrives. Use with [`tokio::select!`] to
/// coordinate clean shutdown of long-running operations.
#[allow(clippy::redundant_pub_crate)] // tokio::select! generates pub(crate) items in a private module
pub async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!("failed to install Ctrl+C handler: {err}");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!("failed to install SIGTERM handler: {err}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

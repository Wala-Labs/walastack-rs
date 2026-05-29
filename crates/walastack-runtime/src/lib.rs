//! # walastack-runtime
//!
//! The WalaStack Runtime Kernel.
//!
//! Single crate (modular internally) hosting the kernel primitives that
//! every long-running participant — HTTP services, agents, sync, jobs,
//! workflows — shares. The kernel depends only on [`tokio`] and
//! [`tracing`]; it deliberately knows nothing about HTTP, serialization,
//! or any optional crate.
//!
//! ## Current Phase 2.0.f surface
//!
//! The Runtime Kernel surface is complete:
//!
//! - [`Runtime`] / [`RuntimeBuilder`] — the kernel composition point.
//! - [`RuntimeContext`] — the unified API surface for participants.
//! - [`Service`] / [`ServiceContext`] / [`ServiceError`] — the contract
//!   for long-running kernel participants.
//! - [`SupervisionTree`] / [`RestartPolicy`] — kernel-owned lifecycle
//!   management with restart-on-failure backoff.
//! - [`Plugin`] / [`PluginManager`] / [`ServicePlanner`] /
//!   [`CapabilityRequirement`] — the ecosystem extension boundary.
//!   Plugins register resources, capability providers, and services;
//!   fail-fast validation rejects unmet capability requirements at
//!   build time.
//! - [`Resources`] / [`ResourceRegistry`] — typed shared values keyed by
//!   [`std::any::TypeId`], lifecycle-managed.
//! - [`Capabilities`] / [`CapabilityRegistry`] — named multi-provider
//!   contracts keyed by `(TypeId, CapabilityName)` with selection
//!   strategies ([`SelectionStrategy::Single`],
//!   [`SelectionStrategy::Fallback`],
//!   [`SelectionStrategy::WeightedRoundRobin`]).
//! - [`EventBus`] — typed in-process pub/sub with broadcast and
//!   work-stealing subscription patterns, plus a watch-backed
//!   [`ShutdownSignal`].
//! - [`Scheduler`] — kernel time/trigger primitive with `After`, `At`,
//!   `FixedRate`, `FixedDelay`, `Cron` triggers and `Timeout`, `Retry`,
//!   `Backoff` policies.
//! - [`init_tracing`] — bootstrap structured logging.
//! - [`wait_for_shutdown_signal`] — cross-platform graceful shutdown.
//!
//! See the
//! [Runtime Kernel architecture overview](https://walastack.com/docs/architecture/runtime/overview/)
//! for the design rationale.

pub mod capabilities;
pub mod context;
pub mod events;
pub mod plugins;
pub mod resources;
pub mod runtime;
pub mod scheduler;
pub mod services;
pub mod supervision;

pub use capabilities::{
    Capabilities, CapabilityName, CapabilityRegistry, DEFAULT_NAME, SelectionStrategy,
};
pub use context::RuntimeContext;
pub use events::{
    DEFAULT_BROADCAST_CAPACITY, DEFAULT_WORK_CAPACITY, EnqueueError, EnqueueErrorKind, EventBus,
    PublishOutcome, RecvError, RuntimeStarted, RuntimeStarting, RuntimeStopped, RuntimeStopping,
    ShutdownSignal, Subscriber, Worker,
};
pub use plugins::{CapabilityRequirement, Plugin, PluginError, PluginManager, ServicePlanner};
pub use resources::{ResourceRegistry, Resources};
pub use runtime::{DEFAULT_SHUTDOWN_DEADLINE, Runtime, RuntimeBuilder, RuntimeError};
pub use scheduler::{
    Backoff, CronSchedule, Policies, RetryPolicy, ScheduleHandle, ScheduleId, ScheduledFn,
    Scheduler, SchedulerError, TaskCompleted, TaskError, TaskFailed, TaskFired, TaskResult,
    TaskRetrying, Trigger,
};
pub use services::{BoxedServiceFuture, Service, ServiceContext, ServiceError};
pub use supervision::{
    RestartPolicy, ServiceFailed, ServiceStarted, ServiceStopped, SupervisionTree,
};

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

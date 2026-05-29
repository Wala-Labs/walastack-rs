//! In-process observability for the WalaStack Runtime Kernel.
//!
//! `walastack-observability` ships an [`ObservabilityPlugin`] that:
//!
//! - Registers an [`ObservabilityService`] under kernel supervision.
//! - Registers a [`HealthRegistry`] capability that exposes aggregated
//!   state for any participant that needs to query system health.
//!
//! The Service subscribes to kernel lifecycle events
//! ([`RuntimeStarted`] / [`RuntimeStopping`] / [`RuntimeStopped`]),
//! supervision events ([`ServiceStarted`] / [`ServiceStopped`] /
//! [`ServiceFailed`]), and scheduler events ([`TaskFired`] /
//! [`TaskCompleted`] / [`TaskFailed`] / [`TaskRetrying`]) and updates the
//! shared state on each event.
//!
//! ## What this crate does NOT do
//!
//! - **No default telemetry phone-home.** Nothing in this crate calls an
//!   external endpoint. Users that want OTLP / Prometheus pushgateway /
//!   anything-else-external must wire it explicitly by querying the
//!   [`HealthRegistry`] capability and exporting on their own cadence.
//! - **No bundled `/metrics` / `/healthz` HTTP endpoints.** Route
//!   registration is HTTP-specific and intentionally decoupled from this
//!   kernel-aligned crate. Users wire endpoints in their `App` directly
//!   using the [`HealthRegistry`] capability returned by
//!   `ctx.capability::<dyn HealthRegistry>()`.
//! - **No commitment to a hosted observability provider.** OTLP support,
//!   Prometheus scrape format, and other export shapes are future
//!   walastack-observability-* crates or opt-in features.
//!
//! ## Late-subscription caveat
//!
//! [`ObservabilityService`] subscribes to events at its `start`. Services
//! started *before* it (registered earlier in the [`RuntimeBuilder`])
//! will not appear in the health surface until they next publish an
//! event (typically [`ServiceStopped`] or [`ServiceFailed`]). The
//! recommended pattern is to register `ObservabilityPlugin` first.
//!
//! [`RuntimeStarted`]: walastack_runtime::RuntimeStarted
//! [`RuntimeStopping`]: walastack_runtime::RuntimeStopping
//! [`RuntimeStopped`]: walastack_runtime::RuntimeStopped
//! [`ServiceStarted`]: walastack_runtime::ServiceStarted
//! [`ServiceStopped`]: walastack_runtime::ServiceStopped
//! [`ServiceFailed`]: walastack_runtime::ServiceFailed
//! [`TaskFired`]: walastack_runtime::TaskFired
//! [`TaskCompleted`]: walastack_runtime::TaskCompleted
//! [`TaskFailed`]: walastack_runtime::TaskFailed
//! [`TaskRetrying`]: walastack_runtime::TaskRetrying
//! [`RuntimeBuilder`]: walastack_runtime::RuntimeBuilder

#![allow(clippy::missing_errors_doc)]
// tokio::select! generates pub(crate) items inside a private module.
#![allow(clippy::redundant_pub_crate)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::task::JoinHandle;
use walastack_runtime::{
    BoxedServiceFuture, CapabilityRegistry, Plugin, RestartPolicy, RuntimeStarted, RuntimeStopped,
    RuntimeStopping, Service, ServiceContext, ServiceError, ServiceFailed, ServicePlanner,
    ServiceStarted, ServiceStopped, TaskCompleted, TaskFailed, TaskFired, TaskRetrying,
};

// =========================================================================
// Public types
// =========================================================================

/// Overall health classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HealthStatus {
    /// All tracked Services are running cleanly.
    Ok,
    /// At least one tracked Service is in restart backoff but the
    /// kernel intends to recover it.
    Degraded,
    /// At least one tracked Service has failed terminally
    /// (`will_restart == false`).
    Failed,
    /// No observations yet — `ObservabilityService` has not received any
    /// events.
    Unknown,
}

/// Kernel lifecycle phase as observed by [`ObservabilityService`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RuntimePhase {
    /// Default before any kernel lifecycle event is observed. Persists
    /// until the first [`RuntimeStarted`] event arrives.
    ///
    /// [`RuntimeStarted`]: walastack_runtime::RuntimeStarted
    Initializing,
    /// Observed [`RuntimeStarted`].
    ///
    /// [`RuntimeStarted`]: walastack_runtime::RuntimeStarted
    Running,
    /// Observed [`RuntimeStopping`].
    ///
    /// [`RuntimeStopping`]: walastack_runtime::RuntimeStopping
    Stopping,
    /// Observed [`RuntimeStopped`].
    ///
    /// [`RuntimeStopped`]: walastack_runtime::RuntimeStopped
    Stopped,
}

/// Per-Service health snapshot.
#[derive(Clone, Debug)]
pub struct ServiceHealth {
    /// The Service's identity (matches `Service::name()`).
    pub name: String,
    /// Current status.
    pub status: HealthStatus,
    /// Number of times this Service has been started (initial + restarts).
    pub starts: u32,
    /// Number of failure events observed for this Service.
    pub failures: u32,
    /// Latest error message reported by a [`ServiceFailed`] event for
    /// this Service, if any.
    ///
    /// [`ServiceFailed`]: walastack_runtime::ServiceFailed
    pub last_error: Option<String>,
}

/// Running totals of scheduler and lifecycle events observed.
#[derive(Clone, Copy, Debug, Default)]
pub struct EventCounts {
    /// Total `ServiceStarted` events observed (counts restarts too).
    pub services_started: u64,
    /// Total `ServiceStopped` events.
    pub services_stopped: u64,
    /// Total `ServiceFailed` events.
    pub services_failed: u64,
    /// Total `TaskFired` events.
    pub tasks_fired: u64,
    /// Total `TaskCompleted` events.
    pub tasks_completed: u64,
    /// Total `TaskFailed` events.
    pub tasks_failed: u64,
    /// Total `TaskRetrying` events.
    pub tasks_retrying: u64,
}

/// Capability exposing aggregated observability state.
///
/// Registered by [`ObservabilityPlugin`] under the default capability
/// name. Participants needing health data call
/// `ctx.capability::<dyn HealthRegistry>()` and query the returned
/// trait object.
pub trait HealthRegistry: Send + Sync + 'static {
    /// Overall health classification across all tracked Services.
    fn overall(&self) -> HealthStatus;

    /// Current observed kernel lifecycle phase.
    fn runtime_phase(&self) -> RuntimePhase;

    /// Snapshot of every Service tracked by this registry.
    fn services(&self) -> Vec<ServiceHealth>;

    /// Snapshot for a specific Service, if tracked.
    fn service(&self, name: &str) -> Option<ServiceHealth>;

    /// Running totals of observed events.
    fn event_counts(&self) -> EventCounts;
}

// =========================================================================
// Internal state
// =========================================================================

#[derive(Default)]
struct ObservabilityState {
    services: HashMap<String, ServiceHealth>,
    counts: EventCounts,
    phase: RuntimePhaseSlot,
}

struct RuntimePhaseSlot(RuntimePhase);

impl Default for RuntimePhaseSlot {
    fn default() -> Self {
        Self(RuntimePhase::Initializing)
    }
}

impl ObservabilityState {
    fn on_service_started(&mut self, event: &ServiceStarted) {
        self.counts.services_started = self.counts.services_started.saturating_add(1);
        let entry = self
            .services
            .entry(event.name.clone())
            .or_insert_with(|| ServiceHealth {
                name: event.name.clone(),
                status: HealthStatus::Ok,
                starts: 0,
                failures: 0,
                last_error: None,
            });
        entry.status = HealthStatus::Ok;
        entry.starts = entry.starts.saturating_add(1);
    }

    fn on_service_stopped(&mut self, event: &ServiceStopped) {
        self.counts.services_stopped = self.counts.services_stopped.saturating_add(1);
        if let Some(entry) = self.services.get_mut(&event.name) {
            entry.status = HealthStatus::Ok;
        }
    }

    fn on_service_failed(&mut self, event: &ServiceFailed) {
        self.counts.services_failed = self.counts.services_failed.saturating_add(1);
        let entry = self
            .services
            .entry(event.name.clone())
            .or_insert_with(|| ServiceHealth {
                name: event.name.clone(),
                status: HealthStatus::Failed,
                starts: 0,
                failures: 0,
                last_error: None,
            });
        entry.failures = entry.failures.saturating_add(1);
        entry.last_error = Some(event.error.clone());
        entry.status = if event.will_restart {
            HealthStatus::Degraded
        } else {
            HealthStatus::Failed
        };
    }

    const fn on_task_fired(&mut self) {
        self.counts.tasks_fired = self.counts.tasks_fired.saturating_add(1);
    }

    const fn on_task_completed(&mut self) {
        self.counts.tasks_completed = self.counts.tasks_completed.saturating_add(1);
    }

    const fn on_task_failed(&mut self) {
        self.counts.tasks_failed = self.counts.tasks_failed.saturating_add(1);
    }

    const fn on_task_retrying(&mut self) {
        self.counts.tasks_retrying = self.counts.tasks_retrying.saturating_add(1);
    }

    fn overall(&self) -> HealthStatus {
        if self.services.is_empty() {
            return HealthStatus::Unknown;
        }
        let mut any_degraded = false;
        for svc in self.services.values() {
            match svc.status {
                HealthStatus::Failed => return HealthStatus::Failed,
                HealthStatus::Degraded => any_degraded = true,
                HealthStatus::Ok | HealthStatus::Unknown => {}
            }
        }
        if any_degraded {
            HealthStatus::Degraded
        } else {
            HealthStatus::Ok
        }
    }
}

// =========================================================================
// HealthRegistry implementation
// =========================================================================

struct StateBackedHealthRegistry {
    state: Arc<Mutex<ObservabilityState>>,
}

impl HealthRegistry for StateBackedHealthRegistry {
    fn overall(&self) -> HealthStatus {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .overall()
    }

    fn runtime_phase(&self) -> RuntimePhase {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .phase
            .0
    }

    fn services(&self) -> Vec<ServiceHealth> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .services
            .values()
            .cloned()
            .collect()
    }

    fn service(&self, name: &str) -> Option<ServiceHealth> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .services
            .get(name)
            .cloned()
    }

    fn event_counts(&self) -> EventCounts {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .counts
    }
}

// =========================================================================
// ObservabilityService
// =========================================================================

/// Long-running Service that subscribes to kernel lifecycle events and
/// updates the shared observability state.
///
/// Registered automatically by [`ObservabilityPlugin`] under the kernel
/// supervision tree with [`RestartPolicy::OneShot`].
pub struct ObservabilityService {
    state: Arc<Mutex<ObservabilityState>>,
}

impl ObservabilityService {
    const fn new(state: Arc<Mutex<ObservabilityState>>) -> Self {
        Self { state }
    }
}

impl Service for ObservabilityService {
    fn name(&self) -> &'static str {
        "observability"
    }

    fn start(
        &self,
        ctx: ServiceContext,
    ) -> BoxedServiceFuture<Result<JoinHandle<()>, ServiceError>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let mut svc_started = ctx.subscribe::<ServiceStarted>();
            let mut svc_stopped = ctx.subscribe::<ServiceStopped>();
            let mut svc_failed = ctx.subscribe::<ServiceFailed>();
            let mut runtime_started = ctx.subscribe::<RuntimeStarted>();
            let mut runtime_stopping = ctx.subscribe::<RuntimeStopping>();
            let mut runtime_stopped = ctx.subscribe::<RuntimeStopped>();
            let mut task_fired = ctx.subscribe::<TaskFired>();
            let mut task_completed = ctx.subscribe::<TaskCompleted>();
            let mut task_failed = ctx.subscribe::<TaskFailed>();
            let mut task_retrying = ctx.subscribe::<TaskRetrying>();
            let mut shutdown = ctx.shutdown_signal();

            let handle = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = shutdown.wait() => break,
                        evt = svc_started.recv() => {
                            if let Ok(event) = evt {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .on_service_started(&event);
                            }
                        }
                        evt = svc_stopped.recv() => {
                            if let Ok(event) = evt {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .on_service_stopped(&event);
                            }
                        }
                        evt = svc_failed.recv() => {
                            if let Ok(event) = evt {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .on_service_failed(&event);
                            }
                        }
                        evt = runtime_started.recv() => {
                            if evt.is_ok() {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .phase = RuntimePhaseSlot(RuntimePhase::Running);
                            }
                        }
                        evt = runtime_stopping.recv() => {
                            if evt.is_ok() {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .phase = RuntimePhaseSlot(RuntimePhase::Stopping);
                            }
                        }
                        evt = runtime_stopped.recv() => {
                            if evt.is_ok() {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .phase = RuntimePhaseSlot(RuntimePhase::Stopped);
                            }
                        }
                        evt = task_fired.recv() => {
                            if evt.is_ok() {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .on_task_fired();
                            }
                        }
                        evt = task_completed.recv() => {
                            if evt.is_ok() {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .on_task_completed();
                            }
                        }
                        evt = task_failed.recv() => {
                            if evt.is_ok() {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .on_task_failed();
                            }
                        }
                        evt = task_retrying.recv() => {
                            if evt.is_ok() {
                                state.lock().unwrap_or_else(PoisonError::into_inner)
                                    .on_task_retrying();
                            }
                        }
                    }
                }
            });

            Ok(handle)
        })
    }
}

impl std::fmt::Debug for ObservabilityService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservabilityService")
            .finish_non_exhaustive()
    }
}

// =========================================================================
// ObservabilityPlugin
// =========================================================================

/// Plugin that registers the [`ObservabilityService`] and exposes the
/// [`HealthRegistry`] capability.
///
/// Recommended to register **first** in the `RuntimeBuilder` chain so
/// that subsequent Services' lifecycle events are captured from the
/// start.
pub struct ObservabilityPlugin {
    state: Arc<Mutex<ObservabilityState>>,
}

impl ObservabilityPlugin {
    /// Construct an `ObservabilityPlugin` with fresh shared state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ObservabilityState::default())),
        }
    }
}

impl Default for ObservabilityPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for ObservabilityPlugin {
    fn name(&self) -> &'static str {
        "observability"
    }

    fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
        let provider: Arc<dyn HealthRegistry> = Arc::new(StateBackedHealthRegistry {
            state: Arc::clone(&self.state),
        });
        registry.register_default::<dyn HealthRegistry>(provider);
    }

    fn register_services(&self, planner: &mut ServicePlanner) {
        let service = ObservabilityService::new(Arc::clone(&self.state));
        planner.add_supervised(service, RestartPolicy::OneShot);
    }
}

impl std::fmt::Debug for ObservabilityPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservabilityPlugin")
            .finish_non_exhaustive()
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::time::Duration;

    use walastack_runtime::{Runtime, ServiceFailed, ServiceStarted, ServiceStopped};

    use super::*;

    /// Brief delay to let the `ObservabilityService` consume an event.
    async fn yield_to_service() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ---- HealthRegistry capability ----

    #[tokio::test]
    async fn plugin_registers_health_registry_capability() {
        let runtime = Runtime::builder()
            .with_plugin(ObservabilityPlugin::new())
            .build()
            .unwrap();
        let registry = runtime.context().capability::<dyn HealthRegistry>();
        assert!(registry.is_some());
    }

    #[tokio::test]
    async fn fresh_registry_reports_initializing_and_unknown() {
        let runtime = Runtime::builder()
            .with_plugin(ObservabilityPlugin::new())
            .build()
            .unwrap();
        let registry = runtime
            .context()
            .capability::<dyn HealthRegistry>()
            .unwrap();
        assert_eq!(registry.runtime_phase(), RuntimePhase::Initializing);
        assert_eq!(registry.overall(), HealthStatus::Unknown);
        assert!(registry.services().is_empty());
        let counts = registry.event_counts();
        assert_eq!(counts.services_started, 0);
    }

    /// Build a started `Runtime` with the `ObservabilityPlugin`,
    /// returning the `(runtime, registry)` pair after letting the
    /// Service subscribe.
    async fn started_runtime() -> (
        walastack_runtime::Runtime,
        std::sync::Arc<dyn HealthRegistry>,
    ) {
        let mut runtime = Runtime::builder()
            .with_plugin(ObservabilityPlugin::new())
            .build()
            .unwrap();
        let registry = runtime
            .context()
            .capability::<dyn HealthRegistry>()
            .unwrap();
        runtime.start().await.unwrap();
        yield_to_service().await;
        (runtime, registry)
    }

    // ---- Runtime lifecycle event aggregation ----

    #[tokio::test]
    async fn runtime_lifecycle_updates_phase_to_running() {
        let (_runtime, registry) = started_runtime().await;
        // `runtime.start()` already published `RuntimeStarted`, which the
        // observability subscriber will have consumed by the time
        // `started_runtime` yielded.
        assert_eq!(registry.runtime_phase(), RuntimePhase::Running);
        // Note: post-shutdown phase observation (`RuntimePhase::Stopped`)
        // is not testable through the current Service shutdown path —
        // the SupervisionTree aborts the observer task when the kernel
        // shutdown signal fires, which happens before `RuntimeStopped`
        // is published. A future RFC could add a "drain pending events
        // before bus drop" hook to support full lifecycle observability.
    }

    // ---- Service lifecycle event aggregation ----

    #[tokio::test]
    async fn service_started_event_records_health() {
        let (runtime, registry) = started_runtime().await;
        runtime.context().publish(ServiceStarted {
            name: "test-svc".into(),
            attempt: 1,
        });
        yield_to_service().await;

        let svc = registry.service("test-svc").unwrap();
        assert_eq!(svc.name, "test-svc");
        assert_eq!(svc.status, HealthStatus::Ok);
        assert_eq!(svc.starts, 1);
        assert_eq!(svc.failures, 0);
    }

    #[tokio::test]
    async fn service_failed_with_will_restart_marks_degraded() {
        let (runtime, registry) = started_runtime().await;
        runtime.context().publish(ServiceFailed {
            name: "test-svc".into(),
            error: "boom".into(),
            attempt: 1,
            will_restart: true,
        });
        yield_to_service().await;

        let svc = registry.service("test-svc").unwrap();
        assert_eq!(svc.status, HealthStatus::Degraded);
        assert_eq!(svc.failures, 1);
        assert_eq!(svc.last_error.as_deref(), Some("boom"));
        assert_eq!(registry.overall(), HealthStatus::Degraded);
    }

    #[tokio::test]
    async fn service_failed_without_restart_marks_failed() {
        let (runtime, registry) = started_runtime().await;
        runtime.context().publish(ServiceFailed {
            name: "test-svc".into(),
            error: "fatal".into(),
            attempt: 3,
            will_restart: false,
        });
        yield_to_service().await;

        let svc = registry.service("test-svc").unwrap();
        assert_eq!(svc.status, HealthStatus::Failed);
        assert_eq!(registry.overall(), HealthStatus::Failed);
    }

    #[tokio::test]
    async fn service_stopped_clears_to_ok() {
        let (runtime, registry) = started_runtime().await;
        runtime.context().publish(ServiceStarted {
            name: "test-svc".into(),
            attempt: 1,
        });
        runtime.context().publish(ServiceStopped {
            name: "test-svc".into(),
        });
        yield_to_service().await;

        let svc = registry.service("test-svc").unwrap();
        assert_eq!(svc.status, HealthStatus::Ok);
        assert_eq!(registry.event_counts().services_stopped, 1);
    }

    // ---- Scheduler event aggregation ----

    #[tokio::test]
    async fn task_events_increment_counters() {
        use walastack_runtime::{
            Policies, TaskCompleted, TaskFailed, TaskFired, TaskRetrying, Trigger,
        };

        let (runtime, registry) = started_runtime().await;

        // Obtain a real ScheduleId from a scheduled handle (the task is
        // immediately cancelled so it never actually fires; we only
        // need the ID to construct synthetic events below).
        let handle = runtime.context().schedule(
            Trigger::After(Duration::from_secs(3600)),
            Policies::new(),
            || async { Ok(()) },
        );
        let id = handle.id();
        handle.cancel();

        runtime.context().publish(TaskFired {
            schedule_id: id,
            trigger_count: 1,
        });
        runtime.context().publish(TaskCompleted {
            schedule_id: id,
            duration: Duration::from_millis(5),
        });
        runtime.context().publish(TaskFailed {
            schedule_id: id,
            error: "x".into(),
            total_attempts: 2,
        });
        runtime.context().publish(TaskRetrying {
            schedule_id: id,
            next_attempt: 2,
            delay: Duration::from_millis(1),
        });
        yield_to_service().await;

        let counts = registry.event_counts();
        assert_eq!(counts.tasks_fired, 1);
        assert_eq!(counts.tasks_completed, 1);
        assert_eq!(counts.tasks_failed, 1);
        assert_eq!(counts.tasks_retrying, 1);
    }

    // ---- Multi-service tracking ----

    #[tokio::test]
    async fn multiple_services_tracked_independently() {
        let (runtime, registry) = started_runtime().await;
        runtime.context().publish(ServiceStarted {
            name: "svc-a".into(),
            attempt: 1,
        });
        runtime.context().publish(ServiceStarted {
            name: "svc-b".into(),
            attempt: 1,
        });
        runtime.context().publish(ServiceFailed {
            name: "svc-b".into(),
            error: "down".into(),
            attempt: 1,
            will_restart: false,
        });
        yield_to_service().await;

        // ObservabilityService itself self-registers via its own
        // ServiceStarted event during runtime startup, so the total
        // includes "observability" as a tracked Service.
        assert!(registry.services().len() >= 3);
        assert_eq!(registry.service("svc-a").unwrap().status, HealthStatus::Ok);
        assert_eq!(
            registry.service("svc-b").unwrap().status,
            HealthStatus::Failed
        );
        assert_eq!(registry.overall(), HealthStatus::Failed);
    }

    // ---- Overall health derivation ----

    #[tokio::test]
    async fn overall_health_is_failed_if_any_service_failed() {
        let (runtime, registry) = started_runtime().await;
        // Yields between same-service publishes ensure the observer's
        // `tokio::select!` loop sees the events in publish-order across
        // distinct broadcast channels.
        runtime.context().publish(ServiceStarted {
            name: "ok".into(),
            attempt: 1,
        });
        yield_to_service().await;
        runtime.context().publish(ServiceStarted {
            name: "degraded".into(),
            attempt: 1,
        });
        yield_to_service().await;
        runtime.context().publish(ServiceFailed {
            name: "degraded".into(),
            error: "x".into(),
            attempt: 1,
            will_restart: true,
        });
        yield_to_service().await;
        runtime.context().publish(ServiceStarted {
            name: "failed".into(),
            attempt: 1,
        });
        yield_to_service().await;
        runtime.context().publish(ServiceFailed {
            name: "failed".into(),
            error: "fatal".into(),
            attempt: 1,
            will_restart: false,
        });
        yield_to_service().await;

        // failed > degraded > ok — overall reports the worst.
        assert_eq!(registry.overall(), HealthStatus::Failed);
    }

    #[tokio::test]
    async fn overall_health_is_degraded_when_no_failed_services() {
        let (runtime, registry) = started_runtime().await;
        runtime.context().publish(ServiceStarted {
            name: "ok".into(),
            attempt: 1,
        });
        yield_to_service().await;
        runtime.context().publish(ServiceStarted {
            name: "degraded".into(),
            attempt: 1,
        });
        yield_to_service().await;
        runtime.context().publish(ServiceFailed {
            name: "degraded".into(),
            error: "transient".into(),
            attempt: 1,
            will_restart: true,
        });
        yield_to_service().await;

        assert_eq!(registry.overall(), HealthStatus::Degraded);
    }
}

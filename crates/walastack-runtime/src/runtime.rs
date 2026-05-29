//! The kernel composition point.
//!
//! The [`Runtime`] is what every WalaStack deployment ultimately runs.
//! It owns the kernel facilities ([`crate::ResourceRegistry`],
//! [`crate::CapabilityRegistry`], [`crate::EventBus`],
//! [`crate::Scheduler`], [`crate::SupervisionTree`]), accepts Services
//! during the Configure phase, sequences the kernel lifecycle, and
//! blocks until shutdown.
//!
//! ## Lifecycle phases
//!
//! 1. **Configure** — [`RuntimeBuilder`] accepts resources, capabilities,
//!    and supervised Services from user code.
//! 2. **Init** — [`RuntimeBuilder::build`] freezes the resource and
//!    capability registries into their `Arc`-shared views and constructs
//!    the kernel context.
//! 3. **Start** — [`Runtime::start`] publishes [`crate::RuntimeStarting`],
//!    starts each registered Service through the `SupervisionTree`, then
//!    publishes [`crate::RuntimeStarted`].
//! 4. **Run** — [`Runtime::wait_for_shutdown`] awaits the kernel shutdown
//!    signal. Services run concurrently under supervision.
//! 5. **Shutdown** — [`Runtime::shutdown_gracefully`] publishes
//!    [`crate::RuntimeStopping`] (via [`crate::EventBus::shutdown`]),
//!    drains supervised Services up to a deadline, then publishes
//!    [`crate::RuntimeStopped`].
//!
//! The convenience [`Runtime::run`] runs all four phases in sequence.
//!
//! See the
//! [Runtime Kernel — Overview](https://walastack.com/docs/architecture/runtime/overview/)
//! architecture page for design rationale.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::capabilities::{CapabilityName, CapabilityRegistry};
use crate::context::RuntimeContext;
use crate::events::{EventBus, RuntimeStarted, RuntimeStarting, RuntimeStopped, ShutdownSignal};
use crate::plugins::{Plugin, PluginError, PluginManager, ServicePlanner};
use crate::resources::ResourceRegistry;
use crate::scheduler::Scheduler;
use crate::services::{Service, ServiceError};
use crate::supervision::{RestartPolicy, SupervisionTree};

/// Default deadline for graceful shutdown.
pub const DEFAULT_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);

// =========================================================================
// RuntimeError
// =========================================================================

/// Errors returned by the [`Runtime`] kernel.
#[derive(Debug)]
pub enum RuntimeError {
    /// A Service failed to start during the Start phase. The Runtime
    /// aborts startup; previously-started Services are left running and
    /// can be drained by the caller via
    /// [`Runtime::shutdown_gracefully`].
    ServiceStart {
        /// Name of the Service that failed.
        name: String,
        /// The Service's reported error.
        source: ServiceError,
    },
    /// A plugin reported a problem during build — typically an unmet
    /// capability requirement caught by fail-fast validation.
    Plugin(PluginError),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServiceStart { name, source } => {
                write!(f, "service {name:?} failed to start: {source}")
            }
            Self::Plugin(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ServiceStart { source, .. } => Some(source),
            Self::Plugin(source) => Some(source),
        }
    }
}

impl From<PluginError> for RuntimeError {
    fn from(err: PluginError) -> Self {
        Self::Plugin(err)
    }
}

// =========================================================================
// RuntimeBuilder
// =========================================================================

/// Builder for assembling a [`Runtime`].
///
/// Accepts resources, capabilities, and supervised Services during the
/// Configure phase. [`Self::build`] freezes the registries and produces
/// a [`Runtime`] ready to start.
pub struct RuntimeBuilder {
    resources: ResourceRegistry,
    capabilities: CapabilityRegistry,
    services: Vec<SupervisedService>,
    plugins: PluginManager,
    shutdown_deadline: Duration,
}

struct SupervisedService {
    service: Arc<dyn Service>,
    policy: RestartPolicy,
}

impl RuntimeBuilder {
    /// Construct an empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            resources: ResourceRegistry::new(),
            capabilities: CapabilityRegistry::new(),
            services: Vec::new(),
            plugins: PluginManager::new(),
            shutdown_deadline: DEFAULT_SHUTDOWN_DEADLINE,
        }
    }

    /// Register a typed shared resource.
    #[must_use]
    pub fn with_resource<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        self.resources.insert(value);
        self
    }

    /// Register a typed shared resource by `Arc`.
    #[must_use]
    pub fn with_resource_arc<T: Send + Sync + 'static>(mut self, value: Arc<T>) -> Self {
        self.resources.insert_arc(value);
        self
    }

    /// Register a Capability provider under the given name.
    #[must_use]
    pub fn with_capability<C>(mut self, name: impl Into<CapabilityName>, provider: Arc<C>) -> Self
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.capabilities.register(name, provider);
        self
    }

    /// Register a Capability provider under the conventional default
    /// name.
    #[must_use]
    pub fn with_default_capability<C>(mut self, provider: Arc<C>) -> Self
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.capabilities.register_default(provider);
        self
    }

    /// Register a Service with the default [`RestartPolicy::OneShot`].
    ///
    /// This is the most common form. For restart-on-failure semantics,
    /// use [`Self::with_supervised`].
    #[must_use]
    pub fn with<S: Service>(self, service: S) -> Self {
        self.with_supervised(service, RestartPolicy::OneShot)
    }

    /// Register a Service with an explicit [`RestartPolicy`].
    #[must_use]
    pub fn with_supervised<S: Service>(mut self, service: S, policy: RestartPolicy) -> Self {
        self.services.push(SupervisedService {
            service: Arc::new(service),
            policy,
        });
        self
    }

    /// Register a Service via an existing `Arc<dyn Service>`.
    ///
    /// Useful when the same Service instance is shared with other
    /// participants outside the kernel (rare).
    #[must_use]
    pub fn with_arc(mut self, service: Arc<dyn Service>, policy: RestartPolicy) -> Self {
        self.services.push(SupervisedService { service, policy });
        self
    }

    /// Register a [`Plugin`] with the kernel.
    ///
    /// Plugins apply in registration order during [`Self::build`]; each
    /// plugin's `register_resources` / `register_capabilities` /
    /// `register_services` hooks run after every previously-registered
    /// plugin's hooks.
    #[must_use]
    pub fn with_plugin<P: Plugin>(mut self, plugin: P) -> Self {
        self.plugins.register(Arc::new(plugin));
        self
    }

    /// Register a plugin via an existing `Arc<dyn Plugin>`.
    #[must_use]
    pub fn with_plugin_arc(mut self, plugin: Arc<dyn Plugin>) -> Self {
        self.plugins.register(plugin);
        self
    }

    /// Set the deadline used during graceful shutdown.
    ///
    /// Defaults to [`DEFAULT_SHUTDOWN_DEADLINE`].
    #[must_use]
    pub const fn with_shutdown_deadline(mut self, deadline: Duration) -> Self {
        self.shutdown_deadline = deadline;
        self
    }

    /// Apply registered plugins, freeze the registries, validate
    /// capability requirements, and construct the [`Runtime`].
    ///
    /// The Runtime is not started yet — call [`Runtime::start`] or use
    /// [`Self::run`] to drive the full lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Plugin`] when a registered plugin's
    /// required capabilities cannot be satisfied by the post-registration
    /// capability set. This is the kernel's fail-fast validation step;
    /// no Service is started if validation fails.
    pub fn build(mut self) -> Result<Runtime, RuntimeError> {
        // Configure: let plugins register resources, capabilities, services.
        let mut planner = ServicePlanner::new();
        self.plugins
            .apply_all(&mut self.resources, &mut self.capabilities, &mut planner);

        // Transfer planner-staged services into the builder's vec.
        for (service, policy) in planner.drain() {
            self.services.push(SupervisedService { service, policy });
        }

        // Init: freeze registries.
        let resources = self.resources.build();
        let capabilities = self.capabilities.build();

        // Validate: fail-fast on unmet capability requirements.
        self.plugins.validate_requirements(&capabilities)?;

        // Construct kernel facilities.
        let events = EventBus::new();
        let scheduler = Scheduler::with_events(events.clone());
        let context = RuntimeContext::new(resources, capabilities, events.clone(), scheduler);
        let supervision = SupervisionTree::new(events);

        Ok(Runtime {
            context,
            supervision,
            services: self.services,
            shutdown_deadline: self.shutdown_deadline,
        })
    }

    /// Build the Runtime, start it, and run until shutdown is signaled.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Plugin`] if plugin validation fails
    /// during [`Self::build`]. Otherwise propagates the first Service
    /// start failure from [`Runtime::start`]. Shutdown is still
    /// attempted after a failed start so that any already-started
    /// Services drain.
    pub async fn run(self) -> Result<(), RuntimeError> {
        let mut runtime = self.build()?;
        let start_result = runtime.start().await;
        if start_result.is_ok() {
            runtime.wait_for_shutdown().await;
        }
        runtime.shutdown_gracefully().await;
        start_result
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for RuntimeBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeBuilder")
            .field("services", &self.services.len())
            .field("plugins", &self.plugins.len())
            .field("shutdown_deadline", &self.shutdown_deadline)
            .finish_non_exhaustive()
    }
}

// =========================================================================
// Runtime
// =========================================================================

/// The kernel composition point.
///
/// Constructed via [`RuntimeBuilder::build`] (or implicitly by
/// [`RuntimeBuilder::run`]). Owns the kernel facilities and coordinates
/// Service lifecycle.
pub struct Runtime {
    context: RuntimeContext,
    supervision: SupervisionTree,
    services: Vec<SupervisedService>,
    shutdown_deadline: Duration,
}

impl Runtime {
    /// Construct a [`RuntimeBuilder`].
    #[must_use]
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::new()
    }

    /// Borrow the kernel [`RuntimeContext`].
    #[must_use]
    pub const fn context(&self) -> &RuntimeContext {
        &self.context
    }

    /// Borrow the kernel [`SupervisionTree`].
    #[must_use]
    pub const fn supervision(&self) -> &SupervisionTree {
        &self.supervision
    }

    /// Get a clone-able [`ShutdownSignal`] for the kernel.
    ///
    /// Equivalent to [`RuntimeContext::shutdown_signal`].
    #[must_use]
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.context.shutdown_signal()
    }

    /// Run the Start phase: publish [`RuntimeStarting`], start every
    /// registered Service under supervision, then publish
    /// [`RuntimeStarted`].
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::ServiceStart`] on the first Service that
    /// fails to start. Previously-started Services remain under
    /// supervision; the caller may invoke [`Self::shutdown_gracefully`]
    /// to drain them.
    pub async fn start(&mut self) -> Result<(), RuntimeError> {
        self.context.publish(RuntimeStarting);

        let services = std::mem::take(&mut self.services);
        for SupervisedService { service, policy } in services {
            let name = service.name().to_string();
            self.supervision
                .start_service(service, policy, self.context.clone())
                .await
                .map_err(|source| RuntimeError::ServiceStart { name, source })?;
        }

        self.context.publish(RuntimeStarted);
        Ok(())
    }

    /// Block until the kernel shutdown signal is asserted.
    ///
    /// The signal can be asserted by:
    /// - external code calling [`RuntimeContext::events()`]
    ///   `.shutdown()` (or [`crate::EventBus::shutdown`] directly),
    /// - OS signal plumbing (see [`crate::wait_for_shutdown_signal`]),
    /// - a Service or scheduled task that explicitly shuts the bus.
    pub async fn wait_for_shutdown(&self) {
        let mut signal = self.context.shutdown_signal();
        signal.wait().await;
    }

    /// Run the Shutdown phase: assert the shutdown signal, drain
    /// supervised Services up to the configured deadline, then publish
    /// [`RuntimeStopped`].
    ///
    /// Idempotent — calling shut down more than once is safe; the first
    /// call performs the work and subsequent calls are no-ops.
    pub async fn shutdown_gracefully(&mut self) {
        if self.context.events().is_shut_down() {
            // Already-shut-down kernel — drain is still safe but
            // RuntimeStopping was already published by the first
            // shutdown call.
        } else {
            self.context.events().shutdown();
        }

        let _graceful = self.supervision.drain(self.shutdown_deadline).await;
        self.context.publish(RuntimeStopped);
    }

    /// Convenience: start, wait for shutdown, drain.
    ///
    /// # Errors
    ///
    /// Propagates the first Service start failure from [`Self::start`].
    pub async fn run(mut self) -> Result<(), RuntimeError> {
        let start_result = self.start().await;
        if start_result.is_ok() {
            self.wait_for_shutdown().await;
        }
        self.shutdown_gracefully().await;
        start_result
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("pending_services", &self.services.len())
            .field("supervision", &self.supervision)
            .field("shutdown_deadline", &self.shutdown_deadline)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::similar_names,
        clippy::unnecessary_literal_bound,
        clippy::items_after_statements
    )]

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    use super::*;
    use crate::events::{RuntimeStarted, RuntimeStarting, RuntimeStopping};
    use crate::services::{BoxedServiceFuture, ServiceContext};
    use crate::supervision::{ServiceStarted, ServiceStopped};

    struct WaitingService {
        name: String,
        starts: Arc<AtomicU32>,
    }

    impl Service for WaitingService {
        fn name(&self) -> &str {
            &self.name
        }

        fn start(
            &self,
            ctx: ServiceContext,
        ) -> BoxedServiceFuture<Result<JoinHandle<()>, ServiceError>> {
            let starts = Arc::clone(&self.starts);
            Box::pin(async move {
                starts.fetch_add(1, Ordering::SeqCst);
                let mut signal = ctx.shutdown_signal();
                let handle = tokio::spawn(async move { signal.wait().await });
                Ok(handle)
            })
        }
    }

    struct FailingStartService {
        name: String,
    }

    impl Service for FailingStartService {
        fn name(&self) -> &str {
            &self.name
        }

        fn start(
            &self,
            _ctx: ServiceContext,
        ) -> BoxedServiceFuture<Result<JoinHandle<()>, ServiceError>> {
            Box::pin(async { Err(ServiceError::new("bind failed")) })
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct AppConfig {
        name: &'static str,
    }

    #[tokio::test]
    async fn builder_constructs_runtime_with_resources_and_capabilities() {
        let runtime = Runtime::builder()
            .with_resource(AppConfig { name: "wala" })
            .build()
            .unwrap();
        assert_eq!(
            runtime.context().resource::<AppConfig>().unwrap().name,
            "wala"
        );
    }

    #[tokio::test]
    async fn start_publishes_runtime_starting_and_started() {
        let mut runtime = Runtime::builder().build().unwrap();
        let mut starting_sub = runtime.context.subscribe::<RuntimeStarting>();
        let mut started_sub = runtime.context.subscribe::<RuntimeStarted>();

        runtime.start().await.unwrap();

        let _ = timeout(Duration::from_secs(1), starting_sub.recv())
            .await
            .unwrap()
            .unwrap();
        let _ = timeout(Duration::from_secs(1), started_sub.recv())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn start_starts_registered_services() {
        let starts = Arc::new(AtomicU32::new(0));
        let svc = WaitingService {
            name: "svc-a".into(),
            starts: Arc::clone(&starts),
        };

        let mut runtime = Runtime::builder().with(svc).build().unwrap();
        let mut started_sub = runtime.context.subscribe::<ServiceStarted>();
        runtime.start().await.unwrap();

        let event = timeout(Duration::from_secs(1), started_sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.name, "svc-a");
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        runtime.context().events().shutdown();
    }

    #[tokio::test]
    async fn start_propagates_service_start_error() {
        let mut runtime = Runtime::builder()
            .with(FailingStartService { name: "bad".into() })
            .build()
            .unwrap();

        let err = runtime.start().await.unwrap_err();
        match err {
            RuntimeError::ServiceStart { name, source } => {
                assert_eq!(name, "bad");
                assert!(source.message.contains("bind failed"));
            }
            RuntimeError::Plugin(_) => panic!("expected ServiceStart, got Plugin"),
        }
    }

    #[tokio::test]
    async fn shutdown_gracefully_publishes_stopping_and_stopped() {
        let mut runtime = Runtime::builder()
            .with(WaitingService {
                name: "w".into(),
                starts: Arc::new(AtomicU32::new(0)),
            })
            .with_shutdown_deadline(Duration::from_secs(2))
            .build()
            .unwrap();

        let mut stopping_sub = runtime.context.subscribe::<RuntimeStopping>();
        let mut stopped_sub = runtime.context.subscribe::<RuntimeStopped>();
        let mut svc_stopped_sub = runtime.context.subscribe::<ServiceStopped>();

        runtime.start().await.unwrap();
        runtime.shutdown_gracefully().await;

        // Generous timeouts: full-workspace test runs put high
        // contention on tokio runtime + broadcast internals.
        let _ = timeout(Duration::from_secs(5), stopping_sub.recv())
            .await
            .unwrap()
            .unwrap();
        let _ = timeout(Duration::from_secs(5), stopped_sub.recv())
            .await
            .unwrap()
            .unwrap();
        // The Service exits because its signal is the same shutdown.
        let svc_stopped = timeout(Duration::from_secs(5), svc_stopped_sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(svc_stopped.name, "w");
    }

    #[tokio::test]
    async fn run_runs_full_lifecycle_until_shutdown_signal() {
        let runtime = Runtime::builder()
            .with(WaitingService {
                name: "w".into(),
                starts: Arc::new(AtomicU32::new(0)),
            })
            .with_shutdown_deadline(Duration::from_secs(2))
            .build()
            .unwrap();

        // Trigger shutdown after a brief delay.
        let bus = runtime.context.events().clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            bus.shutdown();
        });

        timeout(Duration::from_secs(5), runtime.run())
            .await
            .expect("run should resolve within deadline")
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_is_idempotent() {
        let mut runtime = Runtime::builder().build().unwrap();
        runtime.start().await.unwrap();
        runtime.shutdown_gracefully().await;
        runtime.shutdown_gracefully().await; // should not panic
    }

    // ---- Plugin integration (Phase 2.0.f) ----

    trait DummyDb: Send + Sync + 'static {}
    struct DummyDbImpl;
    impl DummyDb for DummyDbImpl {}

    struct DbPlugin;
    impl crate::plugins::Plugin for DbPlugin {
        fn name(&self) -> &str {
            "db"
        }
        fn register_capabilities(&self, reg: &mut CapabilityRegistry) {
            reg.register_default::<dyn DummyDb>(Arc::new(DummyDbImpl));
        }
    }

    struct AppPlugin;
    impl crate::plugins::Plugin for AppPlugin {
        fn name(&self) -> &str {
            "app"
        }
        fn required_capabilities(&self) -> Vec<crate::plugins::CapabilityRequirement> {
            vec![crate::plugins::CapabilityRequirement::any::<dyn DummyDb>()]
        }
    }

    #[tokio::test]
    async fn builder_with_plugin_registers_capabilities() {
        let runtime = Runtime::builder().with_plugin(DbPlugin).build().unwrap();
        assert!(runtime.context().capability::<dyn DummyDb>().is_some());
    }

    #[tokio::test]
    async fn builder_validates_required_capabilities_satisfied() {
        let runtime = Runtime::builder()
            .with_plugin(DbPlugin)
            .with_plugin(AppPlugin)
            .build();
        assert!(runtime.is_ok());
    }

    #[tokio::test]
    async fn builder_rejects_unsatisfied_required_capabilities() {
        let err = Runtime::builder()
            .with_plugin(AppPlugin)
            .build()
            .unwrap_err();
        match err {
            RuntimeError::Plugin(crate::plugins::PluginError::MissingRequirement {
                plugin,
                requirement,
            }) => {
                assert_eq!(plugin, "app");
                assert!(requirement.contains("DummyDb"));
            }
            other => panic!("expected Plugin/MissingRequirement, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn plugin_can_register_a_service_via_planner() {
        let starts = Arc::new(AtomicU32::new(0));
        let starts_clone = Arc::clone(&starts);

        struct ServicePlugin {
            starts: Arc<AtomicU32>,
        }
        impl crate::plugins::Plugin for ServicePlugin {
            fn name(&self) -> &str {
                "svc-plugin"
            }
            fn register_services(&self, planner: &mut crate::plugins::ServicePlanner) {
                planner.add(WaitingService {
                    name: "from-plugin".into(),
                    starts: Arc::clone(&self.starts),
                });
            }
        }

        let mut runtime = Runtime::builder()
            .with_plugin(ServicePlugin {
                starts: starts_clone,
            })
            .build()
            .unwrap();

        runtime.start().await.unwrap();
        // Brief delay so the plugin-registered service has actually run.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        runtime.context().events().shutdown();
    }
}

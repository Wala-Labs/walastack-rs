//! The Service trait and `ServiceContext`.
//!
//! A [`Service`] is a long-running participant in the Runtime: an HTTP
//! server, an agent loop, an offline sync engine, a job queue. Services
//! are not infrastructure — the kernel owns Services through the
//! [`crate::SupervisionTree`].
//!
//! ## The contract
//!
//! - **Identity** — every Service has a name used for logs, supervision,
//!   and the kernel-published [`crate::supervision::ServiceStarted`] /
//!   [`crate::supervision::ServiceStopped`] /
//!   [`crate::supervision::ServiceFailed`] events.
//! - **Start** — `start(&self, ctx)` produces a future that spawns the
//!   service's root work and returns its [`tokio::task::JoinHandle`].
//!   `&self` (not `Box<Self>`) so the `SupervisionTree` can call `start`
//!   again on restart.
//! - **Shutdown** — there is no explicit `shutdown` hook. Services that
//!   need graceful drain subscribe to
//!   [`crate::ShutdownSignal`] obtained via
//!   [`ServiceContext::shutdown_signal`] inside their started task.
//!   The kernel awaits the returned [`tokio::task::JoinHandle`] with a
//!   deadline; tasks that exit themselves on the shutdown signal drain
//!   cleanly.
//!
//! ## Cross-Service interaction
//!
//! Services interact through the four kernel-provided mechanisms:
//!
//! 1. **Capability call** — typed data access through swappable providers.
//! 2. **Direct Arc handle** — only for co-owned hot paths.
//! 3. **Event publish** — fan-out coordination.
//! 4. **Scheduled trigger** — time-driven work.
//!
//! Services **must not** depend on other Services by type. Cross-Service
//! data access goes through Capabilities; cross-Service coordination goes
//! through Events. See
//! [Service Communication](https://walastack.com/docs/architecture/runtime/service-communication/)
//! for the full rules.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::capabilities::Capabilities;
use crate::context::RuntimeContext;
use crate::events::{EnqueueError, EventBus, PublishOutcome, ShutdownSignal, Subscriber, Worker};
use crate::resources::Resources;
use crate::scheduler::{Policies, ScheduleHandle, ScheduledFn, Scheduler, Trigger};

/// Boxed future type returned by [`Service::start`].
pub type BoxedServiceFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A long-running participant in the Runtime.
///
/// The [`crate::SupervisionTree`] owns Services after registration. Each
/// Service produces a [`tokio::task::JoinHandle`] representing its root
/// spawned work; the kernel watches that handle for completion and
/// applies the configured [`crate::RestartPolicy`] on failure.
pub trait Service: Send + Sync + 'static {
    /// Human-readable name for logs, metrics, and supervision identity.
    ///
    /// Should be stable across restarts of the same Service instance —
    /// the kernel uses this string as the [`ServiceStarted`] /
    /// [`ServiceStopped`] / [`ServiceFailed`] event identity.
    ///
    /// [`ServiceStarted`]: crate::supervision::ServiceStarted
    /// [`ServiceStopped`]: crate::supervision::ServiceStopped
    /// [`ServiceFailed`]: crate::supervision::ServiceFailed
    fn name(&self) -> &str;

    /// Start the service.
    ///
    /// Returns a future that does start-time work (binding sockets,
    /// opening connections, spawning the root task) and resolves to a
    /// [`tokio::task::JoinHandle`] for the service's long-running root
    /// task. The kernel awaits the handle to observe service exit.
    ///
    /// Takes `&self` so the `SupervisionTree` can call `start` again on
    /// restart. Service implementations should hold shared state in
    /// [`Arc`]s and clone them into the spawned task.
    fn start(
        &self,
        ctx: ServiceContext,
    ) -> BoxedServiceFuture<Result<JoinHandle<()>, ServiceError>>;
}

/// The Service-scoped view of the kernel API.
///
/// Created by the `SupervisionTree` and passed to [`Service::start`]. A
/// `ServiceContext` is a [`RuntimeContext`] plus the Service's name —
/// the name lets future supervision and observability layers correlate
/// kernel operations with the originating Service.
///
/// All [`RuntimeContext`] operations are delegated through this type;
/// Services should reach kernel facilities exclusively via the
/// `ServiceContext`.
#[derive(Clone)]
pub struct ServiceContext {
    runtime: RuntimeContext,
    name: Arc<str>,
}

impl ServiceContext {
    /// Construct a new `ServiceContext` wrapping a [`RuntimeContext`]
    /// with the given Service name.
    #[must_use]
    pub fn new(runtime: RuntimeContext, name: impl Into<Arc<str>>) -> Self {
        Self {
            runtime,
            name: name.into(),
        }
    }

    /// The owning Service's identity.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Borrow the underlying [`RuntimeContext`].
    #[must_use]
    pub const fn runtime(&self) -> &RuntimeContext {
        &self.runtime
    }

    // ---- Resources ----

    /// See [`RuntimeContext::resource`].
    #[must_use]
    pub fn resource<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.runtime.resource::<T>()
    }

    /// See [`RuntimeContext::resources`].
    #[must_use]
    pub fn resources(&self) -> &Resources {
        self.runtime.resources()
    }

    // ---- Capabilities ----

    /// See [`RuntimeContext::capability`].
    #[must_use]
    pub fn capability<C>(&self) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.runtime.capability::<C>()
    }

    /// See [`RuntimeContext::capability_named`].
    #[must_use]
    pub fn capability_named<C>(&self, name: &str) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.runtime.capability_named::<C>(name)
    }

    /// See [`RuntimeContext::capabilities`].
    #[must_use]
    pub fn capabilities(&self) -> &Capabilities {
        self.runtime.capabilities()
    }

    // ---- Events ----

    /// See [`RuntimeContext::events`].
    #[must_use]
    pub fn events(&self) -> &EventBus {
        self.runtime.events()
    }

    /// See [`RuntimeContext::publish`].
    pub fn publish<E>(&self, event: E) -> PublishOutcome
    where
        E: Clone + Send + Sync + 'static,
    {
        self.runtime.publish(event)
    }

    /// See [`RuntimeContext::subscribe`].
    #[must_use]
    pub fn subscribe<E>(&self) -> Subscriber<E>
    where
        E: Clone + Send + Sync + 'static,
    {
        self.runtime.subscribe::<E>()
    }

    /// See [`RuntimeContext::enqueue`].
    ///
    /// # Errors
    ///
    /// Returns an [`EnqueueError`] when the work queue for `E` is closed.
    pub async fn enqueue<E>(&self, event: E) -> Result<(), EnqueueError<E>>
    where
        E: Send + 'static,
    {
        self.runtime.enqueue(event).await
    }

    /// See [`RuntimeContext::worker`].
    #[must_use]
    pub fn worker<E>(&self) -> Worker<E>
    where
        E: Send + 'static,
    {
        self.runtime.worker::<E>()
    }

    /// See [`RuntimeContext::shutdown_signal`].
    #[must_use]
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.runtime.shutdown_signal()
    }

    // ---- Scheduler ----

    /// See [`RuntimeContext::scheduler`].
    #[must_use]
    pub fn scheduler(&self) -> &Scheduler {
        self.runtime.scheduler()
    }

    /// See [`RuntimeContext::schedule`].
    pub fn schedule<F>(&self, trigger: Trigger, policies: Policies, task: F) -> ScheduleHandle
    where
        F: ScheduledFn,
    {
        self.runtime.schedule(trigger, policies, task)
    }
}

impl fmt::Debug for ServiceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceContext")
            .field("name", &self.name)
            .field("runtime", &self.runtime)
            .finish()
    }
}

/// Error returned when a Service fails to start or signals failure to
/// the `SupervisionTree`.
#[derive(Clone, Debug)]
pub struct ServiceError {
    /// Human-readable error message.
    pub message: String,
}

impl ServiceError {
    /// Construct a Service error from a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Construct a Service error from an underlying error's
    /// [`std::fmt::Display`] representation.
    pub fn from_error<E: fmt::Display>(err: &E) -> Self {
        Self::new(err.to_string())
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ServiceError {}

impl From<std::io::Error> for ServiceError {
    fn from(err: std::io::Error) -> Self {
        Self::from_error(&err)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    #[derive(Debug)]
    struct DbPool(u32);

    trait Llm: Send + Sync + 'static {
        fn name(&self) -> &'static str;
    }

    struct OpenAiClient;
    impl Llm for OpenAiClient {
        fn name(&self) -> &'static str {
            "openai"
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Tick;

    #[test]
    fn service_context_exposes_service_name() {
        let ctx = ServiceContext::new(RuntimeContext::empty(), "http");
        assert_eq!(ctx.name(), "http");
    }

    #[test]
    fn service_context_delegates_resource_access() {
        let mut registry = crate::resources::ResourceRegistry::new();
        registry.insert(DbPool(42));
        let runtime = RuntimeContext::new(
            registry.build(),
            Capabilities::empty(),
            EventBus::new(),
            Scheduler::new(),
        );
        let ctx = ServiceContext::new(runtime, "svc");
        assert_eq!(ctx.resource::<DbPool>().unwrap().0, 42);
    }

    #[test]
    fn service_context_delegates_capability_access() {
        let mut registry = crate::capabilities::CapabilityRegistry::new();
        registry.register_default::<dyn Llm>(Arc::new(OpenAiClient));
        let runtime = RuntimeContext::new(
            Resources::empty(),
            registry.build(),
            EventBus::new(),
            Scheduler::new(),
        );
        let ctx = ServiceContext::new(runtime, "svc");
        assert_eq!(ctx.capability::<dyn Llm>().unwrap().name(), "openai");
    }

    #[tokio::test]
    async fn service_context_publish_subscribe_round_trip() {
        let ctx = ServiceContext::new(RuntimeContext::empty(), "svc");
        let mut sub = ctx.subscribe::<Tick>();
        ctx.publish(Tick);
        let event = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event, Tick);
    }

    #[tokio::test]
    async fn service_context_shutdown_signal_observes_bus_shutdown() {
        let ctx = ServiceContext::new(RuntimeContext::empty(), "svc");
        let signal = ctx.shutdown_signal();
        assert!(!signal.is_shut_down());
        ctx.events().shutdown();
        assert!(signal.is_shut_down());
    }

    #[test]
    fn service_error_from_io_error_preserves_message() {
        let io_err = std::io::Error::new(std::io::ErrorKind::AddrInUse, "boom");
        let svc_err: ServiceError = io_err.into();
        assert!(svc_err.message.contains("boom"));
    }

    // ---- Service trait — implementable + restartable ----

    struct DummyService {
        name: String,
        invocations: Arc<std::sync::atomic::AtomicU32>,
    }

    impl Service for DummyService {
        fn name(&self) -> &str {
            &self.name
        }

        fn start(
            &self,
            ctx: ServiceContext,
        ) -> BoxedServiceFuture<Result<JoinHandle<()>, ServiceError>> {
            let invocations = Arc::clone(&self.invocations);
            Box::pin(async move {
                invocations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let mut signal = ctx.shutdown_signal();
                let handle = tokio::spawn(async move { signal.wait().await });
                Ok(handle)
            })
        }
    }

    #[tokio::test]
    async fn service_trait_can_be_implemented_and_called_repeatedly() {
        let svc = DummyService {
            name: "dummy".into(),
            invocations: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        };

        let ctx_a = ServiceContext::new(RuntimeContext::empty(), "dummy");
        let ctx_b = ServiceContext::new(RuntimeContext::empty(), "dummy");

        let h1 = svc.start(ctx_a).await.unwrap();
        let h2 = svc.start(ctx_b).await.unwrap();

        assert_eq!(svc.invocations.load(std::sync::atomic::Ordering::SeqCst), 2);
        h1.abort();
        h2.abort();
    }
}

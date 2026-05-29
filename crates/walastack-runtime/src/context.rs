//! The unified API surface for Runtime Kernel participants.
//!
//! [`RuntimeContext`] is the only legal path through which Services,
//! Plugins, scheduled tasks, event handlers, HTTP handlers, and Agent
//! steps reach kernel facilities. It is a cheap-to-clone `Arc` handle
//! analogous to Bevy's `World` access tokens, Tokio's
//! [`tokio::runtime::Handle`], and Temporal's `ActivityContext`.
//!
//! ## Phase 2.0.d scope
//!
//! `RuntimeContext` exposes resource, capability, event-bus, and
//! scheduler access plus shutdown signaling. Future sub-batches extend
//! the surface:
//!
//! - **2.0.e** — `ServiceContext` scoped variant for Service-level usage;
//!   `spawn` (supervised).
//! - **2.0.f** — `tracer`, `metrics`, `config` once observability + config
//!   plumbing lands.
//!
//! See the
//! [Runtime Kernel — RuntimeContext](https://walastack.com/docs/architecture/runtime/context/)
//! architecture page for the full design.

use std::fmt;
use std::sync::Arc;

use crate::capabilities::Capabilities;
use crate::events::{EnqueueError, EventBus, PublishOutcome, ShutdownSignal, Subscriber, Worker};
use crate::resources::Resources;
use crate::scheduler::{Policies, ScheduleHandle, ScheduledFn, Scheduler, Trigger};

/// The kernel API surface presented to every participant.
///
/// Cheap to clone (one atomic increment). Distributed to Services during
/// `start`, to Plugins during `init`, to scheduled tasks at invocation,
/// and to HTTP handlers via the extractor system (Phase 2.0.e+).
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use walastack_runtime::{
///     Capabilities, EventBus, ResourceRegistry, RuntimeContext, Scheduler,
/// };
///
/// #[derive(Debug)]
/// struct AppConfig { name: &'static str }
///
/// let mut registry = ResourceRegistry::new();
/// registry.insert(AppConfig { name: "wala" });
/// let bus = EventBus::new();
/// let scheduler = Scheduler::with_events(bus.clone());
/// let ctx = RuntimeContext::new(
///     registry.build(),
///     Capabilities::empty(),
///     bus,
///     scheduler,
/// );
///
/// let config: Arc<AppConfig> = ctx.resource::<AppConfig>().expect("registered");
/// assert_eq!(config.name, "wala");
/// ```
#[derive(Clone)]
pub struct RuntimeContext {
    inner: Arc<ContextInner>,
}

struct ContextInner {
    resources: Resources,
    capabilities: Capabilities,
    events: EventBus,
    scheduler: Scheduler,
}

impl RuntimeContext {
    /// Construct a Context wrapping the kernel's frozen [`Resources`]
    /// view, [`Capabilities`] view, [`EventBus`], and [`Scheduler`].
    ///
    /// Typically called by the Runtime kernel after the `Configure` and
    /// `Init` phases complete. End users do not construct Contexts
    /// directly in normal usage; they receive one from the Runtime.
    #[must_use]
    pub fn new(
        resources: Resources,
        capabilities: Capabilities,
        events: EventBus,
        scheduler: Scheduler,
    ) -> Self {
        Self {
            inner: Arc::new(ContextInner {
                resources,
                capabilities,
                events,
                scheduler,
            }),
        }
    }

    /// Construct an empty Context with no resources, no capabilities, a
    /// freshly-allocated [`EventBus`], and a [`Scheduler`] wired to that
    /// bus.
    ///
    /// Primarily useful for tests, stub Services, and pre-Init kernel
    /// scaffolding.
    #[must_use]
    pub fn empty() -> Self {
        let events = EventBus::new();
        let scheduler = Scheduler::with_events(events.clone());
        Self::new(Resources::empty(), Capabilities::empty(), events, scheduler)
    }

    // ---- Resources ----

    /// Retrieve a shared resource by its concrete type.
    ///
    /// Returns `None` if no resource of the given type was registered
    /// during the Configure phase.
    #[must_use]
    pub fn resource<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.inner.resources.get::<T>()
    }

    /// Borrow the underlying [`Resources`] view.
    #[must_use]
    pub fn resources(&self) -> &Resources {
        &self.inner.resources
    }

    // ---- Capabilities ----

    /// Resolve a capability via its selection strategy.
    ///
    /// Equivalent to [`Capabilities::get`].
    #[must_use]
    pub fn capability<C>(&self) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.inner.capabilities.get::<C>()
    }

    /// Resolve a specific named capability provider.
    ///
    /// Equivalent to [`Capabilities::get_named`].
    #[must_use]
    pub fn capability_named<C>(&self, name: &str) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.inner.capabilities.get_named::<C>(name)
    }

    /// Borrow the underlying [`Capabilities`] view.
    #[must_use]
    pub fn capabilities(&self) -> &Capabilities {
        &self.inner.capabilities
    }

    // ---- Events ----

    /// Borrow the underlying [`EventBus`].
    #[must_use]
    pub fn events(&self) -> &EventBus {
        &self.inner.events
    }

    /// Publish a broadcast event. Delegates to [`EventBus::publish`].
    pub fn publish<E>(&self, event: E) -> PublishOutcome
    where
        E: Clone + Send + Sync + 'static,
    {
        self.inner.events.publish(event)
    }

    /// Subscribe to broadcast events of type `E`.
    ///
    /// Delegates to [`EventBus::subscribe`].
    #[must_use]
    pub fn subscribe<E>(&self) -> Subscriber<E>
    where
        E: Clone + Send + Sync + 'static,
    {
        self.inner.events.subscribe::<E>()
    }

    /// Enqueue a work-stealing event for `E`.
    ///
    /// Delegates to [`EventBus::enqueue`].
    ///
    /// # Errors
    ///
    /// Returns an [`EnqueueError`] if every [`Worker`] for `E` has been
    /// dropped — see [`EventBus::enqueue`] for full semantics.
    pub async fn enqueue<E>(&self, event: E) -> Result<(), EnqueueError<E>>
    where
        E: Send + 'static,
    {
        self.inner.events.enqueue(event).await
    }

    /// Obtain a [`Worker`] for the work-stealing queue of `E`.
    ///
    /// Delegates to [`EventBus::worker`].
    #[must_use]
    pub fn worker<E>(&self) -> Worker<E>
    where
        E: Send + 'static,
    {
        self.inner.events.worker::<E>()
    }

    /// Clone-able shutdown signal handle.
    ///
    /// Delegates to [`EventBus::shutdown_signal`].
    #[must_use]
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.inner.events.shutdown_signal()
    }

    // ---- Scheduler ----

    /// Borrow the underlying [`Scheduler`].
    #[must_use]
    pub fn scheduler(&self) -> &Scheduler {
        &self.inner.scheduler
    }

    /// Schedule a task. Delegates to [`Scheduler::schedule`].
    pub fn schedule<F>(&self, trigger: Trigger, policies: Policies, task: F) -> ScheduleHandle
    where
        F: ScheduledFn,
    {
        self.inner.scheduler.schedule(trigger, policies, task)
    }
}

impl fmt::Debug for RuntimeContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeContext")
            .field("resources", &self.inner.resources)
            .field("capabilities", &self.inner.capabilities)
            .field("events", &self.inner.events)
            .field("scheduler", &self.inner.scheduler)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::capabilities::CapabilityRegistry;
    use crate::events::EventBus;
    use crate::resources::ResourceRegistry;

    #[derive(Debug, PartialEq, Eq)]
    struct DbPool(u32);

    #[derive(Debug, PartialEq, Eq)]
    struct Config(&'static str);

    trait Llm: Send + Sync + 'static {
        fn name(&self) -> &'static str;
    }

    struct OpenAiClient;
    impl Llm for OpenAiClient {
        fn name(&self) -> &'static str {
            "openai"
        }
    }

    struct OllamaClient;
    impl Llm for OllamaClient {
        fn name(&self) -> &'static str {
            "ollama"
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Tick {
        seq: u64,
    }

    // ---- Resource access (Phase 2.0.a) ----

    #[test]
    fn context_resolves_registered_resource() {
        let mut registry = ResourceRegistry::new();
        registry.insert(DbPool(42));
        let ctx = RuntimeContext::new(
            registry.build(),
            Capabilities::empty(),
            EventBus::new(),
            Scheduler::new(),
        );

        let pool = ctx.resource::<DbPool>().unwrap();
        assert_eq!(*pool, DbPool(42));
    }

    #[test]
    fn context_returns_none_for_unregistered_resource() {
        let ctx = RuntimeContext::empty();
        assert!(ctx.resource::<DbPool>().is_none());
    }

    #[test]
    fn context_clone_shares_underlying_resources() {
        let mut registry = ResourceRegistry::new();
        registry.insert(DbPool(7));
        let a = RuntimeContext::new(
            registry.build(),
            Capabilities::empty(),
            EventBus::new(),
            Scheduler::new(),
        );
        let b = Clone::clone(&a);

        let pool_a = a.resource::<DbPool>().unwrap();
        let pool_b = b.resource::<DbPool>().unwrap();
        assert!(Arc::ptr_eq(&pool_a, &pool_b));
    }

    #[test]
    fn context_resources_accessor_exposes_view() {
        let mut registry = ResourceRegistry::new();
        registry.insert(Config("wala"));
        let ctx = RuntimeContext::new(
            registry.build(),
            Capabilities::empty(),
            EventBus::new(),
            Scheduler::new(),
        );

        assert!(ctx.resources().contains::<Config>());
        assert_eq!(ctx.resources().len(), 1);
    }

    // ---- Capability access (Phase 2.0.b) ----

    #[test]
    fn context_resolves_default_capability() {
        let mut registry = CapabilityRegistry::new();
        registry.register_default::<dyn Llm>(Arc::new(OpenAiClient));
        let ctx = RuntimeContext::new(
            Resources::empty(),
            registry.build(),
            EventBus::new(),
            Scheduler::new(),
        );

        let llm = ctx.capability::<dyn Llm>().unwrap();
        assert_eq!(llm.name(), "openai");
    }

    #[test]
    fn context_resolves_named_capability() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("local", Arc::new(OllamaClient));
        let ctx = RuntimeContext::new(
            Resources::empty(),
            registry.build(),
            EventBus::new(),
            Scheduler::new(),
        );

        let llm = ctx.capability_named::<dyn Llm>("local").unwrap();
        assert_eq!(llm.name(), "ollama");
    }

    #[test]
    fn context_capability_returns_none_when_unregistered() {
        let ctx = RuntimeContext::empty();
        assert!(ctx.capability::<dyn Llm>().is_none());
        assert!(ctx.capability_named::<dyn Llm>("openai").is_none());
    }

    #[test]
    fn context_capabilities_accessor_exposes_view() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("openai", Arc::new(OpenAiClient));
        registry.register::<dyn Llm>("local", Arc::new(OllamaClient));
        let ctx = RuntimeContext::new(
            Resources::empty(),
            registry.build(),
            EventBus::new(),
            Scheduler::new(),
        );

        assert_eq!(ctx.capabilities().len(), 2);
        assert!(ctx.capabilities().contains::<dyn Llm>("openai"));
        assert!(ctx.capabilities().contains::<dyn Llm>("local"));
    }

    #[test]
    fn context_resource_and_capability_coexist() {
        let mut resources = ResourceRegistry::new();
        resources.insert(Config("wala"));
        let mut caps = CapabilityRegistry::new();
        caps.register_default::<dyn Llm>(Arc::new(OpenAiClient));

        let ctx = RuntimeContext::new(
            resources.build(),
            caps.build(),
            EventBus::new(),
            Scheduler::new(),
        );

        assert_eq!(ctx.resource::<Config>().unwrap().0, "wala");
        assert_eq!(ctx.capability::<dyn Llm>().unwrap().name(), "openai");
    }

    // ---- EventBus delegations (Phase 2.0.c) ----

    #[tokio::test]
    async fn context_publish_subscribe_round_trip() {
        let ctx = RuntimeContext::empty();
        let mut sub = ctx.subscribe::<Tick>();
        ctx.publish(Tick { seq: 11 });
        assert_eq!(sub.recv().await.unwrap(), Tick { seq: 11 });
    }

    #[tokio::test]
    async fn context_enqueue_worker_round_trip() {
        let ctx = RuntimeContext::empty();
        let worker = ctx.worker::<Tick>();
        ctx.enqueue(Tick { seq: 22 }).await.unwrap();
        assert_eq!(worker.recv().await.unwrap().seq, 22);
    }

    #[tokio::test]
    async fn context_shutdown_signal_resolves() {
        let ctx = RuntimeContext::empty();
        let mut signal = ctx.shutdown_signal();

        let bus = ctx.events().clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            bus.shutdown();
        });

        timeout(Duration::from_secs(1), signal.wait())
            .await
            .expect("shutdown signal should resolve");
    }

    #[tokio::test]
    async fn context_clone_shares_event_bus() {
        let a = RuntimeContext::empty();
        let b = a.clone();
        let mut sub = a.subscribe::<Tick>();
        b.publish(Tick { seq: 33 });
        assert_eq!(sub.recv().await.unwrap(), Tick { seq: 33 });
    }

    // ---- Scheduler delegations (Phase 2.0.d) ----

    #[tokio::test]
    async fn context_schedule_runs_task() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let ctx = RuntimeContext::empty();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_task = Arc::clone(&counter);

        ctx.schedule(
            crate::scheduler::Trigger::After(Duration::from_millis(10)),
            crate::scheduler::Policies::new(),
            move || {
                let c = Arc::clone(&counter_task);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        );

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn context_scheduler_accessor_wires_event_bus() {
        let ctx = RuntimeContext::empty();
        let mut fired_sub = ctx.subscribe::<crate::scheduler::TaskFired>();

        ctx.schedule(
            crate::scheduler::Trigger::After(Duration::from_millis(10)),
            crate::scheduler::Policies::new(),
            || async { Ok(()) },
        );

        let fired = timeout(Duration::from_secs(1), fired_sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fired.trigger_count, 1);
    }
}

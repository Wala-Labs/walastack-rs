//! The Plugin trait and `PluginManager`.
//!
//! A [`Plugin`] is the `WalaStack` unit of extensibility: a single
//! cohesive unit that registers Resources, Capability providers, and
//! Services. Auth, `OpenAPI`, Postgres, Redis, `OpenAI`, Ollama — all
//! ship as Plugins.
//!
//! Plugins are the **only** mechanism for extending the Runtime kernel
//! through the [`crate::RuntimeBuilder`]. They are explicit (no
//! auto-discovery, no inventory-style magic) and ordered by registration
//! sequence.
//!
//! ## The contract
//!
//! A Plugin overrides only the hooks it needs:
//!
//! - `name()` — identity used in error messages and (future) Plugin
//!   dependency declarations.
//! - `register_resources(&mut ResourceRegistry)` — add typed shared
//!   values. Default: no-op.
//! - `register_capabilities(&mut CapabilityRegistry)` — bind contracts
//!   to providers. Default: no-op.
//! - `register_services(&mut ServicePlanner)` — add long-running
//!   participants under supervision. Default: no-op.
//! - `required_capabilities() -> Vec<CapabilityRequirement>` —
//!   capabilities this plugin requires for correct operation. Validated
//!   after all plugins have registered; missing requirements fail the
//!   build with [`PluginError::MissingRequirement`]. Default: empty.
//!
//! ## Dependency discipline
//!
//! Plugins **declare dependencies through capabilities, not through
//! concrete Services or other plugin names**. The Plugin trait has no
//! `dependencies(&str)` hook by design — cross-plugin coordination goes
//! through the Capability registry and the kernel `EventBus`.
//!
//! ## Ordering
//!
//! Plugins apply in registration order. If Plugin B requires a
//! capability registered by Plugin A, the user must add A first:
//!
//! ```no_run
//! # use walastack_runtime::{Plugin, Runtime};
//! # struct DbPlugin; impl Plugin for DbPlugin { fn name(&self) -> &str { "db" } }
//! # struct AppPlugin; impl Plugin for AppPlugin { fn name(&self) -> &str { "app" } }
//! Runtime::builder()
//!     .with_plugin(DbPlugin)      // registers Database capability
//!     .with_plugin(AppPlugin);    // requires Database capability
//! ```
//!
//! ## Deferred surfaces (future RFC)
//!
//! - **Async lifecycle hooks** (`init`, `on_start`, `on_shutdown`) are
//!   not yet provided. The Phase 2.0.f scope focuses on synchronous
//!   registration; async hooks land when Phase 3 domain services have
//!   concrete consumers.
//! - **Route registration** is HTTP-specific and intentionally absent
//!   from the kernel Plugin trait. A future walastack-app-level
//!   mechanism will provide it without expanding the kernel surface.
//!
//! See the
//! [Runtime Kernel — Plugins](https://walastack.com/docs/architecture/runtime/plugins/)
//! architecture page for design rationale.

use std::any::TypeId;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use crate::capabilities::{Capabilities, CapabilityName, CapabilityRegistry};
use crate::resources::ResourceRegistry;
use crate::services::Service;
use crate::supervision::RestartPolicy;

// =========================================================================
// Plugin trait
// =========================================================================

/// The WalaStack unit of extensibility.
///
/// See the [module docs](crate::plugins) for the full contract.
pub trait Plugin: Send + Sync + 'static {
    /// Identity used in plugin error messages.
    fn name(&self) -> &str;

    /// Register typed shared values with the kernel's
    /// [`ResourceRegistry`]. Default: no-op.
    fn register_resources(&self, _registry: &mut ResourceRegistry) {}

    /// Register Capability providers with the kernel's
    /// [`CapabilityRegistry`]. Default: no-op.
    fn register_capabilities(&self, _registry: &mut CapabilityRegistry) {}

    /// Register long-running Services with the kernel's
    /// [`ServicePlanner`]. Default: no-op.
    fn register_services(&self, _planner: &mut ServicePlanner) {}

    /// Capabilities this plugin requires for correct operation.
    ///
    /// Validated after all plugins have applied their `register_*`
    /// hooks. Missing requirements abort the build with
    /// [`PluginError::MissingRequirement`].
    ///
    /// Default: no requirements.
    fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
        Vec::new()
    }
}

// =========================================================================
// ServicePlanner
// =========================================================================

/// Collects supervised Services during plugin registration.
///
/// Passed to [`Plugin::register_services`]. Each `add` / `add_supervised`
/// / `add_arc` call registers a Service that will be started under
/// supervision when the [`crate::Runtime`] enters its Start phase.
pub struct ServicePlanner {
    services: Vec<(Arc<dyn Service>, RestartPolicy)>,
}

impl ServicePlanner {
    /// Construct an empty planner.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            services: Vec::new(),
        }
    }

    /// Add a Service with the default [`RestartPolicy::OneShot`].
    pub fn add<S: Service>(&mut self, service: S) -> &mut Self {
        self.add_supervised(service, RestartPolicy::OneShot)
    }

    /// Add a Service with an explicit [`RestartPolicy`].
    pub fn add_supervised<S: Service>(&mut self, service: S, policy: RestartPolicy) -> &mut Self {
        self.services.push((Arc::new(service), policy));
        self
    }

    /// Add a Service via an existing `Arc<dyn Service>`.
    pub fn add_arc(&mut self, service: Arc<dyn Service>, policy: RestartPolicy) -> &mut Self {
        self.services.push((service, policy));
        self
    }

    /// Number of Services collected so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// Whether no Services have been collected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    /// Drain the planner, returning the collected services.
    ///
    /// Kernel-internal: called by [`crate::RuntimeBuilder::build`] to
    /// transfer the staged Services into the `SupervisionTree`.
    pub(crate) fn drain(self) -> Vec<(Arc<dyn Service>, RestartPolicy)> {
        self.services
    }
}

impl Default for ServicePlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ServicePlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServicePlanner")
            .field("services", &self.services.len())
            .finish()
    }
}

// =========================================================================
// CapabilityRequirement
// =========================================================================

/// A capability the kernel must have registered for a plugin to operate.
///
/// Constructed via [`Self::any`] or [`Self::named`]. Validated by
/// [`PluginManager::validate_requirements`] after the Capability
/// registry is frozen.
#[derive(Clone, Debug)]
pub struct CapabilityRequirement {
    type_id: TypeId,
    name: Option<CapabilityName>,
    description: Cow<'static, str>,
}

impl CapabilityRequirement {
    /// Require that *any* provider be registered for capability `C`,
    /// regardless of name.
    #[must_use]
    pub fn any<C: ?Sized + 'static>() -> Self {
        Self {
            type_id: TypeId::of::<C>(),
            name: None,
            description: Cow::Borrowed(std::any::type_name::<C>()),
        }
    }

    /// Require a specific named provider for capability `C`.
    #[must_use]
    pub fn named<C: ?Sized + 'static>(name: impl Into<CapabilityName>) -> Self {
        let name = name.into();
        let description = Cow::Owned(format!(
            "{} (provider {:?})",
            std::any::type_name::<C>(),
            name
        ));
        Self {
            type_id: TypeId::of::<C>(),
            name: Some(name),
            description,
        }
    }

    /// Human-readable description of this requirement, used in error
    /// messages.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    fn is_satisfied_by(&self, capabilities: &Capabilities) -> bool {
        self.name.as_ref().map_or_else(
            || capabilities.contains_type(self.type_id),
            |n| capabilities.contains_typed_name(self.type_id, n),
        )
    }
}

// =========================================================================
// PluginManager
// =========================================================================

/// Owns the kernel's set of registered Plugins and orchestrates their
/// application during [`crate::RuntimeBuilder::build`].
///
/// Plugins apply in registration order; no automatic dependency
/// resolution. Requirement validation runs after all plugins have
/// applied their `register_*` hooks.
pub struct PluginManager {
    plugins: Vec<Arc<dyn Plugin>>,
}

impl PluginManager {
    /// Construct an empty manager.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            plugins: Vec::new(),
        }
    }

    /// Register a plugin.
    pub fn register(&mut self, plugin: Arc<dyn Plugin>) -> &mut Self {
        self.plugins.push(plugin);
        self
    }

    /// Number of registered plugins.
    #[must_use]
    pub fn len(&self) -> usize {
        self.plugins.len()
    }

    /// Whether no plugins are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    /// Iterate the registered plugins in registration order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn Plugin>> {
        self.plugins.iter()
    }

    /// Apply every plugin's `register_resources`, `register_capabilities`,
    /// and `register_services` hooks in registration order.
    pub fn apply_all(
        &self,
        resources: &mut ResourceRegistry,
        capabilities: &mut CapabilityRegistry,
        planner: &mut ServicePlanner,
    ) {
        for plugin in &self.plugins {
            plugin.register_resources(resources);
            plugin.register_capabilities(capabilities);
            plugin.register_services(planner);
        }
    }

    /// Validate that every plugin's required capabilities are registered
    /// in the frozen [`Capabilities`] view.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::MissingRequirement`] on the first unmet
    /// requirement, identifying the requesting plugin and the
    /// requirement description.
    pub fn validate_requirements(&self, capabilities: &Capabilities) -> Result<(), PluginError> {
        for plugin in &self.plugins {
            for req in plugin.required_capabilities() {
                if !req.is_satisfied_by(capabilities) {
                    return Err(PluginError::MissingRequirement {
                        plugin: plugin.name().to_string(),
                        requirement: req.description().to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PluginManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginManager")
            .field("plugins", &self.plugins.len())
            .finish()
    }
}

// =========================================================================
// PluginError
// =========================================================================

/// Errors returned by the plugin subsystem.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PluginError {
    /// A required capability was not registered.
    MissingRequirement {
        /// Name of the plugin whose requirement was unmet.
        plugin: String,
        /// Human-readable description of the unmet requirement.
        requirement: String,
    },
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRequirement {
                plugin,
                requirement,
            } => write!(
                f,
                "plugin {plugin:?} requires capability {requirement:?}, but no provider is registered",
            ),
        }
    }
}

impl std::error::Error for PluginError {}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unnecessary_literal_bound,
        clippy::items_after_statements
    )]

    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::capabilities::CapabilityRegistry;
    use crate::resources::ResourceRegistry;
    use crate::services::{BoxedServiceFuture, ServiceContext, ServiceError};
    use tokio::task::JoinHandle;

    // ---- Test fixtures ----

    #[derive(Debug, PartialEq, Eq)]
    struct AppConfig {
        name: &'static str,
    }

    trait Database: Send + Sync + 'static {
        fn label(&self) -> &'static str;
    }

    struct PostgresPool;
    impl Database for PostgresPool {
        fn label(&self) -> &'static str {
            "postgres"
        }
    }

    struct DuckdbPool;
    impl Database for DuckdbPool {
        fn label(&self) -> &'static str {
            "duckdb"
        }
    }

    struct NoopService {
        name: &'static str,
        starts: Arc<AtomicU32>,
    }

    impl Service for NoopService {
        fn name(&self) -> &str {
            self.name
        }

        fn start(
            &self,
            _ctx: ServiceContext,
        ) -> BoxedServiceFuture<Result<JoinHandle<()>, ServiceError>> {
            let starts = Arc::clone(&self.starts);
            Box::pin(async move {
                starts.fetch_add(1, Ordering::SeqCst);
                Ok(tokio::spawn(async {}))
            })
        }
    }

    // ---- Plugin trait — default-implementation hooks ----

    struct EmptyPlugin;
    impl Plugin for EmptyPlugin {
        fn name(&self) -> &str {
            "empty"
        }
    }

    #[test]
    fn empty_plugin_default_hooks_are_noops() {
        let mut resources = ResourceRegistry::new();
        let mut capabilities = CapabilityRegistry::new();
        let mut planner = ServicePlanner::new();

        EmptyPlugin.register_resources(&mut resources);
        EmptyPlugin.register_capabilities(&mut capabilities);
        EmptyPlugin.register_services(&mut planner);

        assert!(resources.is_empty());
        assert!(capabilities.is_empty());
        assert!(planner.is_empty());
        assert!(EmptyPlugin.required_capabilities().is_empty());
    }

    // ---- Resource registration ----

    struct ConfigPlugin;
    impl Plugin for ConfigPlugin {
        fn name(&self) -> &str {
            "config"
        }
        fn register_resources(&self, reg: &mut ResourceRegistry) {
            reg.insert(AppConfig { name: "wala" });
        }
    }

    #[test]
    fn plugin_registers_resource() {
        let mut resources = ResourceRegistry::new();
        ConfigPlugin.register_resources(&mut resources);
        let frozen = resources.build();
        assert_eq!(frozen.get::<AppConfig>().unwrap().name, "wala");
    }

    // ---- Capability registration ----

    struct PostgresPlugin;
    impl Plugin for PostgresPlugin {
        fn name(&self) -> &str {
            "postgres"
        }
        fn register_capabilities(&self, reg: &mut CapabilityRegistry) {
            reg.register_default::<dyn Database>(Arc::new(PostgresPool));
        }
    }

    #[test]
    fn plugin_registers_default_capability() {
        let mut caps = CapabilityRegistry::new();
        PostgresPlugin.register_capabilities(&mut caps);
        let frozen = caps.build();
        assert_eq!(frozen.get::<dyn Database>().unwrap().label(), "postgres");
    }

    struct AnalyticsPlugin;
    impl Plugin for AnalyticsPlugin {
        fn name(&self) -> &str {
            "analytics"
        }
        fn register_capabilities(&self, reg: &mut CapabilityRegistry) {
            reg.register::<dyn Database>("analytics", Arc::new(DuckdbPool));
        }
    }

    #[test]
    fn plugin_registers_named_capability() {
        let mut caps = CapabilityRegistry::new();
        AnalyticsPlugin.register_capabilities(&mut caps);
        let frozen = caps.build();
        assert_eq!(
            frozen
                .get_named::<dyn Database>("analytics")
                .unwrap()
                .label(),
            "duckdb"
        );
    }

    // ---- Service registration ----

    #[test]
    fn plugin_registers_service() {
        let starts = Arc::new(AtomicU32::new(0));
        struct WithService {
            starts: Arc<AtomicU32>,
        }
        impl Plugin for WithService {
            fn name(&self) -> &str {
                "with-service"
            }
            fn register_services(&self, planner: &mut ServicePlanner) {
                planner.add(NoopService {
                    name: "noop",
                    starts: Arc::clone(&self.starts),
                });
            }
        }

        let mut planner = ServicePlanner::new();
        WithService {
            starts: Arc::clone(&starts),
        }
        .register_services(&mut planner);
        assert_eq!(planner.len(), 1);
    }

    #[test]
    fn plugin_registers_service_with_explicit_policy() {
        struct WithSupervised;
        impl Plugin for WithSupervised {
            fn name(&self) -> &str {
                "with-supervised"
            }
            fn register_services(&self, planner: &mut ServicePlanner) {
                planner.add_supervised(
                    NoopService {
                        name: "noop",
                        starts: Arc::new(AtomicU32::new(0)),
                    },
                    RestartPolicy::OnFailure {
                        max_attempts: 3,
                        backoff: crate::Backoff::Linear {
                            base: std::time::Duration::from_millis(5),
                            step: std::time::Duration::ZERO,
                        },
                    },
                );
            }
        }

        let mut planner = ServicePlanner::new();
        WithSupervised.register_services(&mut planner);
        assert_eq!(planner.len(), 1);
    }

    // ---- Capability requirement validation ----

    struct RequiresAnyDb;
    impl Plugin for RequiresAnyDb {
        fn name(&self) -> &str {
            "requires-any-db"
        }
        fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
            vec![CapabilityRequirement::any::<dyn Database>()]
        }
    }

    struct RequiresNamedDb;
    impl Plugin for RequiresNamedDb {
        fn name(&self) -> &str {
            "requires-named-db"
        }
        fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
            vec![CapabilityRequirement::named::<dyn Database>("primary")]
        }
    }

    #[test]
    fn manager_validates_satisfied_any_requirement() {
        let mut manager = PluginManager::new();
        manager.register(Arc::new(PostgresPlugin));
        manager.register(Arc::new(RequiresAnyDb));

        let mut resources = ResourceRegistry::new();
        let mut caps = CapabilityRegistry::new();
        let mut planner = ServicePlanner::new();
        manager.apply_all(&mut resources, &mut caps, &mut planner);

        let frozen = caps.build();
        assert!(manager.validate_requirements(&frozen).is_ok());
    }

    #[test]
    fn manager_rejects_unsatisfied_any_requirement() {
        let mut manager = PluginManager::new();
        manager.register(Arc::new(RequiresAnyDb));

        let mut resources = ResourceRegistry::new();
        let mut caps = CapabilityRegistry::new();
        let mut planner = ServicePlanner::new();
        manager.apply_all(&mut resources, &mut caps, &mut planner);

        let frozen = caps.build();
        let err = manager.validate_requirements(&frozen).unwrap_err();
        match err {
            PluginError::MissingRequirement {
                plugin,
                requirement,
            } => {
                assert_eq!(plugin, "requires-any-db");
                assert!(requirement.contains("Database"));
            }
        }
    }

    #[test]
    fn manager_validates_satisfied_named_requirement() {
        // PostgresPlugin registers under "default"; we need a plugin
        // that registers "primary" explicitly.
        struct PrimaryPlugin;
        impl Plugin for PrimaryPlugin {
            fn name(&self) -> &str {
                "primary"
            }
            fn register_capabilities(&self, reg: &mut CapabilityRegistry) {
                reg.register::<dyn Database>("primary", Arc::new(PostgresPool));
            }
        }

        let mut manager = PluginManager::new();
        manager.register(Arc::new(PrimaryPlugin));
        manager.register(Arc::new(RequiresNamedDb));

        let mut resources = ResourceRegistry::new();
        let mut caps = CapabilityRegistry::new();
        let mut planner = ServicePlanner::new();
        manager.apply_all(&mut resources, &mut caps, &mut planner);

        let frozen = caps.build();
        assert!(manager.validate_requirements(&frozen).is_ok());
    }

    #[test]
    fn manager_rejects_unsatisfied_named_requirement_when_type_present() {
        // PostgresPlugin registers under "default", not "primary".
        let mut manager = PluginManager::new();
        manager.register(Arc::new(PostgresPlugin));
        manager.register(Arc::new(RequiresNamedDb));

        let mut resources = ResourceRegistry::new();
        let mut caps = CapabilityRegistry::new();
        let mut planner = ServicePlanner::new();
        manager.apply_all(&mut resources, &mut caps, &mut planner);

        let frozen = caps.build();
        let err = manager.validate_requirements(&frozen).unwrap_err();
        match err {
            PluginError::MissingRequirement {
                plugin,
                requirement,
            } => {
                assert_eq!(plugin, "requires-named-db");
                assert!(requirement.contains("primary"));
            }
        }
    }

    // ---- Ordering: registration order is the apply order ----

    #[test]
    fn plugins_apply_in_registration_order() {
        let order: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(vec![]));

        struct Recorder {
            name: &'static str,
            order: Arc<std::sync::Mutex<Vec<String>>>,
        }
        impl Plugin for Recorder {
            fn name(&self) -> &str {
                self.name
            }
            fn register_resources(&self, _reg: &mut ResourceRegistry) {
                self.order.lock().unwrap().push(self.name.to_string());
            }
        }

        let mut manager = PluginManager::new();
        manager.register(Arc::new(Recorder {
            name: "first",
            order: Arc::clone(&order),
        }));
        manager.register(Arc::new(Recorder {
            name: "second",
            order: Arc::clone(&order),
        }));
        manager.register(Arc::new(Recorder {
            name: "third",
            order: Arc::clone(&order),
        }));

        let mut resources = ResourceRegistry::new();
        let mut caps = CapabilityRegistry::new();
        let mut planner = ServicePlanner::new();
        manager.apply_all(&mut resources, &mut caps, &mut planner);

        let recorded = order.lock().unwrap().clone();
        assert_eq!(recorded, vec!["first", "second", "third"]);
    }

    // ---- Multi-hook plugin ----

    struct FullPlugin {
        starts: Arc<AtomicU32>,
    }
    impl Plugin for FullPlugin {
        fn name(&self) -> &str {
            "full"
        }
        fn register_resources(&self, reg: &mut ResourceRegistry) {
            reg.insert(AppConfig { name: "wala" });
        }
        fn register_capabilities(&self, reg: &mut CapabilityRegistry) {
            reg.register_default::<dyn Database>(Arc::new(PostgresPool));
        }
        fn register_services(&self, planner: &mut ServicePlanner) {
            planner.add(NoopService {
                name: "noop",
                starts: Arc::clone(&self.starts),
            });
        }
    }

    #[test]
    fn plugin_can_register_resources_capabilities_and_services_together() {
        let mut manager = PluginManager::new();
        manager.register(Arc::new(FullPlugin {
            starts: Arc::new(AtomicU32::new(0)),
        }));

        let mut resources = ResourceRegistry::new();
        let mut caps = CapabilityRegistry::new();
        let mut planner = ServicePlanner::new();
        manager.apply_all(&mut resources, &mut caps, &mut planner);

        assert_eq!(resources.len(), 1);
        assert_eq!(caps.len(), 1);
        assert_eq!(planner.len(), 1);
    }

    // ---- PluginManager bookkeeping ----

    #[test]
    fn manager_tracks_registered_plugin_count() {
        let mut manager = PluginManager::new();
        assert_eq!(manager.len(), 0);
        assert!(manager.is_empty());
        manager.register(Arc::new(EmptyPlugin));
        assert_eq!(manager.len(), 1);
        manager.register(Arc::new(EmptyPlugin));
        assert_eq!(manager.len(), 2);
    }

    // ---- CapabilityRequirement helpers ----

    #[test]
    fn capability_requirement_any_describes_type() {
        let req = CapabilityRequirement::any::<dyn Database>();
        assert!(req.description().contains("Database"));
    }

    #[test]
    fn capability_requirement_named_describes_type_and_name() {
        let req = CapabilityRequirement::named::<dyn Database>("primary");
        assert!(req.description().contains("Database"));
        assert!(req.description().contains("primary"));
    }
}

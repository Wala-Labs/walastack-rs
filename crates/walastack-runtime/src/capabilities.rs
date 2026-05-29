//! Named multi-provider contracts for the Runtime Kernel.
//!
//! A **Capability** is a named contract — typically a trait — that multiple
//! providers can implement. Plugins register providers under names;
//! Services request providers by contract, not by concrete type. This is
//! the kernel mechanism that makes WalaStack substitutable: handlers say
//! "give me a [`Database`-like trait]"; operator configuration picks the
//! provider.
//!
//! The registry is keyed by `(TypeId, CapabilityName)` where
//! `CapabilityName` is [`Cow<'static, str>`] with `"default"` as the
//! conventional default name.
//!
//! ## Phase 2.0.b scope
//!
//! - Builder/frozen split mirroring [`crate::resources`].
//! - Three selection strategies: [`SelectionStrategy::Single`],
//!   [`SelectionStrategy::Fallback`],
//!   [`SelectionStrategy::WeightedRoundRobin`].
//! - Trait-object providers via `Arc<dyn Trait>` storage with type-erased
//!   downcast at lookup.
//! - Health-aware fallback / probe surface lands in a later sub-batch
//!   alongside the `EventBus`.
//!
//! See the
//! [Runtime Kernel — CapabilityRegistry](https://walastack.com/docs/architecture/runtime/capabilities/)
//! architecture page for design rationale.

use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The conventional default name for a Capability provider.
///
/// Plugins that register only one provider should register it under this
/// name. Services that do not need a specific provider should request via
/// [`Capabilities::get`] / [`crate::RuntimeContext::capability`], which
/// looks up this name under the default ([`SelectionStrategy::Single`])
/// strategy.
pub const DEFAULT_NAME: &str = "default";

/// The name a provider is registered under in the [`CapabilityRegistry`].
///
/// `Cow<'static, str>` so that `&'static str` literals (the common case)
/// require no allocation while dynamically-constructed names are still
/// representable.
pub type CapabilityName = Cow<'static, str>;

type ErasedProvider = Arc<dyn Any + Send + Sync>;

/// Mutable capability registry used during the kernel `Configure` phase.
///
/// Plugins register providers here. Once `Configure` completes, the
/// registry is frozen into a [`Capabilities`] view via [`Self::build`]
/// and distributed to participants via [`crate::RuntimeContext`].
#[derive(Default)]
pub struct CapabilityRegistry {
    providers: HashMap<TypeId, HashMap<CapabilityName, ErasedProvider>>,
    strategies: HashMap<TypeId, SelectionStrategy>,
}

impl CapabilityRegistry {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a provider under the given name.
    ///
    /// If a provider was already registered under the same name for the
    /// same capability, it is replaced and returned to the caller.
    ///
    /// `C` may be a trait object (`dyn Trait`) when callers pass
    /// `Arc<dyn Trait>` directly, allowing the contract pattern that
    /// Capability semantics imply.
    pub fn register<C>(
        &mut self,
        name: impl Into<CapabilityName>,
        provider: Arc<C>,
    ) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<C>();
        let bucket = self.providers.entry(type_id).or_default();
        let previous = bucket.insert(name.into(), store_provider(provider));
        previous.and_then(|erased| retrieve_provider::<C>(&erased))
    }

    /// Register a provider under the conventional [`DEFAULT_NAME`].
    ///
    /// Equivalent to `register(DEFAULT_NAME, provider)`.
    pub fn register_default<C>(&mut self, provider: Arc<C>) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.register(Cow::Borrowed(DEFAULT_NAME), provider)
    }

    /// Set the selection strategy for a capability.
    ///
    /// Strategy is consulted by [`Capabilities::get`] (and by
    /// [`crate::RuntimeContext::capability`]). [`Capabilities::get_named`]
    /// always does direct exact-name lookup and ignores the strategy.
    ///
    /// Replaces any prior strategy for the same capability.
    pub fn set_strategy<C>(&mut self, strategy: SelectionStrategy)
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.strategies.insert(TypeId::of::<C>(), strategy);
    }

    /// Look up the provider registered under [`DEFAULT_NAME`] during the
    /// Configure phase.
    ///
    /// This bypasses the selection strategy; for strategy-aware lookup,
    /// freeze the registry with [`Self::build`] and call
    /// [`Capabilities::get`].
    #[must_use]
    pub fn get<C>(&self) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.get_named::<C>(DEFAULT_NAME)
    }

    /// Look up the provider registered under the given name during the
    /// Configure phase.
    #[must_use]
    pub fn get_named<C>(&self, name: &str) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        let bucket = self.providers.get(&TypeId::of::<C>())?;
        let erased = bucket.get(name)?;
        retrieve_provider::<C>(erased)
    }

    /// Whether a provider with the given name is registered for the
    /// capability.
    #[must_use]
    pub fn contains<C>(&self, name: &str) -> bool
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.providers
            .get(&TypeId::of::<C>())
            .is_some_and(|bucket| bucket.contains_key(name))
    }

    /// List the names of all providers registered for a capability.
    ///
    /// Order is unspecified.
    #[must_use]
    pub fn providers<C>(&self) -> Vec<CapabilityName>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.providers
            .get(&TypeId::of::<C>())
            .map(|bucket| bucket.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Total number of registered providers across every capability.
    #[must_use]
    pub fn len(&self) -> usize {
        self.providers.values().map(HashMap::len).sum()
    }

    /// Whether no providers are registered for any capability.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.values().all(HashMap::is_empty)
    }

    /// Freeze the registry into an immutable, `Arc`-shared
    /// [`Capabilities`] view.
    #[must_use]
    pub fn build(self) -> Capabilities {
        let rotation = self
            .strategies
            .iter()
            .filter(|(_, strategy)| matches!(strategy, SelectionStrategy::WeightedRoundRobin(_)))
            .map(|(type_id, _)| (*type_id, AtomicUsize::new(0)))
            .collect();
        Capabilities {
            inner: Arc::new(CapabilitiesInner {
                providers: self.providers,
                strategies: self.strategies,
                rotation,
            }),
        }
    }
}

impl fmt::Debug for CapabilityRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapabilityRegistry")
            .field("capabilities", &self.providers.len())
            .field("providers", &self.len())
            .field("strategies", &self.strategies.len())
            .finish()
    }
}

/// How a capability picks among multiple registered providers when
/// resolved via [`Capabilities::get`].
///
/// [`Capabilities::get_named`] always performs direct exact-name lookup
/// regardless of strategy.
#[derive(Clone, Debug)]
pub enum SelectionStrategy {
    /// Look up the provider registered under [`DEFAULT_NAME`].
    ///
    /// This is the default behavior when no strategy is explicitly set.
    Single,

    /// Walk the chain of names in order, returning the first provider
    /// that is registered.
    ///
    /// In Phase 2.0.b, "first registered" is the only criterion; health
    /// probes land in a later sub-batch.
    Fallback(Vec<CapabilityName>),

    /// Pick a provider by weighted round-robin across the listed names.
    ///
    /// Weights are summed; an atomic counter increments on each lookup
    /// and the picked index is `counter % total_weight`. A provider with
    /// weight `0` is excluded from rotation. If the total weight is zero
    /// or no listed name has a registered provider, returns `None`.
    WeightedRoundRobin(Vec<(CapabilityName, u32)>),
}

impl Default for SelectionStrategy {
    fn default() -> Self {
        Self::Single
    }
}

/// Frozen, `Arc`-shared, read-only capability view.
///
/// Constructed by [`CapabilityRegistry::build`]. Cloning is one atomic
/// increment. Distributed to participants through
/// [`crate::RuntimeContext`].
#[derive(Clone)]
pub struct Capabilities {
    inner: Arc<CapabilitiesInner>,
}

struct CapabilitiesInner {
    providers: HashMap<TypeId, HashMap<CapabilityName, ErasedProvider>>,
    strategies: HashMap<TypeId, SelectionStrategy>,
    rotation: HashMap<TypeId, AtomicUsize>,
}

impl Capabilities {
    /// Construct an empty `Capabilities` view (useful for tests and stub
    /// contexts before any plugin has registered a provider).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(CapabilitiesInner {
                providers: HashMap::new(),
                strategies: HashMap::new(),
                rotation: HashMap::new(),
            }),
        }
    }

    /// Resolve a capability via its selection strategy.
    ///
    /// - [`SelectionStrategy::Single`] (the default): looks up the provider
    ///   registered under [`DEFAULT_NAME`].
    /// - [`SelectionStrategy::Fallback`]: walks the chain in order and
    ///   returns the first registered provider.
    /// - [`SelectionStrategy::WeightedRoundRobin`]: picks a provider by
    ///   weighted round-robin and increments the rotation counter.
    ///
    /// Returns `None` if no provider matches.
    #[must_use]
    pub fn get<C>(&self) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<C>();
        let bucket = self.inner.providers.get(&type_id)?;
        let strategy = self
            .inner
            .strategies
            .get(&type_id)
            .unwrap_or(&SelectionStrategy::Single);

        match strategy {
            SelectionStrategy::Single => resolve_provider::<C>(bucket, DEFAULT_NAME),
            SelectionStrategy::Fallback(chain) => chain
                .iter()
                .find_map(|name| resolve_provider::<C>(bucket, name.as_ref())),
            SelectionStrategy::WeightedRoundRobin(weights) => {
                self.weighted_pick::<C>(type_id, bucket, weights)
            }
        }
    }

    /// Resolve a specific named provider.
    ///
    /// Always performs direct exact-name lookup, bypassing the selection
    /// strategy. Returns `None` if no provider is registered under that
    /// name for this capability.
    #[must_use]
    pub fn get_named<C>(&self, name: &str) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        let bucket = self.inner.providers.get(&TypeId::of::<C>())?;
        resolve_provider::<C>(bucket, name)
    }

    /// Whether a provider with the given name is registered for the
    /// capability.
    #[must_use]
    pub fn contains<C>(&self, name: &str) -> bool
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.inner
            .providers
            .get(&TypeId::of::<C>())
            .is_some_and(|bucket| bucket.contains_key(name))
    }

    /// List the names of all providers registered for a capability.
    ///
    /// Order is unspecified.
    #[must_use]
    pub fn providers<C>(&self) -> Vec<CapabilityName>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        self.inner
            .providers
            .get(&TypeId::of::<C>())
            .map(|bucket| bucket.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Total number of registered providers across every capability.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.providers.values().map(HashMap::len).sum()
    }

    /// Whether no providers are registered for any capability.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.providers.values().all(HashMap::is_empty)
    }

    /// Whether any provider is registered for the given capability type
    /// id.
    ///
    /// Kernel-internal helper used by plugin requirement validation.
    pub(crate) fn contains_type(&self, type_id: TypeId) -> bool {
        self.inner
            .providers
            .get(&type_id)
            .is_some_and(|bucket| !bucket.is_empty())
    }

    /// Whether a provider with the given name is registered for the
    /// capability type id.
    ///
    /// Kernel-internal helper used by plugin requirement validation.
    pub(crate) fn contains_typed_name(&self, type_id: TypeId, name: &str) -> bool {
        self.inner
            .providers
            .get(&type_id)
            .is_some_and(|bucket| bucket.contains_key(name))
    }

    fn weighted_pick<C>(
        &self,
        type_id: TypeId,
        bucket: &HashMap<CapabilityName, ErasedProvider>,
        weights: &[(CapabilityName, u32)],
    ) -> Option<Arc<C>>
    where
        C: ?Sized + Send + Sync + 'static,
    {
        let total: u64 = weights.iter().map(|(_, w)| u64::from(*w)).sum();
        if total == 0 {
            return None;
        }
        let counter = self.inner.rotation.get(&type_id)?;
        // Wrapping is fine — only modulo matters.
        let pick = (counter.fetch_add(1, Ordering::Relaxed) as u64) % total;
        let mut accumulated: u64 = 0;
        for (name, weight) in weights {
            if *weight == 0 {
                continue;
            }
            accumulated += u64::from(*weight);
            if pick < accumulated {
                return resolve_provider::<C>(bucket, name.as_ref());
            }
        }
        None
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Capabilities")
            .field("capabilities", &self.inner.providers.len())
            .field("providers", &self.len())
            .field("strategies", &self.inner.strategies.len())
            .finish()
    }
}

fn resolve_provider<C>(
    bucket: &HashMap<CapabilityName, ErasedProvider>,
    name: &str,
) -> Option<Arc<C>>
where
    C: ?Sized + Send + Sync + 'static,
{
    let erased = bucket.get(name)?;
    retrieve_provider::<C>(erased)
}

fn store_provider<C>(provider: Arc<C>) -> ErasedProvider
where
    C: ?Sized + Send + Sync + 'static,
{
    // `Arc<C>` is `Sized` even when `C` is not; wrap it in another `Arc`
    // to obtain a sized `Any + Send + Sync` payload that the registry
    // can store uniformly.
    let outer: Arc<Arc<C>> = Arc::new(provider);
    outer
}

fn retrieve_provider<C>(erased: &ErasedProvider) -> Option<Arc<C>>
where
    C: ?Sized + Send + Sync + 'static,
{
    let outer: Arc<Arc<C>> = Arc::clone(erased).downcast::<Arc<C>>().ok()?;
    Some(Arc::clone(&outer))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    // ---------- Test fixtures: a contract trait + two providers ----------

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

    fn db(provider: impl Database + 'static) -> Arc<dyn Database> {
        Arc::new(provider)
    }

    fn llm(provider: impl Llm + 'static) -> Arc<dyn Llm> {
        Arc::new(provider)
    }

    // ---------- Registration & basic lookup ----------

    #[test]
    fn register_default_then_get_returns_provider() {
        let mut registry = CapabilityRegistry::new();
        assert!(
            registry
                .register_default::<dyn Database>(db(PostgresPool))
                .is_none()
        );

        let resolved = registry.get::<dyn Database>().unwrap();
        assert_eq!(resolved.label(), "postgres");
    }

    #[test]
    fn register_named_then_get_named_returns_provider() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Database>("primary", db(PostgresPool));

        let resolved = registry.get_named::<dyn Database>("primary").unwrap();
        assert_eq!(resolved.label(), "postgres");
    }

    #[test]
    fn get_returns_none_when_only_named_provider_registered() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Database>("primary", db(PostgresPool));

        assert!(registry.get::<dyn Database>().is_none());
    }

    #[test]
    fn get_named_returns_none_when_name_not_registered() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Database>("primary", db(PostgresPool));

        assert!(registry.get_named::<dyn Database>("secondary").is_none());
    }

    #[test]
    fn missing_capability_returns_none_from_both_accessors() {
        let registry = CapabilityRegistry::new();
        assert!(registry.get::<dyn Database>().is_none());
        assert!(registry.get_named::<dyn Database>("primary").is_none());
        assert!(!registry.contains::<dyn Database>("primary"));
        assert!(registry.is_empty());
    }

    #[test]
    fn duplicate_named_registration_returns_previous() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Database>("primary", db(PostgresPool));
        let previous = registry
            .register::<dyn Database>("primary", db(DuckdbPool))
            .unwrap();
        assert_eq!(previous.label(), "postgres");

        let current = registry.get_named::<dyn Database>("primary").unwrap();
        assert_eq!(current.label(), "duckdb");
    }

    #[test]
    fn multiple_names_for_same_capability_coexist() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Database>("primary", db(PostgresPool));
        registry.register::<dyn Database>("analytics", db(DuckdbPool));

        assert_eq!(
            registry
                .get_named::<dyn Database>("primary")
                .unwrap()
                .label(),
            "postgres"
        );
        assert_eq!(
            registry
                .get_named::<dyn Database>("analytics")
                .unwrap()
                .label(),
            "duckdb"
        );
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn multiple_capabilities_coexist() {
        let mut registry = CapabilityRegistry::new();
        registry.register_default::<dyn Database>(db(PostgresPool));
        registry.register_default::<dyn Llm>(llm(OpenAiClient));

        assert_eq!(registry.get::<dyn Database>().unwrap().label(), "postgres");
        assert_eq!(registry.get::<dyn Llm>().unwrap().name(), "openai");
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn providers_lists_registered_names() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Database>("primary", db(PostgresPool));
        registry.register::<dyn Database>("analytics", db(DuckdbPool));

        let mut names: Vec<String> = registry
            .providers::<dyn Database>()
            .into_iter()
            .map(Cow::into_owned)
            .collect();
        names.sort();
        assert_eq!(names, vec!["analytics".to_owned(), "primary".to_owned()]);
    }

    // ---------- Selection strategies ----------

    #[test]
    fn single_strategy_is_default_when_unset() {
        let mut registry = CapabilityRegistry::new();
        registry.register_default::<dyn Database>(db(PostgresPool));
        let caps = registry.build();

        assert_eq!(caps.get::<dyn Database>().unwrap().label(), "postgres");
    }

    #[test]
    fn fallback_strategy_returns_first_present_in_chain() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("openai", llm(OpenAiClient));
        registry.register::<dyn Llm>("local", llm(OllamaClient));
        registry.set_strategy::<dyn Llm>(SelectionStrategy::Fallback(vec![
            Cow::Borrowed("openai"),
            Cow::Borrowed("local"),
        ]));
        let caps = registry.build();

        assert_eq!(caps.get::<dyn Llm>().unwrap().name(), "openai");
    }

    #[test]
    fn fallback_strategy_skips_missing_entries() {
        let mut registry = CapabilityRegistry::new();
        // Only "local" is registered; chain lists "openai" first.
        registry.register::<dyn Llm>("local", llm(OllamaClient));
        registry.set_strategy::<dyn Llm>(SelectionStrategy::Fallback(vec![
            Cow::Borrowed("openai"),
            Cow::Borrowed("local"),
        ]));
        let caps = registry.build();

        assert_eq!(caps.get::<dyn Llm>().unwrap().name(), "ollama");
    }

    #[test]
    fn fallback_strategy_returns_none_when_all_chain_entries_missing() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("unrelated", llm(OllamaClient));
        registry.set_strategy::<dyn Llm>(SelectionStrategy::Fallback(vec![
            Cow::Borrowed("openai"),
            Cow::Borrowed("local"),
        ]));
        let caps = registry.build();

        assert!(caps.get::<dyn Llm>().is_none());
    }

    #[test]
    fn weighted_round_robin_rotates_across_providers() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("openai", llm(OpenAiClient));
        registry.register::<dyn Llm>("local", llm(OllamaClient));
        registry.set_strategy::<dyn Llm>(SelectionStrategy::WeightedRoundRobin(vec![
            (Cow::Borrowed("openai"), 1),
            (Cow::Borrowed("local"), 1),
        ]));
        let caps = registry.build();

        let mut openai_hits = 0_u32;
        let mut ollama_hits = 0_u32;
        for _ in 0..10 {
            match caps.get::<dyn Llm>().unwrap().name() {
                "openai" => openai_hits += 1,
                "ollama" => ollama_hits += 1,
                other => panic!("unexpected provider {other}"),
            }
        }
        assert_eq!(openai_hits, 5);
        assert_eq!(ollama_hits, 5);
    }

    #[test]
    fn weighted_round_robin_excludes_zero_weight_providers() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("openai", llm(OpenAiClient));
        registry.register::<dyn Llm>("local", llm(OllamaClient));
        registry.set_strategy::<dyn Llm>(SelectionStrategy::WeightedRoundRobin(vec![
            (Cow::Borrowed("openai"), 3),
            (Cow::Borrowed("local"), 0),
        ]));
        let caps = registry.build();

        for _ in 0..20 {
            assert_eq!(caps.get::<dyn Llm>().unwrap().name(), "openai");
        }
    }

    #[test]
    fn weighted_round_robin_returns_none_when_total_weight_zero() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("openai", llm(OpenAiClient));
        registry.set_strategy::<dyn Llm>(SelectionStrategy::WeightedRoundRobin(vec![(
            Cow::Borrowed("openai"),
            0,
        )]));
        let caps = registry.build();

        assert!(caps.get::<dyn Llm>().is_none());
    }

    #[test]
    fn weighted_round_robin_honors_unequal_weights() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("openai", llm(OpenAiClient));
        registry.register::<dyn Llm>("local", llm(OllamaClient));
        // 3:1 weighting in a 4-cycle.
        registry.set_strategy::<dyn Llm>(SelectionStrategy::WeightedRoundRobin(vec![
            (Cow::Borrowed("openai"), 3),
            (Cow::Borrowed("local"), 1),
        ]));
        let caps = registry.build();

        let mut counts = std::collections::HashMap::<&str, u32>::new();
        for _ in 0..40 {
            let name = caps.get::<dyn Llm>().unwrap().name();
            *counts.entry(name).or_insert(0) += 1;
        }
        assert_eq!(counts.get("openai").copied().unwrap_or(0), 30);
        assert_eq!(counts.get("ollama").copied().unwrap_or(0), 10);
    }

    #[test]
    fn get_named_bypasses_strategy() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("openai", llm(OpenAiClient));
        registry.register::<dyn Llm>("local", llm(OllamaClient));
        // Strategy says "always prefer openai"; get_named must still
        // honor explicit name selection.
        registry
            .set_strategy::<dyn Llm>(SelectionStrategy::Fallback(vec![Cow::Borrowed("openai")]));
        let caps = registry.build();

        assert_eq!(caps.get_named::<dyn Llm>("local").unwrap().name(), "ollama");
    }

    // ---------- Frozen view round-trip + clone semantics ----------

    #[test]
    fn frozen_view_round_trips_through_build() {
        let mut registry = CapabilityRegistry::new();
        registry.register_default::<dyn Database>(db(PostgresPool));
        registry.register::<dyn Database>("analytics", db(DuckdbPool));
        let caps = registry.build();

        assert_eq!(caps.len(), 2);
        assert!(caps.contains::<dyn Database>("default"));
        assert!(caps.contains::<dyn Database>("analytics"));
        assert_eq!(caps.get::<dyn Database>().unwrap().label(), "postgres");
        assert_eq!(
            caps.get_named::<dyn Database>("analytics").unwrap().label(),
            "duckdb"
        );
    }

    #[test]
    fn capabilities_clone_shares_underlying_storage() {
        let mut registry = CapabilityRegistry::new();
        registry.register_default::<dyn Database>(db(PostgresPool));
        let a = registry.build();
        let b = Clone::clone(&a);

        let provider_a = a.get::<dyn Database>().unwrap();
        let provider_b = b.get::<dyn Database>().unwrap();
        // Both views resolve to the *same* underlying Arc<dyn Database>.
        assert!(Arc::ptr_eq(&provider_a, &provider_b));
    }

    #[test]
    fn capabilities_empty_is_truly_empty() {
        let caps = Capabilities::empty();
        assert!(caps.is_empty());
        assert_eq!(caps.len(), 0);
        assert!(caps.get::<dyn Database>().is_none());
        assert!(caps.get_named::<dyn Database>("primary").is_none());
        assert!(caps.providers::<dyn Database>().is_empty());
    }
}

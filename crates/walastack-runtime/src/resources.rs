//! Typed shared values for the Runtime Kernel.
//!
//! The resource module provides two related types:
//!
//! - [`ResourceRegistry`] — the mutable, Configure-phase builder. Plugins
//!   register shared values during kernel startup.
//! - [`Resources`] — the frozen, `Arc`-shared, read-only view. Constructed
//!   from a [`ResourceRegistry`] via [`ResourceRegistry::build`] and handed
//!   to participants through [`crate::RuntimeContext`].
//!
//! Resources are addressed by the [`TypeId`] of their concrete type, giving
//! compile-time-checked typed access without runtime string lookups. The
//! storage layer is type-erased
//! (`Arc<dyn Any + Send + Sync>`) so plugins from independent crates can
//! register their own types without propagating generics through the kernel.
//!
//! See the
//! [Runtime Kernel — Resources](https://walastack.com/docs/architecture/runtime/resources/)
//! architecture page for the design rationale.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

type ErasedResource = Arc<dyn Any + Send + Sync>;

/// Mutable resource registry used during the kernel `Configure` phase.
///
/// Plugins and the Runtime builder insert typed shared values here. Once
/// `Configure` completes, the registry is frozen into a [`Resources`] view
/// via [`Self::build`] and distributed to participants via
/// [`crate::RuntimeContext`].
#[derive(Default)]
pub struct ResourceRegistry {
    entries: HashMap<TypeId, ErasedResource>,
}

impl ResourceRegistry {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a resource, keyed by its concrete type.
    ///
    /// If a resource of the same type was already registered, it is replaced
    /// and returned to the caller.
    ///
    /// # Common gotcha — do NOT pre-wrap in `Arc`
    ///
    /// `insert` wraps the value in an `Arc<T>` for you. Passing an
    /// `Arc<T>` here registers it under `TypeId::of::<Arc<T>>()`, which
    /// downstream `get::<T>()` lookups will silently miss:
    ///
    /// ```ignore
    /// // Wrong — registers Arc<Arc<MyConfig>>; get::<MyConfig>() returns None.
    /// registry.insert(Arc::new(MyConfig::default()));
    ///
    /// // Right — registers Arc<MyConfig>; get::<MyConfig>() returns Some(_).
    /// registry.insert(MyConfig::default());
    /// ```
    ///
    /// If you genuinely need to register an existing `Arc<T>`, use
    /// [`insert_arc`](Self::insert_arc) instead, which is explicit
    /// about the wrap semantics.
    ///
    /// A `debug_assert!` in debug builds catches the common mistake of
    /// passing an `Arc<T>` to `insert` when the underlying type already
    /// implements an Arc-like shape — release builds are unchanged for
    /// performance.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<Arc<T>> {
        debug_assert!(
            !std::any::type_name::<T>().starts_with("alloc::sync::Arc<"),
            "ResourceRegistry::insert({}) — you are double-Arc-ing. Pass the raw \
             value (insert wraps it) or use insert_arc to keep your existing Arc. \
             See the `insert` doc comment for the right pattern.",
            std::any::type_name::<T>()
        );
        self.insert_arc(Arc::new(value))
    }

    /// Insert an already-`Arc`-wrapped resource.
    ///
    /// Useful when the value is shared with code outside the kernel during
    /// the Configure phase (e.g., a plugin that hands out the same `Arc`
    /// to multiple registries).
    pub fn insert_arc<T: Send + Sync + 'static>(&mut self, value: Arc<T>) -> Option<Arc<T>> {
        let previous = self.entries.insert(TypeId::of::<T>(), value);
        previous.and_then(downcast_arc::<T>)
    }

    /// Look up a resource by type during the Configure phase.
    #[must_use]
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.entries
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(downcast_arc::<T>)
    }

    /// Whether a resource of the given type is registered.
    #[must_use]
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.entries.contains_key(&TypeId::of::<T>())
    }

    /// Number of registered resources.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Freeze the registry into an immutable, `Arc`-shared [`Resources`] view.
    #[must_use]
    pub fn build(self) -> Resources {
        Resources {
            entries: Arc::new(self.entries),
        }
    }
}

impl fmt::Debug for ResourceRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceRegistry")
            .field("len", &self.entries.len())
            .finish()
    }
}

/// Frozen, `Arc`-shared, read-only resource view.
///
/// Constructed by [`ResourceRegistry::build`]. Cloning is one atomic
/// increment. Distributed to participants through
/// [`crate::RuntimeContext`].
#[derive(Clone)]
pub struct Resources {
    entries: Arc<HashMap<TypeId, ErasedResource>>,
}

impl Resources {
    /// Construct an empty `Resources` view (useful for tests and stub
    /// contexts before any plugin has registered a resource).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            entries: Arc::new(HashMap::new()),
        }
    }

    /// Retrieve a shared resource by its concrete type.
    ///
    /// Returns `None` if no resource of the given type was registered.
    #[must_use]
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        self.entries
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(downcast_arc::<T>)
    }

    /// Whether a resource of the given type is registered.
    #[must_use]
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.entries.contains_key(&TypeId::of::<T>())
    }

    /// Number of registered resources.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no resources are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl fmt::Debug for Resources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Resources")
            .field("len", &self.entries.len())
            .finish()
    }
}

fn downcast_arc<T: Send + Sync + 'static>(arc: ErasedResource) -> Option<Arc<T>> {
    arc.downcast::<T>().ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct DbPool(u32);

    #[derive(Debug, PartialEq, Eq)]
    struct Config {
        name: &'static str,
    }

    #[test]
    fn registry_insert_then_get_returns_value() {
        let mut registry = ResourceRegistry::new();
        assert!(registry.insert(DbPool(42)).is_none());

        let pool = registry.get::<DbPool>().unwrap();
        assert_eq!(*pool, DbPool(42));
    }

    #[test]
    fn registry_insert_replaces_existing_resource() {
        let mut registry = ResourceRegistry::new();
        registry.insert(DbPool(1));
        let previous = registry.insert(DbPool(2)).unwrap();
        assert_eq!(*previous, DbPool(1));

        let current = registry.get::<DbPool>().unwrap();
        assert_eq!(*current, DbPool(2));
    }

    #[test]
    fn registry_distinct_types_coexist() {
        let mut registry = ResourceRegistry::new();
        registry.insert(DbPool(7));
        registry.insert(Config { name: "wala" });

        assert_eq!(registry.len(), 2);
        assert_eq!(*registry.get::<DbPool>().unwrap(), DbPool(7));
        assert_eq!(*registry.get::<Config>().unwrap(), Config { name: "wala" });
    }

    #[test]
    fn registry_missing_type_returns_none() {
        let registry = ResourceRegistry::new();
        assert!(registry.get::<DbPool>().is_none());
        assert!(!registry.contains::<DbPool>());
        assert!(registry.is_empty());
    }

    #[test]
    fn resources_round_trips_through_build() {
        let mut registry = ResourceRegistry::new();
        registry.insert(DbPool(9));
        let resources = registry.build();

        assert_eq!(resources.len(), 1);
        assert!(resources.contains::<DbPool>());
        assert_eq!(*resources.get::<DbPool>().unwrap(), DbPool(9));
        assert!(resources.get::<Config>().is_none());
    }

    #[test]
    fn resources_clone_shares_storage() {
        let mut registry = ResourceRegistry::new();
        registry.insert(DbPool(3));
        let a = registry.build();
        let b = Clone::clone(&a);

        let pool_a = a.get::<DbPool>().unwrap();
        let pool_b = b.get::<DbPool>().unwrap();
        assert!(Arc::ptr_eq(&pool_a, &pool_b));
    }

    #[test]
    fn resources_empty_is_truly_empty() {
        let resources = Resources::empty();
        assert!(resources.is_empty());
        assert_eq!(resources.len(), 0);
        assert!(resources.get::<DbPool>().is_none());
    }

    #[test]
    fn insert_arc_accepts_externally_owned_value() {
        let shared = Arc::new(DbPool(11));
        let mut registry = ResourceRegistry::new();
        assert!(registry.insert_arc(Arc::clone(&shared)).is_none());

        let pulled = registry.get::<DbPool>().unwrap();
        assert!(Arc::ptr_eq(&shared, &pulled));
    }
}

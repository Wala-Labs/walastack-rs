//! Database capability for the WalaStack Runtime Kernel.
//!
//! Per the locked architecture, the `Database` capability ships as an
//! **identity-only** marker — providers expose their native APIs
//! (e.g. `sqlx::SqlitePool`, `sqlx::PgPool`) rather than implementing
//! a lowest-common-denominator trait. Handler code that needs to be
//! portable across backends should be written against a user-owned
//! repository abstraction, not against a framework-owned `Database`
//! trait.
//!
//! ## What plugins register
//!
//! Each provider plugin registers the pool under **two** capability
//! slots:
//!
//! - The concrete provider type (e.g. `SqlitePool`) — for typed access
//!   in handler code.
//! - The [`Database`] marker trait — for discoverability and
//!   capability-requirement validation in Plugins that need *some*
//!   database without caring which.
//!
//! ## Feature flags
//!
//! - `sqlite` *(default)* — enables [`sqlite::SqlitePlugin`].
//! - `postgres` — enables `postgres::PostgresPlugin`.
//!
//! ## Example
//!
//! ```no_run
//! # use walastack_runtime::Runtime;
//! # use walastack_db::sqlite::SqlitePlugin;
//! # async fn _example() -> Result<(), walastack_runtime::RuntimeError> {
//! Runtime::builder()
//!     .with_plugin(SqlitePlugin::in_memory())
//!     .build()?
//!     .start()
//!     .await
//! # }
//! ```

#![allow(clippy::missing_errors_doc)]

use std::any::Any;

/// Identity marker for the Database capability.
///
/// Providers (sqlite, postgres, …) register their concrete pool type
/// under this marker so that capability-requirement validation in
/// downstream Plugins can request "some database" via
/// [`walastack_runtime::CapabilityRequirement::any::<dyn Database>()`].
///
/// Per the locked architecture, this is a **marker trait only** —
/// providers do not implement a homogenized SQL API. Handler code
/// reaches for the concrete provider type (e.g. `Arc<SqlitePool>`)
/// through `ctx.capability::<SqlitePool>()` and uses the provider's
/// native API directly.
pub trait Database: Any + Send + Sync + 'static {}

// ---------------------------------------------------------------------------
// sqlite
// ---------------------------------------------------------------------------

#[cfg(feature = "sqlite")]
pub mod sqlite {
    //! Sqlite provider for the [`Database`] capability.
    //!
    //! Built on [`sqlx::SqlitePool`]. Connections are opened lazily on
    //! first use; URL validation happens at plugin construction time.
    //!
    //! Sqlite is the sovereign-friendly default — embedded, no external
    //! server, no network dependency. Use `:memory:` for tests and
    //! short-lived demos; use a file path (e.g. `sqlite:///var/lib/app.db`)
    //! for persistent deployments.

    use std::sync::Arc;

    use sqlx::SqlitePool;
    use walastack_runtime::{CapabilityRegistry, Plugin};

    use crate::Database;

    impl Database for SqlitePool {}

    /// Plugin that registers a [`sqlx::SqlitePool`] as both the concrete
    /// provider and the [`Database`] marker capability.
    pub struct SqlitePlugin {
        pool: SqlitePool,
    }

    impl SqlitePlugin {
        /// Construct a plugin with a lazy-connected pool at the given
        /// URL.
        ///
        /// Connections are opened on first query. URL parsing failures
        /// are surfaced here so misconfiguration is caught at plugin
        /// construction time, not on first query.
        pub fn new(url: &str) -> Result<Self, sqlx::Error> {
            Ok(Self {
                pool: SqlitePool::connect_lazy(url)?,
            })
        }

        /// Construct a plugin backed by an in-memory sqlite database
        /// (`sqlite::memory:`). Useful for tests and short-lived demos.
        ///
        /// # Panics
        ///
        /// Panics only if `sqlx`'s URL parser rejects the literal
        /// `"sqlite::memory:"` — which it does not under any documented
        /// behavior. Treat a panic here as an sqlx invariant violation
        /// rather than user-recoverable misconfiguration.
        #[must_use]
        pub fn in_memory() -> Self {
            #[allow(clippy::expect_used)]
            let pool = SqlitePool::connect_lazy("sqlite::memory:")
                .expect("in-memory sqlite URL is always valid");
            Self { pool }
        }

        /// Construct a plugin wrapping an existing [`sqlx::SqlitePool`].
        ///
        /// Useful when the application code already owns a pool and
        /// wants to share it with the kernel.
        #[must_use]
        pub const fn from_pool(pool: SqlitePool) -> Self {
            Self { pool }
        }

        /// Borrow the underlying pool. Allows the constructing
        /// application to retain a clone for direct use without going
        /// through the capability registry.
        #[must_use]
        pub const fn pool(&self) -> &SqlitePool {
            &self.pool
        }
    }

    impl Plugin for SqlitePlugin {
        fn name(&self) -> &'static str {
            "sqlite"
        }

        fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
            let typed: Arc<SqlitePool> = Arc::new(self.pool.clone());
            registry.register_default::<SqlitePool>(typed);

            let marker: Arc<dyn Database> = Arc::new(self.pool.clone());
            registry.register_default::<dyn Database>(marker);
        }
    }

    impl std::fmt::Debug for SqlitePlugin {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SqlitePlugin").finish_non_exhaustive()
        }
    }
}

// ---------------------------------------------------------------------------
// Prelude
// ---------------------------------------------------------------------------

/// Common imports for applications using `walastack-db`.
///
/// ```rust
/// use walastack_db::prelude::*;
/// ```
///
/// Re-exports:
/// - [`Database`] capability trait
/// - [`sqlite::SqlitePlugin`] (when the `sqlite` feature is enabled)
/// - [`postgres::PostgresPlugin`] (when the `postgres` feature is enabled)
pub mod prelude {
    pub use crate::Database;

    #[cfg(feature = "sqlite")]
    pub use crate::sqlite::SqlitePlugin;

    #[cfg(feature = "postgres")]
    pub use crate::postgres::PostgresPlugin;
}

// ---------------------------------------------------------------------------
// postgres
// ---------------------------------------------------------------------------

#[cfg(feature = "postgres")]
pub mod postgres {
    //! Postgres provider for the [`Database`] capability.
    //!
    //! Built on [`sqlx::PgPool`]. Connections are opened lazily on first
    //! use; URL validation happens at plugin construction time.
    //!
    //! Postgres is the production-cloud-friendly provider. Sovereign
    //! deployments typically prefer the [`sqlite`](crate::sqlite) provider
    //! (embedded, no external server). Operators can compose both with
    //! the [Named Capability Registry](walastack_runtime::CapabilityRegistry)
    //! when distinct workloads (e.g. primary + analytics) require
    //! different backends.

    use std::sync::Arc;

    use sqlx::PgPool;
    use walastack_runtime::{CapabilityRegistry, Plugin};

    use crate::Database;

    impl Database for PgPool {}

    /// Plugin that registers a [`sqlx::PgPool`] as both the concrete
    /// provider and the [`Database`] marker capability.
    pub struct PostgresPlugin {
        pool: PgPool,
    }

    impl PostgresPlugin {
        /// Construct a plugin with a lazy-connected pool at the given
        /// URL.
        pub fn new(url: &str) -> Result<Self, sqlx::Error> {
            Ok(Self {
                pool: PgPool::connect_lazy(url)?,
            })
        }

        /// Construct a plugin wrapping an existing [`sqlx::PgPool`].
        #[must_use]
        pub const fn from_pool(pool: PgPool) -> Self {
            Self { pool }
        }

        /// Borrow the underlying pool.
        #[must_use]
        pub const fn pool(&self) -> &PgPool {
            &self.pool
        }
    }

    impl Plugin for PostgresPlugin {
        fn name(&self) -> &'static str {
            "postgres"
        }

        fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
            let typed: Arc<PgPool> = Arc::new(self.pool.clone());
            registry.register_default::<PgPool>(typed);

            let marker: Arc<dyn Database> = Arc::new(self.pool.clone());
            registry.register_default::<dyn Database>(marker);
        }
    }

    impl std::fmt::Debug for PostgresPlugin {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PostgresPlugin").finish_non_exhaustive()
        }
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unnecessary_literal_bound,
        clippy::items_after_statements
    )]

    use sqlx::Row;
    use walastack_runtime::{CapabilityRequirement, Plugin, Runtime, RuntimeError};

    use super::Database;
    use super::sqlite::SqlitePlugin;

    #[tokio::test]
    async fn in_memory_plugin_constructs_successfully() {
        let plugin = SqlitePlugin::in_memory();
        assert_eq!(plugin.name(), "sqlite");
    }

    #[tokio::test]
    async fn runtime_with_plugin_exposes_sqlite_pool_capability() {
        let runtime = Runtime::builder()
            .with_plugin(SqlitePlugin::in_memory())
            .build()
            .unwrap();
        assert!(runtime.context().capability::<sqlx::SqlitePool>().is_some());
    }

    #[tokio::test]
    async fn runtime_with_plugin_exposes_database_marker_capability() {
        let runtime = Runtime::builder()
            .with_plugin(SqlitePlugin::in_memory())
            .build()
            .unwrap();
        assert!(runtime.context().capability::<dyn Database>().is_some());
    }

    #[tokio::test]
    async fn capability_requirement_any_database_is_satisfied() {
        struct NeedsDatabase;
        impl Plugin for NeedsDatabase {
            fn name(&self) -> &str {
                "needs-db"
            }
            fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
                vec![CapabilityRequirement::any::<dyn Database>()]
            }
        }

        let result = Runtime::builder()
            .with_plugin(SqlitePlugin::in_memory())
            .with_plugin(NeedsDatabase)
            .build();
        assert!(result.is_ok(), "Database requirement should be satisfied");
    }

    #[tokio::test]
    async fn capability_requirement_unsatisfied_when_no_db_plugin() {
        struct NeedsDatabase;
        impl Plugin for NeedsDatabase {
            fn name(&self) -> &str {
                "needs-db"
            }
            fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
                vec![CapabilityRequirement::any::<dyn Database>()]
            }
        }

        let err = Runtime::builder()
            .with_plugin(NeedsDatabase)
            .build()
            .unwrap_err();
        match err {
            RuntimeError::Plugin(p) => {
                assert!(p.to_string().contains("needs-db"));
            }
            RuntimeError::ServiceStart { .. } => {
                panic!("expected Plugin error, got ServiceStart")
            }
        }
    }

    #[tokio::test]
    async fn sqlite_pool_can_execute_queries_through_capability() {
        let runtime = Runtime::builder()
            .with_plugin(SqlitePlugin::in_memory())
            .build()
            .unwrap();

        let pool = runtime.context().capability::<sqlx::SqlitePool>().unwrap();

        sqlx::query("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .execute(&*pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO widgets (id, name) VALUES (1, 'wala')")
            .execute(&*pool)
            .await
            .unwrap();

        let row = sqlx::query("SELECT name FROM widgets WHERE id = 1")
            .fetch_one(&*pool)
            .await
            .unwrap();
        let name: String = row.get("name");
        assert_eq!(name, "wala");
    }

    #[tokio::test]
    async fn from_pool_wraps_existing_pool() {
        let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
        let plugin = SqlitePlugin::from_pool(pool);
        let runtime = Runtime::builder().with_plugin(plugin).build().unwrap();
        assert!(runtime.context().capability::<sqlx::SqlitePool>().is_some());
    }

    #[tokio::test]
    async fn plugin_pool_accessor_returns_underlying_pool() {
        let plugin = SqlitePlugin::in_memory();
        // Smoke test: pool() is callable and returns a reference.
        let _: &sqlx::SqlitePool = plugin.pool();
    }
}

#[cfg(all(test, feature = "postgres"))]
mod postgres_tests {
    #![allow(clippy::unwrap_used)]

    use super::postgres::PostgresPlugin;
    use walastack_runtime::{Plugin, Runtime};

    /// Compile-only: postgres plugin construction with a valid URL
    /// succeeds without an actual server (connection is lazy).
    #[tokio::test]
    async fn lazy_postgres_plugin_constructs_with_valid_url() {
        let plugin =
            PostgresPlugin::new("postgres://wala:secret@localhost:5432/walastack").unwrap();
        assert_eq!(plugin.name(), "postgres");
    }

    #[tokio::test]
    async fn runtime_with_postgres_plugin_registers_capability() {
        let plugin =
            PostgresPlugin::new("postgres://wala:secret@localhost:5432/walastack").unwrap();
        let runtime = Runtime::builder().with_plugin(plugin).build().unwrap();
        assert!(runtime.context().capability::<sqlx::PgPool>().is_some());
    }
}

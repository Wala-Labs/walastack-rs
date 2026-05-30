//! SQLite-backed [`JobStore`](crate::JobStore) provider.
//!
//! Built on `sqlx::SqlitePool`. Connections are opened lazily on first
//! query. Schema migration is opt-in via
//! [`SqliteJobStorePlugin::with_auto_migrate`](crate::sqlite::SqliteJobStorePlugin::with_auto_migrate)
//! — defaults to off so production deployments can own their migration
//! tooling.
//!
//! Sovereign-friendly: in-memory SQLite (`sqlite::memory:`) keeps the
//! durability semantics of the file-backed variant for tests + demos
//! without requiring on-disk state.

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use walastack_runtime::{
    BoxedServiceFuture, CapabilityRegistry, Plugin, ResourceRegistry, RuntimeContext, Service,
    ServiceContext, ServiceError,
};

use crate::{
    BoxedJobStoreFuture, JobId, JobName, JobRecord, JobStatus, JobStore, JobsConfig, JobsError,
    NewJob,
};

// =========================================================================
// SQL strings
// =========================================================================

/// Idempotent schema migration. Called by `SqliteAutoMigrateService`
/// at start when `with_auto_migrate` was opted in.
///
/// Schema notes:
/// - `id` is `INTEGER PRIMARY KEY AUTOINCREMENT` to give a stable
///   monotonic id even across deletes. Mapped to `JobId(u64)`.
/// - `payload` is `TEXT` holding a JSON document.
/// - `status` stores the [`JobStatus`] discriminant string.
/// - The `(queue, status, scheduled_at)` index supports the worker's
///   pull query.
const MIGRATION_SQL: &str = "
    CREATE TABLE IF NOT EXISTS walastack_jobs (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        name          TEXT    NOT NULL,
        payload       TEXT    NOT NULL,
        queue         TEXT    NOT NULL,
        attempt       INTEGER NOT NULL DEFAULT 0,
        max_attempts  INTEGER NOT NULL,
        status        TEXT    NOT NULL,
        enqueued_at   TEXT    NOT NULL,
        scheduled_at  TEXT    NOT NULL,
        last_error    TEXT
    );
    CREATE INDEX IF NOT EXISTS walastack_jobs_pull_idx
        ON walastack_jobs (queue, status, scheduled_at);
";

// =========================================================================
// JobStore implementation
// =========================================================================

/// SQLite-backed `JobStore`. Wraps a `sqlx::SqlitePool` cheaply
/// (the pool itself is internally `Arc`-counted).
#[derive(Clone, Debug)]
pub struct SqliteJobStore {
    pool: SqlitePool,
}

impl SqliteJobStore {
    /// Construct from an existing pool.
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Borrow the underlying pool.
    #[must_use]
    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Run the idempotent schema migration. Exposed publicly so
    /// operators can run migrations explicitly from their own tooling
    /// (recommended for production).
    pub async fn migrate(&self) -> Result<(), JobsError> {
        sqlx::query(MIGRATION_SQL)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| JobsError::Store(e.to_string()))
    }
}

impl JobStore for SqliteJobStore {
    fn enqueue(&self, job: NewJob) -> BoxedJobStoreFuture<Result<JobId, JobsError>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            let payload_json = serde_json::to_string(&job.payload)?;
            let id_i64: i64 = sqlx::query_scalar(
                "INSERT INTO walastack_jobs
                    (name, payload, queue, attempt, max_attempts, status, enqueued_at, \
                  scheduled_at)
                 VALUES (?, ?, ?, 0, ?, 'Pending', ?, ?)
                 RETURNING id",
            )
            .bind(&job.name.0)
            .bind(&payload_json)
            .bind(&job.queue)
            .bind(i64::from(job.max_attempts))
            .bind(Utc::now().to_rfc3339())
            .bind(job.scheduled_at.to_rfc3339())
            .fetch_one(&pool)
            .await
            .map_err(|e| JobsError::Store(e.to_string()))?;
            #[allow(clippy::cast_sign_loss)]
            Ok(JobId(id_i64 as u64))
        })
    }

    fn pull_next(
        &self,
        queues: Vec<String>,
    ) -> BoxedJobStoreFuture<Result<Option<JobRecord>, JobsError>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            // SQLite doesn't have FOR UPDATE SKIP LOCKED, but a single
            // BEGIN IMMEDIATE / UPDATE ... RETURNING is atomic against
            // other workers in the same process. Multi-process pulls
            // serialize on the database file's write lock. Build the
            // queue-list as a parameterized IN clause.
            if queues.is_empty() {
                return Ok(None);
            }
            let placeholders = vec!["?"; queues.len()].join(",");
            let now_iso = Utc::now().to_rfc3339();
            let sql = format!(
                "UPDATE walastack_jobs
                 SET status = 'Running', attempt = attempt + 1
                 WHERE id = (
                     SELECT id FROM walastack_jobs
                     WHERE queue IN ({placeholders})
                       AND status IN ('Pending', 'Retrying')
                       AND scheduled_at <= ?
                     ORDER BY scheduled_at ASC, id ASC
                     LIMIT 1
                 )
                 RETURNING id, name, payload, queue, attempt, max_attempts, status,
                           enqueued_at, scheduled_at"
            );
            let mut q = sqlx::query_as::<_, SqliteJobRow>(&sql);
            for queue in &queues {
                q = q.bind(queue);
            }
            q = q.bind(&now_iso);
            let row = q
                .fetch_optional(&pool)
                .await
                .map_err(|e| JobsError::Store(e.to_string()))?;
            row.map(SqliteJobRow::into_record).transpose()
        })
    }

    fn mark_completed(&self, id: JobId) -> BoxedJobStoreFuture<Result<(), JobsError>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            sqlx::query("UPDATE walastack_jobs SET status = 'Completed' WHERE id = ?")
                .bind(id_to_i64(id))
                .execute(&pool)
                .await
                .map_err(|e| JobsError::Store(e.to_string()))?;
            Ok(())
        })
    }

    fn mark_failed(
        &self,
        id: JobId,
        error: String,
        next_attempt_at: Option<DateTime<Utc>>,
    ) -> BoxedJobStoreFuture<Result<(), JobsError>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            match next_attempt_at {
                Some(at) => {
                    sqlx::query(
                        "UPDATE walastack_jobs
                         SET status = 'Retrying',
                             last_error = ?,
                             scheduled_at = ?
                         WHERE id = ?",
                    )
                    .bind(&error)
                    .bind(at.to_rfc3339())
                    .bind(id_to_i64(id))
                    .execute(&pool)
                    .await
                    .map_err(|e| JobsError::Store(e.to_string()))?;
                }
                None => {
                    sqlx::query(
                        "UPDATE walastack_jobs
                         SET status = 'Dead', last_error = ?
                         WHERE id = ?",
                    )
                    .bind(&error)
                    .bind(id_to_i64(id))
                    .execute(&pool)
                    .await
                    .map_err(|e| JobsError::Store(e.to_string()))?;
                }
            }
            Ok(())
        })
    }

    fn fetch(&self, id: JobId) -> BoxedJobStoreFuture<Result<Option<JobRecord>, JobsError>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            let row = sqlx::query_as::<_, SqliteJobRow>(
                "SELECT id, name, payload, queue, attempt, max_attempts, status,
                        enqueued_at, scheduled_at
                 FROM walastack_jobs WHERE id = ?",
            )
            .bind(id_to_i64(id))
            .fetch_optional(&pool)
            .await
            .map_err(|e| JobsError::Store(e.to_string()))?;
            row.map(SqliteJobRow::into_record).transpose()
        })
    }
}

fn id_to_i64(id: JobId) -> i64 {
    #[allow(clippy::cast_possible_wrap)]
    let v = id.0 as i64;
    v
}

// =========================================================================
// Row mapping
// =========================================================================

#[derive(sqlx::FromRow)]
struct SqliteJobRow {
    id: i64,
    name: String,
    payload: String,
    queue: String,
    attempt: i64,
    max_attempts: i64,
    status: String,
    enqueued_at: String,
    scheduled_at: String,
}

impl SqliteJobRow {
    fn into_record(self) -> Result<JobRecord, JobsError> {
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        Ok(JobRecord {
            id: JobId(self.id as u64),
            name: JobName(self.name),
            payload: serde_json::from_str(&self.payload)?,
            queue: self.queue,
            attempt: self.attempt as u32,
            max_attempts: self.max_attempts as u32,
            status: parse_status(&self.status)?,
            enqueued_at: parse_ts(&self.enqueued_at)?,
            scheduled_at: parse_ts(&self.scheduled_at)?,
        })
    }
}

fn parse_status(s: &str) -> Result<JobStatus, JobsError> {
    match s {
        "Pending" => Ok(JobStatus::Pending),
        "Running" => Ok(JobStatus::Running),
        "Completed" => Ok(JobStatus::Completed),
        "Retrying" => Ok(JobStatus::Retrying),
        "Dead" => Ok(JobStatus::Dead),
        other => Err(JobsError::Store(format!("unknown status {other:?}"))),
    }
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>, JobsError> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| JobsError::Store(format!("timestamp parse: {e}")))
}

// =========================================================================
// Plugin
// =========================================================================

/// Plugin that registers a [`SqliteJobStore`] under the
/// [`JobStore`] capability. Optionally runs the
/// schema migration at start.
pub struct SqliteJobStorePlugin {
    pool: SqlitePool,
    auto_migrate: bool,
}

impl SqliteJobStorePlugin {
    /// Construct with a lazy-connected pool at the given URL.
    pub fn new(url: &str) -> Result<Self, sqlx::Error> {
        Ok(Self {
            pool: SqlitePool::connect_lazy(url)?,
            auto_migrate: false,
        })
    }

    /// Construct backed by an in-memory SQLite database. Useful for
    /// tests and short-lived demos.
    ///
    /// # Panics
    ///
    /// Panics only if `sqlx` rejects the literal `"sqlite::memory:"` —
    /// which it does not under any documented behavior.
    #[must_use]
    pub fn in_memory() -> Self {
        #[allow(clippy::expect_used)]
        let pool = SqlitePool::connect_lazy("sqlite::memory:")
            .expect("in-memory sqlite URL is always valid");
        Self {
            pool,
            auto_migrate: false,
        }
    }

    /// Wrap an existing pool.
    #[must_use]
    pub const fn from_pool(pool: SqlitePool) -> Self {
        Self {
            pool,
            auto_migrate: false,
        }
    }

    /// Opt in to running the idempotent schema migration at start.
    /// Default off; production operators should own their migration
    /// tooling.
    #[must_use]
    pub const fn with_auto_migrate(mut self) -> Self {
        self.auto_migrate = true;
        self
    }

    /// Borrow the underlying pool.
    #[must_use]
    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

impl Plugin for SqliteJobStorePlugin {
    fn name(&self) -> &'static str {
        "sqlite-job-store"
    }

    fn register_resources(&self, registry: &mut ResourceRegistry) {
        // Stash the pool + auto-migrate flag as a Resource so the
        // migration service can reach them. Plugin registration is
        // sync; the actual migration runs at service start.
        registry.insert(SqliteJobStoreSettings {
            pool: self.pool.clone(),
            auto_migrate: self.auto_migrate,
        });
    }

    fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
        let store: Arc<dyn JobStore> = Arc::new(SqliteJobStore::new(self.pool.clone()));
        registry.register_default::<dyn JobStore>(store);
    }

    fn register_services(&self, planner: &mut walastack_runtime::ServicePlanner) {
        if self.auto_migrate {
            planner.add(SqliteAutoMigrateService);
        }
    }
}

impl fmt::Debug for SqliteJobStorePlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteJobStorePlugin")
            .field("auto_migrate", &self.auto_migrate)
            .finish_non_exhaustive()
    }
}

/// Resource that carries the pool + auto-migrate flag from plugin
/// registration through to service start.
#[derive(Clone)]
struct SqliteJobStoreSettings {
    pool: SqlitePool,
    auto_migrate: bool,
}

/// One-shot service that runs the idempotent migration at start. The
/// service returns a no-op JoinHandle after migration; it exists only
/// to bridge sync plugin registration to the async migration query.
struct SqliteAutoMigrateService;

impl Service for SqliteAutoMigrateService {
    fn name(&self) -> &'static str {
        "sqlite-job-store-migrate"
    }

    fn start(
        &self,
        ctx: ServiceContext,
    ) -> BoxedServiceFuture<std::result::Result<tokio::task::JoinHandle<()>, ServiceError>> {
        Box::pin(async move {
            let settings = ctx
                .runtime()
                .resource::<SqliteJobStoreSettings>()
                .ok_or_else(|| ServiceError::new("SqliteJobStoreSettings missing"))?;
            if settings.auto_migrate {
                let store = SqliteJobStore::new(settings.pool.clone());
                store
                    .migrate()
                    .await
                    .map_err(|e| ServiceError::new(format!("migration failed: {e}")))?;
                tracing::info!("walastack-jobs sqlite schema migrated");
            }
            // Park a trivial completion task so the supervision tree
            // sees a clean exit.
            Ok(tokio::spawn(async {}))
        })
    }
}

// =========================================================================
// Convenience for tests / callers — re-export the marker types from
// the parent module so users don't need a second use line.
// =========================================================================

#[doc(hidden)]
pub use crate::JobsConfig as _JobsConfigForDocLink;
#[allow(unused_imports)]
use _JobsConfigForDocLink as _;

// Use `RuntimeContext` to keep the import lifeline obvious for future
// readers.
#[allow(dead_code, unused_imports)]
fn _doc_link_runtime(_: RuntimeContext, _: JobsConfig) {}

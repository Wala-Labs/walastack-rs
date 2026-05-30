//! Durable job queue + worker fan-out for WalaStack.
//!
//! ## What this crate ships (Iteration 1 Sub-batch A — in-memory only)
//!
//! - [`Job`] — trait that turns a payload type into an executable unit.
//!   Associated `Output` / `Error` types + `const NAME` + an
//!   optional [`Job::metadata`] hook for queue routing / max attempts /
//!   timeout / backoff overrides.
//! - [`JobMetadata`] — extension seam for per-job configuration. Most
//!   fields default to `None`; the worker resolves them against
//!   `JobsConfig` at dispatch time.
//! - [`JobContext`] — handed to `Job::run`. Carries the runtime context
//!   (capability + resource access), the attempt number, and the
//!   `JobName` of the running job.
//! - [`JobStore`] — capability for persistence + queueing. Ships an
//!   in-memory provider ([`InMemoryJobStorePlugin`]) for dev, tests,
//!   and sovereign single-node deployments. SQLite + PostgreSQL
//!   providers ship in Sub-batch B / Iteration 2.
//! - [`JobsConfig`] — top-level configuration registered as a kernel
//!   `Resource`. **Third Resource-as-Configuration adoption** after
//!   `JwtSettings` (auth) and `OpenApiConfig` (openapi); see
//!   `project-ecosystem-conventions` memory.
//! - [`JobsPlugin`] — declarative composition: registers `JobsConfig`
//!   as a Resource, the handler dispatcher as a Resource, declares the
//!   `JobStore` capability requirement, and supervises a worker pool.
//! - Lifecycle events ([`JobEnqueued`], [`JobStarted`], [`JobCompleted`],
//!   [`JobFailed`], [`JobRetrying`], [`JobDead`]) — published on the
//!   kernel `EventBus` for observability + custom domain reactions.
//!
//! ## Deferred
//!
//! - SQLite + PostgreSQL providers — Sub-batch B / Iteration 2.
//! - Distributed coordination, priority queues, cancellation, webhooks,
//!   result futures, LISTEN/NOTIFY, job DAGs, sagas, macro-based
//!   registration — all out of Iteration 1 scope per locked decisions.
//!
//! ## Contracts
//!
//! - **At-least-once delivery.** Job handlers must be idempotent.
//! - **JSON serialization only.** Payloads are `serde::Serialize` +
//!   `serde::de::DeserializeOwned`.
//! - **Queue names are namespaces, not priorities.** A worker pulls
//!   from any queue listed in `JobsConfig::queues`; default is
//!   `["default"]`.

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
// "JobsPlugin", "SQLite", "PostgreSQL", "JSON" are domain names, not
// code identifiers. Backticking every mention hurts readability.
#![allow(clippy::doc_markdown)]
// worker_loop + dispatch_one have natural error-handling branches; the
// cognitive-complexity lint flags them but splitting further would
// fragment the lifecycle for no readability gain.
#![allow(clippy::cognitive_complexity)]
// MutexGuard scoping is intentional: store operations bind a `result`
// inside a scope to release the lock before the async boundary. The
// "tighten" lint suggests explicit drop() noise that doesn't help.
#![allow(clippy::significant_drop_tightening)]
// Several config builder methods could be `const fn` but the underlying
// Backoff variants involve f64 / non-const-friendly fields. Allowing the
// lint keeps the surface uniform.
#![allow(clippy::missing_const_for_fn)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use walastack_runtime::{
    Backoff, BoxedServiceFuture, CapabilityRegistry, CapabilityRequirement, Plugin,
    ResourceRegistry, RestartPolicy, RuntimeContext, Service, ServiceContext, ServiceError,
    ServicePlanner,
};

// =========================================================================
// Job trait + JobMetadata + JobContext + JobName
// =========================================================================

/// Logical name of a job type, used as the storage discriminator and
/// the dispatcher key. Defined as a newtype so callers cannot confuse
/// it with arbitrary `String`s.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JobName(pub String);

impl JobName {
    /// Construct from a `&str`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl fmt::Display for JobName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for JobName {
    fn from(s: &str) -> Self {
        Self(s.into())
    }
}

/// Per-job extension seam. All fields default to `None`; the worker
/// resolves missing values against [`JobsConfig`] at dispatch time.
///
/// Reserved-but-deferred: `timeout` and full `backoff` honoring land
/// in Sub-batch B / Iteration 2. `queue` and `max_attempts` are
/// honored in Sub-batch A.
#[derive(Clone, Debug, Default)]
pub struct JobMetadata {
    /// Logical queue namespace. Routing only — not a priority.
    /// `None` means "use `JobsConfig::default_queue`".
    pub queue: Option<String>,
    /// Overrides `JobsConfig::default_max_attempts` when set.
    pub max_attempts: Option<u32>,
    /// Reserved. Honored in a later iteration.
    pub timeout: Option<Duration>,
    /// Reserved. Honored in a later iteration. Falls back to
    /// `JobsConfig::default_backoff` for now.
    pub backoff: Option<Backoff>,
}

impl JobMetadata {
    /// Set the queue namespace.
    #[must_use]
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Override the maximum retry attempts for this job type.
    #[must_use]
    pub const fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = Some(n);
        self
    }

    /// Reserved. Currently captured but not honored — Sub-batch B+ will
    /// thread this into the worker dispatch path.
    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Reserved. Currently captured but not honored — Sub-batch B+ will
    /// honor this in place of `JobsConfig::default_backoff`.
    #[must_use]
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = Some(backoff);
        self
    }
}

/// Handed to [`Job::run`] at dispatch time. Carries the runtime
/// context (capability + resource lookup), the current attempt number
/// (1-indexed), and the job name.
#[derive(Clone, Debug)]
pub struct JobContext {
    runtime: RuntimeContext,
    name: JobName,
    attempt: u32,
}

impl JobContext {
    /// Construct. Public so providers in `tests/` or downstream crates
    /// can synthesize a context if needed; the worker is the normal
    /// caller.
    #[must_use]
    pub const fn new(runtime: RuntimeContext, name: JobName, attempt: u32) -> Self {
        Self {
            runtime,
            name,
            attempt,
        }
    }

    /// Borrow the runtime context for capability / resource lookup.
    #[must_use]
    pub const fn runtime(&self) -> &RuntimeContext {
        &self.runtime
    }

    /// The job's logical name (matches `Job::NAME`).
    #[must_use]
    pub const fn name(&self) -> &JobName {
        &self.name
    }

    /// 1-indexed attempt number. Equal to 1 on first dispatch.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// Trait that turns a payload type into an executable, persistable
/// unit of work.
///
/// The trait IS the executable unit — handler registration is implicit:
/// `JobsPlugin::register::<MyJob>()` is enough. No separate handler fn.
pub trait Job: Serialize + DeserializeOwned + Send + 'static {
    /// Output produced on success. `()` is fine for fire-and-forget.
    type Output: Send + 'static;
    /// Error type produced on failure. Must be `Display` so the worker
    /// can record the error string into the lifecycle event.
    type Error: fmt::Display + Send + 'static;

    /// Stable identifier for this job type. Used as the storage
    /// discriminator + dispatcher key. Must be unique across the
    /// registered handler set.
    const NAME: &'static str;

    /// Per-job-type metadata. Most jobs return the default; override to
    /// pin a queue, change `max_attempts`, etc.
    #[must_use]
    fn metadata() -> JobMetadata {
        JobMetadata::default()
    }

    /// Execute the job. The implementor receives ownership of the
    /// payload (it was deserialized just before this call).
    fn run(
        self,
        ctx: JobContext,
    ) -> impl std::future::Future<Output = std::result::Result<Self::Output, Self::Error>> + Send;
}

// =========================================================================
// JobStore capability + records
// =========================================================================

/// Persistence + queue surface. The capability is a `Send + Sync` trait
/// object with async methods returning boxed futures (no async-trait
/// dep; matches the `Service::start` pattern).
pub trait JobStore: Send + Sync + 'static {
    /// Persist a new job record. Returns the assigned id.
    fn enqueue(&self, job: NewJob) -> BoxedJobStoreFuture<Result<JobId, JobsError>>;

    /// Atomically pull the next pending job from any of the listed
    /// queues. Returns `None` when no job is ready. Implementations are
    /// expected to mark the returned job as `Running` before returning.
    fn pull_next(
        &self,
        queues: Vec<String>,
    ) -> BoxedJobStoreFuture<Result<Option<JobRecord>, JobsError>>;

    /// Mark a job completed.
    fn mark_completed(&self, id: JobId) -> BoxedJobStoreFuture<Result<(), JobsError>>;

    /// Mark a job failed and schedule the next attempt. If
    /// `next_attempt_at` is `None`, the job is dead.
    fn mark_failed(
        &self,
        id: JobId,
        error: String,
        next_attempt_at: Option<DateTime<Utc>>,
    ) -> BoxedJobStoreFuture<Result<(), JobsError>>;

    /// Fetch a record by id. Used by tests + observability tooling.
    fn fetch(&self, id: JobId) -> BoxedJobStoreFuture<Result<Option<JobRecord>, JobsError>>;
}

/// Boxed future shape for `JobStore` methods. Matches `Service::start`'s
/// `BoxedServiceFuture` shape — avoids `async-trait`.
pub type BoxedJobStoreFuture<T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'static>>;

/// Identifier for a persisted job. Backed by a `u64` for the in-memory
/// store; future SQL providers may map this to a `BIGINT` PK.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JobId(pub u64);

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A new job staged for `JobStore::enqueue`.
#[derive(Clone, Debug)]
pub struct NewJob {
    /// Logical job name (`Job::NAME`).
    pub name: JobName,
    /// JSON-serialized payload.
    pub payload: serde_json::Value,
    /// Target queue namespace.
    pub queue: String,
    /// Maximum total attempts (including the first).
    pub max_attempts: u32,
    /// Earliest time the job may be pulled. `now()` for immediate.
    pub scheduled_at: DateTime<Utc>,
}

/// A persisted job record returned by the store.
#[derive(Clone, Debug)]
pub struct JobRecord {
    /// Storage-assigned id.
    pub id: JobId,
    /// Logical job name.
    pub name: JobName,
    /// JSON payload.
    pub payload: serde_json::Value,
    /// Queue namespace.
    pub queue: String,
    /// Current attempt count (1 on first dispatch).
    pub attempt: u32,
    /// Maximum total attempts.
    pub max_attempts: u32,
    /// Current status.
    pub status: JobStatus,
    /// When the job was first enqueued.
    pub enqueued_at: DateTime<Utc>,
    /// Earliest time the job may be pulled.
    pub scheduled_at: DateTime<Utc>,
}

/// Lifecycle state of a persisted job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    /// Awaiting dispatch.
    Pending,
    /// Pulled by a worker, currently executing.
    Running,
    /// Completed successfully.
    Completed,
    /// Failed; awaiting retry with a scheduled time.
    Retrying,
    /// Failed; retry budget exhausted.
    Dead,
}

// =========================================================================
// Lifecycle events
// =========================================================================

/// Emitted on `JobStore::enqueue`.
#[derive(Clone, Debug)]
pub struct JobEnqueued {
    pub id: JobId,
    pub name: JobName,
    pub queue: String,
    pub scheduled_at: DateTime<Utc>,
}

/// Emitted when a worker pulls the job and is about to call `run`.
#[derive(Clone, Debug)]
pub struct JobStarted {
    pub id: JobId,
    pub name: JobName,
    pub attempt: u32,
}

/// Emitted on successful completion.
#[derive(Clone, Debug)]
pub struct JobCompleted {
    pub id: JobId,
    pub name: JobName,
    pub attempt: u32,
    pub duration: Duration,
}

/// Emitted when `run` returns an error. Retry decision lives in the
/// worker; this event always fires for failures, including the final
/// one that produces [`JobDead`].
#[derive(Clone, Debug)]
pub struct JobFailed {
    pub id: JobId,
    pub name: JobName,
    pub attempt: u32,
    /// Sanitized error string (`Display` impl of `Job::Error`).
    pub error: String,
}

/// Emitted when the worker schedules a retry.
#[derive(Clone, Debug)]
pub struct JobRetrying {
    pub id: JobId,
    pub name: JobName,
    pub next_attempt: u32,
    pub next_attempt_at: DateTime<Utc>,
}

/// Emitted when the retry budget is exhausted.
#[derive(Clone, Debug)]
pub struct JobDead {
    pub id: JobId,
    pub name: JobName,
    pub total_attempts: u32,
}

// =========================================================================
// JobsConfig (Resource)
// =========================================================================

/// Configuration for [`JobsPlugin`]. Registered as a kernel `Resource`
/// so the worker can resolve it at startup. **Third
/// Resource-as-Configuration adoption** — see
/// `project-ecosystem-conventions` memory.
#[derive(Clone, Debug)]
pub struct JobsConfig {
    /// Number of worker tasks to spawn under supervision.
    pub worker_count: u32,
    /// Queues each worker considers when pulling jobs. Order is
    /// preserved for fairness but is not a priority.
    pub queues: Vec<String>,
    /// Default queue used when a job's metadata does not pin one.
    pub default_queue: String,
    /// Default maximum attempts when `JobMetadata::max_attempts` is
    /// not set.
    pub default_max_attempts: u32,
    /// Default backoff applied to retries when `JobMetadata::backoff`
    /// is not set.
    pub default_backoff: Backoff,
    /// Poll interval when no job is available.
    pub poll_interval: Duration,
}

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            worker_count: 4,
            queues: vec!["default".into()],
            default_queue: "default".into(),
            default_max_attempts: 3,
            default_backoff: Backoff::Exponential {
                base: Duration::from_secs(1),
                factor: 2.0,
                max: Duration::from_secs(60 * 5),
            },
            poll_interval: Duration::from_millis(500),
        }
    }
}

impl JobsConfig {
    /// Set the worker pool size.
    #[must_use]
    pub const fn with_worker_count(mut self, n: u32) -> Self {
        self.worker_count = n;
        self
    }

    /// Replace the queue list. Use this when adding new queue
    /// namespaces beyond `default`.
    #[must_use]
    pub fn with_queues<I, S>(mut self, queues: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.queues = queues.into_iter().map(Into::into).collect();
        self
    }

    /// Set the default `max_attempts`.
    #[must_use]
    pub const fn with_default_max_attempts(mut self, n: u32) -> Self {
        self.default_max_attempts = n;
        self
    }

    /// Set the default backoff policy.
    #[must_use]
    pub fn with_default_backoff(mut self, backoff: Backoff) -> Self {
        self.default_backoff = backoff;
        self
    }

    /// Override the polling interval.
    #[must_use]
    pub const fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }
}

// =========================================================================
// JobsError
// =========================================================================

/// Errors returned by `walastack-jobs` internal operations.
#[derive(Clone, Debug)]
pub enum JobsError {
    /// JSON encode/decode failure.
    Serialization(String),
    /// Store-level failure (DB connection, transaction conflict, etc.).
    Store(String),
    /// Generic operational failure surfacing through the store.
    Other(String),
}

impl fmt::Display for JobsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
            Self::Store(msg) => write!(f, "store error: {msg}"),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for JobsError {}

impl From<serde_json::Error> for JobsError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}

// =========================================================================
// In-memory JobStore + plugin
// =========================================================================

/// In-memory job store. Sovereign-friendly default per Doctrine 2.
/// Jobs are held in process memory and do not survive restart.
///
/// Implements the canonical pull semantics (FIFO per-queue, scheduled
/// time honored, status transitions).
#[derive(Debug, Default)]
pub struct InMemoryJobStore {
    inner: Mutex<InMemoryStoreInner>,
}

#[derive(Debug, Default)]
struct InMemoryStoreInner {
    next_id: u64,
    // Keyed by queue name; each queue is a FIFO of job ids in pending /
    // retrying order. Lookup of the actual record goes through `records`.
    queues: BTreeMap<String, VecDeque<u64>>,
    records: HashMap<u64, JobRecord>,
}

impl InMemoryJobStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl JobStore for InMemoryJobStore {
    fn enqueue(&self, job: NewJob) -> BoxedJobStoreFuture<Result<JobId, JobsError>> {
        let result = {
            let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            inner.next_id += 1;
            let id = JobId(inner.next_id);
            let record = JobRecord {
                id,
                name: job.name,
                payload: job.payload,
                queue: job.queue.clone(),
                attempt: 0, // incremented on pull
                max_attempts: job.max_attempts,
                status: JobStatus::Pending,
                enqueued_at: Utc::now(),
                scheduled_at: job.scheduled_at,
            };
            inner.records.insert(id.0, record);
            inner.queues.entry(job.queue).or_default().push_back(id.0);
            Ok(id)
        };
        Box::pin(async move { result })
    }

    fn pull_next(
        &self,
        queues: Vec<String>,
    ) -> BoxedJobStoreFuture<Result<Option<JobRecord>, JobsError>> {
        let result = {
            let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            let now = Utc::now();
            let mut pulled: Option<JobRecord> = None;
            for queue_name in &queues {
                // First scan immutably to find an eligible id + its
                // position in the queue. Then mutate, which avoids the
                // simultaneous &mut queue + & records borrow.
                let pick: Option<(usize, u64)> = inner.queues.get(queue_name).and_then(|q| {
                    q.iter().copied().enumerate().find(|(_, id)| {
                        inner.records.get(id).is_some_and(|r| {
                            matches!(r.status, JobStatus::Pending | JobStatus::Retrying)
                                && r.scheduled_at <= now
                        })
                    })
                });
                if let Some((idx, id)) = pick {
                    if let Some(q) = inner.queues.get_mut(queue_name) {
                        q.remove(idx);
                    }
                    if let Some(record) = inner.records.get_mut(&id) {
                        record.attempt = record.attempt.saturating_add(1);
                        record.status = JobStatus::Running;
                        pulled = Some(record.clone());
                        break;
                    }
                }
            }
            Ok(pulled)
        };
        Box::pin(async move { result })
    }

    fn mark_completed(&self, id: JobId) -> BoxedJobStoreFuture<Result<(), JobsError>> {
        let result: Result<(), JobsError> = {
            let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(record) = inner.records.get_mut(&id.0) {
                record.status = JobStatus::Completed;
            }
            Ok(())
        };
        Box::pin(async move { result })
    }

    fn mark_failed(
        &self,
        id: JobId,
        _error: String,
        next_attempt_at: Option<DateTime<Utc>>,
    ) -> BoxedJobStoreFuture<Result<(), JobsError>> {
        let result: Result<(), JobsError> = {
            let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            // Snapshot the queue + status decision before re-borrowing.
            let Some((queue, dead)) = inner
                .records
                .get(&id.0)
                .map(|r| (r.queue.clone(), next_attempt_at.is_none()))
            else {
                return Box::pin(async { Ok(()) });
            };
            if let Some(record) = inner.records.get_mut(&id.0) {
                if dead {
                    record.status = JobStatus::Dead;
                } else {
                    record.status = JobStatus::Retrying;
                    if let Some(at) = next_attempt_at {
                        record.scheduled_at = at;
                    }
                }
            }
            if !dead {
                inner.queues.entry(queue).or_default().push_back(id.0);
            }
            Ok(())
        };
        Box::pin(async move { result })
    }

    fn fetch(&self, id: JobId) -> BoxedJobStoreFuture<Result<Option<JobRecord>, JobsError>> {
        let result = {
            let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            Ok(inner.records.get(&id.0).cloned())
        };
        Box::pin(async move { result })
    }
}

/// Plugin that registers an [`InMemoryJobStore`] under
/// `dyn JobStore`. Sovereign-friendly per Doctrine 2.
#[derive(Debug, Default)]
pub struct InMemoryJobStorePlugin;

impl InMemoryJobStorePlugin {
    /// Construct.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Plugin for InMemoryJobStorePlugin {
    fn name(&self) -> &'static str {
        "in-memory-job-store"
    }

    fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
        let store: Arc<dyn JobStore> = Arc::new(InMemoryJobStore::new());
        registry.register_default::<dyn JobStore>(store);
    }
}

// =========================================================================
// Dispatcher (handler registry)
// =========================================================================

/// Type-erased dispatcher entry. The `JobsPlugin::register::<J>` call
/// closes over the concrete `J::run` and stores a function that
/// deserializes JSON, calls `run`, and returns
/// `Result<(), String>` for the worker to record.
type HandlerFn = Arc<
    dyn Fn(serde_json::Value, JobContext) -> BoxedJobStoreFuture<Result<(), String>> + Send + Sync,
>;

/// Registered handlers, looked up by [`JobName`]. Registered as a
/// `Resource` so the worker can resolve it at startup.
#[derive(Clone, Default)]
pub struct JobsDispatcher {
    handlers: HashMap<JobName, HandlerEntry>,
}

#[derive(Clone)]
struct HandlerEntry {
    run: HandlerFn,
    metadata: JobMetadata,
}

impl fmt::Debug for JobsDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobsDispatcher")
            .field("registered", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl JobsDispatcher {
    fn register<J: Job>(&mut self) {
        let metadata = J::metadata();
        let run: HandlerFn = Arc::new(|payload, ctx| {
            Box::pin(async move {
                let job: J = match serde_json::from_value(payload) {
                    Ok(j) => j,
                    Err(e) => return Err(format!("payload deserialization failed: {e}")),
                };
                match job.run(ctx).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.to_string()),
                }
            })
        });
        self.handlers
            .insert(JobName::new(J::NAME), HandlerEntry { run, metadata });
    }

    /// Look up a handler by name. Public for tests + future
    /// observability tooling.
    #[must_use]
    pub fn metadata(&self, name: &JobName) -> Option<&JobMetadata> {
        self.handlers.get(name).map(|h| &h.metadata)
    }
}

// =========================================================================
// Worker Service
// =========================================================================

/// One worker. JobsPlugin registers `JobsConfig::worker_count` of these
/// under independent supervision so a worker crash restarts that
/// worker without bringing down peers.
struct Worker {
    name: &'static str,
}

impl Worker {
    /// Construct with a name. The name is `Box::leak`-ed once at
    /// plugin construction time to satisfy `Service::name`'s
    /// `&'static str` contract. The leak is bounded: one short string
    /// per worker, per process — typically <500 bytes total.
    fn new(id: u32) -> Self {
        let name: &'static str = Box::leak(format!("jobs-worker-{id}").into_boxed_str());
        Self { name }
    }
}

impl Service for Worker {
    fn name(&self) -> &'static str {
        self.name
    }

    fn start(
        &self,
        ctx: ServiceContext,
    ) -> BoxedServiceFuture<std::result::Result<JoinHandle<()>, ServiceError>> {
        let worker_name = self.name;
        Box::pin(async move {
            let runtime = ctx.runtime().clone();
            let shutdown = ctx.shutdown_signal();
            let store = runtime.capability::<dyn JobStore>().ok_or_else(|| {
                ServiceError::new(
                    "JobStore capability not registered; attach InMemoryJobStorePlugin or a \
                     persistent JobStore provider before JobsPlugin",
                )
            })?;
            let config = runtime
                .resource::<JobsConfig>()
                .ok_or_else(|| ServiceError::new("JobsConfig resource missing"))?;
            let dispatcher = runtime
                .resource::<JobsDispatcher>()
                .ok_or_else(|| ServiceError::new("JobsDispatcher resource missing"))?;
            tracing::info!(worker = %worker_name, "jobs worker started");
            let handle = tokio::spawn(worker_loop(store, config, dispatcher, shutdown, runtime));
            Ok(handle)
        })
    }
}

async fn worker_loop(
    store: Arc<dyn JobStore>,
    config: Arc<JobsConfig>,
    dispatcher: Arc<JobsDispatcher>,
    mut shutdown: walastack_runtime::ShutdownSignal,
    runtime: RuntimeContext,
) {
    loop {
        // Exit cleanly when the kernel signals shutdown.
        if shutdown.is_shut_down() {
            break;
        }
        let pulled = tokio::select! {
            () = shutdown.wait() => break,
            res = store.pull_next(config.queues.clone()) => res,
        };
        match pulled {
            Ok(Some(record)) => {
                dispatch_one(&store, &dispatcher, &runtime, &config, record).await;
            }
            Ok(None) => {
                // No work; sleep with shutdown-cancellable timer.
                tokio::select! {
                    () = shutdown.wait() => break,
                    () = tokio::time::sleep(config.poll_interval) => {}
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "jobs worker pull failed; sleeping before retry");
                tokio::select! {
                    () = shutdown.wait() => break,
                    () = tokio::time::sleep(config.poll_interval) => {}
                }
            }
        }
    }
    tracing::info!("jobs worker stopping");
}

async fn dispatch_one(
    store: &Arc<dyn JobStore>,
    dispatcher: &Arc<JobsDispatcher>,
    runtime: &RuntimeContext,
    config: &Arc<JobsConfig>,
    record: JobRecord,
) {
    let Some(entry) = dispatcher.handlers.get(&record.name).cloned() else {
        // Unknown job name. Mark dead so it doesn't loop forever.
        runtime.publish(JobFailed {
            id: record.id,
            name: record.name.clone(),
            attempt: record.attempt,
            error: format!("no handler registered for job '{}'", record.name),
        });
        runtime.publish(JobDead {
            id: record.id,
            name: record.name.clone(),
            total_attempts: record.attempt,
        });
        let _ = store
            .mark_failed(record.id, "no handler registered".into(), None)
            .await;
        return;
    };

    runtime.publish(JobStarted {
        id: record.id,
        name: record.name.clone(),
        attempt: record.attempt,
    });

    let ctx = JobContext::new(runtime.clone(), record.name.clone(), record.attempt);
    let started_at = std::time::Instant::now();
    let result = (entry.run)(record.payload.clone(), ctx).await;

    match result {
        Ok(()) => {
            runtime.publish(JobCompleted {
                id: record.id,
                name: record.name.clone(),
                attempt: record.attempt,
                duration: started_at.elapsed(),
            });
            if let Err(e) = store.mark_completed(record.id).await {
                tracing::error!(error = %e, id = %record.id, "failed to persist completion");
            }
        }
        Err(error) => {
            runtime.publish(JobFailed {
                id: record.id,
                name: record.name.clone(),
                attempt: record.attempt,
                error: error.clone(),
            });
            let max_attempts = record.max_attempts;
            if record.attempt >= max_attempts {
                runtime.publish(JobDead {
                    id: record.id,
                    name: record.name.clone(),
                    total_attempts: record.attempt,
                });
                if let Err(e) = store.mark_failed(record.id, error, None).await {
                    tracing::error!(error = %e, id = %record.id, "failed to persist dead");
                }
            } else {
                let next_attempt = record.attempt.saturating_add(1);
                // Backoff's retry_index is 0-indexed for the delay before
                // the second attempt; passing the next attempt as a
                // retry_index works out to the right magnitude.
                let delay = config.default_backoff.delay_for(record.attempt);
                let next_at = Utc::now()
                    + chrono::Duration::from_std(delay)
                        .unwrap_or_else(|_| chrono::Duration::seconds(1));
                runtime.publish(JobRetrying {
                    id: record.id,
                    name: record.name.clone(),
                    next_attempt,
                    next_attempt_at: next_at,
                });
                if let Err(e) = store.mark_failed(record.id, error, Some(next_at)).await {
                    tracing::error!(error = %e, id = %record.id, "failed to persist retry");
                }
            }
        }
    }
}

// =========================================================================
// JobsPlugin
// =========================================================================

/// Top-level plugin. Composes:
///
/// - `JobsConfig` as a `Resource` (third Resource-as-Configuration
///   adoption).
/// - `JobsDispatcher` (handler map) as a `Resource`.
/// - `JobStore` capability requirement.
/// - `JobsConfig::worker_count` independent `Worker` services
///   registered under `RestartPolicy::OnFailure`.
pub struct JobsPlugin {
    config: JobsConfig,
    dispatcher: JobsDispatcher,
    auto_migrate: bool,
}

impl JobsPlugin {
    /// Construct from a [`JobsConfig`].
    #[must_use]
    pub fn new(config: JobsConfig) -> Self {
        Self {
            config,
            dispatcher: JobsDispatcher::default(),
            auto_migrate: false,
        }
    }

    /// Register a [`Job`] implementation for dispatch.
    #[must_use]
    pub fn register<J: Job>(mut self) -> Self {
        self.dispatcher.register::<J>();
        self
    }

    /// Opt in to auto-migration. Stubbed in Sub-batch A (in-memory has
    /// no schema); SQLite provider in Sub-batch B reads this flag at
    /// service start.
    #[must_use]
    pub const fn with_auto_migrate(mut self) -> Self {
        self.auto_migrate = true;
        self
    }

    /// Whether auto-migrate was opted in. Used by storage providers in
    /// Sub-batch B+.
    #[must_use]
    pub const fn auto_migrate(&self) -> bool {
        self.auto_migrate
    }
}

impl fmt::Debug for JobsPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobsPlugin")
            .field("worker_count", &self.config.worker_count)
            .field("queues", &self.config.queues)
            .field("dispatcher", &self.dispatcher)
            .field("auto_migrate", &self.auto_migrate)
            .finish()
    }
}

impl Plugin for JobsPlugin {
    fn name(&self) -> &'static str {
        "jobs"
    }

    fn register_resources(&self, registry: &mut ResourceRegistry) {
        registry.insert(self.config.clone());
        registry.insert(self.dispatcher.clone());
    }

    fn register_services(&self, planner: &mut ServicePlanner) {
        for i in 0..self.config.worker_count {
            planner.add_supervised(
                Worker::new(i),
                RestartPolicy::OnFailure {
                    // Effectively unlimited — worker crash should not
                    // burn the supervision budget; we want continuous
                    // restart of crashed workers under realistic load.
                    max_attempts: u32::MAX,
                    backoff: Backoff::Linear {
                        base: Duration::from_secs(1),
                        step: Duration::ZERO,
                    },
                },
            );
        }
    }

    fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
        vec![CapabilityRequirement::any::<dyn JobStore>()]
    }
}

// =========================================================================
// Public enqueue helper
// =========================================================================

/// Enqueue a job through the kernel.
///
/// Looks up the `JobStore` capability, resolves the `JobsConfig`
/// defaults + per-job-type `JobMetadata` overrides, serializes the
/// payload, and stages the record. Publishes a [`JobEnqueued`] event
/// on success.
///
/// Returns the assigned [`JobId`]. The job will be picked up by a
/// worker at most `poll_interval` later (or immediately if a worker
/// is idle).
pub async fn enqueue<J: Job>(ctx: &RuntimeContext, job: J) -> Result<JobId, JobsError> {
    let store = ctx
        .capability::<dyn JobStore>()
        .ok_or_else(|| JobsError::Other("JobStore capability missing".into()))?;
    let config = ctx
        .resource::<JobsConfig>()
        .ok_or_else(|| JobsError::Other("JobsConfig resource missing".into()))?;
    let metadata = J::metadata();
    let queue = metadata
        .queue
        .clone()
        .unwrap_or_else(|| config.default_queue.clone());
    let max_attempts = metadata.max_attempts.unwrap_or(config.default_max_attempts);
    let payload = serde_json::to_value(&job)?;
    let now = Utc::now();
    let record = NewJob {
        name: JobName::new(J::NAME),
        payload,
        queue: queue.clone(),
        max_attempts,
        scheduled_at: now,
    };
    let id = store.enqueue(record).await?;
    ctx.publish(JobEnqueued {
        id,
        name: JobName::new(J::NAME),
        queue,
        scheduled_at: now,
    });
    Ok(id)
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use walastack_runtime::Runtime;

    // ---- Helpers ----

    fn build_runtime_with_jobs(plugin: JobsPlugin) -> Runtime {
        Runtime::builder()
            .with_plugin(InMemoryJobStorePlugin::new())
            .with_plugin(plugin)
            .build()
            .expect("runtime builds")
    }

    // ---- Job trait + JobMetadata ----

    #[derive(Clone, Serialize, Deserialize)]
    struct NoopJob;

    impl Job for NoopJob {
        type Output = ();
        type Error = String;
        const NAME: &'static str = "noop";

        async fn run(self, _ctx: JobContext) -> Result<(), String> {
            Ok(())
        }
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct CustomMetaJob;

    impl Job for CustomMetaJob {
        type Output = ();
        type Error = String;
        const NAME: &'static str = "custom_meta";
        fn metadata() -> JobMetadata {
            JobMetadata::default().queue("email").max_attempts(7)
        }
        async fn run(self, _ctx: JobContext) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn job_metadata_defaults_are_none() {
        let m = JobMetadata::default();
        assert!(m.queue.is_none());
        assert!(m.max_attempts.is_none());
        assert!(m.timeout.is_none());
    }

    #[test]
    fn job_metadata_builder_sets_queue_and_max_attempts() {
        let m = JobMetadata::default().queue("email").max_attempts(5);
        assert_eq!(m.queue.as_deref(), Some("email"));
        assert_eq!(m.max_attempts, Some(5));
    }

    // ---- JobsDispatcher ----

    #[test]
    fn dispatcher_records_metadata_per_job_type() {
        let mut d = JobsDispatcher::default();
        d.register::<NoopJob>();
        d.register::<CustomMetaJob>();
        let m_default = d.metadata(&JobName::new("noop")).unwrap();
        assert!(m_default.queue.is_none());
        let m_custom = d.metadata(&JobName::new("custom_meta")).unwrap();
        assert_eq!(m_custom.queue.as_deref(), Some("email"));
        assert_eq!(m_custom.max_attempts, Some(7));
    }

    // ---- InMemoryJobStore ----

    #[tokio::test]
    async fn in_memory_store_enqueue_then_pull_returns_record() {
        let store = InMemoryJobStore::new();
        let id = store
            .enqueue(NewJob {
                name: JobName::new("noop"),
                payload: serde_json::json!({}),
                queue: "default".into(),
                max_attempts: 3,
                scheduled_at: Utc::now(),
            })
            .await
            .unwrap();
        let pulled = store.pull_next(vec!["default".into()]).await.unwrap();
        let r = pulled.expect("pulled");
        assert_eq!(r.id, id);
        assert_eq!(r.attempt, 1);
        assert_eq!(r.status, JobStatus::Running);
    }

    #[tokio::test]
    async fn in_memory_store_skips_queues_without_pending_jobs() {
        let store = InMemoryJobStore::new();
        store
            .enqueue(NewJob {
                name: JobName::new("noop"),
                payload: serde_json::json!({}),
                queue: "email".into(),
                max_attempts: 3,
                scheduled_at: Utc::now(),
            })
            .await
            .unwrap();
        let pulled = store.pull_next(vec!["default".into()]).await.unwrap();
        assert!(pulled.is_none());
        let pulled_email = store.pull_next(vec!["email".into()]).await.unwrap();
        assert!(pulled_email.is_some());
    }

    #[tokio::test]
    async fn in_memory_store_mark_completed_updates_status() {
        let store = InMemoryJobStore::new();
        let id = store
            .enqueue(NewJob {
                name: JobName::new("noop"),
                payload: serde_json::json!({}),
                queue: "default".into(),
                max_attempts: 3,
                scheduled_at: Utc::now(),
            })
            .await
            .unwrap();
        store.pull_next(vec!["default".into()]).await.unwrap();
        store.mark_completed(id).await.unwrap();
        let r = store.fetch(id).await.unwrap().unwrap();
        assert_eq!(r.status, JobStatus::Completed);
    }

    #[tokio::test]
    async fn in_memory_store_mark_failed_with_retry_re_enqueues() {
        let store = InMemoryJobStore::new();
        let id = store
            .enqueue(NewJob {
                name: JobName::new("noop"),
                payload: serde_json::json!({}),
                queue: "default".into(),
                max_attempts: 3,
                scheduled_at: Utc::now(),
            })
            .await
            .unwrap();
        store.pull_next(vec!["default".into()]).await.unwrap();
        store
            .mark_failed(id, "boom".into(), Some(Utc::now()))
            .await
            .unwrap();
        let r = store.fetch(id).await.unwrap().unwrap();
        assert_eq!(r.status, JobStatus::Retrying);
        // Available to pull again.
        let pulled = store.pull_next(vec!["default".into()]).await.unwrap();
        assert!(pulled.is_some());
    }

    #[tokio::test]
    async fn in_memory_store_mark_failed_without_next_marks_dead() {
        let store = InMemoryJobStore::new();
        let id = store
            .enqueue(NewJob {
                name: JobName::new("noop"),
                payload: serde_json::json!({}),
                queue: "default".into(),
                max_attempts: 3,
                scheduled_at: Utc::now(),
            })
            .await
            .unwrap();
        store.pull_next(vec!["default".into()]).await.unwrap();
        store.mark_failed(id, "boom".into(), None).await.unwrap();
        let r = store.fetch(id).await.unwrap().unwrap();
        assert_eq!(r.status, JobStatus::Dead);
        let pulled = store.pull_next(vec!["default".into()]).await.unwrap();
        assert!(pulled.is_none());
    }

    // ---- InMemoryJobStorePlugin + capability ----

    #[tokio::test]
    async fn in_memory_plugin_registers_job_store_capability() {
        let runtime = Runtime::builder()
            .with_plugin(InMemoryJobStorePlugin::new())
            .build()
            .unwrap();
        let store = runtime.context().capability::<dyn JobStore>();
        assert!(store.is_some());
    }

    // ---- JobsPlugin requirements ----

    #[tokio::test]
    async fn jobs_plugin_requires_job_store_capability() {
        let err = Runtime::builder()
            .with_plugin(JobsPlugin::new(JobsConfig::default()))
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("JobStore"));
    }

    #[tokio::test]
    async fn jobs_plugin_satisfied_by_in_memory_store() {
        let runtime = Runtime::builder()
            .with_plugin(InMemoryJobStorePlugin::new())
            .with_plugin(JobsPlugin::new(JobsConfig::default()))
            .build();
        assert!(runtime.is_ok());
    }

    #[tokio::test]
    async fn jobs_plugin_registers_jobs_config_as_resource() {
        let runtime =
            build_runtime_with_jobs(JobsPlugin::new(JobsConfig::default().with_worker_count(2)));
        let config = runtime.context().resource::<JobsConfig>();
        assert!(config.is_some());
        assert_eq!(config.unwrap().worker_count, 2);
    }

    // ---- End-to-end through Runtime ----

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    #[derive(Clone, Serialize, Deserialize)]
    struct IncrementJob;

    impl Job for IncrementJob {
        type Output = ();
        type Error = String;
        const NAME: &'static str = "increment";

        async fn run(self, _ctx: JobContext) -> Result<(), String> {
            COUNTER.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn end_to_end_enqueue_runs_through_worker_to_completion() {
        COUNTER.store(0, Ordering::SeqCst);
        let plugin = JobsPlugin::new(
            JobsConfig::default()
                .with_worker_count(1)
                .with_poll_interval(Duration::from_millis(10)),
        )
        .register::<IncrementJob>();
        let mut runtime = build_runtime_with_jobs(plugin);
        // Start the runtime; workers come up.
        runtime.start().await.expect("runtime starts");
        // Enqueue.
        let id = enqueue(runtime.context(), IncrementJob).await.unwrap();
        // Wait up to ~2s for the worker to process the job.
        let store = runtime.context().capability::<dyn JobStore>().unwrap();
        let mut completed = false;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let r = store.fetch(id).await.unwrap().unwrap();
            if r.status == JobStatus::Completed {
                completed = true;
                break;
            }
        }
        assert!(completed, "job did not complete within 2s");
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
        runtime.shutdown_gracefully().await;
    }

    static FAIL_BEFORE_ATTEMPT: AtomicU32 = AtomicU32::new(2);

    #[derive(Clone, Serialize, Deserialize)]
    struct FlakyJob;

    impl Job for FlakyJob {
        type Output = ();
        type Error = String;
        const NAME: &'static str = "flaky";
        fn metadata() -> JobMetadata {
            JobMetadata::default().max_attempts(3)
        }
        async fn run(self, ctx: JobContext) -> Result<(), String> {
            if ctx.attempt() < FAIL_BEFORE_ATTEMPT.load(Ordering::SeqCst) {
                Err(format!("flake on attempt {}", ctx.attempt()))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn end_to_end_retries_until_success() {
        FAIL_BEFORE_ATTEMPT.store(2, Ordering::SeqCst);
        let plugin = JobsPlugin::new(
            JobsConfig::default()
                .with_worker_count(1)
                .with_poll_interval(Duration::from_millis(10))
                .with_default_backoff(Backoff::Linear {
                    base: Duration::from_millis(20),
                    step: Duration::ZERO,
                }),
        )
        .register::<FlakyJob>();
        let mut runtime = build_runtime_with_jobs(plugin);
        runtime.start().await.expect("runtime starts");
        let id = enqueue(runtime.context(), FlakyJob).await.unwrap();
        let store = runtime.context().capability::<dyn JobStore>().unwrap();
        let mut completed = false;
        let mut last_attempt = 0;
        for _ in 0..300 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let r = store.fetch(id).await.unwrap().unwrap();
            last_attempt = r.attempt;
            if r.status == JobStatus::Completed {
                completed = true;
                break;
            }
        }
        assert!(completed, "flaky job did not complete within 3s");
        assert!(last_attempt >= 2, "expected at least 2 attempts");
        runtime.shutdown_gracefully().await;
    }
}

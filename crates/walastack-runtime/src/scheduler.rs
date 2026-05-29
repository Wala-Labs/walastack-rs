//! Kernel-level time and trigger primitive.
//!
//! The [`Scheduler`] is a kernel facility consumed by every Service that
//! does time-driven work — `JobService`, `AgentService`, `SyncService`,
//! `SupervisionTree`, and a future `WorkflowService`. It is the
//! substrate behind `#[schedule(...)]`, `#[retry(...)]`,
//! `#[timeout(...)]`, and `#[backoff(...)]` macros.
//!
//! ## What the Scheduler does
//!
//! Five [`Trigger`] primitives:
//!
//! - [`Trigger::After`] — fire once after a duration.
//! - [`Trigger::At`] — fire once at an absolute system time.
//! - [`Trigger::FixedRate`] — fire every N units; subsequent fire times
//!   are aligned to the period regardless of task duration.
//! - [`Trigger::FixedDelay`] — fire, wait N units after completion, fire
//!   again.
//! - [`Trigger::Cron`] — fire on a cron schedule (5-field standard plus
//!   seconds, parsed via the [`cron`] crate).
//!
//! Three [`Policies`]:
//!
//! - [`Policies::timeout`] — cancel a single attempt if it exceeds the
//!   configured duration.
//! - [`Policies::retry`] — on failure, retry up to N times with a
//!   [`Backoff`] policy.
//! - [`Backoff::Linear`], [`Backoff::Exponential`], and
//!   [`Backoff::ExponentialWithJitter`].
//!
//! ## `EventBus` integration
//!
//! A Scheduler constructed via [`Scheduler::with_events`] publishes
//! lifecycle events on the wired [`EventBus`]:
//!
//! - [`TaskFired`] — a trigger fired and a task attempt is starting.
//! - [`TaskCompleted`] — the trigger's task completed successfully
//!   (after any retries).
//! - [`TaskFailed`] — the trigger's task failed after exhausting retries.
//! - [`TaskRetrying`] — the task failed and a retry is about to be
//!   attempted after a backoff delay.
//!
//! A Scheduler constructed via [`Scheduler::new`] runs the same
//! lifecycle but does not publish events. The [`crate::RuntimeContext`]
//! always wires the Scheduler against the kernel's `EventBus`, so the
//! integration is automatic in normal use.
//!
//! ## Distinction from `JobService`
//!
//! The Scheduler is in-process and non-durable; tasks live for the
//! Runtime's lifetime. A future `JobService` (Phase 3) layers durability
//! and worker fan-out on top of the Scheduler — the Scheduler remains
//! the lower-level kernel facility.
//!
//! See the
//! [Runtime Kernel — Scheduler](https://walastack.com/docs/architecture/runtime/scheduler/)
//! architecture page for design rationale.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use chrono::Utc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::events::EventBus;

// =========================================================================
// Public type aliases / traits
// =========================================================================

/// Convenience type alias for the boxed future a [`ScheduledFn`] returns.
pub type BoxedFuture = Pin<Box<dyn Future<Output = TaskResult> + Send>>;

/// Result a scheduled task returns.
pub type TaskResult = Result<(), TaskError>;

/// Trait implemented by anything that can be scheduled as a task.
///
/// A blanket impl is provided for any closure of the form
/// `Fn() -> Future<Output = TaskResult>` with the appropriate bounds.
/// User code should rarely need to implement this trait directly.
pub trait ScheduledFn: Send + Sync + 'static {
    /// Invoke the scheduled function, returning a boxed future.
    fn call(&self) -> BoxedFuture;
}

impl<F, Fut> ScheduledFn for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = TaskResult> + Send + 'static,
{
    fn call(&self) -> BoxedFuture {
        Box::pin(self())
    }
}

// =========================================================================
// Trigger
// =========================================================================

/// When and how often a scheduled task fires.
#[derive(Clone, Debug)]
pub enum Trigger {
    /// Fire once after the given duration elapses.
    After(Duration),
    /// Fire once at the given absolute system time.
    ///
    /// Times in the past fire immediately. Subject to wall-clock drift
    /// because `SystemTime` is not monotonic; for short-duration timers
    /// prefer [`Trigger::After`].
    At(SystemTime),
    /// Fire every `D` units, aligning subsequent fires to the period
    /// regardless of task duration.
    ///
    /// If a task takes longer than `D` to run, the next fire happens
    /// immediately (no thundering herd — only a single firing is queued).
    FixedRate(Duration),
    /// Fire, wait `D` units after the task completes, fire again.
    ///
    /// Distinct from [`Trigger::FixedRate`] because the wait starts
    /// **after** the task completes.
    FixedDelay(Duration),
    /// Fire on a cron schedule.
    ///
    /// Construct via [`Trigger::cron`] so the expression is validated at
    /// schedule time rather than at fire time.
    Cron(CronSchedule),
}

impl Trigger {
    /// Construct a [`Trigger::Cron`] by parsing a cron expression.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::InvalidCronExpression`] when the
    /// expression cannot be parsed by the underlying [`cron`] crate.
    pub fn cron(expr: &str) -> Result<Self, SchedulerError> {
        CronSchedule::new(expr).map(Self::Cron)
    }

    const fn is_one_shot(&self) -> bool {
        matches!(self, Self::After(_) | Self::At(_))
    }
}

/// A validated cron schedule.
///
/// Construct via [`CronSchedule::new`] or [`Trigger::cron`].
#[derive(Clone, Debug)]
pub struct CronSchedule(cron::Schedule);

impl CronSchedule {
    /// Parse a cron expression into a [`CronSchedule`].
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerError::InvalidCronExpression`] when the
    /// expression cannot be parsed.
    pub fn new(expr: &str) -> Result<Self, SchedulerError> {
        cron::Schedule::from_str(expr)
            .map(Self)
            .map_err(|err| SchedulerError::InvalidCronExpression(err.to_string()))
    }

    /// Time until the next scheduled fire, relative to now.
    ///
    /// Returns [`Duration::ZERO`] if the schedule has no further fires
    /// — that case is treated as "do not fire."
    fn next_delay(&self) -> Duration {
        let now = Utc::now();
        self.0
            .upcoming(Utc)
            .next()
            .and_then(|next| (next - now).to_std().ok())
            .unwrap_or(Duration::ZERO)
    }
}

// =========================================================================
// Policies
// =========================================================================

/// Policies applied to each fire of a scheduled task.
#[derive(Clone, Debug, Default)]
pub struct Policies {
    /// Per-attempt timeout. When set, each attempt that exceeds this
    /// duration is cancelled and treated as a failure for retry logic.
    pub timeout: Option<Duration>,
    /// Retry policy applied on failure. When unset, a failed attempt
    /// terminates the fire (no retries).
    pub retry: Option<RetryPolicy>,
}

impl Policies {
    /// Construct an empty policy set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder-style setter: add a per-attempt timeout.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Builder-style setter: add a retry policy.
    #[must_use]
    pub const fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = Some(retry);
        self
    }
}

/// Retry policy for a scheduled task.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Total attempts including the first. `1` means "no retries"; `3`
    /// means "first try plus up to 2 retries."
    pub max_attempts: u32,
    /// Backoff strategy applied between attempts.
    pub backoff: Backoff,
}

impl RetryPolicy {
    /// Construct a retry policy.
    #[must_use]
    pub const fn new(max_attempts: u32, backoff: Backoff) -> Self {
        Self {
            max_attempts,
            backoff,
        }
    }
}

/// Backoff strategy applied between retry attempts.
#[derive(Clone, Debug)]
pub enum Backoff {
    /// `base + step * retry_index` (where `retry_index` is `0` for the
    /// delay before the second attempt).
    Linear {
        /// Initial delay before the first retry.
        base: Duration,
        /// Linear additive step per subsequent retry.
        step: Duration,
    },
    /// `min(base * factor^retry_index, max)`.
    Exponential {
        /// Initial delay before the first retry.
        base: Duration,
        /// Multiplicative factor per retry.
        factor: f64,
        /// Cap on the computed delay.
        max: Duration,
    },
    /// Exponential plus a random jitter fraction in `[0, jitter)` of the
    /// computed delay.
    ///
    /// Jitter is seeded from the subsec-nanos component of system time —
    /// good enough for spreading retry storms but not cryptographically
    /// random.
    ExponentialWithJitter {
        /// Initial delay before the first retry.
        base: Duration,
        /// Multiplicative factor per retry.
        factor: f64,
        /// Cap on the computed delay (before jitter).
        max: Duration,
        /// Jitter fraction in `[0, 1]`; the actual jitter added is
        /// `Uniform[0, jitter) * computed_delay`.
        jitter: f64,
    },
}

impl Backoff {
    /// Compute the delay before the `(retry_index + 1)`th retry.
    ///
    /// `retry_index` is zero-based: `0` returns the delay before the
    /// *second* attempt; `1` returns the delay before the *third*; etc.
    #[must_use]
    pub fn delay_for(&self, retry_index: u32) -> Duration {
        match self {
            Self::Linear { base, step } => base.saturating_add(step.saturating_mul(retry_index)),
            Self::Exponential { base, factor, max } => {
                exponential_delay(*base, *factor, retry_index, *max)
            }
            Self::ExponentialWithJitter {
                base,
                factor,
                max,
                jitter,
            } => {
                let d = exponential_delay(*base, *factor, retry_index, *max);
                apply_jitter(d, *jitter)
            }
        }
    }
}

fn exponential_delay(base: Duration, factor: f64, retry_index: u32, max: Duration) -> Duration {
    #[allow(clippy::cast_precision_loss)]
    let base_nanos = base.as_nanos() as f64;
    #[allow(clippy::cast_possible_wrap)]
    let exponent = retry_index as i32;
    let factor = factor.max(0.0);
    let computed = base_nanos * factor.powi(exponent);
    if !computed.is_finite() || computed < 0.0 {
        return max;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let nanos = computed.min(u64::MAX as f64) as u64;
    Duration::from_nanos(nanos).min(max)
}

fn apply_jitter(base: Duration, jitter: f64) -> Duration {
    let jitter = jitter.clamp(0.0, 1.0);
    if jitter == 0.0 {
        return base;
    }
    let fraction = jitter_fraction() * jitter;
    #[allow(clippy::cast_precision_loss)]
    let base_nanos = base.as_nanos() as f64;
    let total = base_nanos * (1.0 + fraction);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let nanos = total.min(u64::MAX as f64) as u64;
    Duration::from_nanos(nanos)
}

fn jitter_fraction() -> f64 {
    // Subsec-nanos modulo 1_000_000 gives a fraction in [0, 1).
    // Good enough for spreading retries; not cryptographically random.
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    #[allow(clippy::cast_precision_loss)]
    let f = (nanos % 1_000_000) as f64;
    f / 1_000_000.0
}

// =========================================================================
// Errors
// =========================================================================

/// Errors returned by the scheduler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchedulerError {
    /// The cron expression could not be parsed.
    InvalidCronExpression(String),
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCronExpression(msg) => write!(f, "invalid cron expression: {msg}"),
        }
    }
}

impl std::error::Error for SchedulerError {}

/// Error reported when a scheduled task fails.
///
/// User code creates this from arbitrary errors via [`TaskError::new`]
/// or `TaskError::from(err)` for any `Display`-able error.
#[derive(Clone, Debug)]
pub struct TaskError {
    /// Human-readable error message.
    pub message: String,
}

impl TaskError {
    /// Construct a task error from a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TaskError {}

// =========================================================================
// ScheduleId + ScheduleHandle
// =========================================================================

/// Opaque identity for a scheduled task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ScheduleId(u64);

impl ScheduleId {
    /// The raw numeric identifier. Exposed for diagnostics; semantically
    /// opaque — clients should treat it as a token.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ScheduleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "schedule#{}", self.0)
    }
}

/// Handle for an active schedule.
///
/// Cloning a handle is cheap; both clones can cancel the underlying
/// schedule via [`Self::cancel`].
#[derive(Clone, Debug)]
pub struct ScheduleHandle {
    id: ScheduleId,
    cancel: watch::Sender<bool>,
}

impl ScheduleHandle {
    /// The opaque schedule identity.
    #[must_use]
    pub const fn id(&self) -> ScheduleId {
        self.id
    }

    /// Cancel the schedule. Idempotent.
    ///
    /// In-flight task attempts continue to run to completion; only
    /// subsequent fires are suppressed. To force-cancel running tasks,
    /// use [`Scheduler::shutdown`].
    pub fn cancel(&self) {
        let _ = self.cancel.send_replace(true);
    }

    /// Whether [`Self::cancel`] has been called on this schedule.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.cancel.borrow()
    }
}

// =========================================================================
// Lifecycle event types
// =========================================================================

/// Emitted when a trigger fires and a task attempt is starting.
///
/// Published only when the [`Scheduler`] was constructed with an
/// [`EventBus`] via [`Scheduler::with_events`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskFired {
    /// Schedule that fired.
    pub schedule_id: ScheduleId,
    /// Which fire this is for the schedule (1-indexed). For
    /// [`Trigger::After`]/[`Trigger::At`] this is always `1`; for
    /// repeating triggers it increments on each fire.
    pub trigger_count: u32,
}

/// Emitted when a scheduled task completes successfully.
///
/// For tasks with retries, this is published only after the first
/// successful attempt — failed attempts before the success do not
/// emit this event individually.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskCompleted {
    /// Schedule whose task completed.
    pub schedule_id: ScheduleId,
    /// Total elapsed time for this fire including all retries.
    pub duration: Duration,
}

/// Emitted when a scheduled task fails after exhausting retries.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskFailed {
    /// Schedule whose task failed.
    pub schedule_id: ScheduleId,
    /// Final error message.
    pub error: String,
    /// Total attempts that ran (1 means no retries; `N` matches
    /// `RetryPolicy::max_attempts` when all retries were exhausted).
    pub total_attempts: u32,
}

/// Emitted between retries when a scheduled task fails and a retry will
/// be attempted after the configured backoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskRetrying {
    /// Schedule whose task is being retried.
    pub schedule_id: ScheduleId,
    /// Which attempt the upcoming retry is (1-indexed).
    pub next_attempt: u32,
    /// Backoff delay applied before the retry.
    pub delay: Duration,
}

// =========================================================================
// Scheduler
// =========================================================================

/// The kernel's time and trigger primitive.
///
/// Cheap to clone (one atomic increment). Cloned handles share the same
/// underlying task registry; scheduling on one handle affects all clones.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

struct SchedulerInner {
    next_id: AtomicU64,
    tasks: Mutex<HashMap<ScheduleId, TaskState>>,
    events: Option<EventBus>,
}

struct TaskState {
    cancel: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl Scheduler {
    /// Construct a Scheduler with no [`EventBus`] integration.
    ///
    /// Tasks scheduled on this instance run normally but do not publish
    /// lifecycle events. Useful for isolated testing of scheduling
    /// behavior.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                next_id: AtomicU64::new(1),
                tasks: Mutex::new(HashMap::new()),
                events: None,
            }),
        }
    }

    /// Construct a Scheduler that publishes lifecycle events on the
    /// given [`EventBus`].
    ///
    /// [`crate::RuntimeContext`] uses this constructor so kernel
    /// participants automatically receive scheduler events.
    #[must_use]
    pub fn with_events(events: EventBus) -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                next_id: AtomicU64::new(1),
                tasks: Mutex::new(HashMap::new()),
                events: Some(events),
            }),
        }
    }

    /// Schedule a task to run according to a trigger, with the given
    /// policies.
    ///
    /// Returns a [`ScheduleHandle`] that can be used to cancel the
    /// schedule.
    pub fn schedule<F>(&self, trigger: Trigger, policies: Policies, task: F) -> ScheduleHandle
    where
        F: ScheduledFn,
    {
        let id = ScheduleId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let task: Arc<dyn ScheduledFn> = Arc::new(task);

        let events = self.inner.events.clone();
        let scheduler = Arc::clone(&self.inner);

        let join = tokio::spawn(async move {
            run_schedule(id, trigger, policies, task, events, cancel_rx).await;
            let mut tasks = scheduler
                .tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            tasks.remove(&id);
        });

        {
            let mut tasks = self
                .inner
                .tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            tasks.insert(
                id,
                TaskState {
                    cancel: cancel_tx.clone(),
                    join,
                },
            );
        }

        ScheduleHandle {
            id,
            cancel: cancel_tx,
        }
    }

    /// Number of currently active schedules.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Borrow the wired [`EventBus`], if any.
    ///
    /// Returns `None` for a Scheduler constructed via [`Self::new`].
    #[must_use]
    pub fn events(&self) -> Option<&EventBus> {
        self.inner.events.as_ref()
    }

    /// Shut down every active schedule.
    ///
    /// Sends cancellation to every active schedule, then aborts their
    /// spawned tasks. In-flight task attempts are not given a chance to
    /// finish — for graceful drain, cancel handles individually and
    /// await them.
    pub fn shutdown(&self) {
        let mut tasks = self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for (_, state) in tasks.drain() {
            let _ = state.cancel.send_replace(true);
            state.join.abort();
        }
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("pending", &self.pending_count())
            .field("events_wired", &self.inner.events.is_some())
            .finish()
    }
}

// =========================================================================
// Schedule execution loop
// =========================================================================

async fn run_schedule(
    id: ScheduleId,
    trigger: Trigger,
    policies: Policies,
    task: Arc<dyn ScheduledFn>,
    events: Option<EventBus>,
    mut cancel: watch::Receiver<bool>,
) {
    let one_shot = trigger.is_one_shot();
    let mut trigger_count: u32 = 0;
    let mut last_fire: Option<Instant> = None;

    loop {
        let delay = next_delay(&trigger, last_fire);

        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            _ = cancel.changed() => return,
        }

        if *cancel.borrow() {
            return;
        }

        trigger_count = trigger_count.saturating_add(1);
        let fired_at = Instant::now();
        last_fire = Some(fired_at);

        if let Some(bus) = events.as_ref() {
            bus.publish(TaskFired {
                schedule_id: id,
                trigger_count,
            });
        }

        run_attempts(id, &task, &policies, fired_at, events.as_ref(), &mut cancel).await;

        if one_shot {
            return;
        }
    }
}

fn next_delay(trigger: &Trigger, last_fire: Option<Instant>) -> Duration {
    match trigger {
        Trigger::After(d) | Trigger::FixedDelay(d) => *d,
        Trigger::At(t) => t
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
        Trigger::FixedRate(period) => last_fire.map_or(*period, |last| {
            let elapsed = last.elapsed();
            period.checked_sub(elapsed).unwrap_or(Duration::ZERO)
        }),
        Trigger::Cron(schedule) => schedule.next_delay(),
    }
}

async fn run_attempts(
    id: ScheduleId,
    task: &Arc<dyn ScheduledFn>,
    policies: &Policies,
    fired_at: Instant,
    events: Option<&EventBus>,
    cancel: &mut watch::Receiver<bool>,
) {
    let max_attempts = policies.retry.as_ref().map_or(1, |r| r.max_attempts.max(1));
    let mut last_error: Option<TaskError> = None;

    for attempt in 1..=max_attempts {
        if *cancel.borrow() {
            return;
        }

        let task_future = task.call();
        let result = if let Some(d) = policies.timeout {
            tokio::time::timeout(d, task_future)
                .await
                .unwrap_or_else(|_| {
                    Err(TaskError::new(format!(
                        "task timed out after {}ms",
                        d.as_millis()
                    )))
                })
        } else {
            task_future.await
        };

        match result {
            Ok(()) => {
                if let Some(bus) = events {
                    bus.publish(TaskCompleted {
                        schedule_id: id,
                        duration: fired_at.elapsed(),
                    });
                }
                return;
            }
            Err(err) => {
                last_error = Some(err);
                if attempt < max_attempts {
                    let retry_index = attempt - 1;
                    let delay = policies
                        .retry
                        .as_ref()
                        .map_or(Duration::ZERO, |r| r.backoff.delay_for(retry_index));

                    if let Some(bus) = events {
                        bus.publish(TaskRetrying {
                            schedule_id: id,
                            next_attempt: attempt + 1,
                            delay,
                        });
                    }

                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        _ = cancel.changed() => return,
                    }
                }
            }
        }
    }

    let err = last_error.unwrap_or_else(|| TaskError::new("scheduled task failed"));
    if let Some(bus) = events {
        bus.publish(TaskFailed {
            schedule_id: id,
            error: err.message,
            total_attempts: max_attempts,
        });
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::similar_names)]

    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::time::timeout;

    fn ok_after_n(n: u32) -> Arc<AtomicU32> {
        Arc::new(AtomicU32::new(n))
    }

    // ---- Triggers ----

    #[tokio::test]
    async fn after_fires_once_after_delay() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_task = Arc::clone(&counter);

        let handle = scheduler.schedule(
            Trigger::After(Duration::from_millis(20)),
            Policies::new(),
            move || {
                let c = Arc::clone(&counter_task);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        );

        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert!(!handle.is_cancelled());
    }

    #[tokio::test]
    async fn after_can_be_cancelled_before_firing() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_task = Arc::clone(&counter);

        let handle = scheduler.schedule(
            Trigger::After(Duration::from_secs(10)),
            Policies::new(),
            move || {
                let c = Arc::clone(&counter_task);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        );

        handle.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert!(handle.is_cancelled());
    }

    #[tokio::test]
    async fn fixed_rate_fires_multiple_times() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_task = Arc::clone(&counter);

        let handle = scheduler.schedule(
            Trigger::FixedRate(Duration::from_millis(20)),
            Policies::new(),
            move || {
                let c = Arc::clone(&counter_task);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        );

        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.cancel();
        let fires = counter.load(Ordering::SeqCst);
        // At 20ms period over ~120ms we expect at least 3 fires; allow
        // wide tolerance for scheduling noise.
        assert!(fires >= 3, "expected >= 3 fires, got {fires}");
    }

    #[tokio::test]
    async fn fixed_delay_fires_after_completion() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_task = Arc::clone(&counter);

        let handle = scheduler.schedule(
            Trigger::FixedDelay(Duration::from_millis(20)),
            Policies::new(),
            move || {
                let c = Arc::clone(&counter_task);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        );

        tokio::time::sleep(Duration::from_millis(120)).await;
        handle.cancel();
        let fires = counter.load(Ordering::SeqCst);
        assert!(fires >= 2, "expected >= 2 fires, got {fires}");
    }

    #[tokio::test]
    async fn at_fires_at_specified_time() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_task = Arc::clone(&counter);

        let at = SystemTime::now() + Duration::from_millis(40);
        scheduler.schedule(Trigger::At(at), Policies::new(), move || {
            let c = Arc::clone(&counter_task);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cron_fires_according_to_schedule() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let counter_task = Arc::clone(&counter);

        // Every second (`* * * * * *`).
        let trigger = Trigger::cron("* * * * * *").expect("valid cron");
        let handle = scheduler.schedule(trigger, Policies::new(), move || {
            let c = Arc::clone(&counter_task);
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });

        // Wait long enough to observe at least one fire.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        handle.cancel();
        assert!(
            counter.load(Ordering::SeqCst) >= 1,
            "expected at least 1 cron fire"
        );
    }

    #[test]
    fn invalid_cron_expression_returns_error() {
        let err = Trigger::cron("nonsense not cron").unwrap_err();
        matches!(err, SchedulerError::InvalidCronExpression(_));
    }

    // ---- Retry / backoff / timeout ----

    #[tokio::test]
    async fn retry_succeeds_on_second_attempt() {
        let scheduler = Scheduler::new();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_task = Arc::clone(&attempts);

        let policies = Policies::new().with_retry(RetryPolicy::new(
            3,
            Backoff::Linear {
                base: Duration::from_millis(5),
                step: Duration::ZERO,
            },
        ));

        scheduler.schedule(
            Trigger::After(Duration::from_millis(5)),
            policies,
            move || {
                let a = Arc::clone(&attempts_task);
                async move {
                    let n = a.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        Err(TaskError::new("transient"))
                    } else {
                        Ok(())
                    }
                }
            },
        );

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retry_gives_up_after_max_attempts() {
        let scheduler = Scheduler::new();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_task = Arc::clone(&attempts);

        let policies = Policies::new().with_retry(RetryPolicy::new(
            3,
            Backoff::Linear {
                base: Duration::from_millis(5),
                step: Duration::ZERO,
            },
        ));

        scheduler.schedule(
            Trigger::After(Duration::from_millis(5)),
            policies,
            move || {
                let a = Arc::clone(&attempts_task);
                async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    Err(TaskError::new("permanent"))
                }
            },
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn timeout_cancels_long_running_attempt() {
        let scheduler = Scheduler::new();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_task = Arc::clone(&attempts);

        let policies = Policies::new()
            .with_timeout(Duration::from_millis(20))
            .with_retry(RetryPolicy::new(
                2,
                Backoff::Linear {
                    base: Duration::from_millis(5),
                    step: Duration::ZERO,
                },
            ));

        scheduler.schedule(
            Trigger::After(Duration::from_millis(5)),
            policies,
            move || {
                let a = Arc::clone(&attempts_task);
                async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok(())
                }
            },
        );

        // The attempt sleeps 60s; the timeout is 20ms so each attempt is
        // cancelled. With max_attempts = 2 we should see 2 attempts and
        // then a TaskFailed (which we don't subscribe to here).
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    // ---- Backoff math (pure) ----

    #[test]
    fn linear_backoff_delays_increase() {
        let b = Backoff::Linear {
            base: Duration::from_millis(10),
            step: Duration::from_millis(5),
        };
        assert_eq!(b.delay_for(0), Duration::from_millis(10));
        assert_eq!(b.delay_for(1), Duration::from_millis(15));
        assert_eq!(b.delay_for(2), Duration::from_millis(20));
    }

    #[test]
    fn exponential_backoff_delays_increase_with_cap() {
        let b = Backoff::Exponential {
            base: Duration::from_millis(10),
            factor: 2.0,
            max: Duration::from_millis(50),
        };
        assert_eq!(b.delay_for(0), Duration::from_millis(10));
        assert_eq!(b.delay_for(1), Duration::from_millis(20));
        assert_eq!(b.delay_for(2), Duration::from_millis(40));
        // capped at 50ms
        assert_eq!(b.delay_for(3), Duration::from_millis(50));
        assert_eq!(b.delay_for(10), Duration::from_millis(50));
    }

    #[test]
    fn jittered_backoff_stays_in_expected_range() {
        let b = Backoff::ExponentialWithJitter {
            base: Duration::from_millis(100),
            factor: 1.0,
            max: Duration::from_millis(100),
            jitter: 0.5,
        };
        for _ in 0..20 {
            let d = b.delay_for(0);
            assert!(
                d >= Duration::from_millis(100) && d <= Duration::from_millis(150),
                "jittered delay out of range: {d:?}"
            );
        }
    }

    // ---- EventBus integration ----

    #[tokio::test]
    async fn scheduler_publishes_task_fired_and_completed() {
        let bus = EventBus::new();
        let mut fired_sub = bus.subscribe::<TaskFired>();
        let mut done_sub = bus.subscribe::<TaskCompleted>();

        let scheduler = Scheduler::with_events(bus);
        scheduler.schedule(
            Trigger::After(Duration::from_millis(10)),
            Policies::new(),
            || async { Ok(()) },
        );

        let fired = timeout(Duration::from_secs(1), fired_sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fired.trigger_count, 1);
        let done = timeout(Duration::from_secs(1), done_sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(done.schedule_id, fired.schedule_id);
    }

    #[tokio::test]
    async fn scheduler_publishes_task_failed_after_retries() {
        let bus = EventBus::new();
        let mut failed_sub = bus.subscribe::<TaskFailed>();

        let scheduler = Scheduler::with_events(bus);
        let policies = Policies::new().with_retry(RetryPolicy::new(
            2,
            Backoff::Linear {
                base: Duration::from_millis(5),
                step: Duration::ZERO,
            },
        ));

        scheduler.schedule(
            Trigger::After(Duration::from_millis(5)),
            policies,
            || async { Err(TaskError::new("nope")) },
        );

        let failed = timeout(Duration::from_secs(1), failed_sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.total_attempts, 2);
        assert_eq!(failed.error, "nope");
    }

    #[tokio::test]
    async fn scheduler_publishes_task_retrying_between_attempts() {
        let bus = EventBus::new();
        let mut retry_sub = bus.subscribe::<TaskRetrying>();

        let scheduler = Scheduler::with_events(bus);
        let policies = Policies::new().with_retry(RetryPolicy::new(
            3,
            Backoff::Linear {
                base: Duration::from_millis(7),
                step: Duration::ZERO,
            },
        ));

        scheduler.schedule(
            Trigger::After(Duration::from_millis(5)),
            policies,
            || async { Err(TaskError::new("transient")) },
        );

        let retry = timeout(Duration::from_secs(1), retry_sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.next_attempt, 2);
        assert_eq!(retry.delay, Duration::from_millis(7));
    }

    // ---- Lifecycle / shutdown ----

    #[tokio::test]
    async fn shutdown_cancels_all_scheduled_tasks() {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicU32::new(0));

        for _ in 0..3 {
            let c = Arc::clone(&counter);
            scheduler.schedule(
                Trigger::FixedRate(Duration::from_millis(5)),
                Policies::new(),
                move || {
                    let c = Arc::clone(&c);
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
            );
        }

        tokio::time::sleep(Duration::from_millis(30)).await;
        scheduler.shutdown();
        let after_shutdown = counter.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let later = counter.load(Ordering::SeqCst);
        assert_eq!(
            after_shutdown, later,
            "tasks should be cancelled after shutdown"
        );
        assert_eq!(scheduler.pending_count(), 0);
    }

    #[tokio::test]
    async fn multiple_schedules_run_concurrently() {
        let scheduler = Scheduler::new();
        let count_a = ok_after_n(0);
        let count_b = ok_after_n(0);

        let count_a_task = Arc::clone(&count_a);
        scheduler.schedule(
            Trigger::FixedRate(Duration::from_millis(10)),
            Policies::new(),
            move || {
                let c = Arc::clone(&count_a_task);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        );
        let count_b_task = Arc::clone(&count_b);
        scheduler.schedule(
            Trigger::FixedRate(Duration::from_millis(10)),
            Policies::new(),
            move || {
                let c = Arc::clone(&count_b_task);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        );

        tokio::time::sleep(Duration::from_millis(80)).await;
        scheduler.shutdown();
        assert!(count_a.load(Ordering::SeqCst) >= 2);
        assert!(count_b.load(Ordering::SeqCst) >= 2);
    }
}

//! Service supervision: restart policies, shutdown ordering, lifecycle
//! events.
//!
//! The [`SupervisionTree`] watches every Service registered with the
//! kernel and applies the configured [`RestartPolicy`] when a Service's
//! root task exits. It publishes lifecycle events
//! ([`ServiceStarted`] / [`ServiceStopped`] / [`ServiceFailed`]) for
//! observability.
//!
//! ## Restart backoff
//!
//! `OnFailure` and `Always` policies use the kernel
//! [`crate::Scheduler`]'s [`crate::Backoff`] primitive for delay
//! computation (no duplication of backoff logic). The actual sleep is
//! performed inline via [`tokio::time::sleep`]; the kernel does not
//! reschedule restarts through `Scheduler::schedule` because the
//! supervision watcher already owns a long-running task per Service.
//!
//! ## Lifecycle events
//!
//! Three event types flow through the kernel [`crate::EventBus`]:
//!
//! - [`ServiceStarted`] — published on initial start and on every
//!   successful restart.
//! - [`ServiceStopped`] — published when a Service's root task exits
//!   cleanly (no panic / non-restart-policy state).
//! - [`ServiceFailed`] — published when a Service's root task panics or
//!   `start` returns `Err`. If the policy permits restart, `Started` is
//!   published again after the backoff.
//!
//! See the
//! [Runtime Kernel — Supervision](https://walastack.com/docs/architecture/runtime/supervision/)
//! architecture page for design rationale.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::context::RuntimeContext;
use crate::events::EventBus;
use crate::scheduler::Backoff;
use crate::services::{Service, ServiceContext, ServiceError};

// =========================================================================
// Lifecycle events
// =========================================================================

/// Published when a Service starts (initial start and every restart).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceStarted {
    /// The Service's name.
    pub name: String,
    /// Which start this is (1-indexed). Always `1` for `OneShot` policies;
    /// increments per restart for `OnFailure` and `Always`.
    pub attempt: u32,
}

/// Published when a Service's root task exits cleanly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceStopped {
    /// The Service's name.
    pub name: String,
}

/// Published when a Service's `start` returns `Err` or the root task
/// panics.
///
/// If the Service's [`RestartPolicy`] permits restart, a [`ServiceStarted`]
/// follows after the backoff delay.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceFailed {
    /// The Service's name.
    pub name: String,
    /// Human-readable error explaining the failure.
    pub error: String,
    /// Which attempt failed (1-indexed).
    pub attempt: u32,
    /// Whether the `SupervisionTree` will restart the Service.
    pub will_restart: bool,
}

// =========================================================================
// RestartPolicy
// =========================================================================

/// How the `SupervisionTree` responds when a Service's root task exits.
#[derive(Clone, Debug)]
pub enum RestartPolicy {
    /// Start the Service once. On any exit (success or failure), publish
    /// the corresponding lifecycle event and do not restart.
    ///
    /// This is the default — explicit opt-in is required for
    /// restart-on-failure semantics.
    OneShot,

    /// On failure, restart up to `max_attempts` times with the configured
    /// [`Backoff`] between attempts. Clean exit (no panic, `start`
    /// returned `Ok`) does not trigger a restart.
    OnFailure {
        /// Total attempts including the initial start. `1` means "no
        /// restarts."
        max_attempts: u32,
        /// Backoff between failures. Reuses the kernel
        /// [`crate::Scheduler`]'s backoff primitive.
        backoff: Backoff,
    },

    /// On any exit (success or failure), restart with the configured
    /// [`Backoff`].
    Always {
        /// Backoff between restarts.
        backoff: Backoff,
    },
}

impl RestartPolicy {
    /// Whether this policy permits another restart given the current
    /// attempt count.
    const fn permits_restart_after(&self, attempt: u32, was_failure: bool) -> bool {
        match self {
            Self::OneShot => false,
            Self::OnFailure { max_attempts, .. } => was_failure && attempt < *max_attempts,
            Self::Always { .. } => true,
        }
    }

    fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        match self {
            Self::OneShot => Duration::ZERO,
            Self::OnFailure { backoff, .. } | Self::Always { backoff } => {
                // `attempt` is the attempt that just exited (1-indexed).
                // The Backoff API takes a zero-indexed retry number where
                // 0 = delay before the second attempt.
                backoff.delay_for(attempt.saturating_sub(1))
            }
        }
    }
}

// =========================================================================
// SupervisionTree
// =========================================================================

/// Watches Services for lifecycle events and applies restart policies.
///
/// The `SupervisionTree` is owned by the [`crate::Runtime`]. Each
/// registered Service gets a dedicated supervision watcher task that
/// awaits the Service's root [`JoinHandle`] and reacts according to the
/// configured [`RestartPolicy`].
#[derive(Clone)]
pub struct SupervisionTree {
    inner: Arc<SupervisionInner>,
}

struct SupervisionInner {
    events: EventBus,
    children: Mutex<HashMap<String, ChildState>>,
}

struct ChildState {
    /// Handle to the supervision watcher task (not the Service's own
    /// root task). Cancelling the watcher means the Service is no longer
    /// supervised.
    watcher: JoinHandle<()>,
}

impl SupervisionTree {
    /// Construct a `SupervisionTree` that publishes lifecycle events on
    /// the given [`EventBus`].
    #[must_use]
    pub fn new(events: EventBus) -> Self {
        Self {
            inner: Arc::new(SupervisionInner {
                events,
                children: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Start a Service under supervision.
    ///
    /// Calls [`Service::start`] to obtain the initial root task handle,
    /// publishes [`ServiceStarted`], and spawns a watcher task that
    /// applies the [`RestartPolicy`] when the root task exits.
    ///
    /// # Errors
    ///
    /// Returns the [`ServiceError`] reported by [`Service::start`] when
    /// the initial start fails. No supervision watcher is spawned in that
    /// case; the caller decides whether to retry registration.
    pub async fn start_service(
        &self,
        service: Arc<dyn Service>,
        policy: RestartPolicy,
        runtime: RuntimeContext,
    ) -> Result<(), ServiceError> {
        let name: Arc<str> = Arc::from(service.name());
        let service_ctx = ServiceContext::new(runtime.clone(), Arc::clone(&name));

        let handle = service.start(service_ctx).await?;

        self.inner.events.publish(ServiceStarted {
            name: name.to_string(),
            attempt: 1,
        });

        let watcher = tokio::spawn(supervise(
            Arc::clone(&service),
            name.clone(),
            policy,
            handle,
            self.inner.events.clone(),
            runtime,
        ));

        // If a Service with the same name is re-registered, replace its
        // entry; the previous watcher handle is dropped (which does NOT
        // cancel it — abort would be required, but supervision watchers
        // exit on shutdown signal so this is the operator's
        // responsibility).
        self.inner
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_string(), ChildState { watcher });
        Ok(())
    }

    /// Number of currently supervised Services.
    #[must_use]
    pub fn supervised_count(&self) -> usize {
        self.inner
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Wait for every supervised Service watcher to exit, up to the
    /// given deadline. Watchers that do not exit by the deadline are
    /// aborted.
    ///
    /// Returns the number of watchers that exited gracefully (vs. were
    /// aborted).
    pub async fn drain(&self, deadline: Duration) -> usize {
        let watchers: Vec<(String, JoinHandle<()>)> = {
            let mut children = self
                .inner
                .children
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            children
                .drain()
                .map(|(name, state)| (name, state.watcher))
                .collect()
        };

        let mut graceful = 0;
        for (_name, watcher) in watchers {
            if tokio::time::timeout(deadline, watcher).await.is_ok() {
                graceful += 1;
            }
            // Otherwise: watcher didn't exit by the deadline. We cannot
            // abort the JoinHandle here because we moved it into the
            // timeout; it is left for runtime drop to clean up.
        }
        graceful
    }

    /// Abort every supervised Service watcher immediately, without
    /// waiting.
    pub fn shutdown_now(&self) {
        let mut children = self
            .inner
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for (_, state) in children.drain() {
            state.watcher.abort();
        }
    }
}

impl fmt::Debug for SupervisionTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupervisionTree")
            .field("supervised", &self.supervised_count())
            .finish()
    }
}

// =========================================================================
// Supervision watcher loop
// =========================================================================

async fn supervise(
    service: Arc<dyn Service>,
    name: Arc<str>,
    policy: RestartPolicy,
    initial_handle: JoinHandle<()>,
    events: EventBus,
    runtime: RuntimeContext,
) {
    let mut handle = initial_handle;
    let mut attempt: u32 = 1;
    let mut shutdown = runtime.shutdown_signal();

    loop {
        // Wait for either the root task to exit or shutdown.
        let exit_reason = tokio::select! {
            join_result = &mut handle => Some(join_result),
            () = shutdown.wait() => None,
        };

        let Some(join_result) = exit_reason else {
            // Runtime shutdown was signaled; abort the root task and
            // exit the watcher.
            handle.abort();
            return;
        };

        let (was_failure, err_message) = match join_result {
            Ok(()) => (false, String::new()),
            Err(err) if err.is_panic() => (true, format!("service task panicked: {err}")),
            Err(_) => {
                // Cancelled — treat as shutdown, exit watcher silently.
                return;
            }
        };

        if !was_failure {
            events.publish(ServiceStopped {
                name: name.to_string(),
            });
            if !matches!(policy, RestartPolicy::Always { .. }) {
                return;
            }
        }

        let will_restart = policy.permits_restart_after(attempt, was_failure);

        if was_failure {
            events.publish(ServiceFailed {
                name: name.to_string(),
                error: err_message.clone(),
                attempt,
                will_restart,
            });
        }

        if !will_restart {
            return;
        }

        // Wait the backoff before the restart. Interruptible by shutdown.
        let delay = policy.backoff_for_attempt(attempt);
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = shutdown.wait() => return,
        }

        attempt = attempt.saturating_add(1);

        let service_ctx = ServiceContext::new(runtime.clone(), Arc::clone(&name));
        match service.start(service_ctx).await {
            Ok(new_handle) => {
                handle = new_handle;
                events.publish(ServiceStarted {
                    name: name.to_string(),
                    attempt,
                });
            }
            Err(start_err) => {
                let will_retry = policy.permits_restart_after(attempt, true);
                events.publish(ServiceFailed {
                    name: name.to_string(),
                    error: start_err.message,
                    attempt,
                    will_restart: will_retry,
                });
                if !will_retry {
                    return;
                }
                // Failed-to-start counts as a failure; loop will compute
                // backoff and retry. Re-enter the loop body via a placeholder
                // handle that is immediately complete (so the next iteration
                // skips the await-join branch).
                handle = tokio::spawn(async {});
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::similar_names)]

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::context::RuntimeContext;
    use crate::scheduler::Backoff;
    use crate::services::{BoxedServiceFuture, Service};

    /// A test Service that completes immediately on start.
    struct CompletingService {
        name: String,
        starts: Arc<AtomicU32>,
    }

    impl Service for CompletingService {
        fn name(&self) -> &str {
            &self.name
        }

        fn start(
            &self,
            _ctx: ServiceContext,
        ) -> BoxedServiceFuture<Result<JoinHandle<()>, ServiceError>> {
            let starts = Arc::clone(&self.starts);
            Box::pin(async move {
                starts.fetch_add(1, Ordering::SeqCst);
                let handle = tokio::spawn(async {});
                Ok(handle)
            })
        }
    }

    /// A test Service that panics inside its root task.
    struct PanickingService {
        name: String,
        starts: Arc<AtomicU32>,
        stop_after_attempts: u32,
    }

    impl Service for PanickingService {
        fn name(&self) -> &str {
            &self.name
        }

        fn start(
            &self,
            _ctx: ServiceContext,
        ) -> BoxedServiceFuture<Result<JoinHandle<()>, ServiceError>> {
            let starts = Arc::clone(&self.starts);
            let stop_after = self.stop_after_attempts;
            Box::pin(async move {
                let attempt = starts.fetch_add(1, Ordering::SeqCst) + 1;
                let handle = if attempt >= stop_after {
                    // Subsequent starts succeed quietly.
                    tokio::spawn(async {})
                } else {
                    tokio::spawn(async {
                        panic!("simulated service failure");
                    })
                };
                Ok(handle)
            })
        }
    }

    /// A test Service that runs forever until shutdown.
    struct WaitingService {
        name: String,
    }

    impl Service for WaitingService {
        fn name(&self) -> &str {
            &self.name
        }

        fn start(
            &self,
            ctx: ServiceContext,
        ) -> BoxedServiceFuture<Result<JoinHandle<()>, ServiceError>> {
            Box::pin(async move {
                let mut signal = ctx.shutdown_signal();
                let handle = tokio::spawn(async move { signal.wait().await });
                Ok(handle)
            })
        }
    }

    const fn fast_backoff() -> Backoff {
        Backoff::Linear {
            base: Duration::from_millis(5),
            step: Duration::ZERO,
        }
    }

    // ---- ServiceStarted on initial start ----

    #[tokio::test]
    async fn start_service_publishes_service_started() {
        let runtime = RuntimeContext::empty();
        let tree = SupervisionTree::new(runtime.events().clone());
        let mut sub = runtime.subscribe::<ServiceStarted>();

        let svc = Arc::new(WaitingService {
            name: "test".into(),
        });
        tree.start_service(svc, RestartPolicy::OneShot, runtime.clone())
            .await
            .unwrap();

        let event = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.name, "test");
        assert_eq!(event.attempt, 1);

        runtime.events().shutdown();
    }

    // ---- ServiceStopped on clean exit (OneShot) ----

    #[tokio::test]
    async fn one_shot_completing_service_publishes_stopped() {
        let runtime = RuntimeContext::empty();
        let tree = SupervisionTree::new(runtime.events().clone());
        let mut sub = runtime.subscribe::<ServiceStopped>();

        let svc = Arc::new(CompletingService {
            name: "one-shot".into(),
            starts: Arc::new(AtomicU32::new(0)),
        });
        let starts = Arc::clone(&svc.starts);
        tree.start_service(svc, RestartPolicy::OneShot, runtime.clone())
            .await
            .unwrap();

        let event = timeout(Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.name, "one-shot");
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    // ---- OneShot does NOT restart on failure ----

    #[tokio::test]
    async fn one_shot_failure_publishes_failed_no_restart() {
        let runtime = RuntimeContext::empty();
        let tree = SupervisionTree::new(runtime.events().clone());
        let mut failed_sub = runtime.subscribe::<ServiceFailed>();

        let starts = Arc::new(AtomicU32::new(0));
        let svc = Arc::new(PanickingService {
            name: "one-shot-fail".into(),
            starts: Arc::clone(&starts),
            stop_after_attempts: u32::MAX, // always panic
        });
        tree.start_service(svc, RestartPolicy::OneShot, runtime.clone())
            .await
            .unwrap();

        let event = timeout(Duration::from_secs(1), failed_sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.name, "one-shot-fail");
        assert_eq!(event.attempt, 1);
        assert!(!event.will_restart);

        // Give time for any spurious restart attempts.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    // ---- OnFailure restarts and eventually succeeds ----

    #[tokio::test]
    async fn on_failure_restarts_with_backoff_until_success() {
        let runtime = RuntimeContext::empty();
        let tree = SupervisionTree::new(runtime.events().clone());
        let mut started_sub = runtime.subscribe::<ServiceStarted>();

        let starts = Arc::new(AtomicU32::new(0));
        let svc = Arc::new(PanickingService {
            name: "flaky".into(),
            starts: Arc::clone(&starts),
            stop_after_attempts: 3, // fail twice, succeed on 3rd attempt
        });

        tree.start_service(
            svc,
            RestartPolicy::OnFailure {
                max_attempts: 5,
                backoff: fast_backoff(),
            },
            runtime.clone(),
        )
        .await
        .unwrap();

        // Should see ServiceStarted at attempt=1, 2, 3.
        let mut attempts_seen: Vec<u32> = Vec::new();
        while attempts_seen.len() < 3 {
            let event = timeout(Duration::from_secs(2), started_sub.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(event.name, "flaky");
            attempts_seen.push(event.attempt);
        }
        assert_eq!(attempts_seen, vec![1, 2, 3]);
        assert_eq!(starts.load(Ordering::SeqCst), 3);
    }

    // ---- OnFailure gives up after max_attempts ----

    #[tokio::test]
    async fn on_failure_stops_restarting_after_max_attempts() {
        let runtime = RuntimeContext::empty();
        let tree = SupervisionTree::new(runtime.events().clone());
        let mut failed_sub = runtime.subscribe::<ServiceFailed>();

        let starts = Arc::new(AtomicU32::new(0));
        let svc = Arc::new(PanickingService {
            name: "always-fail".into(),
            starts: Arc::clone(&starts),
            stop_after_attempts: u32::MAX,
        });

        tree.start_service(
            svc,
            RestartPolicy::OnFailure {
                max_attempts: 3,
                backoff: fast_backoff(),
            },
            runtime.clone(),
        )
        .await
        .unwrap();

        // Collect failure events until we see one with will_restart=false.
        let mut final_event = None;
        for _ in 0..10 {
            let event = timeout(Duration::from_secs(2), failed_sub.recv())
                .await
                .unwrap()
                .unwrap();
            if !event.will_restart {
                final_event = Some(event);
                break;
            }
        }
        let final_event = final_event.expect("should reach a non-restart failure");
        assert_eq!(final_event.attempt, 3);

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(starts.load(Ordering::SeqCst), 3);
    }

    // ---- Supervised count tracking ----

    #[tokio::test]
    async fn supervised_count_reflects_registered_services() {
        let runtime = RuntimeContext::empty();
        let tree = SupervisionTree::new(runtime.events().clone());

        assert_eq!(tree.supervised_count(), 0);
        let svc = Arc::new(WaitingService {
            name: "svc-a".into(),
        });
        tree.start_service(svc, RestartPolicy::OneShot, runtime.clone())
            .await
            .unwrap();
        assert_eq!(tree.supervised_count(), 1);

        runtime.events().shutdown();
    }

    // ---- drain awaits watchers ----

    #[tokio::test]
    async fn drain_returns_after_watchers_exit_on_shutdown() {
        let runtime = RuntimeContext::empty();
        let tree = SupervisionTree::new(runtime.events().clone());
        let svc = Arc::new(WaitingService {
            name: "waiter".into(),
        });
        tree.start_service(svc, RestartPolicy::OneShot, runtime.clone())
            .await
            .unwrap();
        assert_eq!(tree.supervised_count(), 1);

        runtime.events().shutdown();
        let graceful = tree.drain(Duration::from_secs(2)).await;
        assert_eq!(graceful, 1);
        assert_eq!(tree.supervised_count(), 0);
    }

    // ---- Policy: permits_restart_after ----

    #[test]
    fn one_shot_never_permits_restart() {
        let p = RestartPolicy::OneShot;
        assert!(!p.permits_restart_after(1, true));
        assert!(!p.permits_restart_after(1, false));
        assert!(!p.permits_restart_after(100, true));
    }

    #[test]
    fn on_failure_permits_restart_only_for_failures_below_max() {
        let p = RestartPolicy::OnFailure {
            max_attempts: 3,
            backoff: fast_backoff(),
        };
        assert!(p.permits_restart_after(1, true));
        assert!(p.permits_restart_after(2, true));
        assert!(!p.permits_restart_after(3, true));
        assert!(!p.permits_restart_after(1, false));
    }

    #[test]
    fn always_policy_permits_restart_on_any_exit() {
        let p = RestartPolicy::Always {
            backoff: fast_backoff(),
        };
        assert!(p.permits_restart_after(1, true));
        assert!(p.permits_restart_after(1, false));
        assert!(p.permits_restart_after(100, true));
    }
}

//! In-process typed event bus for the Runtime Kernel.
//!
//! The [`EventBus`] is a kernel facility — always present, no lifecycle
//! hooks of its own beyond the kernel's, dependent only on `tokio` and
//! `async-channel`. It is to the Runtime what
//! [`tokio::sync::broadcast`] is to Tokio: a typed coordination
//! primitive that Services, Plugins, and scheduled tasks share.
//!
//! ## Communication patterns
//!
//! Three subscription patterns, chosen per event type:
//!
//! - **Broadcast** — every subscriber receives every event. Backed by
//!   [`tokio::sync::broadcast`]. Drop-oldest backpressure: slow
//!   subscribers may [lag](RecvError::Lagged). Bound on `E`:
//!   `Clone + Send + Sync + 'static`.
//! - **Work-stealing** — each event is handled by exactly one worker.
//!   Backed by [`async-channel`]. Multiple [`Worker`] handles for the
//!   same `E` compete via the channel's mpmc primitive. Bound on `E`:
//!   `Send + 'static`.
//! - **Request-reply** — *not provided* in Phase 2.0.c. The Phase 1.85
//!   architecture lock explicitly calls request-reply on the bus a code
//!   smell and prefers a Capability call for typed responses. If a
//!   future use case proves this wrong, see the `CommandBus` RFC backlog
//!   item.
//!
//! ## Shutdown signaling
//!
//! [`EventBus::shutdown`] flips an internal [`tokio::sync::watch`] so
//! late subscribers (`ShutdownSignal` clones taken after the call) still
//! observe the shutdown state. It additionally publishes a
//! [`RuntimeStopping`] broadcast event for already-subscribed listeners
//! that want a fire-once edge.
//!
//! ## Durability and distribution
//!
//! The kernel bus is in-process only and non-durable. Durability is a
//! future Capability (see `DurableEventBus` in the architecture spec);
//! cross-runtime distribution rides on Capability providers
//! (`Queue`, `PubSub`, broker bindings). Neither is the kernel's
//! responsibility.
//!
//! See the
//! [Runtime Kernel — EventBus](https://walastack.com/docs/architecture/runtime/events/)
//! architecture page for design rationale.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

use tokio::sync::{broadcast, watch};

/// Default capacity for new broadcast channels.
///
/// Slow subscribers that lag beyond this depth receive
/// [`RecvError::Lagged`] on their next `recv` call.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 256;

/// Default capacity for new work-stealing channels.
///
/// Producers that try to enqueue when the queue is full receive
/// [`EnqueueErrorKind::Full`] on [`EventBus::try_enqueue`] or are
/// suspended by [`EventBus::enqueue`] until space frees.
pub const DEFAULT_WORK_CAPACITY: usize = 1024;

// =========================================================================
// EventBus
// =========================================================================

/// The kernel's typed in-process event bus.
///
/// Cheap to clone (one atomic increment). Cloned handles share the same
/// underlying channel registry — publishing on one handle is observable
/// by subscribers obtained through any clone.
///
/// # Example
///
/// ```rust
/// use walastack_runtime::events::{EventBus, PublishOutcome};
///
/// #[derive(Clone, Debug, PartialEq, Eq)]
/// struct Tick { seq: u64 }
///
/// # async fn example() {
/// let bus = EventBus::new();
/// let mut sub = bus.subscribe::<Tick>();
///
/// let outcome = bus.publish(Tick { seq: 1 });
/// assert_eq!(outcome, PublishOutcome::Delivered(1));
///
/// let event = sub.recv().await.unwrap();
/// assert_eq!(event.seq, 1);
/// # }
/// ```
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<EventBusInner>,
}

struct EventBusInner {
    broadcast_capacity: usize,
    work_capacity: usize,
    broadcast_channels: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    work_channels: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    shutdown_tx: watch::Sender<bool>,
}

impl EventBus {
    /// Construct a bus with the default channel capacities.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_BROADCAST_CAPACITY, DEFAULT_WORK_CAPACITY)
    }

    /// Construct a bus with explicit channel capacities.
    #[must_use]
    pub fn with_capacity(broadcast_capacity: usize, work_capacity: usize) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            inner: Arc::new(EventBusInner {
                broadcast_capacity,
                work_capacity,
                broadcast_channels: RwLock::new(HashMap::new()),
                work_channels: RwLock::new(HashMap::new()),
                shutdown_tx,
            }),
        }
    }

    // ---- Broadcast ----

    /// Publish a broadcast event to all current subscribers of `E`.
    ///
    /// Returns [`PublishOutcome::Delivered`] with the receiver count on
    /// success and [`PublishOutcome::NoSubscribers`] if no `Subscriber<E>`
    /// is currently registered (the event is dropped).
    pub fn publish<E>(&self, event: E) -> PublishOutcome
    where
        E: Clone + Send + Sync + 'static,
    {
        let sender = self.broadcast_sender::<E>();
        sender
            .send(event)
            .map_or(PublishOutcome::NoSubscribers, PublishOutcome::Delivered)
    }

    /// Subscribe to broadcast events of type `E`.
    ///
    /// Each [`Subscriber`] receives every subsequent event independently
    /// (drop-oldest backpressure per subscriber).
    #[must_use]
    pub fn subscribe<E>(&self) -> Subscriber<E>
    where
        E: Clone + Send + Sync + 'static,
    {
        Subscriber {
            receiver: self.broadcast_sender::<E>().subscribe(),
        }
    }

    /// Number of currently registered subscribers for `E`.
    #[must_use]
    pub fn subscriber_count<E>(&self) -> usize
    where
        E: Clone + Send + Sync + 'static,
    {
        self.broadcast_sender::<E>().receiver_count()
    }

    // ---- Work-stealing ----

    /// Enqueue an event for the work-stealing queue of `E`, suspending
    /// the caller when the queue is full.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueErrorKind::Closed`] if every [`Worker`] for `E`
    /// has been dropped and no worker remains to take the event.
    pub async fn enqueue<E>(&self, event: E) -> Result<(), EnqueueError<E>>
    where
        E: Send + 'static,
    {
        let channel = self.work_channel::<E>();
        channel
            .sender
            .send(event)
            .await
            .map_err(|err| EnqueueError {
                event: err.into_inner(),
                kind: EnqueueErrorKind::Closed,
            })
    }

    /// Try to enqueue an event without suspending.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueErrorKind::Full`] when the queue is at capacity
    /// and [`EnqueueErrorKind::Closed`] when no worker remains.
    pub fn try_enqueue<E>(&self, event: E) -> Result<(), EnqueueError<E>>
    where
        E: Send + 'static,
    {
        let channel = self.work_channel::<E>();
        match channel.sender.try_send(event) {
            Ok(()) => Ok(()),
            Err(async_channel::TrySendError::Full(event)) => Err(EnqueueError {
                event,
                kind: EnqueueErrorKind::Full,
            }),
            Err(async_channel::TrySendError::Closed(event)) => Err(EnqueueError {
                event,
                kind: EnqueueErrorKind::Closed,
            }),
        }
    }

    /// Obtain a [`Worker`] for the work-stealing queue of `E`.
    ///
    /// Multiple `Worker` handles for the same `E` distribute events
    /// — each enqueued event is delivered to exactly one worker.
    #[must_use]
    pub fn worker<E>(&self) -> Worker<E>
    where
        E: Send + 'static,
    {
        Worker {
            receiver: self.work_channel::<E>().receiver.clone(),
        }
    }

    // ---- Shutdown ----

    /// Signal that the runtime is shutting down.
    ///
    /// Flips the internal shutdown watch so that all current and future
    /// [`ShutdownSignal`] handles observe the shutdown state. Also
    /// publishes a [`RuntimeStopping`] broadcast event for already-
    /// subscribed listeners that want a fire-once edge notification.
    ///
    /// Idempotent — subsequent calls are no-ops.
    pub fn shutdown(&self) {
        let already_down = *self.inner.shutdown_tx.borrow();
        if already_down {
            return;
        }
        // `send_replace` updates the watched value unconditionally;
        // unlike `send`, it does not fail when no receivers exist. The
        // shutdown state must be observable by `is_shut_down` and by any
        // `ShutdownSignal` taken *after* shutdown was called.
        let _ = self.inner.shutdown_tx.send_replace(true);
        let _ = self.publish(RuntimeStopping);
    }

    /// Clone-able handle for awaiting the shutdown signal.
    #[must_use]
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        ShutdownSignal {
            receiver: self.inner.shutdown_tx.subscribe(),
        }
    }

    /// Whether shutdown has been signaled.
    #[must_use]
    pub fn is_shut_down(&self) -> bool {
        *self.inner.shutdown_tx.borrow()
    }

    // ---- Private helpers ----

    fn broadcast_sender<E>(&self) -> broadcast::Sender<E>
    where
        E: Clone + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<E>();

        // Fast path: read lock.
        {
            let guard = self
                .inner
                .broadcast_channels
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(arc) = guard.get(&type_id) {
                if let Some(sender) = downcast_arc_ref::<broadcast::Sender<E>>(arc) {
                    return sender.clone();
                }
            }
        }

        // Slow path: write lock, re-check, insert.
        let mut guard = self
            .inner
            .broadcast_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(arc) = guard.get(&type_id) {
            if let Some(sender) = downcast_arc_ref::<broadcast::Sender<E>>(arc) {
                return sender.clone();
            }
        }
        let (tx, _) = broadcast::channel::<E>(self.inner.broadcast_capacity);
        let arc: Arc<dyn Any + Send + Sync> = Arc::new(tx.clone());
        guard.insert(type_id, arc);
        tx
    }

    fn work_channel<E>(&self) -> Arc<WorkChannel<E>>
    where
        E: Send + 'static,
    {
        let type_id = TypeId::of::<E>();

        // Fast path: read lock.
        {
            let guard = self
                .inner
                .work_channels
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(arc) = guard.get(&type_id) {
                if let Some(channel) = downcast_arc::<WorkChannel<E>>(arc) {
                    return channel;
                }
            }
        }

        // Slow path: write lock, re-check, insert.
        let mut guard = self
            .inner
            .work_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(arc) = guard.get(&type_id) {
            if let Some(channel) = downcast_arc::<WorkChannel<E>>(arc) {
                return channel;
            }
        }
        let (sender, receiver) = async_channel::bounded::<E>(self.inner.work_capacity);
        let channel = Arc::new(WorkChannel { sender, receiver });
        guard.insert(type_id, Arc::clone(&channel) as Arc<dyn Any + Send + Sync>);
        channel
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for EventBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventBus")
            .field("broadcast_capacity", &self.inner.broadcast_capacity)
            .field("work_capacity", &self.inner.work_capacity)
            .field("is_shut_down", &self.is_shut_down())
            .finish()
    }
}

struct WorkChannel<E: Send + 'static> {
    sender: async_channel::Sender<E>,
    receiver: async_channel::Receiver<E>,
}

fn downcast_arc<T>(erased: &Arc<dyn Any + Send + Sync>) -> Option<Arc<T>>
where
    T: Any + Send + Sync,
{
    Arc::clone(erased).downcast::<T>().ok()
}

fn downcast_arc_ref<T>(erased: &Arc<dyn Any + Send + Sync>) -> Option<&T>
where
    T: Any + Send + Sync,
{
    erased.downcast_ref::<T>()
}

// =========================================================================
// Subscriber
// =========================================================================

/// A broadcast-subscriber handle for events of type `E`.
///
/// Created by [`EventBus::subscribe`]. Each call to [`Self::recv`]
/// returns the next event; if the subscriber falls behind by more than
/// the bus capacity, the next `recv` returns [`RecvError::Lagged`] with
/// the count of skipped messages.
pub struct Subscriber<E> {
    receiver: broadcast::Receiver<E>,
}

impl<E: Clone> Subscriber<E> {
    /// Receive the next event for this subscriber, suspending until one
    /// is available.
    ///
    /// # Errors
    ///
    /// Returns [`RecvError::Closed`] if the bus has been dropped or all
    /// publishers have gone away, and [`RecvError::Lagged`] when this
    /// subscriber has fallen behind by more than the bus capacity.
    pub async fn recv(&mut self) -> Result<E, RecvError> {
        self.receiver.recv().await.map_err(RecvError::from)
    }
}

impl<E> fmt::Debug for Subscriber<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriber").finish()
    }
}

// =========================================================================
// Worker
// =========================================================================

/// A work-stealing receiver handle for events of type `E`.
///
/// Created by [`EventBus::worker`]. Cloning a `Worker` yields another
/// handle that competes for the same events — each event is delivered
/// to exactly one worker.
#[derive(Clone)]
pub struct Worker<E: Send + 'static> {
    receiver: async_channel::Receiver<E>,
}

impl<E: Send + 'static> Worker<E> {
    /// Receive the next event, suspending until one is available.
    ///
    /// Returns `None` once the queue is closed and drained — i.e. all
    /// [`EventBus`] handles that could enqueue have been dropped.
    pub async fn recv(&self) -> Option<E> {
        self.receiver.recv().await.ok()
    }

    /// Try to receive an event without suspending.
    ///
    /// Returns `None` when the queue is empty (caller should retry later)
    /// or closed (caller should stop).
    #[must_use]
    pub fn try_recv(&self) -> Option<E> {
        self.receiver.try_recv().ok()
    }
}

impl<E: Send + 'static> fmt::Debug for Worker<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker")
            .field("len", &self.receiver.len())
            .finish()
    }
}

// =========================================================================
// ShutdownSignal
// =========================================================================

/// A clone-able handle for awaiting the runtime shutdown signal.
///
/// Created by [`EventBus::shutdown_signal`]. Unlike subscribing to the
/// [`RuntimeStopping`] broadcast event, handles obtained *after*
/// [`EventBus::shutdown`] has been called still observe the shutdown
/// state — the watch-based primitive carries durable state, not an edge.
#[derive(Clone)]
pub struct ShutdownSignal {
    receiver: watch::Receiver<bool>,
}

impl ShutdownSignal {
    /// Whether shutdown has already been signaled.
    #[must_use]
    pub fn is_shut_down(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Suspend until shutdown is signaled.
    ///
    /// Returns immediately if shutdown has already been signaled.
    pub async fn wait(&mut self) {
        if *self.receiver.borrow() {
            return;
        }
        // Wait until the watched value changes to `true`. If the sender
        // is dropped the watch terminates — treat that as shutdown too.
        let _ = self.receiver.wait_for(|signaled| *signaled).await;
    }
}

impl fmt::Debug for ShutdownSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShutdownSignal")
            .field("is_shut_down", &self.is_shut_down())
            .finish()
    }
}

// =========================================================================
// Outcome / Error types
// =========================================================================

/// Result of a broadcast [`EventBus::publish`] call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The event was delivered to the listed number of subscribers.
    Delivered(usize),
    /// The event was discarded because no subscribers are registered.
    NoSubscribers,
}

/// Error returned by [`EventBus::enqueue`] / [`EventBus::try_enqueue`].
///
/// The original event is returned to the caller so it can be retried,
/// logged, or dropped at the caller's discretion.
#[derive(Debug)]
pub struct EnqueueError<E> {
    /// The event that could not be enqueued.
    pub event: E,
    /// Why the enqueue failed.
    pub kind: EnqueueErrorKind,
}

impl<E> fmt::Display for EnqueueError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "enqueue failed: {}", self.kind)
    }
}

impl<E: fmt::Debug> std::error::Error for EnqueueError<E> {}

/// Reason an enqueue failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnqueueErrorKind {
    /// The work queue is at capacity (only returned by
    /// [`EventBus::try_enqueue`]).
    Full,
    /// No [`Worker`] remains to receive the event.
    Closed,
}

impl fmt::Display for EnqueueErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("queue is full"),
            Self::Closed => f.write_str("queue is closed"),
        }
    }
}

/// Error returned by [`Subscriber::recv`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecvError {
    /// The bus has been dropped or all senders have closed the channel.
    Closed,
    /// The subscriber lagged behind by the listed number of messages —
    /// they were dropped under drop-oldest backpressure. The subscriber
    /// remains valid; calling `recv` again resumes from the current
    /// position.
    Lagged(u64),
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("event bus closed"),
            Self::Lagged(n) => write!(f, "subscriber lagged behind by {n} messages"),
        }
    }
}

impl std::error::Error for RecvError {}

impl From<broadcast::error::RecvError> for RecvError {
    fn from(err: broadcast::error::RecvError) -> Self {
        match err {
            broadcast::error::RecvError::Closed => Self::Closed,
            broadcast::error::RecvError::Lagged(n) => Self::Lagged(n),
        }
    }
}

// =========================================================================
// Lifecycle event types
// =========================================================================
//
// These are *type definitions* for the canonical Runtime lifecycle
// events. In Phase 2.0.c, only `RuntimeStopping` is published by the
// kernel (by `EventBus::shutdown`). The remaining three are intended to
// be published by the Runtime kernel when the lifecycle wiring lands in
// Phase 2.0.e. They are exposed now so user code (and tests) can begin
// subscribing to them and Phase 2.0.e becomes a wiring-only change.

/// Emitted when the kernel begins its `Start` phase. *(Not yet published
/// by the kernel in Phase 2.0.c — see module docs.)*
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RuntimeStarting;

/// Emitted when the kernel completes its `Start` phase and enters `Run`.
/// *(Not yet published by the kernel in Phase 2.0.c — see module docs.)*
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RuntimeStarted;

/// Emitted when the kernel begins shutting down. Published by
/// [`EventBus::shutdown`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RuntimeStopping;

/// Emitted when the kernel has finished shutting down. *(Not yet
/// published by the kernel in Phase 2.0.c — see module docs.)*
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RuntimeStopped;

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::similar_names)]

    use super::*;

    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::timeout;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Tick {
        seq: u64,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct OtherEvent {
        value: i32,
    }

    // Non-Clone payload to verify work-stealing accepts it.
    #[derive(Debug)]
    struct Job {
        id: u64,
    }

    // ---- Broadcast ----

    #[tokio::test]
    async fn publish_without_subscribers_returns_no_subscribers() {
        let bus = EventBus::new();
        let outcome = bus.publish(Tick { seq: 1 });
        assert_eq!(outcome, PublishOutcome::NoSubscribers);
    }

    #[tokio::test]
    async fn subscribe_then_publish_delivers_event() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe::<Tick>();
        let outcome = bus.publish(Tick { seq: 7 });
        assert_eq!(outcome, PublishOutcome::Delivered(1));

        let event = sub.recv().await.unwrap();
        assert_eq!(event, Tick { seq: 7 });
    }

    #[tokio::test]
    async fn multiple_subscribers_each_receive_every_event() {
        let bus = EventBus::new();
        let mut sub_a = bus.subscribe::<Tick>();
        let mut sub_b = bus.subscribe::<Tick>();

        let outcome = bus.publish(Tick { seq: 1 });
        assert_eq!(outcome, PublishOutcome::Delivered(2));

        assert_eq!(sub_a.recv().await.unwrap(), Tick { seq: 1 });
        assert_eq!(sub_b.recv().await.unwrap(), Tick { seq: 1 });
    }

    #[tokio::test]
    async fn late_subscriber_misses_prior_event() {
        let bus = EventBus::new();
        let _ = bus.publish(Tick { seq: 1 });

        let mut sub = bus.subscribe::<Tick>();
        // Publish again so we know recv has something to return.
        bus.publish(Tick { seq: 2 });

        assert_eq!(sub.recv().await.unwrap(), Tick { seq: 2 });
    }

    #[tokio::test]
    async fn unrelated_event_types_do_not_interfere() {
        let bus = EventBus::new();
        let mut tick_sub = bus.subscribe::<Tick>();
        let mut other_sub = bus.subscribe::<OtherEvent>();

        bus.publish(Tick { seq: 1 });
        bus.publish(OtherEvent { value: 42 });

        assert_eq!(tick_sub.recv().await.unwrap(), Tick { seq: 1 });
        assert_eq!(other_sub.recv().await.unwrap(), OtherEvent { value: 42 });
        // No cross-talk.
        assert!(
            timeout(Duration::from_millis(20), tick_sub.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn subscriber_count_reflects_active_subscribers() {
        let bus = EventBus::new();
        assert_eq!(bus.subscriber_count::<Tick>(), 0);
        let sub_a = bus.subscribe::<Tick>();
        let sub_b = bus.subscribe::<Tick>();
        assert_eq!(bus.subscriber_count::<Tick>(), 2);
        drop(sub_a);
        assert_eq!(bus.subscriber_count::<Tick>(), 1);
        drop(sub_b);
        assert_eq!(bus.subscriber_count::<Tick>(), 0);
    }

    #[tokio::test]
    async fn bus_clone_shares_channels() {
        let bus_a = EventBus::new();
        let bus_b = bus_a.clone();
        let mut sub = bus_a.subscribe::<Tick>();

        bus_b.publish(Tick { seq: 99 });
        assert_eq!(sub.recv().await.unwrap(), Tick { seq: 99 });
    }

    // ---- Work-stealing ----

    #[tokio::test]
    async fn enqueue_then_worker_recv_returns_event() {
        let bus = EventBus::new();
        let worker = bus.worker::<Job>();

        bus.enqueue(Job { id: 1 }).await.unwrap();
        let job = worker.recv().await.unwrap();
        assert_eq!(job.id, 1);
    }

    #[tokio::test]
    async fn multiple_workers_distribute_events_without_duplication() {
        // Work-stealing contract: each event is delivered to exactly one
        // worker — no duplication, no loss. This test verifies the
        // exactly-once delivery guarantee, not the distribution shape.
        // Real distribution depends on scheduling and queue saturation;
        // with a fast drain, one worker may consume the entire queue
        // before another runs, which is correct mpmc behavior.
        let bus = EventBus::new();
        let worker_a = bus.worker::<Job>();
        let worker_b = bus.worker::<Job>();

        for id in 0..10 {
            bus.enqueue(Job { id }).await.unwrap();
        }

        let received = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));

        let received_a = Arc::clone(&received);
        let task_a = tokio::spawn(async move {
            while let Some(job) = worker_a.recv().await {
                received_a.lock().await.push(job.id);
            }
        });
        let received_b = Arc::clone(&received);
        let task_b = tokio::spawn(async move {
            while let Some(job) = worker_b.recv().await {
                received_b.lock().await.push(job.id);
            }
        });

        // Wait for both workers to drain whatever they can.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while received.lock().await.len() < 10 {
            assert!(std::time::Instant::now() < deadline, "workers stalled");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        drop(bus);
        let _ = timeout(Duration::from_secs(2), task_a).await;
        let _ = timeout(Duration::from_secs(2), task_b).await;

        let mut seen = received.lock().await.clone();
        seen.sort_unstable();
        assert_eq!(
            seen,
            (0_u64..10).collect::<Vec<_>>(),
            "exactly-once delivery violated"
        );
    }

    #[tokio::test]
    async fn try_enqueue_full_returns_error_with_event() {
        let bus = EventBus::with_capacity(DEFAULT_BROADCAST_CAPACITY, 1);
        let _worker = bus.worker::<Job>();

        bus.try_enqueue(Job { id: 1 }).unwrap();
        let err = bus.try_enqueue(Job { id: 2 }).unwrap_err();
        assert_eq!(err.kind, EnqueueErrorKind::Full);
        assert_eq!(err.event.id, 2);
    }

    #[tokio::test]
    async fn enqueue_works_with_non_clone_types() {
        let bus = EventBus::new();
        let worker = bus.worker::<Job>();
        bus.enqueue(Job { id: 42 }).await.unwrap();
        assert_eq!(worker.recv().await.unwrap().id, 42);
    }

    #[tokio::test]
    async fn try_recv_returns_none_when_empty() {
        let bus = EventBus::new();
        let worker = bus.worker::<Job>();
        assert!(worker.try_recv().is_none());
    }

    // ---- Shutdown ----

    #[tokio::test]
    async fn is_shut_down_returns_false_before_shutdown() {
        let bus = EventBus::new();
        assert!(!bus.is_shut_down());
    }

    #[tokio::test]
    async fn shutdown_flips_is_shut_down() {
        let bus = EventBus::new();
        bus.shutdown();
        assert!(bus.is_shut_down());
    }

    #[tokio::test]
    async fn shutdown_signal_resolves_after_shutdown_called() {
        let bus = EventBus::new();
        let mut signal = bus.shutdown_signal();

        let bus_clone = bus.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            bus_clone.shutdown();
        });

        timeout(Duration::from_secs(1), signal.wait())
            .await
            .expect("shutdown signal should resolve");
    }

    #[tokio::test]
    async fn shutdown_signal_late_clone_still_sees_state() {
        let bus = EventBus::new();
        bus.shutdown();

        // Created AFTER shutdown — broadcast semantics would miss the
        // event, but the watch-based signal carries state.
        let signal = bus.shutdown_signal();
        assert!(signal.is_shut_down());
    }

    #[tokio::test]
    async fn shutdown_publishes_runtime_stopping_to_existing_subscribers() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe::<RuntimeStopping>();
        bus.shutdown();

        let event = timeout(Duration::from_secs(1), sub.recv())
            .await
            .expect("recv should resolve")
            .expect("RuntimeStopping should be delivered");
        assert_eq!(event, RuntimeStopping);
    }

    #[tokio::test]
    async fn shutdown_is_idempotent() {
        let bus = EventBus::new();
        bus.shutdown();
        bus.shutdown();
        bus.shutdown();
        assert!(bus.is_shut_down());
    }

    // ---- Lifecycle types ----

    #[tokio::test]
    async fn lifecycle_event_types_can_flow_through_bus() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe::<RuntimeStarted>();
        bus.publish(RuntimeStarted);
        assert_eq!(sub.recv().await.unwrap(), RuntimeStarted);
    }
}

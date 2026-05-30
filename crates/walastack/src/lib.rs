//! # walastack
//!
//! Umbrella crate for the WalaStack ecosystem.
//!
//! Re-exports the primary user-facing framework API and provides a
//! [`prelude`] module of common imports. Most applications import from this
//! crate rather than from the individual `walastack-*` crates directly.
//!
//! # Example
//!
//! ```no_run
//! use walastack::prelude::*;
//!
//! async fn index() -> &'static str {
//!     "Hello, WalaStack!"
//! }
//!
//! #[tokio::main]
//! async fn main() -> walastack::Result<()> {
//!     App::new().get("/", index).run("127.0.0.1:3000").await
//! }
//! ```

pub use walastack_app::{App, Cap, CapRejection, Handler, Route};
pub use walastack_http::{
    Body, Bytes, Error, ExtractionRejection, FromRequest, FromRequestParts, HeaderMap, HeaderName,
    HeaderValue, Html, IntoResponse, Json, Method, Path, PathParams, Query, Request, Response,
    Result, StatusCode, Uri, Version, header,
};
pub use walastack_macros::{delete, get, main, post, put};
pub use walastack_router::Router;
pub use walastack_runtime::{
    Backoff, BoxedServiceFuture, Capabilities, CapabilityName, CapabilityRegistry,
    CapabilityRequirement, CronSchedule, DEFAULT_BROADCAST_CAPACITY, DEFAULT_NAME,
    DEFAULT_SHUTDOWN_DEADLINE, DEFAULT_WORK_CAPACITY, EnqueueError, EnqueueErrorKind, EventBus,
    Plugin, PluginError, PluginManager, Policies, PublishOutcome, RecvError, ResourceRegistry,
    Resources, RestartPolicy, RetryPolicy, Runtime, RuntimeBuilder, RuntimeContext, RuntimeError,
    RuntimeStarted, RuntimeStarting, RuntimeStopped, RuntimeStopping, ScheduleHandle, ScheduleId,
    ScheduledFn, Scheduler, SchedulerError, SelectionStrategy, Service, ServiceContext,
    ServiceError, ServiceFailed, ServicePlanner, ServiceStarted, ServiceStopped, ShutdownSignal,
    Subscriber, SupervisionTree, TaskCompleted, TaskError, TaskFailed, TaskFired, TaskResult,
    TaskRetrying, Trigger, Worker, init_tracing, wait_for_shutdown_signal,
};

/// Internal re-exports used by procedural macros. Not part of the public API.
#[doc(hidden)]
pub mod __macro_support {
    pub use tokio;
}

/// Common imports for WalaStack applications.
///
/// Glob-import to bring the canonical framework types into scope:
///
/// ```rust
/// use walastack::prelude::*;
/// ```
pub mod prelude {
    pub use crate::{
        App, Body, Bytes, Cap, FromRequest, FromRequestParts, Handler, HeaderMap, Html,
        IntoResponse, Json, Method, Path, Query, Request, Response, Result, Route, StatusCode, Uri,
        delete, get, post, put,
    };

    /// Kitchen-sink prelude that re-exports every ecosystem crate's
    /// `prelude` module under `walastack::prelude::full`. Available
    /// when the `full` feature is enabled.
    ///
    /// ```ignore
    /// // In Cargo.toml:
    /// // walastack = { version = "...", features = ["full"] }
    ///
    /// use walastack::prelude::*;
    /// use walastack::prelude::full::*;
    ///
    /// // Now Auth / AuthPlugin / SqlitePlugin / OpenApiPlugin / JobsPlugin
    /// // and friends are all in scope.
    /// ```
    ///
    /// Each ecosystem crate's prelude curates its own re-exports — the
    /// `full` module just bundles them. See:
    /// - [`walastack_auth::prelude`]
    /// - [`walastack_db::prelude`]
    /// - [`walastack_jobs::prelude`]
    /// - [`walastack_llm::prelude`]
    /// - [`walastack_openapi::prelude`]
    #[cfg(feature = "full")]
    pub mod full {
        pub use walastack_auth::prelude::*;
        pub use walastack_db::prelude::*;
        pub use walastack_jobs::prelude::*;
        pub use walastack_llm::prelude::*;
        pub use walastack_openapi::prelude::*;
    }
}

#[cfg(test)]
mod tests {
    /// Smoke test: the umbrella crate compiles and the prelude module exists.
    /// Once `prelude` exports real items, this test will exercise them.
    #[test]
    const fn smoke() {}
}

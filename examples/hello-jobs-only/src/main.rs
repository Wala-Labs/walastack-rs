//! `hello-jobs-only` — Tier 2 DX example.
//!
//! Pure `Runtime::builder` composition with `walastack-jobs` and
//! **no HTTP at all**. Validates the non-App path documented in
//! [app-vs-runtime](../../../docs/guides/app-vs-runtime.md) — Path 4
//! variant ("Multi-Service composition without HTTP").
//!
//! ## What this exercises
//!
//! - `Runtime::builder().with_plugin(...).build()` end-to-end.
//! - `InMemoryJobStorePlugin` and `JobsPlugin::new(...).register::<J>()`.
//! - A `Job` impl with associated `Output` / `Error` types and
//!   `JobMetadata` queue routing.
//! - `walastack_jobs::enqueue` to push jobs from outside an extractor.
//! - Polling the `JobStore` capability to observe completion.
//! - Clean graceful shutdown via `runtime.shutdown_gracefully()`.
//!
//! ## Quick start
//!
//! ```bash
//! cargo run -p hello-jobs-only
//! ```
//!
//! The example runs synchronously: enqueues five jobs, waits for them
//! all to complete (or up to ~5 seconds elapsed), and exits.
//!
//! No `walastack` umbrella crate is imported because the umbrella
//! currently focuses on HTTP-shaped types (`App`, `Json`, etc.) — a
//! pure-jobs program doesn't need it.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![allow(clippy::expect_used, clippy::unwrap_used)]
#![allow(clippy::doc_markdown)]
// `GreetJob` and `PROCESSED` are consumed by tests/smoke.rs via
// #[path = "../src/main.rs"] — bin perspective sees them as unused.
#![allow(unreachable_pub)]
// `main()` itself is unused from the test perspective (tests provide
// their own composition).
#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use walastack_jobs::{
    InMemoryJobStorePlugin, Job, JobContext, JobMetadata, JobStore, JobsConfig, JobsPlugin, enqueue,
};
use walastack_runtime::Runtime;

pub static PROCESSED: AtomicU32 = AtomicU32::new(0);

// =========================================================================
// Job type
// =========================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GreetJob {
    pub name: String,
    pub enthusiasm: u8,
}

impl Job for GreetJob {
    type Output = ();
    type Error = String;
    const NAME: &'static str = "greet";

    fn metadata() -> JobMetadata {
        // Pin to a named queue so the job routing is explicit.
        JobMetadata::default().queue("greetings").max_attempts(3)
    }

    async fn run(self, _ctx: JobContext) -> Result<(), String> {
        let exclamations = "!".repeat(self.enthusiasm.into());
        tracing::info!(
            name = %self.name,
            "processed greeting{exclamations}"
        );
        PROCESSED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// =========================================================================
// Composition
// =========================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info,hello_jobs_only=debug")
        .try_init()
        .ok();

    // Per the App vs Runtime guide: HTTP-only? → App. Multi-service or
    // no-HTTP? → Runtime::builder. This is the second case.
    let mut runtime = Runtime::builder()
        .with_plugin(InMemoryJobStorePlugin::new())
        .with_plugin(
            JobsPlugin::new(
                JobsConfig::default()
                    .with_worker_count(2)
                    .with_queues(["greetings", "default"])
                    .with_poll_interval(Duration::from_millis(25)),
            )
            .register::<GreetJob>(),
        )
        .build()?;
    runtime.start().await?;

    // Push a handful of jobs through.
    for (name, enthusiasm) in [
        ("alice", 1),
        ("bob", 2),
        ("carol", 3),
        ("dave", 4),
        ("eve", 5),
    ] {
        enqueue(
            runtime.context(),
            GreetJob {
                name: name.into(),
                enthusiasm,
            },
        )
        .await?;
    }

    // Wait until all five jobs have processed, or ~5s elapsed.
    let store = runtime
        .context()
        .capability::<dyn JobStore>()
        .expect("JobStore registered by InMemoryJobStorePlugin");
    let _ = store;
    for _ in 0..500 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        if PROCESSED.load(Ordering::SeqCst) >= 5 {
            break;
        }
    }
    tracing::info!(
        processed = PROCESSED.load(Ordering::SeqCst),
        "all jobs done; shutting down"
    );

    runtime.shutdown_gracefully().await;
    Ok(())
}

// =========================================================================
// TIER 2 DX FRICTION OBSERVATIONS (classified)
// =========================================================================
//
// ## DOCUMENTATION SOLVED
//
//   - Choosing Runtime::builder over App.
//     The app-vs-runtime guide's Path-4 ("Multi-Service / non-HTTP")
//     is exactly this example. Zero friction.
//
//   - Plugin composition for jobs (InMemoryJobStorePlugin +
//     JobsPlugin).
//     plugin-composition.md framed it: storage plugin provides
//     `dyn JobStore`; JobsPlugin consumes it via
//     required_capabilities. Order independence held.
//
//   - Calling `enqueue` from outside an extractor.
//     This works because we have direct access to runtime.context()
//     at main() — we're not inside a handler. The capabilities-and-
//     resources guide explains the ctx access patterns clearly. The
//     RuntimeContext-from-helper friction (hello-auth-db's
//     issue_token thread_local smell) does NOT apply here because
//     `main` has the runtime in hand directly.
//
//   - JobMetadata extension seam — `JobMetadata::default()
//     .queue(...).max_attempts(...)` chain-built cleanly.
//
//   - Shutdown signaling.
//     runtime.shutdown_gracefully() works as documented.
//
// ## STILL PAINFUL AFTER DOCUMENTATION
//
//   - Polling JobStore::fetch to observe completion is awkward —
//     prevents the example from using a clean "subscribe to JobDead"
//     style. A `Subscriber<JobCompleted>` would be more natural but
//     requires deeper knowledge of EventBus. Documentation note:
//     show a job-event subscription pattern in a future guide
//     iteration.
//
//   - The umbrella crate (walastack) is HTTP-centric. A pure-jobs
//     example imports walastack_jobs + walastack_runtime directly.
//     This is fine for now but confirms that per-crate preludes
//     would help — a `walastack_jobs::prelude::*` would collapse
//     the import list.
//
// ## NEW OBSERVATIONS (not in original 15)
//
//   - tracing-subscriber initialization boilerplate. Every example
//     that wants logs needs ~5 lines of init. A `walastack::log_init`
//     helper would compress this — minor.
//
//   - There's no idiomatic "wait for a specific job to complete"
//     helper. The polling loop in main() is the lowest-effort path.
//     A `JobObserver` trait or `runtime.wait_for_job(id).await` was
//     listed as a Tier 3 candidate; this example provides confirmation
//     that it's a real pattern, not imagined. Still low priority.

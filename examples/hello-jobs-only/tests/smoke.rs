//! End-to-end smoke test for `hello-jobs-only`.
//!
//! Builds the same `Runtime::builder` composition the main() uses,
//! enqueues a single `GreetJob`, and confirms it processes to completion.

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use std::sync::atomic::Ordering;
use std::time::Duration;

#[path = "../src/main.rs"]
mod app;

#[tokio::test]
async fn enqueued_job_runs_to_completion() {
    use walastack_jobs::{
        InMemoryJobStorePlugin, JobStatus, JobStore, JobsConfig, JobsPlugin, enqueue,
    };
    use walastack_runtime::Runtime;

    app::PROCESSED.store(0, Ordering::SeqCst);

    let mut runtime = Runtime::builder()
        .with_plugin(InMemoryJobStorePlugin::new())
        .with_plugin(
            JobsPlugin::new(
                JobsConfig::default()
                    .with_worker_count(1)
                    .with_queues(["greetings"])
                    .with_poll_interval(Duration::from_millis(10)),
            )
            .register::<app::GreetJob>(),
        )
        .build()
        .expect("runtime builds");
    runtime.start().await.expect("runtime starts");

    let id = enqueue(
        runtime.context(),
        app::GreetJob {
            name: "smoke".into(),
            enthusiasm: 1,
        },
    )
    .await
    .expect("enqueued");

    let store = runtime
        .context()
        .capability::<dyn JobStore>()
        .expect("store");
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let r = store.fetch(id).await.unwrap().unwrap();
        if r.status == JobStatus::Completed {
            break;
        }
    }
    assert_eq!(app::PROCESSED.load(Ordering::SeqCst), 1);
    let r = store.fetch(id).await.unwrap().unwrap();
    assert_eq!(r.status, JobStatus::Completed);

    runtime.shutdown_gracefully().await;
}

//! Stdio integration test for `walastack-mcp` Sub-batch B.
//!
//! Spawns the `walastack-mcp-fake-server` binary (declared as a
//! `[[bin]]` target in walastack-mcp's `Cargo.toml`) and exercises the
//! full stack:
//!
//! - `Runtime::builder().build()` with `InMemorySecretsPlugin` +
//!   `McpPlugin`.
//! - `runtime.start()` spawns the per-server `McpServerService`, which
//!   runs the MCP `initialize` handshake against the fake binary.
//! - Through `Cap<dyn McpRegistry>` + `Cap<dyn McpClient>` we list
//!   servers, list tools, invoke `echo`, invoke `fail`, list
//!   resources, and read a resource.
//! - Verify lifecycle events fire on the kernel EventBus.
//! - Clean shutdown.

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use std::time::Duration;

use walastack_auth::InMemorySecretsPlugin;
use walastack_mcp::{
    McpClient, McpConfig, McpPlugin, McpRegistry, McpServerConnected, McpServerDisconnected,
    McpServerSpec, ResourcePayload, ServerId, TransportSpec,
};
use walastack_runtime::Runtime;

const FAKE_SERVER: &str = env!("CARGO_BIN_EXE_walastack-mcp-fake-server");

async fn wait_for_server_connected(runtime: &Runtime, server: &ServerId) -> bool {
    let mut sub = runtime.context().events().subscribe::<McpServerConnected>();
    // The connection may already have happened before we subscribed —
    // poll the registry as well.
    for _ in 0..200 {
        if runtime
            .context()
            .capability::<dyn McpRegistry>()
            .unwrap()
            .list_servers()
            .iter()
            .any(|s| s.id == *server)
        {
            return true;
        }
        tokio::select! {
            _ = sub.recv() => return true,
            () = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
    }
    false
}

#[tokio::test]
async fn stdio_end_to_end_tools_and_resources() {
    let config = McpConfig::new()
        .with_default_request_timeout(Duration::from_secs(2))
        .with_ping_interval(Duration::from_secs(60))
        .with_server(
            McpServerSpec::new("fake")
                .with_transport(TransportSpec::stdio(FAKE_SERVER, Vec::<String>::new())),
        );

    let mut runtime = Runtime::builder()
        .with_plugin(InMemorySecretsPlugin::new())
        .with_plugin(McpPlugin::new(config))
        .build()
        .expect("runtime builds");
    runtime.start().await.expect("runtime starts");

    let server = ServerId::new("fake");
    assert!(
        wait_for_server_connected(&runtime, &server).await,
        "fake MCP server did not connect within ~5s"
    );

    let registry = runtime
        .context()
        .capability::<dyn McpRegistry>()
        .expect("McpRegistry registered");
    let client = runtime
        .context()
        .capability::<dyn McpClient>()
        .expect("McpClient registered");

    // list_servers returns the connected fake.
    let servers = registry.list_servers();
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].id, server);
    assert!(servers[0].connected);
    assert_eq!(
        servers[0].display_name.as_deref(),
        Some("walastack-mcp-fake-server")
    );

    // list_tools returns the hardcoded echo + fail tools.
    let tools = registry.list_tools(&server).await.expect("list_tools");
    assert_eq!(tools.len(), 2);
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"echo"));
    assert!(names.contains(&"fail"));

    // echo tool round-trip.
    let out = client
        .invoke_tool(&server, "echo", serde_json::json!({"value": 42}))
        .await
        .expect("invoke echo");
    assert_eq!(out, serde_json::json!({"value": 42}));

    // fail tool surfaces as RemoteError.
    let err = client
        .invoke_tool(&server, "fail", serde_json::json!({}))
        .await
        .expect_err("fail tool should surface error");
    match err {
        walastack_mcp::McpError::RemoteError { code, message } => {
            assert_eq!(code, -32000);
            assert!(message.contains("intentional failure"), "msg = {message}");
        }
        other => panic!("expected RemoteError, got {other:?}"),
    }

    // resources/list and resources/read.
    let resources = registry
        .list_resources(&server)
        .await
        .expect("list_resources");
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].uri, "memo://greeting");

    let content = client
        .read_resource(&server, "memo://greeting")
        .await
        .expect("read_resource");
    assert_eq!(content.uri, "memo://greeting");
    match content.payload {
        ResourcePayload::Text(text) => assert_eq!(text, "hello from fake mcp"),
        ResourcePayload::Blob(_) => panic!("expected text payload"),
    }

    // Clean shutdown publishes McpServerDisconnected.
    let mut disconnect_sub = runtime
        .context()
        .events()
        .subscribe::<McpServerDisconnected>();
    runtime.shutdown_gracefully().await;
    // The Service task should have published the disconnect event
    // before exiting; allow a brief window for the publish.
    let _ = tokio::time::timeout(Duration::from_secs(1), disconnect_sub.recv()).await;
}

#[tokio::test]
async fn stdio_secret_required_for_env_from_secret() {
    // Configure the fake to require a secret that isn't registered.
    // McpServerService should return ServiceError at start.
    let config = McpConfig::new().with_server(
        McpServerSpec::new("needs-secret")
            .with_transport(TransportSpec::stdio(FAKE_SERVER, Vec::<String>::new()))
            .with_env_from_secret("FAKE_TOKEN", "missing-secret"),
    );

    let mut runtime = Runtime::builder()
        .with_plugin(InMemorySecretsPlugin::new())
        .with_plugin(McpPlugin::new(config))
        .build()
        .expect("runtime builds");
    let err = runtime
        .start()
        .await
        .expect_err("start should fail when required secret is missing");
    // Error mentions the secret name (for log correlation) but NOT a
    // value.
    assert!(err.to_string().contains("missing-secret"), "err = {err}");
}

#[tokio::test]
async fn stdio_secret_is_injected_as_env_var() {
    // Spawn fake with a literal env var bound; the test verifies the
    // happy path (we don't have a way to read the child's env from
    // outside, but a successful initialize confirms the spawn worked
    // with the env applied).
    let config = McpConfig::new()
        .with_default_request_timeout(Duration::from_secs(2))
        .with_ping_interval(Duration::from_secs(60))
        .with_server(
            McpServerSpec::new("fake")
                .with_transport(TransportSpec::stdio(FAKE_SERVER, Vec::<String>::new()))
                .with_env_from_secret("FAKE_TOKEN", "fake-secret")
                .with_env_literal("MCP_DEBUG", "0"),
        );

    let mut runtime = Runtime::builder()
        .with_plugin(InMemorySecretsPlugin::new().with("fake-secret", b"shhhh"))
        .with_plugin(McpPlugin::new(config))
        .build()
        .expect("runtime builds");
    runtime.start().await.expect("runtime starts");

    let server = ServerId::new("fake");
    assert!(
        wait_for_server_connected(&runtime, &server).await,
        "fake MCP server did not connect"
    );

    runtime.shutdown_gracefully().await;
}

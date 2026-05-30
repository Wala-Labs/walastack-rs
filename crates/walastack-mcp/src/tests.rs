//! Sub-batch A unit tests.
//!
//! Cover: `ServerId` newtype, error rendering, descriptor
//! serialization shape, `McpConfig` builders, `InMemoryMcpPlugin`
//! capability registration, `McpPlugin` requirement validation, and
//! end-to-end happy-path through the in-memory provider for both
//! `McpRegistry` and `McpClient`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use walastack_auth::InMemorySecretsPlugin;
use walastack_runtime::Runtime;

use crate::{
    EnvVar, InMemoryMcp, InMemoryMcpPlugin, InMemoryServer, McpClient, McpConfig, McpError,
    McpPlugin, McpRegistry, McpServerSpec, ResourceContent, ResourceDescriptor, ResourcePayload,
    ServerId, ToolDescriptor, TransportSpec,
};

// ---- ServerId ----

#[test]
fn server_id_round_trips_string_construction() {
    let from_str = ServerId::from("alpha");
    let from_string = ServerId::from(String::from("alpha"));
    let from_new = ServerId::new("alpha");
    assert_eq!(from_str, from_string);
    assert_eq!(from_string, from_new);
    assert_eq!(from_new.as_str(), "alpha");
    assert_eq!(format!("{from_new}"), "alpha");
}

// ---- McpError display ----

#[test]
fn mcp_error_display_includes_diagnostic_context() {
    let err = McpError::UnknownTool {
        server: ServerId::new("gh"),
        tool: "create_issue".into(),
    };
    let msg = format!("{err}");
    assert!(msg.contains("create_issue"), "msg = {msg}");
    assert!(msg.contains("gh"), "msg = {msg}");
}

// ---- McpConfig + McpServerSpec builders ----

#[test]
fn mcp_server_spec_builder_attaches_env_and_timeout() {
    let spec = McpServerSpec::new("gh")
        .with_transport(TransportSpec::stdio("npx", ["-y", "server-github"]))
        .with_env_from_secret("GITHUB_TOKEN", "github-pat")
        .with_env_literal("MCP_DEBUG", "0")
        .with_request_timeout(Duration::from_secs(45))
        .with_display_name("GitHub MCP");
    assert_eq!(spec.id.as_str(), "gh");
    assert_eq!(spec.env.len(), 2);
    assert_eq!(spec.request_timeout, Some(Duration::from_secs(45)));
    assert_eq!(spec.display_name.as_deref(), Some("GitHub MCP"));
    assert!(matches!(
        spec.env[0],
        EnvVar::FromSecret { ref var, ref secret_name }
            if var == "GITHUB_TOKEN" && secret_name == "github-pat"
    ));
    assert!(matches!(
        spec.env[1],
        EnvVar::Literal { ref var, ref value } if var == "MCP_DEBUG" && value == "0"
    ));
}

#[test]
fn mcp_config_default_request_timeout_is_30s() {
    let config = McpConfig::new();
    assert_eq!(config.default_request_timeout, Duration::from_secs(30));
    assert!(config.servers.is_empty());
}

#[test]
fn mcp_config_with_overrides_chains_correctly() {
    let config = McpConfig::new()
        .with_default_request_timeout(Duration::from_secs(60))
        .with_ping_interval(Duration::from_secs(5))
        .with_server(McpServerSpec::new("alpha"))
        .with_server(McpServerSpec::new("beta"));
    assert_eq!(config.default_request_timeout, Duration::from_secs(60));
    assert_eq!(config.ping_interval, Duration::from_secs(5));
    assert_eq!(config.servers.len(), 2);
    assert_eq!(config.servers[0].id.as_str(), "alpha");
    assert_eq!(config.servers[1].id.as_str(), "beta");
}

// ---- McpPlugin requirements ----

#[tokio::test]
async fn mcp_plugin_requires_secrets_provider() {
    let err = Runtime::builder()
        .with_plugin(McpPlugin::new(McpConfig::new()))
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("SecretsProvider"), "err = {err}");
}

#[tokio::test]
async fn mcp_plugin_satisfied_by_in_memory_secrets() {
    let runtime = Runtime::builder()
        .with_plugin(InMemorySecretsPlugin::new())
        .with_plugin(McpPlugin::new(McpConfig::new()))
        .build();
    assert!(runtime.is_ok());
}

#[tokio::test]
async fn mcp_plugin_registers_mcp_config_as_resource() {
    let runtime = Runtime::builder()
        .with_plugin(InMemorySecretsPlugin::new())
        .with_plugin(McpPlugin::new(
            McpConfig::new().with_server(McpServerSpec::new("alpha")),
        ))
        .build()
        .unwrap();
    let config = runtime.context().resource::<McpConfig>();
    assert!(config.is_some());
    assert_eq!(config.unwrap().servers.len(), 1);
}

// ---- InMemoryMcpPlugin capability registration ----

#[tokio::test]
async fn in_memory_plugin_registers_registry_and_client_capabilities() {
    let runtime = Runtime::builder()
        .with_plugin(InMemoryMcpPlugin::new())
        .build()
        .unwrap();
    let registry = runtime.context().capability::<dyn McpRegistry>();
    let client = runtime.context().capability::<dyn McpClient>();
    assert!(registry.is_some());
    assert!(client.is_some());
}

// ---- End-to-end through InMemoryMcp ----

fn echo_server() -> InMemoryServer {
    InMemoryServer::new("echo")
        .with_display_name("Echo server")
        .with_tool(
            ToolDescriptor {
                name: "echo".into(),
                description: Some("Returns its input unchanged".into()),
                input_schema: None,
                output_schema: None,
            },
            Ok,
        )
        .with_tool(
            ToolDescriptor {
                name: "fail".into(),
                description: None,
                input_schema: None,
                output_schema: None,
            },
            |_args| Err("intentional failure".into()),
        )
        .with_resource(
            ResourceDescriptor {
                uri: "memo://greeting".into(),
                name: Some("greeting".into()),
                description: None,
                mime_type: Some("text/plain".into()),
            },
            ResourceContent {
                uri: "memo://greeting".into(),
                mime_type: Some("text/plain".into()),
                payload: ResourcePayload::Text("hello, walastack".into()),
            },
        )
}

#[tokio::test]
async fn in_memory_provider_lists_servers_tools_and_resources() {
    let mcp = InMemoryMcp::new().with_server(echo_server());
    let servers = mcp.list_servers();
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].id.as_str(), "echo");
    assert!(servers[0].connected);

    let tools = mcp.list_tools(&ServerId::new("echo")).await.unwrap();
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0].name, "echo");

    let resources = mcp.list_resources(&ServerId::new("echo")).await.unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].uri, "memo://greeting");
}

#[tokio::test]
async fn in_memory_provider_invokes_tool_round_trip() {
    let mcp = InMemoryMcp::new().with_server(echo_server());
    let out = mcp
        .invoke_tool(
            &ServerId::new("echo"),
            "echo",
            serde_json::json!({"value": 42}),
        )
        .await
        .unwrap();
    assert_eq!(out, serde_json::json!({"value": 42}));
}

#[tokio::test]
async fn in_memory_provider_propagates_tool_failure_as_remote_error() {
    let mcp = InMemoryMcp::new().with_server(echo_server());
    let err = mcp
        .invoke_tool(&ServerId::new("echo"), "fail", serde_json::json!({}))
        .await
        .unwrap_err();
    match err {
        McpError::RemoteError { code, message } => {
            assert_eq!(code, -32000);
            assert!(message.contains("intentional failure"));
        }
        other => panic!("expected RemoteError, got {other:?}"),
    }
}

#[tokio::test]
async fn in_memory_provider_returns_unknown_server_for_missing_id() {
    let mcp = InMemoryMcp::new().with_server(echo_server());
    let err = mcp
        .list_tools(&ServerId::new("does-not-exist"))
        .await
        .unwrap_err();
    assert!(matches!(err, McpError::UnknownServer(_)), "err = {err}");
}

#[tokio::test]
async fn in_memory_provider_returns_unknown_tool_for_missing_tool_on_real_server() {
    let mcp = InMemoryMcp::new().with_server(echo_server());
    let err = mcp
        .invoke_tool(&ServerId::new("echo"), "nonexistent", serde_json::json!({}))
        .await
        .unwrap_err();
    assert!(matches!(err, McpError::UnknownTool { .. }), "err = {err}");
}

#[tokio::test]
async fn in_memory_provider_reads_resource_content() {
    let mcp = InMemoryMcp::new().with_server(echo_server());
    let content = mcp
        .read_resource(&ServerId::new("echo"), "memo://greeting")
        .await
        .unwrap();
    assert_eq!(content.uri, "memo://greeting");
    match content.payload {
        ResourcePayload::Text(s) => assert_eq!(s, "hello, walastack"),
        ResourcePayload::Blob(_) => panic!("expected text"),
    }
}

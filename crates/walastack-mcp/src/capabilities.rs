//! Capability trait surfaces for MCP integration.
//!
//! Three capabilities form the hybrid tool-model architecture locked
//! for Iteration 1:
//!
//! - [`McpRegistry`] — discovery (list servers, list tools, list
//!   resources). Cheap, idempotent metadata.
//! - [`McpClient`] — invocation (call a tool, read a resource).
//!   Round-trips to the underlying transport.
//! - [`McpTransport`] — the protocol pipe (stdio, future HTTP/SSE,
//!   future WebSocket). Providers implement this and the higher-level
//!   capabilities are built on top.
//!
//! The default in-memory and stdio implementations both satisfy
//! `McpRegistry` and `McpClient` with the **same internal handle** —
//! the split is for **consumer access pattern** (discovery vs
//! invocation), not implementation.

use std::pin::Pin;

use crate::descriptors::{
    ResourceContent, ResourceDescriptor, ServerDescriptor, ServerId, ToolDescriptor,
};
use crate::errors::McpError;

/// Boxed future returned by MCP capability methods.
///
/// Mirrors `walastack_runtime::BoxedServiceFuture` /
/// `walastack_jobs::BoxedJobStoreFuture` — avoids the `async-trait`
/// dep at the cost of one allocation per call.
pub type BoxedMcpFuture<T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'static>>;

/// **Discovery** surface for MCP servers.
///
/// Implementations expose the configured server catalog + per-server
/// tool / resource inventories. Lookups are expected to be cheap;
/// the default stdio-backed implementation caches catalogs across
/// invocations and refreshes on lifecycle events.
pub trait McpRegistry: Send + Sync + 'static {
    /// Enumerate all configured servers (regardless of connection
    /// status; consult [`ServerDescriptor::connected`] to filter).
    fn list_servers(&self) -> Vec<ServerDescriptor>;

    /// List the tools exposed by `server`.
    fn list_tools(
        &self,
        server: &ServerId,
    ) -> BoxedMcpFuture<Result<Vec<ToolDescriptor>, McpError>>;

    /// List the resources exposed by `server`.
    fn list_resources(
        &self,
        server: &ServerId,
    ) -> BoxedMcpFuture<Result<Vec<ResourceDescriptor>, McpError>>;
}

/// **Invocation** surface for MCP servers.
///
/// Splits from [`McpRegistry`] so consumers can hold a narrow handle
/// matching their access pattern — agents wanting only discovery, HTTP
/// shims wanting only invocation, etc.
pub trait McpClient: Send + Sync + 'static {
    /// Invoke `tool` on `server` with the given JSON `arguments`.
    /// Returns the tool's JSON-shaped result on success.
    fn invoke_tool(
        &self,
        server: &ServerId,
        tool: &str,
        arguments: serde_json::Value,
    ) -> BoxedMcpFuture<Result<serde_json::Value, McpError>>;

    /// Read `uri` from `server` and return its contents.
    fn read_resource(
        &self,
        server: &ServerId,
        uri: &str,
    ) -> BoxedMcpFuture<Result<ResourceContent, McpError>>;
}

/// **Transport** capability — the wire protocol pipe to an individual
/// MCP server.
///
/// Iteration 1 ships a `StdioTransportPlugin` (*in Sub-batch B*) as
/// the default provider; future iterations add HTTP/SSE
/// and WebSocket variants. The capability split lets each transport
/// land as a focused plugin without touching the higher-level Registry
/// / Client surface.
///
/// Most application code never names this trait directly — it's a
/// substrate the Registry and Client are built on. Operators choose
/// the concrete transport via the [`McpServerSpec`](crate::McpServerSpec)
/// `transport` field.
pub trait McpTransport: Send + Sync + 'static {
    /// The identifier of the server this transport speaks to.
    fn server_id(&self) -> &ServerId;

    /// Send a JSON-RPC request payload and await the response.
    ///
    /// Iteration 1 Sub-batch B implements this for stdio.
    fn request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> BoxedMcpFuture<Result<serde_json::Value, McpError>>;
}

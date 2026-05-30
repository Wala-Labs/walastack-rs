//! Model Context Protocol (MCP) integration for WalaStack.
//!
//! WalaStack apps consume external MCP servers (subprocesses speaking
//! JSON-RPC) to expose tools and resources to AI workloads. MCP slots
//! cleanly into the locked Runtime architecture:
//!
//! - **Server** → `Service` under `SupervisionTree`.
//! - **Transport** → `dyn McpTransport` capability with provider plugins.
//! - **Discovery** → `dyn McpRegistry` capability.
//! - **Invocation** → `dyn McpClient` capability.
//! - **Credentials** → composes with the existing `dyn SecretsProvider`
//!   capability from `walastack-auth`.
//! - **Lifecycle** → events on the kernel `EventBus`.
//!
//! ## What this crate ships
//!
//! ### Sub-batch A (current — foundation)
//!
//! - [`ServerId`] newtype + descriptor types ([`ServerDescriptor`],
//!   [`ToolDescriptor`], [`ResourceDescriptor`], [`ResourceContent`]).
//! - Capability traits: [`McpRegistry`], [`McpClient`], [`McpTransport`]
//!   with the shared [`BoxedMcpFuture`] alias.
//! - Configuration types: [`McpConfig`], [`McpServerSpec`],
//!   [`TransportSpec`], [`EnvVar`].
//! - Six lifecycle events: [`McpServerConnected`],
//!   [`McpServerDisconnected`], [`McpToolInvoked`], [`McpToolFailed`],
//!   [`McpResourceRead`], [`McpServerUnhealthy`].
//! - [`InMemoryMcpPlugin`] — sovereign-friendly default, populates
//!   both `dyn McpRegistry` and `dyn McpClient` with hand-built fake
//!   servers. Suitable for tests + local demos.
//! - [`McpPlugin`] — top-level plugin that registers [`McpConfig`] as
//!   a kernel `Resource` (**fourth Resource-as-Configuration
//!   adoption**) and declares the `SecretsProvider` requirement.
//! - [`prelude`] module per the Tier 3 convention.
//!
//! ### Sub-batch B (next)
//!
//! - `StdioTransportPlugin` (subprocess MCP servers via stdin/stdout).
//! - Minimal JSON-RPC client (no third-party JSON-RPC dependency).
//! - `McpServerService` — one supervised Service per configured server.
//! - Basic JSON-RPC `ping` liveness check.
//! - Real stdio-backed implementation of `McpRegistry` + `McpClient`.
//! - End-to-end integration test against a fake stdio MCP server.
//!
//! ## Out of scope (per locked architecture proposal)
//!
//! - **MCP server side** (exposing WalaStack as an MCP server).
//! - HTTP/SSE/WebSocket transports.
//! - Prompt templates.
//! - Advanced MCP features (subscriptions, notifications, progress,
//!   streaming results).
//! - OAuth credential acquisition flows.
//! - Multi-server tool aggregation.
//! - Tool result caching.
//! - Agent loop primitives (separate crate, future work).
//!
//! ## Secrets composition
//!
//! Per locked Doctrine 1 + 2 and the architecture proposal:
//! [`McpServerSpec`] env-var bindings carry secret **names**, never
//! values. The value is resolved at server-start time via the
//! kernel-registered `dyn SecretsProvider` capability — operators
//! choose the concrete provider (in-memory for dev / sovereign,
//! vault-backed for production, future Wala Cloud managed-secrets).

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
// "MCP", "JSON-RPC", "OAuth", "WebSocket" are domain names, not code
// identifiers.
#![allow(clippy::doc_markdown)]
// MutexGuard scoping is intentional across `stdio.rs` write paths
// (same pattern as walastack-jobs); explicit `drop(...)` noise doesn't
// help readability.
#![allow(
    clippy::significant_drop_tightening,
    clippy::significant_drop_in_scrutinee
)]
// Reader/writer loops + capability methods have natural error-handling
// branches; splitting further fragments the lifecycle.
#![allow(clippy::cognitive_complexity)]
// StdioConnection::open does subprocess spawn + reader-task setup +
// stderr drain + handshake in one place by design; splitting fragments
// the protocol initialization story.
#![allow(clippy::too_many_lines)]

pub mod capabilities;
pub mod config;
pub mod descriptors;
pub mod errors;
pub mod events;
pub mod inmemory;
pub mod jsonrpc;
pub mod plugin;
pub mod prelude;
pub mod service;
pub mod stdio;

// Top-level re-exports of the most-named types so first-time users can
// `use walastack_mcp::{...}` without descending into modules.
pub use capabilities::{BoxedMcpFuture, McpClient, McpRegistry, McpTransport};
pub use config::{EnvVar, McpConfig, McpServerSpec, TransportSpec};
pub use descriptors::{
    ResourceContent, ResourceDescriptor, ResourcePayload, ServerDescriptor, ServerId,
    ToolDescriptor,
};
pub use errors::McpError;
pub use events::{
    McpResourceRead, McpServerConnected, McpServerDisconnected, McpServerUnhealthy, McpToolFailed,
    McpToolInvoked,
};
pub use inmemory::{InMemoryMcp, InMemoryMcpPlugin, InMemoryServer, ToolImpl};
pub use plugin::McpPlugin;

#[cfg(test)]
mod tests;

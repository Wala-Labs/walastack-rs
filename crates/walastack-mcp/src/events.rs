//! Lifecycle events published on the kernel `EventBus`.
//!
//! Observability subscribers and custom domain reactions consume these
//! through the standard `RuntimeContext` / `EventBus` access pattern
//! (see the `capabilities-and-resources` guide).

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::descriptors::ServerId;

/// Emitted when a configured MCP server completes its initial JSON-RPC
/// `initialize` handshake. For the in-memory provider, fires once at
/// service start.
#[derive(Clone, Debug)]
pub struct McpServerConnected {
    /// Identifier of the server that connected.
    pub server: ServerId,
    /// When the connection was established.
    pub at: DateTime<Utc>,
}

/// Emitted when a connected MCP server drops — subprocess exit, ping
/// timeout, or operator-initiated shutdown.
#[derive(Clone, Debug)]
pub struct McpServerDisconnected {
    /// Identifier of the server that disconnected.
    pub server: ServerId,
    /// When the disconnect was detected.
    pub at: DateTime<Utc>,
    /// Optional sanitized reason for the disconnect.
    pub reason: Option<String>,
}

/// Emitted when a tool invocation completes successfully.
#[derive(Clone, Debug)]
pub struct McpToolInvoked {
    /// Identifier of the server the tool was invoked on.
    pub server: ServerId,
    /// Tool name.
    pub tool: String,
    /// Wall-clock duration of the invocation (request → response).
    pub duration: Duration,
}

/// Emitted when a tool invocation fails (transport error, remote error,
/// timeout, etc.).
#[derive(Clone, Debug)]
pub struct McpToolFailed {
    /// Identifier of the server the tool was invoked on.
    pub server: ServerId,
    /// Tool name.
    pub tool: String,
    /// Sanitized error string (per the Rejection-Mapping Discipline,
    /// detailed diagnostic information stays in `tracing::error!`
    /// logs).
    pub error: String,
}

/// Emitted when a resource read completes successfully.
#[derive(Clone, Debug)]
pub struct McpResourceRead {
    /// Identifier of the server the resource was read from.
    pub server: ServerId,
    /// Resource URI.
    pub uri: String,
    /// Size of the returned payload in bytes (UTF-8 length for text
    /// content; raw byte count for blobs after base64 decode).
    pub size_bytes: usize,
}

/// Emitted when a ping liveness check fails for a connected server.
/// SupervisionTree restart follows.
#[derive(Clone, Debug)]
pub struct McpServerUnhealthy {
    /// Identifier of the server that failed the liveness check.
    pub server: ServerId,
    /// When the failure was observed.
    pub at: DateTime<Utc>,
    /// Sanitized failure reason.
    pub reason: String,
}

//! Error types for `walastack-mcp`.

use std::fmt;

use crate::descriptors::ServerId;

/// Failures surfaced by `walastack-mcp` capability methods.
///
/// Follows the locked **Rejection-Mapping Discipline** (see
/// `project-ecosystem-conventions`): public error variants carry
/// sanitized strings; detailed diagnostic information should land in
/// `tracing::error!` at the call site that maps to this enum, never in
/// the variant payload itself.
#[derive(Clone, Debug)]
pub enum McpError {
    /// The named server is not configured / not present.
    UnknownServer(ServerId),
    /// The named tool was not found on the indicated server.
    UnknownTool {
        /// The server that was queried.
        server: ServerId,
        /// The tool name that was missing.
        tool: String,
    },
    /// The named resource URI was not found on the indicated server.
    UnknownResource {
        /// The server that was queried.
        server: ServerId,
        /// The resource URI that was missing.
        uri: String,
    },
    /// Tool arguments could not be serialized / deserialized.
    Serialization(String),
    /// Transport-level failure (IO, subprocess exit, protocol error).
    Transport(String),
    /// The remote server returned a JSON-RPC error response.
    RemoteError {
        /// JSON-RPC error code surfaced by the remote.
        code: i32,
        /// Sanitized error message from the remote.
        message: String,
    },
    /// The remote server failed to respond within the configured
    /// timeout.
    Timeout {
        /// The server that did not respond.
        server: ServerId,
    },
    /// The configured secret could not be resolved when the MCP server
    /// was about to start. Includes the secret name for log
    /// correlation; does NOT include the missing value.
    SecretNotFound(String),
    /// Catch-all for ad-hoc operational failures. Prefer specific
    /// variants where possible.
    Other(String),
}

impl fmt::Display for McpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownServer(id) => write!(f, "unknown MCP server {id:?}"),
            Self::UnknownTool { server, tool } => {
                write!(f, "tool {tool:?} not found on server {server:?}")
            }
            Self::UnknownResource { server, uri } => {
                write!(f, "resource {uri:?} not found on server {server:?}")
            }
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::RemoteError { code, message } => {
                write!(f, "remote MCP error {code}: {message}")
            }
            Self::Timeout { server } => write!(f, "timeout waiting for server {server:?}"),
            Self::SecretNotFound(name) => write!(f, "secret {name:?} not found"),
            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for McpError {}

impl From<serde_json::Error> for McpError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}

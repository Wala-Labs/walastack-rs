//! Typed descriptors flowing through the [`McpRegistry`](crate::McpRegistry)
//! and [`McpClient`](crate::McpClient) capability methods.
//!
//! These are **data, not capabilities** — they're the typed payloads
//! that callers receive when discovering servers / tools / resources.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable identifier for an MCP server.
///
/// Newtype'd over `String` so consumer code cannot confuse it with
/// arbitrary strings (tool names, resource URIs, secret names).
///
/// **Future extension seam (per architecture proposal):** as MCP
/// servers accumulate metadata / permissions / ownership / health /
/// policy / observability information, this type is the natural
/// expansion point. Iteration 1 ships it as a simple newtype with
/// zero behavior — the value lives in the type identity, not in
/// runtime overhead.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ServerId(pub String);

impl ServerId {
    /// Construct from a `&str` or `String`.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the underlying string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ServerId {
    fn from(s: &str) -> Self {
        Self(s.into())
    }
}

impl From<String> for ServerId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Summary of an MCP server returned by
/// [`McpRegistry::list_servers`](crate::McpRegistry::list_servers).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerDescriptor {
    /// Identifier the registry uses to refer to this server.
    pub id: ServerId,
    /// Optional human-readable display name from the server's
    /// `serverInfo` block (MCP `initialize` handshake).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub display_name: Option<String>,
    /// Whether the registry currently considers the server connected.
    /// In-memory providers always report `true`; stdio-backed
    /// providers update this in response to lifecycle events.
    pub connected: bool,
}

/// A tool exposed by an MCP server.
///
/// `input_schema` and `output_schema` are JSON-Schema-shaped documents
/// (see `walastack-openapi`'s `Schema` if you want a typed model);
/// stored here as `serde_json::Value` so the descriptor can travel
/// through the registry without forcing every consumer to depend on
/// `walastack-openapi`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolDescriptor {
    /// Tool name as the server exposes it (the value passed to
    /// `invoke_tool`).
    pub name: String,
    /// Optional human-readable description.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub description: Option<String>,
    /// JSON Schema for the `arguments` payload.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub input_schema: Option<serde_json::Value>,
    /// JSON Schema for the tool's return value when known.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub output_schema: Option<serde_json::Value>,
}

/// A resource exposed by an MCP server.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceDescriptor {
    /// Resource URI (the value passed to `read_resource`).
    pub uri: String,
    /// Optional display name.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub name: Option<String>,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub description: Option<String>,
    /// MIME type when known.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub mime_type: Option<String>,
}

/// Contents returned by
/// [`McpClient::read_resource`](crate::McpClient::read_resource).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceContent {
    /// URI that was read.
    pub uri: String,
    /// MIME type, when known.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub mime_type: Option<String>,
    /// Either UTF-8 text or base64-encoded bytes per MCP convention.
    pub payload: ResourcePayload,
}

/// One arm of an MCP resource read response.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePayload {
    /// UTF-8 text content.
    Text(String),
    /// Base64-encoded binary content.
    Blob(String),
}

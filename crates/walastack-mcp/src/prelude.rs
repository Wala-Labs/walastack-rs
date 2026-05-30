//! Common imports for applications using `walastack-mcp`.
//!
//! ```rust
//! use walastack_mcp::prelude::*;
//! ```
//!
//! Re-exports the capability traits, public configuration / descriptor
//! types, the [`McpPlugin`], and the sovereign-friendly
//! [`InMemoryMcpPlugin`]. Lower-level types (`BoxedMcpFuture`,
//! `ResourcePayload`) remain available from the crate root.

pub use crate::capabilities::{McpClient, McpRegistry, McpTransport};
pub use crate::config::{EnvVar, McpConfig, McpServerSpec, TransportSpec};
pub use crate::descriptors::{
    ResourceContent, ResourceDescriptor, ServerDescriptor, ServerId, ToolDescriptor,
};
pub use crate::errors::McpError;
pub use crate::events::{
    McpResourceRead, McpServerConnected, McpServerDisconnected, McpServerUnhealthy, McpToolFailed,
    McpToolInvoked,
};
pub use crate::inmemory::{InMemoryMcp, InMemoryMcpPlugin, InMemoryServer};
pub use crate::plugin::McpPlugin;

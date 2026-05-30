//! In-memory MCP provider — sovereign-friendly default per Doctrine 2.
//!
//! Backs the [`McpRegistry`] and [`McpClient`] capabilities with a
//! hand-populated map of fake servers / tools / resources. Suitable
//! for:
//!
//! - Unit tests across the ecosystem (handlers can call MCP capabilities
//!   without spawning subprocesses).
//! - Sovereign single-node demos where the operator hardcodes a tool
//!   catalog.
//! - Local prototyping before a real MCP server is wired up.
//!
//! Real MCP servers — anything that speaks JSON-RPC over a transport —
//! land in Sub-batch B's `stdio` module.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use walastack_runtime::{CapabilityRegistry, Plugin};

use crate::capabilities::{BoxedMcpFuture, McpClient, McpRegistry};
use crate::descriptors::{
    ResourceContent, ResourceDescriptor, ServerDescriptor, ServerId, ToolDescriptor,
};
use crate::errors::McpError;

/// Type alias for a tool implementation function. Takes JSON arguments
/// and returns either a JSON value or a sanitized error string.
pub type ToolImpl =
    Arc<dyn Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync>;

/// In-memory MCP server — a bag of tool descriptors + implementations
/// + resource descriptors + contents, addressable by [`ServerId`].
#[derive(Clone)]
pub struct InMemoryServer {
    id: ServerId,
    display_name: Option<String>,
    tools: Vec<ToolDescriptor>,
    tool_impls: HashMap<String, ToolImpl>,
    resources: Vec<ResourceDescriptor>,
    resource_contents: HashMap<String, ResourceContent>,
}

impl std::fmt::Debug for InMemoryServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryServer")
            .field("id", &self.id)
            .field("display_name", &self.display_name)
            .field("tools", &self.tools.len())
            .field("resources", &self.resources.len())
            .finish_non_exhaustive()
    }
}

impl InMemoryServer {
    /// Construct an empty in-memory server with the given id.
    #[must_use]
    pub fn new(id: impl Into<ServerId>) -> Self {
        Self {
            id: id.into(),
            display_name: None,
            tools: Vec::new(),
            tool_impls: HashMap::new(),
            resources: Vec::new(),
            resource_contents: HashMap::new(),
        }
    }

    /// Set the display name.
    #[must_use]
    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }

    /// Register a tool: its descriptor + its implementation.
    #[must_use]
    pub fn with_tool<F>(mut self, descriptor: ToolDescriptor, run: F) -> Self
    where
        F: Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync + 'static,
    {
        let name = descriptor.name.clone();
        self.tools.push(descriptor);
        self.tool_impls.insert(name, Arc::new(run));
        self
    }

    /// Register a resource: its descriptor + its contents.
    #[must_use]
    pub fn with_resource(
        mut self,
        descriptor: ResourceDescriptor,
        content: ResourceContent,
    ) -> Self {
        let uri = descriptor.uri.clone();
        self.resources.push(descriptor);
        self.resource_contents.insert(uri, content);
        self
    }
}

/// Combined in-memory implementation of `McpRegistry` + `McpClient`.
/// Same handle satisfies both traits — the split is for consumer
/// access pattern.
#[derive(Debug, Default)]
pub struct InMemoryMcp {
    inner: Mutex<InMemoryMcpInner>,
}

#[derive(Debug, Default)]
struct InMemoryMcpInner {
    servers: Vec<InMemoryServer>,
}

impl InMemoryMcp {
    /// Construct an empty registry / client.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a server.
    #[must_use]
    pub fn with_server(self, server: InMemoryServer) -> Self {
        {
            let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
            inner.servers.push(server);
        }
        self
    }

    fn find_server<R>(&self, id: &ServerId, f: impl FnOnce(&InMemoryServer) -> R) -> Option<R> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.servers.iter().find(|s| s.id == *id).map(f)
    }
}

impl McpRegistry for InMemoryMcp {
    fn list_servers(&self) -> Vec<ServerDescriptor> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner
            .servers
            .iter()
            .map(|s| ServerDescriptor {
                id: s.id.clone(),
                display_name: s.display_name.clone(),
                connected: true,
            })
            .collect()
    }

    fn list_tools(
        &self,
        server: &ServerId,
    ) -> BoxedMcpFuture<Result<Vec<ToolDescriptor>, McpError>> {
        let result = self
            .find_server(server, |s| s.tools.clone())
            .ok_or_else(|| McpError::UnknownServer(server.clone()));
        Box::pin(async move { result })
    }

    fn list_resources(
        &self,
        server: &ServerId,
    ) -> BoxedMcpFuture<Result<Vec<ResourceDescriptor>, McpError>> {
        let result = self
            .find_server(server, |s| s.resources.clone())
            .ok_or_else(|| McpError::UnknownServer(server.clone()));
        Box::pin(async move { result })
    }
}

impl McpClient for InMemoryMcp {
    fn invoke_tool(
        &self,
        server: &ServerId,
        tool: &str,
        arguments: serde_json::Value,
    ) -> BoxedMcpFuture<Result<serde_json::Value, McpError>> {
        let result = self
            .find_server(server, |s| s.tool_impls.get(tool).cloned())
            .ok_or_else(|| McpError::UnknownServer(server.clone()))
            .and_then(|opt| {
                opt.ok_or_else(|| McpError::UnknownTool {
                    server: server.clone(),
                    tool: tool.to_string(),
                })
            });
        let server_id = server.clone();
        let tool = tool.to_string();
        Box::pin(async move {
            match result {
                Ok(run) => run(arguments).map_err(|message| McpError::RemoteError {
                    code: -32000,
                    message,
                }),
                Err(McpError::UnknownTool { .. }) => Err(McpError::UnknownTool {
                    server: server_id,
                    tool,
                }),
                Err(e) => Err(e),
            }
        })
    }

    fn read_resource(
        &self,
        server: &ServerId,
        uri: &str,
    ) -> BoxedMcpFuture<Result<ResourceContent, McpError>> {
        let result = self
            .find_server(server, |s| s.resource_contents.get(uri).cloned())
            .ok_or_else(|| McpError::UnknownServer(server.clone()))
            .and_then(|opt| {
                opt.ok_or_else(|| McpError::UnknownResource {
                    server: server.clone(),
                    uri: uri.to_string(),
                })
            });
        Box::pin(async move { result })
    }
}

/// Plugin that registers an [`InMemoryMcp`] under both `dyn McpRegistry`
/// and `dyn McpClient`. Sovereign-friendly default per Doctrine 2.
pub struct InMemoryMcpPlugin {
    inner: Arc<InMemoryMcp>,
}

impl InMemoryMcpPlugin {
    /// Construct empty.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryMcp::new()),
        }
    }

    /// Append a server.
    #[must_use]
    pub fn with_server(self, server: InMemoryServer) -> Self {
        Self {
            inner: Arc::new(
                Arc::try_unwrap(self.inner)
                    .unwrap_or_else(|arc| InMemoryMcp {
                        inner: Mutex::new(
                            arc.inner
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .clone(),
                        ),
                    })
                    .with_server(server),
            ),
        }
    }
}

impl Default for InMemoryMcpPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for InMemoryMcpInner {
    fn clone(&self) -> Self {
        Self {
            servers: self.servers.clone(),
        }
    }
}

impl std::fmt::Debug for InMemoryMcpPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryMcpPlugin")
            .field("servers", &self.inner.list_servers().len())
            .finish()
    }
}

impl Plugin for InMemoryMcpPlugin {
    fn name(&self) -> &'static str {
        "in-memory-mcp"
    }

    fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
        let registry_provider: Arc<dyn McpRegistry> = self.inner.clone();
        registry.register_default::<dyn McpRegistry>(registry_provider);
        let client_provider: Arc<dyn McpClient> = self.inner.clone();
        registry.register_default::<dyn McpClient>(client_provider);
    }
}

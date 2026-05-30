//! Stdio transport — subprocess MCP servers spoken to over
//! line-delimited JSON-RPC.
//!
//! [`StdioConnection`] owns the subprocess + I/O machinery for a
//! single configured server. [`StdioMcp`] holds the shared map of
//! active connections and implements both
//! [`McpRegistry`] and
//! [`McpClient`] by looking up the relevant
//! connection and forwarding the request.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::time::timeout;

use crate::capabilities::{BoxedMcpFuture, McpClient, McpRegistry};
use crate::descriptors::{
    ResourceContent, ResourceDescriptor, ResourcePayload, ServerDescriptor, ServerId,
    ToolDescriptor,
};
use crate::errors::McpError;
use crate::jsonrpc::{IncomingMessage, JsonRpcNotification, JsonRpcRequest, parse_incoming};

// =========================================================================
// StdioConnection — one subprocess + JSON-RPC pipe per MCP server
// =========================================================================

/// Per-server stdio connection. Holds the subprocess child handle, a
/// mutex-guarded stdin writer, and a shared pending-request map that
/// the reader task uses to dispatch responses.
///
/// Construction is async because the MCP `initialize` handshake runs
/// as part of [`StdioConnection::open`]. The reader task is spawned
/// before the handshake so the initialize response can land in the
/// pending map.
pub struct StdioConnection {
    server_id: ServerId,
    next_id: std::sync::atomic::AtomicU64,
    stdin: Mutex<ChildStdin>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<crate::jsonrpc::JsonRpcResponse>>>>,
    request_timeout: Duration,
    child: Mutex<Option<Child>>,
    server_display_name: Option<String>,
}

impl std::fmt::Debug for StdioConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdioConnection")
            .field("server_id", &self.server_id)
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl StdioConnection {
    /// Spawn the subprocess, perform the MCP `initialize` handshake,
    /// and return the live connection ready for tool / resource calls.
    pub async fn open(
        server_id: ServerId,
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
        request_timeout: Duration,
    ) -> Result<Self, McpError> {
        let mut cmd = Command::new(command);
        cmd.args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Apply env vars after the base command so test fixtures + real
        // operators get consistent ordering semantics.
        for (var, value) in env {
            cmd.env(var, value);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| McpError::Transport(format!("spawn {command:?} failed: {e}")))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| McpError::Transport("subprocess stdin unavailable".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| McpError::Transport("subprocess stdout unavailable".into()))?;
        let stderr = child.stderr.take();

        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<_>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Reader task — pumps stdout, dispatches responses to pending
        // oneshots, logs notifications.
        let reader_pending = Arc::clone(&pending);
        let reader_server_id = server_id.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            loop {
                match reader.next_line().await {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        match parse_incoming(&line) {
                            Ok(IncomingMessage::Response(r)) => {
                                let mut pending = reader_pending.lock().await;
                                if let Some(tx) = pending.remove(&r.id) {
                                    let _ = tx.send(r);
                                }
                            }
                            Ok(IncomingMessage::Notification { method, .. }) => {
                                tracing::debug!(
                                    server = %reader_server_id,
                                    %method,
                                    "MCP notification received"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    server = %reader_server_id,
                                    error = %e,
                                    line = %line,
                                    "failed to parse MCP message"
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::debug!(server = %reader_server_id, "MCP stdout closed");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            server = %reader_server_id,
                            error = %e,
                            "MCP stdout read error; reader task exiting"
                        );
                        break;
                    }
                }
            }
        });

        // Stderr drain task — just logs anything the subprocess writes.
        if let Some(stderr) = stderr {
            let stderr_server_id = server_id.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    tracing::debug!(server = %stderr_server_id, stderr = %line, "MCP stderr");
                }
            });
        }

        let connection = Self {
            server_id,
            next_id: std::sync::atomic::AtomicU64::new(1),
            stdin: Mutex::new(stdin),
            pending,
            request_timeout,
            child: Mutex::new(Some(child)),
            server_display_name: None,
        };

        // MCP initialize handshake.
        let init_params = serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "walastack-mcp",
                "version": env!("CARGO_PKG_VERSION"),
            },
        });
        let init_result = connection.request("initialize", init_params).await?;
        let display_name = init_result
            .get("serverInfo")
            .and_then(|v| v.get("name"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        // `initialized` notification finalizes the handshake.
        connection
            .notify("notifications/initialized", serde_json::json!({}))
            .await?;

        Ok(Self {
            server_display_name: display_name,
            ..connection
        })
    }

    /// Send a JSON-RPC request and await the response, subject to the
    /// configured per-request timeout.
    pub async fn request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, McpError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let req = JsonRpcRequest::new(id, method, params);
        let mut payload = serde_json::to_vec(&req)?;
        payload.push(b'\n');

        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(id, tx);
        }

        {
            let mut stdin = self.stdin.lock().await;
            stdin
                .write_all(&payload)
                .await
                .map_err(|e| McpError::Transport(format!("stdin write failed: {e}")))?;
            stdin
                .flush()
                .await
                .map_err(|e| McpError::Transport(format!("stdin flush failed: {e}")))?;
        }

        let response = match timeout(self.request_timeout, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => {
                // Reader task dropped the sender — connection lost.
                let mut pending = self.pending.lock().await;
                pending.remove(&id);
                return Err(McpError::Transport(
                    "reader task closed before responding".into(),
                ));
            }
            Err(_) => {
                let mut pending = self.pending.lock().await;
                pending.remove(&id);
                return Err(McpError::Timeout {
                    server: self.server_id.clone(),
                });
            }
        };
        if let Some(err) = response.error {
            return Err(McpError::RemoteError {
                code: err.code,
                message: err.message,
            });
        }
        Ok(response.result.unwrap_or(serde_json::Value::Null))
    }

    /// Send a JSON-RPC notification (fire-and-forget).
    pub async fn notify(&self, method: &str, params: serde_json::Value) -> Result<(), McpError> {
        let n = JsonRpcNotification::new(method, params);
        let mut payload = serde_json::to_vec(&n)?;
        payload.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(&payload)
            .await
            .map_err(|e| McpError::Transport(format!("stdin write failed: {e}")))?;
        stdin
            .flush()
            .await
            .map_err(|e| McpError::Transport(format!("stdin flush failed: {e}")))?;
        Ok(())
    }

    /// Kill the subprocess if still running. Idempotent.
    pub async fn close(&self) {
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.kill().await;
        }
    }

    /// The server's display name from `initialize`'s `serverInfo`, if
    /// the server provided one.
    #[must_use]
    pub fn display_name(&self) -> Option<&str> {
        self.server_display_name.as_deref()
    }
}

// =========================================================================
// StdioMcp — shared connection map + capability impls
// =========================================================================

/// Combined `McpRegistry` + `McpClient` implementation backed by
/// stdio connections.
///
/// Per-server connections are inserted into the shared map by
/// `McpServerService` at service
/// start. The map remains empty until services come up — list_servers
/// reflects what has connected so far.
///
/// Same handle satisfies both capability traits — the public split is
/// for **consumer access pattern**, not implementation.
pub struct StdioMcp {
    connections: Arc<RwLock<HashMap<ServerId, Arc<StdioConnection>>>>,
}

impl std::fmt::Debug for StdioMcp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdioMcp").finish_non_exhaustive()
    }
}

impl Default for StdioMcp {
    fn default() -> Self {
        Self::new()
    }
}

impl StdioMcp {
    /// Construct empty. The connection map is populated by per-server
    /// services at startup.
    #[must_use]
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert a freshly-opened connection. Called by
    /// `McpServerService` at start.
    pub async fn insert(&self, server: ServerId, connection: Arc<StdioConnection>) {
        self.connections.write().await.insert(server, connection);
    }

    /// Remove a connection on disconnect / shutdown.
    pub async fn remove(&self, server: &ServerId) -> Option<Arc<StdioConnection>> {
        self.connections.write().await.remove(server)
    }
}

impl McpRegistry for StdioMcp {
    fn list_servers(&self) -> Vec<ServerDescriptor> {
        // Block briefly on the read lock. `list_servers` is sync in the
        // trait signature; the lock is uncontended on the typical
        // application path.
        let Ok(map) = self.connections.try_read() else {
            return Vec::new();
        };
        map.iter()
            .map(|(id, conn)| ServerDescriptor {
                id: id.clone(),
                display_name: conn.display_name().map(str::to_string),
                connected: true,
            })
            .collect()
    }

    fn list_tools(
        &self,
        server: &ServerId,
    ) -> BoxedMcpFuture<Result<Vec<ToolDescriptor>, McpError>> {
        let server = server.clone();
        let connections = Arc::clone(&self.connections);
        Box::pin(async move {
            let conn = connections
                .read()
                .await
                .get(&server)
                .cloned()
                .ok_or_else(|| McpError::UnknownServer(server.clone()))?;
            let result = conn.request("tools/list", serde_json::json!({})).await?;
            let raw = result
                .get("tools")
                .cloned()
                .ok_or_else(|| McpError::Transport("tools/list result missing `tools`".into()))?;
            let tools: Vec<ToolDescriptor> = serde_json::from_value(raw)?;
            Ok(tools)
        })
    }

    fn list_resources(
        &self,
        server: &ServerId,
    ) -> BoxedMcpFuture<Result<Vec<ResourceDescriptor>, McpError>> {
        let server = server.clone();
        let connections = Arc::clone(&self.connections);
        Box::pin(async move {
            let conn = connections
                .read()
                .await
                .get(&server)
                .cloned()
                .ok_or_else(|| McpError::UnknownServer(server.clone()))?;
            let result = conn
                .request("resources/list", serde_json::json!({}))
                .await?;
            let raw = result.get("resources").cloned().ok_or_else(|| {
                McpError::Transport("resources/list result missing `resources`".into())
            })?;
            let resources: Vec<ResourceDescriptor> = serde_json::from_value(raw)?;
            Ok(resources)
        })
    }
}

impl McpClient for StdioMcp {
    fn invoke_tool(
        &self,
        server: &ServerId,
        tool: &str,
        arguments: serde_json::Value,
    ) -> BoxedMcpFuture<Result<serde_json::Value, McpError>> {
        let server = server.clone();
        let tool = tool.to_string();
        let connections = Arc::clone(&self.connections);
        Box::pin(async move {
            let conn = connections
                .read()
                .await
                .get(&server)
                .cloned()
                .ok_or_else(|| McpError::UnknownServer(server.clone()))?;
            let params = serde_json::json!({
                "name": tool,
                "arguments": arguments,
            });
            conn.request("tools/call", params).await
        })
    }

    fn read_resource(
        &self,
        server: &ServerId,
        uri: &str,
    ) -> BoxedMcpFuture<Result<ResourceContent, McpError>> {
        let server = server.clone();
        let uri = uri.to_string();
        let connections = Arc::clone(&self.connections);
        Box::pin(async move {
            let conn = connections
                .read()
                .await
                .get(&server)
                .cloned()
                .ok_or_else(|| McpError::UnknownServer(server.clone()))?;
            let params = serde_json::json!({"uri": &uri});
            let result = conn.request("resources/read", params).await?;
            // The MCP spec returns a `contents` array; for simplicity
            // we pick the first text/blob entry. Multi-content
            // responses are rare for read; revisit if needed.
            let contents = result
                .get("contents")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| {
                    McpError::Transport("resources/read result missing `contents` array".into())
                })?;
            let first = contents.first().ok_or_else(|| {
                McpError::Transport("resources/read returned empty contents".into())
            })?;
            let mime_type = first
                .get("mimeType")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            let payload = if let Some(text) = first.get("text").and_then(serde_json::Value::as_str)
            {
                ResourcePayload::Text(text.to_string())
            } else if let Some(blob) = first.get("blob").and_then(serde_json::Value::as_str) {
                ResourcePayload::Blob(blob.to_string())
            } else {
                return Err(McpError::Transport(
                    "resources/read content has neither text nor blob".into(),
                ));
            };
            Ok(ResourceContent {
                uri,
                mime_type,
                payload,
            })
        })
    }
}

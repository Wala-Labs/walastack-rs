//! [`McpServerService`] — one supervised Service per configured MCP
//! server.
//!
//! Each service owns its subprocess lifecycle: resolve secrets, spawn,
//! run the MCP `initialize` handshake, register the connection into
//! the shared [`StdioMcp`] map, run a periodic `ping` liveness check,
//! and drain cleanly on the kernel shutdown signal.
//!
//! Crash semantics: any failure during start returns a `ServiceError`
//! which the SupervisionTree's `OnFailure` policy restarts with the
//! configured backoff. Failures mid-run (subprocess exit, ping
//! timeout) are surfaced by terminating the per-service task with an
//! `McpServerUnhealthy` event published first.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::task::JoinHandle;
use walastack_auth::SecretsProvider;
use walastack_runtime::{
    BoxedServiceFuture, RuntimeContext, Service, ServiceContext, ServiceError,
};

use crate::config::{EnvVar, McpConfig, McpServerSpec, TransportSpec};
use crate::events::{McpServerConnected, McpServerDisconnected, McpServerUnhealthy};
use crate::stdio::{StdioConnection, StdioMcp};

/// One per-server supervised Service.
///
/// Holds the `McpServerSpec`, the shared `StdioMcp` connection map,
/// the parent `McpConfig` (for default request timeout + ping
/// interval), and a `Box::leak`-ed `&'static str` server name so the
/// supervision tree can address it.
pub struct McpServerService {
    spec: McpServerSpec,
    stdio: Arc<StdioMcp>,
    config: McpConfig,
    name: &'static str,
}

impl McpServerService {
    /// Construct. `Box::leak`s the per-server static name (bounded leak
    /// of ~30 bytes per server, per process — same pattern as
    /// `walastack-jobs` workers; documented kernel-API friction
    /// observation #10).
    #[must_use]
    pub fn new(spec: McpServerSpec, stdio: Arc<StdioMcp>, config: McpConfig) -> Self {
        let name: &'static str = Box::leak(format!("mcp-{}", spec.id).into_boxed_str());
        Self {
            spec,
            stdio,
            config,
            name,
        }
    }
}

impl std::fmt::Debug for McpServerService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpServerService")
            .field("server_id", &self.spec.id)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Service for McpServerService {
    fn name(&self) -> &'static str {
        self.name
    }

    fn start(
        &self,
        ctx: ServiceContext,
    ) -> BoxedServiceFuture<std::result::Result<JoinHandle<()>, ServiceError>> {
        let spec = self.spec.clone();
        let stdio = Arc::clone(&self.stdio);
        let request_timeout = self
            .spec
            .request_timeout
            .unwrap_or(self.config.default_request_timeout);
        let ping_interval = self.config.ping_interval;
        Box::pin(async move {
            let runtime = ctx.runtime().clone();

            // Resolve secrets at start time. Catches misconfigured
            // secret names early so SupervisionTree's restart loop
            // doesn't churn on a permanently-broken config.
            let env_map = resolve_env(&runtime, &spec)?;

            // Spawn subprocess + initialize handshake.
            let (command, args) = match &spec.transport {
                TransportSpec::Stdio { command, args } => (command.clone(), args.clone()),
            };
            let connection =
                StdioConnection::open(spec.id.clone(), &command, &args, &env_map, request_timeout)
                    .await
                    .map_err(|e| {
                        ServiceError::new(format!("MCP server {} failed to open: {e}", spec.id))
                    })?;

            let connection = Arc::new(connection);
            stdio.insert(spec.id.clone(), Arc::clone(&connection)).await;
            runtime.publish(McpServerConnected {
                server: spec.id.clone(),
                at: Utc::now(),
            });
            tracing::info!(server = %spec.id, "MCP server connected");

            // Per-server task: ping loop + shutdown signal listener.
            let mut shutdown = runtime.shutdown_signal();
            let handle = tokio::spawn(async move {
                let server_id = spec.id.clone();
                loop {
                    tokio::select! {
                        () = shutdown.wait() => {
                            tracing::info!(server = %server_id, "MCP server shutdown signal");
                            break;
                        }
                        () = tokio::time::sleep(ping_interval) => {
                            match connection.request("ping", serde_json::json!({})).await {
                                Ok(_) => {
                                    tracing::debug!(server = %server_id, "MCP ping ok");
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        server = %server_id,
                                        error = %e,
                                        "MCP ping failed; marking unhealthy"
                                    );
                                    runtime.publish(McpServerUnhealthy {
                                        server: server_id.clone(),
                                        at: Utc::now(),
                                        reason: e.to_string(),
                                    });
                                    break;
                                }
                            }
                        }
                    }
                }
                // Clean up: kill subprocess, drop from the connection map.
                connection.close().await;
                stdio.remove(&server_id).await;
                runtime.publish(McpServerDisconnected {
                    server: server_id.clone(),
                    at: Utc::now(),
                    reason: None,
                });
                tracing::info!(server = %server_id, "MCP server disconnected");
            });
            Ok(handle)
        })
    }
}

/// Build the final environment-variable map by resolving each
/// [`EnvVar::FromSecret`] against the `SecretsProvider` capability and
/// passing [`EnvVar::Literal`] through unchanged.
///
/// Failures here surface as `ServiceError` — visible at service-start
/// time, before any subprocess is spawned. Secrets that fail to
/// resolve carry the *name* in the error, never the (would-be) value.
fn resolve_env(
    runtime: &RuntimeContext,
    spec: &McpServerSpec,
) -> Result<HashMap<String, String>, ServiceError> {
    let mut out = HashMap::new();
    if spec.env.is_empty() {
        return Ok(out);
    }
    let secrets = runtime
        .capability::<dyn SecretsProvider>()
        .ok_or_else(|| ServiceError::new("SecretsProvider capability not registered"))?;
    for entry in &spec.env {
        match entry {
            EnvVar::FromSecret { var, secret_name } => {
                let bytes = secrets.get(secret_name).ok_or_else(|| {
                    ServiceError::new(format!(
                        "MCP server {} requires secret {secret_name:?} which is not registered",
                        spec.id
                    ))
                })?;
                let value = String::from_utf8(bytes).map_err(|_| {
                    ServiceError::new(format!(
                        "secret {secret_name:?} is not valid UTF-8 for env var {var:?}"
                    ))
                })?;
                out.insert(var.clone(), value);
            }
            EnvVar::Literal { var, value } => {
                out.insert(var.clone(), value.clone());
            }
        }
    }
    Ok(out)
}

// Make `Duration` linker-visible to the user-facing module docs.
#[doc(hidden)]
pub const fn _doc_keep_duration(_: Duration) {}

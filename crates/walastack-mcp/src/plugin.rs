//! [`McpPlugin`] — top-level plugin composing the MCP configuration,
//! the stdio-backed `McpRegistry` + `McpClient` capabilities, and one
//! supervised [`McpServerService`] per configured server.
//!
//! Composition with `walastack-auth`: the plugin declares a
//! `SecretsProvider` capability requirement so the build fails fast
//! if no secrets backend is registered. Secret resolution happens at
//! per-server start time in [`McpServerService`]; configuration carries
//! secret **names**, never values.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use walastack_auth::SecretsProvider;
use walastack_runtime::{
    Backoff, CapabilityRegistry, CapabilityRequirement, Plugin, ResourceRegistry, RestartPolicy,
    ServicePlanner,
};

use crate::capabilities::{McpClient, McpRegistry};
use crate::config::McpConfig;
use crate::service::McpServerService;
use crate::stdio::StdioMcp;

/// Top-level MCP plugin.
///
/// Composes:
///
/// - [`McpConfig`] as a kernel `Resource` (**4th
///   Resource-as-Configuration adoption**).
/// - A shared [`StdioMcp`] registered under both `dyn McpRegistry` and
///   `dyn McpClient`.
/// - One [`McpServerService`] per configured server, supervised under
///   `RestartPolicy::OnFailure` so subprocess crashes restart the
///   affected server without bringing down peers.
/// - Declared `SecretsProvider` capability requirement.
///
/// Composition pattern (typical production deployment, shown via the
/// `walastack` umbrella's `full` prelude):
///
/// ```ignore
/// use walastack::prelude::*;
/// use walastack::prelude::full::*;
///
/// let mcp_config = McpConfig::new().with_server(
///     McpServerSpec::new("github").with_transport(TransportSpec::stdio(
///         "npx",
///         ["-y", "@modelcontextprotocol/server-github"],
///     )).with_env_from_secret("GITHUB_TOKEN", "github-pat"),
/// );
///
/// App::new()
///     .with_plugin(InMemorySecretsPlugin::new().with("github-pat", b"..."))
///     .with_plugin(McpPlugin::new(mcp_config))
///     .run("127.0.0.1:3000")
///     .await
/// ```
///
/// For tests + sovereign single-node demos with hand-built fake
/// servers, use [`crate::inmemory::InMemoryMcpPlugin`] instead. The
/// two plugins should NOT be composed together — they both register
/// providers under the default-name `dyn McpRegistry` / `dyn McpClient`
/// slots.
pub struct McpPlugin {
    config: McpConfig,
    stdio: Arc<StdioMcp>,
}

impl McpPlugin {
    /// Construct from a configured [`McpConfig`].
    #[must_use]
    pub fn new(config: McpConfig) -> Self {
        Self {
            config,
            stdio: Arc::new(StdioMcp::new()),
        }
    }

    /// Borrow the underlying config.
    #[must_use]
    pub const fn config(&self) -> &McpConfig {
        &self.config
    }
}

impl fmt::Debug for McpPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpPlugin")
            .field("servers", &self.config.servers.len())
            .field(
                "default_request_timeout",
                &self.config.default_request_timeout,
            )
            .field("ping_interval", &self.config.ping_interval)
            .finish_non_exhaustive()
    }
}

impl Plugin for McpPlugin {
    fn name(&self) -> &'static str {
        "mcp"
    }

    fn register_resources(&self, registry: &mut ResourceRegistry) {
        registry.insert(self.config.clone());
    }

    fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
        let as_registry: Arc<dyn McpRegistry> = self.stdio.clone();
        let as_client: Arc<dyn McpClient> = self.stdio.clone();
        registry.register_default::<dyn McpRegistry>(as_registry);
        registry.register_default::<dyn McpClient>(as_client);
    }

    fn register_services(&self, planner: &mut ServicePlanner) {
        let policy = RestartPolicy::OnFailure {
            // Unlimited restarts — subprocess crashes shouldn't burn
            // the supervision budget.
            max_attempts: u32::MAX,
            // Modest restart backoff so a permanently-broken server
            // doesn't churn the host.
            backoff: Backoff::Linear {
                base: Duration::from_secs(1),
                step: Duration::from_secs(1),
            },
        };
        for spec in &self.config.servers {
            planner.add_supervised(
                McpServerService::new(spec.clone(), Arc::clone(&self.stdio), self.config.clone()),
                policy.clone(),
            );
        }
    }

    fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
        // SecretsProvider is required even if no server uses
        // `EnvVar::FromSecret`, so that the requirement is visible at
        // build time and operators always wire up a secrets backend.
        vec![CapabilityRequirement::any::<dyn SecretsProvider>()]
    }
}

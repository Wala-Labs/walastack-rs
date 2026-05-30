//! [`McpPlugin`] — top-level plugin composing the MCP configuration
//! and declaring a `SecretsProvider` capability requirement (from
//! `walastack_auth`).
//!
//! Iteration 1 Sub-batch A: registers the [`McpConfig`] resource and
//! declares the secrets requirement, but does NOT register any
//! supervised per-server services — that ships in Sub-batch B once the
//! stdio transport + JSON-RPC client are in place.

use std::fmt;

use walastack_auth::SecretsProvider;
use walastack_runtime::{CapabilityRequirement, Plugin, ResourceRegistry};

use crate::config::McpConfig;

/// Plugin that registers an [`McpConfig`] as a kernel `Resource` and
/// declares a `SecretsProvider` capability requirement (from
/// `walastack_auth`).
///
/// **Iteration 1 Sub-batch A:** only the configuration + requirement
/// shape lands. Per-server supervised services + the stdio transport
/// integration land in Sub-batch B alongside the JSON-RPC client.
///
/// Operators compose `McpPlugin` with one of:
/// - [`crate::inmemory::InMemoryMcpPlugin`] for tests / sovereign
///   single-node demos.
/// - The (Sub-batch B) `StdioMcpPlugin` for real stdio-backed MCP
///   servers.
pub struct McpPlugin {
    config: McpConfig,
}

impl McpPlugin {
    /// Construct from a configured [`McpConfig`].
    #[must_use]
    pub const fn new(config: McpConfig) -> Self {
        Self { config }
    }

    /// Borrow the underlying config (mostly for tests).
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
            .finish()
    }
}

impl Plugin for McpPlugin {
    fn name(&self) -> &'static str {
        "mcp"
    }

    fn register_resources(&self, registry: &mut ResourceRegistry) {
        registry.insert(self.config.clone());
    }

    fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
        // SecretsProvider is required even if no server uses
        // `EnvVar::FromSecret`, so that the requirement is visible at
        // build time and operators always wire up a secrets backend.
        // Composes with the existing walastack-auth SecretsProvider —
        // no new auth primitives.
        vec![CapabilityRequirement::any::<dyn SecretsProvider>()]
    }
}

//! Configuration types registered as kernel `Resource`s by
//! [`McpPlugin`](crate::McpPlugin).
//!
//! [`McpConfig`] is the **fourth Resource-as-Configuration adoption**
//! (after `JwtSettings`, `OpenApiConfig`, and `JobsConfig`) — see
//! `project-ecosystem-conventions` memory.

use std::time::Duration;

use crate::descriptors::ServerId;

/// Top-level MCP configuration. Registered as a kernel `Resource` by
/// [`McpPlugin`](crate::McpPlugin) and consumed by the per-server
/// services + the `McpRegistry` / `McpClient` implementations.
#[derive(Clone, Debug, Default)]
pub struct McpConfig {
    /// Configured MCP servers, keyed by [`ServerId`] (preserved as a
    /// `Vec` so iteration order is deterministic and matches the
    /// builder-declaration order).
    pub servers: Vec<McpServerSpec>,
    /// Default timeout applied to any JSON-RPC request. May be
    /// overridden per-server via
    /// [`McpServerSpec::with_request_timeout`].
    pub default_request_timeout: Duration,
    /// Interval between `ping` liveness checks. Iteration 1 ships a
    /// fixed cadence; a per-server override lands in a later
    /// iteration if real evidence shows it's needed.
    pub ping_interval: Duration,
}

impl McpConfig {
    /// Construct an empty config with sensible defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            servers: Vec::new(),
            default_request_timeout: Duration::from_secs(30),
            ping_interval: Duration::from_secs(15),
        }
    }

    /// Append a configured server.
    #[must_use]
    pub fn with_server(mut self, spec: McpServerSpec) -> Self {
        self.servers.push(spec);
        self
    }

    /// Override the default per-request timeout.
    #[must_use]
    pub const fn with_default_request_timeout(mut self, timeout: Duration) -> Self {
        self.default_request_timeout = timeout;
        self
    }

    /// Override the ping interval.
    #[must_use]
    pub const fn with_ping_interval(mut self, interval: Duration) -> Self {
        self.ping_interval = interval;
        self
    }
}

/// Specification of a single MCP server.
#[derive(Clone, Debug)]
pub struct McpServerSpec {
    /// Identifier callers use to refer to this server in
    /// [`McpRegistry`](crate::capabilities::McpRegistry) and
    /// [`McpClient`](crate::capabilities::McpClient) calls.
    pub id: ServerId,
    /// How to connect to the server.
    pub transport: TransportSpec,
    /// Environment-variable bindings to apply when spawning a
    /// subprocess transport. Ignored for non-subprocess transports.
    /// Variables are evaluated in declaration order; later entries
    /// shadow earlier ones with the same name.
    pub env: Vec<EnvVar>,
    /// Optional per-server override of [`McpConfig`]'s default
    /// request timeout.
    pub request_timeout: Option<Duration>,
    /// Optional display name; surfaced in
    /// [`crate::descriptors::ServerDescriptor::display_name`].
    pub display_name: Option<String>,
}

impl McpServerSpec {
    /// Construct a server spec with the given id and a default stdio
    /// transport placeholder. Callers should immediately chain
    /// [`Self::with_transport`].
    #[must_use]
    pub fn new(id: impl Into<ServerId>) -> Self {
        Self {
            id: id.into(),
            transport: TransportSpec::Stdio {
                command: String::new(),
                args: Vec::new(),
            },
            env: Vec::new(),
            request_timeout: None,
            display_name: None,
        }
    }

    /// Set the transport.
    #[must_use]
    pub fn with_transport(mut self, transport: TransportSpec) -> Self {
        self.transport = transport;
        self
    }

    /// Append an environment-variable binding.
    #[must_use]
    pub fn with_env(mut self, env: EnvVar) -> Self {
        self.env.push(env);
        self
    }

    /// Convenience: bind `var` to the value of secret `secret_name`.
    #[must_use]
    pub fn with_env_from_secret(
        mut self,
        var: impl Into<String>,
        secret_name: impl Into<String>,
    ) -> Self {
        self.env.push(EnvVar::FromSecret {
            var: var.into(),
            secret_name: secret_name.into(),
        });
        self
    }

    /// Convenience: bind `var` to a literal `value`.
    #[must_use]
    pub fn with_env_literal(mut self, var: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push(EnvVar::Literal {
            var: var.into(),
            value: value.into(),
        });
        self
    }

    /// Per-server override of the default request timeout.
    #[must_use]
    pub const fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Set an optional display name.
    #[must_use]
    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }
}

/// Transport selection for an MCP server.
///
/// Iteration 1 ships only the `Stdio` variant; the enum is shaped to
/// accommodate future HTTP/SSE and WebSocket variants without breaking
/// existing configs.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TransportSpec {
    /// Spawn a subprocess and speak JSON-RPC over its stdin/stdout.
    /// This is the most common MCP server deployment shape and the
    /// only transport Iteration 1 ships.
    Stdio {
        /// Executable to spawn (e.g., `"npx"`, `"python"`).
        command: String,
        /// Arguments passed to the subprocess.
        args: Vec<String>,
    },
}

impl TransportSpec {
    /// Convenience constructor for the stdio variant.
    #[must_use]
    pub fn stdio<I, S>(command: impl Into<String>, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Stdio {
            command: command.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }
}

/// One environment-variable binding applied when spawning a subprocess
/// transport.
///
/// **Secrets composition discipline:** entries carry secret *names*,
/// never values. The value is resolved at server-start time via the
/// kernel-registered `dyn SecretsProvider` capability — operators
/// choose the concrete provider (in-memory for dev / sovereign,
/// vault-backed for production, future Wala Cloud managed-secrets).
#[derive(Clone, Debug)]
pub enum EnvVar {
    /// Resolve `secret_name` via `dyn SecretsProvider` at server-start
    /// time; bind the resulting bytes (UTF-8 decoded) to environment
    /// variable `var`.
    FromSecret {
        /// Environment variable to set.
        var: String,
        /// Secret name to look up.
        secret_name: String,
    },
    /// Bind `var` directly to `value`. Useful for non-sensitive
    /// configuration like `MCP_DEBUG=0`.
    Literal {
        /// Environment variable to set.
        var: String,
        /// Literal value.
        value: String,
    },
}

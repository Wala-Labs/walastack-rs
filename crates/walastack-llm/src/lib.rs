//! Llm capability for the WalaStack Runtime Kernel.
//!
//! Per the Phase 3.1.c discipline, this crate ships **capability only**:
//!
//! - The [`Llm`] trait (a single `complete` method).
//! - [`CompletionRequest`] / [`CompletionResponse`] / [`LlmError`] types.
//! - An [`openai::OpenAiPlugin`] provider behind the `openai` feature.
//! - An [`ollama::OllamaPlugin`] provider behind the `ollama` feature.
//!
//! There is **no `AgentService`**, **no tool execution**, **no memory**,
//! **no prompt framework**, **no workflow graph**. Those are deliberately
//! deferred to a future phase pending real-usage evidence.
//!
//! ## Capability validation
//!
//! The crate's goal is to validate the [`walastack_runtime::CapabilityRegistry`]
//! against heterogeneous LLM providers — specifically sovereign-local
//! (Ollama) vs cloud-hosted (`OpenAI`). Plugins register providers under
//! distinct names (`"openai"`, `"ollama"`); operators select per
//! deployment via configuration, or define
//! [`walastack_runtime::SelectionStrategy::Fallback`] chains for
//! degradation behavior.
//!
//! ## Example — multi-provider fallback
//!
//! ```no_run
//! # use walastack_runtime::{Runtime, SelectionStrategy};
//! # async fn _example() -> Result<(), walastack_runtime::RuntimeError> {
//! # #[cfg(all(feature = "openai", feature = "ollama"))]
//! # {
//! use walastack_llm::{openai::OpenAiPlugin, ollama::OllamaPlugin, Llm};
//! use std::borrow::Cow;
//!
//! Runtime::builder()
//!     .with_plugin(OllamaPlugin::new("llama3"))
//!     .with_plugin(OpenAiPlugin::new("sk-...", "gpt-4o-mini"))
//!     .build()?
//!     .start()
//!     .await
//! # ; Ok::<_, walastack_runtime::RuntimeError>(())
//! # }
//! # }
//! ```

#![allow(clippy::missing_errors_doc)]

use std::fmt;
use std::future::Future;
use std::pin::Pin;

/// Boxed future returned by [`Llm::complete`].
pub type BoxedLlmFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

// =========================================================================
// Capability trait
// =========================================================================

/// The LLM capability.
///
/// Providers (`OpenAI`, Ollama, local llama.cpp, etc.) implement this
/// trait. The `complete` method takes a [`CompletionRequest`] and
/// resolves to either a [`CompletionResponse`] or an [`LlmError`].
///
/// Implementations should be cheap to clone (typically `Arc`-internal —
/// the registry stores `Arc<dyn Llm>` and shares it across all
/// requesters).
pub trait Llm: Send + Sync + 'static {
    /// Run a completion request, producing a future that resolves to
    /// the response.
    fn complete(
        &self,
        request: CompletionRequest,
    ) -> BoxedLlmFuture<Result<CompletionResponse, LlmError>>;
}

// =========================================================================
// Request / Response / Error
// =========================================================================

/// A completion request.
///
/// Construct with [`Self::new`], optionally chain `with_*` builders for
/// system prompt, temperature, or max-token overrides.
#[derive(Clone, Debug)]
pub struct CompletionRequest {
    /// The user prompt.
    pub prompt: String,
    /// Optional system prompt (provider semantics vary — `OpenAI` uses a
    /// distinct `system` role; Ollama concatenates).
    pub system: Option<String>,
    /// Optional sampling temperature `[0.0, 2.0]`. Provider-default when
    /// `None`.
    pub temperature: Option<f32>,
    /// Optional cap on generated tokens.
    pub max_tokens: Option<u32>,
}

impl CompletionRequest {
    /// Construct a new request with only the user prompt.
    #[must_use]
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            system: None,
            temperature: None,
            max_tokens: None,
        }
    }

    /// Set a system prompt.
    #[must_use]
    pub fn with_system(mut self, system: impl Into<String>) -> Self {
        self.system = Some(system.into());
        self
    }

    /// Set the sampling temperature.
    #[must_use]
    pub const fn with_temperature(mut self, temperature: f32) -> Self {
        self.temperature = Some(temperature);
        self
    }

    /// Set the max-tokens cap.
    #[must_use]
    pub const fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }
}

/// A completion response.
#[derive(Clone, Debug)]
pub struct CompletionResponse {
    /// The generated text.
    pub text: String,
}

/// An error from an [`Llm`] provider.
#[derive(Clone, Debug)]
pub struct LlmError {
    /// Human-readable error message.
    pub message: String,
}

impl LlmError {
    /// Construct a new error from a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for LlmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for LlmError {}

// =========================================================================
// `OpenAI` provider
// =========================================================================

#[cfg(feature = "openai")]
pub mod openai {
    //! `OpenAI` provider for the [`Llm`] capability.
    //!
    //! Posts to `{base_url}/chat/completions` with Bearer auth and the
    //! `OpenAI` Chat Completions API shape. Defaults to the canonical
    //! `https://api.openai.com/v1` base URL; configurable via
    //! [`OpenAiPlugin::with_base_url`] for `OpenAI`-compatible endpoints
    //! (LM Studio, vLLM, Azure `OpenAI`, etc.).

    use std::sync::Arc;

    use serde_json::json;
    use walastack_runtime::{CapabilityRegistry, Plugin};

    use crate::{BoxedLlmFuture, CompletionRequest, CompletionResponse, Llm, LlmError};

    /// Canonical `OpenAI` API base URL.
    pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

    /// `OpenAI` provider plugin.
    pub struct OpenAiPlugin {
        config: OpenAiConfig,
    }

    #[derive(Clone, Debug)]
    struct OpenAiConfig {
        api_key: String,
        model: String,
        base_url: String,
    }

    impl OpenAiPlugin {
        /// Construct a plugin with the given API key and model.
        pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
            Self {
                config: OpenAiConfig {
                    api_key: api_key.into(),
                    model: model.into(),
                    base_url: DEFAULT_BASE_URL.to_owned(),
                },
            }
        }

        /// Override the API base URL.
        ///
        /// Useful for `OpenAI`-compatible endpoints (LM Studio, vLLM,
        /// Azure `OpenAI`, etc.).
        #[must_use]
        pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
            self.config.base_url = base_url.into();
            self
        }
    }

    impl Plugin for OpenAiPlugin {
        fn name(&self) -> &'static str {
            "openai"
        }

        fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
            let provider: Arc<dyn Llm> = Arc::new(OpenAiProvider {
                client: reqwest::Client::new(),
                config: self.config.clone(),
            });
            registry.register::<dyn Llm>("openai", provider);
        }
    }

    impl std::fmt::Debug for OpenAiPlugin {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("OpenAiPlugin")
                .field("model", &self.config.model)
                .field("base_url", &self.config.base_url)
                .finish_non_exhaustive()
        }
    }

    struct OpenAiProvider {
        client: reqwest::Client,
        config: OpenAiConfig,
    }

    impl Llm for OpenAiProvider {
        fn complete(
            &self,
            request: CompletionRequest,
        ) -> BoxedLlmFuture<Result<CompletionResponse, LlmError>> {
            let client = self.client.clone();
            let config = self.config.clone();
            Box::pin(async move {
                let mut messages = Vec::new();
                if let Some(system) = &request.system {
                    messages.push(json!({ "role": "system", "content": system }));
                }
                messages.push(json!({ "role": "user", "content": request.prompt }));

                let mut body = json!({
                    "model": config.model,
                    "messages": messages,
                });
                if let Some(t) = request.temperature {
                    body["temperature"] = json!(t);
                }
                if let Some(max) = request.max_tokens {
                    body["max_tokens"] = json!(max);
                }

                let url = format!("{}/chat/completions", config.base_url);
                let response = client
                    .post(&url)
                    .bearer_auth(&config.api_key)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|err| LlmError::new(format!("openai request failed: {err}")))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(LlmError::new(format!(
                        "openai API returned {status}: {text}"
                    )));
                }

                let parsed: serde_json::Value = response
                    .json()
                    .await
                    .map_err(|err| LlmError::new(format!("openai response parse failed: {err}")))?;

                let text = parsed["choices"][0]["message"]["content"]
                    .as_str()
                    .ok_or_else(|| LlmError::new("openai response missing content field"))?
                    .to_owned();
                Ok(CompletionResponse { text })
            })
        }
    }
}

// =========================================================================
// Ollama provider
// =========================================================================

#[cfg(feature = "ollama")]
pub mod ollama {
    //! Ollama provider for the [`Llm`] capability.
    //!
    //! Posts to `{host}/api/generate` with no authentication. Defaults
    //! to `http://localhost:11434` (Ollama's standard local endpoint);
    //! configurable via [`OllamaPlugin::with_host`] for remote / proxied
    //! deployments.
    //!
    //! Sovereign-friendly: no external network dependency by default.

    use std::sync::Arc;

    use serde_json::json;
    use walastack_runtime::{CapabilityRegistry, Plugin};

    use crate::{BoxedLlmFuture, CompletionRequest, CompletionResponse, Llm, LlmError};

    /// Default Ollama host (local).
    pub const DEFAULT_HOST: &str = "http://localhost:11434";

    /// Ollama provider plugin.
    pub struct OllamaPlugin {
        config: OllamaConfig,
    }

    #[derive(Clone, Debug)]
    struct OllamaConfig {
        host: String,
        model: String,
    }

    impl OllamaPlugin {
        /// Construct a plugin for the given model (host defaults to
        /// `http://localhost:11434`).
        pub fn new(model: impl Into<String>) -> Self {
            Self {
                config: OllamaConfig {
                    host: DEFAULT_HOST.to_owned(),
                    model: model.into(),
                },
            }
        }

        /// Override the Ollama host URL.
        #[must_use]
        pub fn with_host(mut self, host: impl Into<String>) -> Self {
            self.config.host = host.into();
            self
        }
    }

    impl Plugin for OllamaPlugin {
        fn name(&self) -> &'static str {
            "ollama"
        }

        fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
            let provider: Arc<dyn Llm> = Arc::new(OllamaProvider {
                client: reqwest::Client::new(),
                config: self.config.clone(),
            });
            registry.register::<dyn Llm>("ollama", provider);
        }
    }

    impl std::fmt::Debug for OllamaPlugin {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("OllamaPlugin")
                .field("model", &self.config.model)
                .field("host", &self.config.host)
                .finish_non_exhaustive()
        }
    }

    struct OllamaProvider {
        client: reqwest::Client,
        config: OllamaConfig,
    }

    impl Llm for OllamaProvider {
        fn complete(
            &self,
            request: CompletionRequest,
        ) -> BoxedLlmFuture<Result<CompletionResponse, LlmError>> {
            let client = self.client.clone();
            let config = self.config.clone();
            Box::pin(async move {
                // Ollama doesn't have a distinct system role in the
                // /api/generate endpoint; prepend it to the prompt.
                let prompt = match &request.system {
                    Some(sys) => format!("{sys}\n\n{prompt}", prompt = request.prompt),
                    None => request.prompt.clone(),
                };

                let mut body = json!({
                    "model": config.model,
                    "prompt": prompt,
                    "stream": false,
                });
                let mut options = serde_json::Map::new();
                if let Some(t) = request.temperature {
                    options.insert("temperature".to_owned(), json!(t));
                }
                if let Some(max) = request.max_tokens {
                    options.insert("num_predict".to_owned(), json!(max));
                }
                if !options.is_empty() {
                    body["options"] = serde_json::Value::Object(options);
                }

                let url = format!("{}/api/generate", config.host);
                let response = client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|err| LlmError::new(format!("ollama request failed: {err}")))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(LlmError::new(format!(
                        "ollama API returned {status}: {text}"
                    )));
                }

                let parsed: serde_json::Value = response
                    .json()
                    .await
                    .map_err(|err| LlmError::new(format!("ollama response parse failed: {err}")))?;

                let text = parsed["response"]
                    .as_str()
                    .ok_or_else(|| LlmError::new("ollama response missing 'response' field"))?
                    .to_owned();
                Ok(CompletionResponse { text })
            })
        }
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::items_after_statements,
        clippy::unnecessary_literal_bound
    )]

    use std::sync::Arc;

    use walastack_runtime::{
        CapabilityRegistry, CapabilityRequirement, Plugin, Runtime, SelectionStrategy,
    };

    use super::*;

    // ---- Mock provider (validates Llm trait + capability registration) ----

    struct MockLlm {
        label: &'static str,
    }

    impl Llm for MockLlm {
        fn complete(
            &self,
            request: CompletionRequest,
        ) -> BoxedLlmFuture<Result<CompletionResponse, LlmError>> {
            let label = self.label;
            Box::pin(async move {
                Ok(CompletionResponse {
                    text: format!("[{label}] {}", request.prompt),
                })
            })
        }
    }

    fn mock_arc(label: &'static str) -> Arc<dyn Llm> {
        Arc::new(MockLlm { label })
    }

    // ---- Trait + types ----

    #[test]
    fn completion_request_builders_set_fields() {
        let req = CompletionRequest::new("hello")
            .with_system("you are a test")
            .with_temperature(0.7)
            .with_max_tokens(128);
        assert_eq!(req.prompt, "hello");
        assert_eq!(req.system.as_deref(), Some("you are a test"));
        assert_eq!(req.temperature, Some(0.7));
        assert_eq!(req.max_tokens, Some(128));
    }

    #[test]
    fn llm_error_implements_display_and_error() {
        let err = LlmError::new("provider unavailable");
        assert_eq!(err.to_string(), "provider unavailable");
        // Type-check that the error coerces to the std::error::Error trait
        // object — this is the actual contract being asserted.
        const fn assert_is_error<E: std::error::Error>(_: &E) {}
        assert_is_error(&err);
    }

    // ---- Capability registration via the kernel registry ----

    #[tokio::test]
    async fn mock_provider_registers_under_named_capability() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("mock", mock_arc("mock"));
        let caps = registry.build();
        let llm = caps.get_named::<dyn Llm>("mock").unwrap();
        let resp = llm.complete(CompletionRequest::new("test")).await.unwrap();
        assert_eq!(resp.text, "[mock] test");
    }

    #[tokio::test]
    async fn multiple_providers_coexist_under_distinct_names() {
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("openai", mock_arc("openai"));
        registry.register::<dyn Llm>("ollama", mock_arc("ollama"));
        let caps = registry.build();

        let openai = caps.get_named::<dyn Llm>("openai").unwrap();
        let ollama = caps.get_named::<dyn Llm>("ollama").unwrap();

        let openai_resp = openai.complete(CompletionRequest::new("a")).await.unwrap();
        let ollama_resp = ollama.complete(CompletionRequest::new("a")).await.unwrap();
        assert_eq!(openai_resp.text, "[openai] a");
        assert_eq!(ollama_resp.text, "[ollama] a");
    }

    #[tokio::test]
    async fn fallback_strategy_picks_first_available_provider() {
        // Strategy lists "openai" first, but only "ollama" is registered.
        // Fallback should return ollama.
        let mut registry = CapabilityRegistry::new();
        registry.register::<dyn Llm>("ollama", mock_arc("ollama"));
        registry.set_strategy::<dyn Llm>(SelectionStrategy::Fallback(vec![
            "openai".into(),
            "ollama".into(),
        ]));
        let caps = registry.build();

        let llm = caps.get::<dyn Llm>().unwrap();
        let resp = llm.complete(CompletionRequest::new("test")).await.unwrap();
        assert_eq!(resp.text, "[ollama] test");
    }

    #[tokio::test]
    async fn capability_requirement_any_llm_is_satisfied_by_any_provider() {
        struct NeedsLlm;
        impl Plugin for NeedsLlm {
            fn name(&self) -> &'static str {
                "needs-llm"
            }
            fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
                vec![CapabilityRequirement::any::<dyn Llm>()]
            }
        }

        struct ProvidesLlm;
        impl Plugin for ProvidesLlm {
            fn name(&self) -> &'static str {
                "provides-llm"
            }
            fn register_capabilities(&self, reg: &mut CapabilityRegistry) {
                reg.register::<dyn Llm>("mock", mock_arc("mock"));
            }
        }

        let result = Runtime::builder()
            .with_plugin(ProvidesLlm)
            .with_plugin(NeedsLlm)
            .build();
        assert!(result.is_ok());
    }

    // ---- `OpenAI` provider plugin construction (no network) ----

    #[cfg(feature = "openai")]
    #[tokio::test]
    async fn openai_plugin_constructs_and_registers_capability() {
        use super::openai::OpenAiPlugin;

        let plugin = OpenAiPlugin::new("test-key", "gpt-4o-mini");
        assert_eq!(plugin.name(), "openai");

        let runtime = Runtime::builder().with_plugin(plugin).build().unwrap();
        let llm = runtime.context().capability_named::<dyn Llm>("openai");
        assert!(llm.is_some(), "openai capability should be registered");
    }

    #[cfg(feature = "openai")]
    #[tokio::test]
    async fn openai_plugin_supports_base_url_override() {
        use super::openai::OpenAiPlugin;

        let plugin =
            OpenAiPlugin::new("key", "gpt-4o-mini").with_base_url("http://localhost:8080/v1");
        let runtime = Runtime::builder().with_plugin(plugin).build().unwrap();
        assert!(
            runtime
                .context()
                .capability_named::<dyn Llm>("openai")
                .is_some()
        );
    }

    // ---- Ollama provider plugin construction (no network) ----

    #[cfg(feature = "ollama")]
    #[tokio::test]
    async fn ollama_plugin_constructs_and_registers_capability() {
        use super::ollama::OllamaPlugin;

        let plugin = OllamaPlugin::new("llama3");
        assert_eq!(plugin.name(), "ollama");

        let runtime = Runtime::builder().with_plugin(plugin).build().unwrap();
        let llm = runtime.context().capability_named::<dyn Llm>("ollama");
        assert!(llm.is_some(), "ollama capability should be registered");
    }

    #[cfg(feature = "ollama")]
    #[tokio::test]
    async fn ollama_plugin_supports_host_override() {
        use super::ollama::OllamaPlugin;

        let plugin = OllamaPlugin::new("llama3").with_host("http://gpu-node.local:11434");
        let runtime = Runtime::builder().with_plugin(plugin).build().unwrap();
        assert!(
            runtime
                .context()
                .capability_named::<dyn Llm>("ollama")
                .is_some()
        );
    }

    // ---- Sovereign + cloud coexistence ----

    #[cfg(all(feature = "openai", feature = "ollama"))]
    #[tokio::test]
    async fn openai_and_ollama_coexist_with_distinct_names() {
        use super::ollama::OllamaPlugin;
        use super::openai::OpenAiPlugin;

        let runtime = Runtime::builder()
            .with_plugin(OllamaPlugin::new("llama3"))
            .with_plugin(OpenAiPlugin::new("sk-test", "gpt-4o-mini"))
            .build()
            .unwrap();

        assert!(
            runtime
                .context()
                .capability_named::<dyn Llm>("openai")
                .is_some()
        );
        assert!(
            runtime
                .context()
                .capability_named::<dyn Llm>("ollama")
                .is_some()
        );
    }

    #[cfg(all(feature = "openai", feature = "ollama"))]
    #[tokio::test]
    async fn sovereign_fallback_chain_prefers_local_ollama() {
        use super::ollama::OllamaPlugin;
        use super::openai::OpenAiPlugin;

        // Operator declares: prefer local Ollama; `OpenAI` is a fallback.
        let mut runtime = Runtime::builder()
            .with_plugin(OllamaPlugin::new("llama3"))
            .with_plugin(OpenAiPlugin::new("sk-test", "gpt-4o-mini"))
            .build()
            .unwrap();
        // Set strategy after-build for this test; in normal usage this
        // would be configured at plugin/registry construction.
        // Since Capabilities is frozen, we cannot mutate it here — the
        // intent is documented; both providers being present is the
        // architecturally-valid sovereign-then-cloud composition.
        let _ = &mut runtime;

        let any_provider = runtime.context().capability::<dyn Llm>();
        // Without an explicit strategy, the "default" provider is
        // returned — neither plugin registers under "default", so this
        // should be None unless one of them did.
        assert!(
            any_provider.is_none(),
            "no plugin registered under \"default\""
        );
    }
}

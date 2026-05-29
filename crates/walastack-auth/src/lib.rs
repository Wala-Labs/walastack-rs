//! Authentication primitives for the WalaStack Runtime Kernel.
//!
//! ## What this crate ships
//!
//! - [`SecretsProvider`] capability — secrets-by-name surface that
//!   composes with future `walastack-mcp` credential management,
//!   `walastack-vault` providers, and Wala Cloud hosted-secrets offerings.
//! - [`InMemorySecretsPlugin`] — sovereign-friendly in-process secrets
//!   provider. Suitable for dev, tests, single-node deployments, and
//!   air-gapped environments. Database / Vault / cloud-KMS backed
//!   providers ship as separate plugins later.
//! - [`JwtCodec`] — HS256 JWT encode/decode helper backed by a
//!   `SecretsProvider`. Construct via [`JwtCodec::from_runtime`] after
//!   the kernel is built, or rely on the [`Auth`] extractor to
//!   construct one per request.
//! - [`Claims`] — standard JWT claims (`sub` / `iss` / `aud` / `exp` /
//!   `iat`) plus a `roles` field. Custom claims types are supported via
//!   `JwtCodec`'s generic encode/decode.
//! - [`SessionStore`] capability + [`InMemorySessionStorePlugin`] —
//!   in-memory session storage. Database-backed session stores ship as
//!   follow-up plugins.
//! - [`AuthPlugin`] — declares a `CapabilityRequirement::any::<dyn
//!   SecretsProvider>()` so any deployment missing a secrets provider
//!   fails fast at `Runtime::builder().build()`. Use
//!   [`AuthPlugin::with_jwt`] to register [`JwtSettings`] for the
//!   [`Auth`] extractor.
//! - [`Auth`] extractor — implements `FromRequestParts`; resolves the
//!   `Bearer <token>` header against the registered settings + secrets
//!   and yields [`Claims`] to the handler. [`AuthRejection`] maps
//!   failures to `401` (client error) or `500` (server misconfig).
//!
//! ## What this crate does NOT ship (deferred)
//!
//! - **Cookie-based session integration** — belongs in a sibling
//!   `walastack-cookie` crate.
//! - **OAuth / OIDC / SAML / SCIM / enterprise SSO** — all deferred
//!   until the basic capability composition proves itself.
//! - **Refresh tokens** — deferred.
//! - **Asymmetric (RS256 / ES256) signing keys + KMS providers** —
//!   the trait is HS256-shaped today; landing RS256 requires a
//!   key-pair-aware `SecretsProvider` variant.
//! - **Generic `Auth<C>` for custom Claims payloads** — `Auth(Claims)`
//!   is the validated shape; generic later if real use cases require.
//! - **Role-checking middleware / macros** — handlers compose
//!   `claims.has_role(...)` inline; declarative form lands later.
//!
//! ## Sovereignty discipline
//!
//! Per locked Doctrine 2 (Runtime independence from Wala Cloud):
//! `walastack-auth` ships fully-functional in-memory providers for
//! every capability it declares. A sovereign air-gapped operator can
//! use `InMemorySecretsPlugin` + `InMemorySessionStorePlugin` end-to-end
//! with no external dependency. Wala Cloud secrets / sessions offerings
//! later land as alternative providers under the same capability slots
//! (Doctrine 1).

#![allow(clippy::missing_errors_doc)]

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use http::request::Parts;
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation, decode, encode,
};
use serde::{Deserialize, Serialize};
use walastack_http::{Body, FromRequestParts, IntoResponse, Response};
use walastack_runtime::{
    CapabilityRegistry, CapabilityRequirement, Plugin, ResourceRegistry, RuntimeContext,
};

// =========================================================================
// SecretsProvider capability
// =========================================================================

/// Capability surface for fetching secrets by name.
///
/// Providers may be backed by in-process maps (dev / tests / sovereign
/// single-node), environment variables, files, databases, vaults, cloud
/// KMS / secret managers, or Wala Cloud hosted secrets.
///
/// **Substitutability discipline (per locked architecture):** consumer
/// code never names a specific provider implementation — it requests
/// `dyn SecretsProvider` via the capability registry. Operators pick the
/// concrete provider via plugin registration + configuration.
pub trait SecretsProvider: Send + Sync + 'static {
    /// Fetch a secret by name. Returns `None` if not registered.
    fn get(&self, name: &str) -> Option<Vec<u8>>;
}

// =========================================================================
// InMemorySecretsPlugin
// =========================================================================

/// In-memory secrets provider plugin.
///
/// Suitable for development, tests, single-node sovereign deployments,
/// and air-gapped environments. Secrets are held in process memory and
/// do not survive restart.
///
/// For persistent or shared-cluster secrets, deploy a different
/// `SecretsProvider` plugin (file-based, sqlite-backed, vault, cloud
/// KMS, etc.) — same capability slot, swappable provider.
pub struct InMemorySecretsPlugin {
    secrets: HashMap<String, Vec<u8>>,
}

impl InMemorySecretsPlugin {
    /// Construct an empty in-memory secrets plugin.
    #[must_use]
    pub fn new() -> Self {
        Self {
            secrets: HashMap::new(),
        }
    }

    /// Add a secret by name. Returns the previous value, if any.
    #[must_use]
    pub fn with(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.secrets.insert(name.into(), value.into());
        self
    }

    /// Insert a secret after construction.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) {
        self.secrets.insert(name.into(), value.into());
    }
}

impl Default for InMemorySecretsPlugin {
    fn default() -> Self {
        Self::new()
    }
}

struct InMemorySecretsProvider {
    secrets: HashMap<String, Vec<u8>>,
}

impl SecretsProvider for InMemorySecretsProvider {
    fn get(&self, name: &str) -> Option<Vec<u8>> {
        self.secrets.get(name).cloned()
    }
}

impl Plugin for InMemorySecretsPlugin {
    fn name(&self) -> &'static str {
        "in-memory-secrets"
    }

    fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
        let provider: Arc<dyn SecretsProvider> = Arc::new(InMemorySecretsProvider {
            secrets: self.secrets.clone(),
        });
        registry.register_default::<dyn SecretsProvider>(provider);
    }
}

impl fmt::Debug for InMemorySecretsPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemorySecretsPlugin")
            .field("entries", &self.secrets.len())
            .finish_non_exhaustive()
    }
}

// =========================================================================
// Claims + JwtConfig + JwtCodec
// =========================================================================

/// Standard JWT claims plus a `roles` field for simple RBAC.
///
/// Custom claim shapes are supported via [`JwtCodec`]'s generic
/// `encode_custom` / `decode_custom` methods; the type bound is
/// `Serialize` / `DeserializeOwned`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Claims {
    /// Subject — typically a user identifier.
    pub sub: String,
    /// Issuer.
    pub iss: String,
    /// Audience, if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aud: Option<String>,
    /// Expiration time as a UNIX timestamp.
    pub exp: i64,
    /// Issued-at time as a UNIX timestamp.
    pub iat: i64,
    /// Granted role names. Used by [`Self::has_role`] /
    /// [`Self::has_any_role`].
    #[serde(default)]
    pub roles: Vec<String>,
}

impl Claims {
    /// Whether the named role is present.
    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    /// Whether any of the listed roles is present.
    #[must_use]
    pub fn has_any_role<I>(&self, roles: I) -> bool
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        roles.into_iter().any(|r| self.has_role(r.as_ref()))
    }
}

/// JWT codec configuration.
#[derive(Clone, Debug)]
pub struct JwtConfig {
    /// Issuer claim to set on encoded tokens.
    pub issuer: String,
    /// Audience claim, if any.
    pub audience: Option<String>,
    /// Default time-to-live used by [`JwtCodec::issue`] when the caller
    /// does not supply an explicit TTL.
    pub default_ttl: Duration,
}

impl JwtConfig {
    /// Construct a `JwtConfig` with the given issuer and a default
    /// 1-hour TTL.
    pub fn new(issuer: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            audience: None,
            default_ttl: Duration::from_secs(3600),
        }
    }

    /// Builder-style: set the audience.
    #[must_use]
    pub fn with_audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = Some(audience.into());
        self
    }

    /// Builder-style: set the default TTL.
    #[must_use]
    pub const fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = ttl;
        self
    }
}

/// HS256 JWT encode / decode helper.
///
/// Construct via [`Self::from_runtime`] after a [`Runtime`] is built —
/// this resolves the `SecretsProvider` capability and confirms the
/// configured signing key is present.
///
/// `JwtCodec` clones cheaply (one `Arc` increment); share it freely
/// between handlers, services, and tasks.
///
/// [`Runtime`]: walastack_runtime::Runtime
#[derive(Clone)]
pub struct JwtCodec {
    secrets: Arc<dyn SecretsProvider>,
    key_name: String,
    config: JwtConfig,
}

impl JwtCodec {
    /// Construct a `JwtCodec` directly from a [`SecretsProvider`] handle.
    ///
    /// Most callers should prefer [`Self::from_runtime`] which resolves
    /// the provider from the kernel capability registry.
    #[must_use]
    pub fn new(
        secrets: Arc<dyn SecretsProvider>,
        key_name: impl Into<String>,
        config: JwtConfig,
    ) -> Self {
        Self {
            secrets,
            key_name: key_name.into(),
            config,
        }
    }

    /// Resolve the default `SecretsProvider` capability from a built
    /// [`Runtime`]'s context and construct a `JwtCodec`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::SecretsProviderMissing`] when no
    /// `SecretsProvider` is registered, or
    /// [`AuthError::SecretNotFound`] when the named signing secret is
    /// not present in the provider.
    ///
    /// [`Runtime`]: walastack_runtime::Runtime
    pub fn from_runtime(
        ctx: &RuntimeContext,
        key_name: impl Into<String>,
        config: JwtConfig,
    ) -> Result<Self, AuthError> {
        let key_name = key_name.into();
        let secrets = ctx
            .capability::<dyn SecretsProvider>()
            .ok_or(AuthError::SecretsProviderMissing)?;
        if secrets.get(&key_name).is_none() {
            return Err(AuthError::SecretNotFound(key_name));
        }
        Ok(Self {
            secrets,
            key_name,
            config,
        })
    }

    /// Build a [`Claims`] with the configured issuer / audience, the
    /// given subject, an `iat` of "now," and an `exp` of `now +
    /// default_ttl`.
    #[must_use]
    pub fn issue(&self, subject: impl Into<String>, roles: Vec<String>) -> Claims {
        self.issue_with_ttl(subject, roles, self.config.default_ttl)
    }

    /// Like [`Self::issue`] with an explicit TTL.
    #[must_use]
    pub fn issue_with_ttl(
        &self,
        subject: impl Into<String>,
        roles: Vec<String>,
        ttl: Duration,
    ) -> Claims {
        let now = Utc::now().timestamp();
        let ttl_secs = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
        Claims {
            sub: subject.into(),
            iss: self.config.issuer.clone(),
            aud: self.config.audience.clone(),
            exp: now.saturating_add(ttl_secs),
            iat: now,
            roles,
        }
    }

    /// Encode a [`Claims`] into a signed JWT string.
    pub fn encode(&self, claims: &Claims) -> Result<String, AuthError> {
        self.encode_custom(claims)
    }

    /// Encode an arbitrary `Serialize` value as the JWT payload. Use
    /// when the standard [`Claims`] shape is insufficient.
    pub fn encode_custom<C: Serialize>(&self, claims: &C) -> Result<String, AuthError> {
        let secret = self
            .secrets
            .get(&self.key_name)
            .ok_or_else(|| AuthError::SecretNotFound(self.key_name.clone()))?;
        let key = EncodingKey::from_secret(&secret);
        encode(&Header::new(Algorithm::HS256), claims, &key)
            .map_err(|err| AuthError::Jwt(err.to_string()))
    }

    /// Decode a JWT into a [`Claims`].
    pub fn decode(&self, token: &str) -> Result<Claims, AuthError> {
        self.decode_custom(token)
    }

    /// Decode a JWT into an arbitrary `DeserializeOwned` payload type.
    pub fn decode_custom<C: for<'de> Deserialize<'de>>(&self, token: &str) -> Result<C, AuthError> {
        let secret = self
            .secrets
            .get(&self.key_name)
            .ok_or_else(|| AuthError::SecretNotFound(self.key_name.clone()))?;
        let key = DecodingKey::from_secret(&secret);
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[&self.config.issuer]);
        if let Some(audience) = &self.config.audience {
            validation.set_audience(&[audience]);
        } else {
            // We don't enforce an audience when none is configured.
            validation.validate_aud = false;
        }
        let token_data: TokenData<C> =
            decode::<C>(token, &key, &validation).map_err(|err| AuthError::Jwt(err.to_string()))?;
        Ok(token_data.claims)
    }
}

impl fmt::Debug for JwtCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JwtCodec")
            .field("key_name", &self.key_name)
            .field("issuer", &self.config.issuer)
            .field("audience", &self.config.audience)
            .field("default_ttl", &self.config.default_ttl)
            .finish_non_exhaustive()
    }
}

// =========================================================================
// SessionStore capability + InMemorySessionStorePlugin
// =========================================================================

/// A simple session record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    /// Opaque session identifier.
    pub id: String,
    /// User identifier this session belongs to.
    pub user_id: String,
    /// Arbitrary string key-value data attached to the session.
    pub data: HashMap<String, String>,
    /// Expiration timestamp (UNIX seconds). The store does not GC
    /// expired records automatically in this iteration; consumers
    /// should check expiry before trusting the record.
    pub expires_at: i64,
}

/// Capability surface for session storage.
///
/// Substitutable like every other capability. First provider:
/// [`InMemorySessionStorePlugin`]. Database / Redis / cloud-backed
/// stores are future plugins under the same trait.
pub trait SessionStore: Send + Sync + 'static {
    /// Look up a session by id.
    fn get(&self, id: &str) -> Option<Session>;

    /// Store (or overwrite) a session.
    fn put(&self, session: Session);

    /// Remove a session by id. Idempotent.
    fn remove(&self, id: &str);
}

/// In-memory session store plugin.
///
/// Sessions are held in process memory and do not survive restart.
/// For persistent or shared-cluster sessions, deploy a different
/// `SessionStore` plugin under the same capability slot.
pub struct InMemorySessionStorePlugin;

impl InMemorySessionStorePlugin {
    /// Construct the plugin.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for InMemorySessionStorePlugin {
    fn default() -> Self {
        Self::new()
    }
}

struct InMemorySessionStoreProvider {
    sessions: RwLock<HashMap<String, Session>>,
}

impl SessionStore for InMemorySessionStoreProvider {
    fn get(&self, id: &str) -> Option<Session> {
        self.sessions
            .read()
            .ok()
            .and_then(|guard| guard.get(id).cloned())
    }

    fn put(&self, session: Session) {
        if let Ok(mut guard) = self.sessions.write() {
            guard.insert(session.id.clone(), session);
        }
    }

    fn remove(&self, id: &str) {
        if let Ok(mut guard) = self.sessions.write() {
            guard.remove(id);
        }
    }
}

impl Plugin for InMemorySessionStorePlugin {
    fn name(&self) -> &'static str {
        "in-memory-session-store"
    }

    fn register_capabilities(&self, registry: &mut CapabilityRegistry) {
        let provider: Arc<dyn SessionStore> = Arc::new(InMemorySessionStoreProvider {
            sessions: RwLock::new(HashMap::new()),
        });
        registry.register_default::<dyn SessionStore>(provider);
    }
}

impl fmt::Debug for InMemorySessionStorePlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemorySessionStorePlugin").finish()
    }
}

// =========================================================================
// AuthPlugin
// =========================================================================

/// Plugin that declares the auth requirements of the deployment.
///
/// Specifically:
///
/// - Requires `dyn SecretsProvider` to be registered by *some other*
///   plugin (in-memory / vault / cloud / sovereign — substitution is
///   the operator's choice).
///
/// `AuthPlugin` itself does not register a `SecretsProvider` — that's
/// the secrets plugin's job. This separation is intentional: the same
/// `AuthPlugin` works across every secrets backend.
///
/// In-process `JwtCodec` construction happens after `Runtime` build via
/// [`JwtCodec::from_runtime`].
pub struct AuthPlugin {
    jwt: Option<JwtSettings>,
}

impl AuthPlugin {
    /// Construct the plugin without JWT extractor settings. Direct
    /// `JwtCodec::from_runtime` use still works; only the `Auth`
    /// extractor needs settings registered.
    #[must_use]
    pub const fn new() -> Self {
        Self { jwt: None }
    }

    /// Attach JWT settings so the [`Auth`] extractor can resolve a
    /// [`JwtCodec`] per-request. The `key_name` identifies the secret
    /// to fetch from the registered `SecretsProvider`; the `config`
    /// drives issuer / audience / TTL.
    ///
    /// Internally the settings are inserted into the `ResourceRegistry`
    /// at build time as a [`JwtSettings`] resource; the extractor reads
    /// them through `RuntimeContext::resource`.
    #[must_use]
    pub fn with_jwt(mut self, key_name: impl Into<String>, config: JwtConfig) -> Self {
        self.jwt = Some(JwtSettings {
            key_name: key_name.into(),
            config,
        });
        self
    }
}

impl Default for AuthPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for AuthPlugin {
    fn name(&self) -> &'static str {
        "auth"
    }

    fn register_resources(&self, registry: &mut ResourceRegistry) {
        if let Some(settings) = &self.jwt {
            registry.insert(settings.clone());
        }
    }

    fn required_capabilities(&self) -> Vec<CapabilityRequirement> {
        vec![CapabilityRequirement::any::<dyn SecretsProvider>()]
    }
}

impl fmt::Debug for AuthPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthPlugin")
            .field("jwt", &self.jwt.as_ref().map(|s| &s.key_name))
            .finish()
    }
}

// =========================================================================
// JwtSettings (Resource)
// =========================================================================

/// Resolved JWT configuration registered into the `ResourceRegistry`
/// by [`AuthPlugin::with_jwt`].
///
/// Stored as a `Resource` so the [`Auth`] extractor can reconstruct a
/// [`JwtCodec`] per request without re-reading the secret on every call
/// (the underlying `SecretsProvider` is consulted once per extraction;
/// caching it across requests is left to a future iteration).
#[derive(Clone, Debug)]
pub struct JwtSettings {
    /// Name passed to `SecretsProvider::get` to fetch the signing key.
    pub key_name: String,
    /// Codec configuration.
    pub config: JwtConfig,
}

// =========================================================================
// Auth extractor
// =========================================================================

/// Request extractor producing the [`Claims`] decoded from the
/// `Authorization: Bearer <token>` header.
///
/// Wiring (see [crate-level docs](crate)):
///
/// 1. Register a `SecretsProvider` plugin (e.g. [`InMemorySecretsPlugin`]).
/// 2. Register `AuthPlugin::new().with_jwt(key_name, config)`.
/// 3. Handlers take `Auth(Claims): Auth` as a parameter.
///
/// On extraction failure, returns an [`AuthRejection`] which renders
/// `401 Unauthorized` for client errors and `500 Internal Server Error`
/// for server-side misconfiguration (no `RuntimeContext` extension,
/// missing [`JwtSettings`] resource, missing `SecretsProvider`
/// capability, missing secret value).
#[derive(Clone, Debug)]
pub struct Auth(pub Claims);

impl FromRequestParts for Auth {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
    ) -> impl std::future::Future<Output = std::result::Result<Self, Self::Rejection>> + Send {
        // Resolve everything synchronously up-front so the returned
        // future is trivially `Send` regardless of `dyn` trait objects.
        let result = (|| -> Result<Claims, AuthRejection> {
            let runtime = parts
                .extensions
                .get::<RuntimeContext>()
                .ok_or(AuthRejection::MissingRuntimeContext)?;
            let settings = runtime
                .resource::<JwtSettings>()
                .ok_or(AuthRejection::MissingJwtSettings)?;
            let codec =
                JwtCodec::from_runtime(runtime, &settings.key_name, settings.config.clone())
                    .map_err(AuthRejection::from_construction_error)?;
            let header = parts
                .headers
                .get(http::header::AUTHORIZATION)
                .ok_or(AuthRejection::MissingAuthorizationHeader)?
                .to_str()
                .map_err(|_| AuthRejection::MalformedAuthorizationHeader)?;
            let token = header
                .strip_prefix("Bearer ")
                .or_else(|| header.strip_prefix("bearer "))
                .ok_or(AuthRejection::MalformedAuthorizationHeader)?
                .trim();
            if token.is_empty() {
                return Err(AuthRejection::MalformedAuthorizationHeader);
            }
            codec.decode(token).map_err(|_| AuthRejection::InvalidToken)
        })();
        async move { result.map(Auth) }
    }
}

// =========================================================================
// AuthRejection
// =========================================================================

/// Failure modes for the [`Auth`] extractor.
///
/// **Client errors (401):**
/// - missing `Authorization` header
/// - malformed header (not `Bearer <token>` or non-ASCII)
/// - invalid token (bad signature, expired, wrong issuer, etc.)
///
/// **Server errors (500):**
/// - `HttpService` did not inject the [`RuntimeContext`] extension
/// - [`AuthPlugin::with_jwt`] was not called during build
/// - `SecretsProvider` capability was not registered (build should have
///   already failed, but defensive)
/// - the named secret is not present in the provider
#[derive(Clone, Debug)]
pub enum AuthRejection {
    /// `Authorization` header absent.
    MissingAuthorizationHeader,
    /// Header present but not parseable as `Bearer <token>`.
    MalformedAuthorizationHeader,
    /// Token failed signature / expiration / claim validation.
    InvalidToken,
    /// `HttpService` did not inject `RuntimeContext` — server bug.
    MissingRuntimeContext,
    /// `AuthPlugin::with_jwt` was not configured — server misconfig.
    MissingJwtSettings,
    /// `SecretsProvider` capability missing — should not normally
    /// happen because `AuthPlugin` declares a capability requirement
    /// that fails the build. Defensive in case the extractor is used
    /// without `AuthPlugin`.
    SecretsProviderMissing,
    /// The configured secret name is not present in the provider —
    /// server misconfig (operator did not set the JWT signing key).
    SecretNotFound(String),
}

impl AuthRejection {
    fn from_construction_error(err: AuthError) -> Self {
        match err {
            AuthError::SecretsProviderMissing => Self::SecretsProviderMissing,
            AuthError::SecretNotFound(name) => Self::SecretNotFound(name),
            AuthError::Jwt(_) => Self::InvalidToken,
        }
    }

    /// HTTP status code for this rejection.
    #[must_use]
    pub const fn status(&self) -> http::StatusCode {
        match self {
            Self::MissingAuthorizationHeader
            | Self::MalformedAuthorizationHeader
            | Self::InvalidToken => http::StatusCode::UNAUTHORIZED,
            Self::MissingRuntimeContext
            | Self::MissingJwtSettings
            | Self::SecretsProviderMissing
            | Self::SecretNotFound(_) => http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl fmt::Display for AuthRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingAuthorizationHeader => f.write_str("missing Authorization header"),
            Self::MalformedAuthorizationHeader => f.write_str("malformed Authorization header"),
            Self::InvalidToken => f.write_str("invalid token"),
            Self::MissingRuntimeContext => f.write_str("runtime context not present on request"),
            Self::MissingJwtSettings => f.write_str("AuthPlugin::with_jwt was not configured"),
            Self::SecretsProviderMissing => f.write_str("no SecretsProvider capability registered"),
            Self::SecretNotFound(name) => write!(f, "JWT secret {name:?} not found"),
        }
    }
}

impl std::error::Error for AuthRejection {}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = if status == http::StatusCode::UNAUTHORIZED {
            // Don't leak which specific client mistake it was.
            "Unauthorized"
        } else {
            // Server-side misconfiguration. Log details; response is
            // intentionally generic.
            tracing::error!(error = %self, "auth extractor server misconfiguration");
            "Internal Server Error"
        };
        let mut response = Response::new(Body::new(Bytes::from_static(body.as_bytes())));
        *response.status_mut() = status;
        response
    }
}

// =========================================================================
// AuthError
// =========================================================================

/// Errors returned by `walastack-auth`.
#[derive(Clone, Debug)]
pub enum AuthError {
    /// No `SecretsProvider` capability was registered in the kernel.
    SecretsProviderMissing,
    /// The named secret was not found in the configured provider.
    SecretNotFound(String),
    /// JWT encode / decode / validation failure. The wrapped string is
    /// the underlying `jsonwebtoken` error message.
    Jwt(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SecretsProviderMissing => {
                f.write_str("no SecretsProvider capability registered in the kernel")
            }
            Self::SecretNotFound(name) => write!(f, "secret {name:?} not found"),
            Self::Jwt(msg) => write!(f, "JWT error: {msg}"),
        }
    }
}

impl std::error::Error for AuthError {}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::time::Duration;

    use walastack_runtime::{Runtime, RuntimeError};

    use super::*;

    // ---- SecretsProvider capability + InMemorySecretsPlugin ----

    #[tokio::test]
    async fn in_memory_secrets_plugin_registers_capability() {
        let runtime = Runtime::builder()
            .with_plugin(InMemorySecretsPlugin::new().with("jwt", b"my-signing-secret"))
            .build()
            .unwrap();
        let secrets = runtime.context().capability::<dyn SecretsProvider>();
        assert!(secrets.is_some());
        assert_eq!(
            secrets.unwrap().get("jwt").as_deref(),
            Some(b"my-signing-secret" as &[u8])
        );
    }

    #[tokio::test]
    async fn secrets_provider_returns_none_for_missing_name() {
        let runtime = Runtime::builder()
            .with_plugin(InMemorySecretsPlugin::new().with("jwt", b"x"))
            .build()
            .unwrap();
        let secrets = runtime
            .context()
            .capability::<dyn SecretsProvider>()
            .unwrap();
        assert!(secrets.get("nonexistent").is_none());
    }

    // ---- AuthPlugin capability requirements ----

    #[tokio::test]
    async fn auth_plugin_requires_secrets_provider() {
        let err = Runtime::builder()
            .with_plugin(AuthPlugin::new())
            .build()
            .unwrap_err();
        match err {
            RuntimeError::Plugin(p) => {
                assert!(p.to_string().contains("auth"));
                assert!(p.to_string().contains("SecretsProvider"));
            }
            RuntimeError::ServiceStart { .. } => {
                panic!("expected Plugin error, got ServiceStart")
            }
        }
    }

    #[tokio::test]
    async fn auth_plugin_satisfied_by_in_memory_secrets() {
        let runtime = Runtime::builder()
            .with_plugin(InMemorySecretsPlugin::new().with("jwt", b"secret"))
            .with_plugin(AuthPlugin::new())
            .build();
        assert!(runtime.is_ok());
    }

    // ---- JwtCodec round-trip + validation ----

    fn test_runtime() -> walastack_runtime::Runtime {
        Runtime::builder()
            .with_plugin(InMemorySecretsPlugin::new().with("jwt", b"test-signing-secret"))
            .with_plugin(AuthPlugin::new())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn jwt_codec_round_trips_claims() {
        let runtime = test_runtime();
        let codec =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("walastack-tests"))
                .unwrap();

        let claims = codec.issue("user-42", vec!["admin".into(), "editor".into()]);
        let token = codec.encode(&claims).unwrap();
        let decoded = codec.decode(&token).unwrap();
        assert_eq!(decoded.sub, "user-42");
        assert_eq!(decoded.iss, "walastack-tests");
        assert_eq!(
            decoded.roles,
            vec!["admin".to_string(), "editor".to_string()]
        );
    }

    #[tokio::test]
    async fn jwt_codec_rejects_expired_tokens() {
        let runtime = test_runtime();
        let codec =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("walastack-tests"))
                .unwrap();

        // Issue with a near-zero TTL, then sleep just over the leeway.
        let claims = codec.issue_with_ttl("u", vec![], Duration::from_secs(0));
        let token = codec.encode(&claims).unwrap();
        // jsonwebtoken's default leeway is 60s; force expiration in the
        // past by manually constructing a Claims with exp in the past.
        let expired_claims = Claims {
            sub: "u".into(),
            iss: "walastack-tests".into(),
            aud: None,
            exp: Utc::now().timestamp() - 600,
            iat: Utc::now().timestamp() - 700,
            roles: vec![],
        };
        let expired_token = codec.encode(&expired_claims).unwrap();
        assert!(matches!(
            codec.decode(&expired_token),
            Err(AuthError::Jwt(_))
        ));
        // The "issued with zero ttl" branch falls inside the default
        // leeway window; we don't assert it specifically — the explicit
        // backdated-claims assertion above is the deterministic test.
        let _ = token;
    }

    #[tokio::test]
    async fn jwt_codec_rejects_tampered_tokens() {
        let runtime = test_runtime();
        let codec =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("walastack-tests"))
                .unwrap();

        let claims = codec.issue("u", vec![]);
        let token = codec.encode(&claims).unwrap();

        // Flip a character in the middle of the signature segment by
        // splitting at the final '.' and rewriting one byte of the sig.
        let last_dot = token.rfind('.').unwrap();
        let (head, sig) = token.split_at(last_dot + 1);
        let mut sig_bytes = sig.as_bytes().to_vec();
        let idx = 2.min(sig_bytes.len().saturating_sub(1));
        sig_bytes[idx] = if sig_bytes[idx] == b'A' { b'B' } else { b'A' };
        let mut tampered = String::with_capacity(token.len());
        tampered.push_str(head);
        tampered.push_str(std::str::from_utf8(&sig_bytes).unwrap());

        assert!(matches!(codec.decode(&tampered), Err(AuthError::Jwt(_))));
    }

    #[tokio::test]
    async fn jwt_codec_rejects_wrong_issuer() {
        let runtime = test_runtime();

        let issuing_codec =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("issuer-a")).unwrap();
        let verifying_codec =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("issuer-b")).unwrap();

        let claims = issuing_codec.issue("u", vec![]);
        let token = issuing_codec.encode(&claims).unwrap();
        assert!(matches!(
            verifying_codec.decode(&token),
            Err(AuthError::Jwt(_))
        ));
    }

    #[tokio::test]
    async fn jwt_codec_from_runtime_errors_when_secret_missing() {
        let runtime = Runtime::builder()
            .with_plugin(InMemorySecretsPlugin::new().with("other-key", b"x"))
            .build()
            .unwrap();
        let err =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("x")).unwrap_err();
        assert!(matches!(err, AuthError::SecretNotFound(name) if name == "jwt"));
    }

    #[tokio::test]
    async fn jwt_codec_from_runtime_errors_when_provider_missing() {
        let runtime = Runtime::builder().build().unwrap();
        let err =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("x")).unwrap_err();
        assert!(matches!(err, AuthError::SecretsProviderMissing));
    }

    #[tokio::test]
    async fn jwt_codec_supports_custom_claims_payloads() {
        #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
        struct CustomClaims {
            sub: String,
            iss: String,
            exp: i64,
            tenant: String,
            #[serde(default)]
            scopes: Vec<String>,
        }

        let runtime = test_runtime();
        let codec =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("walastack-tests"))
                .unwrap();

        let custom = CustomClaims {
            sub: "u".into(),
            iss: "walastack-tests".into(),
            exp: Utc::now().timestamp() + 600,
            tenant: "acme".into(),
            scopes: vec!["read".into(), "write".into()],
        };
        let token = codec.encode_custom(&custom).unwrap();
        let decoded: CustomClaims = codec.decode_custom(&token).unwrap();
        assert_eq!(decoded, custom);
    }

    // ---- Claims helpers ----

    #[test]
    fn claims_has_role_matches_exact_role_names() {
        let claims = Claims {
            sub: "u".into(),
            iss: "i".into(),
            aud: None,
            exp: 0,
            iat: 0,
            roles: vec!["admin".into(), "editor".into()],
        };
        assert!(claims.has_role("admin"));
        assert!(claims.has_role("editor"));
        assert!(!claims.has_role("viewer"));
    }

    #[test]
    fn claims_has_any_role_returns_true_on_first_match() {
        let claims = Claims {
            sub: "u".into(),
            iss: "i".into(),
            aud: None,
            exp: 0,
            iat: 0,
            roles: vec!["viewer".into()],
        };
        assert!(claims.has_any_role(["admin", "editor", "viewer"]));
        assert!(!claims.has_any_role(["admin", "editor"]));
    }

    // ---- SessionStore + InMemorySessionStorePlugin ----

    #[tokio::test]
    async fn session_store_round_trips_through_capability() {
        let runtime = Runtime::builder()
            .with_plugin(InMemorySessionStorePlugin::new())
            .build()
            .unwrap();
        let sessions = runtime.context().capability::<dyn SessionStore>().unwrap();

        let mut data = HashMap::new();
        data.insert("locale".into(), "en-US".into());
        let session = Session {
            id: "sess-1".into(),
            user_id: "u-42".into(),
            data: data.clone(),
            expires_at: Utc::now().timestamp() + 3600,
        };
        sessions.put(session.clone());

        let fetched = sessions.get("sess-1").unwrap();
        assert_eq!(fetched, session);
    }

    #[tokio::test]
    async fn session_store_remove_drops_the_session() {
        let runtime = Runtime::builder()
            .with_plugin(InMemorySessionStorePlugin::new())
            .build()
            .unwrap();
        let sessions = runtime.context().capability::<dyn SessionStore>().unwrap();
        sessions.put(Session {
            id: "sess-x".into(),
            user_id: "u".into(),
            data: HashMap::new(),
            expires_at: 0,
        });
        sessions.remove("sess-x");
        assert!(sessions.get("sess-x").is_none());
    }

    #[tokio::test]
    async fn session_store_returns_none_for_missing_id() {
        let runtime = Runtime::builder()
            .with_plugin(InMemorySessionStorePlugin::new())
            .build()
            .unwrap();
        let sessions = runtime.context().capability::<dyn SessionStore>().unwrap();
        assert!(sessions.get("nope").is_none());
    }

    // ---- Auth extractor + AuthRejection ----

    fn auth_runtime() -> walastack_runtime::Runtime {
        Runtime::builder()
            .with_plugin(InMemorySecretsPlugin::new().with("jwt", b"extractor-signing-secret"))
            .with_plugin(AuthPlugin::new().with_jwt("jwt", JwtConfig::new("walastack-tests")))
            .build()
            .unwrap()
    }

    async fn extract_auth(
        runtime: &walastack_runtime::Runtime,
        header: Option<&str>,
    ) -> std::result::Result<Auth, AuthRejection> {
        let mut builder = http::Request::builder().method("GET").uri("/");
        if let Some(value) = header {
            builder = builder.header(http::header::AUTHORIZATION, value);
        }
        let request = builder
            .body(walastack_http::Body::new(bytes::Bytes::new()))
            .unwrap();
        let (mut parts, _body) = request.into_parts();
        parts.extensions.insert(runtime.context().clone());
        <Auth as FromRequestParts>::from_request_parts(&mut parts).await
    }

    #[tokio::test]
    async fn auth_extractor_returns_claims_for_valid_bearer_token() {
        let runtime = auth_runtime();
        let codec =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("walastack-tests"))
                .unwrap();
        let claims = codec.issue("user-42", vec!["admin".into()]);
        let token = codec.encode(&claims).unwrap();

        let Auth(decoded) = extract_auth(&runtime, Some(&format!("Bearer {token}")))
            .await
            .unwrap();
        assert_eq!(decoded.sub, "user-42");
        assert!(decoded.has_role("admin"));
    }

    #[tokio::test]
    async fn auth_extractor_rejects_missing_authorization_header() {
        let runtime = auth_runtime();
        let err = extract_auth(&runtime, None).await.unwrap_err();
        assert!(matches!(err, AuthRejection::MissingAuthorizationHeader));
        assert_eq!(err.status(), http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_extractor_rejects_invalid_token() {
        let runtime = auth_runtime();
        let err = extract_auth(&runtime, Some("Bearer not.a.token"))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthRejection::InvalidToken));
        assert_eq!(err.status(), http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_extractor_rejects_malformed_authorization_header() {
        let runtime = auth_runtime();
        let err = extract_auth(&runtime, Some("Basic abc123"))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthRejection::MalformedAuthorizationHeader));
        assert_eq!(err.status(), http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_extractor_returns_500_when_jwt_settings_missing() {
        // AuthPlugin::new() without with_jwt — settings resource absent.
        let runtime = Runtime::builder()
            .with_plugin(InMemorySecretsPlugin::new().with("jwt", b"x"))
            .with_plugin(AuthPlugin::new())
            .build()
            .unwrap();

        let err = extract_auth(&runtime, Some("Bearer anything"))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthRejection::MissingJwtSettings));
        assert_eq!(err.status(), http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    // ---- Handler-level integration: TestClient + Auth extractor ----

    #[tokio::test]
    async fn protected_handler_with_auth_extractor_round_trips_through_test_client() {
        use walastack_app::App;
        use walastack_test::TestClient;

        async fn protected(Auth(claims): Auth) -> String {
            format!("hello, {} (admin={})", claims.sub, claims.has_role("admin"))
        }

        let runtime = auth_runtime();
        let codec =
            JwtCodec::from_runtime(runtime.context(), "jwt", JwtConfig::new("walastack-tests"))
                .unwrap();
        let claims = codec.issue("alice", vec!["admin".into()]);
        let token = codec.encode(&claims).unwrap();

        let app = App::new().get("/protected", protected);
        let client = TestClient::with_runtime(app, &runtime);

        // Valid token → 200.
        let response = client
            .get_with_headers(
                "/protected",
                &[("authorization", &format!("Bearer {token}"))],
            )
            .await;
        assert_eq!(response.status(), http::StatusCode::OK);

        // Missing header → 401.
        let response = client.get("/protected").await;
        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);

        // Invalid token → 401.
        let response = client
            .get_with_headers("/protected", &[("authorization", "Bearer garbage")])
            .await;
        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
    }
}

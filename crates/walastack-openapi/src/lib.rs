//! OpenAPI 3.0 spec generation for WalaStack.
//!
//! ## What this crate ships (Iteration 1)
//!
//! - [`Schema`] + [`ToSchema`] — JSON-Schema-shaped Schema model and a
//!   trait Rust types implement to describe their wire form. Hand-built
//!   today; a `#[derive(Schema)]` is intentionally deferred until the
//!   hand-built ergonomics stabilize.
//! - [`OpenApiConfig`] — top-level document configuration (title,
//!   version, description, servers, contact, license).
//! - [`RouteSpec`] + [`ParameterSpec`] + [`ResponseSpec`] — per-route
//!   metadata captured via a small builder.
//! - [`OpenApiPlugin`] — registers `OpenApiConfig` as a kernel
//!   `Resource` so the spec endpoint can resolve it through the
//!   `RuntimeContext` → request-extensions pattern that
//!   `walastack-app` injects.
//! - `App::with_plugin` / `App::openapi_route` / `App::openapi_serve_at`
//!   — application-side integration helpers exposed by `walastack-app`.
//!
//! ## What this crate does NOT ship (deferred)
//!
//! - **`#[derive(Schema)]` proc-macro** — hand-built first; derive
//!   later if/when the hand-built model proves stable.
//! - **Route-attribute integration** (e.g. extending `#[get(...)]` to
//!   accept OpenAPI metadata) — adds macro coupling; deferred until
//!   the spec model has converged.
//! - **Plugin → HttpService route extension primitive** — `App` owns
//!   the spec endpoint registration in Iteration 1 via
//!   `openapi_serve_at`; a generic plugin-injects-routes mechanism is
//!   a separate architectural batch.
//! - **OpenAPI security schemes / oauth2 flows** — auth-as-security
//!   integration arrives once the auth + openapi crates have both
//!   stabilized.
//! - **`$ref` / components reuse** — Iteration 1 inlines schemas;
//!   component reuse is a focused follow-up once we see how the
//!   document looks in practice.
//! - **Client SDK / codegen / Swagger UI hosting** — out of scope.
//!
//! ## Schema shape discipline
//!
//! Per a directional preference locked 2026-05-29, [`Schema`] is
//! **JSON-Schema-shaped** even where it only implements a subset.
//! Field names mirror JSON Schema (`type`, `format`, `properties`,
//! `required`, `items`, `enum`, `description`, `nullable`) so the same
//! representation can flow to OpenAPI, JSON Schema validators, future
//! forms engines, AI schema generation, and codegen ecosystems without
//! requiring representation changes.

#![allow(clippy::missing_errors_doc)]
// "OpenAPI", "JSON Schema", "WalaStack" are domain names, not code
// identifiers. Backticking them every mention hurts readability more
// than the lint helps.
#![allow(clippy::doc_markdown)]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use walastack_runtime::{Plugin, ResourceRegistry};

// =========================================================================
// Schema (JSON-Schema-shaped)
// =========================================================================

/// JSON-Schema-shaped schema descriptor.
///
/// Implements `Serialize` directly so the wire form is valid JSON
/// Schema (suitable for OpenAPI 3.0 Schema Object, JSON Schema draft 7+
/// with minor adjustments, forms-engine schemas, validation engines,
/// and code generation ecosystems).
#[derive(Clone, Debug, Default, Serialize)]
pub struct Schema {
    /// `type` keyword (JSON Schema). `string` / `integer` / `number` /
    /// `boolean` / `object` / `array`.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub ty: Option<SchemaType>,

    /// `format` keyword. Common values: `date-time`, `email`, `uuid`,
    /// `uri`, `int32`, `int64`, `float`, `double`, `byte`, `binary`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Human-readable description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Object property schemas, keyed by property name. Empty for
    /// non-object schemas.
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub properties: BTreeMap<String, Schema>,

    /// Required property names for object schemas.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub required: Vec<String>,

    /// Item schema for `array` types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Box<Schema>>,

    /// OpenAPI 3.0 nullable flag. `true` for types derived from
    /// `Option<T>`. (OpenAPI 3.0 idiom; JSON Schema draft 7+ uses
    /// `type: ["...", "null"]` — we ship the 3.0 representation for
    /// now and convert at a draft-7 emitter later if needed.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,

    /// `enum` values, when the schema is constrained to a fixed set.
    #[serde(rename = "enum", skip_serializing_if = "Vec::is_empty", default)]
    pub enum_values: Vec<serde_json::Value>,

    /// Example value for documentation purposes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub example: Option<serde_json::Value>,
}

/// JSON Schema type keyword. OpenAPI 3.0 idioms.
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SchemaType {
    /// `"string"`.
    String,
    /// `"integer"`.
    Integer,
    /// `"number"`.
    Number,
    /// `"boolean"`.
    Boolean,
    /// `"object"`.
    Object,
    /// `"array"`.
    Array,
}

impl Schema {
    /// Construct a `string`-typed schema.
    #[must_use]
    pub fn string() -> Self {
        Self {
            ty: Some(SchemaType::String),
            ..Self::default()
        }
    }

    /// Construct an `integer`-typed schema.
    #[must_use]
    pub fn integer() -> Self {
        Self {
            ty: Some(SchemaType::Integer),
            ..Self::default()
        }
    }

    /// Construct a `number`-typed schema (floating-point).
    #[must_use]
    pub fn number() -> Self {
        Self {
            ty: Some(SchemaType::Number),
            ..Self::default()
        }
    }

    /// Construct a `boolean`-typed schema.
    #[must_use]
    pub fn boolean() -> Self {
        Self {
            ty: Some(SchemaType::Boolean),
            ..Self::default()
        }
    }

    /// Construct an `object`-typed schema with no properties.
    /// Build it up via [`Schema::property`] / [`Schema::required`].
    #[must_use]
    pub fn object() -> Self {
        Self {
            ty: Some(SchemaType::Object),
            ..Self::default()
        }
    }

    /// Construct an `array`-typed schema with the given items schema.
    #[must_use]
    pub fn array(items: Self) -> Self {
        Self {
            ty: Some(SchemaType::Array),
            items: Some(Box::new(items)),
            ..Self::default()
        }
    }

    /// Set the `format` keyword. Returns `self` for chaining.
    #[must_use]
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    /// Set the `description`. Returns `self` for chaining.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Mark this schema as nullable. Returns `self` for chaining.
    #[must_use]
    pub const fn nullable(mut self) -> Self {
        self.nullable = Some(true);
        self
    }

    /// Add an object property. Returns `self` for chaining. Implicitly
    /// promotes `ty` to `object` if unset.
    #[must_use]
    pub fn property(mut self, name: impl Into<String>, schema: Self) -> Self {
        if self.ty.is_none() {
            self.ty = Some(SchemaType::Object);
        }
        self.properties.insert(name.into(), schema);
        self
    }

    /// Add a required property name. Returns `self` for chaining.
    /// Implicitly promotes `ty` to `object` if unset.
    #[must_use]
    pub fn require(mut self, name: impl Into<String>) -> Self {
        if self.ty.is_none() {
            self.ty = Some(SchemaType::Object);
        }
        self.required.push(name.into());
        self
    }

    /// Add an example value. Returns `self` for chaining.
    #[must_use]
    pub fn with_example(mut self, example: serde_json::Value) -> Self {
        self.example = Some(example);
        self
    }
}

/// Trait for Rust types that describe their JSON Schema shape.
///
/// Implemented for primitives, `String`, `Option<T>`, `Vec<T>`,
/// `serde_json::Value`, and the framework's `Json<T>` responder
/// (delegating to `T::schema`). Domain types implement this trait by
/// hand in Iteration 1; a `#[derive(Schema)]` arrives in a later
/// iteration once the hand-built ergonomics stabilize.
pub trait ToSchema {
    /// Produce the schema describing this type's wire form.
    fn schema() -> Schema;
}

macro_rules! impl_to_schema_primitive {
    ($($t:ty => $body:expr),* $(,)?) => {
        $(
            impl ToSchema for $t {
                fn schema() -> Schema { $body }
            }
        )*
    };
}

impl_to_schema_primitive! {
    String => Schema::string(),
    &'static str => Schema::string(),
    bool => Schema::boolean(),
    i8 => Schema::integer().with_format("int32"),
    i16 => Schema::integer().with_format("int32"),
    i32 => Schema::integer().with_format("int32"),
    i64 => Schema::integer().with_format("int64"),
    u8 => Schema::integer().with_format("int32"),
    u16 => Schema::integer().with_format("int32"),
    u32 => Schema::integer().with_format("int32"),
    u64 => Schema::integer().with_format("int64"),
    f32 => Schema::number().with_format("float"),
    f64 => Schema::number().with_format("double"),
    serde_json::Value => Schema::default(),
}

impl<T: ToSchema> ToSchema for Option<T> {
    fn schema() -> Schema {
        T::schema().nullable()
    }
}

impl<T: ToSchema> ToSchema for Vec<T> {
    fn schema() -> Schema {
        Schema::array(T::schema())
    }
}

// =========================================================================
// OpenApiConfig (top-level document configuration)
// =========================================================================

/// Top-level OpenAPI document configuration.
///
/// Registered as a kernel `Resource` by [`OpenApiPlugin`] so handlers
/// (including the spec endpoint installed by `openapi_serve_at`) can
/// resolve it through `RuntimeContext::resource`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct OpenApiConfig {
    /// Document title.
    pub title: String,
    /// Document version (independent of the OpenAPI spec version).
    pub version: String,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Servers list. Optional.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub servers: Vec<ServerSpec>,
    /// Contact block. Optional.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact: Option<ContactSpec>,
    /// License block. Optional.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<LicenseSpec>,
}

/// OpenAPI server entry.
#[derive(Clone, Debug, Serialize)]
pub struct ServerSpec {
    /// Server URL.
    pub url: String,
    /// Description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// OpenAPI contact info.
#[derive(Clone, Debug, Serialize)]
pub struct ContactSpec {
    /// Contact name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Contact URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Contact email.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

/// OpenAPI license info.
#[derive(Clone, Debug, Serialize)]
pub struct LicenseSpec {
    /// SPDX identifier or human name.
    pub name: String,
    /// Identifier URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl OpenApiConfig {
    /// Construct with title + version. All other fields are empty.
    #[must_use]
    pub fn new(title: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            version: version.into(),
            ..Self::default()
        }
    }

    /// Set the description. Returns `self` for chaining.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add a server entry. Returns `self` for chaining.
    #[must_use]
    pub fn with_server(mut self, url: impl Into<String>) -> Self {
        self.servers.push(ServerSpec {
            url: url.into(),
            description: None,
        });
        self
    }

    /// Attach a contact block.
    #[must_use]
    pub fn with_contact(mut self, contact: ContactSpec) -> Self {
        self.contact = Some(contact);
        self
    }

    /// Attach a license block.
    #[must_use]
    pub fn with_license(mut self, license: LicenseSpec) -> Self {
        self.license = Some(license);
        self
    }
}

// =========================================================================
// RouteSpec + ParameterSpec + RequestBodySpec + ResponseSpec
// =========================================================================

/// Per-route OpenAPI metadata.
///
/// Builder-driven so the common case is concise:
///
/// ```ignore
/// RouteSpec::get("/users/:id")
///     .summary("Get user")
///     .path_param("id", Schema::string().with_format("uuid"))
///     .response::<User>(200)
/// ```
///
/// The recorded `path` may use the WalaStack routing syntax (`:name`);
/// at document assembly time the path is normalized to OpenAPI's
/// `{name}` syntax automatically.
#[derive(Clone, Debug)]
pub struct RouteSpec {
    /// HTTP method.
    pub method: Method,
    /// Path. Either WalaStack syntax (`:name`) or OpenAPI syntax
    /// (`{name}`); normalized at assembly.
    pub path: String,
    /// Short summary.
    pub summary: Option<String>,
    /// Longer description.
    pub description: Option<String>,
    /// Tags for grouping in the generated document.
    pub tags: Vec<String>,
    /// Path / query / header parameters.
    pub parameters: Vec<ParameterSpec>,
    /// Optional request body.
    pub request_body: Option<RequestBodySpec>,
    /// Responses keyed by status code (as a string for OpenAPI
    /// compatibility — also accepts `"default"`).
    pub responses: BTreeMap<String, ResponseSpec>,
}

/// HTTP method enum used by [`RouteSpec`]. Distinct from
/// [`http::Method`] to avoid pulling http types into the spec model
/// where we don't need them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    /// `GET`.
    Get,
    /// `POST`.
    Post,
    /// `PUT`.
    Put,
    /// `DELETE`.
    Delete,
    /// `PATCH`.
    Patch,
    /// `OPTIONS`.
    Options,
    /// `HEAD`.
    Head,
    /// `TRACE`.
    Trace,
}

impl Method {
    /// Lowercase method name used as the OpenAPI Path Item Object key.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Put => "put",
            Self::Delete => "delete",
            Self::Patch => "patch",
            Self::Options => "options",
            Self::Head => "head",
            Self::Trace => "trace",
        }
    }
}

/// Parameter location keyword from OpenAPI.
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ParameterLocation {
    /// `path`.
    Path,
    /// `query`.
    Query,
    /// `header`.
    Header,
    /// `cookie`.
    Cookie,
}

/// OpenAPI parameter spec.
#[derive(Clone, Debug, Serialize)]
pub struct ParameterSpec {
    /// Parameter name.
    pub name: String,
    /// Where the parameter lives.
    #[serde(rename = "in")]
    pub location: ParameterLocation,
    /// Required-ness. Path parameters are always required.
    pub required: bool,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Schema for the parameter value.
    pub schema: Schema,
}

/// Request body spec.
#[derive(Clone, Debug, Serialize)]
pub struct RequestBodySpec {
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether the body is required.
    pub required: bool,
    /// Content-type → schema mapping.
    pub content: BTreeMap<String, MediaTypeSpec>,
}

/// One entry of the `content` map.
#[derive(Clone, Debug, Serialize)]
pub struct MediaTypeSpec {
    /// Schema for the body of this content type.
    pub schema: Schema,
}

/// Response spec for a single status code.
#[derive(Clone, Debug, Serialize)]
pub struct ResponseSpec {
    /// Human-readable description (OpenAPI requires this field).
    pub description: String,
    /// Optional content-type map. Empty for empty-body responses.
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub content: BTreeMap<String, MediaTypeSpec>,
}

impl RouteSpec {
    /// Construct a new spec for the given method and path.
    #[must_use]
    pub fn new(method: Method, path: impl Into<String>) -> Self {
        Self {
            method,
            path: path.into(),
            summary: None,
            description: None,
            tags: Vec::new(),
            parameters: Vec::new(),
            request_body: None,
            responses: BTreeMap::new(),
        }
    }

    /// Convenience constructor for `GET`.
    #[must_use]
    pub fn get(path: impl Into<String>) -> Self {
        Self::new(Method::Get, path)
    }

    /// Convenience constructor for `POST`.
    #[must_use]
    pub fn post(path: impl Into<String>) -> Self {
        Self::new(Method::Post, path)
    }

    /// Convenience constructor for `PUT`.
    #[must_use]
    pub fn put(path: impl Into<String>) -> Self {
        Self::new(Method::Put, path)
    }

    /// Convenience constructor for `DELETE`.
    #[must_use]
    pub fn delete(path: impl Into<String>) -> Self {
        Self::new(Method::Delete, path)
    }

    /// Set a short summary.
    #[must_use]
    pub fn summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }

    /// Set the longer description.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add a tag for documentation grouping.
    #[must_use]
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add a path parameter. `required` is forced to `true`.
    #[must_use]
    pub fn path_param(mut self, name: impl Into<String>, schema: Schema) -> Self {
        self.parameters.push(ParameterSpec {
            name: name.into(),
            location: ParameterLocation::Path,
            required: true,
            description: None,
            schema,
        });
        self
    }

    /// Add a query parameter.
    #[must_use]
    pub fn query_param(mut self, name: impl Into<String>, required: bool, schema: Schema) -> Self {
        self.parameters.push(ParameterSpec {
            name: name.into(),
            location: ParameterLocation::Query,
            required,
            description: None,
            schema,
        });
        self
    }

    /// Set a JSON request body using `T::schema`.
    #[must_use]
    pub fn json_body<T: ToSchema>(mut self) -> Self {
        let mut content = BTreeMap::new();
        content.insert(
            "application/json".into(),
            MediaTypeSpec {
                schema: T::schema(),
            },
        );
        self.request_body = Some(RequestBodySpec {
            description: None,
            required: true,
            content,
        });
        self
    }

    /// Declare a JSON response for the given status using `T::schema`.
    /// Description defaults to a generic phrase if not set explicitly.
    #[must_use]
    pub fn response<T: ToSchema>(mut self, status: u16) -> Self {
        let mut content = BTreeMap::new();
        content.insert(
            "application/json".into(),
            MediaTypeSpec {
                schema: T::schema(),
            },
        );
        self.responses.insert(
            status.to_string(),
            ResponseSpec {
                description: default_status_description(status).into(),
                content,
            },
        );
        self
    }

    /// Declare an empty-body response for the given status.
    #[must_use]
    pub fn empty_response(mut self, status: u16, description: impl Into<String>) -> Self {
        self.responses.insert(
            status.to_string(),
            ResponseSpec {
                description: description.into(),
                content: BTreeMap::new(),
            },
        );
        self
    }
}

const fn default_status_description(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        422 => "Unprocessable Entity",
        500 => "Internal Server Error",
        _ => "Response",
    }
}

// =========================================================================
// Document assembly
// =========================================================================

/// Accumulated route specifications, registered as a kernel `Resource`
/// by `App::openapi_serve_at` so the spec handler can resolve them.
///
/// This is a `Vec<RouteSpec>` rather than the JSON document directly so
/// the spec handler can re-render with the live `OpenApiConfig` from
/// the registry (which a hot-reload mechanism could swap later without
/// rebuilding the route list).
#[derive(Clone, Debug)]
pub struct OpenApiRoutes(pub Arc<Vec<RouteSpec>>);

/// Render `config` + `routes` to a `serde_json::Value` containing the
/// full OpenAPI 3.0 document.
///
/// Paths in `routes` may use WalaStack's `:name` syntax; they are
/// normalized to OpenAPI's `{name}` syntax during rendering.
#[must_use]
pub fn render_document(config: &OpenApiConfig, routes: &[RouteSpec]) -> serde_json::Value {
    let mut paths: BTreeMap<String, BTreeMap<String, serde_json::Value>> = BTreeMap::new();

    for route in routes {
        let normalized = normalize_path(&route.path);
        let path_item = paths.entry(normalized).or_default();
        path_item.insert(route.method.as_str().to_string(), render_operation(route));
    }

    let mut info = serde_json::Map::new();
    info.insert("title".into(), config.title.clone().into());
    info.insert("version".into(), config.version.clone().into());
    if let Some(d) = &config.description {
        info.insert("description".into(), d.clone().into());
    }
    if let Some(c) = &config.contact {
        info.insert(
            "contact".into(),
            serde_json::to_value(c).unwrap_or_default(),
        );
    }
    if let Some(l) = &config.license {
        info.insert(
            "license".into(),
            serde_json::to_value(l).unwrap_or_default(),
        );
    }

    let mut doc = serde_json::Map::new();
    doc.insert("openapi".into(), "3.0.3".into());
    doc.insert("info".into(), serde_json::Value::Object(info));
    if !config.servers.is_empty() {
        doc.insert(
            "servers".into(),
            serde_json::to_value(&config.servers).unwrap_or_default(),
        );
    }
    doc.insert(
        "paths".into(),
        serde_json::to_value(paths).unwrap_or_default(),
    );

    serde_json::Value::Object(doc)
}

fn render_operation(route: &RouteSpec) -> serde_json::Value {
    let mut op = serde_json::Map::new();
    if let Some(s) = &route.summary {
        op.insert("summary".into(), s.clone().into());
    }
    if let Some(d) = &route.description {
        op.insert("description".into(), d.clone().into());
    }
    if !route.tags.is_empty() {
        op.insert(
            "tags".into(),
            serde_json::to_value(&route.tags).unwrap_or_default(),
        );
    }
    if !route.parameters.is_empty() {
        op.insert(
            "parameters".into(),
            serde_json::to_value(&route.parameters).unwrap_or_default(),
        );
    }
    if let Some(b) = &route.request_body {
        op.insert(
            "requestBody".into(),
            serde_json::to_value(b).unwrap_or_default(),
        );
    }
    // OpenAPI requires a non-empty `responses` map. Add a fallback if
    // the user did not declare any.
    let mut responses = route.responses.clone();
    if responses.is_empty() {
        responses.insert(
            "default".into(),
            ResponseSpec {
                description: "Default response".into(),
                content: BTreeMap::new(),
            },
        );
    }
    op.insert(
        "responses".into(),
        serde_json::to_value(responses).unwrap_or_default(),
    );
    serde_json::Value::Object(op)
}

/// Normalize a WalaStack-style path (`:name`) to OpenAPI-style
/// (`{name}`). Idempotent for already-normalized paths.
#[must_use]
pub fn normalize_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for segment in path.split('/') {
        if !out.is_empty() || path.starts_with('/') {
            out.push('/');
        }
        if let Some(rest) = segment.strip_prefix(':') {
            out.push('{');
            out.push_str(rest);
            out.push('}');
        } else {
            out.push_str(segment);
        }
    }
    // Collapse the leading slash that the loop adds when `path`
    // already starts with one.
    if path.starts_with('/') && out.starts_with("//") {
        out.remove(0);
    }
    out
}

// =========================================================================
// OpenApiPlugin
// =========================================================================

/// Plugin that registers an [`OpenApiConfig`] as a kernel `Resource`.
///
/// The spec endpoint installed by `App::openapi_serve_at` resolves the
/// config via `RuntimeContext::resource` at request time. This follows
/// the **Resource-as-Configuration** candidate ecosystem convention
/// first validated in `walastack-auth` (`JwtSettings`).
#[derive(Clone, Debug)]
pub struct OpenApiPlugin {
    config: OpenApiConfig,
}

impl OpenApiPlugin {
    /// Construct from a configured [`OpenApiConfig`].
    #[must_use]
    pub const fn new(config: OpenApiConfig) -> Self {
        Self { config }
    }
}

impl Plugin for OpenApiPlugin {
    fn name(&self) -> &'static str {
        "openapi"
    }

    fn register_resources(&self, registry: &mut ResourceRegistry) {
        registry.insert(self.config.clone());
    }
}

// =========================================================================
// Prelude
// =========================================================================

/// Common imports for applications using `walastack-openapi`.
///
/// ```rust
/// use walastack_openapi::prelude::*;
/// ```
///
/// Re-exports the spec model + plugin types. Document-rendering
/// internals (`render_document`, `normalize_path`) and the
/// component-spec types (`ServerSpec`, `ContactSpec`, etc.) remain
/// in the crate root for advanced users.
pub mod prelude {
    pub use crate::{
        Method, OpenApiConfig, OpenApiPlugin, ParameterLocation, ParameterSpec, RouteSpec, Schema,
        SchemaType, ToSchema,
    };
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    // ---- Schema primitives ----

    #[test]
    fn schema_primitives_emit_expected_json_types() {
        let s = serde_json::to_value(String::schema()).unwrap();
        assert_eq!(s["type"], "string");

        let s = serde_json::to_value(i32::schema()).unwrap();
        assert_eq!(s["type"], "integer");
        assert_eq!(s["format"], "int32");

        let s = serde_json::to_value(f64::schema()).unwrap();
        assert_eq!(s["type"], "number");
        assert_eq!(s["format"], "double");

        let s = serde_json::to_value(bool::schema()).unwrap();
        assert_eq!(s["type"], "boolean");
    }

    #[test]
    fn schema_option_is_nullable() {
        let s = serde_json::to_value(Option::<String>::schema()).unwrap();
        assert_eq!(s["type"], "string");
        assert_eq!(s["nullable"], true);
    }

    #[test]
    fn schema_vec_is_array_with_items() {
        let s = serde_json::to_value(Vec::<i64>::schema()).unwrap();
        assert_eq!(s["type"], "array");
        assert_eq!(s["items"]["type"], "integer");
        assert_eq!(s["items"]["format"], "int64");
    }

    #[test]
    fn schema_object_builder_emits_properties_and_required() {
        let s = Schema::object()
            .property("name", Schema::string())
            .property("age", Schema::integer().with_format("int32"))
            .require("name");
        let v = serde_json::to_value(s).unwrap();
        assert_eq!(v["type"], "object");
        assert_eq!(v["properties"]["name"]["type"], "string");
        assert_eq!(v["properties"]["age"]["type"], "integer");
        assert_eq!(v["required"][0], "name");
    }

    #[test]
    fn schema_does_not_emit_empty_optional_fields() {
        let v = serde_json::to_value(Schema::string()).unwrap();
        // Should only have "type", not properties/required/items/etc.
        let obj = v.as_object().unwrap();
        assert!(obj.contains_key("type"));
        assert!(!obj.contains_key("properties"));
        assert!(!obj.contains_key("required"));
        assert!(!obj.contains_key("items"));
        assert!(!obj.contains_key("nullable"));
    }

    // ---- RouteSpec ----

    #[test]
    fn route_spec_get_constructor_sets_method_and_path() {
        let r = RouteSpec::get("/users");
        assert_eq!(r.method, Method::Get);
        assert_eq!(r.path, "/users");
    }

    #[test]
    fn route_spec_response_adds_json_content_with_schema() {
        let r = RouteSpec::get("/users").response::<Vec<String>>(200);
        let response = r.responses.get("200").unwrap();
        assert_eq!(response.description, "OK");
        let media = response.content.get("application/json").unwrap();
        let v = serde_json::to_value(&media.schema).unwrap();
        assert_eq!(v["type"], "array");
        assert_eq!(v["items"]["type"], "string");
    }

    #[test]
    fn route_spec_path_param_marks_required_path_location() {
        let r = RouteSpec::get("/users/:id").path_param("id", Schema::string().with_format("uuid"));
        assert_eq!(r.parameters.len(), 1);
        let p = &r.parameters[0];
        assert_eq!(p.name, "id");
        assert!(p.required);
        assert_eq!(p.location, ParameterLocation::Path);
    }

    // ---- Path normalization ----

    #[test]
    fn normalize_path_converts_colon_segments_to_braces() {
        assert_eq!(normalize_path("/users/:id"), "/users/{id}");
        assert_eq!(
            normalize_path("/orgs/:org/projects/:proj"),
            "/orgs/{org}/projects/{proj}"
        );
        assert_eq!(normalize_path("/health"), "/health");
        assert_eq!(normalize_path("/"), "/");
    }

    #[test]
    fn normalize_path_is_idempotent_for_already_braced_paths() {
        assert_eq!(normalize_path("/users/{id}"), "/users/{id}");
    }

    // ---- Document assembly ----

    #[test]
    fn render_document_emits_openapi_3_with_info_and_paths() {
        let config = OpenApiConfig::new("MyApp", "0.1.0")
            .with_description("a test app")
            .with_server("https://api.example.test");

        let routes = vec![
            RouteSpec::get("/users")
                .summary("List users")
                .response::<Vec<String>>(200),
            RouteSpec::get("/users/:id")
                .summary("Get user")
                .path_param("id", Schema::string())
                .response::<String>(200),
        ];

        let doc = render_document(&config, &routes);

        assert_eq!(doc["openapi"], "3.0.3");
        assert_eq!(doc["info"]["title"], "MyApp");
        assert_eq!(doc["info"]["version"], "0.1.0");
        assert_eq!(doc["info"]["description"], "a test app");
        assert_eq!(doc["servers"][0]["url"], "https://api.example.test");
        // Path normalization applied
        assert_eq!(doc["paths"]["/users/{id}"]["get"]["summary"], "Get user");
        assert_eq!(doc["paths"]["/users"]["get"]["summary"], "List users");
    }

    #[test]
    fn render_document_groups_methods_under_the_same_path() {
        let config = OpenApiConfig::new("MyApp", "0.1.0");
        let routes = vec![
            RouteSpec::get("/items").summary("List items"),
            RouteSpec::post("/items").summary("Create item"),
        ];

        let doc = render_document(&config, &routes);
        let item = &doc["paths"]["/items"];
        assert_eq!(item["get"]["summary"], "List items");
        assert_eq!(item["post"]["summary"], "Create item");
    }

    #[test]
    fn render_document_with_empty_responses_includes_fallback() {
        let config = OpenApiConfig::new("MyApp", "0.1.0");
        let routes = vec![RouteSpec::get("/").summary("root")];
        let doc = render_document(&config, &routes);
        // OpenAPI requires responses; we fill a `default` entry.
        assert!(doc["paths"]["/"]["get"]["responses"]["default"].is_object());
    }

    // ---- OpenApiPlugin ----

    #[tokio::test]
    async fn openapi_plugin_registers_openapi_config_as_resource() {
        use walastack_runtime::Runtime;
        let config = OpenApiConfig::new("PluginTest", "0.0.1");
        let runtime = Runtime::builder()
            .with_plugin(OpenApiPlugin::new(config))
            .build()
            .unwrap();
        let resolved = runtime.context().resource::<OpenApiConfig>();
        assert!(resolved.is_some());
        let resolved = resolved.unwrap();
        assert_eq!(resolved.title, "PluginTest");
        assert_eq!(resolved.version, "0.0.1");
    }
}

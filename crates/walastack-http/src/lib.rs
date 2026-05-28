//! # walastack-http
//!
//! HTTP types, body abstractions, and protocol helpers for WalaStack.
//!
//! Provides the framework's canonical [`Body`], [`Request`], and [`Response`]
//! type aliases plus the [`IntoResponse`] trait that converts handler return
//! values into HTTP responses. Also defines the framework's top-level
//! [`Error`] and [`Result`] types.
//!
//! Built on top of `http`, `http-body`, `http-body-util`, and `bytes`. Phase 1
//! uses non-streaming `Full<Bytes>` bodies; streaming bodies land in a later
//! phase.

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

pub use bytes::Bytes;
pub use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, Version, header};

/// The canonical body type for WalaStack requests and responses.
///
/// Phase 1 uses a single-buffer non-streaming body. Streaming bodies are
/// deferred to a later phase.
pub type Body = http_body_util::Full<Bytes>;

/// The canonical HTTP request type — `http::Request<`[`Body`]`>`.
pub type Request = http::Request<Body>;

/// The canonical HTTP response type — `http::Response<`[`Body`]`>`.
pub type Response = http::Response<Body>;

/// Convert a value into a [`Response`].
///
/// Implemented for the common handler return types — `&'static str`, `String`,
/// and `Response` itself (identity). More implementations land as the framework
/// grows.
pub trait IntoResponse {
    /// Consume `self` and produce a [`Response`].
    fn into_response(self) -> Response;
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}

impl IntoResponse for &'static str {
    fn into_response(self) -> Response {
        text_response(Bytes::from_static(self.as_bytes()))
    }
}

impl IntoResponse for String {
    fn into_response(self) -> Response {
        text_response(Bytes::from(self.into_bytes()))
    }
}

impl IntoResponse for std::convert::Infallible {
    fn into_response(self) -> Response {
        // `Infallible` is uninhabited — this branch is unreachable. The impl
        // exists so that extractors with `Rejection = Infallible` satisfy the
        // `FromRequest::Rejection: IntoResponse` bound.
        match self {}
    }
}

fn text_response(bytes: Bytes) -> Response {
    let mut response = Response::new(Body::new(bytes));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// HTML-typed responder.
///
/// Wraps a value that converts into [`Bytes`] and serves it as
/// `text/html; charset=utf-8`. The common cases are `Html<&'static str>` for
/// static markup and `Html<String>` for rendered templates.
///
/// # Example
///
/// ```rust
/// use walastack_http::{Html, IntoResponse};
///
/// let response = Html("<h1>Hello</h1>").into_response();
/// ```
pub struct Html<T>(pub T);

impl<T> std::fmt::Debug for Html<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Html").field(&self.0).finish()
    }
}

impl<T> IntoResponse for Html<T>
where
    T: Into<Bytes>,
{
    fn into_response(self) -> Response {
        let mut response = Response::new(Body::new(self.0.into()));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
        response
    }
}

/// JSON-typed responder.
///
/// Wraps a value that implements [`serde::Serialize`] and serves it as
/// `application/json`. Serialization failures produce a `500 Internal Server
/// Error` response with a minimal JSON error body — the trait stays
/// infallible.
///
/// # Example
///
/// ```rust
/// use serde::Serialize;
/// use walastack_http::{IntoResponse, Json};
///
/// #[derive(Serialize)]
/// struct Greeting { message: String }
///
/// let response = Json(Greeting { message: "hello".into() }).into_response();
/// ```
pub struct Json<T>(pub T);

impl<T> std::fmt::Debug for Json<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Json").field(&self.0).finish()
    }
}

impl<T> IntoResponse for Json<T>
where
    T: serde::Serialize,
{
    fn into_response(self) -> Response {
        match serde_json::to_vec(&self.0) {
            Ok(bytes) => {
                let mut response = Response::new(Body::new(Bytes::from(bytes)));
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                response
            }
            Err(error) => json_serialization_error(&error),
        }
    }
}

fn json_serialization_error(error: &serde_json::Error) -> Response {
    tracing::error!("json serialization failed: {error}");
    let mut response = Response::new(Body::new(Bytes::from_static(
        br#"{"error":"json serialization failed"}"#,
    )));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// The framework's error type.
///
/// Phase 1 covers the common error sources encountered while binding,
/// listening, and serving HTTP connections. Future variants will cover
/// AI orchestration, sync, deployment, and other ecosystem-specific
/// errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// I/O error from `tokio::net` or the underlying socket layer.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// An invalid socket address was passed to a binding operation.
    #[error("invalid address: {0}")]
    InvalidAddress(String),

    /// An ad-hoc error with a free-form message — escape hatch for early
    /// development. Specific variants will replace these over time.
    #[error("{0}")]
    Custom(String),
}

/// A specialized [`Result`](std::result::Result) for WalaStack operations.
pub type Result<T> = std::result::Result<T, Error>;

// ============================================================================
// Extractors (Phase 1 polish — Batch 5)
// ============================================================================

use std::collections::HashMap;
use std::future::Future;

use http::request::Parts;

/// Extracted URL path parameters.
///
/// The router populates this from `matchit` matches and inserts it into the
/// request extensions before invoking the handler. The [`Path<T>`] extractor
/// reads from this extension.
#[derive(Debug, Clone, Default)]
pub struct PathParams(pub HashMap<String, String>);

/// Path-parameter extractor.
///
/// For a route like `/users/:id`, extract the parameter as `Path<u32>`,
/// `Path<String>`, or `Path<MyStruct>` (where `MyStruct` has fields named
/// after the route parameters).
pub struct Path<T>(pub T);

impl<T: std::fmt::Debug> std::fmt::Debug for Path<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Path").field(&self.0).finish()
    }
}

/// Query-string extractor.
///
/// Deserializes the URL's query string into `T` via `serde_urlencoded`. Use
/// `T: Deserialize` — typically a struct with the expected query parameter
/// fields.
pub struct Query<T>(pub T);

impl<T: std::fmt::Debug> std::fmt::Debug for Query<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Query").field(&self.0).finish()
    }
}

/// Error returned by built-in extractors.
///
/// Implements [`IntoResponse`] and produces a `4xx` or `5xx` response with a
/// descriptive message.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExtractionRejection {
    /// Path parameters were not inserted into request extensions — indicates
    /// a router / app misconfiguration rather than client error.
    #[error("missing path parameters in request extensions")]
    MissingPathParams,

    /// Path parameter deserialization failed (type mismatch, etc.).
    #[error("invalid path parameter: {0}")]
    PathDeserializationFailed(String),

    /// Query string deserialization failed.
    #[error("invalid query string: {0}")]
    QueryDeserializationFailed(String),

    /// JSON body deserialization failed.
    #[error("invalid JSON body: {0}")]
    JsonDeserializationFailed(String),

    /// Reading the request body failed.
    #[error("failed to read request body: {0}")]
    BodyReadFailed(String),
}

impl IntoResponse for ExtractionRejection {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::MissingPathParams | Self::BodyReadFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::PathDeserializationFailed(_)
            | Self::QueryDeserializationFailed(_)
            | Self::JsonDeserializationFailed(_) => StatusCode::BAD_REQUEST,
        };
        let message = self.to_string();
        let mut response = Response::new(Body::new(Bytes::from(message.into_bytes())));
        *response.status_mut() = status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        response
    }
}

/// Extract a value from request parts (URI, headers, extensions) — non-consuming.
///
/// Multiple [`FromRequestParts`] extractors can be combined in a single handler
/// since each borrows from the parts.
pub trait FromRequestParts: Sized + Send + 'static {
    /// Error type returned on extraction failure.
    type Rejection: IntoResponse + Send + 'static;

    /// Extract from request parts.
    fn from_request_parts(
        parts: &mut Parts,
    ) -> impl Future<Output = std::result::Result<Self, Self::Rejection>> + Send;
}

/// Extract a value from a full request (parts + body) — consuming.
///
/// At most one [`FromRequest`] extractor per handler since it takes the
/// request by value. Every [`FromRequestParts`] type is also a `FromRequest`
/// via blanket implementation.
pub trait FromRequest: Sized + Send + 'static {
    /// Error type returned on extraction failure.
    type Rejection: IntoResponse + Send + 'static;

    /// Extract from a request.
    fn from_request(
        req: Request,
    ) -> impl Future<Output = std::result::Result<Self, Self::Rejection>> + Send;
}

// Every FromRequestParts is also a FromRequest.
impl<T: FromRequestParts> FromRequest for T {
    type Rejection = <T as FromRequestParts>::Rejection;

    async fn from_request(req: Request) -> std::result::Result<Self, Self::Rejection> {
        let (mut parts, _body) = req.into_parts();
        <T as FromRequestParts>::from_request_parts(&mut parts).await
    }
}

// Identity extractor — gives the handler the entire request.
impl FromRequest for Request {
    type Rejection = std::convert::Infallible;

    async fn from_request(req: Request) -> std::result::Result<Self, Self::Rejection> {
        Ok(req)
    }
}

impl<T> FromRequestParts for Path<T>
where
    T: std::str::FromStr + Send + 'static,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    type Rejection = ExtractionRejection;

    async fn from_request_parts(parts: &mut Parts) -> std::result::Result<Self, Self::Rejection> {
        let params = parts
            .extensions
            .get::<PathParams>()
            .ok_or(ExtractionRejection::MissingPathParams)?;

        // Phase 1 polish supports single-parameter routes: take the only value
        // in the params map and parse via `FromStr`. Multi-param routes
        // (`Path<(T1, T2)>`) and struct-based extraction (`Path<MyStruct>`)
        // land in a later batch with a custom deserializer.
        let value_str = params.0.values().next().ok_or_else(|| {
            ExtractionRejection::PathDeserializationFailed("no path parameter found".to_string())
        })?;

        let value = value_str
            .parse::<T>()
            .map_err(|e| ExtractionRejection::PathDeserializationFailed(e.to_string()))?;

        Ok(Self(value))
    }
}

impl<T> FromRequestParts for Query<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    type Rejection = ExtractionRejection;

    async fn from_request_parts(parts: &mut Parts) -> std::result::Result<Self, Self::Rejection> {
        let query = parts.uri.query().unwrap_or("");
        let value =
            serde_urlencoded::from_str(query).map_err(|e: serde_urlencoded::de::Error| {
                ExtractionRejection::QueryDeserializationFailed(e.to_string())
            })?;
        Ok(Self(value))
    }
}

// ============================================================================
// Body extractors (Phase 1 polish — Batch 6)
// ============================================================================

use http_body_util::BodyExt;

impl<T> FromRequest for Json<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    type Rejection = ExtractionRejection;

    async fn from_request(req: Request) -> std::result::Result<Self, Self::Rejection> {
        let body = req.into_body();
        let bytes = body
            .collect()
            .await
            .map_err(|e| ExtractionRejection::BodyReadFailed(e.to_string()))?
            .to_bytes();
        let value = serde_json::from_slice(&bytes).map_err(|e: serde_json::Error| {
            ExtractionRejection::JsonDeserializationFailed(e.to_string())
        })?;
        Ok(Self(value))
    }
}

impl FromRequest for Bytes {
    type Rejection = ExtractionRejection;

    async fn from_request(req: Request) -> std::result::Result<Self, Self::Rejection> {
        let body = req.into_body();
        let collected = body
            .collect()
            .await
            .map_err(|e| ExtractionRejection::BodyReadFailed(e.to_string()))?;
        Ok(collected.to_bytes())
    }
}

// ============================================================================
// Parts extractors — request metadata (Phase 1 polish — Batch 6)
// ============================================================================

impl FromRequestParts for Method {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts) -> std::result::Result<Self, Self::Rejection> {
        Ok(parts.method.clone())
    }
}

impl FromRequestParts for Uri {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts) -> std::result::Result<Self, Self::Rejection> {
        Ok(parts.uri.clone())
    }
}

impl FromRequestParts for HeaderMap {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts) -> std::result::Result<Self, Self::Rejection> {
        Ok(parts.headers.clone())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::{IntoResponse, StatusCode, header};

    #[test]
    fn into_response_for_static_str_sets_text_content_type() {
        let r = "hello".into_response();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(
            r.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8",
        );
    }

    #[test]
    fn into_response_for_string_sets_text_content_type() {
        let r = String::from("hello").into_response();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(
            r.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8",
        );
    }

    #[test]
    fn html_responder_sets_html_content_type() {
        use super::Html;
        let r = Html("<h1>hi</h1>").into_response();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(
            r.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8",
        );
    }

    #[test]
    fn json_responder_sets_json_content_type() {
        use super::Json;
        use serde::Serialize;

        #[derive(Serialize)]
        struct Payload {
            message: &'static str,
        }

        let r = Json(Payload { message: "hi" }).into_response();
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(
            r.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json",
        );
    }
}

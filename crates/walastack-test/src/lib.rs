//! # walastack-test
//!
//! Testing utilities for WalaStack applications.
//!
//! The [`TestClient`] dispatches requests directly through an [`App`]'s
//! router. No socket is bound and no HTTP framing happens — dispatches go
//! straight to handler execution. Use for fast integration tests that don't
//! require real network I/O.
//!
//! # Example
//!
//! ```rust
//! use walastack_app::App;
//! use walastack_test::TestClient;
//!
//! # async fn run() {
//! let app = App::new().get("/", || async { "hello" });
//! let client = TestClient::new(app);
//!
//! let response = client.get("/").await;
//! assert_eq!(response.status(), 200);
//! # }
//! ```

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Method, StatusCode};
use walastack_app::{App, dispatch_request};
use walastack_http::{Body, Response};
use walastack_router::Router;
use walastack_runtime::{Runtime, RuntimeContext};

/// In-memory test client for a WalaStack application.
///
/// Constructed from an [`App`]; dispatches requests directly through the
/// router. Returns the [`Response`] the handler produced, or a 404 if no
/// handler matched.
///
/// By default the client uses an empty `Runtime` so handlers that don't
/// touch the kernel work unchanged. To test handlers that depend on
/// kernel state (Auth, Jobs dashboards, Forms, MCP, future ecosystem
/// extractors), construct a populated [`Runtime`] and pass it via
/// [`TestClient::with_runtime`].
pub struct TestClient {
    router: Arc<Router>,
    runtime: RuntimeContext,
}

impl TestClient {
    /// Create a test client by consuming `app` and binding it to an
    /// empty `Runtime`. Use [`TestClient::with_runtime`] to attach a
    /// configured kernel.
    ///
    /// # Panics
    ///
    /// Panics if `Runtime::builder().build()` fails. With no plugins
    /// registered the builder cannot have unmet capability requirements,
    /// so this path is effectively infallible.
    #[must_use]
    pub fn new(app: App) -> Self {
        let runtime = match Runtime::builder().build() {
            Ok(rt) => rt,
            Err(e) => panic!("empty Runtime should always build: {e}"),
        };
        Self {
            router: Arc::new(app.into_router()),
            runtime: runtime.context().clone(),
        }
    }

    /// Attach a pre-built `Runtime` so handlers can resolve
    /// capabilities and resources through the `RuntimeContext` extension
    /// that `HttpService` injects in production.
    #[must_use]
    pub fn with_runtime(app: App, runtime: &Runtime) -> Self {
        Self {
            router: Arc::new(app.into_router()),
            runtime: runtime.context().clone(),
        }
    }

    /// Dispatch a `GET` request to `path`.
    pub async fn get(&self, path: &str) -> Response {
        self.dispatch(Method::GET, path, &[], Bytes::new()).await
    }

    /// Dispatch a `GET` request with custom headers.
    pub async fn get_with_headers(&self, path: &str, headers: &[(&str, &str)]) -> Response {
        self.dispatch(Method::GET, path, headers, Bytes::new())
            .await
    }

    /// Dispatch a `POST` request to `path` with no body.
    pub async fn post(&self, path: &str) -> Response {
        self.dispatch(Method::POST, path, &[], Bytes::new()).await
    }

    /// Dispatch a `POST` request with a JSON body. Sets
    /// `Content-Type: application/json` automatically.
    ///
    /// # Errors
    ///
    /// Returns a `400 Bad Request` response if the body cannot be
    /// serialized.
    pub async fn post_json<T: serde::Serialize + Sync>(&self, path: &str, body: &T) -> Response {
        let Ok(bytes) = serde_json::to_vec(body) else {
            return bad_request();
        };
        self.dispatch(
            Method::POST,
            path,
            &[("content-type", "application/json")],
            Bytes::from(bytes),
        )
        .await
    }

    /// Dispatch a `POST` request with custom headers and a raw body.
    pub async fn post_with(&self, path: &str, headers: &[(&str, &str)], body: Bytes) -> Response {
        self.dispatch(Method::POST, path, headers, body).await
    }

    /// Dispatch a `PUT` request to `path` with no body.
    pub async fn put(&self, path: &str) -> Response {
        self.dispatch(Method::PUT, path, &[], Bytes::new()).await
    }

    /// Dispatch a `DELETE` request to `path`.
    pub async fn delete(&self, path: &str) -> Response {
        self.dispatch(Method::DELETE, path, &[], Bytes::new()).await
    }

    async fn dispatch(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: Bytes,
    ) -> Response {
        let mut builder = http::Request::builder().method(method).uri(path);
        for (name, value) in headers {
            let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
                return bad_request();
            };
            let Ok(header_value) = HeaderValue::from_str(value) else {
                return bad_request();
            };
            builder = builder.header(header_name, header_value);
        }
        let Ok(request) = builder.body(Body::new(body)) else {
            return bad_request();
        };

        dispatch_request(&self.router, &self.runtime, request).await
    }
}

impl std::fmt::Debug for TestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestClient")
            .field("router", &self.router)
            .finish_non_exhaustive()
    }
}

fn bad_request() -> Response {
    let mut response = Response::new(Body::new(Bytes::from_static(b"Bad Request")));
    *response.status_mut() = StatusCode::BAD_REQUEST;
    response
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::TestClient;
    use http::StatusCode;
    use walastack_app::App;

    #[tokio::test]
    async fn dispatches_get_request() {
        let app = App::new().get("/", || async { "hello" });
        let client = TestClient::new(app);
        let response = client.get("/").await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn returns_404_for_unknown_path() {
        let client = TestClient::new(App::new());
        let response = client.get("/missing").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn dispatches_post_separately_from_get() {
        let app = App::new()
            .get("/items", || async { "list" })
            .post("/items", || async { "created" });
        let client = TestClient::new(app);

        assert_eq!(client.get("/items").await.status(), StatusCode::OK);
        assert_eq!(client.post("/items").await.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unknown_method_returns_404() {
        let app = App::new().get("/items", || async { "list" });
        let client = TestClient::new(app);
        assert_eq!(client.post("/items").await.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn dispatches_handler_with_path_extractor() {
        use walastack_http::Path;

        async fn greet(Path(name): Path<String>) -> String {
            format!("Hello, {name}!")
        }

        let app = App::new().get("/greet/:name", greet);
        let client = TestClient::new(app);
        let response = client.get("/greet/alice").await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}

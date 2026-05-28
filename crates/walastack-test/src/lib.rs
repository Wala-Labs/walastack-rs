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
use http::{Method, StatusCode};
use walastack_app::App;
use walastack_http::{Body, Response};
use walastack_router::Router;

/// In-memory test client for a WalaStack application.
///
/// Constructed from an [`App`]; dispatches requests directly through the
/// router. Returns the [`Response`] the handler produced, or a 404 if no
/// handler matched.
pub struct TestClient {
    router: Arc<Router>,
}

impl TestClient {
    /// Create a test client by consuming `app`.
    #[must_use]
    pub fn new(app: App) -> Self {
        Self {
            router: Arc::new(app.into_router()),
        }
    }

    /// Dispatch a `GET` request to `path`.
    pub async fn get(&self, path: &str) -> Response {
        self.dispatch(Method::GET, path).await
    }

    /// Dispatch a `POST` request to `path`.
    pub async fn post(&self, path: &str) -> Response {
        self.dispatch(Method::POST, path).await
    }

    /// Dispatch a `PUT` request to `path`.
    pub async fn put(&self, path: &str) -> Response {
        self.dispatch(Method::PUT, path).await
    }

    /// Dispatch a `DELETE` request to `path`.
    pub async fn delete(&self, path: &str) -> Response {
        self.dispatch(Method::DELETE, path).await
    }

    async fn dispatch(&self, method: Method, path: &str) -> Response {
        let Ok(mut request) = http::Request::builder()
            .method(method.clone())
            .uri(path)
            .body(Body::new(Bytes::new()))
        else {
            return bad_request();
        };

        if let Some((handler, path_params)) = self.router.dispatch(&method, path) {
            request.extensions_mut().insert(path_params);
            handler(request).await
        } else {
            not_found()
        }
    }
}

impl std::fmt::Debug for TestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestClient")
            .field("router", &self.router)
            .finish()
    }
}

fn not_found() -> Response {
    let mut response = Response::new(Body::new(Bytes::from_static(b"Not Found")));
    *response.status_mut() = StatusCode::NOT_FOUND;
    response
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

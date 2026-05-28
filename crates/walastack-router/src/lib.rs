//! # walastack-router
//!
//! Resource path matching and route registration for WalaStack.
//!
//! Built on a trie-backed router (`matchit`) for low-overhead path matching.
//! Phase 1 supports static and dynamic paths (`/users/:id`) with method-based
//! dispatch. Path parameters extracted during matching are passed alongside
//! the handler so the framework can insert them into request extensions.

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use http::Method;
use walastack_http::{PathParams, Request, Response};

/// A boxed, type-erased async handler stored in the router for runtime
/// dispatch.
///
/// Handlers take a [`Request`] and return a future producing a [`Response`].
/// The framework reads the request body into a single buffer before invoking
/// the handler.
pub type BoxedHandler =
    Box<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Response> + Send>> + Send + Sync>;

/// Router that maps `(method, path)` tuples to handlers and extracts path
/// parameters.
///
/// Internally maintains one `matchit::Router<BoxedHandler>` per HTTP method.
pub struct Router {
    routes: HashMap<Method, matchit::Router<BoxedHandler>>,
}

impl Router {
    /// Create an empty router.
    #[must_use]
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
        }
    }

    /// Register a handler for the given `method` and `path`.
    ///
    /// Path parameters use `:name` syntax (e.g. `/users/:id`). The router
    /// translates this internally to the underlying `matchit` 0.8+ syntax
    /// (`{name}`) so user code keeps the conventional form.
    ///
    /// Phase 1 deliberately panics on invalid patterns — this is a developer
    /// error caught at startup, not a runtime concern.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn route(mut self, method: Method, path: &str, handler: BoxedHandler) -> Self {
        let entry = self.routes.entry(method).or_default();
        let translated = translate_path(path);
        entry
            .insert(translated, handler)
            .expect("walastack-router: invalid route pattern");
        self
    }

    /// Find a handler for the given `method` and `path`, also returning any
    /// extracted path parameters.
    ///
    /// Returns `None` if no handler is registered for the combination.
    #[must_use]
    pub fn dispatch(&self, method: &Method, path: &str) -> Option<(&BoxedHandler, PathParams)> {
        let m = self.routes.get(method)?.at(path).ok()?;
        let params: HashMap<String, String> = m
            .params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Some((m.value, PathParams(params)))
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let methods: Vec<&Method> = self.routes.keys().collect();
        f.debug_struct("Router").field("methods", &methods).finish()
    }
}

/// Translate WalaStack route syntax (`:name`) to `matchit` 0.8+ syntax
/// (`{name}`).
///
/// Handles named parameters; catch-all wildcards (`*name`) land in a later
/// phase if needed.
fn translate_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut iter = path.chars().peekable();
    while let Some(c) = iter.next() {
        if c == ':' {
            out.push('{');
            while let Some(&next) = iter.peek() {
                if next == '/' {
                    break;
                }
                out.push(next);
                iter.next();
            }
            out.push('}');
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::{BoxedHandler, Router};
    use bytes::Bytes;
    use http::{Method, StatusCode};
    use walastack_http::{Body, Request, Response};

    fn make_handler(text: &'static str) -> BoxedHandler {
        Box::new(move |_req: Request| {
            Box::pin(async move { Response::new(Body::new(Bytes::from_static(text.as_bytes()))) })
        })
    }

    fn empty_request(method: Method, path: &str) -> Request {
        http::Request::builder()
            .method(method)
            .uri(path)
            .body(Body::new(Bytes::new()))
            .unwrap()
    }

    #[tokio::test]
    async fn dispatch_finds_registered_handler() {
        let router = Router::new().route(Method::GET, "/", make_handler("ok"));
        let (handler, _params) = router.dispatch(&Method::GET, "/").unwrap();
        let req = empty_request(Method::GET, "/");
        let response = handler(req).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn dispatch_returns_none_for_unknown_path() {
        let router = Router::new();
        assert!(router.dispatch(&Method::GET, "/").is_none());
    }

    #[test]
    fn dispatch_returns_none_for_unknown_method() {
        let router = Router::new().route(Method::GET, "/", make_handler("ok"));
        assert!(router.dispatch(&Method::POST, "/").is_none());
    }

    #[test]
    fn dispatch_extracts_path_params() {
        let router = Router::new().route(Method::GET, "/users/:id", make_handler("ok"));
        let (_handler, params) = router.dispatch(&Method::GET, "/users/42").unwrap();
        assert_eq!(params.0.get("id").unwrap(), "42");
    }

    #[test]
    fn translate_path_converts_colon_to_brace_syntax() {
        use super::translate_path;
        assert_eq!(translate_path("/users/:id"), "/users/{id}");
        assert_eq!(
            translate_path("/users/:id/posts/:post_id"),
            "/users/{id}/posts/{post_id}"
        );
        assert_eq!(translate_path("/static/path"), "/static/path");
    }
}

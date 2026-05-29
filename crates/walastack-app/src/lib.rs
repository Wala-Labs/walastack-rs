//! # walastack-app
//!
//! User-facing application framework primitive for WalaStack.
//!
//! Provides the [`App`] builder and the [`Handler`] trait. Handlers can take
//! zero, one, or two parameters; each parameter must implement either
//! [`FromRequestParts`] (for borrowing extractors like `Path<T>` and
//! `Query<T>`) or [`FromRequest`] (for body-consuming extractors like
//! `Request`). The last parameter in a multi-parameter handler is the only
//! one allowed to be `FromRequest`-only.
//!
//! # Example
//!
//! ```no_run
//! use walastack_app::App;
//! use walastack_http::Path;
//!
//! async fn index() -> &'static str { "hello" }
//!
//! async fn greet(Path(name): Path<String>) -> String {
//!     format!("Hello, {name}!")
//! }
//!
//! #[tokio::main]
//! async fn main() -> walastack_http::Result<()> {
//!     App::new()
//!         .get("/", index)
//!         .get("/greet/:name", greet)
//!         .run("127.0.0.1:3000")
//!         .await
//! }
//! ```

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
// tokio::select! generates pub(crate) items inside a private module.
#![allow(clippy::redundant_pub_crate)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http::Method;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use walastack_http::{
    Body, Error, FromRequest, FromRequestParts, IntoResponse, Request, Response, Result,
};
use walastack_router::{BoxedHandler, Router};
use walastack_runtime::{
    BoxedServiceFuture, Runtime, Service, ServiceContext, ServiceError, ShutdownSignal,
};

/// Trait for types that register themselves as routes on an [`App`].
///
/// Implemented by the route attribute macros (`#[get("/")]`, `#[post("/")]`,
/// etc.) in `walastack-macros` — those macros generate a unit struct that
/// implements this trait by registering the original handler at the
/// macro-declared path and method.
pub trait Route {
    /// Register this route on the given app, consuming the app and returning
    /// it with the route added.
    fn register(self, app: App) -> App;
}

/// Trait for handler functions registered on an [`App`].
///
/// Implemented via blanket impls for `Fn` closures and function items that
/// return a future of `impl IntoResponse`, taking zero or more extractor
/// parameters.
///
/// The type parameter `P` is the handler's parameter tuple — `()` for arity 0,
/// `(P1,)` for arity 1, `(P1, P2)` for arity 2. The compiler infers it from
/// the handler's signature; user code never names it explicitly.
pub trait Handler<P>: Clone + Send + Sync + 'static {
    /// Invoke the handler against the given request, producing the response.
    fn call(self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>>;
}

// --- Arity 0: async fn() -> R ---

impl<F, Fut, R> Handler<()> for F
where
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse + 'static,
{
    fn call(self, _req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        Box::pin(async move { self().await.into_response() })
    }
}

// --- Arity 1: async fn(P) -> R where P: FromRequest ---

impl<F, Fut, R, P> Handler<(P,)> for F
where
    F: Fn(P) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse + 'static,
    P: FromRequest,
{
    fn call(self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        Box::pin(async move {
            let p = match P::from_request(req).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            self(p).await.into_response()
        })
    }
}

// --- Arity 2: async fn(P1, P2) -> R where P1: FromRequestParts, P2: FromRequest ---

impl<F, Fut, R, P1, P2> Handler<(P1, P2)> for F
where
    F: Fn(P1, P2) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse + 'static,
    P1: FromRequestParts,
    P2: FromRequest,
{
    fn call(self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        Box::pin(async move {
            let (mut parts, body) = req.into_parts();
            let p1 = match P1::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let req2 = Request::from_parts(parts, body);
            let p2 = match P2::from_request(req2).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            self(p1, p2).await.into_response()
        })
    }
}

// --- Arity 3: async fn(P1, P2, P3) -> R; first two are FromRequestParts, last is FromRequest ---

impl<F, Fut, R, P1, P2, P3> Handler<(P1, P2, P3)> for F
where
    F: Fn(P1, P2, P3) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse + 'static,
    P1: FromRequestParts,
    P2: FromRequestParts,
    P3: FromRequest,
{
    fn call(self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        Box::pin(async move {
            let (mut parts, body) = req.into_parts();
            let p1 = match P1::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let p2 = match P2::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let req3 = Request::from_parts(parts, body);
            let p3 = match P3::from_request(req3).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            self(p1, p2, p3).await.into_response()
        })
    }
}

// --- Arity 4 ---

impl<F, Fut, R, P1, P2, P3, P4> Handler<(P1, P2, P3, P4)> for F
where
    F: Fn(P1, P2, P3, P4) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse + 'static,
    P1: FromRequestParts,
    P2: FromRequestParts,
    P3: FromRequestParts,
    P4: FromRequest,
{
    fn call(self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        Box::pin(async move {
            let (mut parts, body) = req.into_parts();
            let p1 = match P1::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let p2 = match P2::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let p3 = match P3::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let req4 = Request::from_parts(parts, body);
            let p4 = match P4::from_request(req4).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            self(p1, p2, p3, p4).await.into_response()
        })
    }
}

// --- Arity 5 ---

impl<F, Fut, R, P1, P2, P3, P4, P5> Handler<(P1, P2, P3, P4, P5)> for F
where
    F: Fn(P1, P2, P3, P4, P5) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse + 'static,
    P1: FromRequestParts,
    P2: FromRequestParts,
    P3: FromRequestParts,
    P4: FromRequestParts,
    P5: FromRequest,
{
    fn call(self, req: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        Box::pin(async move {
            let (mut parts, body) = req.into_parts();
            let p1 = match P1::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let p2 = match P2::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let p3 = match P3::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let p4 = match P4::from_request_parts(&mut parts).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            let req5 = Request::from_parts(parts, body);
            let p5 = match P5::from_request(req5).await {
                Ok(p) => p,
                Err(rejection) => return rejection.into_response(),
            };
            self(p1, p2, p3, p4, p5).await.into_response()
        })
    }
}

/// The WalaStack application builder.
///
/// Compose routes via the method helpers (`.get`, `.post`, `.put`, `.delete`)
/// or the macro-driven [`App::route`] method, then call [`App::run`] to bind
/// to a socket and serve indefinitely.
pub struct App {
    router: Router,
}

impl App {
    /// Create a new empty application.
    #[must_use]
    pub fn new() -> Self {
        Self {
            router: Router::new(),
        }
    }

    /// Register a `GET` handler for `path`.
    #[must_use]
    pub fn get<H, P>(self, path: &str, handler: H) -> Self
    where
        H: Handler<P>,
        P: 'static,
    {
        self.add_route(Method::GET, path, handler)
    }

    /// Register a `POST` handler for `path`.
    #[must_use]
    pub fn post<H, P>(self, path: &str, handler: H) -> Self
    where
        H: Handler<P>,
        P: 'static,
    {
        self.add_route(Method::POST, path, handler)
    }

    /// Register a `PUT` handler for `path`.
    #[must_use]
    pub fn put<H, P>(self, path: &str, handler: H) -> Self
    where
        H: Handler<P>,
        P: 'static,
    {
        self.add_route(Method::PUT, path, handler)
    }

    /// Register a `DELETE` handler for `path`.
    #[must_use]
    pub fn delete<H, P>(self, path: &str, handler: H) -> Self
    where
        H: Handler<P>,
        P: 'static,
    {
        self.add_route(Method::DELETE, path, handler)
    }

    /// Consume the app and return its router.
    ///
    /// Useful for testing helpers — `walastack-test::TestClient` wraps a
    /// `Router` directly so test dispatches don't bind a socket or go through
    /// `hyper`.
    #[must_use]
    pub fn into_router(self) -> Router {
        self.router
    }

    /// Register a route from a macro-generated route type.
    ///
    /// Used in combination with the route attribute macros:
    ///
    /// ```ignore
    /// use walastack::prelude::*;
    ///
    /// #[get("/")]
    /// async fn index() -> &'static str { "hello" }
    ///
    /// let app = App::new().route(index);
    /// ```
    #[must_use]
    pub fn route<R: Route>(self, route: R) -> Self {
        route.register(self)
    }

    fn add_route<H, P>(mut self, method: Method, path: &str, handler: H) -> Self
    where
        H: Handler<P>,
        P: 'static,
    {
        let boxed: BoxedHandler = Box::new(move |req: Request| {
            let h = handler.clone();
            Box::pin(h.call(req))
        });
        self.router = self.router.route(method, path, boxed);
        self
    }

    /// Bind to `addr` and serve until the kernel shutdown signal fires.
    ///
    /// `addr` is parsed as a [`SocketAddr`] (e.g. `"127.0.0.1:3000"`).
    /// Returns `Err` if the address is malformed, the socket cannot be
    /// bound, or the kernel returns a startup error.
    ///
    /// Internally this constructs a [`Runtime`] with a single
    /// [`HttpService`] and runs it. To compose the HTTP surface with
    /// other Services, drop down to [`Runtime::builder`] directly and
    /// add the [`HttpService`] explicitly via [`App::into_http_service`].
    pub async fn run(self, addr: impl AsRef<str> + Send) -> Result<()> {
        let parsed = parse_addr(addr.as_ref())?;
        let http = HttpService::new(parsed, self.router);

        Runtime::builder()
            .with(http)
            .run()
            .await
            .map_err(|e| Error::Custom(e.to_string()))
    }

    /// Consume the app and return an [`HttpService`] bound to `addr`.
    ///
    /// Useful for composing the HTTP surface alongside other Services in
    /// an explicit [`Runtime::builder`] composition:
    ///
    /// ```no_run
    /// # use walastack_app::App;
    /// # use walastack_runtime::Runtime;
    /// # async fn _example() -> walastack_runtime::RuntimeError {
    /// let http = App::new()
    ///     .get("/", || async { "hello" })
    ///     .into_http_service("127.0.0.1:3000")
    ///     .expect("valid addr");
    ///
    /// Runtime::builder()
    ///     .with(http)
    ///     .run()
    ///     .await
    ///     .unwrap_err()
    /// # }
    /// ```
    pub fn into_http_service(self, addr: impl AsRef<str>) -> Result<HttpService> {
        let parsed = parse_addr(addr.as_ref())?;
        Ok(HttpService::new(parsed, self.router))
    }
}

fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse()
        .map_err(|e: std::net::AddrParseError| Error::InvalidAddress(format!("{addr}: {e}")))
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App").field("router", &self.router).finish()
    }
}

async fn serve_request(router: &Router, req: hyper::Request<Incoming>) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let (parts, body) = req.into_parts();
    let body_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            tracing::warn!(error = %e, "failed to read request body");
            return bad_request("failed to read request body");
        }
    };
    let walastack_body = Body::new(body_bytes);
    let mut walastack_req = Request::from_parts(parts, walastack_body);

    if let Some((handler, path_params)) = router.dispatch(&method, &path) {
        walastack_req.extensions_mut().insert(path_params);
        handler(walastack_req).await
    } else {
        not_found()
    }
}

fn not_found() -> Response {
    let mut response = Response::new(Body::new(Bytes::from_static(b"Not Found")));
    *response.status_mut() = http::StatusCode::NOT_FOUND;
    response
}

fn bad_request(message: &'static str) -> Response {
    let mut response = Response::new(Body::new(Bytes::from_static(message.as_bytes())));
    *response.status_mut() = http::StatusCode::BAD_REQUEST;
    response
}

// =========================================================================
// HttpService
// =========================================================================

/// The HTTP transport [`Service`].
///
/// Constructed via [`App::into_http_service`] or directly via
/// [`HttpService::new`]. Registered with a [`Runtime`] through
/// [`walastack_runtime::RuntimeBuilder::with`]:
///
/// ```no_run
/// # use walastack_app::App;
/// # use walastack_runtime::Runtime;
/// # async fn _example() -> walastack_runtime::RuntimeError {
/// let http = App::new()
///     .get("/health", || async { "ok" })
///     .into_http_service("127.0.0.1:3000")
///     .expect("valid addr");
///
/// Runtime::builder()
///     .with(http)
///     .run()
///     .await
///     .unwrap_err()
/// # }
/// ```
///
/// The accept loop subscribes to the kernel shutdown signal via
/// [`ServiceContext::shutdown_signal`] and drains cleanly when the
/// kernel signals shutdown.
pub struct HttpService {
    addr: SocketAddr,
    router: Arc<Router>,
}

impl HttpService {
    /// Construct an `HttpService` bound to the given address with the
    /// given router.
    #[must_use]
    pub fn new(addr: SocketAddr, router: Router) -> Self {
        Self {
            addr,
            router: Arc::new(router),
        }
    }

    /// The bound address.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Service for HttpService {
    fn name(&self) -> &'static str {
        "http"
    }

    fn start(
        &self,
        ctx: ServiceContext,
    ) -> BoxedServiceFuture<core::result::Result<JoinHandle<()>, ServiceError>> {
        let addr = self.addr;
        let router = Arc::clone(&self.router);
        Box::pin(async move {
            let listener = TcpListener::bind(addr).await.map_err(ServiceError::from)?;
            tracing::info!(%addr, "walastack listening");
            let shutdown = ctx.shutdown_signal();
            let handle = tokio::spawn(accept_loop(listener, router, shutdown));
            Ok(handle)
        })
    }
}

impl std::fmt::Debug for HttpService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpService")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

async fn accept_loop(listener: TcpListener, router: Arc<Router>, mut shutdown: ShutdownSignal) {
    loop {
        tokio::select! {
            () = shutdown.wait() => {
                tracing::info!("walastack shutdown signal received");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, peer_addr)) => {
                        let router = Arc::clone(&router);
                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let service = service_fn(move |req: hyper::Request<Incoming>| {
                                let router = Arc::clone(&router);
                                async move {
                                    let response = serve_request(&router, req).await;
                                    Ok::<_, std::convert::Infallible>(response)
                                }
                            });
                            if let Err(err) =
                                http1::Builder::new().serve_connection(io, service).await
                            {
                                tracing::warn!(%peer_addr, error = %err, "connection error");
                            }
                        });
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "accept error; continuing");
                    }
                }
            }
        }
    }
}

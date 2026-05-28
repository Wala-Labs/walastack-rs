//! # walastack
//!
//! Umbrella crate for the WalaStack ecosystem.
//!
//! Re-exports the primary user-facing framework API and provides a
//! [`prelude`] module of common imports. Most applications import from this
//! crate rather than from the individual `walastack-*` crates directly.
//!
//! # Example
//!
//! ```no_run
//! use walastack::prelude::*;
//!
//! async fn index() -> &'static str {
//!     "Hello, WalaStack!"
//! }
//!
//! #[tokio::main]
//! async fn main() -> walastack::Result<()> {
//!     App::new().get("/", index).run("127.0.0.1:3000").await
//! }
//! ```

pub use walastack_app::{App, Handler, Route};
pub use walastack_http::{
    Body, Bytes, Error, ExtractionRejection, FromRequest, FromRequestParts, HeaderMap, HeaderName,
    HeaderValue, Html, IntoResponse, Json, Method, Path, PathParams, Query, Request, Response,
    Result, StatusCode, Uri, Version, header,
};
pub use walastack_macros::{delete, get, main, post, put};
pub use walastack_router::Router;
pub use walastack_runtime::{init_tracing, wait_for_shutdown_signal};

/// Internal re-exports used by procedural macros. Not part of the public API.
#[doc(hidden)]
pub mod __macro_support {
    pub use tokio;
}

/// Common imports for WalaStack applications.
///
/// Glob-import to bring the canonical framework types into scope:
///
/// ```rust
/// use walastack::prelude::*;
/// ```
pub mod prelude {
    pub use crate::{
        App, Body, Bytes, FromRequest, FromRequestParts, Handler, HeaderMap, Html, IntoResponse,
        Json, Method, Path, Query, Request, Response, Result, Route, StatusCode, Uri, delete, get,
        post, put,
    };
}

#[cfg(test)]
mod tests {
    /// Smoke test: the umbrella crate compiles and the prelude module exists.
    /// Once `prelude` exports real items, this test will exercise them.
    #[test]
    const fn smoke() {}
}

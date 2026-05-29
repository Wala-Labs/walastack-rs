//! `walastack` example — typed responders + the full extractor suite.
//!
//! Demonstrates:
//! - `#[walastack::main]` + `#[get(...)]` + `#[post(...)]` macros
//! - `Html<T>` and `Json<T>` typed responders
//! - `Path<T>` extractor (URL path parameters)
//! - `Query<T>` extractor (URL query strings, with `serde::Deserialize` struct)
//! - `Json<T>` extractor (JSON request bodies)
//! - `HeaderMap` extractor (request headers)
//! - Multi-parameter handlers via the higher-arity `Handler` impls
//!
//! Run with:
//!
//! ```bash
//! cargo run -p hello-world
//! ```
//!
//! Then in another terminal:
//!
//! ```bash
//! curl http://127.0.0.1:3000
//! curl http://127.0.0.1:3000/greet/Alice
//! curl 'http://127.0.0.1:3000/search?q=rust&limit=5'
//! curl -H 'X-Trace-Id: abc' http://127.0.0.1:3000/headers
//! curl -X POST -H 'Content-Type: application/json' \
//!      -d '{"name":"World","loud":true}' \
//!      http://127.0.0.1:3000/echo
//! curl http://127.0.0.1:3000/health
//! ```

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use walastack::prelude::*;

#[get("/")]
async fn index() -> Html<&'static str> {
    Html("<h1>Hello, WalaStack!</h1>")
}

#[get("/greet/:name")]
async fn greet(Path(name): Path<String>) -> String {
    format!("Hello, {name}!")
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: String,
    #[serde(default = "default_limit")]
    limit: u32,
}

const fn default_limit() -> u32 {
    10
}

#[get("/search")]
async fn search(Query(params): Query<SearchParams>) -> String {
    format!("Searching for '{}' (limit {})", params.q, params.limit)
}

#[get("/headers")]
async fn show_headers(headers: HeaderMap) -> String {
    let mut output = String::from("Request headers:\n");
    for (name, value) in &headers {
        let value_str = value.to_str().unwrap_or("<binary>");
        let _ = writeln!(output, "  {name}: {value_str}");
    }
    output
}

#[derive(Debug, Deserialize)]
struct EchoRequest {
    name: String,
    #[serde(default)]
    loud: bool,
}

#[derive(Debug, Serialize)]
struct EchoResponse {
    message: String,
}

#[post("/echo")]
async fn echo(Json(body): Json<EchoRequest>) -> Json<EchoResponse> {
    let message = if body.loud {
        format!("HELLO, {}!", body.name.to_uppercase())
    } else {
        format!("Hello, {}!", body.name)
    };
    Json(EchoResponse { message })
}

#[get("/health")]
async fn health() -> &'static str {
    "ok"
}

#[walastack::main]
async fn main() -> walastack::Result<()> {
    App::new()
        .route(index)
        .route(greet)
        .route(search)
        .route(show_headers)
        .route(echo)
        .route(health)
        .run("127.0.0.1:3000")
        .await
}

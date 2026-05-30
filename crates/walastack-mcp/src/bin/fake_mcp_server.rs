//! Fake MCP server fixture used by `tests/stdio_integration.rs`.
//!
//! Implements the minimum subset of MCP needed to validate the stdio
//! transport + JSON-RPC client:
//!
//! - `initialize` — returns `serverInfo`.
//! - `notifications/initialized` — accepted, no-op.
//! - `tools/list` — returns a single hardcoded `echo` tool.
//! - `tools/call` for `echo` — returns the arguments unchanged.
//! - `tools/call` for `fail` — returns a JSON-RPC error.
//! - `resources/list` — returns one hardcoded resource at
//!   `memo://greeting`.
//! - `resources/read` for `memo://greeting` — returns the text
//!   `"hello from fake mcp"`.
//! - `ping` — returns an empty result. Used by the
//!   `McpServerService` liveness loop.
//!
//! Unrecognized methods return a `-32601 method not found` error so
//! tests can also assert error propagation through the client.
//!
//! Reads newline-delimited JSON from stdin; writes newline-delimited
//! JSON to stdout. Exits on EOF.

#![allow(clippy::expect_used, clippy::unwrap_used)]
// Test fixture — readability over linter preferences.
#![allow(
    clippy::doc_markdown,
    clippy::match_same_arms,
    clippy::print_stderr,
    clippy::needless_pass_by_value
)]

use std::io::{self, BufRead, Write};

use serde_json::{Value, json};

fn respond(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn respond_error(id: Value, code: i32, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
}

fn handle(message: Value) -> Option<Value> {
    let id = message.get("id").cloned();
    let method = message
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let params = message.get("params").cloned().unwrap_or(Value::Null);

    match (id, method.as_str()) {
        // Notifications — no response.
        (None, "notifications/initialized") => None,
        // Requests.
        (Some(id), "initialize") => Some(respond(
            id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "serverInfo": {
                    "name": "walastack-mcp-fake-server",
                    "version": env!("CARGO_PKG_VERSION"),
                },
            }),
        )),
        (Some(id), "tools/list") => Some(respond(
            id,
            json!({
                "tools": [
                    {
                        "name": "echo",
                        "description": "Returns its arguments unchanged",
                    },
                    {
                        "name": "fail",
                        "description": "Always fails with a structured error",
                    }
                ]
            }),
        )),
        (Some(id), "tools/call") => {
            let tool = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(Value::Null);
            match tool {
                "echo" => Some(respond(id, args)),
                "fail" => Some(respond_error(id, -32000, "intentional failure")),
                other => Some(respond_error(
                    id,
                    -32602,
                    &format!("unknown tool {other:?}"),
                )),
            }
        }
        (Some(id), "resources/list") => Some(respond(
            id,
            json!({
                "resources": [
                    {
                        "uri": "memo://greeting",
                        "name": "greeting",
                        "mimeType": "text/plain",
                    }
                ]
            }),
        )),
        (Some(id), "resources/read") => {
            let uri = params.get("uri").and_then(Value::as_str).unwrap_or("");
            if uri == "memo://greeting" {
                Some(respond(
                    id,
                    json!({
                        "contents": [
                            {
                                "uri": uri,
                                "mimeType": "text/plain",
                                "text": "hello from fake mcp",
                            }
                        ]
                    }),
                ))
            } else {
                Some(respond_error(
                    id,
                    -32602,
                    &format!("unknown resource {uri:?}"),
                ))
            }
        }
        (Some(id), "ping") => Some(respond(id, json!({}))),
        (Some(id), other) => Some(respond_error(
            id,
            -32601,
            &format!("method not found: {other}"),
        )),
        (None, _) => None,
    }
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let message: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("fake_mcp_server: parse error: {e} on line {line:?}");
                continue;
            }
        };
        if let Some(response) = handle(message) {
            let payload = serde_json::to_string(&response).expect("serialize response");
            writeln!(out, "{payload}")?;
            out.flush()?;
        }
    }
    Ok(())
}

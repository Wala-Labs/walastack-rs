//! Minimal JSON-RPC 2.0 primitives.
//!
//! Implements just enough of the spec for MCP client traffic over a
//! line-delimited transport: requests, responses, errors, and
//! notifications. Per the locked architecture proposal, we roll our
//! own client rather than pulling in a third-party JSON-RPC crate —
//! the scope is bounded enough that a focused ~150-line module is
//! cleaner than a transitive dep.

use serde::{Deserialize, Serialize};

/// A JSON-RPC 2.0 request envelope.
///
/// Requests always carry an `id` so the server's response can be
/// matched. Notifications use [`JsonRpcNotification`] instead.
#[derive(Clone, Debug, Serialize)]
pub struct JsonRpcRequest<'a> {
    /// Always `"2.0"`.
    pub jsonrpc: &'static str,
    /// Numeric id allocated by the client.
    pub id: u64,
    /// Method name (e.g., `"initialize"`, `"tools/call"`).
    pub method: &'a str,
    /// Method parameters. Use `serde_json::Value::Null` if none.
    pub params: serde_json::Value,
}

impl<'a> JsonRpcRequest<'a> {
    /// Construct a new request.
    #[must_use]
    pub const fn new(id: u64, method: &'a str, params: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            method,
            params,
        }
    }
}

/// A JSON-RPC 2.0 notification — no `id`, no response expected.
#[derive(Clone, Debug, Serialize)]
pub struct JsonRpcNotification<'a> {
    /// Always `"2.0"`.
    pub jsonrpc: &'static str,
    /// Method name.
    pub method: &'a str,
    /// Method parameters.
    pub params: serde_json::Value,
}

impl<'a> JsonRpcNotification<'a> {
    /// Construct a notification.
    #[must_use]
    pub const fn new(method: &'a str, params: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0",
            method,
            params,
        }
    }
}

/// A JSON-RPC 2.0 response envelope.
///
/// Exactly one of `result` or `error` is `Some` per the spec.
#[derive(Clone, Debug, Deserialize)]
pub struct JsonRpcResponse {
    /// The id from the matching request.
    pub id: u64,
    /// On success.
    #[serde(default)]
    pub result: Option<serde_json::Value>,
    /// On failure.
    #[serde(default)]
    pub error: Option<JsonRpcError>,
}

/// A JSON-RPC 2.0 error object.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct JsonRpcError {
    /// Spec-defined or implementation-specific error code.
    pub code: i32,
    /// Sanitized human-readable description.
    pub message: String,
    /// Optional implementation-specific extra data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// One incoming JSON-RPC message — either a response to a previous
/// request, or an inbound notification (e.g.,
/// `notifications/tools/list_changed`).
#[derive(Debug)]
pub enum IncomingMessage {
    /// A response keyed by the matching request id.
    Response(JsonRpcResponse),
    /// An inbound notification. Iteration 1 does not act on these
    /// beyond optional `tracing::debug!` logging.
    Notification {
        /// Method name.
        method: String,
        /// Notification parameters.
        params: serde_json::Value,
    },
}

/// Parse a single line of newline-delimited JSON into an
/// [`IncomingMessage`].
pub fn parse_incoming(line: &str) -> Result<IncomingMessage, serde_json::Error> {
    // Both responses and notifications are JSON objects. We
    // distinguish by the presence of the `id` field — responses
    // always have one; notifications never do.
    let value: serde_json::Value = serde_json::from_str(line)?;
    if value.get("id").is_some() {
        let response: JsonRpcResponse = serde_json::from_value(value)?;
        Ok(IncomingMessage::Response(response))
    } else {
        let method = value
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        let params = value
            .get("params")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        Ok(IncomingMessage::Notification { method, params })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn request_serializes_jsonrpc_envelope() {
        let req = JsonRpcRequest::new(1, "tools/call", serde_json::json!({"name": "echo"}));
        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains("\"jsonrpc\":\"2.0\""));
        assert!(s.contains("\"id\":1"));
        assert!(s.contains("\"method\":\"tools/call\""));
    }

    #[test]
    fn notification_omits_id() {
        let n = JsonRpcNotification::new("notifications/initialized", serde_json::json!({}));
        let s = serde_json::to_string(&n).unwrap();
        assert!(s.contains("\"jsonrpc\":\"2.0\""));
        assert!(s.contains("\"method\":\"notifications/initialized\""));
        assert!(!s.contains("\"id\""));
    }

    #[test]
    fn parse_incoming_handles_response_with_result() {
        let line = r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#;
        match parse_incoming(line).unwrap() {
            IncomingMessage::Response(r) => {
                assert_eq!(r.id, 1);
                assert!(r.result.is_some());
                assert!(r.error.is_none());
            }
            IncomingMessage::Notification { .. } => panic!("expected response"),
        }
    }

    #[test]
    fn parse_incoming_handles_response_with_error() {
        let line =
            r#"{"jsonrpc":"2.0","id":7,"error":{"code":-32601,"message":"method not found"}}"#;
        match parse_incoming(line).unwrap() {
            IncomingMessage::Response(r) => {
                let err = r.error.unwrap();
                assert_eq!(err.code, -32601);
                assert_eq!(err.message, "method not found");
            }
            IncomingMessage::Notification { .. } => panic!("expected response"),
        }
    }

    #[test]
    fn parse_incoming_handles_notification() {
        let line = r#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{}}"#;
        match parse_incoming(line).unwrap() {
            IncomingMessage::Notification { method, .. } => {
                assert_eq!(method, "notifications/tools/list_changed");
            }
            IncomingMessage::Response(_) => panic!("expected notification"),
        }
    }
}

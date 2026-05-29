//! End-to-end integration: `App` + `OpenApiPlugin` + `TestClient`
//! produce a parseable OpenAPI 3.0 document at the configured path.

#![allow(clippy::unwrap_used)]
#![allow(clippy::doc_markdown)]

use walastack_app::App;
use walastack_openapi::{OpenApiConfig, OpenApiPlugin, RouteSpec, Schema};
use walastack_test::TestClient;

async fn list_users() -> &'static str {
    "[]"
}

async fn get_user() -> &'static str {
    "{}"
}

#[tokio::test]
async fn openapi_serve_at_returns_document_through_test_client() {
    let config = OpenApiConfig::new("Hello", "1.0.0").with_description("integration test");

    let app = App::new()
        .with_plugin(OpenApiPlugin::new(config))
        .openapi_route(
            list_users,
            RouteSpec::get("/users")
                .summary("List users")
                .response::<Vec<String>>(200),
        )
        .openapi_route(
            get_user,
            RouteSpec::get("/users/:id")
                .summary("Get user")
                .path_param("id", Schema::string())
                .response::<String>(200),
        )
        .openapi_serve_at("/openapi.json");

    // Build a runtime mirroring what App::run() builds, so the
    // TestClient dispatch path injects the same RuntimeContext that
    // production HttpService would.
    let runtime = walastack_runtime::Runtime::builder()
        .with_plugin(OpenApiPlugin::new(OpenApiConfig::new("Hello", "1.0.0")))
        .build()
        .unwrap();
    let client = TestClient::with_runtime(app, &runtime);

    let response = client.get("/openapi.json").await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap()),
        Some("application/json")
    );

    let body = collect_body(response).await;
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(doc["openapi"], "3.0.3");
    assert_eq!(doc["info"]["title"], "Hello");
    assert_eq!(doc["info"]["version"], "1.0.0");
    // Path was normalized from `:id` to `{id}`.
    assert_eq!(doc["paths"]["/users/{id}"]["get"]["summary"], "Get user");
    assert_eq!(doc["paths"]["/users"]["get"]["summary"], "List users");
    // Response schema reached the wire.
    assert_eq!(
        doc["paths"]["/users"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["type"],
        "array"
    );
}

#[tokio::test]
async fn openapi_serve_at_returns_500_when_plugin_not_attached() {
    let app = App::new().openapi_serve_at("/openapi.json");
    let client = TestClient::new(app);
    let response = client.get("/openapi.json").await;
    assert_eq!(response.status(), http::StatusCode::INTERNAL_SERVER_ERROR);
}

async fn collect_body(response: walastack_http::Response) -> bytes::Bytes {
    use http_body_util::BodyExt;
    response.into_body().collect().await.unwrap().to_bytes()
}

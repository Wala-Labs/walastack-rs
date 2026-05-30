//! End-to-end smoke test for `hello-auth-db-openapi`.
//!
//! Verifies that the OpenAPI document is served correctly AND that
//! the existing signup / me round-trip from hello-auth-db still
//! works through the new composition.

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use http_body_util::BodyExt;
use serde::Deserialize;
use sqlx::SqlitePool;
use walastack_auth::{AuthPlugin, InMemorySecretsPlugin, JwtConfig};
use walastack_db::sqlite::SqlitePlugin;
use walastack_openapi::{OpenApiConfig, OpenApiPlugin};
use walastack_runtime::Runtime;
use walastack_test::TestClient;

#[path = "../src/main.rs"]
mod app;

const JWT_KEY_NAME: &str = "jwt";
const JWT_ISSUER: &str = "hello-auth-db-openapi";

#[derive(Deserialize)]
struct TokenResponse {
    token: String,
}

async fn build_test_runtime() -> (Runtime, TestClient) {
    let db_plugin = SqlitePlugin::in_memory();
    let pool: SqlitePool = db_plugin.pool().clone();
    app::migrate(&pool).await.expect("migrate");

    let runtime = Runtime::builder()
        .with_plugin(db_plugin)
        .with_plugin(InMemorySecretsPlugin::new().with(JWT_KEY_NAME, b"test-signing-key"))
        .with_plugin(AuthPlugin::new().with_jwt(JWT_KEY_NAME, JwtConfig::new(JWT_ISSUER)))
        .with_plugin(OpenApiPlugin::new(OpenApiConfig::new(
            "hello-auth-db-openapi",
            "0.1.0",
        )))
        .build()
        .expect("runtime builds");
    app::set_runtime_context(runtime.context().clone());

    let test_app = app::build_app();
    let client = TestClient::with_runtime(test_app, &runtime);
    (runtime, client)
}

async fn read_json<T: serde::de::DeserializeOwned>(response: walastack_http::Response) -> T {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).expect("valid JSON response")
}

#[tokio::test]
async fn openapi_json_lists_all_routes_with_correct_metadata() {
    let (_runtime, client) = build_test_runtime().await;

    let response = client.get("/openapi.json").await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let doc: serde_json::Value = read_json(response).await;

    // Document-level metadata.
    assert_eq!(doc["openapi"], "3.0.3");
    assert_eq!(doc["info"]["title"], "hello-auth-db-openapi");

    // All four routes present.
    assert!(doc["paths"]["/health"]["get"].is_object());
    assert!(doc["paths"]["/signup"]["post"].is_object());
    assert!(doc["paths"]["/login"]["post"].is_object());
    assert!(doc["paths"]["/me"]["get"].is_object());

    // The hand-built schema reached the wire.
    let signup_req =
        &doc["paths"]["/signup"]["post"]["requestBody"]["content"]["application/json"]["schema"];
    assert_eq!(signup_req["type"], "object");
    assert_eq!(signup_req["properties"]["email"]["type"], "string");
    assert_eq!(signup_req["properties"]["email"]["format"], "email");
    assert!(signup_req["required"].as_array().unwrap().len() >= 2);
}

#[tokio::test]
async fn signup_login_me_still_works_under_4_crate_composition() {
    let (_runtime, client) = build_test_runtime().await;

    let signup = client
        .post_json(
            "/signup",
            &serde_json::json!({"email": "alice@example.test", "password": "hunter2"}),
        )
        .await;
    assert_eq!(signup.status(), http::StatusCode::OK);
    let token = read_json::<TokenResponse>(signup).await.token;

    let me = client
        .get_with_headers("/me", &[("authorization", &format!("Bearer {token}"))])
        .await;
    assert_eq!(me.status(), http::StatusCode::OK);
}

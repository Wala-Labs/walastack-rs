//! End-to-end smoke test for `hello-auth-db`.
//!
//! Drives the full Plugin composition + handler chain through
//! `walastack-test::TestClient` without binding a socket. Confirms:
//!
//! - The composed runtime (db + secrets + auth) builds cleanly.
//! - `POST /signup` issues a JWT for a new user.
//! - `POST /signup` returns `409 Conflict` for a duplicate email.
//! - `POST /login` rejects bad credentials with `401 Unauthorized`.
//! - `GET /me` returns the user's identity when called with a valid
//!   Bearer token, and `401 Unauthorized` without one.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use http_body_util::BodyExt;
use serde::Deserialize;
use sqlx::SqlitePool;
use walastack::App;
use walastack_auth::{AuthPlugin, InMemorySecretsPlugin, JwtConfig};
use walastack_db::sqlite::SqlitePlugin;
use walastack_runtime::Runtime;
use walastack_test::TestClient;

#[path = "../src/main.rs"]
mod app;

const JWT_KEY_NAME: &str = "jwt";
const JWT_ISSUER: &str = "hello-auth-db";

#[derive(Deserialize)]
struct TokenResponse {
    token: String,
}

#[derive(Deserialize)]
struct UserInfo {
    id: i64,
    email: String,
}

async fn build_test_runtime() -> (Runtime, TestClient) {
    let db_plugin = SqlitePlugin::in_memory();
    let pool: SqlitePool = db_plugin.pool().clone();
    app::migrate(&pool).await.expect("migrate");

    let runtime = Runtime::builder()
        .with_plugin(db_plugin)
        .with_plugin(InMemorySecretsPlugin::new().with(JWT_KEY_NAME, b"test-signing-key"))
        .with_plugin(AuthPlugin::new().with_jwt(JWT_KEY_NAME, JwtConfig::new(JWT_ISSUER)))
        .build()
        .expect("runtime builds");
    // Required by `issue_token` which reads through the thread-local
    // (a documented friction point in main.rs).
    app::set_runtime_context(runtime.context().clone());

    let test_app = App::new()
        .route(app::health)
        .route(app::signup)
        .route(app::login)
        .route(app::me);
    let client = TestClient::with_runtime(test_app, &runtime);
    (runtime, client)
}

async fn read_json<T: serde::de::DeserializeOwned>(response: walastack_http::Response) -> T {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).expect("valid JSON response")
}

#[tokio::test]
async fn health_returns_ok() {
    let (_runtime, client) = build_test_runtime().await;
    let response = client.get("/health").await;
    assert_eq!(response.status(), http::StatusCode::OK);
}

#[tokio::test]
async fn signup_login_me_round_trip() {
    let (_runtime, client) = build_test_runtime().await;

    // Sign up.
    let signup = client
        .post_json(
            "/signup",
            &serde_json::json!({"email": "alice@example.test", "password": "hunter2"}),
        )
        .await;
    assert_eq!(signup.status(), http::StatusCode::OK);
    let token = read_json::<TokenResponse>(signup).await.token;
    assert!(!token.is_empty());

    // /me with the issued token returns the right user.
    let me = client
        .get_with_headers("/me", &[("authorization", &format!("Bearer {token}"))])
        .await;
    assert_eq!(me.status(), http::StatusCode::OK);
    let user: UserInfo = read_json(me).await;
    assert_eq!(user.email, "alice@example.test");
    assert!(user.id > 0);

    // Login with the same credentials produces a new working token.
    let login = client
        .post_json(
            "/login",
            &serde_json::json!({"email": "alice@example.test", "password": "hunter2"}),
        )
        .await;
    assert_eq!(login.status(), http::StatusCode::OK);
    let _: TokenResponse = read_json(login).await;
}

#[tokio::test]
async fn signup_with_existing_email_returns_409() {
    let (_runtime, client) = build_test_runtime().await;

    // First signup succeeds.
    client
        .post_json(
            "/signup",
            &serde_json::json!({"email": "bob@example.test", "password": "x"}),
        )
        .await;
    // Second signup with the same email returns 409.
    let dup = client
        .post_json(
            "/signup",
            &serde_json::json!({"email": "bob@example.test", "password": "y"}),
        )
        .await;
    assert_eq!(dup.status(), http::StatusCode::CONFLICT);
}

#[tokio::test]
async fn login_with_bad_password_returns_401() {
    let (_runtime, client) = build_test_runtime().await;

    client
        .post_json(
            "/signup",
            &serde_json::json!({"email": "carol@example.test", "password": "right"}),
        )
        .await;
    let bad = client
        .post_json(
            "/login",
            &serde_json::json!({"email": "carol@example.test", "password": "wrong"}),
        )
        .await;
    assert_eq!(bad.status(), http::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn me_without_authorization_returns_401() {
    let (_runtime, client) = build_test_runtime().await;
    let response = client.get("/me").await;
    assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
}

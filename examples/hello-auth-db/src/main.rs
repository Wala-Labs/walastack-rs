//! `hello-auth-db` — composed application example.
//!
//! Demonstrates the **platform-shaped** composition surface
//! (Plugin → Capability → Resource → Extractor → Handler) using three
//! real ecosystem crates together:
//!
//! - `walastack-db`     — SQLite as a `Database` capability.
//! - `walastack-auth`   — JWT issuance + `Auth` extractor.
//! - `walastack`        — `App`, route macros, responders.
//!
//! ## What the example exercises
//!
//! - Multi-plugin composition via `App::with_plugin(...)`.
//! - A SQLite-backed `users` table created at startup.
//! - A `Db` extractor pulled from the `SqlitePool` capability via
//!   the request's `RuntimeContext` extension.
//! - JWT issuance through `JwtCodec::from_runtime`.
//! - Bearer-token authentication through the `Auth` extractor.
//! - End-to-end test coverage via `walastack-test::TestClient` (see
//!   the `tests/` directory).
//!
//! ## Quick start
//!
//! ```bash
//! cargo run -p hello-auth-db
//! ```
//!
//! Then, in another terminal:
//!
//! ```bash
//! # Sign up a new user; receive a JWT in the response body.
//! curl -X POST -H 'Content-Type: application/json' \
//!     -d '{"email":"alice@example.test","password":"hunter2"}' \
//!     http://127.0.0.1:3000/signup
//!
//! # Log in to receive a fresh JWT.
//! curl -X POST -H 'Content-Type: application/json' \
//!     -d '{"email":"alice@example.test","password":"hunter2"}' \
//!     http://127.0.0.1:3000/login
//!
//! # Call a protected endpoint with the JWT.
//! curl -H 'Authorization: Bearer <token>' http://127.0.0.1:3000/me
//! ```
//!
//! ## SECURITY NOTE
//!
//! This example stores passwords in plaintext for brevity. **Never do
//! this in production.** A real deployment should use a password
//! hashing library (`argon2`, `bcrypt`, etc.) and a real signing key
//! sourced from a `SecretsProvider` backed by a secrets vault rather
//! than the dev-only `InMemorySecretsPlugin` used here.

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
// `migrate` + `set_runtime_context` are also consumed by tests/smoke.rs
// which imports main.rs as a module — the unreachable_pub lint sees
// them as unused from a bin perspective but they're real test entry
// points.
#![allow(unreachable_pub)]
// Examples are intentionally lenient on `expect`/`unwrap` — example
// code is meant to be read top-to-bottom without scaffolding noise.
#![allow(clippy::expect_used, clippy::unwrap_used)]
// Domain names (SQLite, JSON, JWT) are not code identifiers; backticking
// them in prose hurts readability.
#![allow(clippy::doc_markdown)]

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use walastack::prelude::*;
use walastack_auth::{Auth, AuthPlugin, InMemorySecretsPlugin, JwtCodec, JwtConfig};
use walastack_db::sqlite::SqlitePlugin;
use walastack_http::Body;
use walastack_runtime::RuntimeContext;

// =========================================================================
// Db type alias
// =========================================================================
//
// As of Tier 3.1, the generic `Cap<T>` extractor in walastack-app
// subsumes the hand-written `Db` shape. A short type alias keeps the
// existing `Db(pool)` destructuring at handler sites:
//
//     async fn signup(Cap(pool): Db, ...) { ... }
//
// In a future iteration walastack-db itself can ship this alias so
// every consumer benefits without redefining it locally.

type Db = Cap<SqlitePool>;

// =========================================================================
// Domain types
// =========================================================================

#[derive(Deserialize)]
struct AuthCredentials {
    email: String,
    password: String,
}

#[derive(Serialize)]
struct TokenResponse {
    token: String,
}

#[derive(Serialize)]
struct UserInfo {
    id: i64,
    email: String,
}

// =========================================================================
// Handler errors
// =========================================================================

enum HandlerError {
    BadRequest(&'static str),
    Unauthorized,
    Conflict(&'static str),
    Internal,
}

impl IntoResponse for HandlerError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.to_string()),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized".into()),
            Self::Conflict(msg) => (StatusCode::CONFLICT, msg.to_string()),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error".into(),
            ),
        };
        let mut response = Response::new(Body::new(Bytes::from(body)));
        *response.status_mut() = status;
        response
    }
}

// =========================================================================
// Routes
// =========================================================================

#[get("/health")]
async fn health() -> &'static str {
    "ok"
}

#[post("/signup")]
async fn signup(
    Cap(pool): Db,
    Json(creds): Json<AuthCredentials>,
) -> std::result::Result<Json<TokenResponse>, HandlerError> {
    if creds.email.is_empty() || creds.password.is_empty() {
        return Err(HandlerError::BadRequest("email + password required"));
    }
    // Insert the new user; bail with 409 if the email is taken.
    let existing: Option<(i64,)> = sqlx::query_as("SELECT id FROM users WHERE email = ?")
        .bind(&creds.email)
        .fetch_optional(&*pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "users select failed");
            HandlerError::Internal
        })?;
    if existing.is_some() {
        return Err(HandlerError::Conflict("email already registered"));
    }
    let id: i64 =
        sqlx::query_scalar("INSERT INTO users (email, password) VALUES (?, ?) RETURNING id")
            .bind(&creds.email)
            .bind(&creds.password)
            .fetch_one(&*pool)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "users insert failed");
                HandlerError::Internal
            })?;
    Ok(Json(TokenResponse {
        token: issue_token(id, &creds.email)?,
    }))
}

#[post("/login")]
async fn login(
    Cap(pool): Db,
    Json(creds): Json<AuthCredentials>,
) -> std::result::Result<Json<TokenResponse>, HandlerError> {
    let row: Option<(i64, String)> =
        sqlx::query_as("SELECT id, password FROM users WHERE email = ?")
            .bind(&creds.email)
            .fetch_optional(&*pool)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "users select failed");
                HandlerError::Internal
            })?;
    let Some((id, stored)) = row else {
        return Err(HandlerError::Unauthorized);
    };
    if stored != creds.password {
        return Err(HandlerError::Unauthorized);
    }
    Ok(Json(TokenResponse {
        token: issue_token(id, &creds.email)?,
    }))
}

#[get("/me")]
async fn me(
    Cap(pool): Db,
    Auth(claims): Auth,
) -> std::result::Result<Json<UserInfo>, HandlerError> {
    let id: i64 = claims.sub.parse().map_err(|_| HandlerError::Unauthorized)?;
    let row: Option<(i64, String)> = sqlx::query_as("SELECT id, email FROM users WHERE id = ?")
        .bind(id)
        .fetch_optional(&*pool)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "users select failed");
            HandlerError::Internal
        })?;
    let Some((id, email)) = row else {
        return Err(HandlerError::Unauthorized);
    };
    Ok(Json(UserInfo { id, email }))
}

// =========================================================================
// JWT issuance helper
// =========================================================================
//
// Constructs a JwtCodec from the kernel at issuance time. JwtCodec is
// cheap to construct (clones an Arc<dyn SecretsProvider> and a small
// config) so per-call construction is fine; a future iteration could
// cache the codec as a Resource at startup.

fn issue_token(user_id: i64, email: &str) -> std::result::Result<String, HandlerError> {
    let ctx = current_runtime_context();
    let codec =
        JwtCodec::from_runtime(&ctx, JWT_KEY_NAME, JwtConfig::new(JWT_ISSUER)).map_err(|e| {
            tracing::error!(error = %e, "JwtCodec construction failed");
            HandlerError::Internal
        })?;
    let claims = codec.issue(user_id.to_string(), vec![]);
    let _ = email;
    codec.encode(&claims).map_err(|e| {
        tracing::error!(error = %e, "JWT encode failed");
        HandlerError::Internal
    })
}

// DX friction: there is currently no per-request way to reach the
// RuntimeContext from a regular function (only from extractors). The
// route macros each take their own RuntimeContext-aware extractor, so
// reaching the runtime *outside* an extractor requires routing it
// down explicitly. For the example we punt via a thread-local set at
// startup; production code should plumb the RuntimeContext through
// the extractor chain instead. See the friction notes at the bottom.

thread_local! {
    static RUNTIME_CTX: std::cell::RefCell<Option<RuntimeContext>> =
        const { std::cell::RefCell::new(None) };
}

pub fn set_runtime_context(ctx: RuntimeContext) {
    RUNTIME_CTX.with(|cell| {
        *cell.borrow_mut() = Some(ctx);
    });
}

fn current_runtime_context() -> RuntimeContext {
    RUNTIME_CTX.with(|cell| {
        cell.borrow()
            .clone()
            .expect("runtime context not set; call set_runtime_context first")
    })
}

// =========================================================================
// Composition
// =========================================================================

const JWT_KEY_NAME: &str = "jwt";
const JWT_ISSUER: &str = "hello-auth-db";

pub async fn migrate(pool: &SqlitePool) -> sqlx::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            email    TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[walastack::main]
async fn main() -> walastack::Result<()> {
    // Build the database plugin separately so we can keep a handle to
    // the pool for the startup migration step.
    let db_plugin = SqlitePlugin::in_memory();
    let pool = db_plugin.pool().clone();
    migrate(&pool)
        .await
        .map_err(|e| walastack::Error::Custom(format!("migration failed: {e}")))?;

    // DX friction: setting RUNTIME_CTX via a startup hook is awkward.
    // A future kernel refinement could expose RuntimeContext to
    // arbitrary helper functions via a context-local injection point.
    // For now we wire it explicitly after build() but before run().
    let runtime = walastack_runtime::Runtime::builder()
        .with_plugin(db_plugin)
        .with_plugin(
            InMemorySecretsPlugin::new().with(JWT_KEY_NAME, b"insecure-dev-signing-key-CHANGE-ME"),
        )
        .with_plugin(AuthPlugin::new().with_jwt(JWT_KEY_NAME, JwtConfig::new(JWT_ISSUER)))
        .with(
            App::new()
                .route(health)
                .route(signup)
                .route(login)
                .route(me)
                .into_http_service("127.0.0.1:3000")?,
        )
        .build()?;
    set_runtime_context(runtime.context().clone());
    runtime.run().await?;
    Ok(())
}

// =========================================================================
// DX FRICTION OBSERVATIONS (Phase 4.1 — Platform Consolidation)
// =========================================================================
//
// Observations collected while writing this example. These are
// surfaced for the formal DX review.
//
// 1. IMPORT FAN-OUT
//    Even a minimal "real" example imports from 5 sources:
//      - walastack::prelude::*       (App, Json, get, post, etc.)
//      - walastack_auth              (Auth, AuthPlugin, JwtCodec, etc.)
//      - walastack_db::sqlite        (SqlitePlugin)
//      - walastack_http              (Body)
//      - walastack_runtime           (RuntimeContext)
//    A curated `walastack::prelude` that re-exports the common Auth /
//    Db plugins and types would collapse most of this to one use line.
//
// 2. NO Db / Capability EXTRACTOR
//    The `Db` extractor in this file is hand-written. Every consumer of
//    a capability needs to write this boilerplate. A generic
//    `Cap<C>` extractor (or a `Db<P: ?Sized>` extractor in walastack-db)
//    would eliminate this for every future ecosystem app.
//
// 3. RUNTIMECONTEXT-FROM-HELPER FRICTION
//    Extractors can reach RuntimeContext (via request extensions) but
//    arbitrary helper functions cannot. The thread_local workaround in
//    issue_token is a smell. Options for future iteration:
//      - Plumb RuntimeContext through helper signatures.
//      - Add a kernel-blessed task-local mechanism.
//      - Cache a `JwtCodec` as a Resource at startup so handlers can
//        extract it directly without re-construction.
//
// 4. `App::route(handler_struct)` vs `App::get("/path", fn)`
//    The route macros generate a unit struct; users call
//    `.route(signup)` to register them. The relationship between this
//    and `.get("/path", fn)` could be clearer in docs.
//
// 5. STARTUP MIGRATION NOT FIRST-CLASS
//    Running `CREATE TABLE IF NOT EXISTS` before `runtime.run()` requires
//    grabbing the pool before plugin registration. The pattern works but
//    is inconsistent with how `walastack-jobs` SQLite handles it (via
//    an auto-migrate service). A cross-crate convention for
//    schema migration would reduce confusion.
//
// 6. ERROR SHAPING
//    Every handler that touches the DB writes the same
//    `.map_err(|e| { tracing::error!(...); HandlerError::Internal })`
//    boilerplate. A `From<sqlx::Error>` impl on a generic error type
//    (or a `tracing` macro that returns the rejection) would compress
//    this materially.
//
// 7. `walastack::Error::Custom` AS THE INTEGRATION-FAILURE PATH
//    Mapping `RuntimeError` and `sqlx::Error` to `walastack::Error`
//    requires re-stringification at the boundary. A `From<RuntimeError>`
//    impl on `walastack::Error` would make `main` cleaner.

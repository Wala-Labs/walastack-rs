//! `hello-auth-db-openapi` — Tier 2 DX example.
//!
//! Extends `hello-auth-db` with `walastack-openapi`. This is a
//! **four-crate composition** (db + auth + openapi + http) used to
//! validate whether the Phase 4.1 Tier 1 documentation actually
//! reduces friction when building realistic applications.
//!
//! ## What this exercises
//!
//! - `App::with_plugin` composing **three** ecosystem plugins
//!   simultaneously (SqlitePlugin + AuthPlugin + OpenApiPlugin) plus
//!   the secrets provider.
//! - `App::openapi_route(handler, RouteSpec)` to register HTTP routes
//!   AND their OpenAPI metadata in one call.
//! - `App::openapi_serve_at("/openapi.json")` to expose the assembled
//!   document.
//! - Hand-built `Schema` instances for request/response types
//!   (`AuthCredentials`, `TokenResponse`, `UserInfo`).
//! - All four guides being consulted in practice:
//!   - [App vs Runtime](../../../docs/guides/app-vs-runtime.md) — Path 3
//!     used because we need a startup migration hook.
//!   - [Plugin composition](../../../docs/guides/plugin-composition.md) —
//!     three providers + one consumer plugin, order-independent.
//!   - [Capabilities and resources](../../../docs/guides/capabilities-and-resources.md)
//!     — `Cap`-style handwritten `Db` extractor (the friction `Cap<T>` will fix).
//!   - [Handler errors](../../../docs/guides/handler-errors.md) — typed
//!     `HandlerError` enum following the Rejection-Mapping Discipline.
//!
//! ## Quick start
//!
//! ```bash
//! cargo run -p hello-auth-db-openapi
//! ```
//!
//! Then:
//!
//! ```bash
//! # Fetch the generated OpenAPI document.
//! curl http://127.0.0.1:3000/openapi.json | jq
//!
//! # Sign up, log in, and call /me as in hello-auth-db.
//! curl -X POST -H 'Content-Type: application/json' \
//!     -d '{"email":"alice@example.test","password":"hunter2"}' \
//!     http://127.0.0.1:3000/signup
//! ```
//!
//! ## SECURITY NOTE
//!
//! Plaintext passwords for brevity. See `hello-auth-db` for the same
//! warning.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![allow(unreachable_pub)]
#![allow(clippy::expect_used, clippy::unwrap_used)]
#![allow(clippy::doc_markdown)]

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use walastack::prelude::*;
use walastack_auth::{Auth, AuthPlugin, InMemorySecretsPlugin, JwtCodec, JwtConfig};
use walastack_db::sqlite::SqlitePlugin;
use walastack_http::Body;
use walastack_openapi::{OpenApiConfig, OpenApiPlugin, RouteSpec, Schema, ToSchema};
use walastack_runtime::RuntimeContext;

// =========================================================================
// Db type alias
// =========================================================================
//
// TIER 3.1 RESULT: the previous hand-written `Db` extractor
// (~40 lines, copied verbatim across two examples in Tier 2) is gone.
// The generic `Cap<T>` extractor from `walastack` (re-exported through
// `walastack::prelude::*`) handles both concrete and trait-object
// capabilities; this short alias keeps the existing destructuring
// pattern intact: `Cap(pool): Db`.

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
// Hand-built schemas
// =========================================================================
//
// TIER 2 OBSERVATION: hand-building schemas for 3 simple types takes
// ~30 lines. For a 10-field domain type the line count explodes.
// Classification: STILL PAINFUL AFTER DOCUMENTATION. The
// Schema-shape preference is documented but doesn't reduce the
// per-field repetition. Confirms #[derive(Schema)] will be wanted
// eventually — though not for Tier 3.

impl ToSchema for AuthCredentials {
    fn schema() -> Schema {
        Schema::object()
            .property("email", Schema::string().with_format("email"))
            .property("password", Schema::string())
            .require("email")
            .require("password")
    }
}

impl ToSchema for TokenResponse {
    fn schema() -> Schema {
        Schema::object()
            .property("token", Schema::string())
            .require("token")
    }
}

impl ToSchema for UserInfo {
    fn schema() -> Schema {
        Schema::object()
            .property("id", Schema::integer().with_format("int64"))
            .property("email", Schema::string().with_format("email"))
            .require("id")
            .require("email")
    }
}

// =========================================================================
// Handler errors (same shape as hello-auth-db, see handler-errors guide)
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
// Handlers — same logic as hello-auth-db
// =========================================================================
//
// Plain handler functions (not route-macro attributed) because they're
// registered through App::openapi_route, not through App::route.
//
// TIER 2 OBSERVATION: two parallel registration mechanisms now
// (#[get("/path")] for plain routes; openapi_route(handler, RouteSpec)
// for OpenAPI-tracked routes). For a real app every route wants
// OpenAPI metadata, so the macro path effectively becomes
// "OpenAPI-not-tracked." Classification: DOCUMENTATION SOLVED — the
// existing openapi crate docs explain this; it's a stylistic split,
// not a friction wall.

async fn health() -> &'static str {
    "ok"
}

async fn signup(
    Cap(pool): Db,
    Json(creds): Json<AuthCredentials>,
) -> std::result::Result<Json<TokenResponse>, HandlerError> {
    if creds.email.is_empty() || creds.password.is_empty() {
        return Err(HandlerError::BadRequest("email + password required"));
    }
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
        token: issue_token(id)?,
    }))
}

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
        token: issue_token(id)?,
    }))
}

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
// JWT issuance (same thread-local pattern as hello-auth-db — see the
// RuntimeContext-from-helper friction note in hello-auth-db)
// =========================================================================

fn issue_token(user_id: i64) -> std::result::Result<String, HandlerError> {
    let ctx = current_runtime_context();
    let codec =
        JwtCodec::from_runtime(&ctx, JWT_KEY_NAME, JwtConfig::new(JWT_ISSUER)).map_err(|e| {
            tracing::error!(error = %e, "JwtCodec construction failed");
            HandlerError::Internal
        })?;
    let claims = codec.issue(user_id.to_string(), vec![]);
    codec.encode(&claims).map_err(|e| {
        tracing::error!(error = %e, "JWT encode failed");
        HandlerError::Internal
    })
}

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
//
// Follows the App vs Runtime guide, Path 3 (Runtime::builder + startup
// hook) because we need to run the schema migration before serving.

const JWT_KEY_NAME: &str = "jwt";
const JWT_ISSUER: &str = "hello-auth-db-openapi";

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

/// Construct the App with all four ecosystem plugin attachments + the
/// OpenAPI route specs. Extracted so the smoke test can reuse it.
pub fn build_app() -> App {
    App::new()
        .openapi_route(
            health,
            RouteSpec::get("/health")
                .summary("Liveness probe")
                .tag("infra")
                .response::<&'static str>(200),
        )
        .openapi_route(
            signup,
            RouteSpec::post("/signup")
                .summary("Sign up a new user; returns a JWT")
                .tag("auth")
                .json_body::<AuthCredentials>()
                .response::<TokenResponse>(200),
        )
        .openapi_route(
            login,
            RouteSpec::post("/login")
                .summary("Log in; returns a JWT")
                .tag("auth")
                .json_body::<AuthCredentials>()
                .response::<TokenResponse>(200),
        )
        .openapi_route(
            me,
            RouteSpec::get("/me")
                .summary("Return the authenticated user")
                .tag("user")
                .response::<UserInfo>(200),
        )
        .openapi_serve_at("/openapi.json")
}

#[walastack::main]
async fn main() -> walastack::Result<()> {
    let db_plugin = SqlitePlugin::in_memory();
    let pool = db_plugin.pool().clone();
    migrate(&pool)
        .await
        .map_err(|e| walastack::Error::Custom(format!("migration failed: {e}")))?;

    let openapi_config = OpenApiConfig::new("hello-auth-db-openapi", "0.1.0")
        .with_description("Tier 2 DX example — 4-crate composition validation");

    let runtime = walastack_runtime::Runtime::builder()
        .with_plugin(db_plugin)
        .with_plugin(InMemorySecretsPlugin::new().with(JWT_KEY_NAME, b"dev-only-CHANGE-ME"))
        .with_plugin(AuthPlugin::new().with_jwt(JWT_KEY_NAME, JwtConfig::new(JWT_ISSUER)))
        .with_plugin(OpenApiPlugin::new(openapi_config))
        .with(build_app().into_http_service("127.0.0.1:3000")?)
        .build()?;
    set_runtime_context(runtime.context().clone());
    runtime.run().await?;
    Ok(())
}

// =========================================================================
// TIER 2 DX FRICTION OBSERVATIONS (classified)
// =========================================================================
//
// Following the user's request that observations be classified into:
//   DOCUMENTATION SOLVED  — the guides made the path discoverable.
//   STILL PAINFUL         — friction remains after the docs.
//
// ## DOCUMENTATION SOLVED (Tier 1 docs were sufficient)
//
//   - Choosing Runtime::builder over App::run.
//     The app-vs-runtime guide's Path-3 example matched this app's
//     startup-migration need exactly. Zero friction.
//
//   - Plugin ordering (3 providers + 2 consumers).
//     plugin-composition.md's "order-independence + validation phase"
//     framing made it obvious that ordering didn't matter.
//
//   - JobStore-vs-Database (referenced but not used here, but the
//     model carried over to "why is there an OpenApi plugin AND an
//     App method? what's the boundary?"). The capabilities-and-
//     resources guide answered this clearly — OpenApiPlugin owns
//     the Resource side; App owns the routing side.
//
//   - Handler error patterns.
//     The HandlerError enum is identical across hello-auth-db and
//     hello-auth-db-openapi; the handler-errors guide is the
//     reference implementation.
//
//   - "Where does ctx.publish come from?" (not used in this example,
//     but no longer asked when reading the existing code).
//
//   - Plugin builder convention.
//     All four plugins composed cleanly via .new(...) + .with_x(...);
//     no surprises.
//
// ## STILL PAINFUL AFTER DOCUMENTATION (strong Tier 3 candidates)
//
//   - Db extractor boilerplate (~40 lines, copied verbatim from
//     hello-auth-db). Cap<T: ?Sized> would eliminate it. Strongest
//     single signal in this example.
//
//   - RuntimeContext-from-helper (thread_local workaround in
//     issue_token). Same as hello-auth-db; guides documented the
//     friction but didn't reduce it. Tier 4 candidate.
//
//   - Import fan-out — now 6 source paths for the example. A
//     walastack::prelude::full or per-crate preludes would compress
//     this. Strong Tier 3 signal.
//
//   - Hand-built Schema for request/response types.
//     ~30 lines for 3 simple types. For a real domain (10 fields per
//     type, 20 routes) this becomes 600+ lines of repetition.
//     #[derive(Schema)] would solve it. Not a Tier 3 priority but
//     evidence is now strong.
//
//   - First attempt: tried `json_body::<serde_json::Value>()` as a
//     placeholder because I didn't realize T: ToSchema was implementable
//     on user types. The actual answer is `impl ToSchema for MyType`
//     using hand-built schemas — this is the documented pattern but it
//     wasn't *obvious* at first reading.
//     Classification: DOCUMENTATION OPPORTUNITY — the openapi crate's
//     module docs say schemas are "hand-built first" but don't show
//     the user-implements-ToSchema pattern as a worked example. One
//     small doc addition would close this. NOT a Tier 3 candidate.
//
//   - issue_token still requires the thread_local. Even with a Cap<T>
//     extractor, helpers called *outside* extractor context still
//     can't reach RuntimeContext. This is now a confirmed Tier 4
//     candidate (the documentation acknowledged the workaround but
//     didn't fix it).
//
// ## NEW OBSERVATIONS (not in original 15)
//
//   - Sample example structure repetition: signup/login/me are
//     identical to hello-auth-db. As more examples land, the
//     duplication grows. A `walastack-examples-common` helper crate
//     could host shared types + extractors + handlers — but this is a
//     refactor concern, not a DX concern. NOT a Tier 3 candidate.
//
//   - Plugin-attached OpenAPI vs App-attached OpenAPI tension:
//     OpenApiConfig is plugin-side (Resource); routes are App-side
//     (accumulator). For most users this composition is fine because
//     they only attach one OpenApiPlugin. But: it's not obvious that
//     OpenApiPlugin alone (without `App::openapi_serve_at`) does
//     nothing useful at the HTTP surface. Documentation note worth
//     adding to the openapi crate's lib.rs.

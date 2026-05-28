use walastack::prelude::*;

#[get("/")]
async fn index() -> Html<&'static str> {
    Html("<h1>Welcome to {{name}}!</h1>")
}

#[get("/health")]
async fn health() -> &'static str {
    "ok"
}

#[walastack::main]
async fn main() -> walastack::Result<()> {
    App::new()
        .route(index)
        .route(health)
        .run("127.0.0.1:3000")
        .await
}

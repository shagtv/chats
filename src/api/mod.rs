pub mod auth;
pub mod chats;
pub mod home;
pub mod messages;

use std::path::Path;

use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::header::{CACHE_CONTROL, HeaderValue};
use axum::http::request::Parts;
use axum::middleware::map_response;
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use tower_http::services::ServeDir;

use crate::app::App;
use crate::error::ApiError;
use crate::ws;

pub fn router(app: App, static_dir: &Path) -> Router {
    let api = Router::new()
        .route("/register", post(auth::register))
        .route("/login", post(auth::login))
        .route("/logout", post(auth::logout))
        .route("/me", get(auth::me))
        .route("/home", get(home::home))
        .route("/chats", post(chats::create))
        .route("/chats/{id}", get(chats::get).delete(chats::delete))
        .route("/chats/{id}/join", post(chats::join))
        .route("/chats/{id}/leave", post(chats::leave))
        .route("/chats/{id}/read", post(messages::mark_read))
        .route("/chats/{id}/messages", get(messages::history).post(messages::post))
        .route("/ws", get(ws::handler))
        .fallback(|| async { ApiError::not_found("no such endpoint") })
        .layer(map_response(no_store));
    Router::new().nest("/api", api).fallback_service(ServeDir::new(static_dir)).with_state(app)
}

async fn no_store(mut resp: Response) -> Response {
    resp.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    resp
}

/// JSON body whose rejections use the API's `{"error": ...}` shape.
pub struct Body<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for Body<T> {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(v)) => Ok(Body(v)),
            Err(e) => Err(ApiError::bad_request(e.body_text())),
        }
    }
}

/// The `{id}` path segment of a chat route; anything non-numeric is a 404.
pub struct ChatId(pub i64);

impl<S: Send + Sync> FromRequestParts<S> for ChatId {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        axum::extract::Path::<i64>::from_request_parts(parts, state)
            .await
            .map(|p| ChatId(p.0))
            .map_err(|_| ApiError::not_found("chat not found"))
    }
}

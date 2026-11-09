pub mod auth;
pub mod chats;
pub mod home;
pub mod messages;
pub mod metrics;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::{FromRequest, FromRequestParts, Request, State};
use axum::http::header::{CACHE_CONTROL, HeaderValue};
use axum::http::request::Parts;
use axum::middleware::map_response_with_state;
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use tower_http::services::ServeDir;

use crate::app::App;
use crate::error::ApiError;
use crate::ws;

pub fn router(app: App, static_dir: &Path, metrics: bool) -> Router {
    let mut api = Router::new()
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
        .route("/ws", get(ws::handler));
    if metrics {
        api = api.route("/metrics", get(metrics::metrics));
    }
    let api = api
        .fallback(|| async { ApiError::not_found("no such endpoint") })
        .layer(map_response_with_state(app.http.clone(), count_and_no_store));
    Router::new().nest("/api", api).fallback_service(ServeDir::new(static_dir)).with_state(app)
}

/// Totals for `/api` responses, exposed by `/api/metrics`.
#[derive(Debug, Default)]
pub struct HttpStats {
    pub requests_total: AtomicU64,
    pub errors_5xx_total: AtomicU64,
}

async fn count_and_no_store(State(stats): State<Arc<HttpStats>>, mut resp: Response) -> Response {
    stats.requests_total.fetch_add(1, Ordering::Relaxed);
    if resp.status().is_server_error() {
        stats.errors_5xx_total.fetch_add(1, Ordering::Relaxed);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body as HttpBody;
    use axum::http::StatusCode;
    use tower::ServiceExt;

    #[tokio::test]
    async fn counting_layer_counts_requests_and_5xx() {
        let stats = Arc::new(HttpStats::default());
        let app = Router::new()
            .route("/ok", get(|| async { "ok" }))
            .route("/boom", get(|| async { StatusCode::INTERNAL_SERVER_ERROR }))
            .layer(map_response_with_state(stats.clone(), count_and_no_store));
        for path in ["/ok", "/boom", "/ok"] {
            let resp = app.clone().oneshot(Request::get(path).body(HttpBody::empty()).unwrap()).await.unwrap();
            assert_eq!(resp.headers()[CACHE_CONTROL], "no-store");
        }
        assert_eq!(stats.requests_total.load(Ordering::Relaxed), 3);
        assert_eq!(stats.errors_5xx_total.load(Ordering::Relaxed), 1);
    }
}

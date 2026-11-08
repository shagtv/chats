use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::SET_COOKIE;
use axum::response::IntoResponse;
use serde::Deserialize;
use serde_json::json;

use super::Body;
use crate::app::App;
use crate::auth::{AuthUser, SESSION_TTL, clear_cookie, new_token, session_cookie};
use crate::db::meta::{self, MetaCmd};
use crate::db::{self, now_s};
use crate::error::{ApiError, ApiResult};

const MAX_PASSWORD_LEN: usize = 1024;

#[derive(Deserialize)]
pub struct Credentials {
    login: String,
    password: String,
}

pub fn validate_login(login: &str) -> ApiResult<()> {
    let ok_chars = login.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !(3..=32).contains(&login.len()) || !ok_chars {
        return Err(ApiError::bad_request("login must be 3-32 characters: letters, digits or _"));
    }
    Ok(())
}

pub fn validate_password(password: &str) -> ApiResult<()> {
    let len = password.chars().count();
    if len < 8 {
        return Err(ApiError::bad_request("password must be at least 8 characters"));
    }
    if password.len() > MAX_PASSWORD_LEN {
        return Err(ApiError::bad_request("password is too long"));
    }
    Ok(())
}

async fn start_session(app: &App, user_id: i64) -> ApiResult<String> {
    let (token, hash) = new_token();
    let now = now_s();
    let expires_at = now + SESSION_TTL;
    app.db
        .meta
        .request(|reply| MetaCmd::CreateSession { token_hash: hash.to_vec(), user_id, expires_at, reply })
        .await?;
    app.auth.cache_session(hash, user_id, expires_at, now);
    Ok(session_cookie(&token))
}

pub async fn register(State(app): State<App>, Body(c): Body<Credentials>) -> ApiResult<impl IntoResponse> {
    validate_login(&c.login)?;
    validate_password(&c.password)?;
    let pw_hash = app.auth.hash_password(c.password).await?;
    let login = c.login;
    let id = app
        .db
        .meta
        .request(|reply| MetaCmd::CreateUser { login: login.clone(), pw_hash, reply })
        .await?;
    app.hub.set_login(id, login.clone());
    let cookie = start_session(&app, id).await?;
    Ok((StatusCode::CREATED, [(SET_COOKIE, cookie)], Json(json!({ "id": id, "login": login }))))
}

pub async fn login(State(app): State<App>, Body(c): Body<Credentials>) -> ApiResult<impl IntoResponse> {
    let login_lc = c.login.to_lowercase();
    if app.auth.throttled(&login_lc) {
        return Err(ApiError::too_many_requests("too many failed attempts, try again in a minute"));
    }
    let wrong = || ApiError::new(StatusCode::UNAUTHORIZED, "wrong login or password");
    if c.password.len() > MAX_PASSWORD_LEN {
        return Err(wrong());
    }
    let lc = login_lc.clone();
    let user = db::read(&app.db.meta_pool, move |conn| meta::user_by_login(conn, &lc)).await?;
    let hash = user.as_ref().map(|u| u.pw_hash.clone());
    if !app.auth.verify_password(c.password, hash).await? {
        app.auth.record_failure(&login_lc);
        return Err(wrong());
    }
    let user = user.ok_or_else(wrong)?;
    app.auth.clear_failures(&login_lc);
    app.hub.set_login(user.id, user.login.clone());
    let cookie = start_session(&app, user.id).await?;
    Ok(([(SET_COOKIE, cookie)], Json(json!({ "id": user.id, "login": user.login }))))
}

pub async fn logout(State(app): State<App>, user: AuthUser) -> ApiResult<impl IntoResponse> {
    app.auth.forget_session(&user.token_hash);
    app.db
        .meta
        .request(|reply| MetaCmd::DeleteSession { token_hash: user.token_hash.to_vec(), reply })
        .await?;
    Ok((StatusCode::NO_CONTENT, [(SET_COOKIE, clear_cookie())]))
}

pub async fn me(user: AuthUser) -> Json<serde_json::Value> {
    Json(json!({ "id": user.id, "login": user.login }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_rules() {
        assert!(validate_login("bob").is_ok());
        assert!(validate_login("Alice_99").is_ok());
        assert!(validate_login("ab").is_err());
        assert!(validate_login(&"a".repeat(33)).is_err());
        assert!(validate_login("bad-name").is_err());
        assert!(validate_login("юзер").is_err());
    }

    #[test]
    fn password_rules() {
        assert!(validate_password("12345678").is_ok());
        assert!(validate_password("1234567").is_err());
        assert!(validate_password(&"x".repeat(MAX_PASSWORD_LEN + 1)).is_err());
    }
}

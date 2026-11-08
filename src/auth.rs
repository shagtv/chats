//! Password hashing, sessions, login throttle and the `AuthUser` extractor (design D11).

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use argon2::Argon2;
use argon2::password_hash::phc::PasswordHash;
use argon2::password_hash::{PasswordHasher, PasswordVerifier};
use axum::extract::FromRequestParts;
use axum::http::header::COOKIE;
use axum::http::request::Parts;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::app::App;
use crate::db::meta::MetaCmd;
use crate::db::{self, meta, now_s};
use crate::error::{ApiError, ApiResult};

pub const COOKIE_NAME: &str = "sid";
pub const SESSION_TTL: i64 = 30 * 24 * 3600;
/// Sliding expiry is exact in memory but written to disk at most this often.
pub const TOUCH_INTERVAL: i64 = 3600;
pub const MAX_FAILURES: u32 = 5;
pub const FAILURE_WINDOW: Duration = Duration::from_secs(60);

pub type TokenHash = [u8; 32];

pub fn new_token() -> (String, TokenHash) {
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).expect("OS random source");
    let token = URL_SAFE_NO_PAD.encode(raw);
    let hash = hash_token(&token);
    (token, hash)
}

pub fn hash_token(token: &str) -> TokenHash {
    Sha256::digest(token.as_bytes()).into()
}

pub fn session_cookie(token: &str) -> String {
    format!("{COOKIE_NAME}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_TTL}")
}

pub fn clear_cookie() -> String {
    format!("{COOKIE_NAME}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0")
}

struct CachedSession {
    user_id: i64,
    expires_at: i64,
    persisted_at: i64,
}

#[derive(Debug, PartialEq)]
pub enum Lookup {
    /// Valid; `touch` carries a new expiry to persist, if one is due.
    Hit { user_id: i64, touch: Option<i64> },
    Expired,
    Miss,
}

pub struct Auth {
    sessions: DashMap<TokenHash, CachedSession>,
    failures: DashMap<String, (Instant, u32)>,
    hashing: Semaphore,
}

impl Default for Auth {
    fn default() -> Self {
        Self::new()
    }
}

impl Auth {
    pub fn new() -> Self {
        let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        Self { sessions: DashMap::new(), failures: DashMap::new(), hashing: Semaphore::new(cpus) }
    }

    // ---- passwords ----

    pub async fn hash_password(&self, password: String) -> ApiResult<String> {
        let _permit = self.hashing.acquire().await.map_err(|_| ApiError::internal())?;
        tokio::task::spawn_blocking(move || {
            Argon2::default().hash_password(password.as_bytes()).map(|h| h.to_string()).map_err(|e| {
                tracing::error!("hash password: {e}");
                ApiError::internal()
            })
        })
        .await
        .map_err(|_| ApiError::internal())?
    }

    /// Verifies against `hash`, or against a dummy hash when the user doesn't
    /// exist so both cases take the same time.
    pub async fn verify_password(&self, password: String, hash: Option<String>) -> ApiResult<bool> {
        let _permit = self.hashing.acquire().await.map_err(|_| ApiError::internal())?;
        tokio::task::spawn_blocking(move || {
            let real = hash.is_some();
            let hash = hash.unwrap_or_else(|| dummy_hash().to_string());
            let ok = PasswordHash::new(&hash)
                .map(|parsed| Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok())
                .unwrap_or(false);
            ok && real
        })
        .await
        .map_err(|_| ApiError::internal())
    }

    // ---- login throttle ----

    pub fn throttled(&self, login_lc: &str) -> bool {
        self.failures
            .get(login_lc)
            .is_some_and(|f| f.0.elapsed() < FAILURE_WINDOW && f.1 >= MAX_FAILURES)
    }

    pub fn record_failure(&self, login_lc: &str) {
        let mut entry = self.failures.entry(login_lc.to_string()).or_insert((Instant::now(), 0));
        if entry.0.elapsed() >= FAILURE_WINDOW {
            *entry = (Instant::now(), 0);
        }
        entry.1 += 1;
    }

    pub fn clear_failures(&self, login_lc: &str) {
        self.failures.remove(login_lc);
    }

    // ---- session cache ----

    pub fn cache_session(&self, hash: TokenHash, user_id: i64, expires_at: i64, now: i64) {
        self.sessions.insert(hash, CachedSession { user_id, expires_at, persisted_at: now });
    }

    pub fn forget_session(&self, hash: &TokenHash) {
        self.sessions.remove(hash);
    }

    /// Checks the cache and slides the expiry of a valid session.
    pub fn lookup(&self, hash: &TokenHash, now: i64) -> Lookup {
        let Some(mut s) = self.sessions.get_mut(hash) else { return Lookup::Miss };
        if s.expires_at <= now {
            drop(s);
            self.sessions.remove(hash);
            return Lookup::Expired;
        }
        s.expires_at = now + SESSION_TTL;
        let touch = if now - s.persisted_at >= TOUCH_INTERVAL {
            s.persisted_at = now;
            Some(s.expires_at)
        } else {
            None
        };
        Lookup::Hit { user_id: s.user_id, touch }
    }

    pub fn cleanup(&self, now: i64) {
        self.sessions.retain(|_, s| s.expires_at > now);
        self.failures.retain(|_, f| f.0.elapsed() < FAILURE_WINDOW);
    }
}

fn dummy_hash() -> &'static str {
    static DUMMY: OnceLock<String> = OnceLock::new();
    DUMMY.get_or_init(|| {
        Argon2::default().hash_password(b"dummy password for timing").expect("hash dummy").to_string()
    })
}

/// Resolves a session token hash to a user id, sliding its expiry.
pub async fn resolve_session(app: &App, hash: TokenHash) -> ApiResult<Option<i64>> {
    let now = now_s();
    match app.auth.lookup(&hash, now) {
        Lookup::Hit { user_id, touch } => {
            if let Some(expires_at) = touch {
                app.db.meta.send(MetaCmd::TouchSession { token_hash: hash.to_vec(), expires_at }).await?;
            }
            Ok(Some(user_id))
        }
        Lookup::Expired => Ok(None),
        Lookup::Miss => {
            let row = db::read(&app.db.meta_pool, move |c| meta::session(c, &hash)).await?;
            match row {
                Some((user_id, expires_at)) if expires_at > now => {
                    let expires_at = now + SESSION_TTL;
                    app.auth.cache_session(hash, user_id, expires_at, now);
                    app.db.meta.send(MetaCmd::TouchSession { token_hash: hash.to_vec(), expires_at }).await?;
                    Ok(Some(user_id))
                }
                _ => Ok(None),
            }
        }
    }
}

pub fn cookie_value<'a>(parts: &'a Parts, name: &str) -> Option<&'a str> {
    parts
        .headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
}

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub id: i64,
    pub login: String,
    pub token_hash: TokenHash,
}

impl FromRequestParts<App> for AuthUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, app: &App) -> Result<Self, Self::Rejection> {
        let token = cookie_value(parts, COOKIE_NAME).ok_or_else(ApiError::unauthorized)?;
        let token_hash = hash_token(token);
        let id = resolve_session(app, token_hash).await?.ok_or_else(ApiError::unauthorized)?;
        let login = app.hub.login(id).await?;
        Ok(AuthUser { id, login, token_hash })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hashes_and_verifies() {
        let auth = Auth::new();
        let hash = auth.hash_password("correct horse".into()).await.unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(!hash.contains("correct horse"));
        assert!(auth.verify_password("correct horse".into(), Some(hash.clone())).await.unwrap());
        assert!(!auth.verify_password("wrong".into(), Some(hash)).await.unwrap());
        assert!(!auth.verify_password("dummy password for timing".into(), None).await.unwrap());
    }

    #[test]
    fn session_expiry_slides_and_persists_hourly() {
        let auth = Auth::new();
        let (_, h) = new_token();
        let t0 = 1_000_000;
        auth.cache_session(h, 7, t0 + SESSION_TTL, t0);
        assert_eq!(auth.lookup(&h, t0 + 10), Lookup::Hit { user_id: 7, touch: None });
        let later = t0 + TOUCH_INTERVAL + 1;
        assert_eq!(auth.lookup(&h, later), Lookup::Hit { user_id: 7, touch: Some(later + SESSION_TTL) });
        // Unused for 30 days after the last use: expired.
        assert_eq!(auth.lookup(&h, later + SESSION_TTL + 1), Lookup::Expired);
        assert_eq!(auth.lookup(&h, later + SESSION_TTL + 2), Lookup::Miss);
    }

    #[test]
    fn forget_invalidates() {
        let auth = Auth::new();
        let (_, h) = new_token();
        auth.cache_session(h, 1, 10_000, 0);
        auth.forget_session(&h);
        assert_eq!(auth.lookup(&h, 1), Lookup::Miss);
    }

    #[test]
    fn throttles_after_five_failures() {
        let auth = Auth::new();
        for _ in 0..4 {
            auth.record_failure("bob");
        }
        assert!(!auth.throttled("bob"));
        auth.record_failure("bob");
        assert!(auth.throttled("bob"));
        assert!(!auth.throttled("alice"));
        auth.clear_failures("bob");
        assert!(!auth.throttled("bob"));
    }

    #[test]
    fn tokens_are_random_and_cookie_is_strict() {
        let (a, ha) = new_token();
        let (b, _) = new_token();
        assert_ne!(a, b);
        assert_eq!(a.len(), 43); // 32 bytes, base64url without padding
        assert_eq!(hash_token(&a), ha);
        let c = session_cookie(&a);
        assert!(c.contains("HttpOnly") && c.contains("SameSite=Strict"));
    }
}

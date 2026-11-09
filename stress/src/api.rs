//! HTTP API client shared by all virtual users (design D3).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::{Method, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::stats::{Ep, Stats};

pub const PASSWORD: &str = "stress-password";

#[derive(Debug, Clone)]
pub struct Session {
    pub id: i64,
    pub cookie: Arc<str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallError {
    /// No answer: connection error or timeout.
    Transport,
    /// An HTTP error status, e.g. 403 for a channel non-owner.
    Status(u16),
}

pub type CallResult<T> = Result<T, CallError>;

#[derive(Debug, Clone, Deserialize)]
pub struct MyChat {
    pub id: i64,
    pub kind: String,
    pub is_owner: bool,
    pub last_seq: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Brief {
    pub id: i64,
    pub kind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Home {
    pub my_chats: Vec<MyChat>,
    pub top_groups: Vec<Brief>,
    pub top_channels: Vec<Brief>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Seq {
    pub seq: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Page {
    pub messages: Vec<Seq>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatInfo {
    pub id: i64,
    pub is_member: bool,
    pub can_post: bool,
    pub last_seq: i64,
}

/// Whether an answer counts as the service working. 4xx answers that are part
/// of normal use (403 for a channel reader, 409 for a full chat) do; auth
/// failures and 5xx don't.
fn healthy(status: StatusCode) -> bool {
    !(status.is_server_error() || status == StatusCode::UNAUTHORIZED || status == StatusCode::TOO_MANY_REQUESTS)
}

pub struct Api {
    http: reqwest::Client,
    base: String,
    pub server: SocketAddr,
    pub stats: Arc<Stats>,
}

impl Api {
    pub fn new(server: SocketAddr, stats: Arc<Stats>) -> Api {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(512)
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .build()
            .expect("http client");
        Api { http, base: format!("http://{server}/api"), server, stats }
    }

    async fn call<T: DeserializeOwned>(
        &self,
        ep: Ep,
        key: usize,
        method: Method,
        path: &str,
        cookie: Option<&str>,
        body: Option<Value>,
    ) -> CallResult<(T, reqwest::header::HeaderMap)> {
        let mut req = self.http.request(method, format!("{}{path}", self.base));
        if let Some(c) = cookie {
            req = req.header("cookie", c);
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        let start = Instant::now();
        let resp = match req.send().await {
            Ok(r) => r,
            Err(_) => {
                self.stats.record_failure(ep);
                return Err(CallError::Transport);
            }
        };
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(_) => {
                self.stats.record_failure(ep);
                return Err(CallError::Transport);
            }
        };
        self.stats.record_call(ep, key, start.elapsed().as_micros() as u64, healthy(status), status.is_success());
        if !status.is_success() {
            return Err(CallError::Status(status.as_u16()));
        }
        let text = if bytes.is_empty() { b"null".as_slice() } else { &bytes };
        serde_json::from_slice(text).map(|v| (v, headers)).map_err(|_| CallError::Status(status.as_u16()))
    }

    pub async fn register(&self, login: &str) -> CallResult<Session> {
        #[derive(Deserialize)]
        struct Me {
            id: i64,
        }
        let body = json!({ "login": login, "password": PASSWORD });
        let (me, headers): (Me, _) = self.call(Ep::Register, 0, Method::POST, "/register", None, Some(body)).await?;
        let cookie = headers
            .get("set-cookie")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .ok_or(CallError::Status(200))?;
        Ok(Session { id: me.id, cookie: cookie.into() })
    }

    pub async fn home(&self, s: &Session) -> CallResult<Home> {
        Ok(self.call(Ep::Home, s.id as usize, Method::GET, "/home", Some(&s.cookie), None).await?.0)
    }

    pub async fn chat(&self, s: &Session, chat: i64) -> CallResult<ChatInfo> {
        Ok(self.call(Ep::Home, s.id as usize, Method::GET, &format!("/chats/{chat}"), Some(&s.cookie), None).await?.0)
    }

    pub async fn history(&self, s: &Session, chat: i64, before: Option<i64>, after: Option<i64>) -> CallResult<Page> {
        let mut path = format!("/chats/{chat}/messages?limit=50");
        if let Some(b) = before {
            path.push_str(&format!("&before={b}"));
        }
        if let Some(a) = after {
            path.push_str(&format!("&after={a}"));
        }
        Ok(self.call(Ep::History, s.id as usize, Method::GET, &path, Some(&s.cookie), None).await?.0)
    }

    pub async fn post(&self, s: &Session, chat: i64, body: &str) -> CallResult<i64> {
        let (m, _): (Seq, _) = self
            .call(Ep::Post, s.id as usize, Method::POST, &format!("/chats/{chat}/messages"), Some(&s.cookie), Some(json!({ "body": body })))
            .await?;
        Ok(m.seq)
    }

    pub async fn join(&self, s: &Session, chat: i64) -> CallResult<()> {
        let path = format!("/chats/{chat}/join");
        self.call::<Value>(Ep::Join, s.id as usize, Method::POST, &path, Some(&s.cookie), None).await.map(|_| ())
    }

    pub async fn leave(&self, s: &Session, chat: i64) -> CallResult<()> {
        let path = format!("/chats/{chat}/leave");
        self.call::<Value>(Ep::Leave, s.id as usize, Method::POST, &path, Some(&s.cookie), None).await.map(|_| ())
    }

    pub async fn create(&self, s: &Session, kind: &str, title: &str) -> CallResult<i64> {
        let body = json!({ "kind": kind, "title": title });
        let (c, _): (ChatInfo, _) = self.call(Ep::Create, s.id as usize, Method::POST, "/chats", Some(&s.cookie), Some(body)).await?;
        Ok(c.id)
    }

    pub async fn read(&self, s: &Session, chat: i64, seq: i64) -> CallResult<()> {
        let path = format!("/chats/{chat}/read");
        self.call::<()>(Ep::Read, s.id as usize, Method::POST, &path, Some(&s.cookie), Some(json!({ "seq": seq }))).await.map(|_| ())
    }

    /// `GET /api/metrics`; not recorded in the statistics.
    pub async fn metrics(&self) -> Option<Value> {
        let r = self.http.get(format!("{}/metrics", self.base)).send().await.ok()?;
        r.json().await.ok()
    }
}

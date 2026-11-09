#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use chats_openspec::{Config, Running, start};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Response, StatusCode};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub struct Server {
    pub running: Option<Running>,
    pub base: String,
    pub data_dir: PathBuf,
    _tmp: Option<tempfile::TempDir>,
}

pub fn static_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("static")
}

impl Server {
    pub async fn start() -> Server {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Server::start_in(tmp.path()).await;
        s._tmp = Some(tmp);
        s
    }

    /// Starts without `--metrics`.
    pub async fn start_without_metrics() -> Server {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = Server::start_with(tmp.path(), false).await;
        s._tmp = Some(tmp);
        s
    }

    /// Starts on an existing data dir (the caller keeps it alive).
    pub async fn start_in(data_dir: &Path) -> Server {
        Server::start_with(data_dir, true).await
    }

    pub async fn start_with(data_dir: &Path, metrics: bool) -> Server {
        let config = Config {
            addr: "127.0.0.1:0".parse().unwrap(),
            data_dir: data_dir.to_path_buf(),
            static_dir: static_dir(),
            shards: Some(4),
            metrics,
        };
        let running = start(config).await.unwrap();
        let base = format!("http://{}", running.addr);
        Server { running: Some(running), base, data_dir: data_dir.to_path_buf(), _tmp: None }
    }

    pub async fn stop(&mut self) {
        if let Some(r) = self.running.take() {
            r.stop().await;
        }
    }

    pub async fn metrics(&self) -> Value {
        let resp = reqwest::get(self.url("/api/metrics")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        resp.json().await.unwrap()
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    pub fn ws_url(&self) -> String {
        format!("{}/api/ws", self.base.replace("http://", "ws://"))
    }

    pub async fn register(&self, login: &str) -> User {
        register(&self.base, login).await
    }
}

/// Registers `login` against the server at `base` (`http://host:port`).
pub async fn register(base: &str, login: &str) -> User {
    let client = Client::builder().cookie_store(true).build().unwrap();
    let resp = client
        .post(format!("{base}/api/register"))
        .json(&json!({ "login": login, "password": "password123" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "register {login}");
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let body: Value = resp.json().await.unwrap();
    User {
        client,
        id: body["id"].as_i64().unwrap(),
        cookie,
        base: base.to_string(),
        ws_url: format!("{}/api/ws", base.replace("http://", "ws://")),
    }
}

pub struct User {
    pub client: Client,
    pub id: i64,
    pub cookie: String,
    pub base: String,
    pub ws_url: String,
}

pub async fn json_of(resp: Response) -> (StatusCode, Value) {
    let status = resp.status();
    let text = resp.text().await.unwrap();
    (status, if text.is_empty() { Value::Null } else { serde_json::from_str(&text).unwrap() })
}

impl User {
    fn url(&self, path: &str) -> String {
        format!("{}/api{}", self.base, path)
    }

    pub async fn get(&self, path: &str) -> (StatusCode, Value) {
        json_of(self.client.get(self.url(path)).send().await.unwrap()).await
    }

    pub async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        json_of(self.client.post(self.url(path)).json(&body).send().await.unwrap()).await
    }

    pub async fn delete(&self, path: &str) -> (StatusCode, Value) {
        json_of(self.client.delete(self.url(path)).send().await.unwrap()).await
    }

    pub async fn create_chat(&self, kind: &str, title: &str) -> i64 {
        let (s, v) = self.post("/chats", json!({ "kind": kind, "title": title })).await;
        assert_eq!(s, StatusCode::CREATED, "{v}");
        v["id"].as_i64().unwrap()
    }

    pub async fn join(&self, chat: i64) -> Value {
        let (s, v) = self.post(&format!("/chats/{chat}/join"), json!({})).await;
        assert_eq!(s, StatusCode::OK, "{v}");
        v
    }

    pub async fn send(&self, chat: i64, body: &str) -> Value {
        let (s, v) = self.post(&format!("/chats/{chat}/messages"), json!({ "body": body })).await;
        assert_eq!(s, StatusCode::CREATED, "{v}");
        v
    }

    pub async fn home(&self) -> Value {
        let (s, v) = self.get("/home").await;
        assert_eq!(s, StatusCode::OK);
        v
    }

    pub async fn my_chat(&self, chat: i64) -> Option<Value> {
        let home = self.home().await;
        home["my_chats"].as_array().unwrap().iter().find(|c| c["id"] == chat).cloned()
    }

    pub async fn ws(&self) -> Ws {
        let mut req = self.ws_url.clone().into_client_request().unwrap();
        req.headers_mut().insert("cookie", self.cookie.parse().unwrap());
        let (stream, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        Ws { stream }
    }
}

pub struct Ws {
    pub stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Ws {
    pub async fn send(&mut self, v: Value) {
        self.stream.send(WsMessage::Text(v.to_string().into())).await.unwrap();
    }

    pub async fn open(&mut self, chat: i64) {
        self.send(json!({ "t": "open", "chat": chat })).await;
        // Give the server a moment to register the subscription.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    /// Next JSON frame, or `None` if nothing arrives within `wait`.
    pub async fn next_within(&mut self, wait: Duration) -> Option<Value> {
        loop {
            match tokio::time::timeout(wait, self.stream.next()).await {
                Ok(Some(Ok(WsMessage::Text(t)))) => return Some(serde_json::from_str(&t).unwrap()),
                Ok(Some(Ok(_))) => continue,
                _ => return None,
            }
        }
    }

    pub async fn next(&mut self) -> Value {
        self.next_within(Duration::from_secs(5)).await.expect("expected a frame")
    }

    /// Skips frames until one with type `t` arrives.
    pub async fn next_of(&mut self, t: &str) -> Value {
        loop {
            let f = self.next().await;
            if f["t"] == t {
                return f;
            }
        }
    }
}

pub async fn eventually<F, Fut>(mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if f().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition not met within 10s");
}

pub fn meta_conn(s: &Server) -> rusqlite::Connection {
    let c = rusqlite::Connection::open(s.data_dir.join("meta.db")).unwrap();
    c.busy_timeout(Duration::from_secs(5)).unwrap();
    c
}

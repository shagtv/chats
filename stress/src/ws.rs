//! WebSocket client for virtual users (design D3, D4).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::{TcpSocket, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::api::{Api, Session};
use crate::net::Ports;
use crate::stats::Ep;

pub type Sink = SplitSink<WebSocketStream<TcpStream>, Message>;
type Stream = SplitStream<WebSocketStream<TcpStream>>;

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectError {
    NoPorts,
    Failed,
}

/// Only the fields the generator needs; the body is skipped by serde.
#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(borrow)]
    t: &'a str,
    #[serde(default)]
    seq: i64,
    #[serde(default)]
    ts: i64,
    #[serde(default)]
    chat: i64,
    #[serde(default)]
    author: Option<AuthorId>,
}

#[derive(Deserialize)]
struct AuthorId {
    id: i64,
}

/// A connected socket's writing half; reading runs in its own task.
pub struct Ws {
    sink: Sink,
    reader: tokio::task::JoinHandle<()>,
    closing: Arc<AtomicBool>,
}

impl Ws {
    /// Opens a chat (`{"t":"open"}`).
    pub async fn open(&mut self, chat: i64) -> bool {
        self.sink.send(Message::Text(format!(r#"{{"t":"open","chat":{chat}}}"#).into())).await.is_ok()
    }

    /// Whether the server side is still connected.
    pub fn alive(&self) -> bool {
        !self.reader.is_finished()
    }

    /// Closes on purpose, so the reader doesn't count a disconnect.
    pub async fn close(mut self) {
        self.closing.store(true, Ordering::Relaxed);
        let _ = self.sink.close().await;
        let _ = self.reader.await;
    }
}

/// Connects from a source address chosen by `ports`, then starts the reader.
pub async fn connect(api: &Arc<Api>, ports: &Arc<Ports>, session: &Session) -> Result<Ws, ConnectError> {
    let src = ports.acquire().ok_or(ConnectError::NoPorts)?;
    let stats = &api.stats;
    stats.connecting.fetch_add(1, Ordering::Relaxed);
    let start = Instant::now();
    let result = handshake(api.server, src, &session.cookie).await;
    stats.connecting.fetch_sub(1, Ordering::Relaxed);
    let socket = match result {
        Some(s) => {
            stats.record_call(Ep::WsConnect, session.id as usize, start.elapsed().as_micros() as u64, true, true);
            s
        }
        None => {
            stats.record_failure(Ep::WsConnect);
            ports.release();
            return Err(ConnectError::Failed);
        }
    };
    stats.online.fetch_add(1, Ordering::Relaxed);
    let (sink, stream) = socket.split();
    let closing = Arc::new(AtomicBool::new(false));
    let reader = tokio::spawn(read_loop(stream, api.clone(), ports.clone(), session.clone(), closing.clone()));
    Ok(Ws { sink, reader, closing })
}

async fn handshake(server: SocketAddr, src: std::net::IpAddr, cookie: &str) -> Option<WebSocketStream<TcpStream>> {
    let socket = TcpSocket::new_v4().ok()?;
    socket.bind(SocketAddr::new(src, 0)).ok()?;
    let stream = socket.connect(server).await.ok()?;
    stream.set_nodelay(true).ok()?;
    let mut req = format!("ws://{server}/api/ws").into_client_request().ok()?;
    req.headers_mut().insert("cookie", cookie.parse().ok()?);
    let (ws, _) = tokio_tungstenite::client_async(req, stream).await.ok()?;
    Some(ws)
}

/// Decrements `online` and frees the port when the reader ends, however it ends.
struct OnlineGuard {
    api: Arc<Api>,
    ports: Arc<Ports>,
}

impl Drop for OnlineGuard {
    fn drop(&mut self) {
        self.api.stats.online.fetch_sub(1, Ordering::Relaxed);
        self.ports.release();
    }
}

async fn read_loop(mut stream: Stream, api: Arc<Api>, ports: Arc<Ports>, session: Session, closing: Arc<AtomicBool>) {
    let _guard = OnlineGuard { api: api.clone(), ports };
    let stats = &api.stats;
    let mut resynced_window = 0;
    let mut last_seq = 0;
    while let Some(msg) = stream.next().await {
        let text = match msg {
            Ok(Message::Text(t)) => t,
            Ok(Message::Close(_)) => break,
            Ok(_) => continue,
            Err(_) => break,
        };
        stats.frames.fetch_add(1, Ordering::Relaxed);
        let Ok(f) = serde_json::from_str::<Frame>(text.as_str()) else { continue };
        match f.t {
            "msg" => {
                last_seq = last_seq.max(f.seq);
                if f.author.is_none_or(|a| a.id != session.id) {
                    stats.delivery_ms.record(session.id as usize, (now_ms() - f.ts).max(0) as u64);
                }
            }
            "resync" => {
                stats.resync_frames.fetch_add(1, Ordering::Relaxed);
                stats.note_resynced(&mut resynced_window);
                // A real client refetches what it missed.
                let (api, session, chat, after) = (api.clone(), session.clone(), f.chat, last_seq);
                tokio::spawn(async move {
                    let _ = api.history(&session, chat, None, Some(after)).await;
                });
            }
            _ => {}
        }
    }
    if !closing.load(Ordering::Relaxed) {
        stats.ws_disconnects.fetch_add(1, Ordering::Relaxed);
    }
}

//! WebSocket protocol (design D8, specs/realtime-updates).
//!
//! Client -> server: `{"t":"open","chat":ID}`, `{"t":"close"}`.
//! Server -> client: `msg`, `seq`, `deleted`, `resync`, `error`.

use axum::extract::State;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc};

use crate::app::App;
use crate::auth::AuthUser;
use crate::db::{self, meta};
use crate::hub::CONTROL_CAPACITY;

#[derive(Deserialize)]
#[serde(tag = "t", rename_all = "lowercase")]
enum ClientFrame {
    Open { chat: i64 },
    Close,
}

pub async fn handler(State(app): State<App>, user: AuthUser, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| run(app, user.id, socket))
}

struct Open {
    chat: i64,
    rx: broadcast::Receiver<Utf8Bytes>,
}

enum Live {
    Frame(Utf8Bytes),
    Lagged,
    Closed,
}

async fn recv_live(open: &mut Option<Open>) -> Live {
    match open {
        None => std::future::pending().await,
        Some(o) => match o.rx.recv().await {
            Ok(f) => Live::Frame(f),
            Err(RecvError::Lagged(_)) => Live::Lagged,
            Err(RecvError::Closed) => Live::Closed,
        },
    }
}

async fn run(app: App, user_id: i64, mut socket: WebSocket) {
    let (control_tx, mut control_rx) = mpsc::channel(CONTROL_CAPACITY);
    let conn_id = app.hub.connect(user_id, control_tx);
    match db::read(&app.db.meta_pool, move |c| meta::user_chats(c, user_id)).await {
        Ok(rows) => app.hub.add_user_chats(user_id, &rows.iter().map(|r| r.0).collect::<Vec<_>>()),
        Err(_) => {
            app.hub.disconnect(user_id, conn_id);
            return;
        }
    }

    let mut open: Option<Open> = None;
    loop {
        let out = tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<ClientFrame>(&text) {
                    Ok(ClientFrame::Open { chat }) => match app.hub.subscribe(chat) {
                        Some(rx) => {
                            open = Some(Open { chat, rx });
                            None
                        }
                        None => {
                            open = None;
                            Some(json!({ "t": "error", "code": "not_found", "chat": chat }).to_string().into())
                        }
                    },
                    Ok(ClientFrame::Close) => {
                        open = None;
                        None
                    }
                    Err(_) => None,
                },
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => None,
            },
            live = recv_live(&mut open) => match live {
                Live::Frame(f) => Some(f),
                Live::Lagged => {
                    // Drop whatever is still queued and let the client refetch.
                    let o = open.as_mut().expect("lagged implies open");
                    o.rx = o.rx.resubscribe();
                    Some(json!({ "t": "resync", "chat": o.chat }).to_string().into())
                }
                Live::Closed => {
                    open = None;
                    None
                }
            },
            control = control_rx.recv() => match control {
                Some(f) => Some(f),
                None => break, // dropped by the hub as too slow
            },
        };
        if let Some(frame) = out
            && socket.send(Message::Text(frame)).await.is_err()
        {
            break;
        }
    }
    app.hub.disconnect(user_id, conn_id);
}

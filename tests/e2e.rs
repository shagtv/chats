//! Runs the compiled binary as a separate process and drives it over HTTP and WebSocket.

mod common;

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::{json_of, register};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

struct Proc {
    child: Child,
    base: String,
}

impl Proc {
    async fn spawn(data_dir: &Path) -> Proc {
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let child = Command::new(env!("CARGO_BIN_EXE_chats-openspec"))
            .args(["--addr", &format!("127.0.0.1:{port}")])
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--static-dir")
            .arg(common::static_dir())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        // Wrapped right away so Drop reaps the process if startup fails.
        let proc = Proc { child, base: format!("http://127.0.0.1:{port}") };
        for _ in 0..100 {
            if reqwest::get(format!("{}/", proc.base)).await.is_ok() {
                return proc;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("binary did not start");
    }

    /// Ctrl-C equivalent; returns once the process has exited.
    fn interrupt(mut self) -> std::process::ExitStatus {
        Command::new("kill").args(["-INT", &self.child.id().to_string()]).status().unwrap();
        for _ in 0..100 {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("binary did not exit after SIGINT");
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn next_frame(ws: &mut common::Ws, t: &str) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.stream.next()).await {
            Ok(Some(Ok(WsMessage::Text(text)))) => {
                let v: Value = serde_json::from_str(&text).unwrap();
                if v["t"] == t {
                    return v;
                }
            }
            Ok(Some(Ok(_))) => {}
            other => panic!("no {t} frame: {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn binary_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let p = Proc::spawn(tmp.path()).await;

    let alice = register(&p.base, "alice").await;
    let bob = register(&p.base, "bob").await;
    let carol = register(&p.base, "carol").await;

    // Group flow: alice owns, bob joins and talks, carol only reads.
    let group = alice.create_chat("group", "rust").await;
    bob.join(group).await;
    let mut carol_ws = carol.ws().await;
    carol_ws.open(group).await;
    alice.send(group, "welcome").await;
    let m = bob.send(group, "thanks").await;
    assert_eq!(m["seq"], 2);
    assert_eq!(next_frame(&mut carol_ws, "msg").await["body"], "welcome");
    assert_eq!(next_frame(&mut carol_ws, "msg").await["body"], "thanks");
    let (st, _) = carol.post(&format!("/chats/{group}/messages"), json!({ "body": "hi" })).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "readers must join to post");
    assert_eq!(alice.my_chat(group).await.unwrap()["unread"], 1);

    // Channel flow: carol broadcasts, alice joins and gets badge updates.
    let channel = carol.create_chat("channel", "news").await;
    alice.join(channel).await;
    let mut alice_ws = alice.ws().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    carol.send(channel, "issue 1").await;
    let f = next_frame(&mut alice_ws, "seq").await;
    assert_eq!(f["chats"][channel.to_string()], 1);
    let (st, _) = alice.post(&format!("/chats/{channel}/messages"), json!({ "body": "me too" })).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "only the owner posts in a channel");

    // Top lists may lag by up to a few seconds.
    common::eventually(|| async {
        let home = bob.home().await;
        home["top_channels"][0]["id"] == channel && home["top_groups"][0]["id"] == group
    })
    .await;

    // Graceful shutdown and restart keep everything.
    let status = p.interrupt();
    assert!(status.success(), "clean exit on SIGINT, got {status:?}");
    let p = Proc::spawn(tmp.path()).await;
    let anon = Client::new();
    let r = anon
        .get(format!("{}/api/chats/{group}/messages", p.base))
        .header("cookie", &bob.cookie)
        .send()
        .await
        .unwrap();
    let (st, page) = json_of(r).await;
    assert_eq!(st, StatusCode::OK);
    let bodies: Vec<&str> = page["messages"].as_array().unwrap().iter().map(|m| m["body"].as_str().unwrap()).collect();
    assert_eq!(bodies, vec!["thanks", "welcome"]);

    let mut req = format!("{}/api/ws", p.base.replace("http", "ws")).into_client_request().unwrap();
    req.headers_mut().insert("cookie", alice.cookie.parse().unwrap());
    assert!(tokio_tungstenite::connect_async(req).await.is_ok(), "sessions survive restart");

    let r = anon.delete(format!("{}/api/chats/{group}", p.base)).header("cookie", &alice.cookie).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::NO_CONTENT);
    let r = anon.get(format!("{}/api/chats/{group}", p.base)).header("cookie", &bob.cookie).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    assert!(p.interrupt().success());
}

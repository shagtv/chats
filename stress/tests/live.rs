//! Runs against the real server binary built in the workspace target dir
//! (`cargo test --workspace` builds it before running tests).

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chats_stress::api::{Api, CallError};
use chats_stress::net::Ports;
use chats_stress::server::ServerProc;
use chats_stress::stats::{Ep, Stats};
use chats_stress::ws;

fn server_binary() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("target");
    for profile in ["debug", "release"] {
        let p = root.join(profile).join("chats-openspec");
        if p.is_file() {
            return p;
        }
    }
    panic!("server binary not found; run `cargo build -p chats-openspec` (or `cargo test --workspace`)");
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

async fn start(out: &tempfile::TempDir) -> ServerProc {
    ServerProc::start(&server_binary(), free_addr(), out.path(), false).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn server_lifecycle() {
    let out = tempfile::tempdir().unwrap();
    let server = start(&out).await;
    let data = server.data_dir.clone();
    assert!(data.join("meta.db").is_file());

    // A second server on the same address is refused before spawning.
    let busy = ServerProc::start(&server_binary(), server.addr, tempfile::tempdir().unwrap().path(), false).await;
    assert!(busy.err().unwrap().contains("not free"));

    server.stop().await.unwrap();
    assert!(!data.exists(), "data dir removed without --keep-data");

    let out2 = tempfile::tempdir().unwrap();
    let kept = ServerProc::start(&server_binary(), free_addr(), out2.path(), true).await.unwrap();
    let data = kept.data_dir.clone();
    kept.stop().await.unwrap();
    assert!(data.exists(), "--keep-data keeps it");
}

#[tokio::test(flavor = "multi_thread")]
async fn api_calls_are_recorded() {
    let out = tempfile::tempdir().unwrap();
    let server = start(&out).await;
    let stats = Arc::new(Stats::default());
    let api = Api::new(server.addr, stats.clone());

    let owner = api.register("owner").await.unwrap();
    let member = api.register("member").await.unwrap();
    let group = api.create(&owner, "group", "g").await.unwrap();
    let channel = api.create(&owner, "channel", "c").await.unwrap();
    api.join(&member, group).await.unwrap();
    api.join(&member, channel).await.unwrap();
    assert_eq!(api.post(&member, group, "hi").await.unwrap(), 1);
    assert_eq!(api.post(&member, channel, "no").await, Err(CallError::Status(403)));
    let home = api.home(&member).await.unwrap();
    assert_eq!(home.my_chats.len(), 2);
    assert!(!home.top_groups.is_empty() || !home.top_channels.is_empty() || home.my_chats.len() == 2);
    let page = api.history(&member, group, None, None).await.unwrap();
    assert_eq!(page.messages[0].seq, 1);
    api.read(&member, group, 1).await.unwrap();
    api.leave(&member, group).await.unwrap();
    assert!(!api.chat(&member, group).await.unwrap().is_member);

    let c = stats.counters();
    assert_eq!(c.ok(Ep::Register), 2);
    assert_eq!(c.ok(Ep::Post), 2, "a 403 is a healthy answer");
    assert_eq!(c.accepted(Ep::Post), 1, "but not an accepted post");
    assert_eq!(c.failures(), 0);
    let i = stats.drain();
    assert_eq!(i.ep(Ep::Join).len(), 2);
    assert!(api.metrics().await.unwrap()["ws"].is_object());
    server.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_delivery_samples() {
    let out = tempfile::tempdir().unwrap();
    let server = start(&out).await;
    let stats = Arc::new(Stats::default());
    let api = Arc::new(Api::new(server.addr, stats.clone()));
    let ports = Arc::new(Ports::detect());

    let owner = api.register("owner").await.unwrap();
    let chat = api.create(&owner, "group", "g").await.unwrap();
    let mut viewers = Vec::new();
    for i in 0..3 {
        let v = api.register(&format!("viewer{i}")).await.unwrap();
        let mut w = ws::connect(&api, &ports, &v).await.unwrap();
        assert!(w.open(chat).await);
        viewers.push(w);
    }
    // The author views too; their own frame is not a delivery sample.
    let mut own = ws::connect(&api, &ports, &owner).await.unwrap();
    own.open(chat).await;
    assert_eq!(stats.online.load(Ordering::Relaxed), 4);
    assert_eq!(ports.used(), 4);
    tokio::time::sleep(Duration::from_millis(100)).await;

    api.post(&owner, chat, "hello").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let i = stats.drain();
    assert_eq!(i.delivery.len(), 3, "one sample per other viewer");
    assert!(i.delivery.max() < 1000);

    for w in viewers {
        w.close().await;
    }
    own.close().await;
    assert_eq!(stats.online.load(Ordering::Relaxed), 0);
    assert_eq!(ports.used(), 0);
    assert_eq!(stats.counters().ws_disconnects, 0, "clean closes are not disconnects");
    server.stop().await.unwrap();
}

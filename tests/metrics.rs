mod common;

use common::{Server, eventually};
use reqwest::StatusCode;
use serde_json::{Value, json};

fn n(v: &Value, path: &str) -> i64 {
    path.split('.').fold(v, |v, k| &v[k]).as_i64().unwrap_or_else(|| panic!("{path} missing in {v}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_without_flag() {
    let s = Server::start_without_metrics().await;
    let r = reqwest::get(s.url("/api/metrics")).await.unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn enabled_without_session() {
    let s = Server::start().await;
    let r = reqwest::get(s.url("/api/metrics")).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK, "no cookie needed");
    let m: Value = r.json().await.unwrap();
    for key in ["uptime_s", "ws", "chats", "writers", "http"] {
        assert!(m.get(key).is_some(), "missing {key}");
    }
    assert_eq!(m["chats"]["max_online"], json!({ "group": null, "channel": null }));
    assert_eq!(n(&m, "writers.meta.queue"), 0);
    assert_eq!(n(&m, "writers.shards.queue_sum"), 0);
    assert_eq!(n(&m, "writers.shards.count"), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_counts_follow_sockets() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let chat = o.create_chat("group", "g").await;
    let before = s.metrics().await;

    let u = s.register("viewer").await;
    let mut a = u.ws().await;
    let _b = u.ws().await;
    a.open(chat).await;
    eventually(|| async {
        let m = s.metrics().await;
        n(&m, "ws.connections") == n(&before, "ws.connections") + 2
            && n(&m, "ws.online_users") == n(&before, "ws.online_users") + 1
            && n(&m, "ws.open_subscriptions") == n(&before, "ws.open_subscriptions") + 1
    })
    .await;

    drop(a);
    drop(_b);
    eventually(|| async {
        let m = s.metrics().await;
        n(&m, "ws.connections") == n(&before, "ws.connections")
            && n(&m, "ws.online_users") == n(&before, "ws.online_users")
            && n(&m, "ws.open_subscriptions") == n(&before, "ws.open_subscriptions")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn chat_and_writer_metrics() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let m = s.register("member").await;
    let g1 = o.create_chat("group", "small").await;
    let g2 = o.create_chat("group", "big").await;
    let c = o.create_chat("channel", "news").await;
    m.join(g2).await;
    let mut ws = m.ws().await;
    ws.open(g2).await;

    let before = s.metrics().await;
    assert_eq!((n(&before, "chats.groups"), n(&before, "chats.channels")), (2, 1));
    assert_eq!(n(&before, "chats.max_members.group"), 2);
    assert_eq!(n(&before, "chats.max_members.channel"), 1);
    eventually(|| async { s.metrics().await["chats"]["max_online"]["group"] == json!({ "id": g2, "online": 1 }) }).await;

    for i in 0..10 {
        o.send(c, &format!("m{i}")).await;
    }
    let after = s.metrics().await;
    let cmds = n(&after, "writers.shards.cmds_total") - n(&before, "writers.shards.cmds_total");
    let commits = n(&after, "writers.shards.commits_total") - n(&before, "writers.shards.commits_total");
    assert!(cmds >= 10, "cmds {cmds}");
    assert!((1..=10).contains(&commits), "commits {commits}");

    for _ in 0..3 {
        o.home().await;
    }
    let later = s.metrics().await;
    assert!(n(&later, "http.requests_total") - n(&after, "http.requests_total") >= 3);
    assert_eq!(n(&later, "http.errors_5xx_total"), 0);

    o.delete(&format!("/chats/{g1}")).await;
    assert_eq!(n(&s.metrics().await, "chats.groups"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn polling_does_not_write() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    o.create_chat("group", "g").await;
    let commits = |m: &Value| n(m, "writers.meta.commits_total") + n(m, "writers.shards.commits_total");
    let first = commits(&s.metrics().await);
    for _ in 0..5 {
        s.metrics().await;
    }
    assert_eq!(commits(&s.metrics().await), first);
}

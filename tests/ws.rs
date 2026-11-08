mod common;

use std::time::Duration;

use common::Server;
use serde_json::json;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[tokio::test(flavor = "multi_thread")]
async fn upgrade_requires_session() {
    let s = Server::start().await;
    let req = s.ws_url().into_client_request().unwrap();
    let err = tokio_tungstenite::connect_async(req).await.unwrap_err();
    match err {
        tokio_tungstenite::tungstenite::Error::Http(resp) => assert_eq!(resp.status().as_u16(), 401),
        other => panic!("unexpected {other:?}"),
    }
    let u = s.register("alice").await;
    let _ws = u.ws().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn open_chat_receives_live_messages() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let chat1 = o.create_chat("group", "one").await;
    let chat2 = o.create_chat("group", "two").await;

    let mut viewers = Vec::new();
    for i in 0..3 {
        let v = s.register(&format!("viewer{i}")).await;
        let mut ws = v.ws().await;
        ws.open(chat1).await; // non-members may open a chat
        viewers.push((v, ws));
    }
    let sent = o.send(chat1, "hello").await;
    for (_, ws) in &mut viewers {
        let f = ws.next_of("msg").await;
        assert_eq!(f["seq"], sent["seq"]);
        assert_eq!(f["body"], "hello");
        assert_eq!(f["chat_id"], chat1);
        assert_eq!(f["author"]["login"], "owner");
    }

    // Switching replaces the subscription.
    let ws = &mut viewers[0].1;
    ws.open(chat2).await;
    o.send(chat1, "not for you").await;
    o.send(chat2, "for you").await;
    let f = ws.next_of("msg").await;
    assert_eq!(f["body"], "for you");

    ws.send(json!({ "t": "open", "chat": 424242 })).await;
    assert_eq!(ws.next_of("error").await, json!({ "t": "error", "code": "not_found", "chat": 424242 }));
    o.send(chat2, "after error").await;
    assert!(ws.next_within(Duration::from_millis(500)).await.is_none(), "no open chat after an error");
}

#[tokio::test(flavor = "multi_thread")]
async fn live_messages_arrive_in_order() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let chat = o.create_chat("group", "g").await;
    let v = s.register("viewer").await;
    let mut ws = v.ws().await;
    ws.open(chat).await;
    for i in 0..30 {
        o.send(chat, &format!("m{i}")).await;
    }
    for seq in 1..=30 {
        assert_eq!(ws.next_of("msg").await["seq"], seq);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn seq_updates_are_batched() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let m = s.register("member").await;
    let channel = o.create_chat("channel", "news").await;
    m.join(channel).await;
    let mut ws = m.ws().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..20 {
        o.send(channel, &format!("n{i}")).await;
    }
    // At most one frame per second, carrying the latest last_seq.
    let mut latest = 0;
    let mut frames = 0;
    let start = std::time::Instant::now();
    while latest < 20 {
        let f = ws.next_of("seq").await;
        frames += 1;
        latest = f["chats"][channel.to_string()].as_i64().unwrap();
    }
    let elapsed = start.elapsed().as_secs_f64();
    assert!(frames as f64 <= elapsed.ceil() + 1.0, "{frames} seq frames in {elapsed:.1}s");

    // A chat joined after connecting starts sending updates without reconnecting.
    let g = o.create_chat("group", "later").await;
    m.join(g).await;
    o.send(g, "x").await;
    loop {
        let f = ws.next_of("seq").await;
        if f["chats"].get(g.to_string()).is_some() {
            assert_eq!(f["chats"][g.to_string()], 1);
            break;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deletion_notifies_viewers_and_members() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let viewer = s.register("viewer").await;
    let member = s.register("member").await;
    let chat = o.create_chat("group", "doomed").await;
    member.join(chat).await;

    let mut vws = viewer.ws().await;
    vws.open(chat).await;
    let mut mws = member.ws().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    o.delete(&format!("/chats/{chat}")).await;
    assert_eq!(vws.next_of("deleted").await, json!({ "t": "deleted", "chat": chat }));
    assert_eq!(mws.next_of("deleted").await, json!({ "t": "deleted", "chat": chat }));
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_reader_gets_resync_without_slowing_others() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let chat = o.create_chat("group", "busy").await;
    let slow = s.register("slow").await;
    let fast = s.register("fast").await;
    let mut slow_ws = slow.ws().await;
    slow_ws.open(chat).await;
    let mut fast_ws = fast.ws().await;
    fast_ws.open(chat).await;

    let total = 1500;
    let body = "x".repeat(4000);
    let reader = tokio::spawn(async move {
        let mut seen = 0;
        while seen < total {
            let f = fast_ws.next_of("msg").await;
            seen += 1;
            assert_eq!(f["seq"], seen, "fast reader sees every message in order");
        }
    });
    // Post concurrently so the server produces frames faster than the slow
    // socket (which nobody reads yet) can absorb them.
    let o = std::sync::Arc::new(o);
    let mut posters = Vec::new();
    for _ in 0..10 {
        let o = o.clone();
        let body = body.clone();
        posters.push(tokio::spawn(async move {
            for _ in 0..total / 10 {
                o.send(chat, &body).await;
            }
        }));
    }
    for p in posters {
        p.await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(30), reader).await.expect("fast reader kept up").unwrap();

    let mut got_resync = false;
    while let Some(f) = slow_ws.next_within(Duration::from_secs(2)).await {
        if f["t"] == "resync" {
            assert_eq!(f["chat"], chat);
            got_resync = true;
            break;
        }
    }
    assert!(got_resync, "slow reader should be told to resync");
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnect_catches_up_via_history() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let v = s.register("viewer").await;
    let chat = o.create_chat("group", "g").await;
    for i in 0..80 {
        o.send(chat, &format!("m{i}")).await;
    }
    let ws = v.ws().await;
    drop(ws); // network loss
    for i in 80..85 {
        o.send(chat, &format!("m{i}")).await;
    }
    let mut ws = v.ws().await;
    ws.open(chat).await;
    let (_, page) = v.get(&format!("/chats/{chat}/messages?after=80")).await;
    let seqs: Vec<i64> = page["messages"].as_array().unwrap().iter().map(|m| m["seq"].as_i64().unwrap()).collect();
    assert_eq!(seqs, vec![81, 82, 83, 84, 85]);
    o.send(chat, "live again").await;
    assert_eq!(ws.next_of("msg").await["seq"], 86);
}

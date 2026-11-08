mod common;

use common::{Server, eventually, json_of, meta_conn};
use reqwest::{Client, StatusCode};
use serde_json::json;

// ---------- user-auth ----------

#[tokio::test(flavor = "multi_thread")]
async fn registration_and_login() {
    let s = Server::start().await;
    let alice = s.register("alice").await;
    let (st, me) = alice.get("/me").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(me, json!({ "id": alice.id, "login": "alice" }));

    let anon = Client::new();
    let r = anon.post(s.url("/api/register")).json(&json!({ "login": "Alice", "password": "password123" })).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);

    for (login, pw, field) in [("ab", "password123", "login"), ("bad-name", "password123", "login"), ("carol", "short", "password")] {
        let (st, v) = json_of(anon.post(s.url("/api/register")).json(&json!({ "login": login, "password": pw })).send().await.unwrap()).await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains(field), "{v}");
    }

    // The stored credential is an argon2id hash, never the plaintext.
    let hash: String = meta_conn(&s).query_row("SELECT pw_hash FROM users WHERE login = 'alice'", [], |r| r.get(0)).unwrap();
    assert!(hash.starts_with("$argon2id$"));
    assert!(!hash.contains("password123"));

    let fresh = Client::builder().cookie_store(true).build().unwrap();
    let r = fresh.post(s.url("/api/login")).json(&json!({ "login": "ALICE", "password": "password123" })).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert!(r.headers().get("set-cookie").unwrap().to_str().unwrap().contains("HttpOnly"));
    assert_eq!(fresh.get(s.url("/api/me")).send().await.unwrap().status(), StatusCode::OK);

    let (st1, wrong_pw) = json_of(anon.post(s.url("/api/login")).json(&json!({ "login": "alice", "password": "nope-nope" })).send().await.unwrap()).await;
    let (st2, no_user) = json_of(anon.post(s.url("/api/login")).json(&json!({ "login": "nobody", "password": "nope-nope" })).send().await.unwrap()).await;
    assert_eq!((st1, st2), (StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED));
    assert_eq!(wrong_pw, no_user, "same generic error for both cases");
}

#[tokio::test(flavor = "multi_thread")]
async fn login_is_throttled() {
    let s = Server::start().await;
    s.register("bob").await;
    let anon = Client::new();
    let attempt = |pw: &'static str| {
        let req = anon.post(s.url("/api/login")).json(&json!({ "login": "bob", "password": pw }));
        async move { req.send().await.unwrap().status() }
    };
    for _ in 0..5 {
        assert_eq!(attempt("wrong-password").await, StatusCode::UNAUTHORIZED);
    }
    assert_eq!(attempt("password123").await, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test(flavor = "multi_thread")]
async fn logout_and_auth_required() {
    let s = Server::start().await;
    let anon = Client::new();
    assert_eq!(anon.get(s.url("/api/me")).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    assert_eq!(anon.get(s.url("/api/home")).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);

    let u = s.register("dave").await;
    let (st, _) = u.post("/logout", json!({})).await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    // Reusing the old token after logout fails.
    let r = anon.get(s.url("/api/me")).header("cookie", &u.cookie).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread")]
async fn sessions_survive_restart_and_expire() {
    let tmp = tempfile::tempdir().unwrap();
    let mut s = Server::start_in(tmp.path()).await;
    let u = s.register("erin").await;
    let other = s.register("frank").await;
    s.stop().await;

    let s = Server::start_in(tmp.path()).await;
    let anon = Client::new();
    let r = anon.get(s.url("/api/me")).header("cookie", &u.cookie).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::OK, "session read from disk after restart");

    // A session whose stored expiry has passed is rejected.
    meta_conn(&s).execute("UPDATE sessions SET expires_at = 1 WHERE user_id = ?", [other.id]).unwrap();
    let r = anon.get(s.url("/api/me")).header("cookie", &other.cookie).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
}

// ---------- chat-management ----------

#[tokio::test(flavor = "multi_thread")]
async fn create_and_get_chat() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let (st, chat) = o.post("/chats", json!({ "kind": "group", "title": "  rust  " })).await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(chat["title"], "rust");
    assert_eq!(chat["member_count"], 1);
    assert_eq!(chat["is_owner"], true);
    assert_eq!(chat["can_post"], true);
    assert_eq!(chat["owner"], json!({ "id": o.id, "login": "owner" }));

    for body in [
        json!({ "kind": "group", "title": "   " }),
        json!({ "kind": "group", "title": "x".repeat(101) }),
        json!({ "kind": "forum", "title": "t" }),
    ] {
        let (st, _) = o.post("/chats", body).await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
    }
    assert_eq!(o.get("/chats/999999").await.0, StatusCode::NOT_FOUND);
    assert_eq!(o.get("/chats/abc").await.0, StatusCode::NOT_FOUND);

    let viewer = s.register("viewer").await;
    let id = chat["id"].as_i64().unwrap();
    o.send(id, "hello").await;
    let (st, page) = viewer.get(&format!("/chats/{id}/messages")).await;
    assert_eq!(st, StatusCode::OK, "non-members can read");
    assert_eq!(page["messages"][0]["body"], "hello");
    let (_, v) = viewer.get(&format!("/chats/{id}")).await;
    assert_eq!((v["is_member"].clone(), v["can_post"].clone()), (json!(false), json!(false)));
}

#[tokio::test(flavor = "multi_thread")]
async fn join_leave_and_caps() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let u = s.register("member").await;
    let chat = o.create_chat("group", "g").await;
    for i in 0..5 {
        o.send(chat, &format!("m{i}")).await;
    }

    let v = u.join(chat).await;
    assert_eq!(v["member_count"], 2);
    assert_eq!(v["is_member"], true);
    let mine = u.my_chat(chat).await.expect("in my chats");
    assert_eq!(mine["unread"], 0, "a new member starts with nothing unread");
    assert_eq!(mine["last_read_seq"], 5);
    assert_eq!(u.join(chat).await["member_count"], 2, "join is idempotent");

    let (st, v) = u.post(&format!("/chats/{chat}/leave"), json!({})).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["member_count"], 1);
    assert!(u.my_chat(chat).await.is_none());
    let (st, v) = u.post(&format!("/chats/{chat}/leave"), json!({})).await;
    assert_eq!((st, v["member_count"].clone()), (StatusCode::OK, json!(1)), "leaving twice is a no-op");

    let (st, _) = o.post(&format!("/chats/{chat}/leave"), json!({})).await;
    assert_eq!(st, StatusCode::CONFLICT, "the owner can't leave");

    // A full group rejects joins.
    meta_conn(&s).execute("UPDATE chats SET member_count = 1000 WHERE id = ?", [chat]).unwrap();
    let (st, _) = u.post(&format!("/chats/{chat}/join"), json!({})).await;
    assert_eq!(st, StatusCode::CONFLICT);
    let n: i64 = meta_conn(&s).query_row("SELECT member_count FROM chats WHERE id = ?", [chat], |r| r.get(0)).unwrap();
    assert_eq!(n, 1000);
    assert_eq!(u.get(&format!("/chats/{chat}")).await.1["is_member"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_chat() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let u = s.register("member").await;
    let chat = o.create_chat("channel", "news").await;
    o.send(chat, "first").await;
    u.join(chat).await;

    let (st, _) = u.delete(&format!("/chats/{chat}")).await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(u.get(&format!("/chats/{chat}")).await.0, StatusCode::OK);

    let (st, _) = o.delete(&format!("/chats/{chat}")).await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    assert_eq!(u.get(&format!("/chats/{chat}")).await.0, StatusCode::NOT_FOUND);
    assert_eq!(u.get(&format!("/chats/{chat}/messages")).await.0, StatusCode::NOT_FOUND);
    assert_eq!(o.post(&format!("/chats/{chat}/messages"), json!({ "body": "x" })).await.0, StatusCode::NOT_FOUND);
    assert!(u.my_chat(chat).await.is_none());
    assert!(o.my_chat(chat).await.is_none());
    let home = o.home().await;
    assert!(home["top_channels"].as_array().unwrap().iter().all(|c| c["id"] != chat));
}

#[tokio::test(flavor = "multi_thread")]
async fn home_lists() {
    let s = Server::start().await;
    let mut users = Vec::new();
    for i in 0..15 {
        users.push(s.register(&format!("user{i}")).await);
    }
    // Group i ends up with i + 1 members.
    let mut groups = Vec::new();
    for i in 0..15 {
        let g = users[0].create_chat("group", &format!("g{i}")).await;
        for u in &users[1..=i] {
            u.join(g).await;
        }
        groups.push(g);
    }
    let channel = users[1].create_chat("channel", "news").await;

    let viewer = &users[14];
    eventually(|| async {
        let home = viewer.home().await;
        let top: Vec<i64> = home["top_groups"].as_array().unwrap().iter().map(|c| c["id"].as_i64().unwrap()).collect();
        top == groups.iter().rev().take(10).copied().collect::<Vec<_>>()
    })
    .await;
    let home = viewer.home().await;
    let counts: Vec<i64> = home["top_groups"].as_array().unwrap().iter().map(|c| c["member_count"].as_i64().unwrap()).collect();
    assert_eq!(counts, (6..=15).rev().collect::<Vec<i64>>());
    assert!(home["top_groups"].as_array().unwrap().iter().all(|c| c["kind"] == "group"));
    eventually(|| async { viewer.home().await["top_channels"][0]["id"] == channel }).await;

    // users[1] owns one channel and joined groups 1..=14.
    let mine = users[1].home().await["my_chats"].as_array().unwrap().clone();
    assert_eq!(mine.len(), 15);
    let owned: Vec<_> = mine.iter().filter(|c| c["is_owner"] == true).collect();
    assert_eq!(owned.len(), 1);
    assert_eq!(owned[0]["id"], channel);
    for c in &mine {
        for key in ["kind", "title", "member_count", "is_owner", "last_seq", "unread"] {
            assert!(c.get(key).is_some(), "missing {key} in {c}");
        }
    }
}

// ---------- messaging ----------

#[tokio::test(flavor = "multi_thread")]
async fn post_permissions_and_content() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let m = s.register("member").await;
    let x = s.register("outsider").await;
    let group = o.create_chat("group", "g").await;
    let channel = o.create_chat("channel", "c").await;
    m.join(group).await;
    m.join(channel).await;

    m.send(group, "member can post").await;
    let (st, _) = x.post(&format!("/chats/{group}/messages"), json!({ "body": "hi" })).await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    let (st, _) = m.post(&format!("/chats/{channel}/messages"), json!({ "body": "hi" })).await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    o.send(channel, "owner can post").await;

    for body in ["   ".to_string(), "x".repeat(4001)] {
        let (st, _) = m.post(&format!("/chats/{group}/messages"), json!({ "body": body })).await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
    }
    let v = m.send(group, "<b>hi</b>").await;
    assert_eq!(v["body"], "<b>hi</b>");
    assert_eq!(v["author"], json!({ "id": m.id, "login": "member" }));
    let (_, page) = x.get(&format!("/chats/{group}/messages?limit=1")).await;
    assert_eq!(page["messages"][0]["body"], "<b>hi</b>");
    let (_, page) = x.get(&format!("/chats/{group}/messages")).await;
    assert_eq!(page["messages"].as_array().unwrap().len(), 2, "rejected posts stored nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn sequences_history_and_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut s = Server::start_in(tmp.path()).await;
    let o = s.register("owner").await;
    let chat = o.create_chat("group", "g").await;
    for i in 1..=120 {
        assert_eq!(o.send(chat, &format!("m{i}")).await["seq"], i);
    }
    let seqs = |v: &serde_json::Value| v["messages"].as_array().unwrap().iter().map(|m| m["seq"].as_i64().unwrap()).collect::<Vec<_>>();
    let (_, p) = o.get(&format!("/chats/{chat}/messages")).await;
    assert_eq!(seqs(&p), (71..=120).rev().collect::<Vec<_>>());
    assert_eq!(p["has_more"], true);
    let (_, p) = o.get(&format!("/chats/{chat}/messages?before=71")).await;
    assert_eq!(seqs(&p), (21..=70).rev().collect::<Vec<_>>());
    let (_, p) = o.get(&format!("/chats/{chat}/messages?before=5")).await;
    assert_eq!(seqs(&p), vec![4, 3, 2, 1]);
    assert_eq!(p["has_more"], false);
    let (_, p) = o.get(&format!("/chats/{chat}/messages?after=116")).await;
    assert_eq!(seqs(&p), vec![117, 118, 119, 120]);
    let (_, p) = o.get(&format!("/chats/{chat}/messages?limit=500")).await;
    assert_eq!(seqs(&p).len(), 100, "limit is capped at 100");
    s.stop().await;

    let s = Server::start_in(tmp.path()).await;
    let anon = Client::new();
    let r = anon.get(s.url(&format!("/api/chats/{chat}/messages?limit=1"))).header("cookie", &o.cookie).send().await.unwrap();
    let (_, p) = json_of(r).await;
    assert_eq!(p["messages"][0]["body"], "m120", "messages survive restart");
    let r = anon
        .post(s.url(&format!("/api/chats/{chat}/messages")))
        .header("cookie", &o.cookie)
        .json(&json!({ "body": "after restart" }))
        .send()
        .await
        .unwrap();
    assert_eq!(json_of(r).await.1["seq"], 121);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_positions_and_unread() {
    let s = Server::start().await;
    let o = s.register("owner").await;
    let m = s.register("member").await;
    let x = s.register("outsider").await;
    let chat = o.create_chat("group", "g").await;
    m.join(chat).await;
    for i in 0..3 {
        o.send(chat, &format!("m{i}")).await;
    }
    assert_eq!(m.my_chat(chat).await.unwrap()["unread"], 3, "others' messages are unread");
    assert_eq!(o.my_chat(chat).await.unwrap()["unread"], 0, "own messages are not unread");

    let (st, _) = m.post(&format!("/chats/{chat}/read"), json!({ "seq": 2 })).await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    assert_eq!(m.my_chat(chat).await.unwrap()["unread"], 1);
    m.post(&format!("/chats/{chat}/read"), json!({ "seq": 1 })).await;
    assert_eq!(m.my_chat(chat).await.unwrap()["last_read_seq"], 2, "never moves backwards");
    m.post(&format!("/chats/{chat}/read"), json!({ "seq": 999 })).await;
    assert_eq!(m.my_chat(chat).await.unwrap()["last_read_seq"], 3, "clamped to last_seq");

    let (st, _) = x.post(&format!("/chats/{chat}/read"), json!({ "seq": 3 })).await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    assert!(x.my_chat(chat).await.is_none());

    // Posting advances your own read position past others' messages too.
    o.send(chat, "unread for m").await;
    m.send(chat, "reply").await;
    assert_eq!(m.my_chat(chat).await.unwrap()["unread"], 0);
    assert_eq!(o.my_chat(chat).await.unwrap()["unread"], 1);
}

// ---------- static files and API conventions ----------

#[tokio::test(flavor = "multi_thread")]
async fn serves_app_and_api_headers() {
    let s = Server::start().await;
    let anon = Client::new();
    let r = anon.get(s.url("/")).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert!(r.text().await.unwrap().contains("<script type=\"module\" src=\"/app.js\">"));
    assert_eq!(anon.get(s.url("/app.js")).send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(anon.get(s.url("/app.css")).send().await.unwrap().status(), StatusCode::OK);

    let r = anon.get(s.url("/api/home")).send().await.unwrap();
    assert_eq!(r.headers().get("cache-control").unwrap(), "no-store");
    let (st, v) = json_of(anon.get(s.url("/api/nope")).send().await.unwrap()).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert!(v["error"].is_string());
}

//! Load check: a full 1,000-member group, every member connected over
//! WebSocket with the chat open, and 50 concurrent posters.
//!
//! Needs more file descriptors than the macOS default:
//!   ulimit -n 8192 && cargo test --release --test load -- --ignored --nocapture
//! Tunables: LOAD_SECS (30), LOAD_MEMBERS (1000), LOAD_POSTERS (50).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

use chats_openspec::{Config, start};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message as WsMessage;

fn env_num(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[derive(Default)]
struct RecvStats {
    frames: i64,
    gaps: i64,
    resyncs: i64,
    last: i64,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "load check; run explicitly with --ignored"]
async fn group_fan_out_under_load() {
    let secs = env_num("LOAD_SECS", 30) as u64;
    let members = env_num("LOAD_MEMBERS", 1000);
    let posters = env_num("LOAD_POSTERS", 50).min(members);

    let tmp = tempfile::tempdir().unwrap();
    let running = start(Config {
        addr: "127.0.0.1:0".parse().unwrap(),
        data_dir: tmp.path().to_path_buf(),
        static_dir: common::static_dir(),
        shards: None,
        metrics: false,
    })
    .await
    .unwrap();
    let base = format!("http://{}", running.addr);
    // Set up members (the owner counts as one).
    let t = Instant::now();
    let owner = common::register(&base, "owner").await;
    let chat = owner.create_chat("group", "load").await;
    let mut users = vec![owner];
    let regs: Vec<_> = (1..members)
        .map(|i| {
            let base = base.clone();
            tokio::spawn(async move {
                let u = common::register(&base, &format!("member{i}")).await;
                u.join(chat).await;
                u
            })
        })
        .collect();
    for r in regs {
        users.push(r.await.unwrap());
    }
    let (_, info) = users[0].get(&format!("/chats/{chat}")).await;
    assert_eq!(info["member_count"].as_i64().unwrap(), members as i64);
    println!("setup: {members} members registered and joined in {:.1}s", t.elapsed().as_secs_f64());

    // Every member opens the chat over WebSocket.
    let stats: Vec<Arc<Mutex<RecvStats>>> = (0..members).map(|_| Arc::default()).collect();
    let mut readers = Vec::new();
    for (u, st) in users.iter().zip(&stats) {
        let mut ws = u.ws().await;
        ws.open(chat).await;
        let st = st.clone();
        readers.push(tokio::spawn(async move {
            while let Some(Ok(msg)) = ws.stream.next().await {
                let WsMessage::Text(text) = msg else { continue };
                let v: Value = serde_json::from_str(&text).unwrap();
                let mut s = st.lock().await;
                match v["t"].as_str() {
                    Some("msg") => {
                        let seq = v["seq"].as_i64().unwrap();
                        if seq != s.last + 1 {
                            s.gaps += 1;
                        }
                        s.last = s.last.max(seq);
                        s.frames += 1;
                    }
                    Some("resync") => s.resyncs += 1,
                    _ => {}
                }
            }
        }));
    }

    // Posters hammer the chat for `secs` seconds.
    let stop = Arc::new(AtomicBool::new(false));
    let posted = Arc::new(AtomicI64::new(0));
    let body = "load test message with a typical length of about eighty characters, give or take";
    let t = Instant::now();
    let mut tasks = Vec::new();
    for u in users.iter().take(posters) {
        let (client, url, stop, posted) =
            (u.client.clone(), format!("{base}/api/chats/{chat}/messages"), stop.clone(), posted.clone());
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                let r = client.post(&url).json(&json!({ "body": body })).send().await.unwrap();
                assert_eq!(r.status().as_u16(), 201);
                latencies.push(t.elapsed());
                posted.fetch_add(1, Ordering::Relaxed);
            }
            latencies
        }));
    }
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    let mut latencies: Vec<Duration> = Vec::new();
    for t in tasks {
        latencies.extend(t.await.unwrap());
    }
    let elapsed = t.elapsed().as_secs_f64();
    let total = posted.load(Ordering::Relaxed);

    // Let receivers drain.
    let drain = Instant::now();
    loop {
        let mut behind = 0;
        for s in &stats {
            let s = s.lock().await;
            if s.last < total && s.resyncs == 0 {
                behind += 1;
            }
        }
        if behind == 0 || drain.elapsed() > Duration::from_secs(20) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    latencies.sort();
    let pct = |p: f64| latencies[((latencies.len() as f64 * p) as usize).min(latencies.len() - 1)];
    let mut frames = 0;
    let mut complete = 0;
    let mut resynced = 0;
    let mut gappy_without_resync = 0;
    for s in &stats {
        let s = s.lock().await;
        frames += s.frames;
        if s.last == total && s.gaps == 0 {
            complete += 1;
        }
        if s.resyncs > 0 {
            resynced += 1;
        } else if s.gaps > 0 {
            gappy_without_resync += 1;
        }
    }

    println!("posted:     {total} messages in {elapsed:.1}s = {:.0} msg/s", total as f64 / elapsed);
    println!("fan-out:    {frames} frames delivered = {:.0} frames/s", frames as f64 / elapsed);
    println!(
        "latency:    p50 {:.1} ms, p99 {:.1} ms, max {:.1} ms",
        pct(0.50).as_secs_f64() * 1e3,
        pct(0.99).as_secs_f64() * 1e3,
        latencies.last().unwrap().as_secs_f64() * 1e3
    );
    println!("receivers:  {complete}/{members} got every message in order, {resynced} were told to resync");

    // Sequence numbers in storage must be exactly 1..=total.
    let mut expected = 1;
    let mut after = 0;
    loop {
        let (_, page) = users[0].get(&format!("/chats/{chat}/messages?after={after}&limit=100")).await;
        for m in page["messages"].as_array().unwrap() {
            assert_eq!(m["seq"].as_i64().unwrap(), expected, "gap in stored seq");
            expected += 1;
        }
        after = expected - 1;
        if page["has_more"] == false {
            break;
        }
    }
    assert_eq!(expected - 1, total, "every accepted post is stored");
    println!("storage:    seq 1..={total} contiguous, no gaps");
    assert_eq!(gappy_without_resync, 0, "a receiver saw a gap without being told to resync");

    for r in readers {
        r.abort();
    }
    running.stop().await;
}

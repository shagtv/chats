//! In-memory runtime state: chats, live fan-out, online index (design D5, D8).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::extract::ws::Utf8Bytes;
use dashmap::DashMap;
use serde::Serialize;
use serde_json::json;
use tokio::sync::{broadcast, mpsc};

use crate::db::shard::MessageSink;
use crate::db::{self, Pool, meta};
use crate::error::{ApiError, ApiResult};
use crate::model::{Kind, Message};

pub const BROADCAST_CAPACITY: usize = 256;
pub const CONTROL_CAPACITY: usize = 64;
pub const TOP_LIMIT: i64 = 10;

pub struct ChatState {
    pub id: i64,
    pub kind: Kind,
    pub title: String,
    pub owner_id: i64,
    pub owner_login: String,
    pub member_count: AtomicI64,
    pub last_seq: AtomicI64,
    tx: OnceLock<broadcast::Sender<Utf8Bytes>>,
}

impl ChatState {
    pub fn new(row: meta::ChatRow, last_seq: i64) -> Self {
        Self {
            id: row.id,
            kind: row.kind,
            title: row.title,
            owner_id: row.owner_id,
            owner_login: row.owner_login,
            member_count: AtomicI64::new(row.member_count),
            last_seq: AtomicI64::new(last_seq),
            tx: OnceLock::new(),
        }
    }

    pub fn last_seq(&self) -> i64 {
        self.last_seq.load(Ordering::Acquire)
    }

    pub fn member_count(&self) -> i64 {
        self.member_count.load(Ordering::Acquire)
    }

    pub fn brief(&self) -> ChatBrief {
        ChatBrief {
            id: self.id,
            kind: self.kind,
            title: self.title.clone(),
            member_count: self.member_count(),
            last_seq: self.last_seq(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatBrief {
    pub id: i64,
    pub kind: Kind,
    pub title: String,
    pub member_count: i64,
    pub last_seq: i64,
}

#[derive(Debug, Default, Serialize)]
pub struct Top {
    pub groups: Vec<ChatBrief>,
    pub channels: Vec<ChatBrief>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct OnlineChat {
    pub id: i64,
    pub online: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct PerKind<T> {
    pub group: T,
    pub channel: T,
}

impl<T> PerKind<T> {
    fn get_mut(&mut self, kind: Kind) -> &mut T {
        match kind {
            Kind::Group => &mut self.group,
            Kind::Channel => &mut self.channel,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ChatStats {
    pub groups: usize,
    pub channels: usize,
    pub max_members: PerKind<i64>,
    pub max_online: PerKind<Option<OnlineChat>>,
}

struct OnlineUser {
    conns: Vec<(u64, mpsc::Sender<Utf8Bytes>)>,
    chats: HashSet<i64>,
}

pub struct Hub {
    meta_pool: Pool,
    chats: DashMap<i64, Arc<ChatState>>,
    logins: DashMap<i64, String>,
    top: ArcSwap<Top>,
    // Lock order: `online` before `chat_online`; never hold a `chat_online`
    // guard while touching `online`.
    online: DashMap<i64, OnlineUser>,
    chat_online: DashMap<i64, HashSet<i64>>,
    dirty: Mutex<HashSet<i64>>,
    next_conn: AtomicU64,
    open_subscriptions: AtomicI64,
    resyncs_total: AtomicU64,
    dropped_slow_total: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct WsStats {
    pub connections: usize,
    pub online_users: usize,
    pub open_subscriptions: i64,
    pub resyncs_total: u64,
    pub dropped_slow_total: u64,
}

impl Hub {
    pub fn new(meta_pool: Pool) -> Arc<Self> {
        Arc::new(Self {
            meta_pool,
            chats: DashMap::new(),
            logins: DashMap::new(),
            top: ArcSwap::from_pointee(Top::default()),
            online: DashMap::new(),
            chat_online: DashMap::new(),
            dirty: Mutex::new(HashSet::new()),
            next_conn: AtomicU64::new(1),
            open_subscriptions: AtomicI64::new(0),
            resyncs_total: AtomicU64::new(0),
            dropped_slow_total: AtomicU64::new(0),
        })
    }

    // ---- chats ----

    pub fn insert_chat(&self, state: ChatState) {
        self.chats.insert(state.id, Arc::new(state));
    }

    pub fn get(&self, chat_id: i64) -> Option<Arc<ChatState>> {
        self.chats.get(&chat_id).map(|c| c.clone())
    }

    pub fn set_last_seq(&self, chat_id: i64, seq: i64) {
        if let Some(c) = self.chats.get(&chat_id) {
            c.last_seq.fetch_max(seq, Ordering::AcqRel);
        }
    }

    pub fn chat_count(&self) -> usize {
        self.chats.len()
    }

    /// Subscribes to live frames of a chat; `None` if the chat doesn't exist.
    pub fn subscribe(&self, chat_id: i64) -> Option<broadcast::Receiver<Utf8Bytes>> {
        let chat = self.chats.get(&chat_id)?;
        Some(chat.tx.get_or_init(|| broadcast::channel(BROADCAST_CAPACITY).0).subscribe())
    }

    /// Forgets a deleted chat and tells its viewers and online members.
    pub fn remove_chat(&self, chat_id: i64) {
        let Some((_, state)) = self.chats.remove(&chat_id) else { return };
        let frame = Utf8Bytes::from(json!({ "t": "deleted", "chat": chat_id }).to_string());
        if let Some(tx) = state.tx.get() {
            let _ = tx.send(frame.clone());
        }
        drop(state); // drops the sender, so viewers see the channel close
        self.dirty.lock().unwrap().remove(&chat_id);
        let users = self.chat_online.remove(&chat_id).map(|(_, u)| u).unwrap_or_default();
        for user in users {
            if let Some(mut o) = self.online.get_mut(&user) {
                o.chats.remove(&chat_id);
            }
            self.send_control(user, frame.clone());
        }
    }

    /// Counts and largest chats by kind; one pass over chats and one over
    /// the online index.
    pub fn chat_stats(&self) -> ChatStats {
        let mut stats = ChatStats::default();
        for chat in self.chats.iter() {
            match chat.kind {
                Kind::Group => stats.groups += 1,
                Kind::Channel => stats.channels += 1,
            }
            let max = stats.max_members.get_mut(chat.kind);
            *max = (*max).max(chat.member_count());
        }
        // Collected first so no `chat_online` guard is held while reading `chats`.
        let sizes: Vec<(i64, usize)> = self.chat_online.iter().map(|e| (*e.key(), e.value().len())).collect();
        for (chat_id, online) in sizes {
            let Some(kind) = self.chats.get(&chat_id).map(|c| c.kind) else { continue };
            let best = stats.max_online.get_mut(kind);
            if online > 0 && best.is_none_or(|b| online > b.online) {
                *best = Some(OnlineChat { id: chat_id, online });
            }
        }
        stats
    }

    // ---- logins ----

    pub fn set_login(&self, user_id: i64, login: String) {
        self.logins.insert(user_id, login);
    }

    pub async fn login(&self, user_id: i64) -> ApiResult<String> {
        self.logins_for(&[user_id])
            .await?
            .remove(&user_id)
            .ok_or_else(ApiError::unauthorized)
    }

    pub async fn logins_for(&self, ids: &[i64]) -> ApiResult<HashMap<i64, String>> {
        let mut out = HashMap::with_capacity(ids.len());
        let mut missing = Vec::new();
        for id in ids {
            match self.logins.get(id) {
                Some(l) => {
                    out.insert(*id, l.clone());
                }
                None if !missing.contains(id) => missing.push(*id),
                None => {}
            }
        }
        if !missing.is_empty() {
            let rows = db::read(&self.meta_pool, move |c| meta::logins(c, &missing)).await?;
            for (id, login) in rows {
                self.logins.insert(id, login.clone());
                out.insert(id, login);
            }
        }
        Ok(out)
    }

    // ---- top lists ----

    pub fn top(&self) -> Arc<Top> {
        self.top.load_full()
    }

    pub async fn refresh_top(&self) -> ApiResult<()> {
        let (groups, channels) = db::read(&self.meta_pool, |c| {
            Ok((meta::top_chat_ids(c, Kind::Group, TOP_LIMIT)?, meta::top_chat_ids(c, Kind::Channel, TOP_LIMIT)?))
        })
        .await?;
        let briefs = |ids: Vec<i64>| {
            let mut v: Vec<ChatBrief> = ids.into_iter().filter_map(|id| self.get(id)).map(|c| c.brief()).collect();
            v.sort_by(|a, b| b.member_count.cmp(&a.member_count).then(a.id.cmp(&b.id)));
            v
        };
        self.top.store(Arc::new(Top { groups: briefs(groups), channels: briefs(channels) }));
        Ok(())
    }

    // ---- online connections ----

    /// Registers a connection; call `add_user_chats` afterwards with the
    /// user's chats so a join that races with connecting isn't missed.
    pub fn connect(&self, user_id: i64, control: mpsc::Sender<Utf8Bytes>) -> u64 {
        let conn_id = self.next_conn.fetch_add(1, Ordering::Relaxed);
        self.online
            .entry(user_id)
            .or_insert_with(|| OnlineUser { conns: Vec::new(), chats: HashSet::new() })
            .conns
            .push((conn_id, control));
        conn_id
    }

    pub fn add_user_chats(&self, user_id: i64, chats: &[i64]) {
        let Some(mut o) = self.online.get_mut(&user_id) else { return };
        for &chat in chats {
            if self.chats.contains_key(&chat) && o.chats.insert(chat) {
                self.chat_online.entry(chat).or_default().insert(user_id);
            }
        }
    }

    pub fn disconnect(&self, user_id: i64, conn_id: u64) {
        self.drop_conns(user_id, &[conn_id]);
    }

    pub fn is_connected(&self, user_id: i64, conn_id: u64) -> bool {
        self.online.get(&user_id).is_some_and(|o| o.conns.iter().any(|(id, _)| *id == conn_id))
    }

    fn drop_conns(&self, user_id: i64, conn_ids: &[u64]) {
        let gone = {
            let Some(mut o) = self.online.get_mut(&user_id) else { return };
            o.conns.retain(|(id, _)| !conn_ids.contains(id));
            o.conns.is_empty()
        };
        if gone && let Some((_, o)) = self.online.remove_if(&user_id, |_, o| o.conns.is_empty()) {
            for chat in o.chats {
                if let Some(mut users) = self.chat_online.get_mut(&chat) {
                    users.remove(&user_id);
                }
                self.chat_online.remove_if(&chat, |_, users| users.is_empty());
            }
        }
    }

    pub fn on_join(&self, user_id: i64, chat_id: i64) {
        self.add_user_chats(user_id, &[chat_id]);
    }

    pub fn on_leave(&self, user_id: i64, chat_id: i64) {
        let Some(mut o) = self.online.get_mut(&user_id) else { return };
        if o.chats.remove(&chat_id) {
            if let Some(mut users) = self.chat_online.get_mut(&chat_id) {
                users.remove(&user_id);
            }
            self.chat_online.remove_if(&chat_id, |_, users| users.is_empty());
        }
    }

    /// Sends a control frame to every connection of a user. A connection whose
    /// queue is full is dropped; its client reconnects and refreshes.
    pub fn send_control(&self, user_id: i64, frame: Utf8Bytes) {
        let stale: Vec<u64> = {
            let Some(o) = self.online.get(&user_id) else { return };
            o.conns
                .iter()
                .filter(|(_, tx)| tx.try_send(frame.clone()).is_err())
                .map(|(id, _)| *id)
                .collect()
        };
        if !stale.is_empty() {
            self.dropped_slow_total.fetch_add(stale.len() as u64, Ordering::Relaxed);
            tracing::debug!("dropping {} slow connection(s) of user {user_id}", stale.len());
            self.drop_conns(user_id, &stale);
        }
    }

    /// Tracks connections that have a chat open (`+1` on open, `-1` on close).
    pub fn note_subscription(&self, delta: i64) {
        self.open_subscriptions.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn note_resync(&self) {
        self.resyncs_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn ws_stats(&self) -> WsStats {
        WsStats {
            connections: self.online.iter().map(|o| o.conns.len()).sum(),
            online_users: self.online.len(),
            open_subscriptions: self.open_subscriptions.load(Ordering::Relaxed),
            resyncs_total: self.resyncs_total.load(Ordering::Relaxed),
            dropped_slow_total: self.dropped_slow_total.load(Ordering::Relaxed),
        }
    }

    /// Sends one `seq` frame per online user with the chats that changed.
    pub fn flush_seq(&self) {
        let dirty = std::mem::take(&mut *self.dirty.lock().unwrap());
        if dirty.is_empty() {
            return;
        }
        let mut per_user: HashMap<i64, serde_json::Map<String, serde_json::Value>> = HashMap::new();
        for chat_id in dirty {
            let Some(seq) = self.chats.get(&chat_id).map(|c| c.last_seq()) else { continue };
            let Some(users) = self.chat_online.get(&chat_id) else { continue };
            for user in users.iter() {
                per_user.entry(*user).or_default().insert(chat_id.to_string(), seq.into());
            }
        }
        for (user, chats) in per_user {
            let frame = Utf8Bytes::from(json!({ "t": "seq", "chats": chats }).to_string());
            self.send_control(user, frame);
        }
    }

    pub fn spawn_tasks(self: &Arc<Self>) -> Vec<tokio::task::JoinHandle<()>> {
        let seq_hub = self.clone();
        let seq = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                seq_hub.flush_seq();
            }
        });
        let top_hub = self.clone();
        let top = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            loop {
                tick.tick().await;
                if let Err(e) = top_hub.refresh_top().await {
                    tracing::warn!("refresh top lists: {}", e.message);
                }
            }
        });
        vec![seq, top]
    }
}

impl MessageSink for Hub {
    fn on_message(&self, msg: &Message, frame: Utf8Bytes) {
        if let Some(chat) = self.chats.get(&msg.chat_id) {
            chat.last_seq.fetch_max(msg.seq, Ordering::AcqRel);
            if let Some(tx) = chat.tx.get() {
                let _ = tx.send(frame);
            }
        } else {
            return;
        }
        self.dirty.lock().unwrap().insert(msg.chat_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::meta::ChatRow;
    use crate::model::Author;

    fn hub_with_db() -> (tempfile::TempDir, Arc<Hub>, rusqlite::Connection) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let conn = meta::open(&path).unwrap();
        let hub = Hub::new(db::read_pool(&path).unwrap());
        (dir, hub, conn)
    }

    fn row(id: i64, kind: Kind, members: i64) -> ChatRow {
        ChatRow { id, kind, title: format!("c{id}"), owner_id: 1, owner_login: "o".into(), member_count: members }
    }

    fn msg(chat_id: i64, seq: i64) -> Message {
        Message { chat_id, seq, author: Author { id: 1, login: "o".into() }, body: "x".into(), ts: 0 }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn top_lists_are_ordered_and_split_by_kind() {
        let (_d, hub, conn) = hub_with_db();
        conn.execute("INSERT INTO users VALUES (1, 'o', 'o', 'h', 0)", []).unwrap();
        for id in 1..=15 {
            let members = id * 3 % 16 + 1; // distinct counts in a shuffled order
            conn.execute("INSERT INTO chats VALUES (?, 'group', ?, 1, ?, 0)", rusqlite::params![id, format!("c{id}"), members]).unwrap();
        }
        conn.execute("INSERT INTO chats VALUES (99, 'channel', 'big', 1, 5000, 0)", []).unwrap();
        for r in meta::load_chats(&conn).unwrap() {
            hub.insert_chat(ChatState::new(r, 0));
        }
        hub.refresh_top().await.unwrap();
        let top = hub.top();
        let counts: Vec<i64> = top.groups.iter().map(|b| b.member_count).collect();
        assert_eq!(counts.len(), 10);
        assert!(counts.windows(2).all(|w| w[0] > w[1]));
        assert_eq!(counts[0], 16);
        assert!(top.groups.iter().all(|b| b.kind == Kind::Group));
        assert_eq!(top.channels.len(), 1);
        assert_eq!(top.channels[0].id, 99);
    }

    #[tokio::test]
    async fn seq_updates_are_batched_per_connection() {
        let (_d, hub, _conn) = hub_with_db();
        hub.insert_chat(ChatState::new(row(5, Kind::Channel, 2), 0));
        hub.insert_chat(ChatState::new(row(6, Kind::Group, 2), 0));
        let (tx, mut rx) = mpsc::channel(CONTROL_CAPACITY);
        hub.connect(42, tx);
        hub.add_user_chats(42, &[5]);
        for seq in 1..=20 {
            hub.on_message(&msg(5, seq), Utf8Bytes::from_static("{}"));
        }
        hub.on_message(&msg(6, 1), Utf8Bytes::from_static("{}")); // not a member
        hub.flush_seq();
        let frame: serde_json::Value = serde_json::from_str(rx.try_recv().unwrap().as_str()).unwrap();
        assert_eq!(frame, json!({ "t": "seq", "chats": { "5": 20 } }));
        assert!(rx.try_recv().is_err(), "exactly one frame");

        // Joining while connected starts updates without reconnecting.
        hub.on_join(42, 6);
        hub.on_message(&msg(6, 2), Utf8Bytes::from_static("{}"));
        hub.flush_seq();
        let frame: serde_json::Value = serde_json::from_str(rx.try_recv().unwrap().as_str()).unwrap();
        assert_eq!(frame, json!({ "t": "seq", "chats": { "6": 2 } }));
    }

    #[tokio::test]
    async fn chat_stats_by_kind() {
        let (_d, hub, _conn) = hub_with_db();
        assert_eq!(hub.chat_stats(), ChatStats::default());
        hub.insert_chat(ChatState::new(row(1, Kind::Group, 3), 0));
        hub.insert_chat(ChatState::new(row(2, Kind::Group, 7), 0));
        hub.insert_chat(ChatState::new(row(9, Kind::Channel, 5), 0));
        hub.insert_chat(ChatState::new(row(10, Kind::Channel, 4), 0));
        let mut queues = Vec::new(); // keeps each connection's queue open
        for user in 1..=4 {
            let (tx, rx) = mpsc::channel(CONTROL_CAPACITY);
            queues.push(rx);
            hub.connect(user, tx);
            hub.add_user_chats(user, &[9]);
        }
        hub.add_user_chats(1, &[10, 2]);
        let s = hub.chat_stats();
        assert_eq!((s.groups, s.channels), (2, 2));
        assert_eq!(s.max_members, PerKind { group: 7, channel: 5 });
        assert_eq!(s.max_online.channel, Some(OnlineChat { id: 9, online: 4 }));
        assert_eq!(s.max_online.group, Some(OnlineChat { id: 2, online: 1 }));

        hub.remove_chat(1);
        assert_eq!(hub.chat_stats().groups, 1);
    }

    /// Run with: cargo test --release --lib metrics_scale -- --ignored
    #[tokio::test]
    #[ignore = "timing check; meaningful only in release builds"]
    async fn metrics_scale_with_100k_chats() {
        let (_d, hub, _conn) = hub_with_db();
        for id in 1..=100_000 {
            let kind = if id % 5 == 0 { Kind::Channel } else { Kind::Group };
            hub.insert_chat(ChatState::new(row(id, kind, id % 1000), 0));
        }
        let mut queues = Vec::new();
        for user in 1..=10_000 {
            let (tx, rx) = mpsc::channel(CONTROL_CAPACITY);
            queues.push(rx);
            hub.connect(user, tx);
            hub.add_user_chats(user, &[user % 20_000 + 1, user % 7 + 1]);
        }
        let start = std::time::Instant::now();
        let body = serde_json::to_string(&json!({ "ws": hub.ws_stats(), "chats": hub.chat_stats() })).unwrap();
        let took = start.elapsed();
        println!("metrics over 100k chats: {took:?} ({} bytes)", body.len());
        assert!(took < Duration::from_millis(50), "took {took:?}");
    }

    #[tokio::test]
    async fn live_frames_reach_subscribers() {
        let (_d, hub, _conn) = hub_with_db();
        hub.insert_chat(ChatState::new(row(5, Kind::Group, 1), 0));
        let mut rx = hub.subscribe(5).unwrap();
        hub.on_message(&msg(5, 1), Utf8Bytes::from_static("frame-1"));
        assert_eq!(rx.recv().await.unwrap().as_str(), "frame-1");
        assert_eq!(hub.get(5).unwrap().last_seq(), 1);
        assert!(hub.subscribe(404).is_none());

        hub.remove_chat(5);
        assert!(rx.recv().await.unwrap().as_str().contains("deleted"));
        assert!(matches!(rx.recv().await, Err(broadcast::error::RecvError::Closed)));
    }

    #[tokio::test]
    async fn full_control_queue_drops_only_that_connection() {
        let (_d, hub, _conn) = hub_with_db();
        let (slow_tx, _slow_rx) = mpsc::channel(CONTROL_CAPACITY);
        let (fast_tx, mut fast_rx) = mpsc::channel(CONTROL_CAPACITY);
        let slow = hub.connect(1, slow_tx);
        let fast = hub.connect(1, fast_tx);
        for i in 0..=CONTROL_CAPACITY {
            hub.send_control(1, Utf8Bytes::from(i.to_string()));
            while fast_rx.try_recv().is_ok() {} // the fast client keeps up
        }
        assert!(!hub.is_connected(1, slow));
        assert!(hub.is_connected(1, fast));
        let stats = hub.ws_stats();
        assert_eq!((stats.connections, stats.online_users, stats.dropped_slow_total), (1, 1, 1));
        hub.send_control(1, Utf8Bytes::from_static("still here"));
        assert_eq!(fast_rx.try_recv().unwrap().as_str(), "still here");
    }
}

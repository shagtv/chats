//! Message shards `msg-NN.db` (design D2, D4, D9).

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;

use axum::extract::ws::Utf8Bytes;
use rusqlite::{Connection, OptionalExtension, params};

use super::now_ms;
use super::writer::{Applied, Handler, Reply, complete, noop};
use crate::error::ApiError;
use crate::model::{Author, Message, msg_frame};

pub const PURGE_CHUNK: i64 = 5_000;

pub const SCHEMA: &str = "
CREATE TABLE messages(
    chat_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    body TEXT NOT NULL,
    ts INTEGER NOT NULL,
    PRIMARY KEY(chat_id, seq)
) WITHOUT ROWID;
CREATE TABLE chat_seq(chat_id INTEGER PRIMARY KEY, last_seq INTEGER NOT NULL);
CREATE TABLE purge_queue(chat_id INTEGER PRIMARY KEY);
";

pub fn open(path: &Path) -> rusqlite::Result<Connection> {
    let conn = super::open_rw(path)?;
    super::migrate(&conn, SCHEMA)?;
    Ok(conn)
}

/// Receives every committed message, in commit order, on the writer thread.
pub trait MessageSink: Send + Sync + 'static {
    fn on_message(&self, msg: &Message, frame: Utf8Bytes);
}

pub enum ShardCmd {
    Append { chat_id: i64, author_id: i64, author_login: String, body: String, reply: Reply<Message> },
    Purge { chat_id: i64 },
}

struct Purge {
    chat_id: i64,
    next: i64,
    last: i64,
}

pub struct ShardHandler {
    seqs: HashMap<i64, i64>,
    /// Chats touched in the current batch, with their `last_seq` before it.
    touched: HashMap<i64, i64>,
    /// Chats deleted while this process runs; appends to them are rejected.
    deleted: HashSet<i64>,
    purges: VecDeque<Purge>,
    sink: Arc<dyn MessageSink>,
}

impl ShardHandler {
    /// Loads sequence counters and any unfinished purges.
    pub fn new(conn: &Connection, sink: Arc<dyn MessageSink>) -> rusqlite::Result<Self> {
        let mut seqs = HashMap::new();
        {
            let mut stmt = conn.prepare("SELECT chat_id, last_seq FROM chat_seq")?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
            for row in rows {
                let (id, seq) = row?;
                seqs.insert(id, seq);
            }
        }
        let mut purges = VecDeque::new();
        let mut deleted = HashSet::new();
        {
            let mut stmt = conn.prepare("SELECT chat_id FROM purge_queue")?;
            let ids: Vec<i64> = stmt.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?;
            for chat_id in ids {
                let last = match seqs.remove(&chat_id) {
                    Some(s) => s,
                    None => conn
                        .query_row("SELECT MAX(seq) FROM messages WHERE chat_id = ?", [chat_id], |r| r.get::<_, Option<i64>>(0))
                        .optional()?
                        .flatten()
                        .unwrap_or(0),
                };
                deleted.insert(chat_id);
                purges.push_back(Purge { chat_id, next: 0, last });
            }
        }
        Ok(Self { seqs, touched: HashMap::new(), deleted, purges, sink })
    }

    pub fn seqs(&self) -> &HashMap<i64, i64> {
        &self.seqs
    }
}

impl Handler for ShardHandler {
    type Cmd = ShardCmd;

    fn apply(&mut self, conn: &Connection, cmd: ShardCmd) -> Applied {
        match cmd {
            ShardCmd::Append { chat_id, author_id, author_login, body, reply } => {
                if self.deleted.contains(&chat_id) {
                    return complete(reply, Err(ApiError::not_found("chat not found")));
                }
                let prev = self.seqs.get(&chat_id).copied().unwrap_or(0);
                let seq = prev + 1;
                let ts = now_ms();
                let r = conn
                    .prepare_cached("INSERT INTO messages(chat_id, seq, author_id, body, ts) VALUES (?, ?, ?, ?, ?)")
                    .and_then(|mut s| s.execute(params![chat_id, seq, author_id, body, ts]));
                if let Err(e) = r {
                    return complete(reply, Err(e.into()));
                }
                self.touched.entry(chat_id).or_insert(prev);
                self.seqs.insert(chat_id, seq);
                let msg = Message { chat_id, seq, author: Author { id: author_id, login: author_login }, body, ts };
                let sink = self.sink.clone();
                Ok(Box::new(move |committed| {
                    if committed {
                        let frame = Utf8Bytes::from(msg_frame(&msg));
                        sink.on_message(&msg, frame);
                        let _ = reply.send(Ok(msg));
                    } else {
                        let _ = reply.send(Err(ApiError::internal()));
                    }
                }))
            }
            ShardCmd::Purge { chat_id } => {
                if let Err(e) = conn.execute("INSERT OR IGNORE INTO purge_queue(chat_id) VALUES (?)", [chat_id]) {
                    tracing::error!("queue purge of chat {chat_id}: {e}");
                    return Err(noop());
                }
                let last = self.seqs.remove(&chat_id).unwrap_or(0);
                self.touched.remove(&chat_id);
                self.deleted.insert(chat_id);
                self.purges.push_back(Purge { chat_id, next: 0, last });
                Ok(noop())
            }
        }
    }

    fn end_batch(&mut self, conn: &Connection) -> rusqlite::Result<()> {
        let mut stmt = conn.prepare_cached(
            "INSERT INTO chat_seq(chat_id, last_seq) VALUES (?, ?)
             ON CONFLICT(chat_id) DO UPDATE SET last_seq = excluded.last_seq",
        )?;
        for chat_id in self.touched.keys() {
            if let Some(seq) = self.seqs.get(chat_id) {
                stmt.execute([chat_id, seq])?;
            }
        }
        Ok(())
    }

    fn after_batch(&mut self, committed: bool) {
        if !committed {
            for (chat_id, prev) in self.touched.drain() {
                if prev == 0 {
                    self.seqs.remove(&chat_id);
                } else {
                    self.seqs.insert(chat_id, prev);
                }
            }
        }
        self.touched.clear();
    }

    fn has_background(&self) -> bool {
        !self.purges.is_empty()
    }

    fn background(&mut self, conn: &Connection) -> rusqlite::Result<()> {
        let Some(p) = self.purges.front_mut() else { return Ok(()) };
        if p.next >= p.last {
            conn.execute("DELETE FROM chat_seq WHERE chat_id = ?", [p.chat_id])?;
            conn.execute("DELETE FROM purge_queue WHERE chat_id = ?", [p.chat_id])?;
            self.purges.pop_front();
        } else {
            let hi = p.next + PURGE_CHUNK;
            conn.prepare_cached("DELETE FROM messages WHERE chat_id = ? AND seq > ? AND seq <= ?")?
                .execute([p.chat_id, p.next, hi])?;
            p.next = hi;
        }
        Ok(())
    }
}

pub struct StoredMessage {
    pub seq: i64,
    pub author_id: i64,
    pub body: String,
    pub ts: i64,
}

/// Newest-first page below `before`, or oldest-first page above `after`.
/// Returns the page and whether more messages exist past it.
pub fn history(
    conn: &Connection,
    chat_id: i64,
    before: Option<i64>,
    after: Option<i64>,
    limit: i64,
) -> rusqlite::Result<(Vec<StoredMessage>, bool)> {
    let (sql, bound) = match after {
        Some(a) => (
            "SELECT seq, author_id, body, ts FROM messages WHERE chat_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?",
            a,
        ),
        None => (
            "SELECT seq, author_id, body, ts FROM messages WHERE chat_id = ? AND seq < ? ORDER BY seq DESC LIMIT ?",
            before.unwrap_or(i64::MAX),
        ),
    };
    let mut stmt = conn.prepare_cached(sql)?;
    let rows = stmt.query_map(params![chat_id, bound, limit + 1], |r| {
        Ok(StoredMessage { seq: r.get(0)?, author_id: r.get(1)?, body: r.get(2)?, ts: r.get(3)? })
    })?;
    let mut page: Vec<StoredMessage> = rows.collect::<Result<_, _>>()?;
    let has_more = page.len() as i64 > limit;
    page.truncate(limit as usize);
    Ok((page, has_more))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::writer::Writer;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Collect(Mutex<Vec<i64>>);
    impl MessageSink for Collect {
        fn on_message(&self, msg: &Message, _frame: Utf8Bytes) {
            self.0.lock().unwrap().push(msg.seq);
        }
    }

    fn start(path: &Path, sink: Arc<Collect>) -> Writer<ShardCmd> {
        let conn = open(path).unwrap();
        let handler = ShardHandler::new(&conn, sink).unwrap();
        Writer::spawn("shard-test", conn, handler).unwrap()
    }

    async fn append(w: &Writer<ShardCmd>, chat_id: i64, body: &str) -> Message {
        w.request(|reply| ShardCmd::Append {
            chat_id,
            author_id: 1,
            author_login: "a".into(),
            body: body.into(),
            reply,
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sequences_are_consecutive_and_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("msg-00.db");
        let sink = Arc::new(Collect::default());
        let w = Arc::new(start(&path, sink.clone()));

        // Concurrent appends land in shared batches but still number 1..=50.
        let tasks: Vec<_> = (0..50)
            .map(|i| {
                let w = w.clone();
                tokio::spawn(async move { append(&w, 7, &format!("m{i}")).await.seq })
            })
            .collect();
        let mut seqs = Vec::new();
        for t in tasks {
            seqs.push(t.await.unwrap());
        }
        seqs.sort();
        assert_eq!(seqs, (1..=50).collect::<Vec<_>>());
        assert_eq!(*sink.0.lock().unwrap(), (1..=50).collect::<Vec<_>>(), "sink sees commit order");
        assert_eq!(append(&w, 8, "other chat").await.seq, 1);
        w.stop().await;

        let w = start(&path, Arc::new(Collect::default()));
        assert_eq!(append(&w, 7, "after restart").await.seq, 51);
        w.stop().await;
    }

    #[test]
    fn failed_batch_rolls_back_counters() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open(&dir.path().join("s.db")).unwrap();
        let mut h = ShardHandler::new(&conn, Arc::new(Collect::default())).unwrap();
        let (tx, _rx) = tokio::sync::oneshot::channel();
        conn.execute_batch("BEGIN").unwrap();
        let _ = h.apply(&conn, ShardCmd::Append { chat_id: 1, author_id: 1, author_login: "a".into(), body: "x".into(), reply: tx });
        assert_eq!(h.seqs().get(&1), Some(&1));
        conn.execute_batch("ROLLBACK").unwrap();
        h.after_batch(false);
        assert_eq!(h.seqs().get(&1), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn history_pages() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.db");
        let w = start(&path, Arc::new(Collect::default()));
        for i in 1..=120 {
            append(&w, 3, &format!("m{i}")).await;
        }
        w.stop().await;
        let c = open(&path).unwrap();
        let seqs = |v: &[StoredMessage]| v.iter().map(|m| m.seq).collect::<Vec<_>>();

        let (page, more) = history(&c, 3, None, None, 50).unwrap();
        assert_eq!(seqs(&page), (71..=120).rev().collect::<Vec<_>>());
        assert!(more);
        let (page, more) = history(&c, 3, Some(71), None, 50).unwrap();
        assert_eq!(seqs(&page), (21..=70).rev().collect::<Vec<_>>());
        assert!(more);
        let (page, more) = history(&c, 3, Some(5), None, 50).unwrap();
        assert_eq!(seqs(&page), vec![4, 3, 2, 1]);
        assert!(!more);
        let (page, more) = history(&c, 3, None, Some(116), 50).unwrap();
        assert_eq!(seqs(&page), vec![117, 118, 119, 120]);
        assert!(!more);
    }

    fn message_count(path: &Path, chat_id: i64) -> i64 {
        open(path).unwrap().query_row("SELECT COUNT(*) FROM messages WHERE chat_id = ?", [chat_id], |r| r.get(0)).unwrap()
    }

    fn seed(path: &Path, chat_id: i64, n: i64) {
        let c = open(path).unwrap();
        let tx = c.unchecked_transaction().unwrap();
        for seq in 1..=n {
            tx.execute("INSERT INTO messages VALUES (?, ?, 1, 'x', 0)", [chat_id, seq]).unwrap();
        }
        tx.execute("INSERT INTO chat_seq VALUES (?, ?)", [chat_id, n]).unwrap();
        tx.commit().unwrap();
    }

    async fn wait_until(mut f: impl FnMut() -> bool) {
        for _ in 0..200 {
            if f() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("condition not reached");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn purge_deletes_in_chunks_and_rejects_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.db");
        seed(&path, 9, 12_000);
        seed(&path, 10, 3);
        let w = start(&path, Arc::new(Collect::default()));
        w.send(ShardCmd::Purge { chat_id: 9 }).await.unwrap();
        let e = w
            .request(|reply| ShardCmd::Append { chat_id: 9, author_id: 1, author_login: "a".into(), body: "late".into(), reply })
            .await
            .unwrap_err();
        assert_eq!(e.status.as_u16(), 404);
        let p = path.clone();
        wait_until(move || message_count(&p, 9) == 0).await;
        assert_eq!(message_count(&path, 10), 3);
        // The queue entry goes away once the purge finishes.
        let p = path.clone();
        wait_until(move || open(&p).unwrap().query_row("SELECT COUNT(*) FROM purge_queue", [], |r| r.get::<_, i64>(0)).unwrap() == 0).await;
        w.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn purge_resumes_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.db");
        seed(&path, 9, 7_000);
        {
            // Simulate a crash after the purge was queued and one chunk was deleted.
            let c = open(&path).unwrap();
            c.execute("INSERT INTO purge_queue VALUES (9)", []).unwrap();
            c.execute("DELETE FROM messages WHERE chat_id = 9 AND seq <= 5000", []).unwrap();
        }
        let w = start(&path, Arc::new(Collect::default()));
        let p = path.clone();
        wait_until(move || message_count(&p, 9) == 0).await;
        w.stop().await;
    }
}

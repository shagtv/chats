//! `meta.db`: users, sessions, chats and memberships (design D2, D7).

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params, params_from_iter};

use super::writer::{Applied, Handler, Reply, complete, noop};
use super::{is_constraint, now_ms};
use crate::error::{ApiError, ApiResult};
use crate::model::Kind;

pub const DEFAULT_SHARDS: u32 = 32;

pub const SCHEMA: &str = "
CREATE TABLE config(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE users(
    id INTEGER PRIMARY KEY,
    login TEXT NOT NULL,
    login_lc TEXT NOT NULL UNIQUE,
    pw_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE sessions(
    token_hash BLOB PRIMARY KEY,
    user_id INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX sessions_user ON sessions(user_id);
CREATE TABLE chats(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    owner_id INTEGER NOT NULL,
    member_count INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX chats_top ON chats(kind, member_count DESC);
CREATE TABLE members(
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    joined_at INTEGER NOT NULL,
    last_read_seq INTEGER NOT NULL,
    PRIMARY KEY(chat_id, user_id)
) WITHOUT ROWID;
CREATE INDEX members_user ON members(user_id, chat_id);
";

pub fn open(path: &Path) -> rusqlite::Result<Connection> {
    let conn = super::open_rw(path)?;
    super::migrate(&conn, SCHEMA)?;
    Ok(conn)
}

/// Returns the shard count stored at first start, storing `requested` (or the
/// default) if there is none yet. A later different `requested` is ignored.
pub fn init_shard_count(conn: &Connection, requested: Option<u32>) -> rusqlite::Result<u32> {
    let stored: Option<String> = conn
        .query_row("SELECT value FROM config WHERE key = 'shard_count'", [], |r| r.get(0))
        .optional()?;
    match stored {
        Some(v) => {
            let n: u32 = v.parse().map_err(|_| rusqlite::Error::InvalidColumnType(0, v.clone(), rusqlite::types::Type::Text))?;
            if let Some(r) = requested
                && r != n
            {
                tracing::warn!("--shards {r} ignored: this database was created with {n} shards");
            }
            Ok(n)
        }
        None => {
            let n = requested.unwrap_or(DEFAULT_SHARDS);
            conn.execute("INSERT INTO config(key, value) VALUES ('shard_count', ?)", [n.to_string()])?;
            Ok(n)
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChatRow {
    pub id: i64,
    pub kind: Kind,
    pub title: String,
    pub owner_id: i64,
    pub owner_login: String,
    pub member_count: i64,
}

pub fn load_chats(conn: &Connection) -> rusqlite::Result<Vec<ChatRow>> {
    let mut stmt = conn.prepare(
        "SELECT c.id, c.kind, c.title, c.owner_id, u.login, c.member_count
         FROM chats c JOIN users u ON u.id = c.owner_id",
    )?;
    let rows = stmt.query_map([], |r| {
        let kind: String = r.get(1)?;
        Ok(ChatRow {
            id: r.get(0)?,
            kind: Kind::parse(&kind).unwrap_or(Kind::Group),
            title: r.get(2)?,
            owner_id: r.get(3)?,
            owner_login: r.get(4)?,
            member_count: r.get(5)?,
        })
    })?;
    rows.collect()
}

// ---- read queries (run on the read pool) ----

pub struct UserRow {
    pub id: i64,
    pub login: String,
    pub pw_hash: String,
}

pub fn user_by_login(conn: &Connection, login_lc: &str) -> rusqlite::Result<Option<UserRow>> {
    conn.prepare_cached("SELECT id, login, pw_hash FROM users WHERE login_lc = ?")?
        .query_row([login_lc], |r| Ok(UserRow { id: r.get(0)?, login: r.get(1)?, pw_hash: r.get(2)? }))
        .optional()
}

pub fn logins(conn: &Connection, ids: &[i64]) -> rusqlite::Result<Vec<(i64, String)>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let marks = vec!["?"; ids.len()].join(",");
    let mut stmt = conn.prepare(&format!("SELECT id, login FROM users WHERE id IN ({marks})"))?;
    let rows = stmt.query_map(params_from_iter(ids), |r| Ok((r.get(0)?, r.get(1)?)))?;
    rows.collect()
}

pub fn session(conn: &Connection, token_hash: &[u8]) -> rusqlite::Result<Option<(i64, i64)>> {
    conn.prepare_cached("SELECT user_id, expires_at FROM sessions WHERE token_hash = ?")?
        .query_row([token_hash], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()
}

/// The member's `last_read_seq`, or `None` if they are not a member.
pub fn membership(conn: &Connection, chat_id: i64, user_id: i64) -> rusqlite::Result<Option<i64>> {
    conn.prepare_cached("SELECT last_read_seq FROM members WHERE chat_id = ? AND user_id = ?")?
        .query_row([chat_id, user_id], |r| r.get(0))
        .optional()
}

/// `(chat_id, last_read_seq)` for every chat the user owns or has joined.
pub fn user_chats(conn: &Connection, user_id: i64) -> rusqlite::Result<Vec<(i64, i64)>> {
    let mut stmt = conn.prepare_cached("SELECT chat_id, last_read_seq FROM members WHERE user_id = ?")?;
    let rows = stmt.query_map([user_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
    rows.collect()
}

pub fn top_chat_ids(conn: &Connection, kind: Kind, limit: i64) -> rusqlite::Result<Vec<i64>> {
    let mut stmt =
        conn.prepare_cached("SELECT id FROM chats WHERE kind = ? ORDER BY member_count DESC, id LIMIT ?")?;
    let rows = stmt.query_map(params![kind.as_str(), limit], |r| r.get(0))?;
    rows.collect()
}

// ---- writer commands ----

pub enum MetaCmd {
    CreateUser { login: String, pw_hash: String, reply: Reply<i64> },
    CreateSession { token_hash: Vec<u8>, user_id: i64, expires_at: i64, reply: Reply<()> },
    TouchSession { token_hash: Vec<u8>, expires_at: i64 },
    DeleteSession { token_hash: Vec<u8>, reply: Reply<()> },
    CreateChat { kind: Kind, title: String, owner_id: i64, reply: Reply<i64> },
    /// Replies `true` if a membership row was added, `false` if already a member.
    Join { chat_id: i64, user_id: i64, cap: i64, last_read_seq: i64, reply: Reply<bool> },
    /// Replies `true` if a membership row was removed.
    Leave { chat_id: i64, user_id: i64, reply: Reply<bool> },
    /// `seq` must already be clamped to the chat's `last_seq`.
    MarkRead { chat_id: i64, user_id: i64, seq: i64, reply: Option<Reply<()>> },
    DeleteChat { chat_id: i64, reply: Reply<()> },
}

pub struct MetaHandler;

impl Handler for MetaHandler {
    type Cmd = MetaCmd;

    fn apply(&mut self, conn: &Connection, cmd: MetaCmd) -> Applied {
        match cmd {
            MetaCmd::CreateUser { login, pw_hash, reply } => {
                let r = conn
                    .prepare_cached("INSERT INTO users(login, login_lc, pw_hash, created_at) VALUES (?, ?, ?, ?)")
                    .and_then(|mut s| s.execute(params![login, login.to_lowercase(), pw_hash, now_ms()]))
                    .map(|_| conn.last_insert_rowid())
                    .map_err(|e| if is_constraint(&e) { ApiError::conflict("login is already taken") } else { e.into() });
                complete(reply, r)
            }
            MetaCmd::CreateSession { token_hash, user_id, expires_at, reply } => {
                let r = conn
                    .prepare_cached("INSERT INTO sessions(token_hash, user_id, expires_at) VALUES (?, ?, ?)")
                    .and_then(|mut s| s.execute(params![token_hash, user_id, expires_at]))
                    .map(|_| ())
                    .map_err(ApiError::from);
                complete(reply, r)
            }
            MetaCmd::TouchSession { token_hash, expires_at } => {
                let r = conn
                    .prepare_cached("UPDATE sessions SET expires_at = ? WHERE token_hash = ?")
                    .and_then(|mut s| s.execute(params![expires_at, token_hash]));
                if let Err(e) = r {
                    tracing::error!("touch session: {e}");
                }
                Ok(noop())
            }
            MetaCmd::DeleteSession { token_hash, reply } => {
                let r = conn
                    .prepare_cached("DELETE FROM sessions WHERE token_hash = ?")
                    .and_then(|mut s| s.execute([token_hash]))
                    .map(|_| ())
                    .map_err(ApiError::from);
                complete(reply, r)
            }
            MetaCmd::CreateChat { kind, title, owner_id, reply } => {
                let r = create_chat(conn, kind, &title, owner_id).map_err(ApiError::from);
                complete(reply, r)
            }
            MetaCmd::Join { chat_id, user_id, cap, last_read_seq, reply } => {
                complete(reply, join(conn, chat_id, user_id, cap, last_read_seq))
            }
            MetaCmd::Leave { chat_id, user_id, reply } => {
                complete(reply, leave(conn, chat_id, user_id).map_err(ApiError::from))
            }
            MetaCmd::MarkRead { chat_id, user_id, seq, reply } => {
                let r = conn
                    .prepare_cached(
                        "UPDATE members SET last_read_seq = MAX(last_read_seq, ?) WHERE chat_id = ? AND user_id = ?",
                    )
                    .and_then(|mut s| s.execute(params![seq, chat_id, user_id]))
                    .map(|_| ())
                    .map_err(ApiError::from);
                match reply {
                    Some(reply) => complete(reply, r),
                    None => Ok(noop()),
                }
            }
            MetaCmd::DeleteChat { chat_id, reply } => {
                complete(reply, delete_chat(conn, chat_id))
            }
        }
    }
}

fn create_chat(conn: &Connection, kind: Kind, title: &str, owner_id: i64) -> rusqlite::Result<i64> {
    let now = now_ms();
    conn.prepare_cached("INSERT INTO chats(kind, title, owner_id, member_count, created_at) VALUES (?, ?, ?, 1, ?)")?
        .execute(params![kind.as_str(), title, owner_id, now])?;
    let id = conn.last_insert_rowid();
    conn.prepare_cached("INSERT INTO members(chat_id, user_id, joined_at, last_read_seq) VALUES (?, ?, ?, 0)")?
        .execute(params![id, owner_id, now])?;
    Ok(id)
}

fn join(conn: &Connection, chat_id: i64, user_id: i64, cap: i64, last_read_seq: i64) -> ApiResult<bool> {
    let inserted = conn
        .prepare_cached(
            "INSERT OR IGNORE INTO members(chat_id, user_id, joined_at, last_read_seq) VALUES (?, ?, ?, ?)",
        )?
        .execute(params![chat_id, user_id, now_ms(), last_read_seq])?;
    if inserted == 0 {
        return Ok(false);
    }
    // Commands are serialized on this thread, so this check can't race.
    let updated = conn
        .prepare_cached("UPDATE chats SET member_count = member_count + 1 WHERE id = ? AND member_count < ?")?
        .execute(params![chat_id, cap])?;
    if updated == 0 {
        return Err(ApiError::conflict("chat is full"));
    }
    Ok(true)
}

fn leave(conn: &Connection, chat_id: i64, user_id: i64) -> rusqlite::Result<bool> {
    let removed = conn
        .prepare_cached("DELETE FROM members WHERE chat_id = ? AND user_id = ?")?
        .execute([chat_id, user_id])?;
    if removed > 0 {
        conn.prepare_cached("UPDATE chats SET member_count = member_count - 1 WHERE id = ?")?.execute([chat_id])?;
    }
    Ok(removed > 0)
}

fn delete_chat(conn: &Connection, chat_id: i64) -> ApiResult<()> {
    let n = conn.prepare_cached("DELETE FROM chats WHERE id = ?")?.execute([chat_id])?;
    if n == 0 {
        return Err(ApiError::not_found("chat not found"));
    }
    conn.prepare_cached("DELETE FROM members WHERE chat_id = ?")?.execute([chat_id])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::writer::Writer;
    use std::sync::Arc;

    fn setup() -> (tempfile::TempDir, Arc<Writer<MetaCmd>>, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let conn = open(&path).unwrap();
        (dir, Arc::new(Writer::spawn("meta-test", conn, MetaHandler).unwrap()), path)
    }

    async fn user(w: &Writer<MetaCmd>, login: &str) -> ApiResult<i64> {
        w.request(|reply| MetaCmd::CreateUser { login: login.into(), pw_hash: "h".into(), reply }).await
    }

    #[test]
    fn shard_count_is_fixed_after_first_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.db");
        assert_eq!(init_shard_count(&open(&path).unwrap(), Some(8)).unwrap(), 8);
        assert_eq!(init_shard_count(&open(&path).unwrap(), Some(100)).unwrap(), 8);
        assert_eq!(init_shard_count(&open(&path).unwrap(), None).unwrap(), 8);

        let other = dir.path().join("other.db");
        assert_eq!(init_shard_count(&open(&other).unwrap(), None).unwrap(), DEFAULT_SHARDS);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn users_are_unique_case_insensitively() {
        let (_d, w, _) = setup();
        user(&w, "alice").await.unwrap();
        let e = user(&w, "Alice").await.unwrap_err();
        assert_eq!(e.status.as_u16(), 409);
        w.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn chat_lifecycle() {
        let (_d, w, path) = setup();
        let owner = user(&w, "owner").await.unwrap();
        let bob = user(&w, "bob").await.unwrap();
        let chat = w
            .request(|reply| MetaCmd::CreateChat { kind: Kind::Group, title: "g".into(), owner_id: owner, reply })
            .await
            .unwrap();
        let join = |uid| w.request(move |reply| MetaCmd::Join { chat_id: chat, user_id: uid, cap: 1000, last_read_seq: 7, reply });
        assert!(join(bob).await.unwrap());
        assert!(!join(bob).await.unwrap());

        let r = open(&path).unwrap();
        assert_eq!(membership(&r, chat, bob).unwrap(), Some(7));
        let count: i64 = r.query_row("SELECT member_count FROM chats WHERE id = ?", [chat], |r| r.get(0)).unwrap();
        assert_eq!(count, 2);

        w.request(|reply| MetaCmd::MarkRead { chat_id: chat, user_id: bob, seq: 9, reply: Some(reply) }).await.unwrap();
        w.request(|reply| MetaCmd::MarkRead { chat_id: chat, user_id: bob, seq: 3, reply: Some(reply) }).await.unwrap();
        assert_eq!(membership(&r, chat, bob).unwrap(), Some(9));

        assert!(w.request(|reply| MetaCmd::Leave { chat_id: chat, user_id: bob, reply }).await.unwrap());
        assert!(!w.request(|reply| MetaCmd::Leave { chat_id: chat, user_id: bob, reply }).await.unwrap());
        assert_eq!(membership(&r, chat, bob).unwrap(), None);

        w.request(|reply| MetaCmd::DeleteChat { chat_id: chat, reply }).await.unwrap();
        assert!(user_chats(&r, owner).unwrap().is_empty());
        let e = w.request(|reply| MetaCmd::DeleteChat { chat_id: chat, reply }).await.unwrap_err();
        assert_eq!(e.status.as_u16(), 404);
        w.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_joins_respect_cap() {
        let (_d, w, path) = setup();
        let owner = user(&w, "owner").await.unwrap();
        let chat = w
            .request(|reply| MetaCmd::CreateChat { kind: Kind::Channel, title: "c".into(), owner_id: owner, reply })
            .await
            .unwrap();
        // Bring the channel to 9,998 members directly.
        {
            let c = open(&path).unwrap();
            c.execute("UPDATE chats SET member_count = 9998 WHERE id = ?", [chat]).unwrap();
        }
        let mut uids = Vec::new();
        for i in 0..5 {
            uids.push(user(&w, &format!("u{i}")).await.unwrap());
        }
        let tasks: Vec<_> = uids
            .into_iter()
            .map(|uid| {
                let w = w.clone();
                tokio::spawn(async move {
                    w.request(|reply| MetaCmd::Join { chat_id: chat, user_id: uid, cap: 10_000, last_read_seq: 0, reply })
                        .await
                })
            })
            .collect();
        let mut ok = 0;
        let mut full = 0;
        for t in tasks {
            match t.await.unwrap() {
                Ok(true) => ok += 1,
                Err(e) if e.status.as_u16() == 409 => full += 1,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!((ok, full), (2, 3));
        let c = open(&path).unwrap();
        let count: i64 = c.query_row("SELECT member_count FROM chats WHERE id = ?", [chat], |r| r.get(0)).unwrap();
        let rows: i64 = c.query_row("SELECT COUNT(*) FROM members WHERE chat_id = ?", [chat], |r| r.get(0)).unwrap();
        assert_eq!(count, 10_000);
        assert_eq!(rows, 3); // the owner plus the two that got in
        w.stop().await;
    }
}

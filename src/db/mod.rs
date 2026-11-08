pub mod meta;
pub mod shard;
pub mod writer;

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OpenFlags};

use crate::error::{ApiError, ApiResult};

pub type Pool = r2d2::Pool<SqliteConnectionManager>;

pub const SCHEMA_VERSION: i64 = 1;
const READERS_PER_FILE: u32 = 4;

/// Opens the single writable connection for a database file.
pub fn open_rw(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
    conn.execute_batch("PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=OFF; PRAGMA cache_size=-8000;")?;
    Ok(conn)
}

/// Applies `schema` to a fresh database and stamps `user_version`.
pub fn migrate(conn: &Connection, schema: &str) -> rusqlite::Result<()> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version < SCHEMA_VERSION {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(schema)?;
        tx.execute_batch(&format!("PRAGMA user_version={SCHEMA_VERSION}"))?;
        tx.commit()?;
    }
    Ok(())
}

/// Read-only pool; connections open lazily so idle shards hold none.
pub fn read_pool(path: &Path) -> Result<Pool, r2d2::Error> {
    let manager = SqliteConnectionManager::file(path)
        .with_flags(OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI)
        .with_init(|c| {
            c.busy_timeout(Duration::from_secs(5))?;
            c.execute_batch("PRAGMA query_only=1; PRAGMA cache_size=-8000;")
        });
    r2d2::Pool::builder()
        .max_size(READERS_PER_FILE)
        .min_idle(Some(0))
        .build(manager)
}

/// Runs a read query on a pooled connection off the async runtime.
pub async fn read<T, F>(pool: &Pool, f: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
{
    let pool = pool.clone();
    tokio::task::spawn_blocking(move || {
        let conn = pool.get()?;
        Ok(f(&conn)?)
    })
    .await
    .map_err(|_| ApiError::internal())?
}

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

pub fn now_s() -> i64 {
    now_ms() / 1000
}

pub fn is_constraint(e: &rusqlite::Error) -> bool {
    matches!(e, rusqlite::Error::SqliteFailure(f, _) if f.code == rusqlite::ErrorCode::ConstraintViolation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_sets_wal_and_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.db");
        let conn = open_rw(&path).unwrap();
        migrate(&conn, "CREATE TABLE a(x)").unwrap();
        let mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
        assert_eq!(mode, "wal");
        assert_eq!(version, 1);
        // Running again is a no-op rather than a "table exists" error.
        migrate(&conn, "CREATE TABLE a(x)").unwrap();

        let pool = read_pool(&path).unwrap();
        let n: i64 = pool.get().unwrap().query_row("SELECT COUNT(*) FROM a", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 0);
        assert!(pool.get().unwrap().execute("INSERT INTO a VALUES (1)", []).is_err());
    }
}

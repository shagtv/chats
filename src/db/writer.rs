//! One writer thread per SQLite file with natural group commit (design D3).
//!
//! The thread blocks for the first command, drains whatever else is queued
//! (up to `MAX_BATCH`) and runs it all in one transaction. Each command runs in
//! its own savepoint so a failing command doesn't abort its neighbours. Replies
//! are sent only after the commit.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use rusqlite::Connection;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};

use crate::error::{ApiError, ApiResult};

pub const MAX_BATCH: usize = 512;
pub const QUEUE_CAPACITY: usize = 4096;

/// Runs after the batch finishes; the argument says whether it committed.
pub type Completion = Box<dyn FnOnce(bool) + Send>;
/// `Ok` keeps the command's savepoint, `Err` rolls it back.
pub type Applied = Result<Completion, Completion>;
pub type Reply<T> = oneshot::Sender<ApiResult<T>>;

/// Turns a command result into a completion that answers `reply` after commit.
pub fn complete<T: Send + 'static>(reply: Reply<T>, result: ApiResult<T>) -> Applied {
    match result {
        Ok(v) => Ok(Box::new(move |committed| {
            let _ = reply.send(if committed { Ok(v) } else { Err(ApiError::internal()) });
        })),
        Err(e) => Err(Box::new(move |_| {
            let _ = reply.send(Err(e));
        })),
    }
}

pub fn noop() -> Completion {
    Box::new(|_| {})
}

pub trait Handler: Send + 'static {
    type Cmd: Send + 'static;

    /// Executes one command inside the open batch transaction.
    fn apply(&mut self, conn: &Connection, cmd: Self::Cmd) -> Applied;

    /// Runs inside the transaction after all commands, before COMMIT.
    fn end_batch(&mut self, _conn: &Connection) -> rusqlite::Result<()> {
        Ok(())
    }

    /// Runs after COMMIT or ROLLBACK, before completions.
    fn after_batch(&mut self, _committed: bool) {}

    /// Background work runs one step per loop iteration, between batches.
    fn has_background(&self) -> bool {
        false
    }

    fn background(&mut self, _conn: &Connection) -> rusqlite::Result<()> {
        Ok(())
    }
}

enum Envelope<C> {
    Cmd(C),
    Stop,
}

pub struct Writer<C> {
    tx: mpsc::Sender<Envelope<C>>,
    commits: Arc<AtomicU64>,
    cmds: Arc<AtomicU64>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl<C: Send + 'static> Writer<C> {
    pub fn spawn<H: Handler<Cmd = C>>(name: &str, conn: Connection, handler: H) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let commits = Arc::new(AtomicU64::new(0));
        let cmds = Arc::new(AtomicU64::new(0));
        let counters = (commits.clone(), cmds.clone());
        let thread = std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || run(conn, handler, rx, &counters.0, &counters.1))?;
        Ok(Self { tx, commits, cmds, thread: Mutex::new(Some(thread)) })
    }

    /// Queues a command; waits only if the queue is full.
    pub async fn send(&self, cmd: C) -> ApiResult<()> {
        self.tx.send(Envelope::Cmd(cmd)).await.map_err(|_| ApiError::unavailable())
    }

    /// Queues a command and waits for its post-commit reply.
    pub async fn request<T>(&self, make: impl FnOnce(Reply<T>) -> C) -> ApiResult<T> {
        let (tx, rx) = oneshot::channel();
        self.send(make(tx)).await?;
        rx.await.map_err(|_| ApiError::unavailable())?
    }

    pub fn commits(&self) -> u64 {
        self.commits.load(Ordering::Relaxed)
    }

    /// Commands handled so far, committed or not.
    pub fn cmds(&self) -> u64 {
        self.cmds.load(Ordering::Relaxed)
    }

    /// Commands waiting in the queue.
    pub fn queue(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }

    /// Lets the thread finish everything queued so far, then joins it.
    pub async fn stop(&self) {
        let _ = self.tx.send(Envelope::Stop).await;
        let handle = self.thread.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = tokio::task::spawn_blocking(move || h.join()).await;
        }
    }
}

fn run<H: Handler>(
    conn: Connection,
    mut handler: H,
    mut rx: mpsc::Receiver<Envelope<H::Cmd>>,
    commits: &AtomicU64,
    cmds: &AtomicU64,
) {
    let mut batch = Vec::with_capacity(MAX_BATCH);
    let mut stopping = false;
    loop {
        let first = if handler.has_background() {
            match rx.try_recv() {
                Ok(e) => Some(e),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => {
                    stopping = true;
                    None
                }
            }
        } else {
            let e = rx.blocking_recv();
            stopping = e.is_none();
            e
        };
        match first {
            Some(Envelope::Cmd(c)) => batch.push(c),
            Some(Envelope::Stop) => stopping = true,
            None => {}
        }
        while !stopping && !batch.is_empty() && batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(Envelope::Cmd(c)) => batch.push(c),
                Ok(Envelope::Stop) => stopping = true,
                Err(_) => break,
            }
        }
        if !batch.is_empty() {
            cmds.fetch_add(batch.len() as u64, Ordering::Relaxed);
            run_batch(&conn, &mut handler, batch.drain(..), commits);
        }
        if stopping {
            break;
        }
        if handler.has_background() {
            begin(&conn);
            let ok = handler.background(&conn).and_then(|_| conn.execute_batch("COMMIT"));
            if let Err(e) = ok {
                tracing::error!("background step failed: {e}");
                let _ = conn.execute_batch("ROLLBACK");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn begin(conn: &Connection) {
    loop {
        match conn.execute_batch("BEGIN IMMEDIATE") {
            Ok(()) => return,
            Err(e) => {
                tracing::error!("BEGIN failed, retrying: {e}");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn run_batch<H: Handler>(
    conn: &Connection,
    handler: &mut H,
    cmds: impl Iterator<Item = H::Cmd>,
    commits: &AtomicU64,
) {
    begin(conn);
    let mut done = Vec::with_capacity(MAX_BATCH);
    for cmd in cmds {
        if let Err(e) = conn.execute_batch("SAVEPOINT cmd") {
            tracing::error!("SAVEPOINT failed: {e}");
        }
        match handler.apply(conn, cmd) {
            Ok(c) => {
                let _ = conn.execute_batch("RELEASE cmd");
                done.push(c);
            }
            Err(c) => {
                let _ = conn.execute_batch("ROLLBACK TO cmd; RELEASE cmd");
                done.push(c);
            }
        }
    }
    let committed = match handler.end_batch(conn).and_then(|_| conn.execute_batch("COMMIT")) {
        Ok(()) => true,
        Err(e) => {
            tracing::error!("batch commit failed: {e}");
            let _ = conn.execute_batch("ROLLBACK");
            false
        }
    };
    if committed {
        commits.fetch_add(1, Ordering::Relaxed);
    }
    handler.after_batch(committed);
    for c in done {
        c(committed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_rw;

    struct TestHandler;

    struct Insert {
        val: i64,
        fail: bool,
        reply: Reply<i64>,
    }

    impl Handler for TestHandler {
        type Cmd = Insert;
        fn apply(&mut self, conn: &Connection, cmd: Insert) -> Applied {
            let r = conn
                .execute("INSERT INTO t(v) VALUES (?)", [cmd.val])
                .map_err(ApiError::from)
                .and_then(|_| if cmd.fail { Err(ApiError::conflict("boom")) } else { Ok(cmd.val) });
            complete(cmd.reply, r)
        }
    }

    fn writer() -> (tempfile::TempDir, Writer<Insert>, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w.db");
        let conn = open_rw(&path).unwrap();
        conn.execute_batch("CREATE TABLE t(v INTEGER)").unwrap();
        (dir, Writer::spawn("test-writer", conn, TestHandler).unwrap(), path)
    }

    fn count(path: &std::path::Path) -> i64 {
        open_rw(path).unwrap().query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0)).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failing_command_does_not_abort_batch() {
        let (_dir, w, path) = writer();
        let mut rxs = Vec::new();
        for i in 0..10 {
            let (tx, rx) = oneshot::channel();
            w.send(Insert { val: i, fail: i == 5, reply: tx }).await.unwrap();
            rxs.push(rx);
        }
        for (i, rx) in rxs.into_iter().enumerate() {
            let r = rx.await.unwrap();
            assert_eq!(r.is_err(), i == 5);
        }
        assert_eq!(count(&path), 9);
        w.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_submissions_are_group_committed() {
        let (_dir, w, path) = writer();
        let w = Arc::new(w);
        let tasks: Vec<_> = (0..1000)
            .map(|i| {
                let w = w.clone();
                tokio::spawn(async move { w.request(|reply| Insert { val: i, fail: false, reply }).await })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(count(&path), 1000);
        assert_eq!(w.cmds(), 1000);
        assert_eq!(w.queue(), 0, "nothing waits once every reply is in");
        let commits = w.commits();
        assert!(commits < 500, "expected batching, got {commits} commits for 1000 commands");
        w.stop().await;
    }
}

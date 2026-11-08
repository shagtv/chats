use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use crate::auth::Auth;
use crate::config::Config;
use crate::db::meta::{self, MetaCmd, MetaHandler};
use crate::db::shard::{self, MessageSink, ShardCmd, ShardHandler};
use crate::db::writer::Writer;
use crate::db::{self, Pool};
use crate::hub::{ChatState, Hub};

pub struct Shard {
    pub writer: Writer<ShardCmd>,
    pub pool: Pool,
}

pub struct Db {
    pub meta: Writer<MetaCmd>,
    pub meta_pool: Pool,
    pub shards: Vec<Shard>,
}

impl Db {
    pub fn shard(&self, chat_id: i64) -> &Shard {
        &self.shards[(chat_id as u64 % self.shards.len() as u64) as usize]
    }

    /// Drains every writer queue and joins the writer threads.
    pub async fn stop(&self) {
        for s in &self.shards {
            s.writer.stop().await;
        }
        self.meta.stop().await;
    }
}

pub struct AppInner {
    pub hub: Arc<Hub>,
    pub db: Db,
    pub auth: Auth,
}

#[derive(Clone)]
pub struct App(Arc<AppInner>);

impl Deref for App {
    type Target = AppInner;
    fn deref(&self) -> &AppInner {
        &self.0
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl App {
    /// Opens (creating if needed) all databases and loads runtime state.
    pub async fn open(config: &Config) -> Result<App, BoxError> {
        let data_dir = config.data_dir.clone();
        let shards = config.shards;
        let app = tokio::task::spawn_blocking(move || open_blocking(&data_dir, shards)).await??;
        app.hub.refresh_top().await.map_err(|e| e.message)?;
        Ok(app)
    }
}

fn open_blocking(data_dir: &Path, requested_shards: Option<u32>) -> Result<App, BoxError> {
    std::fs::create_dir_all(data_dir)?;
    let meta_path = data_dir.join("meta.db");
    let meta_conn = meta::open(&meta_path)?;
    let shard_count = meta::init_shard_count(&meta_conn, requested_shards)?;
    let chats = meta::load_chats(&meta_conn)?;
    let meta_pool = db::read_pool(&meta_path)?;
    let hub = Hub::new(meta_pool.clone());
    for row in chats {
        hub.insert_chat(ChatState::new(row, 0));
    }

    let mut shards = Vec::with_capacity(shard_count as usize);
    for i in 0..shard_count {
        let path = data_dir.join(format!("msg-{i:02}.db"));
        let conn = shard::open(&path)?;
        let sink: Arc<dyn MessageSink> = hub.clone();
        let handler = ShardHandler::new(&conn, sink)?;
        for (&chat_id, &seq) in handler.seqs() {
            hub.set_last_seq(chat_id, seq);
        }
        let writer = Writer::spawn(&format!("shard-{i:02}"), conn, handler)?;
        shards.push(Shard { writer, pool: db::read_pool(&path)? });
    }
    let meta = Writer::spawn("meta", meta_conn, MetaHandler)?;
    tracing::info!("opened {} with {shard_count} shards and {} chats", data_dir.display(), hub.chat_count());

    Ok(App(Arc::new(AppInner { hub, db: Db { meta, meta_pool, shards }, auth: Auth::new() })))
}

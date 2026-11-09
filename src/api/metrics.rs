//! `GET /api/metrics` (specs/server-metrics). Routed only with `--metrics`;
//! takes no session and reads only in-memory state.

use std::sync::atomic::Ordering;

use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};

use crate::app::App;

pub async fn metrics(State(app): State<App>) -> Json<Value> {
    Json(snapshot(&app))
}

pub fn snapshot(app: &App) -> Value {
    let shards = &app.db.shards;
    let (mut queue_max, mut queue_max_shard, mut queue_sum, mut commits, mut cmds) = (0, 0, 0, 0, 0);
    for (i, s) in shards.iter().enumerate() {
        let q = s.writer.queue();
        if q > queue_max {
            (queue_max, queue_max_shard) = (q, i);
        }
        queue_sum += q;
        commits += s.writer.commits();
        cmds += s.writer.cmds();
    }
    json!({
        "uptime_s": app.started.elapsed().as_secs(),
        "ws": app.hub.ws_stats(),
        "chats": app.hub.chat_stats(),
        "writers": {
            "meta": {
                "queue": app.db.meta.queue(),
                "commits_total": app.db.meta.commits(),
                "cmds_total": app.db.meta.cmds(),
            },
            "shards": {
                "count": shards.len(),
                "queue_max": queue_max,
                "queue_max_shard": queue_max_shard,
                "queue_sum": queue_sum,
                "commits_total": commits,
                "cmds_total": cmds,
            },
        },
        "http": {
            "requests_total": app.http.requests_total.load(Ordering::Relaxed),
            "errors_5xx_total": app.http.errors_5xx_total.load(Ordering::Relaxed),
        },
    })
}

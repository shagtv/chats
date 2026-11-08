use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};

use crate::app::App;
use crate::auth::AuthUser;
use crate::db::{self, meta};
use crate::error::ApiResult;

pub async fn home(State(app): State<App>, user: AuthUser) -> ApiResult<Json<Value>> {
    let user_id = user.id;
    let rows = db::read(&app.db.meta_pool, move |c| meta::user_chats(c, user_id)).await?;
    let mut my: Vec<(i64, Value)> = rows
        .into_iter()
        .filter_map(|(chat_id, last_read)| {
            let chat = app.hub.get(chat_id)?;
            let last_seq = chat.last_seq();
            Some((
                chat_id,
                json!({
                    "id": chat.id,
                    "kind": chat.kind,
                    "title": chat.title,
                    "member_count": chat.member_count(),
                    "is_owner": chat.owner_id == user.id,
                    "last_seq": last_seq,
                    "last_read_seq": last_read,
                    "unread": (last_seq - last_read).max(0),
                }),
            ))
        })
        .collect();
    my.sort_by_key(|c| std::cmp::Reverse(c.0));
    let top = app.hub.top();
    Ok(Json(json!({
        "top_groups": top.groups,
        "top_channels": top.channels,
        "my_chats": my.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
    })))
}

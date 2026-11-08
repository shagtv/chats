use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

use super::chats::find;
use super::{Body, ChatId};
use crate::app::App;
use crate::auth::AuthUser;
use crate::db::meta::{self, MetaCmd};
use crate::db::shard::{self, ShardCmd};
use crate::db;
use crate::error::{ApiError, ApiResult};
use crate::model::{Author, Kind, Message};

pub const MAX_BODY_CHARS: usize = 4_000;
pub const DEFAULT_PAGE: i64 = 50;
pub const MAX_PAGE: i64 = 100;

#[derive(Deserialize)]
pub struct NewMessage {
    body: String,
}

pub fn validate_body(body: &str) -> ApiResult<()> {
    let len = body.trim().chars().count();
    if len == 0 || len > MAX_BODY_CHARS {
        return Err(ApiError::bad_request("message must be 1-4000 characters"));
    }
    Ok(())
}

pub async fn post(
    State(app): State<App>,
    user: AuthUser,
    ChatId(id): ChatId,
    Body(req): Body<NewMessage>,
) -> ApiResult<(StatusCode, Json<Message>)> {
    let chat = find(&app, id)?;
    validate_body(&req.body)?;
    let allowed = match chat.kind {
        Kind::Channel => chat.owner_id == user.id,
        Kind::Group => {
            let user_id = user.id;
            db::read(&app.db.meta_pool, move |c| meta::membership(c, id, user_id)).await?.is_some()
        }
    };
    if !allowed {
        return Err(ApiError::forbidden(match chat.kind {
            Kind::Channel => "only the owner can post in a channel",
            Kind::Group => "join the group to post",
        }));
    }
    let (author_id, author_login) = (user.id, user.login.clone());
    let msg = app
        .db
        .shard(id)
        .writer
        .request(|reply| ShardCmd::Append { chat_id: id, author_id, author_login, body: req.body, reply })
        .await?;
    // The author has read everything up to their own message.
    let seq = msg.seq;
    app.db
        .meta
        .request(|reply| MetaCmd::MarkRead { chat_id: id, user_id: author_id, seq, reply: Some(reply) })
        .await?;
    Ok((StatusCode::CREATED, Json(msg)))
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    before: Option<i64>,
    after: Option<i64>,
    limit: Option<i64>,
}

pub async fn history(
    State(app): State<App>,
    _user: AuthUser,
    ChatId(id): ChatId,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<Json<Value>> {
    find(&app, id)?;
    let limit = q.limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
    let (rows, has_more) =
        db::read(&app.db.shard(id).pool, move |c| shard::history(c, id, q.before, q.after, limit)).await?;
    let mut author_ids: Vec<i64> = rows.iter().map(|m| m.author_id).collect();
    author_ids.sort_unstable();
    author_ids.dedup();
    let logins = app.hub.logins_for(&author_ids).await?;
    let messages: Vec<Message> = rows
        .into_iter()
        .map(|m| Message {
            chat_id: id,
            seq: m.seq,
            author: Author { id: m.author_id, login: logins.get(&m.author_id).cloned().unwrap_or_default() },
            body: m.body,
            ts: m.ts,
        })
        .collect();
    Ok(Json(json!({ "messages": messages, "has_more": has_more })))
}

#[derive(Deserialize)]
pub struct ReadRequest {
    seq: i64,
}

pub async fn mark_read(
    State(app): State<App>,
    user: AuthUser,
    ChatId(id): ChatId,
    Body(req): Body<ReadRequest>,
) -> ApiResult<StatusCode> {
    let chat = find(&app, id)?;
    let seq = req.seq.min(chat.last_seq());
    let user_id = user.id;
    // A non-member has no row, so the update changes nothing.
    app.db
        .meta
        .request(|reply| MetaCmd::MarkRead { chat_id: id, user_id, seq, reply: Some(reply) })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_rules() {
        assert!(validate_body("hi").is_ok());
        assert!(validate_body("   ").is_err());
        assert!(validate_body(&"я".repeat(4000)).is_ok());
        assert!(validate_body(&"x".repeat(4001)).is_err());
    }
}

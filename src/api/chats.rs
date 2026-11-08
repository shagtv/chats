use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Body, ChatId};
use crate::app::App;
use crate::auth::AuthUser;
use crate::db::meta::{self, ChatRow, MetaCmd};
use crate::db::shard::ShardCmd;
use crate::db;
use crate::error::{ApiError, ApiResult};
use crate::hub::ChatState;
use crate::model::Kind;

pub const MAX_TITLE_CHARS: usize = 100;

pub fn find(app: &App, chat_id: i64) -> ApiResult<Arc<ChatState>> {
    app.hub.get(chat_id).ok_or_else(|| ApiError::not_found("chat not found"))
}

/// The chat as seen by `user`, including their membership and permissions.
pub async fn view(app: &App, chat: &ChatState, user: &AuthUser) -> ApiResult<Value> {
    let (chat_id, user_id) = (chat.id, user.id);
    let last_read = db::read(&app.db.meta_pool, move |c| meta::membership(c, chat_id, user_id)).await?;
    let is_member = last_read.is_some();
    let is_owner = chat.owner_id == user.id;
    let can_post = match chat.kind {
        Kind::Channel => is_owner,
        Kind::Group => is_member,
    };
    Ok(json!({
        "id": chat.id,
        "kind": chat.kind,
        "title": chat.title,
        "owner": { "id": chat.owner_id, "login": chat.owner_login },
        "member_count": chat.member_count(),
        "last_seq": chat.last_seq(),
        "last_read_seq": last_read,
        "is_member": is_member,
        "is_owner": is_owner,
        "can_post": can_post,
    }))
}

#[derive(Deserialize)]
pub struct NewChat {
    kind: String,
    title: String,
}

pub async fn create(
    State(app): State<App>,
    user: AuthUser,
    Body(req): Body<NewChat>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    let kind = Kind::parse(&req.kind).ok_or_else(|| ApiError::bad_request("kind must be group or channel"))?;
    let title = req.title.trim().to_string();
    let len = title.chars().count();
    if len == 0 || len > MAX_TITLE_CHARS {
        return Err(ApiError::bad_request("title must be 1-100 characters"));
    }
    let owner_id = user.id;
    let t = title.clone();
    let id = app
        .db
        .meta
        .request(|reply| MetaCmd::CreateChat { kind, title: t, owner_id, reply })
        .await?;
    let row = ChatRow { id, kind, title, owner_id, owner_login: user.login.clone(), member_count: 1 };
    app.hub.insert_chat(ChatState::new(row, 0));
    app.hub.on_join(user.id, id);
    let chat = find(&app, id)?;
    Ok((StatusCode::CREATED, Json(view(&app, &chat, &user).await?)))
}

pub async fn get(State(app): State<App>, user: AuthUser, ChatId(id): ChatId) -> ApiResult<Json<Value>> {
    let chat = find(&app, id)?;
    Ok(Json(view(&app, &chat, &user).await?))
}

pub async fn join(State(app): State<App>, user: AuthUser, ChatId(id): ChatId) -> ApiResult<Json<Value>> {
    let chat = find(&app, id)?;
    let (user_id, cap, last_read_seq) = (user.id, chat.kind.member_cap(), chat.last_seq());
    let joined = app
        .db
        .meta
        .request(|reply| MetaCmd::Join { chat_id: id, user_id, cap, last_read_seq, reply })
        .await?;
    if joined {
        chat.member_count.fetch_add(1, Ordering::AcqRel);
        app.hub.on_join(user.id, id);
    }
    Ok(Json(view(&app, &chat, &user).await?))
}

pub async fn leave(State(app): State<App>, user: AuthUser, ChatId(id): ChatId) -> ApiResult<Json<Value>> {
    let chat = find(&app, id)?;
    if chat.owner_id == user.id {
        return Err(ApiError::conflict("the owner can't leave; delete the chat instead"));
    }
    let user_id = user.id;
    let left = app.db.meta.request(|reply| MetaCmd::Leave { chat_id: id, user_id, reply }).await?;
    if left {
        chat.member_count.fetch_sub(1, Ordering::AcqRel);
        app.hub.on_leave(user.id, id);
    }
    Ok(Json(view(&app, &chat, &user).await?))
}

pub async fn delete(State(app): State<App>, user: AuthUser, ChatId(id): ChatId) -> ApiResult<StatusCode> {
    let chat = find(&app, id)?;
    if chat.owner_id != user.id {
        return Err(ApiError::forbidden("only the owner can delete this chat"));
    }
    app.db.meta.request(|reply| MetaCmd::DeleteChat { chat_id: id, reply }).await?;
    app.hub.remove_chat(id);
    app.db.shard(id).writer.send(ShardCmd::Purge { chat_id: id }).await?;
    Ok(StatusCode::NO_CONTENT)
}

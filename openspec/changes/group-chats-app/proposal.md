# Proposal

## Why

We want a group chat web app that stays fast as it grows: groups of up to 1,000 members, broadcast channels of up to 10,000 members, and many chats written to at once. It should ship as one self-contained Rust binary with a plain static frontend. The project is currently an empty Rust crate, so this change builds the whole first version.

## What Changes

- Add a single Rust (tokio + axum) server binary that serves `/` and static files from a `static/` folder next to it, and serves a JSON + WebSocket API under `/api`.
- Add user accounts: register (login, password, repeat password) and sign in with login and password. Passwords are hashed with argon2id. Sessions are cookie-based, and the server stores only a hash of each session token.
- Add two chat kinds:
  - **group**: up to 1,000 members, and any member can post.
  - **channel**: up to 10,000 members, and only the creator can post.
- Anyone who is signed in can read any chat. Joining increases the member count and adds the chat to "My chats". Members can leave a chat. Owners can delete their chat instead of leaving it.
- Add text messages (up to 4,000 characters), numbered with a sequence per chat. History is paginated, newest first, with a `before=seq` cursor.
- Add unread counters for every chat a user has joined or created. The counter is `last_seq - last_read_seq`, and opening a chat resets it.
- Add realtime delivery over one WebSocket per tab. The open chat receives every message as it is posted. A user's other chats receive batched sequence updates (at most one frame per second) to drive their unread badges.
- Add a single-page client in plain HTML, CSS and JS:
  - a login/register view
  - a home view with Top 10 groups by members, Top 10 channels by members, and My chats (joined and created, with unread badges)
  - a "New chat" action
  - a chat view that loads older messages as you scroll up
- Split storage across SQLite files:
  - `meta.db` holds users, sessions, chats and memberships.
  - A configurable number of message shards (default 32, fixed at first start) hold messages. Each shard has its own writer thread, and the writer commits messages in batches.

## Capabilities

### New Capabilities

- `user-auth`: registration, login and logout, session cookies, password rules, and the current-user endpoint.
- `chat-management`: creating, listing, joining, leaving and deleting chats. Also covers chat kinds, member caps, the read-access model, and the home page lists (top groups, top channels, my chats).
- `messaging`: posting text messages with permission rules, per-chat sequence numbers, paginated history, read markers and unread counts.
- `realtime-updates`: the WebSocket protocol. Covers subscribing to the open chat, live message frames, batched sequence updates for the user's chats, chat-deleted notices, and resync after a client falls behind.
- `web-client`: the single-page HTML/CSS/JS client, including its views and behaviour (auth forms, home sections, chat view with scroll-back, unread badges).

### Modified Capabilities

None. This is the first change in the project.

## Impact

- **Code:** replaces the stub `src/main.rs` with the full server. Adds a `static/` folder (`index.html`, `app.js`, `app.css`).
- **Dependencies:** adds tokio, axum (with ws), tower-http (fs), rusqlite (bundled), serde/serde_json, argon2, rand, sha2, and tracing.
- **Runtime layout:** one binary plus `static/` and a `data/` directory (created on first run) holding `meta.db` and `msg-NN.db` files.
- **API:** `/api/register`, `/api/login`, `/api/logout`, `/api/me`, `/api/home`, `/api/chats`, `/api/chats/{id}`, `/api/chats/{id}/join`, `/api/chats/{id}/leave`, `/api/chats/{id}/messages`, `/api/chats/{id}/read`, `/api/ws`.
- **Out of scope:** file and image uploads, fonts, editing or deleting individual messages, running more than one process, changing the shard count after first start, password reset, and limits on how fast users can post.

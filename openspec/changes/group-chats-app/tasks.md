# Tasks

## 1. Project setup

- [x] 1.1 Add dependencies to `Cargo.toml`: tokio (full), axum (ws), tower-http (fs), rusqlite (bundled), r2d2, r2d2_sqlite, serde, serde_json, argon2, rand, sha2, base64, dashmap, arc-swap, tracing, tracing-subscriber. Dev-dependencies: reqwest (json, cookies) and tokio-tungstenite. Verify `cargo build` succeeds.
- [x] 1.2 Create the module skeleton from design D14 (`config`, `db/{mod,writer,meta,shard}`, `hub`, `auth`, `api/*`, `ws`, `error`) and verify `cargo check` passes.
- [x] 1.3 Implement `config.rs` (`--addr`, `--data-dir`, `--static-dir`, `--shards`, each with an env override) and verify unit tests for defaults and overrides pass.
- [x] 1.4 Implement `error.rs` (`ApiError` mapping to a status and `{"error": ...}` body) and verify a unit test asserts the status and body for 400, 401, 403, 404, 409 and 429.

## 2. Storage layer

- [x] 2.1 Implement `db/mod.rs`: open with pragmas (WAL, synchronous=NORMAL, busy_timeout), `user_version` migrations, and read-only r2d2 pools with `min_idle` 0. Verify a test opens a temp DB and asserts `journal_mode=wal` and `user_version=1`.
- [x] 2.2 Implement `db/writer.rs`: a generic writer thread that drains up to 512 commands per batch into one `BEGIN IMMEDIATE` transaction, wraps each command in a savepoint, and replies after commit. Verify tests show that a failing command doesn't abort the rest of its batch, and that 1,000 concurrent submissions finish in far fewer commits than commands.
- [x] 2.3 Implement the `db/meta.rs` schema (config, users, sessions, chats, members and their indexes) and the shard count persisted on first start, with a warning when `--shards` disagrees later. Verify a test where a restart with a different `--shards` keeps the stored count.
- [x] 2.4 Implement the meta commands: CreateUser, CreateSession, TouchSession, DeleteSession, CreateChat (with the owner as a member), Join (cap enforced with a conditional UPDATE), Leave, MarkRead (MAX and clamp), DeleteChat. Verify unit tests, including 5 concurrent joins at 9,998/10,000 giving exactly 2 successes.
- [x] 2.5 Implement the `db/shard.rs` schema (messages, chat_seq, purge_queue) and the Append command, which assigns gap-free seq in the writer and upserts `chat_seq` once per touched chat per batch. Verify tests for consecutive seq within a batch, rollback on transaction failure, and seq continuing after reopening the file.
- [x] 2.6 Implement the shard Purge command: delete in 5,000-seq chunks, one chunk per loop iteration, and resume from `purge_queue` at startup. Verify a test that purges a 12,000-message chat, and one that resumes after a simulated restart.
- [x] 2.7 Implement the history queries (`before`/`after`/`limit` capped at 100, `has_more`) on the shard read pools. Verify tests for the 120-message pagination and catch-up scenarios from `specs/messaging`.

## 3. Runtime hub

- [x] 3.1 Implement `hub.rs` ChatState (kind, title, owner, atomic member_count and last_seq, lazy broadcast sender) and startup loading from `meta.db` and every shard's `chat_seq`. Verify a test that the loaded state matches the seeded databases.
- [x] 3.2 Implement the user login cache and the top-10 snapshot task (every 2 s, pre-serialized JSON in an ArcSwap). Verify a test that seeds 15 groups and asserts top-10 order and that no channels are included.
- [x] 3.3 Implement the online-membership index (online_users, chat_online), the dirty set, and the 1-second seq ticker that builds one frame per user. Verify a test where 20 appends in one tick produce one `seq` frame per connection, carrying the latest seq.
- [x] 3.4 Wire post-commit side effects from the shard writer: update `last_seq`, broadcast the frame serialized once, mark the chat dirty, send the author's MarkRead. Verify a test where a subscriber receives the `msg` frame and the author's unread count stays 0.

## 4. Authentication

- [x] 4.1 Implement argon2id hashing behind a CPU-sized semaphore in spawn_blocking, plus dummy-hash verification for unknown logins. Verify a unit test for hash and verify, and confirm that no plaintext is stored.
- [x] 4.2 Implement session tokens (32 bytes from OsRng, base64url cookie `sid`, HttpOnly and SameSite=Strict, sha256 stored), the session cache, sliding 30-day expiry persisted at most hourly, and an axum `AuthUser` extractor that returns 401. Verify tests for expiry, renewal, and logout invalidation.
- [x] 4.3 Implement the login throttle (5 failures per minute per login, giving 429) with periodic cleanup. Verify a unit test.
- [x] 4.4 Implement the `/api/register`, `/api/login`, `/api/logout` and `/api/me` handlers with validation for login `[A-Za-z0-9_]{3,32}`, case-insensitive uniqueness, and password of at least 8 characters. Verify integration tests that cover every scenario in `specs/user-auth`.

## 5. Chat and message API

- [x] 5.1 Implement `POST /api/chats` and `GET /api/chats/{id}` (fields include is_member, is_owner, can_post). Verify integration tests for creation, validation errors, and 404 for unknown IDs.
- [x] 5.2 Implement the join, leave and delete handlers: owner can't leave (409), non-owner can't delete (403), and delete removes the chat from the hub, sends `deleted` notices and enqueues the purge. Verify integration tests that cover every scenario in `specs/chat-management`.
- [x] 5.3 Implement `GET /api/home` (top lists from the snapshot, my_chats from the indexed join with unread computed from the hub). Verify an integration test with owned and joined chats and their unread values.
- [x] 5.4 Implement `POST /api/chats/{id}/messages` (permission rules, 1 to 4,000 characters after trimming, 201 after commit) and `GET /api/chats/{id}/messages`. Verify integration tests for every permission and content scenario, including a restart that keeps the message.
- [x] 5.5 Implement `POST /api/chats/{id}/read` (no going backwards, clamped to last_seq, ignored for non-members). Verify integration tests for the scenarios in `specs/messaging` "Read position" and "Unread count".
- [x] 5.6 Wire the router in `main.rs`: `/api` routes, ServeDir fallback with `index.html` at `/`, `Cache-Control: no-store` on the API, graceful shutdown that drains the writer queues. Verify `curl /` returns index.html and `curl /api/home` without a cookie returns 401.

## 6. WebSocket

- [x] 6.1 Implement the `/api/ws` upgrade with the auth check (401), the per-connection task that selects over socket, broadcast and control channel, and registration in the online index. Verify an integration test with tokio-tungstenite for an authenticated and an unauthenticated upgrade.
- [x] 6.2 Implement `open` and `close` (replacing the previous subscription, `error not_found` for a missing chat), live `msg` delivery, and Lagged turning into `resync`. Verify integration tests for switching chats, and that a slow reader gets `resync` while a fast reader gets every message.
- [x] 6.3 Deliver `seq` frames for owned and joined chats, including chats joined after connecting, and `deleted` frames for viewers and members. Verify integration tests for the batched-updates and deletion-notice scenarios in `specs/realtime-updates`.
- [x] 6.4 Close a connection whose control channel is full, and verify a test that the connection is closed while other connections keep working.

## 7. Web client

- [x] 7.1 Create `static/index.html` (auth, home and chat containers) and `static/app.css` (two-column layout, badges, forms). Verify the page loads at `/` with no console errors.
- [x] 7.2 Implement the `api()` wrapper, the hash router, and the auth views in `static/app.js` (sign-in, register with a client-side check that the passwords match, server errors shown). Verify by hand: register, logout and login all work, and a mismatched password sends no request (Network tab).
- [x] 7.3 Implement the home view (Top groups, Top channels, My chats with unread badges, New chat dialog). Verify by hand that creating a chat opens it and lists it in My chats.
- [x] 7.4 Implement the chat view: latest page, bottom anchoring, Join, Leave and Delete controls by role, input hidden when the user can't post, all rendering through `textContent`. Verify by hand that a non-owner sees no input in a channel, and that `<img src=x onerror=alert(1)>` displays literally.
- [x] 7.5 Implement scroll-back with an IntersectionObserver and scroll anchoring, and debounced mark-read while the chat is visible. Verify by hand with a chat of more than 150 messages that older pages load without the view jumping and the badge resets.
- [x] 7.6 Implement the WebSocket manager: backoff reconnect, reopening the current chat, `after=lastSeq` catch-up with de-duplication, `seq` updating badges, `deleted` returning home with a notice, `resync` refetching. Verify by hand that stopping and restarting the server while another tab posts shows the missed messages in order with no duplicates.

## 8. Integration checks

- [x] 8.1 Run an end-to-end test that starts the binary on a temp data directory, registers 3 users, and exercises the group and channel flows over HTTP and WebSocket. Verify `cargo test` passes.
- [x] 8.2 Run a load check (an ignored test or example binary): 1,000 WebSocket members in a group and 50 concurrent posters for 30 s. Record throughput and p99 post latency in the test output, and confirm there are no gaps in seq.
- [x] 8.3 Build the release binary and run it from a separate directory holding only the binary and `static/`. Verify `cargo build --release` succeeds and the app works from that layout, with `data/` created on first start.
- [x] 8.4 Write `README.md` covering run flags, the directory layout (binary, `static/`, `data/`), and the fixed shard count. Verify the commands it documents run as written.

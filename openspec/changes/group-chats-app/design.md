# Design

## Context

This is a greenfield project. The crate is an empty `src/main.rs` with edition 2024 and no dependencies. Motivation and scope are in proposal.md, and the behaviour contracts are in `specs/`. The constraints that shape this design are:

- It runs as a single process, so in-memory state is the source of truth for anything that only lives at runtime.
- Storage is SQLite only. SQLite allows one writer per file, so write parallelism comes from having more files.
- Group fan-out goes to up to 1,000 members and channel fan-out to up to 10,000.
- The deliverable is one binary plus a `static/` folder, with no frontend build step.

## Goals / Non-Goals

**Goals:**
- Writing a message costs one prepared INSERT inside a shared batch transaction. It never touches `meta.db`.
- The home page, unread counts and permission checks are served from memory, or from indexed point reads.
- One slow or busy chat or client can never delay other chats or clients.

**Non-Goals:**
- Running more than one process, replicating data, or changing the shard count online.
- Power-loss durability for the last few milliseconds of commits (see Risks).
- Full-text search, message edits and attachments.
- Per-user limits on how fast people can post. This is a deliberate choice: any member allowed to post can send as fast as they like.

## Decisions

### D1. Runtime stack
We use tokio (multi-thread), axum 0.8 with the `ws` feature, and tower-http `ServeDir` for `static/`, plus rusqlite with `bundled` and serde_json. Configuration comes from CLI flags and environment variables: `--addr` (default `127.0.0.1:8080`), `--data-dir` (default `./data`), `--static-dir` (default `<exe dir>/static`, falling back to `./static`), and `--shards` (default 32, used only when the database is first created).
*Alternatives:* actix-web, which needs a separate runtime model, and hyper used directly, which means more boilerplate. We rejected sqlx because it is async-first, and our design gives each database a dedicated writer thread, which needs a synchronous API.

### D2. Storage layout
```
data/meta.db
  config(key PK, value)                      -- shard_count, schema version
  users(id INTEGER PK, login TEXT, login_lc TEXT UNIQUE, pw_hash TEXT, created_at)
  sessions(token_hash BLOB PK, user_id, expires_at)   INDEX(user_id)
  chats(id INTEGER PK AUTOINCREMENT, kind, title, owner_id, member_count, created_at)
        INDEX(kind, member_count DESC)
  members(chat_id, user_id, joined_at, last_read_seq, PK(chat_id,user_id)) WITHOUT ROWID
        INDEX(user_id, chat_id)
data/msg-00.db .. msg-31.db
  messages(chat_id, seq, author_id, body, ts, PK(chat_id,seq)) WITHOUT ROWID
  chat_seq(chat_id PK, last_seq)
  purge_queue(chat_id PK)
```
A chat's shard is `chat_id % shard_count`. Chat IDs come from AUTOINCREMENT, so they are sequential and spread evenly across shards, and a deleted ID is never reused. The shard count is written to `config` the first time the app starts and read from there afterwards. If `--shards` disagrees with the stored value, we log a warning and ignore the flag. Each file has `user_version` set for future migrations.
Pragmas: `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=OFF` (integrity is kept by the application, because tables span files), `busy_timeout=5000`, and `cache_size` of about 8 MB per file.
*Alternatives:* one file per chat, rejected for unbounded fds and checkpoints. Members stored in shards, rejected because "my chats" would then have to query every shard.

### D3. One writer thread per database, with natural group commit
Each database file (`meta.db` and each shard) has a dedicated OS thread that owns the only writable connection. Requests reach it through a bounded `tokio::sync::mpsc` (capacity 4096), each carrying a `oneshot` for the reply.
The loop works like this. It does a `blocking_recv` for the first command, then drains up to 512 more with `try_recv`, runs them all in one `BEGIN IMMEDIATE ... COMMIT`, and only after the commit sends the replies and side effects.
There is no artificial delay. When the app is idle, a batch is one command and latency is a single commit. Under load, batches grow by themselves and the number of fsyncs per message falls.
If a command fails (a constraint error, for example), it is wrapped in a SAVEPOINT so the rest of the batch still commits. If the whole transaction fails, every command in the batch gets an error.
*Alternatives:* a pool of writers with busy retries, rejected because of lock contention and `SQLITE_BUSY` storms. A fixed batching timer, rejected because it adds latency when the app is idle.

### D4. Sequence numbers are assigned inside the shard writer
The shard writer keeps a `HashMap<chat_id, last_seq>`, loaded from `chat_seq` at startup. For each message in a batch it assigns `seq = last_seq + 1`. At the end of the batch it upserts `chat_seq` once for each chat that was touched, in the same transaction. If the transaction fails, the counters are rolled back.
Because assignment and commit happen on the one thread that owns the chat, seq has no gaps and stays in order, and broadcast order matches commit order.
*Alternatives:* assigning seq in the async hub before the write, rejected because a failed write would leave gaps.

### D5. ChatHub holds runtime state
`ChatHub` is an `Arc` shared through axum state. It holds:
- `chats: DashMap<chat_id, Arc<ChatState>>`. Each `ChatState` has `kind`, `title`, `owner_id`, an atomic `member_count`, an atomic `last_seq`, and an `OnceLock`-style lazily created `broadcast::Sender<Utf8Bytes>` (capacity 256).
- At startup, every chat row is loaded from `meta.db` and `last_seq` is loaded from each shard's `chat_seq`. This takes about 100 bytes per chat, so a million chats needs roughly 100 MB. A chat that is missing from `chats` is a 404.
- A user login cache, `DashMap<user_id, Arc<str>>`, filled lazily (logins never change). It is used to fill in `author.login` in history and live frames.
- Top 10 snapshots, refreshed by a task every 2 seconds from the `(kind, member_count DESC)` index and stored as pre-serialized JSON in an `ArcSwap`. This meets the spec's 5-second staleness limit.

### D6. Post path
1. Auth: the session is looked up in memory.
2. The chat is looked up in the hub. If missing, return 404.
3. Permission. For a channel, `owner_id == user`. For a group, a membership point read on the `meta.db` read pool (PK lookup).
4. The body is validated.
5. The message is sent to the shard writer, and the handler waits for the reply.
6. After the commit, the writer thread:
   - sets `ChatState.last_seq`,
   - serializes the `msg` frame once into `Utf8Bytes` and sends it on the chat's broadcast, if one exists,
   - marks the chat dirty for seq updates,
   - sends a fire-and-forget `MarkRead(author, chat, seq)` to the meta writer.
7. The handler responds 201.

A message is broadcast only after it is durable, which meets the spec's "respond once durably stored" rule and avoids "ghost" messages.

### D7. Membership operations on the meta writer
These run as meta-writer commands, so they are serialized and the member cap can't be exceeded:
- `Join`: `INSERT OR IGNORE` into members with `last_read_seq` set to the current `last_seq`. Only if a row was inserted, run `UPDATE chats SET member_count = member_count + 1 WHERE id = ? AND member_count < cap`. If that update changes no rows, roll back to the savepoint and return 409.
- `Leave`: the opposite. The owner check happens in the handler.
- `MarkRead`: `UPDATE members SET last_read_seq = MAX(last_read_seq, ?)`, with the value clamped first to the `last_seq` held in memory.
- `CreateChat`: inserts the chat and the owner's membership. The new ID is registered in the hub, and its shard's `chat_seq` starts empty (`last_seq` 0).

After commit, the hub's `member_count` is updated and the online-membership index (D8) is adjusted.

### D8. WebSocket fan-out: open chat vs. seq updates
Each connection is one task, which `select!`s over three sources:
- (a) incoming socket frames: `open` and `close`,
- (b) `broadcast::Receiver` for the open chat, if any,
- (c) its own bounded control `mpsc` (capacity 64) for `seq`, `deleted` and `error` frames.

Live messages use (b). Every frame is serialized once and shared by refcount (`Utf8Bytes`) among all receivers. `RecvError::Lagged` turns into a `resync` frame, which meets the slow-consumer requirement without any back-pressure on the sender.

Seq updates use an online-membership index:
- `online_users: DashMap<user_id, Vec<conn control senders>>`
- `chat_online: DashMap<chat_id, HashSet<user_id>>`

When a user's first connection arrives, their chat IDs are loaded with `members WHERE user_id = ?` on the indexed read and inserted. When their last connection closes, the entries are removed. Join and leave adjust the index for online users.

Shard writers add chats to a `dirty: Mutex<HashSet<chat_id>>`. A 1-second ticker swaps that set out, and for each dirty chat it goes through `chat_online[chat]`, building one `{chat: last_seq}` map per user. It then sends one `seq` frame to each of the user's connections.
If a control channel is full, the connection is closed. The client reconnects and refreshes `/api/home`, so no missed update is lost.
Cost per tick is O(online members of chats that changed), whatever the message rate.
*Alternatives:* sending every message to every member, rejected because a 10k-member channel would produce frames equal to the message rate times 10k. SSE, rejected because the HTTP/1.1 limit of 6 connections per origin breaks multi-tab use.

### D9. Chat deletion
The `meta.db` writer deletes the chat row and all its member rows in one transaction. Then:
- the hub removes the `ChatState`, so every endpoint immediately returns 404,
- `deleted` is sent on the chat's broadcast and to online members through the index,
- the shard writer gets a `Purge(chat_id)` command. This inserts the chat into `purge_queue` and then deletes messages in chunks of 5,000 seq values (`seq > a AND seq <= a + 5000`), one chunk per writer loop iteration, mixed in with normal batches. When it finishes, it removes the `purge_queue` and `chat_seq` rows.

At startup, any leftover `purge_queue` entries are resumed.

### D10. Read paths
Each database has a small pool of read-only connections (r2d2 + r2d2_sqlite, max 4 per file, `min_idle` 0 so idle shards hold no connections). Queries run in `spawn_blocking`.

History is a PK range scan: `WHERE chat_id = ? AND seq < ? ORDER BY seq DESC LIMIT ?`, or `seq > ? ORDER BY seq ASC` for `after`. The response includes `has_more`.

`/api/home` gets `my_chats` from one indexed `members JOIN chats` read, then adds `unread = hub.last_seq - last_read_seq` from memory. The top lists come from the snapshot.

### D11. Auth implementation
- **Password hashing:** argon2id with default parameters, run in `spawn_blocking` behind a `Semaphore` sized to the CPU count. This stops a login flood from tying up the blocking pool.
- **Session tokens:** 32 random bytes from `OsRng`, base64url-encoded in a cookie named `sid`. The database stores `sha256(token)`.
- **Session cache:** a `DashMap<token_hash, (user_id, expires_at)>`, read through from `meta.db`. Sliding expiry is exact in memory and written to `meta.db` at most once an hour per session, through the meta writer.
- **Login throttle:** an in-memory `DashMap<login_lc, (window_start, failures)>`, cleaned up periodically.
- **Timing:** a dummy hash is verified when the login is unknown, so response timing doesn't reveal which logins exist.
- **Login uniqueness:** the `login_lc` column is the UNIQUE key.

### D12. HTTP API conventions
JSON bodies are used throughout, and errors look like `{"error": "<message>"}` with the status codes listed in the specs. Every response has `Cache-Control: no-store` except static files.

Routes: `/api/register`, `/api/login`, `/api/logout`, `/api/me`, `/api/home`, `POST /api/chats`, `GET|DELETE /api/chats/{id}`, `POST /api/chats/{id}/join|leave|read`, `GET|POST /api/chats/{id}/messages`, `GET /api/ws`. Anything that isn't `/api` falls through to `ServeDir`, with `index.html` for `/`.

### D13. Client structure
`static/index.html` contains three view containers: auth, home and chat. `static/app.js` is a plain ES module with no dependencies. It contains:
- a tiny `api()` fetch wrapper,
- a hash router,
- the WebSocket manager, which reconnects with exponential backoff (0.5 s up to 10 s, with jitter),
- rendering through `textContent` and `createElement` only.

The chat view keeps `firstSeq` and `lastSeq`. An `IntersectionObserver` on a top sentinel triggers `before=firstSeq`, and the view anchors scroll position by `scrollHeight` difference. Mark-read is debounced to 1 second while the chat is open and the tab is visible.

`static/app.css` is hand-written with a two-column layout: a sidebar holding the home lists, and the chat pane.

### D14. Module layout
```
src/main.rs        config, startup, router, graceful shutdown
src/config.rs      CLI/env parsing
src/db/mod.rs      open/pragmas/migrations, pools
src/db/writer.rs   generic batched writer thread
src/db/meta.rs     meta schema + commands
src/db/shard.rs    shard schema + commands (seq, purge)
src/hub.rs         ChatHub, online index, dirty ticker, top-10 snapshot
src/auth.rs        hashing, sessions, extractor, throttle
src/api/*.rs       handlers: auth, chats, messages, home
src/ws.rs          connection task + protocol types
src/error.rs       ApiError -> response
```

## Risks / Trade-offs

- [`synchronous=NORMAL` in WAL can lose the last commits on power loss, though not on a process crash] → This is acceptable for chat, and it makes commits much cheaper. A `--sync full` flag can switch to `synchronous=FULL` later.
- [`meta.db` is a single writer for joins, read markers and session renewals] → All of these happen at human speed and are batched. Session renewal is throttled to once an hour and mark-read is debounced on the client. If needed later, read markers can move to the shards.
- [A hot chat is limited by a single shard writer] → Group commit gives tens of thousands of inserts per second per shard, far beyond what a 1,000-member group produces.
- [All chats are loaded into memory at startup] → This is bounded at about 100 bytes per chat, and startup time grows with the number of chats. If it becomes a problem, switch to lazy loading.
- [The membership check on every group post is a `meta.db` read] → It is a single PK lookup on a WAL read connection, taking microseconds. A per-chat member set can be cached later.
- [A full control channel closes the connection] → The client reconnects automatically and refreshes, so only a brief flicker is visible.
- [Changing the shard count needs an offline rehash] → This is documented as out of scope, and the startup warning makes it visible.
- [With no posting limit, one user can flood a chat] → Each chat's writer is bounded and commits in batches, slow viewers get `resync` instead of slowing everyone, and unread badges update at most once a second. A flood stays inside the one shard and chat it targets.

## Migration Plan

This is a new deployment, so there is nothing to migrate. On first start the binary creates `data/` and every schema at `user_version = 1`. Later schema changes will check `user_version` at startup and apply migrations in order. To roll back, stop the binary. The data files stay compatible as long as the version is unchanged.

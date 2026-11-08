# Chats

Group chats and broadcast channels: a single Rust binary (tokio + axum + SQLite) plus a folder of static files (plain HTML/CSS/JS).

- **Groups**: up to 1,000 members; any member can post.
- **Channels**: up to 10,000 members; only the creator can post.
- Anyone signed in can read any chat. Joining adds it to *My chats* with an unread counter.

## Build and run

```sh
cargo build --release
./target/release/chats-openspec            # http://127.0.0.1:8080
```

To deploy, copy the binary and the `static/` folder side by side:

```
dist/
  chats-openspec      the server
  static/             index.html, app.js, app.css
  data/               created on first start
```

```sh
mkdir -p dist && cp target/release/chats-openspec dist/ && cp -R static dist/
cd dist && ./chats-openspec --addr 0.0.0.0:8080
```

### Flags

| Flag | Env | Default |
|---|---|---|
| `--addr HOST:PORT` | `CHATS_ADDR` | `127.0.0.1:8080` |
| `--data-dir DIR` | `CHATS_DATA_DIR` | `./data` |
| `--static-dir DIR` | `CHATS_STATIC_DIR` | `static/` next to the binary, else `./static` |
| `--shards N` | `CHATS_SHARDS` | `32` (used only on first start) |

Flags win over environment variables. `Ctrl-C` shuts down gracefully: in-flight writes are committed before exit.

## Storage

```
data/meta.db              users, sessions, chats, memberships, read positions
data/msg-00.db ... NN.db  messages, sharded by chat id % N
```

Every file runs in WAL mode with one dedicated writer thread that commits queued work in batches, so writes to different shards proceed in parallel and a burst of messages costs a handful of fsyncs.

**The shard count is fixed when `data/` is first created.** It is stored in `meta.db`; a later `--shards` that disagrees is ignored with a warning. Changing it would require rehashing every message offline.

## API

All endpoints are under `/api` and use JSON; errors look like `{"error": "..."}`. Auth is a `sid` session cookie (`HttpOnly`, `SameSite=Strict`, 30-day sliding expiry).

| Method | Path | |
|---|---|---|
| POST | `/api/register`, `/api/login` | `{login, password}` |
| POST | `/api/logout` | |
| GET | `/api/me` | |
| GET | `/api/home` | `top_groups`, `top_channels`, `my_chats` (with `unread`) |
| POST | `/api/chats` | `{kind: "group" \| "channel", title}` |
| GET, DELETE | `/api/chats/{id}` | delete: owner only |
| POST | `/api/chats/{id}/join`, `/leave` | |
| GET | `/api/chats/{id}/messages?before=&after=&limit=` | newest first; `after` returns oldest first |
| POST | `/api/chats/{id}/messages` | `{body}` (1-4000 chars) |
| POST | `/api/chats/{id}/read` | `{seq}` |
| GET | `/api/ws` | WebSocket |

WebSocket frames are JSON with a `t` field. The client sends `{"t":"open","chat":ID}` / `{"t":"close"}`. The server sends `msg` (live messages for the open chat), `seq` (at most once a second: new `last_seq` of your chats, for unread badges), `deleted`, `resync` (you fell behind; refetch with `after=`), and `error`.

## Tests

```sh
cargo test                       # unit, HTTP, WebSocket and binary end-to-end tests
ulimit -n 8192 && cargo test --release --test load -- --ignored --nocapture
```

The load check fills a 1,000-member group, connects every member over WebSocket with the chat open, and runs 50 concurrent posters for 30 s (`LOAD_SECS`, `LOAD_MEMBERS`, `LOAD_POSTERS` override). It reports throughput and post latency and verifies the stored sequence has no gaps. On an Apple-silicon laptop, with the load generator in the same process, it measured about 1,100 messages/s (1.1 M delivered frames/s), p99 post latency of about 80 ms.

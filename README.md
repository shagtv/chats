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
| `--metrics` | `CHATS_METRICS=1` | off |

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

## Metrics

Start the server with `--metrics` to enable `GET /api/metrics`. It needs no session, so only enable it where that's acceptable (load testing, a private network). Without the flag the endpoint is a 404.

```sh
./target/release/chats-openspec --metrics &
curl -s http://127.0.0.1:8080/api/metrics
```

| Field | Meaning |
|---|---|
| `uptime_s` | seconds since start |
| `ws.connections`, `ws.online_users` | open WebSockets, distinct connected users |
| `ws.open_subscriptions` | connections with a chat open |
| `ws.resyncs_total`, `ws.dropped_slow_total` | slow-client events since start |
| `chats.groups`, `chats.channels` | existing chats by kind |
| `chats.max_members.{group,channel}` | largest `member_count` per kind |
| `chats.max_online.{group,channel}` | `{id, online}` of the chat with most online members, or `null` |
| `writers.meta` | `queue`, `commits_total`, `cmds_total` of the `meta.db` writer |
| `writers.shards` | `count`, `queue_max`, `queue_max_shard`, `queue_sum`, `commits_total`, `cmds_total` across message shards |
| `http.requests_total`, `http.errors_5xx_total` | `/api` responses since start |

`cmds_total / commits_total` is the average group-commit batch size. Polling reads only in-memory state and never writes to the database.

## Stress test

`chats-stress` (the `stress/` crate) finds this machine's limits. It starts the server with `--metrics` on a fresh data directory, seeds users and chats, and raises load step by step until a step misses a target:

| Target (per 45 s step) | Limit |
|---|---|
| delivery p99 (post until other viewers receive it) | 1 s |
| HTTP p99 | 500 ms |
| failed requests and connection attempts | 1% |
| clients sent `resync` | 1% |

Loads double until a step fails, then bisect to within 5%. Phases:

| Phase | What grows | Maximum it reports |
|---|---|---|
| A | online users with realistic behaviour (90% lurkers / 9% occasional / 1% heavy posters, Zipf-sized chats) | most online users |
| B1 | online viewers of one channel, owner posting 2 msg/s | most viewers in one channel (cap 10,000) |
| B2 | concurrent posters (1 msg/s each) in one fully online 1,000-member group | members / posters in one group |
| B3 | total message rate over 1,000 five-member groups | sustained messages per second |
| B4 | connected users doing nothing | idle connections |

```sh
cargo build --release --workspace
./target/release/chats-stress                  # full run, about 25 minutes
./target/release/chats-stress --only b1        # one phase
./target/release/chats-stress --smoke --step-secs 5 --warmup-secs 2   # quick check, about 2 minutes
```

| Flag | Default |
|---|---|
| `--server PATH` | `target/release/chats-openspec` |
| `--addr HOST:PORT` | `127.0.0.1:18080` |
| `--out DIR` | `stress-out/<timestamp>/` |
| `--only a\|b1\|b2\|b3\|b4` | all phases |
| `--gen-threads N` | 6 (the rest of the cores are left to the server) |
| `--step-secs N` / `--warmup-secs N` | 45 / 10 |
| `--seed N` | 42 |
| `--keep-data` | delete the server's data afterwards |
| `--smoke` | tiny loads, at most 2 steps per phase |

In a terminal it redraws a status panel every second (users, chat totals and largest chats, traffic, latencies against targets, queues, CPU, ports). When piped, it prints one line every 5 seconds. The output directory gets:

- `stress-report.md` / `stress-report.json`: a maximums table naming what limited each one, a run-validity section, and one row per step for every phase
- `stress-timeline.csv`: one row per second
- `server.log`

**Reading the report:** "limited by" names the failed target plus the likely bottleneck read from `/api/metrics` (a hot shard writer, the meta writer, WebSocket fan-out, or server CPU). If the generator itself was above 70% of its threads, the result says *load generator*, so it measures the tool, not the server. *Product cap* means the limit was reached without failing.

**Ports:** every WebSocket uses an ephemeral port, so connections from 127.0.0.1 stop at about 13,700. Each extra loopback alias adds about 14,700 more, and the tool detects them automatically:

```sh
sudo ifconfig lo0 alias 127.0.0.2 up    # remove with: sudo ifconfig lo0 -alias 127.0.0.2
```

## Tests

```sh
cargo test --workspace           # unit, HTTP, WebSocket, binary end-to-end and stress-tool tests
ulimit -n 8192 && cargo test --release --test load -- --ignored --nocapture
```

The load check fills a 1,000-member group, connects every member over WebSocket with the chat open, and runs 50 concurrent posters for 30 s (`LOAD_SECS`, `LOAD_MEMBERS`, `LOAD_POSTERS` override). It reports throughput and post latency and verifies the stored sequence has no gaps. On an Apple-silicon laptop, with the load generator in the same process, it measured about 1,100 messages/s (1.1 M delivered frames/s), p99 post latency of about 80 ms.

# Tasks

## 1. Server metrics

- [x] 1.1 Add `metrics: bool` to `Config` (`--metrics` flag, `CHATS_METRICS=1` env), and list it in `USAGE`. Verify config unit tests cover the default (off), the flag and the env override.
- [x] 1.2 Add `cmds_total` (an `AtomicU64` increased by batch length) and a `queue()` accessor (`max_capacity - capacity`) to `Writer`. Verify writer unit tests: 1,000 commands give `cmds_total == 1000`, and `queue()` is 0 when idle.
- [x] 1.3 Add the `open_subscriptions`, `resyncs_total` and `dropped_slow_total` counters to `Hub`, wired in `ws.rs` (open, close, disconnect, `Lagged`) and `send_control`. Verify the hub unit test for a full control queue also asserts `dropped_slow_total == 1`.
- [x] 1.4 Implement `Hub::chat_stats()`: group and channel counts, `max_members` per kind, and `max_online` per kind from `chat_online`. Verify a unit test seeding 2 groups (3 and 7 members), 1 channel (5), and online members, matching the server-metrics spec scenarios.
- [x] 1.5 Turn the `/api` `map_response` layer into a counting layer (`requests_total`, `errors_5xx_total`), and store an `Instant` for `uptime_s` in `App`. Verify a unit test of the layer with a handler that returns 500.
- [x] 1.6 Add `src/api/metrics.rs` with `GET /api/metrics`, routed only when `config.metrics` is true and needing no session. Pass the flag into `api::router`. Verify integration tests:
  - 404 without the flag
  - 200 without a cookie with the flag
  - the WebSocket counts follow two connections and an open chat, then drop on disconnect
  - posting 10 messages raises `shards.cmds_total` by at least 10 and `commits_total` by 1 to 10
  - deleting a group lowers `chats.groups`
- [x] 1.7 Add a check that metrics stay fast. Seed a hub with 100,000 chats, time `chat_stats()` plus serializing the body, and confirm no writer commit happens during polling. Verify an ignored unit test asserts under 50 ms in release mode, and the default test asserts `commits_total` doesn't change across 5 polls.
- [x] 1.8 Document `--metrics` and the `/api/metrics` fields in `README.md`, and verify the README's example `curl` command works against a server started with `--metrics`.

## 2. Workspace and stress crate skeleton

- [x] 2.1 Turn the root `Cargo.toml` into a workspace (`members = [".", "stress"]`), and create the `stress` crate (binary `chats-stress`) with tokio, reqwest (json), tokio-tungstenite, serde, serde_json, hdrhistogram and rand. Verify `cargo build --workspace` succeeds and `cargo tree -p chats-openspec` shows no stress dependencies.
- [x] 2.2 Implement CLI parsing: `--server`, `--addr`, `--out`, `--only`, `--gen-threads` (default 6), `--step-secs` (45), `--warmup-secs` (10), `--seed`, `--keep-data`, with `--help`. Verify unit tests for defaults, every flag, and rejecting an unknown phase.
- [x] 2.3 Implement server lifecycle in `stress/src/server.rs`:
  - refuse to start if the address is busy
  - spawn `--server` with `--metrics --data-dir <tmp>`, and poll `/api/metrics` until ready
  - stop it with SIGINT and wait for it to exit
  - delete the data dir unless `--keep-data` is given
  - warn on a debug build

  Verify an integration test that starts and stops the real server binary, and checks the busy-address refusal.
- [x] 2.4 Implement machine detection (`sysctl` on macOS, `/proc` on Linux, "unknown" otherwise) and process sampling: per-second CPU% from the difference in `ps -o time=,rss=` cumulative CPU time. Verify a unit test of the `ps` output parser, and a test that samples the current process.

## 3. Load-generation core

- [x] 3.1 Implement source-address probing (bind `127.0.0.1` to `127.0.0.16` on port 0), port capacity from `sysctl -n net.inet.ip.portrange.first/last` minus 10%, and round-robin assignment. Verify unit tests for the capacity maths and a probe test that finds 127.0.0.1.
- [x] 3.2 Implement the API client: one shared reqwest client with a cookie header per request, register/login/home/create/join/leave/history/post/read calls, and a per-endpoint latency histogram and error count for each. Verify an integration test against a real server that registers a user and runs every call.
- [x] 3.3 Implement the WebSocket client: bind a `TcpSocket` to the source address, then `client_async`. Decode frames into `{t, chat_id, seq, ts}`, record delivery latency `now_ms - ts` into the sharded histograms, and count resyncs, connection failures and disconnects. Verify an integration test where a posted message gives exactly one delivery sample per viewer.
- [x] 3.4 Implement the metrics store: 16 histogram shards merged per second and per window, counters, and a `Snapshot` struct that serves as both the panel input and a CSV row. Verify unit tests for merging, resetting at a window start, and p50/p99 calculation.
- [x] 3.5 Implement seeding: concurrent registration capped at 2 × cores, Zipf membership for the realistic phase (head made of channels, caps respected by drawing again), and the fixed layouts for B1-B4. Verify unit tests that, for a 10,000-user seed, no chat exceeds its cap and the top 1% of chats hold more than 30% of memberships.

## 4. Behaviour, ramp and phases

- [x] 4.1 Implement the realistic VU (`stress/src/user.rs`):
  - the home/pick/open/read/join/dwell/switch loop
  - the 70/25/5 chat choice and 80/20 kind split for new chats
  - history scroll-back about 30% of the time
  - the 90/9/1 posting profiles with seeded randomness

  Verify a unit test running the decision logic 100,000 times with a fixed seed, where each share is within 1 percentage point of its target.
- [x] 4.2 Implement step evaluation against the four targets, with the failure reason, plus early failure when errors exceed 10% in the first 10 s. Verify unit tests for each target failing alone, all passing, and early failure.
- [x] 4.3 Implement the adaptive ramp: double until the first failure, bisect to within 5% or 4 bisection steps, stop at a cap or at port capacity, and ramp down by disconnecting the newest VUs. Verify unit tests driving the controller with a fake pass/fail function: limits at 27,000, at a cap of 10,000, and at a port limit.
- [x] 4.4 Implement the phases A, B1, B2, B3 and B4 as `PhaseSpec`s with the D7 loads and rates, and `--only`. Verify a short smoke run (`--step-secs 5 --warmup-secs 2`, small starting loads) of each phase against the real server, where each phase completes at least one step.
- [x] 4.5 Implement the limiting-factor rules from D10. Verify unit tests for each rule, including the generator-bound rule taking precedence.

## 5. Output

- [x] 5.1 Implement the live panel (ANSI redraw every second, only when stdout is a terminal) and the 5-second plain line otherwise. Verify a unit test rendering a `Snapshot` to the panel and to the plain line, where the plain line contains no `ESC` bytes.
- [x] 5.2 Write `stress-timeline.csv` (header plus one row per second) to `--out`. Verify a test that a 5-second smoke run writes a header and 4 to 6 rows.
- [x] 5.3 Implement `Report` and write `stress-report.json` and `stress-report.md`: machine details, the maximums table with limiting factors, step tables per phase, and the `interrupted` flag. Verify a golden-file unit test of the Markdown rendering for a fixed `Report`, and a JSON round-trip test.
- [x] 5.4 Handle Ctrl-C: abandon the current step, stop the VUs and the server, and write the report with `interrupted: true`. Verify an integration test that sends SIGINT to `chats-stress` during a smoke run and checks the report exists and is marked interrupted.
- [x] 5.5 Document `chats-stress` in `README.md`: how to build it, flags, phases, output files, the loopback alias command, and how to read the report. Verify the documented smoke-run command works as written.

## 6. Integration checks

- [x] 6.1 Run `cargo test --workspace` and `cargo clippy --workspace --all-targets`, and verify both pass with no warnings.
- [ ] 6.2 Run a full `chats-stress` on this laptop with release builds. Verify it finishes in about 25 minutes (30 at most), and that `stress-report.md` has all five maximums with limiting factors and a validity section. Record the headline numbers in the README.

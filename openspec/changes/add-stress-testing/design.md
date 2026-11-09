# Design

## Context

The server is a single axum binary. Runtime state lives in `Hub`:
- the chat map
- the `online` and `chat_online` indexes
- per-chat broadcast senders

Writes go through `Writer<C>` threads, one per SQLite file, which already count commits. The only load check today is `tests/load.rs`, a fixed-size in-process test.

The load generator and the server will share one laptop: Apple M5 Max, 18 cores (6 performance and 12 efficiency), 48 GB RAM. The ephemeral port range is 49152-65535, so each loopback source address has about 16k ports. Motivation is in proposal.md, and behaviour contracts are in `specs/server-metrics`, `specs/stress-testing` and the `user-auth` delta.

## Goals / Non-Goals

**Goals:**
- Metrics that cost nothing when off, and stay read-only and in memory when on.
- A generator efficient enough that, on this laptop, the server saturates first. When it doesn't, the report says so.
- Results that can be compared between runs: fixed seeds, fixed step lengths, and the same report layout every time.

**Non-Goals:**
- A metrics format for Prometheus or other scrapers.
- Running the generator on another machine.
- Exact percentiles. HDR histograms at 3 significant figures are enough.

## Decisions

### D1. Workspace with a black-box stress crate
The root `Cargo.toml` gains `[workspace] members = [".", "stress"]`. The `stress` crate (binary `chats-stress`) does **not** depend on the server crate. It only talks HTTP and WebSocket, so it measures exactly what a browser would see, and the server binary gains no dependencies.

The stress crate's dependencies are tokio, reqwest (json), tokio-tungstenite, serde/serde_json, hdrhistogram and rand (with a seeded `StdRng`).

The tool runs the server from `--server PATH`, defaulting to `target/release/chats-openspec`. If the path looks like a debug build, it warns.

*Alternatives:* a test or example inside the server crate. Rejected because it pulls the generator's dependencies into server builds and invites using internals.

### D2. Metrics plumbing in the server
`Config` gains `metrics: bool`, set by `--metrics` or `CHATS_METRICS=1`. `api::router` adds `.route("/metrics", get(metrics::metrics))` only when it is set. Otherwise the existing fallback answers 404. The handler takes no `AuthUser`, which is what implements the `user-auth` exception.

| Field | Source |
|---|---|
| `ws.connections` / `online_users` | sum of `conns.len()` over `hub.online` / `hub.online.len()` |
| `ws.open_subscriptions` | new `AtomicI64` in `Hub`, incremented and decremented in `ws.rs` when `open` is set or cleared and on disconnect |
| `ws.resyncs_total` | new `AtomicU64`, incremented where `ws.rs` handles `Lagged` |
| `ws.dropped_slow_total` | new `AtomicU64`, increased by `stale.len()` in `Hub::send_control` |
| `chats.*` | one pass over `hub.chats` (count by kind, max `member_count`) and one over `hub.chat_online` (largest set per kind) |
| `writers.*.queue` | `tx.max_capacity() - tx.capacity()` on each `Writer` |
| `writers.*.cmds_total` | new `AtomicU64` per `Writer`, increased by batch length |
| `writers.*.commits_total` | existing `commits` |
| `http.*` | the existing `map_response(no_store)` layer becomes a counting layer: every `/api` response increments `requests_total`, and 5xx responses also increment `errors_5xx_total` |
| `uptime_s` | an `Instant` stored when `App` opens |

The two scans are O(chats) per poll. At one poll per second and 100k chats that's a few milliseconds of one core, well inside the 50 ms target, so no incremental bookkeeping is needed. Nothing in this path touches SQLite.

### D3. Virtual users are cheap tasks on a capped runtime
Each virtual user (VU) is one tokio task holding its session cookie, its current chat, and its WebSocket split into reader and writer halves.

The generator runs on a multi-thread runtime with `--gen-threads N` workers. The default is 6, one third of the cores, which leaves the rest to the server. "Cores available to it" in the run-validity requirement means these N threads, so the generator-bound threshold is 70% × N cores.

**HTTP:** one shared `reqwest::Client` for all VUs, with `pool_max_idle_per_host` at 512 and the cookie header set per request instead of a cookie store per VU. HTTP then uses hundreds of sockets in total rather than one per user, which leaves the ephemeral ports for WebSockets.

**WebSocket:**
- Each WebSocket is made with `tokio::net::TcpSocket`, bound to a chosen loopback source address, then `tokio_tungstenite::client_async`.
- At startup the tool probes source addresses `127.0.0.1` through `127.0.0.16` by trying to bind each to port 0. A bind that fails means the alias isn't configured.
- Capacity per address is the `net.inet.ip.portrange` size, read with an unprivileged `sysctl -n`, minus a 10% margin. Connections are spread round-robin across the addresses.

*Alternatives:* a Python or Locust generator. Rejected because it would saturate long before the server, which is the main thing this design avoids.

### D4. Measuring delivery latency without parsing bodies
Messages already carry the server `ts` in milliseconds, set when the shard writer applies the insert, which is just before commit. Generator and server share a clock, so delivery latency is `now_ms - ts` at frame receipt. That slightly overstates latency by including commit time, which is the conservative direction.

Frames are decoded into a struct with only `{t, chat_id, seq, ts}`. serde skips the body without allocating it.

Latencies are recorded into a small set of `Mutex<Histogram>` shards, chosen by `vu_id % 16`, to avoid contention. They are merged each second for the panel and at the end of each step window for evaluation.

Each HTTP endpoint (`home`, `history`, `post`, `join`, `create`, `ws_connect`) has its own histogram. The HTTP target uses the merged p99 across all of them.

### D5. One server per run; phases reuse it
Restarting and reseeding per phase would add minutes, so the tool starts one server on a fresh temporary data directory under `--out` and runs every phase against it. Between phases, all VUs disconnect. Offline users and their chats cost the server only memory, so earlier phases don't bias later ones. The data directory is deleted at the end unless `--keep-data` is given.

### D6. Seeding
Users are registered concurrently, with at most 2 × cores requests in flight, before any measured step. Because the server's argon2 hashing is the bottleneck, seeding 40k users takes about a minute.

The realistic phase seeds `users / 20` chats, with memberships drawn from a Zipf distribution (s ≈ 1.1). Each user joins 3 to 8 chats. The most popular ranks are made channels, so the Zipf head can exceed 1,000 members without hitting the group cap. Draws that would exceed a cap are drawn again.

Probe phases seed exactly what they need:
- **B1:** one channel with 10,000 members
- **B2:** one group with 1,000 members
- **B3:** many 5-member groups
- **B4:** users only

Joins go through the API, so they exercise the meta writer's batching.

### D7. Ramp controller
Each phase is described by `PhaseSpec { start, cap, unit, apply(load) }`:

| Phase | `apply` | start, cap |
|---|---|---|
| A | sets the number of online realistic VUs | 1,000, none |
| B1 | sets online viewers of the channel; the owner posts at 2 msg/s | 1,000, 10,000 |
| B2 | fills the group to 1,000 online members, then sets the number of posters (each 1 msg/s) | 10, 1,000 |
| B3 | sets the target total msg/s, spread over the small groups with paced posters | 500, none |
| B4 | sets the number of connected idle VUs | 2,000, none |

A step runs like this:
1. Apply the load.
2. Warm up until at least 95% of the target VUs are connected, plus 10 s.
3. Run the 45 s window (`--step-secs`), with histograms and counters reset at its start.
4. Evaluate the four targets from `specs/stress-testing`.

If more than 10% of requests fail in the first 10 s of a window, the step is failed at once instead of waiting it out.

**Next load:** double it until the first failure, then bisect `(last_pass + first_fail) / 2` until they are within 5% of each other or 4 bisection steps have run. Reaching the cap ends the phase with the cap as the limit. So does needing more connections than the ports allow, with the OS port limit as the limit.

Ramping down for a bisection step disconnects the newest VUs first.

### D8. Process sampling
Every second the tool runs `ps -o time=,rss= -p <server_pid>,<own_pid>`. It takes the difference in cumulative CPU time to get exact per-second CPU%. The `%cpu` column on macOS is a decaying average, so it is avoided.

Machine details come from:
- `sysctl -n machdep.cpu.brand_string hw.ncpu hw.memsize` on macOS
- `/proc/cpuinfo` and `/proc/meminfo` on Linux
- otherwise, "unknown"

### D9. Live panel and timeline
When `std::io::IsTerminal` is true, the panel is redrawn with plain ANSI codes (`ESC[H ESC[2J`, then fixed-width lines), so no TUI crate is needed. Otherwise the tool prints one line every 5 s.

Each second's sample is a serde struct, which serves both as a CSV row in `stress-timeline.csv` and as the panel's input. All output goes into `--out`, which defaults to `stress-out/<YYYYMMDD-HHMMSS>/`.

### D10. Limiting-factor rules
Each phase's maximum gets a limiting factor. The first rule that matches wins:
1. Generator CPU above 70% of `--gen-threads` cores during the failing window: **load generator**.
2. Stopped for port capacity: **OS ephemeral ports**, with the `sudo ifconfig lo0 alias 127.0.0.N up` command for the next free N.
3. Reached the cap while passing: **product cap**.
4. Otherwise, the failed target, plus a hint:
   - `writers.shards.queue_max` above 500, or growing for the whole window: **hot shard writer (shard #k)**
   - `writers.meta.queue` above 500: **meta writer**
   - the resync target failed: **WebSocket fan-out / slow consumers**
   - server CPU above 85% of all cores: **server CPU**
   - otherwise: **unknown**

### D11. Report
`Report { machine, config, interrupted, maximums: Vec<Max>, phases: Vec<PhaseResult{ steps: Vec<StepResult> }> }` is serialized straight to `stress-report.json`. `stress-report.md` is rendered from the same struct, with a heading, a maximums table and one step table per phase.

Ctrl-C sets a flag. The current step is abandoned (not recorded), the server is stopped, and the report is written with `interrupted: true`.

## Risks / Trade-offs

- [The generator and server share CPU, so absolute numbers are lower than on separate machines] → The worker threads are capped and generator saturation is labelled. The report gives the generator's thread count, so readers know the budget split.
- [Zipf seeding with caps can distort the distribution's head] → The head is made of channels (cap 10,000), and the realistic phase seeds chats in proportion to users, so the cap is rarely reached.
- [Delivery latency includes the time from `ts` to commit] → It overstates by roughly one commit, a few milliseconds. That's conservative, and recorded here.
- [Adaptive bisection can land on noise near the limit] → A fixed RNG seed (`--seed`) and fixed step lengths keep runs comparable. A rerun of one phase with `--only` confirms a borderline result.
- [Long runs leave about 1 GB of SQLite data] → Data lives in a temporary directory that is deleted after the run unless `--keep-data` is given.
- [Port probing by bind is macOS-specific in practice] → On Linux the loopback range is all usable, so the probe simply succeeds for every address.

## Migration Plan

The server change is additive and off by default. Existing deployments behave identically without `--metrics`. To roll back, drop the flag, or revert the change; there is no data migration.

## Open Questions

- What default posting rates should B1 and B2 use (2 msg/s for the channel owner, 1 msg/s per group poster)? They can be tuned after the first run without changing the specs or the task breakdown.

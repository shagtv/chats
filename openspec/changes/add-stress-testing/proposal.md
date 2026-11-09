# Proposal

## Why

We know the server handles a full 1,000-member group at a fixed load, but not where it actually breaks on this laptop: how many users can be online at once, how big one channel or group can get, or how many messages per second it can sustain. A repeatable stress tool that mimics real user behaviour, raises load until the service stops meeting its targets, and reports the maximums lets us measure that, and compare against it after future changes.

## What Changes

- Add a `--metrics` server flag, off by default. When it is on, `GET /api/metrics` returns a JSON snapshot of server internals without requiring a session:
  - WebSocket connections and online users
  - resync and slow-connection-drop totals
  - total groups and channels, the most members in a group and in a channel
  - the most online users in one group and in one channel
  - writer queue depths and commit/command totals
  - HTTP request and 5xx totals

  When the flag is off, the endpoint answers 404.
- Add a stress-test tool, `chats-stress`, as a separate crate in a new Cargo workspace. The server binary and its dependencies are unchanged.
- The tool starts the server with `--metrics` on a fresh data directory and seeds users and chats. It then runs virtual users that behave like real ones:
  - They sign in, load the home page, and pick chats from My chats, the top lists, or by creating new ones.
  - They read the latest messages and sometimes scroll back, join some chats, and switch or leave chats.
  - They post with realistic frequency: 90% never post, 9% occasionally, 1% heavily. Chat popularity is skewed, with a few large chats and many small ones.
- Load rises in steps until a step fails its targets. A step fails if any of these is true:
  - p99 delivery latency (post until other viewers receive it) is over 1 s
  - p99 HTTP latency is over 500 ms
  - more than 1% of requests or connections fail
  - more than 1% of online clients were told to resync
- The run has two phases, each of which can also run on its own:
  - **Phase A:** a realistic ramp that finds the most users who can be online at once.
  - **Phase B:** targeted probes for one channel (up to 10,000 viewers), one 1,000-member group (more and more posters), the sustained write ceiling, and idle connections.
- While running, the tool shows a live terminal panel, refreshed every second:
  - online, connecting and registered users
  - group and channel totals, the most members in a group and in a channel, the largest online chat of each kind
  - traffic rates and latencies, each checked against the targets
  - errors, resyncs and writer queues
  - server and tool CPU and memory, ports in use

  Without a terminal it prints one summary line every 5 seconds. Every second's numbers are also written to `stress-timeline.csv`.
- At the end it writes `stress-report.md` and `stress-report.json`. They include a table of maximums, saying what limited each one (a target, a product cap, the OS port limit, or the load generator itself), a row per step for every phase, and a run-validity section.
- A full run takes about 25 minutes.

## Capabilities

### New Capabilities

- `server-metrics`: the opt-in `GET /api/metrics` endpoint, its JSON fields and how the `--metrics` flag controls it.
- `stress-testing`: the stress tool's user behaviour, load ramp, pass/fail targets, phases, live progress output, report contents, and run-validity checks.

### Modified Capabilities

- `user-auth`: "Authentication required for chat API" gains one more exception: `GET /api/metrics` when metrics are enabled.

## Impact

- **Code:**
  - `src/config.rs` gains the `--metrics` flag.
  - New counters in `src/hub.rs`, `src/ws.rs`, `src/db/writer.rs` and the API layer.
  - A new metrics handler in `src/api/`.
  - A new `stress/` crate.
  - The root `Cargo.toml` becomes a workspace.
- **Dependencies:** none added to the server. The stress crate uses tokio, reqwest, tokio-tungstenite, serde_json, hdrhistogram and rand.
- **API:** adds `GET /api/metrics`, which exists only when the server runs with `--metrics`. No existing endpoint changes.
- **Operations:** running past about 16,000 connections needs extra loopback addresses, which take a one-time `sudo ifconfig lo0 alias` per address. The tool detects them and prints the command, but never runs `sudo` itself.
- **Out of scope:**
  - HTML reports or charts
  - running the load generator on another machine
  - testing over TLS
  - any server performance tuning

# Spec Delta

## Purpose

Exposes a JSON snapshot of the running server's internal state, such as connections, chat sizes, writer queues and error totals, so load tests and operators can see what the server is doing. It is available only when explicitly enabled.

## ADDED Requirements

### Requirement: Metrics are opt-in
The system SHALL serve `GET /api/metrics` only when the server was started with the `--metrics` flag (or `CHATS_METRICS=1`). Without the flag, the endpoint SHALL respond 404, the same as any unknown `/api` path.

#### Scenario: Disabled by default
- **WHEN** the server starts without `--metrics` and a client calls `GET /api/metrics`
- **THEN** the system responds 404

#### Scenario: Enabled by flag
- **WHEN** the server starts with `--metrics` and a client calls `GET /api/metrics` without a session cookie
- **THEN** the system responds 200 with a JSON body

### Requirement: Connection metrics
The metrics body SHALL include a `ws` object with:
- `connections`: open WebSocket connections
- `online_users`: distinct users with at least one connection
- `open_subscriptions`: connections that currently have a chat open
- `resyncs_total`: `resync` frames sent since start
- `dropped_slow_total`: connections closed for being too slow since start

#### Scenario: Counts follow connections
- **WHEN** one user opens two WebSocket connections and one of them opens a chat
- **THEN** `ws.connections` is 2, `ws.online_users` is 1, and `ws.open_subscriptions` is 1

#### Scenario: Counts drop on disconnect
- **WHEN** both of that user's connections close
- **THEN** `ws.connections` and `ws.online_users` return to their earlier values

#### Scenario: Resyncs are counted
- **WHEN** the server sends a `resync` frame to a lagging connection
- **THEN** `ws.resyncs_total` increases by 1

### Requirement: Chat metrics
The metrics body SHALL include a `chats` object with:
- `groups` and `channels`: the number of existing chats of each kind
- `max_members.group` and `max_members.channel`: the largest `member_count` of each kind, or 0 if none exist
- `max_online.group` and `max_online.channel`: either `{id, online}` for the chat of that kind with the most online members, or `null` when no member of any chat of that kind is online

#### Scenario: Totals and largest chats
- **WHEN** there are 2 groups with 3 and 7 members and 1 channel with 5 members
- **THEN** `chats.groups` is 2, `chats.channels` is 1, `chats.max_members.group` is 7, and `chats.max_members.channel` is 5

#### Scenario: Most online chat
- **WHEN** 4 members of channel 9 are connected, and fewer online members are in every other channel
- **THEN** `chats.max_online.channel` is `{"id": 9, "online": 4}`

#### Scenario: Deleted chats are not counted
- **WHEN** a group is deleted
- **THEN** `chats.groups` decreases by 1

### Requirement: Writer metrics
The metrics body SHALL include a `writers` object:
- `meta`: `{queue, commits_total, cmds_total}`
- `shards`: `{count, queue_max, queue_max_shard, queue_sum, commits_total, cmds_total}`, aggregated across all message shards

`queue` is the number of commands waiting for a writer. `cmds_total / commits_total` gives the average batch size.

#### Scenario: Commands and commits are counted
- **WHEN** 10 messages are posted in one chat
- **THEN** `writers.shards.cmds_total` increases by at least 10, and `writers.shards.commits_total` increases by at least 1 and at most 10

#### Scenario: Idle queues are empty
- **WHEN** no requests are in flight
- **THEN** `writers.meta.queue` and `writers.shards.queue_sum` are 0

### Requirement: HTTP metrics
The metrics body SHALL include an `http` object with `requests_total` (requests handled under `/api`) and `errors_5xx_total` (those answered with a 5xx status), both counted since start. It SHALL also include `uptime_s`.

#### Scenario: Requests are counted
- **WHEN** a client makes 3 requests to `/api/home`
- **THEN** `http.requests_total` increases by at least 3

### Requirement: Metrics are cheap
Serving `GET /api/metrics` SHALL NOT write to any database, and SHALL answer within 50 ms with up to 100,000 chats loaded, on the reference laptop.

#### Scenario: Read-only
- **WHEN** metrics are polled once a second during a run
- **THEN** no database commit is caused by the polling

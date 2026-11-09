# Spec Delta

## Purpose

Provides `chats-stress`, a tool that runs the chat server under realistic and targeted load on one machine, raises the load until the service stops meeting its performance targets, and reports the maximum capacity it found.

## ADDED Requirements

### Requirement: Self-contained run
`chats-stress` SHALL start the server binary itself with `--metrics` on a new, empty data directory, wait until it answers, and stop it with SIGINT when the run ends or is interrupted. It SHALL refuse to start if the server address is already in use.

#### Scenario: Clean start and stop
- **WHEN** the user runs `chats-stress` with no arguments
- **THEN** a server with metrics enabled is started on a new data directory, and when the run ends it is stopped and the tool exits

#### Scenario: Interrupted run
- **WHEN** the user presses Ctrl-C during a run
- **THEN** the tool stops all virtual users, stops the server, and writes a report covering the steps completed so far, marked as interrupted

### Requirement: Seeding before measurement
Before any measured step, the tool SHALL register every user the run needs and create an initial set of chats. Seeding time SHALL NOT count towards any step's measurements.

#### Scenario: Registration excluded
- **WHEN** a phase needs 20,000 users
- **THEN** all 20,000 are registered before the first measured step starts, and registration latency appears in no step's results

### Requirement: Realistic session loop
In the realistic phase, each online virtual user SHALL repeat a loop:
- load `/api/home` and keep a WebSocket connected
- pick a chat, open it over the WebSocket and fetch its latest page, scrolling back 1 to 3 older pages about 30% of the time
- join it about 10% of the time when not a member
- stay 1 to 5 minutes, then switch chats, leaving the old one about 20% of the time

#### Scenario: Reads dominate
- **WHEN** a realistic step runs with at least 5,000 online users
- **THEN** home and history reads outnumber posts

### Requirement: Realistic chat choice
When picking a chat, a virtual user SHALL choose from My chats about 70% of the time, from the top lists about 25%, and create a new chat about 5% of the time. New chats SHALL be about 80% groups and 20% channels.

#### Scenario: Actions follow the mix
- **WHEN** a realistic step runs with at least 5,000 online users
- **THEN** each choice's observed share is within 5 percentage points of its configured share

### Requirement: Realistic posting mix
Virtual users SHALL fall into three posting profiles:
- 90% lurkers, who never post
- 9% occasional posters, about one message per 2 minutes while in a chat they may post in
- 1% heavy posters, about one message per 10 seconds

Chat popularity SHALL follow a Zipf-like distribution: a few chats attract most users, and a long tail has few members each.

#### Scenario: Few users write
- **WHEN** a realistic step completes
- **THEN** no more than 11% of online users posted during it

#### Scenario: Skewed chat sizes
- **WHEN** the realistic phase has seeded its chats and users
- **THEN** the largest 1% of chats hold more than 30% of all memberships

### Requirement: Step-wise load with pass/fail
The tool SHALL raise load in steps. Each step is a warm-up that isn't measured, followed by a 45-second measured window. A step SHALL pass only if all of these hold during its window:
- p99 delivery latency (post accepted until each other online viewer of the chat receives the frame) is at most 1 s
- p99 HTTP latency is at most 500 ms
- at most 1% of requests and connection attempts fail
- at most 1% of online clients were sent `resync`

#### Scenario: Slow step fails
- **WHEN** a step's p99 delivery latency is 1.4 s and every other target is met
- **THEN** the step is marked failed, with delivery latency named as the reason

#### Scenario: Healthy step passes
- **WHEN** every target is met during a step's window
- **THEN** the step is marked passed

### Requirement: Adaptive ramp
Each phase's ramp SHALL double its load per step until a step fails, then bisect between the last passing and the first failing load until they are within 5% of each other or 4 bisection steps have run. The phase's maximum is its highest passing load.

#### Scenario: Converging on the limit
- **WHEN** 16,000 users pass and 32,000 fail
- **THEN** later steps try loads between 16,000 and 32,000, and the reported maximum is the highest load that passed

#### Scenario: Reaching a product cap
- **WHEN** a probe reaches a product limit (10,000 channel members or 1,000 group members) and still passes
- **THEN** the ramp stops there, and the report gives the cap as the limiting factor

### Requirement: Phases
The tool SHALL run five phases in order:
- **A:** a realistic ramp of online users
- **B1:** online viewers of one channel, up to 10,000, with its owner posting steadily
- **B2:** one group filled to 1,000 online members, with more and more concurrent posters
- **B3:** the total message rate across many small chats
- **B4:** connected users who don't do anything

#### Scenario: Full run order
- **WHEN** the user runs `chats-stress` with no phase selected
- **THEN** phases A, B1, B2, B3 and B4 run in that order

### Requirement: Single-phase runs
`chats-stress --only <phase>` SHALL run just the named phase (`a`, `b1`, `b2`, `b3` or `b4`).

#### Scenario: Single phase
- **WHEN** the user runs `chats-stress --only b1`
- **THEN** only the one-channel probe runs, and the report contains only its results

### Requirement: Live progress
While a terminal is attached, the tool SHALL redraw a status panel every second showing:
- the current phase, step and load
- online, connecting and registered users
- group and channel totals, the most members in a group and in a channel, and the largest online chat of each kind
- traffic rates
- latencies marked ok or FAIL against their targets
- error rates, resync rates and writer queues
- CPU, memory and port usage

#### Scenario: Interactive run
- **WHEN** the tool runs in a terminal
- **THEN** the panel updates every second with current online users and chat totals

### Requirement: Non-interactive progress
Without a terminal, the tool SHALL print one plain summary line every 5 seconds instead of the panel, with no terminal control codes.

#### Scenario: Piped output
- **WHEN** the tool's output is redirected to a file
- **THEN** it contains plain summary lines with no terminal control codes

### Requirement: Timeline file
The tool SHALL append one row per second to `stress-timeline.csv`, with a header row, containing the values shown in the live panel.

#### Scenario: Timeline written
- **WHEN** a run lasts 10 minutes
- **THEN** `stress-timeline.csv` has a header and about 600 data rows

### Requirement: Report files
At the end of a run, the tool SHALL write `stress-report.md` and `stress-report.json` with the same content. That content is the machine details (CPU model, cores, memory), a maximums table, and a step table for each phase.

#### Scenario: Report after a full run
- **WHEN** a full run completes
- **THEN** both files exist and contain the machine details, the maximums table and one step table per phase

### Requirement: Maximums table
The maximums table SHALL have one row for each measure, giving its value and what limited it. The measures are:
- most online users (A)
- most online viewers in one channel (B1)
- most online members and concurrent posters in one group (B2)
- highest sustained messages per second (B3)
- most idle connections (B4)

#### Scenario: Every measure reported
- **WHEN** a full run completes
- **THEN** the maximums table has five rows, each naming its limiting factor

### Requirement: Step tables
Each phase's step table SHALL have one row per step. Each row gives the load, rates, p50/p99 latencies, error and resync rates, server and tool CPU, and pass/fail with the reason.

#### Scenario: Failed step explained
- **WHEN** a step fails on HTTP latency
- **THEN** its row is marked failed, with HTTP latency named as the reason

### Requirement: Limiting factor attribution
For each maximum, the report SHALL name what stopped the ramp. It is one of:
- the failed target, with a hint from server metrics (a growing shard queue, the meta writer queue, resyncs, or server CPU)
- a product cap
- the OS ephemeral-port limit
- the load generator itself

#### Scenario: Shard queue named
- **WHEN** a step fails on delivery latency and the busiest shard's queue keeps growing during the window
- **THEN** the limiting factor reads as a failed delivery-latency target with a hot shard writer as the likely bottleneck

### Requirement: Generator-bound detection
If the tool's CPU use exceeds 70% of its available cores during a step that failed, the report SHALL label that maximum "limited by the load generator", not by the server.

#### Scenario: Generator-bound result
- **WHEN** a step fails while the tool's CPU use is above 70% of its cores
- **THEN** the report labels that maximum as limited by the load generator, not the server

### Requirement: Port capacity
At startup the tool SHALL detect usable loopback source addresses (127.0.0.1 and any aliases) and spread connections across them. It SHALL never run privileged commands. When port capacity runs out, the ramp stops, the limit is reported as the OS port limit, and the report prints the command that adds a loopback alias.

#### Scenario: Port limit reached
- **WHEN** only 127.0.0.1 is available and the next step would need more connections than its ephemeral ports allow
- **THEN** the ramp stops, the report gives the OS port limit as the cause, and it includes the `sudo ifconfig lo0 alias` command to raise it

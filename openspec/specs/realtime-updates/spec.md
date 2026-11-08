# realtime-updates Specification

## Purpose

Defines the WebSocket protocol. The open chat receives live messages as they are posted, a user's chats receive unread updates, deletions are announced, and a client that falls behind can recover.

## Requirements

### Requirement: WebSocket endpoint
The system SHALL provide a WebSocket at `GET /api/ws` for signed-in users. Every frame is a JSON text message with a `t` (type) field. A user may have several connections open at once, for example one per browser tab.

#### Scenario: Authenticated upgrade
- **WHEN** a client with a valid session cookie opens `/api/ws`
- **THEN** the connection is accepted

#### Scenario: Unauthenticated upgrade
- **WHEN** a client without a valid session opens `/api/ws`
- **THEN** the upgrade is rejected with 401

### Requirement: Open chat subscription
The system SHALL let a client set its single open chat with `{"t":"open","chat":<id>}` and clear it with `{"t":"close"}`. A client need not be a member to open a chat. Opening a new chat replaces the previous one.

#### Scenario: Switching chats
- **WHEN** a client opens chat 1 and then opens chat 2
- **THEN** it receives live messages for chat 2 only

#### Scenario: Opening a missing chat
- **WHEN** a client opens a chat ID that does not exist
- **THEN** the server sends `{"t":"error","code":"not_found","chat":<id>}`, and the client has no open chat

### Requirement: Live message delivery
The system SHALL send `{"t":"msg", ...message}` to every connection that has the chat open as soon as the message is accepted. Messages are delivered in seq order.

#### Scenario: Message reaches open viewers
- **WHEN** a member posts in a chat that 3 other connections have open
- **THEN** each of those connections receives a `msg` frame with the new seq

### Requirement: Batched sequence updates
For each connection, the system SHALL send at most one `{"t":"seq","chats":{"<id>":<last_seq>,...}}` frame per second. The frame lists every chat the user owns or has joined whose `last_seq` changed since the last frame. It does not depend on which chat is open.

#### Scenario: Busy channel is batched
- **WHEN** a channel posts 20 messages within one second, and a member is connected but does not have it open
- **THEN** that connection receives at most one `seq` frame for that second, carrying the latest `last_seq`

#### Scenario: Membership changes apply to live connections
- **WHEN** a connected user joins a chat, and then a message is posted there
- **THEN** that user's connections receive `seq` updates for the chat without reconnecting

### Requirement: Chat deletion notice
The system SHALL send `{"t":"deleted","chat":<id>}` to every connection that has the deleted chat open, or that belongs to one of its members.

#### Scenario: Viewer is notified
- **WHEN** the owner deletes a chat while another user has it open
- **THEN** that user's connection receives a `deleted` frame for the chat

### Requirement: Slow consumer handling
When a connection cannot keep up with live messages, the system SHALL drop its pending live messages for the open chat and send `{"t":"resync","chat":<id>}`. It SHALL NOT slow down delivery to other connections. The client recovers using the history endpoint with `after=seq`.

#### Scenario: Lagging client
- **WHEN** a connection's outbound buffer for its open chat overflows
- **THEN** it receives a `resync` frame, and other connections keep receiving messages without delay

### Requirement: Reconnect recovery
The protocol SHALL let a client that reconnects recover what it missed, using its last known seq for each chat together with the history and home endpoints. The server does not need to replay anything.

#### Scenario: Reconnect after network loss
- **WHEN** a client with chat 5 open at seq 80 reconnects and chat 5's `last_seq` is now 85
- **THEN** fetching `after=80` returns seq 81 to 85, and live frames continue from seq 86

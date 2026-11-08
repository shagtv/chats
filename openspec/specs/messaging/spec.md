# messaging Specification

## Purpose

Defines how text messages are posted, numbered, stored and paged through, and how each member's read position and unread count are tracked.

## Requirements

### Requirement: Post permission
The system SHALL accept a new message in a group only from a member of that group, and in a channel only from its owner.

#### Scenario: Member posts in group
- **WHEN** a group member posts a message
- **THEN** the system accepts it

#### Scenario: Non-member posts in group
- **WHEN** a user who is not a member posts in a group
- **THEN** the system responds 403 and stores nothing

#### Scenario: Non-owner posts in channel
- **WHEN** a channel member who is not the owner posts in the channel
- **THEN** the system responds 403 and stores nothing

### Requirement: Message content
The system SHALL accept message bodies of 1 to 4,000 characters after trimming. Bodies are stored and returned as plain text, exactly as submitted.

#### Scenario: Empty or oversized message
- **WHEN** a member posts a whitespace-only body, or a body longer than 4,000 characters
- **THEN** the system responds 400 and stores nothing

#### Scenario: Markup is not interpreted
- **WHEN** a member posts `<b>hi</b>`
- **THEN** the stored and returned body is the literal string `<b>hi</b>`

### Requirement: Post message endpoint
The system SHALL accept messages via `POST /api/chats/{id}/messages` with `{body}`. It responds once the message is durably stored, returning the message `{chat_id, seq, author: {id, login}, body, ts}`.

#### Scenario: Message is durable on success
- **WHEN** the post endpoint responds 201 and the server process restarts afterwards
- **THEN** the message is still returned by the history endpoint

### Requirement: Per-chat sequence numbers
The system SHALL give each message a sequence number `seq` that starts at 1 in each chat and goes up by exactly 1 with each message, with no gaps or duplicates, including across restarts.

#### Scenario: Consecutive numbering
- **WHEN** three messages are posted in a new chat
- **THEN** they have seq 1, 2 and 3, and the chat's `last_seq` is 3

#### Scenario: Numbering survives restart
- **WHEN** a chat has `last_seq` 41, the server restarts, and a new message is posted
- **THEN** the new message has seq 42

### Requirement: Paginated history
The system SHALL return messages from `GET /api/chats/{id}/messages`, newest first, with `limit` (default 50, max 100). An optional `before=seq` returns only messages with a lower seq. An optional `after=seq` returns messages with a higher seq, oldest first, for catching up.

#### Scenario: Latest page
- **WHEN** a client requests messages without `before`, for a chat with 120 messages
- **THEN** it receives seq 120 down to 71

#### Scenario: Older page
- **WHEN** a client requests `before=71`
- **THEN** it receives seq 70 down to 21

#### Scenario: Beginning of history
- **WHEN** a client requests `before=5`
- **THEN** it receives seq 4 down to 1, and the response shows there are no more messages

#### Scenario: Catch up after a gap
- **WHEN** a client requests `after=100` for a chat whose `last_seq` is 104
- **THEN** it receives seq 101 to 104, oldest first

### Requirement: Read position
The system SHALL keep a read position (`last_read_seq`) for each member of each chat. `POST /api/chats/{id}/read` with `{seq}` sets it to the given seq, but it never moves backwards and never goes past the chat's `last_seq`.

#### Scenario: Marking read
- **WHEN** a member with `last_read_seq` 30 marks seq 42 as read in a chat whose `last_seq` is 42
- **THEN** their `last_read_seq` becomes 42

#### Scenario: Read position never goes backwards
- **WHEN** a member with `last_read_seq` 42 marks seq 10 as read
- **THEN** their `last_read_seq` stays 42

#### Scenario: Non-member mark-read is ignored
- **WHEN** a non-member calls mark-read
- **THEN** the system responds 204 and records nothing

### Requirement: Unread count
The system SHALL compute each member's unread count for a chat as `last_seq - last_read_seq`. A member's own message advances their read position to that message's seq.

#### Scenario: Others' messages are unread
- **WHEN** a member's `last_read_seq` is 40, and others post 3 messages so `last_seq` is 43
- **THEN** that member's unread count is 3

#### Scenario: Own messages are not unread
- **WHEN** a member with unread count 0 posts a message
- **THEN** their unread count stays 0

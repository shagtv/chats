# web-client Specification

## Purpose

Defines the single-page browser client served at `/`: plain HTML, CSS and JavaScript loaded from the static folder. It covers signing in, the home lists, creating chats, and reading and writing in a chat.

## Requirements

### Requirement: Single page delivery
The system SHALL serve the client at `/` from a `static/` directory next to the binary, with no build step and no third-party runtime libraries. Navigation inside the app SHALL use the URL hash (for example `#/chat/12`) and SHALL NOT reload the page.

#### Scenario: Root serves the app
- **WHEN** a browser requests `/`
- **THEN** it receives `index.html`, which loads the app's JS and CSS from the static directory

#### Scenario: Deep link
- **WHEN** a signed-in user opens `/#/chat/12`
- **THEN** the app opens chat 12 directly

### Requirement: Auth views
The client SHALL show a sign-in form (login, password) when there is no valid session. A Register button SHALL switch to a form with login, password and repeat password. Registration SHALL NOT be submitted while the two passwords differ.

#### Scenario: Not signed in
- **WHEN** the page loads and `GET /api/me` returns 401
- **THEN** the sign-in form is shown

#### Scenario: Passwords mismatch
- **WHEN** a visitor enters different values in password and repeat password, then submits
- **THEN** the client shows an error and sends no request

#### Scenario: Server error shown
- **WHEN** login or registration fails (for example 401, 409 or 429)
- **THEN** the client shows the server's error message next to the form

### Requirement: Home view
When signed in, the client SHALL show three sections: Top groups, Top channels, and My chats. Each entry shows the title and member count. Entries in My chats also show an unread badge when the unread count is above zero. A New chat button SHALL let the user choose a kind and title and create the chat.

#### Scenario: Home sections
- **WHEN** a signed-in user is on the home view
- **THEN** the three sections are filled in from `GET /api/home`

#### Scenario: Live unread badge
- **WHEN** a `seq` frame raises a My chats entry's `last_seq` above the user's read position
- **THEN** the badge updates to the new unread count without reloading

#### Scenario: Create chat
- **WHEN** a user creates a group titled `rust`
- **THEN** the new chat opens and appears in My chats

### Requirement: Chat view
Opening a chat SHALL show its title, kind and member count, and its latest messages scrolled to the bottom. For members, it SHALL reset the unread badge to zero. Messages arriving while the chat is open SHALL be appended live and marked read.

#### Scenario: Opening resets unread
- **WHEN** a member opens a chat with 7 unread messages
- **THEN** the latest messages are shown, and the badge for that chat becomes 0

#### Scenario: Live append
- **WHEN** another user posts in the open chat
- **THEN** the message appears at the bottom without reloading

### Requirement: Scroll-back history
The chat view SHALL load the next older page of messages when the user scrolls to the top, keeping the visible messages in place, until the start of the history is reached.

#### Scenario: Load older
- **WHEN** the user scrolls to the top of a chat that has older messages
- **THEN** older messages are added above, and the messages that were visible stay where they were

### Requirement: Membership controls
The chat view SHALL show Join to non-members, Leave to non-owner members, and Delete (after confirmation) to the owner. It SHALL show the message input only when the user may post.

#### Scenario: Channel viewer
- **WHEN** a non-owner opens a channel
- **THEN** no message input is shown, and Join or Leave is shown depending on membership

#### Scenario: Chat deleted while open
- **WHEN** the client receives a `deleted` frame for the open chat
- **THEN** it returns to the home view, tells the user the chat was deleted, and removes the chat from its lists

### Requirement: Safe rendering
The client SHALL render every user-provided text (logins, titles, message bodies) as plain text, never as HTML.

#### Scenario: Markup in a message
- **WHEN** a message body contains `<img src=x onerror=alert(1)>`
- **THEN** it is displayed literally, and no script runs

### Requirement: Automatic reconnect
The client SHALL reconnect the WebSocket with backoff after it disconnects. After reconnecting it SHALL reopen the current chat, catch up with `after=<last seq>`, and refresh the home data.

#### Scenario: Recover after drop
- **WHEN** the connection drops, and 3 messages are posted in the open chat before it reconnects
- **THEN** after reconnecting, those 3 messages are shown in order with no duplicates

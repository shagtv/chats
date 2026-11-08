# Spec Delta

## Purpose

Defines the chat kinds (group and channel), how chats are created, read, joined, left and deleted, the member caps, and the lists shown on the home page.

## ADDED Requirements

### Requirement: Chat kinds
The system SHALL support two chat kinds. A `group` allows up to 1,000 members, and any member may post. A `channel` allows up to 10,000 members, and only its owner may post.

#### Scenario: Kind is reported
- **WHEN** a client fetches a chat with `GET /api/chats/{id}`
- **THEN** the response includes `kind` (`group` or `channel`), `title`, `owner` (`{id, login}`), `member_count`, `last_seq`, and the caller's `is_member`, `is_owner` and `can_post`

### Requirement: Create chat
The system SHALL let a signed-in user create a chat with `POST /api/chats` and `{kind, title}`. The title MUST be 1 to 100 characters after trimming, and titles need not be unique. The creator becomes the owner and the first member.

#### Scenario: Create a group
- **WHEN** a user posts `{kind: "group", title: "rust"}`
- **THEN** the system responds 201 with the new chat, where `member_count` is 1 and `is_owner` is true

#### Scenario: Invalid title or kind
- **WHEN** a user posts an empty or whitespace-only title, a title longer than 100 characters, or a kind other than `group` or `channel`
- **THEN** the system responds 400 and creates no chat

### Requirement: Open read access
The system SHALL let any signed-in user view any existing chat and its message history, whether or not they are a member.

#### Scenario: Non-member reads history
- **WHEN** a user who is not a member requests `GET /api/chats/{id}/messages`
- **THEN** the system responds 200 with the messages

#### Scenario: Unknown or deleted chat
- **WHEN** a user requests a chat ID that does not exist or was deleted
- **THEN** the system responds 404

### Requirement: Join chat
The system SHALL let a signed-in non-member join a chat with `POST /api/chats/{id}/join`. A join increments `member_count` by exactly 1. Joining when already a member succeeds without changing the count.

#### Scenario: Join adds membership
- **WHEN** a non-member joins a group that has 10 members
- **THEN** the group has 11 members, and the chat appears in the user's My chats

#### Scenario: Join is idempotent
- **WHEN** a member calls join again
- **THEN** the system responds 200 and `member_count` is unchanged

#### Scenario: New member starts with nothing unread
- **WHEN** a user joins a chat whose `last_seq` is 500
- **THEN** the user's read position for that chat is 500, so their unread count is 0

### Requirement: Member cap
The system SHALL reject a join that would push a group above 1,000 members or a channel above 10,000 members. This SHALL hold even when many joins arrive at the same time.

#### Scenario: Full group
- **WHEN** a non-member tries to join a group that has 1,000 members
- **THEN** the system responds 409 and `member_count` stays 1,000

#### Scenario: Concurrent joins at the cap
- **WHEN** 5 users join at the same time a channel that has 9,998 members
- **THEN** exactly 2 joins succeed, and `member_count` is 10,000

### Requirement: Leave chat
The system SHALL let a non-owner member leave a chat with `POST /api/chats/{id}/leave`. Leaving decrements `member_count` by 1 and removes the chat from the user's My chats. The owner MUST NOT be able to leave.

#### Scenario: Member leaves
- **WHEN** a non-owner member of a 12-member group leaves
- **THEN** the group has 11 members, and it is no longer in the user's My chats

#### Scenario: Owner cannot leave
- **WHEN** the owner calls leave on their own chat
- **THEN** the system responds 409 and membership is unchanged

#### Scenario: Leaving when not a member
- **WHEN** a non-member calls leave
- **THEN** the system responds 200 and `member_count` is unchanged

### Requirement: Delete chat
The system SHALL let only a chat's owner delete it with `DELETE /api/chats/{id}`. A deleted chat MUST disappear from every listing and every endpoint right away, together with its memberships and messages.

#### Scenario: Owner deletes
- **WHEN** the owner deletes their chat
- **THEN** the system responds 204, and later requests for that chat or its messages return 404

#### Scenario: Non-owner cannot delete
- **WHEN** a user who is not the owner calls delete
- **THEN** the system responds 403 and the chat still exists

### Requirement: Home lists
The system SHALL return the home page data from `GET /api/home`. It returns `top_groups` (up to 10 groups by `member_count`, highest first), `top_channels` (the same for channels), and `my_chats` (every chat the user owns or has joined, with `unread`).

#### Scenario: Top lists ordering
- **WHEN** 15 groups exist with distinct member counts
- **THEN** `top_groups` contains the 10 groups with the most members, highest first, and contains no channels

#### Scenario: My chats includes owned and joined
- **WHEN** a user owns 1 channel and has joined 2 groups
- **THEN** `my_chats` contains those 3 chats, each with `kind`, `title`, `member_count`, `is_owner`, `last_seq` and `unread`

#### Scenario: Top lists may be slightly stale
- **WHEN** member counts change
- **THEN** `top_groups` and `top_channels` reflect the change within 5 seconds

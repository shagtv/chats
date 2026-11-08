# user-auth Specification

## Purpose

Lets people create an account with a login and password, sign in and out, and be identified on every API request and WebSocket connection through a session cookie.

## Requirements

### Requirement: Registration
The system SHALL let a visitor create an account with `POST /api/register` and a login and password. The login MUST be 3 to 32 characters from `[A-Za-z0-9_]` and unique, ignoring case. The password MUST be at least 8 characters. When registration succeeds, the user is signed in.

#### Scenario: Successful registration
- **WHEN** a visitor submits login `alice` and an 8+ character password and no user `alice` exists in any letter case
- **THEN** the system responds 201 with `{id, login}` and sets a session cookie

#### Scenario: Login already taken
- **WHEN** a visitor registers with login `Alice` and the user `alice` already exists
- **THEN** the system responds 409 and does not create a user

#### Scenario: Invalid input
- **WHEN** a visitor registers with a login shorter than 3 characters, a login with characters outside `[A-Za-z0-9_]`, or a password shorter than 8 characters
- **THEN** the system responds 400 with an error message naming the invalid field

### Requirement: Password storage
The system SHALL store passwords only as salted argon2id hashes and SHALL never return a password or hash in any response.

#### Scenario: Stored credential is a hash
- **WHEN** a user registers with password `correct horse`
- **THEN** the stored credential is an argon2id hash string, and the plaintext password appears nowhere in storage

### Requirement: Login
The system SHALL sign a user in with `POST /api/login` when given a login (case-insensitive) and the correct password, and set a session cookie.

#### Scenario: Correct credentials
- **WHEN** a user submits their login and correct password
- **THEN** the system responds 200 with `{id, login}` and sets a session cookie

#### Scenario: Wrong credentials
- **WHEN** a user submits a login that does not exist, or a wrong password
- **THEN** the system responds 401 with the same generic error in both cases

### Requirement: Login throttling
The system SHALL limit repeated failed login attempts for each login to 5 failures per minute.

#### Scenario: Too many failures
- **WHEN** a login has failed 5 times within the last minute and another attempt is made
- **THEN** the system responds 429 without checking the password

### Requirement: Session cookie
The system SHALL identify sessions with a random token of at least 128 bits. The token is sent in an `HttpOnly`, `SameSite=Strict` cookie and stored server-side only as a hash. A session expires 30 days after it was last used.

#### Scenario: Session expiry is renewed by use
- **WHEN** an authenticated request is made with a valid session
- **THEN** the session's expiry moves to 30 days from now

#### Scenario: Expired session
- **WHEN** a request presents a session token that expired more than 30 days after its last use
- **THEN** the request is treated as unauthenticated

### Requirement: Logout
The system SHALL end the current session with `POST /api/logout` and clear the cookie.

#### Scenario: Logout invalidates the token
- **WHEN** a signed-in user calls `POST /api/logout` and later reuses the old token
- **THEN** the later request is treated as unauthenticated

### Requirement: Current user
The system SHALL return the signed-in user from `GET /api/me`.

#### Scenario: Signed in
- **WHEN** a request with a valid session calls `GET /api/me`
- **THEN** the system responds 200 with `{id, login}`

#### Scenario: Not signed in
- **WHEN** a request without a valid session calls `GET /api/me`
- **THEN** the system responds 401

### Requirement: Authentication required for chat API
The system SHALL reject requests to every `/api` endpoint, and WebSocket upgrades, that lack a valid session. The only exceptions are register and login.

#### Scenario: Unauthenticated chat request
- **WHEN** a request without a valid session calls `GET /api/home`
- **THEN** the system responds 401

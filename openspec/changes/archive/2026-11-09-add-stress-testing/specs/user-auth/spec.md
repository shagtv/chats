# Spec Delta

## MODIFIED Requirements

### Requirement: Authentication required for chat API
The system SHALL reject requests to every `/api` endpoint, and WebSocket upgrades, that lack a valid session. The only exceptions are register, login, and `GET /api/metrics` when the server runs with metrics enabled.

#### Scenario: Unauthenticated chat request
- **WHEN** a request without a valid session calls `GET /api/home`
- **THEN** the system responds 401

#### Scenario: Metrics need no session when enabled
- **WHEN** the server runs with `--metrics` and a request without a session calls `GET /api/metrics`
- **THEN** the system responds 200

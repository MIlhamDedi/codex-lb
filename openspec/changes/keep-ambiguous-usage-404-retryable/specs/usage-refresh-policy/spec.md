# usage-refresh-policy Delta

## MODIFIED Requirements

### Requirement: Usage refresh cools down repeated auth-like failures

Background usage refresh MUST apply a cooldown to accounts that fail usage
refresh with ambiguous `401`, `403`, or `404` responses. Accounts in that
cooldown window MUST be skipped until the cooldown expires or a later
successful refresh clears it.

#### Scenario: Zero-capacity monthly primary does not keep free accounts rate-limited

- **GIVEN** a free-plan account whose persisted status is `rate_limited`
- **AND** its latest primary usage row is a zero-capacity non-5h window (for
  example a monthly upstream snapshot)
- **AND** its normalized quota state reports available monthly quota
- **WHEN** codex-lb derives account status for account summaries or proxy
  runtime state
- **THEN** the non-5h primary row is ignored for rate-limit recovery
- **AND** the account is treated as `active`
- **AND** downstream account views keep the monthly-only quota presentation

#### Scenario: Ambiguous usage 404 enters cooldown without deactivation

- **GIVEN** an active account
- **WHEN** usage refresh receives HTTP `404`
- **AND** the upstream response has no known permanent error code
- **AND** the upstream message does not explicitly identify an account-level
  deactivation
- **THEN** the account remains active
- **AND** subsequent refresh cycles skip the account until the cooldown expires
- **AND** refresh resumes after the cooldown so later endpoint or quota recovery
  can be observed

### Requirement: Usage refresh deactivates on clear deactivation signals

The system MUST deactivate accounts when usage refresh receives a permanent
account deactivation signal. Credential or session invalidation codes MUST be
marked `reauth_required` according to the existing permanent-failure mapping.
A bare HTTP `404` without a known permanent error code or explicit account
deactivation message MUST NOT be treated as a permanent account signal.

#### Scenario: Usage 401 app session terminated requires re-authentication

- **WHEN** usage refresh receives HTTP `401`
- **AND** the upstream error code is `app_session_terminated`
- **THEN** the account is marked `reauth_required`
- **AND** later usage refresh cycles skip that account until re-authentication

#### Scenario: Usage 404 with an explicit deactivation code deactivates the account

- **WHEN** usage refresh receives HTTP `404`
- **AND** the upstream error code is `account_deactivated`
- **THEN** the account is marked `deactivated`
- **AND** later usage refresh cycles skip that account

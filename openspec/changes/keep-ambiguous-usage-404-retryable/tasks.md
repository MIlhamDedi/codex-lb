## 1. Usage refresh classification

- [x] 1.1 Stop treating a bare usage HTTP `404` as a permanent account-level
  deactivation signal.
- [x] 1.2 Include ambiguous usage HTTP `404` failures in the existing refresh
  cooldown.
- [x] 1.3 Preserve deactivation for explicit permanent error codes and
  deactivation messages carried by a `404` response.

## 2. Verification

- [x] 2.1 Add regression coverage proving a bare `404` leaves the account
  active and suppresses repeated refresh attempts during the cooldown.
- [x] 2.2 Add coverage proving an explicit `account_deactivated` code still
  deactivates when returned with HTTP `404`.
- [x] 2.3 Run targeted unit tests, Ruff, type checks, and strict validation for
  the change and affected OpenSpec capability.

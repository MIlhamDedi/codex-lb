## Why

A bare `404` from the upstream usage endpoint is currently interpreted as
proof that the account itself was permanently deactivated. That moves the
account into the terminal `deactivated` state, removes it from routing, and
stops later usage refreshes. If the `404` was endpoint-specific or transient,
the account can no longer recover automatically after the endpoint or quota
window recovers.

The response status alone does not distinguish a missing usage route from a
deleted account. Permanent upstream error codes and explicit deactivation
messages already provide the stronger signal needed for a terminal transition.

## What Changes

- Treat a usage `404` without a known permanent code or explicit account
  deactivation message as an ambiguous refresh failure.
- Apply the existing usage-refresh cooldown to ambiguous `404` responses so
  the scheduler does not hammer the upstream endpoint.
- Preserve terminal handling for `402`, known permanent error codes, and
  explicit account-deactivation messages, including when they arrive with a
  `404` status.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `usage-refresh-policy`: distinguish ambiguous usage-route `404` responses
  from explicit permanent account failures.

## Impact

No new setting, dependency, schema change, API field, or dashboard change.
Accounts remain eligible for routing after an ambiguous usage `404`, and the
background scheduler retries them after the existing cooldown expires.

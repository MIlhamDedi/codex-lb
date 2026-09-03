# Ambiguous usage 404 handling

## Purpose

Usage refresh should remove an account from service only when upstream sends
account-level evidence that the account is permanently unavailable. A status
code that can also describe the usage route itself is not enough evidence for
a terminal account transition.

## Decision

A bare usage `404` follows the same temporary cooldown path as ambiguous usage
authentication failures. The existing permanent-code and message classifiers
still run first, so `account_deactivated`, `account_suspended`,
`account_deleted`, or an explicit account-deactivation message remains a hard
failure regardless of the HTTP status.

This reuses the existing cooldown and setting rather than adding another
operator control. The cooldown is process-local and deliberately temporary;
after it expires, normal usage polling resumes and can observe quota reset or
endpoint recovery.

## Example

An otherwise active account receives `HTTP 404` with no error code and the
generic message `Usage fetch failed (404)`. codex-lb leaves the account active,
waits for the configured usage-refresh failure cooldown, and tries the usage
endpoint again. If a later response succeeds, normal snapshots and automatic
quota recovery continue. If the response instead contains
`code=account_deactivated`, codex-lb immediately transitions the account to
`deactivated`.

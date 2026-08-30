# Tasks

## 1. Lock Discipline

- [x] 1.1 Add `_http_bridge_fair_share_threshold_pct` (lock-free snapshot resolve) and a `fair_share_threshold_pct` parameter to `_ensure_http_bridge_session_stream_lease_locked`; keep the inline resolve only as a fallback for lock-free callers.
- [x] 1.2 Resolve the snapshot before both `pending_lock` acquisitions in `_submit_http_bridge_request` and pass it through, so no settings/DB await runs under the lock.

## 2. Statement Bound

- [x] 2.1 Add the fixed `_POSTGRES_COMMAND_TIMEOUT_SECONDS` application constant and set asyncpg `command_timeout` in `_postgres_async_connect_args` (PostgreSQL only; Alembic's synchronous engine unaffected).

## 3. Regression Coverage

- [x] 3.1 Reacquire with a provided snapshot never awaits the settings cache and forwards the threshold to lease acquisition.
- [x] 3.2 Product path: a stalled settings-cache refresh stalls the submit BEFORE `pending_lock` — the lock stays acquirable — and cancelling the stalled submit leaves no admission-waiter or lease residue. Sabotage-verified: fails against the old in-lock resolve.
- [x] 3.3 Engine connect-args tests assert the `command_timeout` bound alongside the UTC pin.

## 4. Verification

- [x] 4.1 Run the idle-leases and db-session unit suites plus lint (ruff) and type check (ty) on touched files.
- [x] 4.2 Strict OpenSpec validation.

# Tasks

## 1. Defer-Cancellation Wait Conversion

- [x] 1.1 Guard the http-bridge `_await_task_deferring_cancellation` with `anyio.CancelScope(shield=True)` and route its wait through `wait_on_shared_future`, covering all ~18 call sites including the shared per-session `resource_close_task` waiters.
- [x] 1.2 Apply the same guard+wait conversion to the retry-module copy (already shield-guarded; wait converted), the compact deferred-health flush loop, and `db/session.py::_shielded_bounded` (deadline-preserving `asyncio.wait` drain).
- [x] 1.3 Convert the SSE keepalive injector's per-tick wait and the timed wait on the shared http-bridge `resource_close_task` to `wait_on_shared_future`.

## 2. Regression Coverage

- [x] 2.1 Test that a level-cancelled anyio scope neither busy-spins the defer-cancellation wait nor grows the owned task's callback list, and that the owned task finishes before cancellation is surfaced.
- [x] 2.2 Test that repeated edge `task.cancel()` deliveries keep the owned task's callback count bounded while deferring and then surfacing the cancellation.
- [x] 2.3 Test that owned-task cancellation and exceptions propagate unchanged.
- [x] 2.4 Test that quiet-upstream keepalive ticks keep the pending chunk task's callback count bounded while keepalive frames are emitted and the next chunk still arrives.
- [x] 2.5 Test that `_shielded_bounded` honors its deadline under repeated cancels, keeps callbacks bounded, and still reports wedged vs finished teardown.
- [x] 2.6 Sabotage-verify: restore the unguarded helper and record the level-cancel regression test failing.

## 3. Verification

- [x] 3.1 Run the new regression file plus the shared-future, SSE, db-session, and defer/cleanup/compact-related proxy unit tests.
- [x] 3.2 Run ruff on all touched files and strict OpenSpec validation.

# codex-lb Core Ownership/Continuity/Timeout Model

This directory contains a TLC-checked TLA+ model of the core concurrency
protocol distilled from `TAXONOMY.md` and `taxonomy.csv`.

The model boundary is intentionally small: three replicas, two accounts, and
two client turns. Durable DB rows are the only truth; local caches are modeled
as versioned snapshots. Payloads and token counts are omitted. Continuity
anchors are represented only by provenance class:

- `client_anchor`
- `proxy_full_resend_anchor`
- `proxy_delta_anchor`

## Files

- `CoreOwnership.tla` - raw TLA+ model of account ownership, reservations,
  bridge/websocket turn lifecycle, anchors, owner epochs, gate waiters,
  shutdown drain, and freshness versions.
- `CoreOwnership.cfg` - full model configuration. Deadlock checking is enabled
  by `check.sh`.
- `weak-*.cfg` - negative controls that set one weakening flag at a time.
- `check.sh` - downloads pinned `tla2tools.jar` v1.7.4 into this directory if
  absent, runs the full model, then runs all weakenings and verifies that they
  fail with TLC counterexamples.

## Checked Invariants

1. `Inv1AnchorCurrent`: anchor use requires current owner epoch, compatible
   lineage, and safe provenance.
2. `Inv2DeadlineOrdering`: connect, first-byte, gate, and request deadlines
   remain bounded and ordered under the original request deadline.
3. `Inv3ReservationSettledExactlyOnce`: every acquired reservation settles on
   every terminal path, including cancellation.
4. `Inv4FreshSnapshots`: routing cannot use a local snapshot behind durable
   freshness evidence.
5. `Inv5SingleOwnerCAS`: singleton/account work mutates under a single durable
   owner epoch.
6. `Inv6TerminalIsolation`: terminal or cancelled producers cannot enqueue into
   later turns, and terminal reason is present.
7. `Inv7GateAccounting`: every gate waiter is exactly queued, holding, or
   terminal and keeps the inherited deadline.
8. `Inv8ShutdownDrain`: draining forbids admission of new externally visible
   work and shutdown completion requires no registered work.

The full configuration also checks natural liveness properties:

- `TurnTermination`: every admitted turn eventually reaches a terminal state.
- `ShutdownEventuallyComplete`: shutdown drain eventually completes.

## Negative Controls

Each weakening is a TLC constant value for `Weakening`. The full model uses
`Weakening = "none"`.

| Config | Disabled guard | Expected violation | Taxonomy class | Exemplar SHAs |
| --- | --- | --- | --- | --- |
| `weak-ignore-owner-epoch.cfg` | Allows continuity anchor reuse without current owner epoch/provenance fencing. | `Inv1AnchorCurrent` | Stale continuity anchor and owner mapping | `85802e64`, `48f083ef`, `4c04e538`, `b1d27bc6` |
| `weak-single-shared-timeout.cfg` | Collapses phase-specific deadlines into a single mismatched timeout. | `Inv2DeadlineOrdering` | Timeout budget mismatch and stuck streams | `aa65e97d`, `de2c5fc0`, `af5051f8` |
| `weak-skip-release-on-cancel.cfg` | Lets cancellation bypass reservation release/finalization. | `Inv3ReservationSettledExactlyOnce` | Lease and reservation leaks | `592d47b3`, `015f669e`, `783665b9` |
| `weak-stale-cache.cfg` | Lets a replica route from a local snapshot behind durable invalidation. | `Inv4FreshSnapshots` | Cache and quota freshness races | `04d8fab8`, `7347745b`, `b7bf87cf` |
| `weak-non-atomic-claim.cfg` | Lets a second turn claim an account without durable compare-and-set exclusion. | `Inv5SingleOwnerCAS` | Cross-replica single-owner coordination | `0a7f354d`, `b5f0541a`, `53f7b463`, `a8e12f8` |
| `weak-lost-waiter.cfg` | Lets cancellation drop a queued waiter slot or misroute a terminal producer. | `Inv7GateAccounting` or `Inv6TerminalIsolation` | Admission gate and lock contention; cancellation contamination | `87fae430`, `03b77781`, `c9da4974` |
| `weak-shutdown-admit.cfg` | Allows new externally visible turns after drain starts. | `Inv8ShutdownDrain` | Shutdown drain and background task lifecycle | `66b9196d`, `ec36ef60`, `3bdc9dea` |

Run:

```sh
bash spec/check.sh
```

Expected result: the full model completes with zero violations and no deadlock;
all weakening runs fail with TLC counterexamples.

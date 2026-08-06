# codex-lb Core Ownership/Continuity/Timeout Model

This directory contains a TLC-checked TLA+ model of the core concurrency
protocol distilled from `spec/evidence/TAXONOMY.md` and
`spec/evidence/taxonomy.csv`.

The model boundary is intentionally small: two replicas, one account, and two
client turns. Durable DB rows are the only truth; local caches are modeled as
versioned snapshots. Payloads and token counts are omitted. Continuity anchors
are represented only by provenance class:

- `client_anchor`
- `proxy_full_resend_anchor`
- `proxy_delta_anchor`

## Files

- `CoreOwnership.tla` - raw TLA+ model of account ownership, reservations,
  bridge/websocket turn lifecycle, anchors, owner epochs, gate waiters,
  snapshot freshness, completed-delivery finalizer ownership, shutdown drain,
  and bounded deadline expiry.
- `CoreOwnership.cfg` - full model configuration. `check.sh` runs TLC without
  the `-deadlock` opt-out, so TLC deadlock checking stays enabled.
- `weak-*.cfg` - negative controls that set one weakening flag at a time.
- `check.sh` - verifies the pinned `tla2tools.jar` sha256, downloads through a
  temporary file before replacing the cache, runs the full model, then runs all
  weakenings and requires each one to fail with its mapped invariant name.
- `evidence/TAXONOMY.md` and `evidence/taxonomy.csv` - taxonomy inputs used to
  classify the historical bug exemplars.

## Checked Invariants

| Invariant | Requirement | Demonstrating action / weakening |
| --- | --- | --- |
| `Inv1AnchorCurrent` | Anchor use requires current owner epoch, compatible lineage, and safe provenance. | `UseAnchor` records `badAnchorUse`; `weak-ignore-owner-epoch.cfg` demonstrates stale anchor reuse can violate it. |
| `Inv2DeadlineOrdering` | Connect, first-byte, gate, and request deadlines remain ordered under the original request deadline. | `QueueTurn` assigns phase deadlines; `weak-single-shared-timeout.cfg` demonstrates a shared timeout can violate ordering. |
| `Inv3ReservationSettledExactlyOnce` | Every acquired terminal turn has exactly one settlement event and a settled reservation state. | `CompleteTurn`, `CancelTurn`, `ExpireDeadline`, and `FinalizeCompletedDelivery` increment `settlementCount`; `weak-skip-release-on-cancel.cfg`, `weak-double-settle.cfg`, and `weak-popped-not-finalized.cfg` demonstrate zero, double, and lost-finalizer failures. |
| `Inv4FreshSnapshots` | Routing cannot use a local snapshot behind durable freshness evidence. | Normal `RouteFromSnapshot` consumes a fresh snapshot; `weak-stale-cache.cfg` removes the freshness guard. |
| `Inv5SingleOwnerCAS` | Singleton/account work mutates under a single durable owner epoch. | `AcquireTurn` enforces empty durable ownership and no live turn on the same account; `weak-non-atomic-claim.cfg` demonstrates duplicate live owners. |
| `Inv6TerminalIsolation` | Terminal or cancelled producers cannot enqueue into later turns, and terminal reason is present. | `MisrouteProducer` is disabled in the full model; `weak-lost-waiter.cfg` demonstrates terminal producer contamination. |
| `Inv7GateAccounting` | Every gate waiter is exactly queued, holding, or terminal and keeps the inherited deadline. | `QueueTurn`, `AcquireTurn`, and terminal actions preserve the gate lattice; `weak-lost-waiter.cfg` demonstrates a dropped queued waiter. |
| `Inv8ShutdownDrain` | Draining forbids admission of new externally visible work and shutdown completion requires no registered work. | `QueueTurn` is gated by `CanAdmit`; `weak-shutdown-admit.cfg` demonstrates post-drain admission. |
| `Inv9TerminalOwnerReleased` | Every acquired terminal turn releases the durable owner slot and finalizer owner. | Terminal actions call epoch-fenced release; `weak-leak-owner-on-terminal.cfg` demonstrates a leaked owner lease. |

The full configuration also checks natural liveness properties:

- `TurnTermination`: every admitted turn eventually reaches a terminal state.
  This is derived from bounded `Tick`, `ExpireDeadline`, and
  `FinalizeCompletedDelivery` fairness, not from fairness on cancellation.
- `ShutdownEventuallyComplete`: committed shutdown drain eventually completes
  once registered work has left.

## Negative Controls

Each weakening is a TLC constant value for `Weakening`. The full model uses
`Weakening = "none"`. `check.sh` treats a passing weakening, a missing
counterexample trace, or a counterexample against the wrong invariant as a
failure.

| Config | Disabled guard | Expected violation | Taxonomy class | Exemplar SHAs |
| --- | --- | --- | --- | --- |
| `weak-ignore-owner-epoch.cfg` | Allows continuity anchor reuse without current owner epoch/provenance fencing. | `Inv1AnchorCurrent` | Stale continuity anchor and owner mapping | `85802e64`, `48f083ef`, `4c04e538`, `b1d27bc6` |
| `weak-single-shared-timeout.cfg` | Collapses phase-specific deadlines into a single mismatched timeout. | `Inv2DeadlineOrdering` | Timeout budget mismatch and stuck streams | `aa65e97d`, `de2c5fc0`, `af5051f8` |
| `weak-skip-release-on-cancel.cfg` | Lets cancellation bypass reservation release/finalization. | `Inv3ReservationSettledExactlyOnce` | Lease and reservation leaks | `592d47b3`, `015f669e`, `783665b9` |
| `weak-double-settle.cfg` | Allows a terminal acquired turn to settle twice. | `Inv3ReservationSettledExactlyOnce` | Duplicate finalization and replayed settlement | `592d47b3`, `015f669e`, `783665b9` |
| `weak-stale-cache.cfg` | Lets a replica route from a local snapshot behind durable invalidation. | `Inv4FreshSnapshots` | Cache and quota freshness races | `04d8fab8`, `7347745b`, `b7bf87cf` |
| `weak-non-atomic-claim.cfg` | Lets a second turn claim an account without durable compare-and-set exclusion. | `Inv5SingleOwnerCAS` | Cross-replica single-owner coordination | `0a7f354d`, `b5f0541a`, `53f7b463`, `a8e12f8` |
| `weak-lost-waiter.cfg` | Lets cancellation drop a queued waiter slot or misroute a terminal producer. | `Inv6TerminalIsolation` or `Inv7GateAccounting` | Admission gate and lock contention; cancellation contamination | `87fae430`, `03b77781`, `c9da4974` |
| `weak-shutdown-admit.cfg` | Allows new externally visible turns after drain starts. | `Inv8ShutdownDrain` | Shutdown drain and background task lifecycle | `66b9196d`, `ec36ef60`, `3bdc9dea` |
| `weak-leak-owner-on-terminal.cfg` | Lets terminal completion/cancel/timeout leave the durable owner slot assigned. | `Inv9TerminalOwnerReleased` | Lease and reservation leaks | `592d47b3`, `015f669e`, `783665b9` |
| `weak-popped-not-finalized.cfg` | Models `response.completed` being popped from pending before the finalizer owns cleanup, then aborting. | `Inv3ReservationSettledExactlyOnce` | HTTP bridge completed-event cleanup ownership loss | `1594`, `778c533f`, `592d47b3` |

## Conformance Gap Modeled

The model now includes `completed_delivery_claimed`, `poppedFromPending`,
`completedDeliveryClaimed`, `finalizerOwner`, and `finalizerAborted`. The full
model requires the claimed completed delivery to be finalized before becoming
terminal; `weak-popped-not-finalized.cfg` permits the abort transition that the
conformance review confirmed from the implementation and proves it violates
`Inv3ReservationSettledExactlyOnce`.

## Running

```sh
bash spec/check.sh
```

Expected result: the full model completes with zero violations and deadlock
checking enabled; all weakening runs fail with their mapped invariant names.
The script prints the distinct-state count for the full model and every
weakening so the checked state-space size is visible.

## Future Work

The conformance review also recommends modeling separate account reservations,
stream leases, durable bridge claim/renew/release states, client-visible
delivery state, excluded-account failover, stale reservation repair, and
reversible operator drain distinct from committed shutdown. Those dimensions
remain outside this tractable core model.

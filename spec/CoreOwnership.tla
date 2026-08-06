----------------------------- MODULE CoreOwnership -----------------------------
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Replicas, Accounts, Turns, Weakening

NoReplica == "no_replica"
NoAccount == "no_account"
NoEpoch == 0

WeakIgnoreOwnerEpoch == Weakening = "ignore_owner_epoch"
WeakSingleTimeout == Weakening = "single_shared_timeout"
WeakSkipReleaseOnCancel == Weakening = "skip_release_on_cancel"
WeakNonAtomicClaim == Weakening = "non_atomic_claim"
WeakStaleCache == Weakening = "stale_cache"
WeakLostWaiter == Weakening = "lost_waiter"
WeakShutdownAdmit == Weakening = "shutdown_admit"

TerminalStates == {"completed", "cancelled", "failed", "retryable_owner_loss"}
NonTerminalStates == {"new", "queued", "active", "streaming"}
ReservationStates == {"none", "held", "released", "finalized", "transferred"}
GateStates == {"none", "queued", "holding", "terminal"}
AnchorKinds == {"none", "client_anchor", "proxy_full_resend_anchor", "proxy_delta_anchor"}
SafeRecoveryKinds == {"client_anchor", "proxy_full_resend_anchor"}
Reasons == {"none", "completed", "cancelled", "timeout", "owner_loss"}

VARIABLES
  owner,
  ownerEpoch,
  turnState,
  turnReplica,
  turnAccount,
  turnEpoch,
  reservation,
  gate,
  gateDeadline,
  requestDeadline,
  connectDeadline,
  firstByteDeadline,
  anchor,
  anchorUsed,
  badAnchorUse,
  durableVersion,
  snapshotVersion,
  routedWithStaleSnapshot,
  producerTarget,
  terminalReason,
  shutdownPhase,
  registered,
  ownerReleased,
  admittedDuringDrain

vars == << owner, ownerEpoch, turnState, turnReplica, turnAccount, turnEpoch,
  reservation, gate, gateDeadline, requestDeadline, connectDeadline,
  firstByteDeadline, anchor, anchorUsed, badAnchorUse, durableVersion, snapshotVersion,
  routedWithStaleSnapshot, producerTarget, terminalReason, shutdownPhase,
  registered, ownerReleased, admittedDuringDrain >>

Init ==
  /\ owner = [a \in Accounts |-> NoReplica]
  /\ ownerEpoch = [a \in Accounts |-> NoEpoch]
  /\ turnState = [t \in Turns |-> "new"]
  /\ turnReplica = [t \in Turns |-> NoReplica]
  /\ turnAccount = [t \in Turns |-> NoAccount]
  /\ turnEpoch = [t \in Turns |-> NoEpoch]
  /\ reservation = [t \in Turns |-> "none"]
  /\ gate = [t \in Turns |-> "none"]
  /\ gateDeadline = [t \in Turns |-> 0]
  /\ requestDeadline = [t \in Turns |-> 0]
  /\ connectDeadline = [t \in Turns |-> 0]
  /\ firstByteDeadline = [t \in Turns |-> 0]
  /\ anchor = [t \in Turns |->
      [kind |-> "none", account |-> NoAccount, epoch |-> NoEpoch, lineageOk |-> TRUE]]
  /\ anchorUsed = [t \in Turns |-> FALSE]
  /\ badAnchorUse = FALSE
  /\ durableVersion = [a \in Accounts |-> 0]
  /\ snapshotVersion = [r \in Replicas |-> [a \in Accounts |-> 0]]
  /\ routedWithStaleSnapshot = [t \in Turns |-> FALSE]
  /\ producerTarget = [t \in Turns |-> t]
  /\ terminalReason = [t \in Turns |-> "none"]
  /\ shutdownPhase = "running"
  /\ registered = [t \in Turns |-> FALSE]
  /\ ownerReleased = [t \in Turns |-> FALSE]
  /\ admittedDuringDrain = FALSE

LiveOnAccount(a) ==
  {t \in Turns : turnState[t] \in {"active", "streaming"} /\ turnAccount[t] = a}

CanAdmit ==
  (shutdownPhase = "running" \/ WeakShutdownAdmit)

QueueTurn(t) ==
  /\ turnState[t] = "new"
  /\ CanAdmit
  /\ turnState' = [turnState EXCEPT ![t] = "queued"]
  /\ gate' = [gate EXCEPT ![t] = "queued"]
  /\ gateDeadline' = [gateDeadline EXCEPT ![t] = IF WeakSingleTimeout THEN 4 ELSE 3]
  /\ requestDeadline' = [requestDeadline EXCEPT ![t] = 3]
  /\ connectDeadline' = [connectDeadline EXCEPT ![t] = IF WeakSingleTimeout THEN 3 ELSE 1]
  /\ firstByteDeadline' = [firstByteDeadline EXCEPT ![t] = IF WeakSingleTimeout THEN 2 ELSE 2]
  /\ registered' = [registered EXCEPT ![t] = TRUE]
  /\ admittedDuringDrain' = (admittedDuringDrain \/ shutdownPhase # "running")
  /\ UNCHANGED << owner, ownerEpoch, turnReplica, turnAccount, turnEpoch,
    reservation, anchor, anchorUsed, badAnchorUse, durableVersion, snapshotVersion,
    routedWithStaleSnapshot, producerTarget, terminalReason, shutdownPhase,
    ownerReleased >>

AcquireTurn(t, r, a) ==
  /\ turnState[t] = "queued"
  /\ r \in Replicas
  /\ a \in Accounts
  /\ (owner[a] = NoReplica \/ WeakNonAtomicClaim)
  /\ (WeakNonAtomicClaim \/ LiveOnAccount(a) = {})
  /\ owner' = [owner EXCEPT ![a] = r]
  /\ ownerEpoch' = [ownerEpoch EXCEPT ![a] = @ + 1]
  /\ turnState' = [turnState EXCEPT ![t] = "active"]
  /\ turnReplica' = [turnReplica EXCEPT ![t] = r]
  /\ turnAccount' = [turnAccount EXCEPT ![t] = a]
  /\ turnEpoch' = [turnEpoch EXCEPT ![t] = ownerEpoch[a] + 1]
  /\ reservation' = [reservation EXCEPT ![t] = "held"]
  /\ gate' = [gate EXCEPT ![t] = "holding"]
  /\ UNCHANGED << gateDeadline, requestDeadline, connectDeadline,
    firstByteDeadline, anchor, anchorUsed, badAnchorUse, durableVersion, snapshotVersion,
    routedWithStaleSnapshot, producerTarget, terminalReason, shutdownPhase,
    registered, ownerReleased, admittedDuringDrain >>

StartStream(t, k) ==
  /\ turnState[t] = "active"
  /\ k \in AnchorKinds \ {"none"}
  /\ turnState' = [turnState EXCEPT ![t] = "streaming"]
  /\ anchor' = [anchor EXCEPT ![t] =
      [kind |-> k, account |-> turnAccount[t], epoch |-> turnEpoch[t], lineageOk |-> TRUE]]
  /\ UNCHANGED << owner, ownerEpoch, turnReplica, turnAccount, turnEpoch,
    reservation, gate, gateDeadline, requestDeadline, connectDeadline,
    firstByteDeadline, anchorUsed, badAnchorUse, durableVersion, snapshotVersion,
    routedWithStaleSnapshot, producerTarget, terminalReason, shutdownPhase,
    registered, ownerReleased, admittedDuringDrain >>

AnchorSafe(t) ==
  /\ anchor[t].kind \in SafeRecoveryKinds
  /\ anchor[t].lineageOk
  /\ anchor[t].account \in Accounts
  /\ anchor[t].epoch = ownerEpoch[anchor[t].account]
  /\ owner[anchor[t].account] # NoReplica

UseAnchor(t) ==
  /\ turnState[t] \in {"active", "streaming"}
  /\ anchor[t].kind # "none"
  /\ (AnchorSafe(t) \/ WeakIgnoreOwnerEpoch)
  /\ anchorUsed' = [anchorUsed EXCEPT ![t] = TRUE]
  /\ badAnchorUse' = (badAnchorUse \/ ~AnchorSafe(t))
  /\ UNCHANGED << owner, ownerEpoch, turnState, turnReplica, turnAccount,
    turnEpoch, reservation, gate, gateDeadline, requestDeadline,
    connectDeadline, firstByteDeadline, anchor, durableVersion, snapshotVersion,
    routedWithStaleSnapshot, producerTarget, terminalReason, shutdownPhase,
    registered, ownerReleased, admittedDuringDrain >>

OwnerLoss(t) ==
  /\ turnState[t] \in {"active", "streaming"}
  /\ turnAccount[t] \in Accounts
  /\ owner' = [owner EXCEPT ![turnAccount[t]] = NoReplica]
  /\ ownerEpoch' = [ownerEpoch EXCEPT ![turnAccount[t]] = @ + 1]
  /\ IF WeakIgnoreOwnerEpoch
     THEN
       /\ UNCHANGED << turnState, reservation, gate, registered, terminalReason,
         ownerReleased >>
     ELSE
       /\ turnState' = [turnState EXCEPT ![t] = "retryable_owner_loss"]
       /\ reservation' = [reservation EXCEPT ![t] = "released"]
       /\ gate' = [gate EXCEPT ![t] = "terminal"]
       /\ registered' = [registered EXCEPT ![t] = FALSE]
       /\ terminalReason' = [terminalReason EXCEPT ![t] = "owner_loss"]
       /\ ownerReleased' = [ownerReleased EXCEPT ![t] = TRUE]
  /\ UNCHANGED << turnReplica, turnAccount, turnEpoch, gateDeadline,
    requestDeadline, connectDeadline, firstByteDeadline, anchor, anchorUsed, badAnchorUse,
    durableVersion, snapshotVersion, routedWithStaleSnapshot, producerTarget,
    shutdownPhase, admittedDuringDrain >>

CompleteTurn(t) ==
  /\ turnState[t] \in {"active", "streaming"}
  /\ turnState' = [turnState EXCEPT ![t] = "completed"]
  /\ reservation' = [reservation EXCEPT ![t] = "finalized"]
  /\ gate' = [gate EXCEPT ![t] = "terminal"]
  /\ terminalReason' = [terminalReason EXCEPT ![t] = "completed"]
  /\ registered' = [registered EXCEPT ![t] = FALSE]
  /\ ownerReleased' = [ownerReleased EXCEPT ![t] = TRUE]
  /\ owner' = [owner EXCEPT ![turnAccount[t]] =
      IF ownerEpoch[turnAccount[t]] = turnEpoch[t] THEN NoReplica ELSE @]
  /\ UNCHANGED << ownerEpoch, turnReplica, turnAccount, turnEpoch,
    gateDeadline, requestDeadline, connectDeadline, firstByteDeadline,
    anchor, anchorUsed, badAnchorUse, durableVersion, snapshotVersion, routedWithStaleSnapshot,
    producerTarget, shutdownPhase, admittedDuringDrain >>

CancelTurn(t) ==
  /\ turnState[t] \in {"queued", "active", "streaming"}
  /\ turnState' = [turnState EXCEPT ![t] = "cancelled"]
  /\ reservation' = [reservation EXCEPT ![t] =
      IF WeakSkipReleaseOnCancel THEN reservation[t]
      ELSE IF reservation[t] = "held" THEN "released" ELSE reservation[t]]
  /\ gate' = [gate EXCEPT ![t] =
      IF WeakLostWaiter /\ gate[t] = "queued" THEN "none" ELSE "terminal"]
  /\ terminalReason' = [terminalReason EXCEPT ![t] = "cancelled"]
  /\ registered' = [registered EXCEPT ![t] = FALSE]
  /\ ownerReleased' = [ownerReleased EXCEPT ![t] = TRUE]
  /\ owner' = IF turnAccount[t] \in Accounts
      THEN [owner EXCEPT ![turnAccount[t]] =
        IF ownerEpoch[turnAccount[t]] = turnEpoch[t] THEN NoReplica ELSE @]
      ELSE owner
  /\ UNCHANGED << ownerEpoch, turnReplica, turnAccount, turnEpoch,
    gateDeadline, requestDeadline, connectDeadline, firstByteDeadline,
    anchor, anchorUsed, badAnchorUse, durableVersion, snapshotVersion, routedWithStaleSnapshot,
    producerTarget, shutdownPhase, admittedDuringDrain >>

InvalidateQuota(a) ==
  /\ a \in Accounts
  /\ durableVersion[a] = 0
  /\ durableVersion' = [durableVersion EXCEPT ![a] = @ + 1]
  /\ UNCHANGED << owner, ownerEpoch, turnState, turnReplica, turnAccount,
    turnEpoch, reservation, gate, gateDeadline, requestDeadline,
    connectDeadline, firstByteDeadline, anchor, anchorUsed, badAnchorUse, snapshotVersion,
    routedWithStaleSnapshot, producerTarget, terminalReason, shutdownPhase,
    registered, ownerReleased, admittedDuringDrain >>

RefreshSnapshot(r, a) ==
  /\ r \in Replicas
  /\ a \in Accounts
  /\ snapshotVersion' = [snapshotVersion EXCEPT ![r][a] = durableVersion[a]]
  /\ UNCHANGED << owner, ownerEpoch, turnState, turnReplica, turnAccount,
    turnEpoch, reservation, gate, gateDeadline, requestDeadline,
    connectDeadline, firstByteDeadline, anchor, anchorUsed, badAnchorUse, durableVersion,
    routedWithStaleSnapshot, producerTarget, terminalReason, shutdownPhase,
    registered, ownerReleased, admittedDuringDrain >>

RouteFromSnapshot(t, r, a) ==
  /\ turnState[t] = "queued"
  /\ r \in Replicas
  /\ a \in Accounts
  /\ snapshotVersion[r][a] < durableVersion[a]
  /\ WeakStaleCache
  /\ routedWithStaleSnapshot' = [routedWithStaleSnapshot EXCEPT ![t] = TRUE]
  /\ UNCHANGED << owner, ownerEpoch, turnState, turnReplica, turnAccount,
    turnEpoch, reservation, gate, gateDeadline, requestDeadline,
    connectDeadline, firstByteDeadline, anchor, anchorUsed, badAnchorUse, durableVersion,
    snapshotVersion, producerTarget, terminalReason, shutdownPhase, registered,
    ownerReleased, admittedDuringDrain >>

MisrouteProducer(t, u) ==
  /\ t \in Turns
  /\ u \in Turns
  /\ t # u
  /\ turnState[t] \in TerminalStates
  /\ turnState[u] \in {"queued", "active", "streaming"}
  /\ WeakLostWaiter
  /\ producerTarget' = [producerTarget EXCEPT ![t] = u]
  /\ UNCHANGED << owner, ownerEpoch, turnState, turnReplica, turnAccount,
    turnEpoch, reservation, gate, gateDeadline, requestDeadline,
    connectDeadline, firstByteDeadline, anchor, anchorUsed, badAnchorUse, durableVersion,
    snapshotVersion, routedWithStaleSnapshot, terminalReason, shutdownPhase,
    registered, ownerReleased, admittedDuringDrain >>

StartDrain ==
  /\ shutdownPhase = "running"
  /\ shutdownPhase' = "draining"
  /\ UNCHANGED << owner, ownerEpoch, turnState, turnReplica, turnAccount,
    turnEpoch, reservation, gate, gateDeadline, requestDeadline,
    connectDeadline, firstByteDeadline, anchor, anchorUsed, badAnchorUse, durableVersion,
    snapshotVersion, routedWithStaleSnapshot, producerTarget, terminalReason,
    registered, ownerReleased, admittedDuringDrain >>

CompleteShutdown ==
  /\ shutdownPhase = "draining"
  /\ \A t \in Turns : registered[t] = FALSE
  /\ shutdownPhase' = "complete"
  /\ UNCHANGED << owner, ownerEpoch, turnState, turnReplica, turnAccount,
    turnEpoch, reservation, gate, gateDeadline, requestDeadline,
    connectDeadline, firstByteDeadline, anchor, anchorUsed, badAnchorUse, durableVersion,
    snapshotVersion, routedWithStaleSnapshot, producerTarget, terminalReason,
    registered, ownerReleased, admittedDuringDrain >>

Quiesce ==
  /\ shutdownPhase = "complete"
  /\ \A t \in Turns : turnState[t] \in TerminalStates \/ turnState[t] = "new"
  /\ UNCHANGED vars

Next ==
  \/ \E t \in Turns : QueueTurn(t)
  \/ \E t \in Turns, r \in Replicas, a \in Accounts : AcquireTurn(t, r, a)
  \/ \E t \in Turns, k \in AnchorKinds \ {"none"} : StartStream(t, k)
  \/ \E t \in Turns : UseAnchor(t)
  \/ \E t \in Turns : OwnerLoss(t)
  \/ \E t \in Turns : CompleteTurn(t)
  \/ \E t \in Turns : CancelTurn(t)
  \/ \E a \in Accounts : InvalidateQuota(a)
  \/ \E r \in Replicas, a \in Accounts : RefreshSnapshot(r, a)
  \/ \E t \in Turns, r \in Replicas, a \in Accounts : RouteFromSnapshot(t, r, a)
  \/ \E t \in Turns, u \in Turns : MisrouteProducer(t, u)
  \/ StartDrain
  \/ CompleteShutdown
  \/ Quiesce

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ \A t \in Turns : WF_vars(CompleteTurn(t) \/ CancelTurn(t) \/ OwnerLoss(t))
  /\ WF_vars(CompleteShutdown)

TypeInvariant ==
  /\ owner \in [Accounts -> Replicas \cup {NoReplica}]
  /\ ownerEpoch \in [Accounts -> Nat]
  /\ turnState \in [Turns -> NonTerminalStates \cup TerminalStates]
  /\ turnReplica \in [Turns -> Replicas \cup {NoReplica}]
  /\ turnAccount \in [Turns -> Accounts \cup {NoAccount}]
  /\ turnEpoch \in [Turns -> Nat]
  /\ reservation \in [Turns -> ReservationStates]
  /\ gate \in [Turns -> GateStates]
  /\ gateDeadline \in [Turns -> Nat]
  /\ requestDeadline \in [Turns -> Nat]
  /\ connectDeadline \in [Turns -> Nat]
  /\ firstByteDeadline \in [Turns -> Nat]
  /\ anchor \in [Turns -> [kind : AnchorKinds,
                           account : Accounts \cup {NoAccount},
                           epoch : Nat,
                           lineageOk : BOOLEAN]]
  /\ anchorUsed \in [Turns -> BOOLEAN]
  /\ badAnchorUse \in BOOLEAN
  /\ durableVersion \in [Accounts -> Nat]
  /\ snapshotVersion \in [Replicas -> [Accounts -> Nat]]
  /\ routedWithStaleSnapshot \in [Turns -> BOOLEAN]
  /\ producerTarget \in [Turns -> Turns]
  /\ terminalReason \in [Turns -> Reasons]
  /\ shutdownPhase \in {"running", "draining", "complete"}
  /\ registered \in [Turns -> BOOLEAN]
  /\ ownerReleased \in [Turns -> BOOLEAN]
  /\ admittedDuringDrain \in BOOLEAN

Inv1AnchorCurrent ==
  badAnchorUse = FALSE

Inv2DeadlineOrdering ==
  \A t \in Turns :
    turnState[t] # "new" =>
      /\ connectDeadline[t] <= firstByteDeadline[t]
      /\ firstByteDeadline[t] <= requestDeadline[t]
      /\ gateDeadline[t] <= requestDeadline[t]

Settled(s) == s \in {"released", "finalized", "transferred"}

Inv3ReservationSettledExactlyOnce ==
  \A t \in Turns :
    turnState[t] \in TerminalStates =>
      reservation[t] = "none" \/ Settled(reservation[t])

Inv4FreshSnapshots ==
  \A t \in Turns : routedWithStaleSnapshot[t] = FALSE

Inv5SingleOwnerCAS ==
  \A a \in Accounts : Cardinality(LiveOnAccount(a)) <= 1

Inv6TerminalIsolation ==
  \A t \in Turns :
    turnState[t] \in TerminalStates =>
      /\ producerTarget[t] = t
      /\ terminalReason[t] # "none"

Inv7GateAccounting ==
  \A t \in Turns :
    /\ gate[t] \in GateStates
    /\ (turnState[t] = "queued" => gate[t] = "queued")
    /\ (turnState[t] \in {"active", "streaming"} => gate[t] = "holding")
    /\ (turnState[t] \in TerminalStates => gate[t] = "terminal")
    /\ (gate[t] = "queued" => gateDeadline[t] <= requestDeadline[t])

Inv8ShutdownDrain ==
  /\ admittedDuringDrain = FALSE
  /\ shutdownPhase = "complete" =>
       \A t \in Turns : registered[t] = FALSE

TurnTermination ==
  \A t \in Turns : (turnState[t] # "new") ~> (turnState[t] \in TerminalStates)

ShutdownEventuallyComplete ==
  shutdownPhase = "draining" ~> shutdownPhase = "complete"

=============================================================================

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CORE_OWNERSHIP = ROOT / "spec" / "CoreOwnership.tla"
CORE_OWNERSHIP_CFG = ROOT / "spec" / "CoreOwnership.cfg"


def test_core_ownership_models_distinct_upstream_phases() -> None:
    spec = CORE_OWNERSHIP.read_text()

    assert 'AttemptPhases == {"none", "connect", "awaiting_first_byte", "awaiting_response", "streaming"}' in spec
    assert "UpstreamConnected(t) ==" in spec
    assert "UpstreamFirstByte(t) ==" in spec
    assert "StreamProgress(t) ==" in spec
    assert 'CASE attemptPhase[t] = "connect" -> connectDeadline[t]' in spec
    assert '[] attemptPhase[t] = "awaiting_first_byte" -> firstByteDeadline[t]' in spec


def test_core_ownership_guards_snapshot_refresh_and_retry_outcomes() -> None:
    spec = CORE_OWNERSHIP.read_text()

    assert "/\\ snapshotVersion[r][a] # durableVersion[a]" in spec
    assert "ClientRetryFails ==" in spec
    assert "ClientRetrySucceeds ==" in spec
    assert "CompletedProducerEventuallyDelivered ==" in spec
    assert "DeliverProducer(t, u) ==" in spec


def test_core_ownership_full_cfg_checks_completed_delivery_liveness() -> None:
    cfg = CORE_OWNERSHIP_CFG.read_text()

    assert "CompletedProducerEventuallyDelivered" in cfg

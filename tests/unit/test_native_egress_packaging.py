from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("dockerfile_name", ["Dockerfile", "Dockerfile.distroless"])
def test_linux_container_builds_and_installs_locked_native_egress(dockerfile_name: str) -> None:
    dockerfile = (_REPO_ROOT / dockerfile_name).read_text(encoding="utf-8")

    assert "COPY native/codex-egress/Cargo.toml native/codex-egress/Cargo.lock ./" in dockerfile
    assert "cargo build --release --locked" in dockerfile
    assert (
        "COPY --from=native-egress-build /tmp/codex-lb-native-egress /usr/local/bin/codex-lb-native-egress"
    ) in dockerfile

    runtime = dockerfile.rsplit(" AS runtime", maxsplit=1)[1]
    assert "cargo build" not in runtime
    assert "COPY --from=native-egress-build" in runtime


def test_native_egress_lockfile_pins_codex_release_family() -> None:
    lockfile = (_REPO_ROOT / "native/codex-egress/Cargo.lock").read_text(encoding="utf-8")

    for name, version in (
        ("reqwest", "0.12.28"),
        ("hyper", "1.8.1"),
        ("hyper-util", "0.1.20"),
        ("rustls", "0.23.36"),
        ("tokio-rustls", "0.26.4"),
        ("hyper-rustls", "0.27.7"),
        ("aws-lc-rs", "1.16.2"),
    ):
        assert f'name = "{name}"\nversion = "{version}"' in lockfile


def test_native_helper_source_supports_persistent_multiplexed_protocol() -> None:
    source = (_REPO_ROOT / "native/codex-egress/src/main.rs").read_text(encoding="utf-8")

    assert "enum NativeCommand" in source
    assert "Request(NativeRequest)" in source
    assert "WebsocketConnect(NativeWebSocketRequest)" in source
    assert "Cancel {" in source
    assert "struct ClientPool" in source
    assert "DeflateConfig::default()" in source
    assert "is_tls_verification_failure" in source
    assert "rustls::Error::InvalidCertificate" in source
    assert "tasks.spawn" in source


def test_native_egress_pins_codex_websocket_forks() -> None:
    manifest = (_REPO_ROOT / "native/codex-egress/Cargo.toml").read_text(encoding="utf-8")
    lockfile = (_REPO_ROOT / "native/codex-egress/Cargo.lock").read_text(encoding="utf-8")

    assert "0e5b2d73aa18dd9f0a50ee9ff199d5aef7594186" in manifest
    assert "4fffad30fe373adbdcffab9545e9e9bf4f2fc19f" in manifest
    assert 'name = "tokio-tungstenite"\nversion = "0.28.0"' in lockfile
    assert 'name = "tungstenite"\nversion = "0.27.0"' in lockfile


def test_application_shutdown_closes_native_helper_before_shared_http_client() -> None:
    source = (_REPO_ROOT / "app/main.py").read_text(encoding="utf-8")

    native_close = source.index("await close_discovered_native_egress_client()")
    http_close = source.index("await close_http_client()")
    assert native_close < http_close

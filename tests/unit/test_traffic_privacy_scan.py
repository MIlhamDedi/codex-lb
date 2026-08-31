from __future__ import annotations

from pathlib import Path

from scripts.traffic_analysis.privacy_scan import scan_tree


def test_privacy_scan_accepts_redacted_metadata(tmp_path: Path) -> None:
    (tmp_path / "capture.jsonl").write_text(
        '{"authorization":"[REDACTED]","access_token":"[SHA256:abc:12]"}\n',
        encoding="utf-8",
    )

    result = scan_tree(tmp_path)

    assert result["passed"] is True
    assert result["findings"] == []


def test_privacy_scan_reports_kinds_without_echoing_secret(tmp_path: Path) -> None:
    secret = "sk-examplecredential123456789"
    (tmp_path / "capture.jsonl").write_text(f'{{"authorization":"Bearer {secret}"}}\n', encoding="utf-8")

    result = scan_tree(tmp_path)

    assert result["passed"] is False
    assert result["findings"] == [
        {"path": "capture.jsonl", "kinds": ["bearer_token", "secret_key"]}
    ]
    assert secret not in str(result)


def test_privacy_scan_fails_closed_on_symlink(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("not scanned", encoding="utf-8")
    (tmp_path / "link").symlink_to(outside)

    result = scan_tree(tmp_path)

    assert result["passed"] is False
    assert result["findings"] == [{"path": "link", "kinds": ["symlink"]}]

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import app.db.compact as compact
import app.db.migrate as migrate
from app.db.sqlite_utils import IntegrityCheck, SqliteIntegrityCheckMode, check_sqlite_integrity

pytestmark = pytest.mark.unit


def _database_url(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path}"


def _create_fragmented_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE alembic_version (version_num TEXT PRIMARY KEY)")
        connection.execute("INSERT INTO alembic_version VALUES ('test_revision')")
        connection.execute("CREATE TABLE payloads (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
        connection.executemany(
            "INSERT INTO payloads(payload) VALUES (?)",
            ((f"{index}:" + "x" * 1024,) for index in range(2_000)),
        )
        connection.execute("DELETE FROM payloads WHERE id <= 1900")


def _remaining_rows(path: Path) -> int:
    with sqlite3.connect(path) as connection:
        return int(connection.execute("SELECT COUNT(*) FROM payloads").fetchone()[0])


def test_compaction_dry_run_reports_without_mutation(tmp_path: Path) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    before_stat = source.stat()
    before_names = {path.name for path in tmp_path.iterdir()}

    plan = compact.plan_sqlite_compaction(_database_url(source))

    assert plan.source == source
    assert plan.source_bytes == before_stat.st_size
    assert plan.page_size > 0
    assert plan.page_count > 0
    assert plan.freelist_pages > 0
    assert plan.reclaimable_bytes == plan.page_size * plan.freelist_pages
    assert plan.required_free_bytes == 2 * plan.source_bytes + compact._MIN_FREE_SPACE_RESERVE
    assert source.stat() == before_stat
    assert {path.name for path in tmp_path.iterdir()} == before_names


def test_compaction_reclaims_space_and_preserves_backup(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    source.chmod(0o600)
    source_stat = source.stat()
    before_bytes = source.stat().st_size
    real_integrity_check = compact.check_sqlite_integrity

    def assert_compacted_permissions(path: Path, *, mode: SqliteIntegrityCheckMode):
        if ".compact-" in str(path):
            assert path.stat().st_mode & 0o777 == 0o600
            assert (path.stat().st_uid, path.stat().st_gid) == (source_stat.st_uid, source_stat.st_gid)
            assert path.parent.stat().st_mode & 0o777 == 0o700
        return real_integrity_check(path, mode=mode)

    monkeypatch.setattr(compact, "check_sqlite_integrity", assert_compacted_permissions)

    outcome = compact.execute_sqlite_compaction(
        _database_url(source),
        confirm_stopped=True,
    )

    assert outcome.source == source
    assert outcome.backup.exists()
    assert outcome.source_bytes_before == before_bytes
    assert outcome.source_bytes_after == source.stat().st_size
    assert outcome.source_bytes_after < before_bytes
    assert outcome.reclaimed_bytes == before_bytes - source.stat().st_size
    assert _remaining_rows(source) == 100
    assert _remaining_rows(outcome.backup) == 100
    with sqlite3.connect(source) as connection:
        assert connection.execute("PRAGMA auto_vacuum").fetchone()[0] == 2
        assert connection.execute("SELECT version_num FROM alembic_version").fetchone()[0] == "test_revision"
    assert check_sqlite_integrity(source, mode=SqliteIntegrityCheckMode.QUICK).ok
    assert not Path(f"{source}.compact.lock").exists()


def test_compaction_requires_stopped_confirmation(tmp_path: Path) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)

    with pytest.raises(RuntimeError, match="--confirm-stopped"):
        compact.execute_sqlite_compaction(_database_url(source), confirm_stopped=False)

    assert _remaining_rows(source) == 100
    assert list(tmp_path.glob("*.pre-compact-*")) == []


@pytest.mark.parametrize(
    "database_url,error",
    [
        ("postgresql+asyncpg://localhost/codex", "file-backed SQLite"),
        ("sqlite+aiosqlite:///:memory:", "file-backed SQLite"),
    ],
)
def test_compaction_rejects_non_file_backends(database_url: str, error: str) -> None:
    with pytest.raises(RuntimeError, match=error):
        compact.plan_sqlite_compaction(database_url)


def test_compaction_rejects_missing_database(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="not found"):
        compact.plan_sqlite_compaction(_database_url(tmp_path / "missing.db"))


def test_compaction_rejects_symbolic_link_database_path(tmp_path: Path) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    link = tmp_path / "linked.db"
    try:
        link.symlink_to(source)
    except OSError:
        pytest.skip("symlink creation unavailable")

    with pytest.raises(RuntimeError, match="not a symbolic link"):
        compact.plan_sqlite_compaction(_database_url(link))


def test_compaction_backup_name_skips_dangling_symlink(tmp_path: Path) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    first = source.with_name("store.pre-compact-20260827T000000Z.db")
    try:
        first.symlink_to(tmp_path / "missing-target")
    except OSError:
        pytest.skip("symlink creation unavailable")

    candidate = compact._next_sibling(source, label="pre-compact", timestamp="20260827T000000Z")

    assert candidate.name == "store.pre-compact-20260827T000000Z-1.db"


def test_compaction_dry_run_rejects_nonempty_wal_without_touching_it(tmp_path: Path) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    wal = Path(f"{source}-wal")
    wal.write_bytes(b"committed-wal-placeholder")
    before = wal.read_bytes()

    with pytest.raises(RuntimeError, match="checkpointed SQLite"):
        compact.plan_sqlite_compaction(_database_url(source))

    assert wal.read_bytes() == before


def test_compaction_dry_run_accepts_zeroed_persistent_journal(tmp_path: Path) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    journal = Path(f"{source}-journal")
    journal.write_bytes(bytes(1024))
    before = journal.read_bytes()

    compact.plan_sqlite_compaction(_database_url(source))

    assert journal.read_bytes() == before


def test_compaction_dry_run_rejects_potentially_hot_journal(tmp_path: Path) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    journal = Path(f"{source}-journal")
    journal.write_bytes(b"hot-data" + bytes(1016))

    with pytest.raises(RuntimeError, match="potentially hot"):
        compact.plan_sqlite_compaction(_database_url(source))


def test_compaction_dry_run_rejects_state_change_during_read(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    signature = compact._database_state_signature(source)
    signatures = iter((signature, (*signature[:-1], compact._PathSignature(exists=True, size=1))))
    monkeypatch.setattr(compact, "_database_state_signature", lambda _source: next(signatures))

    with pytest.raises(RuntimeError, match="changed during dry-run"):
        compact.plan_sqlite_compaction(_database_url(source))


def test_compaction_fails_when_uid_gid_cannot_be_preserved(tmp_path: Path, monkeypatch) -> None:
    compacted = tmp_path / "compacted.db"
    compacted.write_bytes(b"sqlite")
    current = compacted.stat()
    source_stat = SimpleNamespace(
        st_mode=current.st_mode,
        st_uid=current.st_uid + 1,
        st_gid=current.st_gid,
    )
    monkeypatch.setattr(
        compact.os,
        "chown",
        lambda *_args: (_ for _ in ()).throw(PermissionError("not permitted")),
    )

    with pytest.raises(RuntimeError, match="cannot preserve SQLite database uid/gid"):
        compact._preserve_file_metadata(compacted, source_stat)


def test_compaction_rejects_insufficient_space_without_artifacts(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    disk_usage_type = type(compact.shutil.disk_usage(tmp_path))
    monkeypatch.setattr(compact.shutil, "disk_usage", lambda _path: disk_usage_type(100, 100, 0))

    with pytest.raises(RuntimeError, match="insufficient free space"):
        compact.execute_sqlite_compaction(_database_url(source), confirm_stopped=True)

    assert _remaining_rows(source) == 100
    assert {path.name for path in tmp_path.iterdir()} == {"store.db"}


def test_compaction_rejects_busy_checkpoint_and_existing_lock(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    monkeypatch.setattr(
        compact,
        "_checkpoint_wal",
        lambda _connection: (_ for _ in ()).throw(RuntimeError("checkpoint is busy")),
    )

    with pytest.raises(RuntimeError, match="checkpoint is busy"):
        compact.execute_sqlite_compaction(_database_url(source), confirm_stopped=True)
    assert not Path(f"{source}.compact.lock").exists()

    lock_path = Path(f"{source}.compact.lock")
    lock_path.write_text("held", encoding="utf-8")
    with pytest.raises(RuntimeError, match="lock already exists"):
        compact.execute_sqlite_compaction(_database_url(source), confirm_stopped=True)
    assert _remaining_rows(source) == 100


def test_compaction_lock_cleanup_does_not_unlink_replacement(tmp_path: Path) -> None:
    lock_path = tmp_path / "store.db.compact.lock"
    descriptor = compact.os.open(lock_path, compact.os.O_CREAT | compact.os.O_EXCL | compact.os.O_WRONLY, 0o600)
    try:
        lock_path.unlink()
        lock_path.write_text("replacement", encoding="utf-8")

        compact._unlink_owned_lock(lock_path, descriptor)

        assert lock_path.read_text(encoding="utf-8") == "replacement"
    finally:
        compact.os.close(descriptor)


def test_compaction_sidecar_backup_includes_rollback_journal(tmp_path: Path) -> None:
    source = tmp_path / "store.db"
    backup = tmp_path / "store.pre-compact.db"
    journal = Path(f"{source}-journal")
    journal.write_bytes(b"persistent-journal")

    moved = compact._backup_sidecars(source, backup)

    assert moved == [(journal, Path(f"{backup}-journal"))]
    assert not journal.exists()
    assert Path(f"{backup}-journal").read_bytes() == b"persistent-journal"


def test_compaction_rejects_external_write_and_corrupt_output(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    versions = iter((1, 2))
    monkeypatch.setattr(compact, "_data_version", lambda _connection: next(versions))

    with pytest.raises(RuntimeError, match="changed during compaction"):
        compact.execute_sqlite_compaction(_database_url(source), confirm_stopped=True)
    assert _remaining_rows(source) == 100
    assert list(tmp_path.glob("*.pre-compact-*")) == []

    monkeypatch.setattr(compact, "_data_version", lambda _connection: 1)
    real_integrity_check = compact.check_sqlite_integrity

    def reject_compacted(path: Path, *, mode: SqliteIntegrityCheckMode):
        if ".compact-" in str(path):
            return IntegrityCheck(ok=False, details="injected corruption")
        return real_integrity_check(path, mode=mode)

    monkeypatch.setattr(compact, "check_sqlite_integrity", reject_compacted)
    with pytest.raises(RuntimeError, match="compacted SQLite quick_check failed"):
        compact.execute_sqlite_compaction(_database_url(source), confirm_stopped=True)
    assert _remaining_rows(source) == 100
    assert list(tmp_path.glob("*.pre-compact-*")) == []


def test_compaction_blocks_concurrent_writer_before_replacement(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    real_replace = compact._replace_path

    def assert_writer_is_blocked(candidate: Path, target: Path) -> None:
        if ".compact-" in str(candidate) and target == source:
            writer = sqlite3.connect(source, timeout=0)
            try:
                writer.execute("PRAGMA busy_timeout=0")
                with pytest.raises(sqlite3.OperationalError, match="database is locked"):
                    writer.execute("INSERT INTO payloads(payload) VALUES ('concurrent-write')")
            finally:
                writer.close()
        real_replace(candidate, target)

    monkeypatch.setattr(compact, "_replace_path", assert_writer_is_blocked)

    compact.execute_sqlite_compaction(_database_url(source), confirm_stopped=True)

    assert _remaining_rows(source) == 100


def test_compaction_restores_source_when_install_rename_fails(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    real_replace = compact._replace_path

    def fail_compacted_install(candidate: Path, target: Path) -> None:
        if ".compact-" in str(candidate) and target == source:
            assert source.exists()
            backups = list(tmp_path.glob("*.pre-compact-*"))
            assert len(backups) == 1
            assert source.samefile(backups[0])
            raise OSError("injected install failure")
        real_replace(candidate, target)

    monkeypatch.setattr(compact, "_replace_path", fail_compacted_install)

    with pytest.raises(OSError, match="injected install failure"):
        compact.execute_sqlite_compaction(_database_url(source), confirm_stopped=True)

    assert source.exists()
    assert _remaining_rows(source) == 100
    assert list(tmp_path.glob("*.pre-compact-*")) == []
    assert not Path(f"{source}.compact.lock").exists()


def test_compaction_restores_original_when_post_install_fsync_fails(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    before = source.read_bytes()
    real_fsync_file = compact._fsync_file
    source_fsync_count = 0

    def fail_replacement_fsync(path: Path) -> None:
        nonlocal source_fsync_count
        if path == source:
            source_fsync_count += 1
            if source_fsync_count == 2:
                raise OSError("injected replacement fsync failure")
        real_fsync_file(path)

    monkeypatch.setattr(compact, "_fsync_file", fail_replacement_fsync)

    with pytest.raises(OSError, match="injected replacement fsync failure"):
        compact.execute_sqlite_compaction(_database_url(source), confirm_stopped=True)

    assert source.read_bytes() == before
    assert list(tmp_path.glob("*.pre-compact-*")) == []
    assert not Path(f"{source}.compact.lock").exists()


def test_compact_cli_dry_run_prints_plan(tmp_path: Path, monkeypatch, capsys) -> None:
    source = tmp_path / "store.db"
    _create_fragmented_database(source)
    monkeypatch.setattr(
        sys,
        "argv",
        ["codex-lb-db", "--db-url", _database_url(source), "compact", "--dry-run"],
    )

    migrate.main()

    output = capsys.readouterr().out
    assert f"source={source}" in output
    assert "reclaimable_bytes=" in output
    assert "required_free_bytes=" in output


def test_compact_cli_execute_does_not_call_dry_run_planner(monkeypatch, capsys) -> None:
    outcome = SimpleNamespace(
        backup=Path("/tmp/store.pre-compact.db"),
        source_bytes_after=123,
        reclaimed_bytes=456,
    )
    execute_calls: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        compact,
        "plan_sqlite_compaction",
        lambda _url: (_ for _ in ()).throw(AssertionError("dry-run planner called")),
    )

    def execute_mock(database_url: str, *, confirm_stopped: bool):
        execute_calls.append((database_url, confirm_stopped))
        return outcome

    monkeypatch.setattr(compact, "execute_sqlite_compaction", execute_mock)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "codex-lb-db",
            "--db-url",
            "sqlite+aiosqlite:////tmp/store.db",
            "compact",
            "--execute",
            "--confirm-stopped",
        ],
    )

    migrate.main()

    output = capsys.readouterr().out
    assert "backup=/tmp/store.pre-compact.db" in output
    assert "source_bytes_after=123" in output
    assert "reclaimed_bytes=456" in output
    assert execute_calls == [("sqlite+aiosqlite:////tmp/store.db", True)]

from __future__ import annotations

import sqlite3
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from sqlalchemy import util as sqlalchemy_util


@dataclass(slots=True)
class IntegrityCheck:
    ok: bool
    details: str | None


class SqliteIntegrityCheckMode(str, Enum):
    QUICK = "quick"
    FULL = "full"


@contextmanager
def sqlite_connection(path: str | Path) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(str(path))
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def _sqlite_path_uses_sqlalchemy_windows_escapes(path: str) -> bool:
    lower_path = path.lower()
    if (
        len(lower_path) >= 5
        and lower_path[1:4] == "%3a"
        and lower_path[0].isalpha()
        and (lower_path[4:7] in ("%5c", "%2f") or lower_path[4] in ("\\", "/"))
    ):
        return True
    return lower_path.startswith("%5c%5c")


def _sqlite_path_is_raw_windows_drive(path: str) -> bool:
    return len(path) >= 3 and path[1] == ":" and path[0].isalpha() and path[2] in ("\\", "/")


def _sqlite_path_is_raw_windows_unc(path: str) -> bool:
    return path.startswith("\\\\")


def _decode_sqlalchemy_windows_sqlite_path(path: str) -> str:
    if not _sqlite_path_uses_sqlalchemy_windows_escapes(path):
        return path
    return urllib.parse.unquote(path)


def _sqlite_uri_mode_active(query: str) -> bool:
    values = urllib.parse.parse_qs(query.partition("#")[0], keep_blank_values=True).get("uri", ())
    for value in values:
        try:
            if sqlalchemy_util.asbool(value):
                return True
        except ValueError:
            continue
    return False


def _sqlite_uri_file_path(path: str) -> str | None:
    parsed = urllib.parse.urlsplit(path)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        return None
    resolved = urllib.parse.unquote(parsed.path)
    if len(resolved) >= 3 and resolved[0] == "/" and resolved[1].isalpha() and resolved[2] == ":":
        return resolved[1:]
    return resolved


def sqlite_url_uses_in_memory_database(url: str) -> bool:
    """Return whether a SQLite URL selects an in-memory database."""

    if not (url.startswith("sqlite+aiosqlite:") or url.startswith("sqlite:")):
        return False

    marker = ":///"
    marker_index = url.find(marker)
    if marker_index < 0:
        return False

    raw_path = url[marker_index + len(marker) :]
    path, _, query = raw_path.partition("?")
    if not path or path == ":memory:":
        return True
    if not _sqlite_uri_mode_active(query) or not path.startswith("file:"):
        return False
    query_values = urllib.parse.parse_qs(query.partition("#")[0], keep_blank_values=True)
    return path == "file::memory:" or any(value.lower() == "memory" for value in query_values.get("mode", ()))


def sqlite_db_path_from_url(url: str) -> Path | None:
    if not (url.startswith("sqlite+aiosqlite:") or url.startswith("sqlite:")):
        return None

    if sqlite_url_uses_in_memory_database(url):
        return None

    marker = ":///"
    marker_index = url.find(marker)
    if marker_index < 0:
        return None

    raw_path = url[marker_index + len(marker) :]
    path, _, query = raw_path.partition("?")
    if _sqlite_path_is_raw_windows_drive(path) or _sqlite_path_is_raw_windows_unc(path):
        # Raw Windows drive and UNC paths are filesystem paths, not URL-encoded
        # forms: a `#` is a legal path character there (e.g. the decoded output
        # of `normalize_sqlite_url()`), so it must not be stripped as a URL
        # fragment separator.
        pass
    else:
        path = path.partition("#")[0]

    # SQLAlchemy's `URL.render_as_string()` percent-encodes Windows drive and
    # UNC SQLite paths (e.g. `sqlite:///C%3A%5CUsers%5C...%5Cstore.db`). Decode
    # those recognizable rendered Windows forms before opening the filesystem
    # path. Do not unquote arbitrary `%xx` sequences here: settings builds the
    # default SQLite URL directly from `data_dir`, so a valid literal path such
    # as `/var/lib/codex%20lb/store.db` must remain literal.
    path = _decode_sqlalchemy_windows_sqlite_path(path)
    if _sqlite_uri_mode_active(query) and path.startswith("file:"):
        path = _sqlite_uri_file_path(path)

    if not path:
        return None

    return Path(path).expanduser()


def normalize_sqlite_url(url: str) -> str:
    if not (url.startswith("sqlite+aiosqlite:") or url.startswith("sqlite:")):
        return url

    marker = ":///"
    marker_index = url.find(marker)
    if marker_index < 0:
        return url

    path_start = marker_index + len(marker)
    suffix_index = len(url)
    for separator in ("?", "#"):
        separator_index = url.find(separator, path_start)
        if separator_index >= 0:
            suffix_index = min(suffix_index, separator_index)

    path = url[path_start:suffix_index]
    if not path or path == ":memory:":
        return url

    decoded_path = _decode_sqlalchemy_windows_sqlite_path(path)
    return f"{url[:path_start]}{decoded_path}{url[suffix_index:]}"


def _integrity_check_pragma(mode: SqliteIntegrityCheckMode) -> str:
    if mode == SqliteIntegrityCheckMode.QUICK:
        return "PRAGMA quick_check;"
    return "PRAGMA integrity_check;"


def check_sqlite_integrity(
    path: Path,
    *,
    mode: SqliteIntegrityCheckMode = SqliteIntegrityCheckMode.FULL,
) -> IntegrityCheck:
    if not path.exists():
        return IntegrityCheck(ok=True, details=None)

    try:
        with sqlite_connection(path) as conn:
            cursor = conn.execute(_integrity_check_pragma(mode))
            rows = [row[0] for row in cursor.fetchall()]
    except sqlite3.DatabaseError as exc:
        return IntegrityCheck(ok=False, details=str(exc))

    if len(rows) == 1 and rows[0] == "ok":
        return IntegrityCheck(ok=True, details=None)

    if not rows:
        return IntegrityCheck(ok=False, details=f"{mode.value}_check returned no rows")

    details = "; ".join(str(row) for row in rows)
    return IntegrityCheck(ok=False, details=details)

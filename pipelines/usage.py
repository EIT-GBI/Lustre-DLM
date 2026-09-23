#!/usr/bin/env python3
"""Publish one owner's usage summary from a completed Parquet inventory.

The inventory has no file-type or allocated-block column. Values are named
``apparent_entry_bytes`` and sum raw ``st_size`` once per inventory entry.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import stat
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

SCHEMA = """
CREATE TABLE metadata (
    schema_version INTEGER NOT NULL,
    snapshot_id TEXT PRIMARY KEY,
    owner_uid INTEGER NOT NULL,
    root TEXT NOT NULL,
    source_path TEXT NOT NULL,
    source_snapshot_at TEXT NOT NULL,
    published_at TEXT NOT NULL,
    status TEXT NOT NULL,
    source_rows INTEGER NOT NULL,
    selected_rows INTEGER NOT NULL,
    error_rows INTEGER NOT NULL,
    apparent_entry_bytes INTEGER NOT NULL,
    directory_count INTEGER NOT NULL
);
CREATE TABLE directory_usage (
    path TEXT PRIMARY KEY,
    parent_path TEXT NOT NULL,
    apparent_entry_bytes INTEGER NOT NULL,
    entry_count INTEGER NOT NULL,
    error_count INTEGER NOT NULL
);
CREATE INDEX directory_usage_parent ON directory_usage(parent_path, path);
"""

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def norm_root(path: str) -> str:
    value = os.path.abspath(path)
    return value.rstrip(os.sep) or os.sep

def relative(path: str, root: str) -> str | None:
    value = os.path.abspath(path)
    if value == root:
        return "."
    prefix = root.rstrip(os.sep) + os.sep
    if not value.startswith(prefix):
        return None
    return PurePosixPath(value[len(prefix):]).as_posix()

def parent(path: str) -> str:
    if path == ".":
        return "."
    result = str(PurePosixPath(path).parent)
    return result if result else "."

def source_files(source: Path) -> list[Path]:
    if source.is_file():
        if source.suffix.lower() != ".parquet":
            raise ValueError("source must be a Parquet file")
        return [source]
    if source.is_dir():
        files = sorted(p for p in source.rglob("*.parquet") if p.is_file())
        if files:
            return files
    raise ValueError(f"source is not a Parquet file or directory: {source}")

def signatures(files: list[Path]) -> list[tuple[str, int, int, int, int]]:
    return [
        (str(path), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
        for path in files
        for info in [path.stat()]
    ]

def check_output(destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        info = destination.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise PermissionError("output must be an existing regular file owned by caller")
    elif not destination.parent.is_dir() or destination.parent.stat().st_uid != os.getuid():
        raise PermissionError("output directory must exist and be owned by caller")

def import_duckdb():
    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise SystemExit("usage publish requires optional dependency: duckdb") from exc
    return duckdb

def publish(args: argparse.Namespace) -> int:
    if not args.completed or not args.snapshot_at:
        raise SystemExit("usage publish requires --completed and --snapshot-at")
    root = norm_root(args.root)
    root_info = os.stat(root)
    if not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid != os.getuid():
        raise SystemExit("root must be an existing directory owned by the caller")
    source = Path(args.input).resolve()
    destination = Path(args.output or (Path.home() / ".gbi" / "usage.sqlite3")).expanduser()
    if destination.exists() and source == destination.resolve():
        raise SystemExit("source and output must differ")
    destination.parent.mkdir(parents=True, exist_ok=True)
    check_output(destination)
    files = source_files(source)
    before = signatures(files)
    duckdb = import_duckdb()
    fd, temporary = tempfile.mkstemp(prefix=".usage-", suffix=".sqlite3", dir=destination.parent)
    os.close(fd)
    try:
        with duckdb.connect() as con:
            relation = str(source / "**" / "*.parquet") if source.is_dir() else str(source)
            prefix = root.rstrip(os.sep) + os.sep
            columns = {row[0] for row in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [relation]
            ).fetchall()}
            if "path" not in columns or "st_size" not in columns:
                raise ValueError("Parquet source must contain path and st_size columns")
            code_expr = "code" if "code" in columns else "0"
            invalid = con.execute("""
                SELECT COUNT(*) FROM read_parquet(?)
                WHERE (path = ? OR starts_with(path, ?))
                  AND (contains(path, '//') OR contains(path, '/../') OR ends_with(path, '/..'))
            """, [relation, root, prefix]).fetchone()[0]
            if invalid:
                raise ValueError(f"source contains {invalid} invalid selected paths")
            groups = con.execute(f"""
                SELECT
                  CASE WHEN path = ? THEN '.' ELSE regexp_replace(path, '/[^/]*$', '') END AS parent_path,
                  SUM(COALESCE(st_size, 0)) AS apparent_entry_bytes,
                  COUNT(*) AS entry_count,
                  SUM(CASE WHEN {code_expr} IS NULL OR {code_expr} = 0 THEN 0 ELSE 1 END) AS error_count
                FROM read_parquet(?)
                WHERE path = ? OR starts_with(path, ?)
                GROUP BY 1 ORDER BY parent_path
            """, [root, relation, root, prefix]).fetchall()
            source_rows = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [relation]).fetchone()[0]
            if not groups:
                raise ValueError("completed source has no entries under owner root")

        with sqlite3.connect(temporary) as db:
            db.executescript(SCHEMA)
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("INSERT INTO directory_usage VALUES ('.', '.', 0, 0, 0)")
            selected = errors = total_bytes = 0
            for path, bytes_, count, error_count in groups:
                rel = "." if path == "." else relative(path, root)
                if rel is None:
                    raise ValueError(f"source produced path outside owner root: {path}")
                count, error_count = int(count), int(error_count or 0)
                bytes_ = int(bytes_ or 0)
                db.execute(
                    "INSERT INTO directory_usage VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(path) DO UPDATE SET apparent_entry_bytes=apparent_entry_bytes+excluded.apparent_entry_bytes, "
                    "entry_count=entry_count+excluded.entry_count, error_count=error_count+excluded.error_count",
                    (rel, parent(rel), bytes_, count, error_count),
                )
                selected += count
                errors += error_count
                total_bytes += bytes_
            rows_desc = db.execute(
                "SELECT path, parent_path FROM directory_usage ORDER BY length(path) DESC, path DESC"
            ).fetchall()
            for path, parent_path in rows_desc:
                if path != ".":
                    db.execute(
                        "UPDATE directory_usage AS p SET apparent_entry_bytes=p.apparent_entry_bytes+c.apparent_entry_bytes, "
                        "entry_count=p.entry_count+c.entry_count, error_count=p.error_count+c.error_count "
                        "FROM directory_usage AS c WHERE p.path=? AND c.path=?",
                        (parent_path, path),
                    )
            # Re-enumerate partition files as well as checking each identity;
            # a newly added partition is a source mutation too.
            after_files = source_files(source)
            after = signatures(after_files)
            if before != after:
                raise RuntimeError("source changed during publication")
            if errors:
                raise RuntimeError(f"selected source contains {errors} scanner errors")
            db.execute("INSERT INTO metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                1, f"{args.snapshot_at}:{os.getpid()}", os.getuid(), root, str(source),
                args.snapshot_at, utc_now(), "complete", int(source_rows), selected,
                errors, total_bytes, len(rows_desc)))
            db.commit()
        os.replace(temporary, destination)
        print(destination)
        return 0
    except Exception as exc:
        print(f"usage: retained previous complete publication: {exc}", file=sys.stderr)
        return 2
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass

def main() -> int:
    parser = argparse.ArgumentParser(prog="usage")
    parser.add_argument("--input", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--output")
    parser.add_argument("--completed", action="store_true")
    parser.add_argument("--snapshot-at")
    return publish(parser.parse_args())

if __name__ == "__main__":
    raise SystemExit(main())

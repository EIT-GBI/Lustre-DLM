#!/usr/bin/env python3
"""Publish a private directory summary from an explicitly completed inventory."""

from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import stat
import sys
import tempfile


SCHEMA = """
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE directories (
    path TEXT PRIMARY KEY, parent_path TEXT NOT NULL,
    apparent_bytes INTEGER NOT NULL, entries INTEGER NOT NULL
);
"""


def source_files(source: Path) -> list[Path]:
    if source.is_file() and source.suffix.lower() in {".parquet", ".jsonl"}:
        return [source]
    if source.is_dir():
        files = sorted(path for path in source.rglob("*.parquet") if path.is_file())
        if files:
            return files
    raise ValueError("input must be completed JSONL, Parquet, or a Parquet dataset directory")


def signatures(files):
    return [(str(path), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
            for path in files for info in [path.stat()]]


def check_output(destination):
    directory = destination.parent.stat()
    if directory.st_uid != os.getuid() or directory.st_mode & 0o022:
        raise ValueError("output directory must be caller-owned and not writable by others")
    try:
        info = destination.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("existing output must be a regular file owned by the caller")


def ensure_directory(db, path):
    """Create inferred ancestors without assuming the inventory listed each one."""
    while True:
        parent = "" if path == "." else str(PurePosixPath(path).parent)
        inserted = db.execute("INSERT OR IGNORE INTO directories VALUES (?, ?, 0, 0)", (path, parent))
        if not inserted.rowcount:
            return  # Its ancestors were created when this directory was inserted.
        if path == ".":
            return
        path = parent


def select_inventory(source, root):
    """Expose the selected source rows without materializing the full subset."""
    import duckdb

    path = duckdb.ColumnExpression("path")
    selected = source.table("inventory").filter(
        (path == duckdb.ConstantExpression(root))
        | duckdb.FunctionExpression("starts_with", path, duckdb.ConstantExpression(root + "/"))
    )
    selected.project("path, st_size, code").create_view("selected")


def aggregate(db, files, root, scratch):
    try:
        import duckdb
    except ImportError as error:
        raise ValueError("install the optional producer dependency: duckdb") from error
    # Only directory groups cross into Python. Larger grouping operations can
    # spill into the private temporary directory instead of growing RAM.
    with duckdb.connect(config={"threads": 1, "memory_limit": "512MB",
                                "preserve_insertion_order": False,
                                "temp_directory": str(scratch)}) as source:
        names = [str(path) for path in files]
        if files[0].suffix.lower() == ".jsonl":
            source.read_json(names, format="newline_delimited", columns={
                "path": "VARCHAR", "st_size": "BIGINT", "code": "BIGINT",
            }).create_view("inventory")
        else:
            source.read_parquet(names).create_view("inventory")
        columns = {row[0] for row in source.execute("DESCRIBE inventory").fetchall()}
        if not {"path", "st_size", "code"}.issubset(columns):
            raise ValueError("inventory needs path, st_size and code columns")
        select_inventory(source, root)
        print("usage: validating completed inventory", file=sys.stderr, flush=True)
        invalid = source.execute("""
            SELECT count(*) FROM selected WHERE code IS NULL OR code <> 0
                OR st_size IS NULL OR st_size < 0
                OR contains(path, '//') OR contains(path, '/./') OR contains(path, '/../')
                OR ends_with(path, '/.') OR ends_with(path, '/..') OR ends_with(path, '/')
        """).fetchone()[0]
        if invalid:
            raise ValueError(f"selected inventory has {invalid} failed or invalid entries")
        count, total = source.execute("SELECT count(*), sum(st_size) FROM selected").fetchone()
        if not count:
            raise ValueError("inventory contains no entries for the selected root")
        # First assign every entry to its parent (the root belongs to itself).
        # Keep this grouping separate from the directory-metadata join below.
        # Parquet streams the grouped output into private scratch storage.
        print("usage: finding directory paths", file=sys.stderr, flush=True)
        grouped = str(Path(scratch) / "parents.parquet")
        source.execute("""
            COPY (
                SELECT CASE WHEN path = $root THEN path
                            ELSE regexp_replace(path, '/[^/]*$', '') END AS path,
                       sum(st_size) AS size, count(*) AS entries
                FROM selected GROUP BY 1
            ) TO $output (FORMAT PARQUET)
        """, {"root": root, "output": grouped})
        source.read_parquet(grouped).create_view("parents")
        cursor = source.execute("SELECT path, size, entries FROM parents")
        print("usage: writing directory totals", file=sys.stderr, flush=True)
        while batch := cursor.fetchmany(1000):
            for directory, size, entries in batch:
                relative = PurePosixPath(directory).relative_to(root).as_posix()
                ensure_directory(db, relative)
                db.execute("UPDATE directories SET apparent_bytes=apparent_bytes+?, "
                           "entries=entries+? WHERE path=?", (int(size), entries, relative))
        # A listed path that is also a parent is a directory. Move its own
        # metadata from its parent into that directory before subtree rollup.
        # Empty directories remain indistinguishable from files in this schema.
        print("usage: assigning directory metadata", file=sys.stderr, flush=True)
        cursor = source.execute("""
            SELECT entry.path, entry.st_size
            FROM selected entry JOIN parents parent ON entry.path = parent.path
            WHERE entry.path <> ?
        """, [root])
        while batch := cursor.fetchmany(1000):
            for directory, size in batch:
                relative = PurePosixPath(directory).relative_to(root)
                db.execute("UPDATE directories SET apparent_bytes=apparent_bytes+?, "
                           "entries=entries+1 WHERE path=?", (int(size), relative.as_posix()))
                db.execute("UPDATE directories SET apparent_bytes=apparent_bytes-?, "
                           "entries=entries-1 WHERE path=?", (int(size), relative.parent.as_posix()))
    # Children always have longer paths than their parents. Read current totals
    # after children contribute and add each subtree to its parent once.
    print("usage: rolling up folder totals", file=sys.stderr, flush=True)
    paths = db.execute("SELECT path, parent_path FROM directories WHERE path <> '.' "
                       "ORDER BY length(path) DESC")
    for path, parent in paths:
        size, entries = db.execute("SELECT apparent_bytes, entries FROM directories WHERE path=?",
                                   (path,)).fetchone()
        db.execute("UPDATE directories SET apparent_bytes=apparent_bytes+?, entries=entries+? "
                   "WHERE path=?", (size, entries, parent))
    observed = db.execute("SELECT apparent_bytes, entries FROM directories WHERE path='.'").fetchone()
    if observed != (int(total), count):
        raise ValueError("directory totals do not reconcile with the source")
    return count, int(total)


def publish(args):
    temporary = None
    destination_temporary = None
    try:
        if not args.completed:
            raise ValueError("use --completed only after the inventory job has finished successfully")
        stamp = datetime.fromisoformat(args.snapshot_at.replace("Z", "+00:00"))
        if stamp.tzinfo is None or stamp > datetime.now(timezone.utc):
            raise ValueError("snapshot time must include a timezone and must not be in the future")
        root = Path(os.path.abspath(args.root))
        root.stat()
        if not root.is_dir() or root.stat().st_uid != os.getuid() or root == Path("/"):
            raise ValueError("root must be an existing directory owned by the caller")
        source = Path(os.path.abspath(args.input))
        source.stat()
        destination = Path(args.output or Path.home() / ".gbi" / "usage.sqlite3").expanduser().absolute()
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        check_output(destination)
        files = source_files(source)
        if destination.resolve() in files:
            raise ValueError("source and output must differ")
        before = signatures(files)
        with tempfile.TemporaryDirectory(prefix=".usage-work-") as scratch:
            # Build the report on worker-local scratch; copy it to FSS only
            # after SQLite has closed it, so FSS sees one sequential write.
            fd, temporary = tempfile.mkstemp(prefix=".usage-", suffix=".sqlite3", dir=scratch)
            os.close(fd)
            with closing(sqlite3.connect(temporary)) as db:
                db.executescript(SCHEMA)
                count, total = aggregate(db, files, str(root), scratch)
                print("usage: indexing folder report", file=sys.stderr, flush=True)
                db.execute("CREATE INDEX directories_parent ON directories"
                           "(parent_path, apparent_bytes DESC, path)")
                if before != signatures(source_files(source)):
                    raise ValueError("source changed during publication")
                metadata = {
                    "schema_version": "1", "owner_uid": str(os.getuid()), "root": str(root),
                    "status": "complete", "complete_input": "true",
                    "snapshot_at": stamp.isoformat(),
                    "published_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "source": str(source), "entries": str(count), "apparent_bytes": str(total),
                }
                db.executemany("INSERT INTO metadata VALUES (?, ?)", metadata.items())
                db.commit()
            fd, destination_temporary = tempfile.mkstemp(
                prefix=".usage-", suffix=".sqlite3", dir=destination.parent
            )
            os.close(fd)
            with open(temporary, "rb") as source_report, open(destination_temporary, "wb") as staged_report:
                shutil.copyfileobj(source_report, staged_report, length=1024 * 1024)
                staged_report.flush()
                os.fsync(staged_report.fileno())
            if before != signatures(source_files(source)):
                raise ValueError("source changed during publication")
        check_output(destination)
        os.replace(destination_temporary, destination)
        destination_temporary = None
        print(destination)
        return 0
    except Exception as error:
        # DuckDB has its own parse/I/O exceptions. Every failed generation must
        # return failure while keeping the last successfully published report.
        print(f"usage: publication failed; previous report retained: {error}", file=sys.stderr)
        return 2
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)
        if destination_temporary is not None:
            Path(destination_temporary).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="completed JSONL, Parquet, or Parquet dataset directory")
    parser.add_argument("--root", required=True, help="your own canonical Lustre directory")
    parser.add_argument("--output", help="private SQLite report (default: ~/.gbi/usage.sqlite3)")
    parser.add_argument("--completed", action="store_true", help="confirm the inventory job finished successfully")
    parser.add_argument("--snapshot-at", required=True, help="original inventory timestamp with timezone, not conversion time")
    return publish(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())

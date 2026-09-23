import importlib.util
import sqlite3
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")


MODULE_PATH = Path(__file__).parents[1] / "pipelines" / "usage.py"
spec = importlib.util.spec_from_file_location("usage", MODULE_PATH)
usage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(usage)


def parquet(tmp_path: Path, root: Path, rows: int = 4) -> Path:
    path = tmp_path / "inventory.parquet"
    con = duckdb.connect()
    con.execute("CREATE TABLE records(path VARCHAR, st_size BIGINT, code INTEGER)")
    values = [(str(root), 100, 0), (str(root / "a%_file"), 10, 0),
              (str(root / "nested"), 20, 0), (str(root / "nested" / "b"), 30, 0),
              (str(root / "weird%_dir" / "file"), 7, 0)]
    values.append((str(root.parent / "alice-other" / "foreign"), 999, 0))
    values.extend((str(root / "bulk" / f"f-{i}"), 1, 0) for i in range(rows))
    con.executemany("INSERT INTO records VALUES (?, ?, ?)", values)
    con.execute("COPY records TO ? (FORMAT PARQUET)", [str(path)])
    con.close()
    return path


def publish(source: Path, root: Path, output: Path, snapshot="2026-09-20T00:00:00Z"):
    args = type("Args", (), {"completed": True, "snapshot_at": snapshot,
                              "root": str(root), "input": str(source),
                              "output": str(output)})
    return usage.publish(args)


def test_rollup_literal_paths_and_foreign_root_filter(tmp_path: Path):
    root = tmp_path / "alice"
    root.mkdir()
    source = parquet(tmp_path, root)
    output = tmp_path / "usage.sqlite3"
    assert publish(source, root, output) == 0
    with sqlite3.connect(output) as db:
        metadata = db.execute("SELECT schema_version, owner_uid, source_snapshot_at, published_at FROM metadata").fetchone()
        assert metadata[0] == 1 and metadata[1] >= 0
        assert metadata[2] == "2026-09-20T00:00:00Z" and metadata[3] != metadata[2]
        assert db.execute("SELECT apparent_entry_bytes FROM directory_usage WHERE path='.'").fetchone() == (171,)
        assert db.execute("SELECT apparent_entry_bytes FROM directory_usage WHERE path='nested'").fetchone() == (30,)
        assert db.execute("SELECT apparent_entry_bytes FROM directory_usage WHERE path=?", ("weird%_dir",)).fetchone() == (7,)


def test_errors_preserve_previous_complete(tmp_path: Path):
    root = tmp_path / "alice"
    root.mkdir()
    source = parquet(tmp_path, root)
    output = tmp_path / "usage.sqlite3"
    assert publish(source, root, output) == 0
    previous = output.read_bytes()
    con = duckdb.connect()
    con.execute("CREATE TABLE records(path VARCHAR, st_size BIGINT, code INTEGER)", [])
    con.execute("INSERT INTO records VALUES (?, ?, ?)", [str(root / "bad"), 1, 1])
    bad_source = tmp_path / "bad.parquet"
    con.execute("COPY records TO ? (FORMAT PARQUET)", [str(bad_source)])
    con.close()
    assert publish(bad_source, root, output) == 2
    assert output.read_bytes() == previous


def test_large_fixture_is_stream_aggregation_shape(tmp_path: Path):
    root = tmp_path / "alice"
    root.mkdir()
    source = parquet(tmp_path, root, rows=100_000)
    output = tmp_path / "usage.sqlite3"
    assert publish(source, root, output) == 0
    with sqlite3.connect(output) as db:
        assert db.execute("SELECT entry_count FROM directory_usage WHERE path='bulk'").fetchone() == (100_000,)


def test_source_mutation_does_not_publish(tmp_path: Path, monkeypatch):
    root = tmp_path / "alice"
    root.mkdir()
    source = parquet(tmp_path, root)
    output = tmp_path / "usage.sqlite3"
    real_signatures = usage.signatures
    calls = 0

    def changing(files):
        nonlocal calls
        calls += 1
        value = real_signatures(files)
        return value if calls == 1 else value[:-1] + [("new-partition", 1, 2, 3, 4)]

    monkeypatch.setattr(usage, "signatures", changing)
    assert publish(source, root, output) == 2
    assert not output.exists()

import importlib.util
from contextlib import closing
import sqlite3
import json
import os
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from pathlib import Path

import duckdb


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
    con.executemany("INSERT INTO records VALUES (?, ?, ?)", values)
    con.execute("INSERT INTO records SELECT ? || CAST(i AS VARCHAR), 1, 0 FROM range(?) rows(i)", [str(root / "bulk" / "f-"), rows])
    con.execute("COPY records TO ? (FORMAT PARQUET)", [str(path)])
    con.close()
    return path


def publish(source: Path, root: Path, output: Path, snapshot="2026-09-20T00:00:00Z"):
    args = type("Args", (), {"completed": True, "snapshot_at": snapshot,
                              "root": str(root), "input": str(source),
                              "output": str(output)})
    return usage.publish(args)


def test_rollup_literal_paths_and_foreign_root_filter(tmp_path: Path):
    root = tmp_path / "ali'ce%_"
    root.mkdir()
    source = parquet(tmp_path, root)
    output = tmp_path / "usage.sqlite3"
    assert publish(source, root, output) == 0
    with closing(sqlite3.connect(output)) as db:
        metadata = dict(db.execute("SELECT key, value FROM metadata"))
        assert metadata["schema_version"] == "1" and metadata["owner_uid"] == str(os.getuid())
        assert metadata["snapshot_at"] == "2026-09-20T00:00:00+00:00"
        assert metadata["published_at"] != metadata["snapshot_at"]
        assert db.execute("SELECT apparent_bytes FROM directories WHERE path='.'").fetchone() == (171,)
        assert db.execute("SELECT apparent_bytes FROM directories WHERE path='nested'").fetchone() == (50,)
        assert db.execute("SELECT apparent_bytes FROM directories WHERE path=?", ("weird%_dir",)).fetchone() == (7,)


def test_selected_inventory_is_lazy_and_treats_root_literally(tmp_path: Path):
    con = duckdb.connect()
    con.execute("CREATE TABLE inventory(path VARCHAR, st_size BIGINT, code INTEGER)")
    root = "/ali'ce%_"
    con.executemany("INSERT INTO inventory VALUES (?, ?, ?)", [
        (root, 100, 0), (root + "/a", 10, 0), (root + "-other/b", 999, 0),
    ])
    usage.select_inventory(con, root)
    assert con.execute("SELECT path FROM selected ORDER BY path").fetchall() == [
        (root,), (root + "/a",)
    ]
    assert con.execute("SELECT view_name FROM duckdb_views() WHERE view_name='selected'").fetchone() == ("selected",)
    con.execute("INSERT INTO inventory VALUES (?, ?, ?)", (root + "/new", 4, 0))
    assert con.execute("SELECT path FROM selected ORDER BY path").fetchall() == [
        (root,), (root + "/a",), (root + "/new",)
    ]
    con.close()


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
    started = time.monotonic()
    assert publish(source, root, output) == 0
    print(f"100000-entry publication seconds: {time.monotonic() - started:.3f}")
    with closing(sqlite3.connect(output)) as db:
        assert db.execute("SELECT entries FROM directories WHERE path='bulk'").fetchone() == (100_000,)


def test_source_mutation_does_not_publish(tmp_path: Path):
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

    with patch.object(usage, "signatures", changing):
        assert publish(source, root, output) == 2
    assert not output.exists()


def test_directory_metadata_rolls_up_across_batches(tmp_path: Path):
    root = tmp_path / "alice"
    root.mkdir()
    rows = [{"path": str(root), "st_size": 13, "code": 0}]
    for index in range(2500):
        directory = root / f"folder-{index}"
        rows.extend([{"path": str(directory), "st_size": 3, "code": 0},
                     {"path": str(directory / "file"), "st_size": 7, "code": 0}])
    source = tmp_path / "inventory.jsonl"
    source.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    output = tmp_path / "usage.sqlite3"
    assert publish(source, root, output) == 0
    with closing(sqlite3.connect(output)) as db:
        assert db.execute("SELECT apparent_bytes, entries FROM directories WHERE path='.'").fetchone() == (25013, 5001)
        assert db.execute("SELECT apparent_bytes, entries FROM directories WHERE path='folder-2499'").fetchone() == (10, 2)
        assert db.execute("SELECT count(*) FROM directories").fetchone() == (2501,)


def test_missing_ancestors_and_client_contract(tmp_path):
    try:
        from gbi_data import usage as client
    except ModuleNotFoundError as error:
        if error.name != "gbi_data":
            raise
        raise unittest.SkipTest("set PYTHONPATH to run the optional GBI consumer check") from error
    root = tmp_path.resolve() / "alice"
    root.mkdir()
    output = root / ".gbi" / "usage.sqlite3"
    source = tmp_path / "inventory.jsonl"
    source.write_text(json.dumps({"path": str(root / "a" / "b" / "c" / "file"), "st_size": 17, "code": 0}) + "\n")
    assert publish(source, root, output) == 0
    site = SimpleNamespace(user="alice", values={"usage_db": str(output), "lfs_bin": "absent-lfs"}, roots={"lustre": root}, path=tmp_path / "site.conf")
    report = client.report(site, None, 4, 20)
    assert report["summary"] == (17, 1)
    assert report["rows"] == [("a", 17, 1), ("a/b", 17, 1), ("a/b/c", 17, 1)]
    assert (output.stat().st_mode & 0o777) == 0o600
    assert not list(output.parent.glob("*-wal"))


def test_invalid_snapshot_preserves_report(tmp_path):
    root = tmp_path / "alice"
    root.mkdir()
    source = parquet(tmp_path, root)
    output = tmp_path / "usage.sqlite3"
    assert publish(source, root, output) == 0
    previous = output.read_bytes()
    for timestamp in ["later", "2026-09-20", "2999-01-01T00:00:00Z"]:
        assert publish(source, root, output, timestamp) == 2
        assert output.read_bytes() == previous


def test_invalid_selected_rows_preserve_report(tmp_path):
    root = tmp_path / "alice"
    root.mkdir()
    source = parquet(tmp_path, root)
    output = tmp_path / "usage.sqlite3"
    assert publish(source, root, output) == 0
    previous = output.read_bytes()
    bad = tmp_path / "bad.jsonl"
    for row in [
        {"path": str(root / "file"), "code": 0, "st_size": None},
        {"path": str(root / "file"), "code": None, "st_size": 1},
        {"path": str(root / "file"), "code": 0, "st_size": -1},
        {"path": str(root) + "/../elsewhere", "code": 0, "st_size": 1},
    ]:
        bad.write_text(json.dumps(row) + "\n")
        assert publish(bad, root, output) == 2
        assert output.read_bytes() == previous


def test_malformed_input_preserves_previous_report(tmp_path):
    root = tmp_path / "alice"
    root.mkdir()
    source = parquet(tmp_path, root)
    output = tmp_path / "usage.sqlite3"
    assert publish(source, root, output) == 0
    previous = output.read_bytes()
    malformed = tmp_path / "malformed.jsonl"
    malformed.write_text("{unfinished\n")
    assert publish(malformed, root, output) == 2
    assert output.read_bytes() == previous


class Publications(unittest.TestCase):
    pass


def case(function):
    def run(self):
        with tempfile.TemporaryDirectory() as directory:
            function(Path(directory))
    return run


for name, function in list(globals().items()):
    if name.startswith("test_"):
        setattr(Publications, name, case(function))


if __name__ == "__main__":
    unittest.main()

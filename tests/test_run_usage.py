import json
import os
import sqlite3
from pathlib import Path
import subprocess
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from pipelines import run_usage


class RunUsage(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.root = self.base / "lustre" / "alice"
        self.root.mkdir(parents=True)
        self.inventory = self.base / "inventory"
        self.output = self.base / "home" / "alice" / ".gbi" / "usage.sqlite3"
        self.output.parent.mkdir(parents=True)
        self.args = SimpleNamespace(
            root=str(self.root), inventory_dir=str(self.inventory), output=str(self.output),
            threads=4, processes=2, nodes=3,
            fss_usage=None,
            stat_command=str(self.base / "stat_srun"),
            publisher=str(self.base / "usage.py"),
        )

    def tearDown(self):
        self.temp.cleanup()

    @patch("pipelines.run_usage.revision", return_value="abc123")
    @patch("pipelines.run_usage.subprocess.run")
    def test_success_runs_scan_then_publisher_and_writes_manifest(self, run, _revision):
        def command(argv, **_kwargs):
            if Path(argv[0]).name == "stat_srun":
                output = Path(next(value.removeprefix("--outfile=") for value in argv
                                   if value.startswith("--outfile=")))
                output.write_text('{"path":"x","st_size":1,"code":0}\n')
            else:
                with sqlite3.connect(self.output) as db:
                    db.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)")
                    db.executemany("INSERT INTO metadata VALUES (?, ?)", [
                        ("entries", "7"), ("apparent_bytes", "4096"),
                        ("snapshot_at", "2026-09-30T00:00:00+00:00"),
                    ])
            return SimpleNamespace(returncode=0, stdout="")

        run.side_effect = command
        self.inventory.mkdir(mode=0o700)
        older = self.inventory / "lustre-20260923T000000Z-00000000.jsonl"
        older.write_text("old\n")
        manifest = run_usage.run(self.args)
        payload = json.loads(manifest.read_text())
        self.assertEqual(payload["status"], "complete")
        self.assertEqual(payload["revision"], "abc123")
        self.assertEqual(Path(payload["report"]), self.output.resolve())
        self.assertTrue(Path(payload["inventory"]).is_file())
        self.assertEqual(run.call_count, 2)
        self.assertIn("--threads=4", run.call_args_list[0].args[0])
        self.assertEqual((payload["report_entries"], payload["report_apparent_bytes"]),
                         ("7", "4096"))
        self.assertGreater(payload["report_bytes"], 0)
        self.assertFalse(older.exists())
        self.assertEqual([path.name for path in self.inventory.glob("lustre-*.jsonl")],
                         [Path(payload["inventory"]).name])

    @patch("pipelines.run_usage.subprocess.run",
           side_effect=subprocess.CalledProcessError(1, ["scan"]))
    def test_scan_failure_retains_previous_outputs_and_removes_pending(self, _run):
        self.inventory.mkdir(mode=0o700)
        previous_manifest = self.inventory / "latest.json"
        previous_manifest.write_text("old\n")
        self.output.write_bytes(b"old sqlite")
        with self.assertRaises(subprocess.CalledProcessError):
            run_usage.run(self.args)
        self.assertEqual(previous_manifest.read_text(), "old\n")
        self.assertEqual(self.output.read_bytes(), b"old sqlite")
        self.assertFalse(list(self.inventory.glob(".*.jsonl")))

    @patch("pipelines.run_usage.subprocess.run",
           side_effect=subprocess.CalledProcessError(1, ["scan"]))
    def test_unfinished_inventory_of_a_killed_run_is_removed_under_the_lock(self, _run):
        self.inventory.mkdir(mode=0o700)
        stale = self.inventory / ".lustre-20260930T184557Z-bdb332e3-m895l4mr.jsonl"
        stale.write_text("partial\n")
        kept = self.inventory / "lustre-20260923T000000Z-00000000.jsonl"
        kept.write_text("complete\n")
        with self.assertRaises(subprocess.CalledProcessError):
            run_usage.run(self.args)
        self.assertFalse(stale.exists())
        self.assertTrue(kept.exists())

    def test_rejects_non_private_inventory_directory(self):
        self.inventory.mkdir(mode=0o755)
        with self.assertRaisesRegex(ValueError, "caller-owned and private"):
            run_usage.run(self.args)

    def test_nonblocking_lock_rejects_overlap(self):
        self.inventory.mkdir(mode=0o700)
        lock = (self.inventory / ".collection.lock").open("a+")
        try:
            import fcntl
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(RuntimeError, "already active"):
                run_usage.run(self.args)
        finally:
            lock.close()


if __name__ == "__main__":
    unittest.main()

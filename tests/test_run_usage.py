import json
import os
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
            if argv[0] == self.args.stat_command:
                output = Path(next(value.removeprefix("--outfile=") for value in argv
                                   if value.startswith("--outfile=")))
                output.write_text('{"path":"x","st_size":1,"code":0}\n')
            else:
                self.output.write_bytes(b"sqlite")
            return SimpleNamespace(returncode=0, stdout="")

        run.side_effect = command
        manifest = run_usage.run(self.args)
        payload = json.loads(manifest.read_text())
        self.assertEqual(payload["status"], "complete")
        self.assertEqual(payload["revision"], "abc123")
        self.assertEqual(Path(payload["report"]), self.output.resolve())
        self.assertTrue(Path(payload["inventory"]).is_file())
        self.assertEqual(run.call_count, 2)
        self.assertIn("--threads=4", run.call_args_list[0].args[0])

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

"""Run the real launcher, qpipe roles and publisher on a small local tree.

srun is replaced by a pass-through that drops its own options and runs the
role command locally, so every role's real argument parser is exercised.
"""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest

PROJECT = Path(__file__).parents[1]
HAVE_RUNTIME = (
    importlib.util.find_spec("qpipe") is not None
    and importlib.util.find_spec("duckdb") is not None
    and (Path(sys.executable).parent / "orchestrator").exists()
)


@unittest.skipUnless(HAVE_RUNTIME, "needs the project environment with the usage extra")
class ScanEndToEnd(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name).resolve()
        bin_dir = self.base / "bin"
        bin_dir.mkdir()
        (bin_dir / "scontrol").write_text("#!/bin/sh\nprintf '%s\\n' 127.0.0.1\n")
        (bin_dir / "srun").write_text(textwrap.dedent(f"""
            #!{sys.executable}
            import os, sys
            argv = sys.argv[1:]
            start = argv.index({sys.executable!r})
            os.execv(argv[start], argv[start:])
        """).lstrip())
        for path in bin_dir.iterdir():
            path.chmod(path.stat().st_mode | stat.S_IXUSR)
        self.env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}",
                        SLURM_JOB_NODELIST="127.0.0.1", LUSTRE_DLM_PYTHON=sys.executable,
                        LUSTRE_DLM_REVISION="test")
        self.root = self.base / "lustre" / "alice"
        (self.root / "results" / "nested").mkdir(parents=True)
        (self.root / "results" / "a.bin").write_bytes(b"x" * 1000)
        (self.root / "results" / "nested" / "b.bin").write_bytes(b"y" * 24)
        (self.root / "notes.txt").write_text("hello")

    def tearDown(self):
        self.temp.cleanup()

    def test_launcher_scans_every_entry(self):
        output = self.base / "scan.jsonl"
        subprocess.run([str(PROJECT / "stat_srun"), f"--prefix={self.root}",
                        f"--outfile={output}", "--threads=2"],
                       env=self.env, check=True, timeout=120, capture_output=True)
        paths = {json.loads(line)["path"] for line in output.read_text().splitlines()}
        self.assertTrue({str(self.root / "notes.txt"), str(self.root / "results" / "a.bin"),
                         str(self.root / "results" / "nested" / "b.bin")} <= paths)

    def test_two_collections_on_one_node_use_separate_pipes(self):
        outputs = [self.base / "a.jsonl", self.base / "b.jsonl"]
        runs = [subprocess.Popen([str(PROJECT / "stat_srun"), f"--prefix={self.root}",
                                  f"--outfile={output}", "--threads=2"],
                                 env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                for output in outputs]
        for run in runs:
            _, stderr = run.communicate(timeout=120)
            self.assertEqual(run.returncode, 0, stderr[-2000:])
        counts = [len(output.read_text().splitlines()) for output in outputs]
        self.assertEqual(counts[0], counts[1])

    def test_entrypoint_publishes_report_and_manifest(self):
        inventory = self.base / "inventory"
        report = self.base / "home" / ".gbi" / "usage.sqlite3"
        report.parent.mkdir(parents=True)
        subprocess.run([sys.executable, str(PROJECT / "pipelines" / "run_usage.py"),
                        "--root", str(self.root), "--inventory-dir", str(inventory),
                        "--output", str(report), "--threads", "2"],
                       env=self.env, check=True, timeout=180, capture_output=True)
        manifest = json.loads((inventory / "latest.json").read_text())
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(manifest["revision"], "test")
        with sqlite3.connect(report) as db:
            metadata = dict(db.execute("SELECT key, value FROM metadata"))
        self.assertEqual(metadata["status"], "complete")
        self.assertEqual(manifest["report_entries"], metadata["entries"])
        self.assertGreaterEqual(int(metadata["entries"]), 5)


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(HAVE_RUNTIME and os.getuid() != 0, "needs the project environment, not root")
class UnreadableDirectory(ScanEndToEnd):
    """An owner scan meets a folder it may not open: publish, but as partial."""

    def setUp(self):
        super().setUp()
        self.locked = self.root / "service-owned"
        self.locked.mkdir()
        (self.locked / "hidden.bin").write_bytes(b"z" * 10)
        self.locked.chmod(0)

    def tearDown(self):
        self.locked.chmod(0o700)
        super().tearDown()

    def test_launcher_scans_every_entry(self):
        output = self.base / "scan.jsonl"
        subprocess.run([str(PROJECT / "stat_srun"), f"--prefix={self.root}",
                        f"--outfile={output}", "--threads=2"],
                       env=self.env, check=True, timeout=120, capture_output=True)
        records = {json.loads(line)["path"]: json.loads(line) for line in output.read_text().splitlines()}
        self.assertEqual(records[str(self.locked)]["code"], 13)
        self.assertNotIn(str(self.locked / "hidden.bin"), records)

    def test_entrypoint_publishes_report_and_manifest(self):
        inventory = self.base / "inventory"
        report = self.base / "home" / ".gbi" / "usage.sqlite3"
        report.parent.mkdir(parents=True)
        subprocess.run([sys.executable, str(PROJECT / "pipelines" / "run_usage.py"),
                        "--root", str(self.root), "--inventory-dir", str(inventory),
                        "--output", str(report), "--threads", "2"],
                       env=self.env, check=True, timeout=180, capture_output=True)
        manifest = json.loads((inventory / "latest.json").read_text())
        self.assertEqual((manifest["report_status"], manifest["report_unreadable_directories"]),
                         ("partial", "1"))
        with sqlite3.connect(report) as db:
            metadata = dict(db.execute("SELECT key, value FROM metadata"))
        self.assertEqual((metadata["status"], metadata["complete_input"]), ("partial", "false"))

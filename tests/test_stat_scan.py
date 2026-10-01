"""The scanner treats paths removed during a scan as no longer in the tree."""

import importlib.util
import os
from pathlib import Path
import unittest

HAVE_QPIPE = importlib.util.find_spec("qpipe") is not None


@unittest.skipUnless(HAVE_QPIPE, "needs qpipe from the project environment")
class LiveTree(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            "dlm_stat", Path(__file__).parents[1] / "pipelines" / "stat.py")
        self.stat = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.stat)
        self.records, self.discovered = [], []

    def scan(self, path):
        self.stat.scan({"path": str(path)}, self.records.append, self.discovered.append)

    def test_directory_removed_before_listing_is_skipped(self):
        self.scan("/nonexistent/removed-directory")
        self.assertEqual((self.records, self.discovered), ([], []))

    def test_entry_removed_between_readdir_and_stat_is_skipped(self):
        class Vanished:
            name, path = "gone.bin", "/x/gone.bin"

            def is_dir(self, follow_symlinks):
                return False

            def stat(self, follow_symlinks):
                raise FileNotFoundError(2, "gone")

        class Kept(Vanished):
            name, path = "kept.bin", "/x/kept.bin"

            def stat(self, follow_symlinks):
                return os.stat_result((0o100600, 0, 0, 1, 0, 0, 7, 0, 0, 0))

        children = list(self.stat.subdirs([Vanished(), Kept()], self.records.append))
        self.assertEqual(children, [])
        self.assertEqual([record["path"] for record in self.records], ["/x/kept.bin"])
        self.assertEqual(self.records[0]["code"], 0)


if __name__ == "__main__":
    unittest.main()

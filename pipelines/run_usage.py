#!/usr/bin/env python3
"""Collect one owner inventory and publish its usage report atomically."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import uuid


def private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("inventory directory must be caller-owned and private")


def report_counts(report: Path) -> dict[str, str]:
    """Read the published report's own totals for the completion manifest."""
    keys = ("entries", "apparent_bytes", "snapshot_at", "fss_observed_at")
    with sqlite3.connect(f"{report.as_uri()}?mode=ro", uri=True) as db:
        rows = dict(db.execute(
            f"SELECT key, value FROM metadata WHERE key IN ({','.join('?' * len(keys))})", keys
        ))
    return {key: rows[key] for key in keys if key in rows}


def prune_inventories(inventory_dir: Path, keep: Path) -> None:
    """Keep only the inventory behind the current report; each can be many GB."""
    for path in inventory_dir.glob("lustre-*.jsonl"):
        if path != keep and path.is_file() and not path.is_symlink():
            path.unlink()


def revision(root: Path) -> str:
    configured = os.environ.get("LUSTRE_DLM_REVISION", "").strip()
    if configured:
        return configured
    result = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def run(args) -> Path:
    project = Path(__file__).resolve().parents[1]
    owner_root = Path(args.root).resolve(strict=True)
    if not owner_root.is_dir() or owner_root.stat().st_uid != os.getuid():
        raise ValueError("root must be an existing directory owned by the caller")
    inventory_dir = Path(args.inventory_dir).expanduser().resolve()
    private_directory(inventory_dir)
    output = Path(args.output).expanduser().resolve()
    stamp = datetime.now(timezone.utc).replace(microsecond=0)
    snapshot_at = stamp.isoformat().replace("+00:00", "Z")
    basename = stamp.strftime("lustre-%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    completed = inventory_dir / f"{basename}.jsonl"
    manifest = inventory_dir / "latest.json"
    lock_path = inventory_dir / ".collection.lock"
    stat_command = Path(args.stat_command or project / "stat_srun").resolve()
    publisher = Path(args.publisher or project / "pipelines" / "usage.py").resolve()

    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another usage collection is already active") from error
        fd, pending_name = tempfile.mkstemp(
            prefix=f".{basename}-", suffix=".jsonl", dir=inventory_dir
        )
        os.close(fd)
        pending = Path(pending_name)
        try:
            subprocess.run([
                str(stat_command), f"--prefix={owner_root}", f"--outfile={pending}",
                f"--threads={args.threads}", f"--process={args.processes}",
                f"--nodes={args.nodes}",
            ], check=True)
            os.replace(pending, completed)
            subprocess.run([
                sys.executable, str(publisher), "--input", str(completed),
                "--root", str(owner_root), "--output", str(output),
                "--completed", "--snapshot-at", snapshot_at,
                *(["--fss-usage", str(Path(args.fss_usage).resolve())]
                  if args.fss_usage else []),
            ], check=True)
            payload = {
                "status": "complete", "snapshot_at": snapshot_at,
                "published_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "root": str(owner_root), "inventory": str(completed),
                "report": str(output), "revision": revision(project),
                "report_bytes": output.stat().st_size,
                **{f"report_{key}": value for key, value in report_counts(output).items()},
            }
            fd, temporary_name = tempfile.mkstemp(
                prefix=".latest-", suffix=".json", dir=inventory_dir
            )
            with os.fdopen(fd, "w", encoding="utf-8") as temporary:
                json.dump(payload, temporary, sort_keys=True)
                temporary.write("\n")
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_name, manifest)
            prune_inventories(inventory_dir, completed)
            return manifest
        finally:
            pending.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, help="owner's canonical Lustre root")
    parser.add_argument("--inventory-dir", required=True, help="private completed inventories and manifest")
    parser.add_argument("--output", required=True, help="owner-scoped SQLite usage report")
    parser.add_argument("--fss-usage", help="owner-scoped OCI FSS usage JSON")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--processes", type=int, default=1)
    parser.add_argument("--nodes", type=int, default=1)
    parser.add_argument("--stat-command", help=argparse.SUPPRESS)
    parser.add_argument("--publisher", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if min(args.threads, args.processes, args.nodes) < 1:
        parser.error("threads, processes and nodes must be positive")
    try:
        print(run(args))
        return 0
    except Exception as error:
        print(f"usage collection failed; previous outputs retained: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

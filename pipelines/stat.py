#!/usr/bin/env python

from __future__ import annotations

import sys
import argparse

from collections.abc import Iterable, Iterator
from dataclasses     import dataclass
from pathlib         import Path
from typing          import Any, Literal

from qpipe.work import (
    Coordinator, Discover, Discovery, Emit, Job, Permanent, Pipeline, Pipes,
    Spec, Worker, run
)


FIELDS = ("st_size", "st_atime", "st_mtime", "st_ctime")


def object_record(name: str, path:str, code:int, obj: Any) -> dict[str, Any]:
    """
    ObjectSummary →  plain dict containing only the fields the listing
    populated.
    """
    rec: dict[str, Any] = {
        "name": name,
        "path": path,
        "code": code
    }

    for attr in FIELDS:
        v = getattr(obj, attr, None)
        if v is not None:
            rec[attr] = v

    return rec


def scan(spec: Spec, result: Emit, discover: Discover) -> None:
    """
    Stat one folder's content. Return stats; and discover more work (by way of
    child directories)
    """
    parent   = Path(spec.get("path", "."))
    children = [x      for x in parent.iterdir()]
    cdirs    = [str(x) for x in children if x.is_dir(follow_symlinks=False)]
    cfiles   = [x      for x in children if not x.is_dir(follow_symlinks=False)]

    for f in cfiles:
        try:
            result(object_record(f.name, str(f), 0, f.lstat()))
        except:
            result(object_record(f.name, str(f), 1, object()))

    if cdirs:
        discover({"children": cdirs})

    # This completes the current task
    result(object_record(parent.name, str(parent), parent.lstat()))


def make_coordinator(args: argparse.Namespace) -> Coordinator:
    """
    Build the listing strategy: one seed (the --prefix subtree), children
    begotten by workers expand at depth + 1, prefixes dedup by identity.
    """
    seed_prefix = args.prefix

    def spec(prefix: str, depth: int) -> Spec:
        """One prefix's work payload"""
        return {"path": prefix, "depth": depth}

    def seeds() -> Iterator[Spec]:
        """The single root task: the seed subtree at depth 0."""
        yield spec(seed_prefix, 0)

    def expand(parent: Spec, disc: Discovery) -> Iterable[Spec]:
        """
        Begotten children become tasks one level deeper than their parent.
        """
        return (
            spec(child, parent["depth"] + 1) for child in disc.get("children", [])
        )

    return Coordinator(
        seeds=seeds, expand=expand, key_of=lambda s: s["path"], dedup="parent"
    )


def make_worker(args: argparse.Namespace) -> Worker:
    """
    Build the listing worker: one prefix per task, object records to the
    results pipe, child prefixes begotten back to the coordinator.
    """
    def process(
            _state: NoneType, job: Job, result: Emit, discover: Discover
        ) -> None:
        """Scan one prefix."""
        try:
            scan(job.spec, result, discover)
        except:
            pass

    return Worker(setup=lambda: None, process=process)


def add_coordinator_args(p: argparse.ArgumentParser) -> None:
    """stat-specific coordinator flags."""
    p.add_argument("prefix")


PIPELINE = Pipeline(
    name="stat",
    describe="Stat an entire directory tree",
    default_pipes=Pipes(
        work="127.0.0.1:9101",
        completions="127.0.0.1:9102",
        results="127.0.0.1:9103",
        wait=30.0
    ),
    make_coordinator=make_coordinator,
    make_worker=make_worker,
    add_coordinator_args=add_coordinator_args
)


if __name__ == "__main__":
    sys.exit(run(PIPELINE))

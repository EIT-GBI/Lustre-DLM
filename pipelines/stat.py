#!/usr/bin/env python

from __future__ import annotations

import os
import sys
import argparse

from collections.abc import Iterable, Iterator
from itertools       import batched
from pathlib         import Path
from typing          import Any

from qpipe import QpipeError
from qpipe.work import (
    Coordinator, Discover, Discovery, Emit, Job, Permanent, Pipeline, Pipes,
    Spec, Worker, run
)


# subdirectories per beget frame
DISCOVER_EVERY = 10_000

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


def subdirs(it, result: Emit) -> Iterator[str]:
    """
    Stream one directory: emit a record per non-directory entry as it goes,
    and yield each subdirectory's path for the caller to batch.
    """
    for e in it:
        try:
            if e.is_dir(follow_symlinks=False):
                yield e.path
                continue
            result(object_record(
                e.name, e.path, 0, e.stat(follow_symlinks=False)
            ))
        except QpipeError:
            raise                        # pipe is gone: the harness must see it
        except OSError:
            result(object_record(e.name, e.path, 1, object()))


def scan(spec: Spec, result: Emit, discover: Discover) -> None:
    """One readdir, one lstat per entry; at most DISCOVER_EVERY paths held."""
    path = spec.get("path", ".")
    try:
        it = os.scandir(path)
    except OSError as err:
        raise Permanent(f"cannot list '{path}': {err}") from err

    with it:
        for chunk in batched(subdirs(it, result), DISCOVER_EVERY):
            discover({"children": list(chunk)})

    result(object_record(os.path.basename(path), path, 0, os.lstat(path)))


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
        scan(job.spec, result, discover)

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

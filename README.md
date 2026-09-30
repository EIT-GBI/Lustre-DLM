# Lustre-DLM

Data Lifecycle Management Utilities for Lustre and FSS.

## Fast folder reports for GBI

`gbi data usage` reads an indexed folder summary instead of walking Lustre or
parsing the full inventory on every request. `pipelines/usage.py` creates that
summary from a **completed** scanner JSONL or Parquet inventory. It does not
start a scan, install a schedule, change quotas, or tag project IDs.

The publisher needs DuckDB (the optional `usage` dependency); the GBI reader
uses Python's standard library. Run it as the user who will read the report:

```sh
python pipelines/usage.py \
  --input /path/to/lustre_2026.09.20.4.parquet \
  --root /mnt/lustre/users/alice \
  --output /mnt/gbi-shared/home/alice/.gbi/usage.sqlite3 \
  --completed --snapshot-at 2026-09-20T15:53:30Z
```

Use the original scan's timestamp, not the later conversion time. Parquet
input can be one file or a dataset directory; an already partitioned user
subset avoids reading other partitions. JSONL is supported directly when no
conversion exists. Publication reads the input for validation and aggregation, so run a
large publication on an allocated CPU worker. The command reports each
publication phase while it runs. The owner-root selection is a
DuckDB view over that input rather than a second materialized copy. Parent
totals are grouped into private temporary Parquet, then directory metadata is
assigned separately. This avoids combining a large join and grouping operation.
Publication uses one DuckDB thread and does not preserve input row order.
DuckDB's configured limit applies to its managed execution memory, while row-group
decoding and the operating system can still add overhead. Grouping work can
spill to the private temporary directory. The SQLite folder index is built
after totals are complete, avoiding index rewrites during aggregation.
The default output is
`~/.gbi/usage.sqlite3`; GBI normally reads the same path in the user's FSS home.

A scheduled scan can call this command **after** its coordinator and collector
have completed successfully. `--completed` is that operator assertion, not a
request to finish an active scan. Scheduling stays with the inventory owner.
The publisher also checks source file identities, sizes and modification times
before and after aggregation. Failed selected records, missing sizes, invalid
paths, changing inputs and invalid timestamps leave the previous report intact.
Set `TMPDIR` to a directory on the worker's local disk with room for the
database and DuckDB spill files. Some clusters mount `/tmp` in RAM; check
before using it for a large report. The report is built in that scratch
directory, then the closed SQLite file is copied sequentially into a private
destination temporary (0600) before it atomically replaces the previous report; the parent directory must be owned by the caller and not
writable by others. Source files and their permissions stay unchanged.

### What the numbers mean

The scanner currently records raw `st_size` but no file type, allocated blocks
or inode identity. Reports therefore show **apparent size** and **inventory
entries**. Directory metadata and symlink lengths are included; hard links are
counted per recorded path. These totals differ from live Lustre quota allocation.
Folders are inferred from descendant paths; empty folders cannot be identified
individually from this schema. The source snapshot date and publication date
remain separate, and the CLI shows the snapshot's age.

Only paths equal to the selected owner root or below its literal slash boundary
are selected. No privileged service or new access grants are introduced.

### Publication format (version 1)

The SQLite `metadata(key, value)` table contains `schema_version`, `owner_uid`,
canonical `root`, `status`, `complete_input`, source `snapshot_at`, and
`published_at`, plus source path and aggregate counts. The publisher emits only
complete snapshots. `directories(path, parent_path, apparent_bytes, entries)`
holds relative directory paths; the root is `.` with an empty parent. Subtree
sums include each recorded entry once. A parent/size index supports ranked
folder queries without scanning inventory rows. GBI opens this database read-only.

Fixture tests also exercise the GBI consumer when its library is on PYTHONPATH:

```sh
PYTHONPATH=/path/to/GBI-Compute-Software-Modules/gbi/src/lib \
  python -m unittest discover -s tests -v
```

`tests/test_scan_end_to_end.py` runs the real `stat_srun`, qpipe roles and
publisher on a small local tree through a pass-through `srun`. It needs the
project environment itself (not `uv run --with`, which hides `orchestrator`):

```sh
uv sync --frozen --extra usage
.venv/bin/python -m unittest tests.test_scan_end_to_end -v
```

## Schedule-ready collection

`pipelines/run_usage.py` is the production boundary for one owner. Run it as
that owner inside an allocated Slurm job; it calls the existing `stat_srun`
collector, publishes only its completed JSONL, and writes `latest.json` only
after the owner-scoped SQLite report succeeds.

Production uses the pinned `lustre-dlm` Lmod module from
[GBI-Compute-Software-Modules](https://github.com/EIT-GBI/GBI-Compute-Software-Modules).
Each module version installs one release tag of this repository with the
frozen `uv.lock` (including the `usage` extra) and a uv-managed Python, and
provides `lustre-dlm-usage`, which sets `LUSTRE_DLM_PYTHON` and
`LUSTRE_DLM_REVISION` before running this entrypoint:

```sh
/mnt/gbi-shared/software/lustre-dlm/0.2.5/bin/lustre-dlm-usage \
  --root /mnt/lustre/users/OWNER \
  --inventory-dir /mnt/lustre/users/OWNER/.gbi/inventory \
  --output /mnt/gbi-shared/home/OWNER/.gbi/usage.sqlite3 \
  --threads 16 --processes 2
```

The weekly Prefect flow `storage-usage` in gbi-data-platform submits exactly
this as each owner. For local development, `uv run --project . --extra usage
python pipelines/run_usage.py ...` is equivalent; without `--extra usage`
DuckDB is not installed and publication fails.

Slurm resources: one node is enough for one owner. With a single-node
allocation `stat_srun` runs the bus, collector, coordinator and workers as
overlapping steps on that node; with more nodes, workers keep off the head
node as before. The largest current owner (about 83 million entries) needed
16 GiB and 1.5 hours to publish; request 48 GiB and at least 12 hours.
Temporary DuckDB and uv state use the job's worker-local `$TMPDIR`. DuckDB's managed
memory is 40% of the job's `SLURM_MEM_PER_NODE` (512 MB outside Slurm);
`LUSTRE_DLM_DUCKDB_MEMORY` overrides it.

When the site collector has produced an owner-scoped OCI measurement, pass
`--fss-usage /path/to/owner.json`. The JSON contract is `owner_uid`,
`used_bytes`, `observed_at`, optional `files` (OCI quota accounting has no file
count), optional `limit_bytes`, and optional `source`. The publisher rejects foreign UIDs, negative counters, future or
timezone-free observations, and control characters before replacing a report.
OCI FSS reports logical data bytes and excludes snapshots.

An owner may not be able to open every directory under their root (for
example a service-owned folder). The scanner records such a directory as one
entry with code 13 (EACCES) instead of failing; the publisher keeps it, sets
`unreadable_directories`, and publishes the report as `partial`, which
`gbi data usage` shows. Any other scan or stat failure still fails the run.

The inventory directory is caller-owned mode 0700. A non-blocking lock
rejects overlapping collections. Scanner or publisher failure removes only the
unfinished inventory and leaves the previous report and completion manifest
untouched. After a successful publication only the newest completed JSONL is
kept (older ones are removed; each can be many GB). Because the inventory
lives under the owner's root, the next scan counts it in that owner's usage.
`latest.json` records the snapshot time, paths, `LUSTRE_DLM_REVISION`, the
report size and the report's own entry, byte and FSS observation metadata.

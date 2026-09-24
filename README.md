# Lustre-DLM

Data Lifecycle Management Utilities for Lustre and FSS.

## Fast folder reports for GBI

`gbi data usage` reads a small indexed summary instead of walking Lustre or
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
large publication on an allocated CPU worker. The owner-root selection is a
DuckDB view over that input rather than a second materialized copy; DuckDB's
configured limit applies to its managed execution memory, while row-group
decoding and the operating system can still add overhead. Grouping work can
spill to the private temporary directory. The default output is
`~/.gbi/usage.sqlite3`; GBI normally reads the same path in the user's FSS home.

A scheduled scan can call this command **after** its coordinator and collector
have completed successfully. `--completed` is that operator assertion, not a
request to finish an active scan. Scheduling stays with the inventory owner.
The publisher also checks source file identities, sizes and modification times
before and after aggregation. Failed selected records, missing sizes, invalid
paths, changing inputs and invalid timestamps leave the previous report intact.
The new SQLite file is private (0600) and closed before it atomically replaces
the previous report; the parent directory must be owned by the caller and not
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

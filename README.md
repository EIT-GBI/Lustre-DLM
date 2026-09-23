# Lustre-DLM
Data Lifecycle Management Utilities for Lustre and FSS

## Owner usage publication

`pipelines/usage.py` turns one completed Parquet inventory into an
owner-scoped SQLite publication. Install the optional producer dependency
with `pip install -e '.[usage]'`.

```sh
python pipelines/usage.py \
  --input /path/to/lustre-inventory.parquet \
  --root /mnt/lustre/users/alice \
  --completed --snapshot-at 2026-09-20T04:00:00Z
```

The default publication is `$HOME/.gbi/usage.sqlite3`; an explicit output
path may point only to an existing regular file owned by the caller (or an
existing caller-owned directory). The source must be marked `--completed`.
`--snapshot-at` is the inventory's source timestamp and is retained exactly;
`published_at` records when this summary was generated. The producer checks
all source partition identities and mtimes before and after aggregation and
atomically replaces the output only for a complete, unchanged, error-free
snapshot. A failed attempt leaves the previous complete file in place.

The SQLite schema is version 1. It stores one metadata row and directory-only
rollups (`path`, `parent_path`, `apparent_entry_bytes`, `entry_count`, and
`error_count`). Since the inventory has no file-type or allocated-block
column, `apparent_entry_bytes` is the sum of raw `st_size` for each selected
entry, including directory and link records; it must not be read as file
bytes or live quota. Empty directories absent from the inventory cannot be
inferred. The source may contain other users: only the exact owner root and
its literal slash boundary are selected. Scheduling, project-ID tagging, and
query presentation remain outside this utility.

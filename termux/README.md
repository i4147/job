# `run_parallel_lite` — memory-friendly parallel helper for Termux

A **separate folder** with a standalone, stdlib-only implementation for
Termux / Android. Copy just `termux/run_parallel_lite.py` to the phone; it has
no imports from the main `run_parallel` package, no joblib and no pytest.

## Why a separate version?

| Constraint | Consequence | What the lite version does |
|---|---|---|
| 1–4 GB RAM, **no swap** | one OOM kill ends the app | never materialises the file list or result list in the streaming path |
| Android restricts `fork` / POSIX semaphores | process pools are fragile and expensive | defaults to a **2-thread** pool; no pickling needed |
| Large media files | `read()` would allocate file-sized buffers | hashes in fixed 1 MiB (configurable) blocks |
| Long directory walks | `os.walk` builds per-directory lists | `os.scandir` DFS generator yields one path at a time |
| Slow/unresponsive files | unbounded queues grow | `max_pending` (default `2 * workers`) caps in-flight futures |
| Long-running processes | Python grows RSS over time | optional `gc_every`, `max_tasks_per_child`, `resource`-based peak-RSS report |

Peak working memory is roughly `workers × block_size` plus the bounded future
window and reorder buffer — it does **not** grow with the number of files.

## API

```python
from termux.run_parallel_lite import LiteParallel, LiteResult, Outcome, hash_folder

# 1) fully streaming: file discovery + hashing + results are all generators
for outcome in hash_folder("/sdcard/Download", workers=2, max_pending=4,
                           block_size=1 << 20, gc_every=50, limit=1000):
    if outcome.ok:
        path, sha256, size = outcome.value
        print(sha256, size, path)
    else:
        print("failed", outcome.item, outcome.error)

# 2) generic bounded parallel map
runner = LiteParallel(workers=2, backend="thread", on_error="collect")
for outcome in runner.imap(abs, [-3, -1, 0, 2]):   # generator
    print(outcome.value)
result = runner.run(abs, [-3, -1, 0, 2])           # LiteResult (materialises)
print(result.unwrap(), result.peak_rss_kb)
```

`LiteParallel` options: `workers`, `max_pending`, `backend`
(`thread` default / `process` / `serial`), `on_error` (`collect` / `skip` /
`raise`), `gc_every`, `max_tasks_per_child`, `timeout`, `ordered`,
`max_reorder`.

- Worker exceptions are normalised to `RuntimeError("OriginalType: message")`
  so no traceback frames (and the buffers they reference) are retained or
  pickled.
- `ordered=True` keeps input order using a **bounded** reorder buffer
  (`max_reorder`, default `2 * max_pending`). If one task is very slow, the
  oldest buffered result is emitted out of order rather than letting the
  buffer grow forever.
- `backend="serial"` uses an inline executor: zero extra processes/threads,
  useful for debugging on a phone.

## CLI

```bash
python3 termux/run_parallel_lite.py                         # $HOME
python3 termux/run_parallel_lite.py /sdcard/DCIM \
    --workers 2 --max-pending 4 --block-size 65536 \
    --algorithm sha256 --gc-every 50 --limit 500 --on-error collect
python3 termux/example_usage.py /sdcard/Download
```

Stdout: `<sha256>  <bytes>  <path>` per file. Stderr: errors plus a summary with
file count, MB, seconds, MB/s and **peak RSS** (from `resource`/`/proc`).

Output on the sandbox (2 MiB fixture, 2 threads):

```
8 files, 2.10 MB, 0 errors, 0.008s, 262.5 MB/s, peak RSS 19.9 MiB
```

## Differences from the main `run_parallel` package

| main package | lite version |
|---|---|
| 3 backend classes (mp.Pool, 3 executors, joblib) | 1 class, thread-first (optional process/serial) |
| `ParallelResult` with rich metadata | `LiteResult` / `Outcome`, no extra metadata |
| `multiprocessing` chunking / memmap / subinterpreters | none (avoided on purpose) |
| collects results into a list by default | generators everywhere by default |
| needs `joblib` for class 3 (optional) | stdlib only, always |
| tuned for multi-core Linux servers | tuned for low-RAM Android devices |

Use the main package on a workstation; use this folder (or just the single
file) on Termux. `examples/hash_folder_example.py` runs both side by side.

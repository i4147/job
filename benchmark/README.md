# Multiprocessing benchmark

This folder is a self-contained benchmark for the nine multiprocessing
implementations in this repository (`jobutil1.py` … `jobutil6.py`,
`jobutils_co.py`, `prunner.py`, `job_helper.py`). It hashes the files of a user
defined folder, records timings as JSON, and reports the top 5 methods **with
the exact function each method uses**.

## Function under test

`benchmark/workers.py::hash_file(path, algorithm="sha256")` returns
`(path, hexdigest)` for one file; `hash_folder(folderpath=None, ...)` is the
serial reference that hashes every file in a folder. When `folderpath` is not
given, **the home folder is used** (`Path.home()`).

Safety rails (all configurable) keep the default home-folder run bounded:
hidden directories, VCS/cache/vendor directories and symlinks are skipped;
files larger than 8 MiB are skipped; at most 256 files / 64 MiB are hashed.

## Run it

```bash
# from the repository root
python3 benchmark/run_benchmark.py                       # hashes $HOME
python3 benchmark/run_benchmark.py /path/to/folder       # your own folder
python3 benchmark/run_benchmark.py /tmp/fixture --repeats 5 --workers 4
python3 benchmark/run_benchmark.py --list-methods        # registry + primitives
python3 benchmark/run_benchmark.py --dry-run             # scan files, no timing
```

Verbose output is **on by default** (`--quiet` reduces it). Every run prints the
configuration, warmup progress, per-repeat timings, digest verification, the
top 5 and the output paths.

Important flags:

| flag | meaning |
|---|---|
| `--repeats N` | timed runs per method (default 3) |
| `--warmup-runs N` | warmup runs per method, results discarded (default 1) |
| `--warmup-files N` | files used in warmup (0 = all) |
| `--workers N` | worker processes/threads (default `min(4, cpu_count)`) |
| `--chunksize N` | chunksize for implementations that accept one (default 1) |
| `--algorithm NAME` | any `hashlib` algorithm (default `sha256`) |
| `--max-files`, `--max-file-bytes`, `--max-total-bytes` | workload limits |
| `--include-hidden` | walk hidden dirs/files too |
| `--exclude SUBSTR` | skip paths containing a substring (repeatable) |
| `--methods PATTERNS` | comma separated id filters, e.g. `jobutil1,jobutil2` |
| `--interpreter auto\|force\|off` | InterpreterPoolExecutor policy |
| `--no-joblib` | skip joblib based methods |
| `--timeout S` | per-run watchdog, seconds (default 60) |
| `--seed N` | shuffle method order per repeat to reduce ordering bias |
| `--output PATH`, `--report PATH` | output locations |

### Python < 3.14

`concurrent.futures.InterpreterPoolExecutor` only exists on CPython 3.14+.
Methods that need it (`prunner.ParallelRunner.interpreter`,
`jobutil6.Parallel.cf_interpreter.map`,
`stdlib.InterpreterPoolExecutor.map`) are **optional**: on older interpreters
they are skipped and the skip reason is recorded in the JSON and the report.
All other methods run on 3.9+. `--interpreter off` disables them explicitly.
joblib methods are skipped the same way when joblib is not installed
(`pip install -r benchmark/requirements.txt` to include them).

## Outputs (in `benchmark/results/` by default)

* `benchmark_results.json` — full results: environment, workload, per-method
  warmup/timed runs, statistics, correctness checks, full ranking, `top5`
  (each row has `primitive` = the function used), status of skipped/failed
  methods, and the complete verbose log.
* `report.md` — human readable report with the top-5 table, an explicit
  "which function does each top-5 method use" section, reference times, full
  ranking, and skip/failure reasons.

Ranking uses the **median** wall time of the successful timed runs; speedup is
relative to a serial `hash_file` loop over the same files. Every timed run is
verified against the serial digests — a method that returns missing or wrong
hashes is marked failed and is not ranked. Serial/reference entries are timed
but excluded from the top-5 ranking.

## Files

| file | purpose |
|---|---|
| `workers.py` | picklable hashing workload + folder walker + serial `hash_folder` |
| `methods.py` | registry of all ~90 methods with their underlying primitive |
| `run_benchmark.py` | CLI, warmup phase, timed phase, verification, JSON + report |

The helper scripts used while building/fixing this (fixture generator, import
checker, single-run smoke test, results verifier) live in `../arena/`.

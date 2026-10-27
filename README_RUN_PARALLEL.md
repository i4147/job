# `run_parallel` — unified, class-based parallel helper

A single, consistent API over the three families of implementations that are
scattered across this repo (`jobutil1`…`jobutil6`, `jobutils_co`, `prunner`,
`job_helper`). It consolidates the best ideas from those files — index tagging,
bounded in-flight futures, explicit mp contexts, worker recycling, error
policies and streaming — and its defaults are chosen **from the measured
benchmark results** in `benchmark/results/benchmark_results.json`.

Target runtime: **CPython 3.14 / Linux**. The code also runs on 3.9+ with
graceful feature gates (see "Compatibility" below).

---

## 1. What the benchmark says

The repo benchmark hashed 64 files / 128 MiB, 5 repeats, 2 workers, on a 2-core
Linux box. Median wall time by primitive family:

| family | median | best method |
|---|---:|---|
| `concurrent.futures.ThreadPoolExecutor` | **0.0730 s** | `jobutil4.run_parallel.thread_pool` (0.0707 s) |
| `joblib.Parallel` | 0.1024 s | `jobutil2.mpf_joblib` (0.1023 s) |
| `concurrent.futures.ProcessPoolExecutor` | 0.1053 s | `jobutils_co.mpf.process_pool` (0.1000 s) |
| `multiprocessing.Pool` | 0.1262 s | `jobutil2.mpf_starmap` (0.1107 s) |
| serial | 0.1721 s | — |

Top 5 were all `ThreadPoolExecutor.map`/`submit`. `hashlib` releases the GIL
while hashing a block, so threads really do run in parallel and avoid process
start-up / pickling. `fork`-based pools beat `spawn`-based pools; `apply`
(sequential dispatch through a pool) was slowest of all.

`recommend()` reads that JSON at runtime, so re-running the benchmark updates
the advice. `python3 -m run_parallel` prints the full report.

### Lineage: which repo ideas were consolidated

| source file | idea reused |
|---|---|
| `jobutil3.py` (`mpf`, `_Catch`, `_Indexed`, `_Retry`) | guarded workers, index tagging to restore `imap_unordered` order, retries |
| `jobutil4.py` / `jobutil5.py` / `jobutils_co.py` (`run_parallel`, `mpf`) | one dispatcher for all seven `Pool` methods + process/thread executors |
| `jobutil1.py` / `jobutil2.py` | `apply_async`/`map_async` timeouts, `as_completed`, bounded deques, streaming `imap` |
| `jobutil6.py` (`Backend`, `PoolMethod`) | enum-driven backend/kind selection incl. optional `InterpreterPoolExecutor` |
| `prunner.py` (`ParallelRunner`) | bounded in-flight futures, `wait(FIRST_COMPLETED)`, `Stats`/rich result object |
| `job_helper.py` | joblib-compatible `delayed`/`Parallel` shim semantics |
| `benchmark/methods.py` + `benchmark_results.json` | primitive-level metadata (`result.primitive`) and measured defaults |

No existing implementation was rewritten — this is additive, so the benchmark
and the earlier PR remain untouched.

---

## 2. Three classes (exactly what was requested)

| # | class | backend | methods / kinds |
|---|---|---|---|
| 1 | `MpPoolRunner` | `multiprocessing.Pool` | `map`, `imap`, `starmap`, `apply_async`, `map_async`, `starmap_async`, `imap_unordered` |
| 2 | `ExecutorRunner` | `concurrent.futures` | `ProcessPoolExecutor`, `ThreadPoolExecutor`, `InterpreterPoolExecutor` (3.14+) |
| 3 | `JoblibRunner` | 3rd-party `joblib` | `joblib.Parallel` + `joblib.delayed`, backends `loky` / `threading` / `multiprocessing` / `sequential` |

Plus `RunParallel` (class-based facade), `run_parallel()` (one-shot function),
`make_runner()` (factory) and `recommend()`.

All three share:

- `run(items, func=None, **overrides) -> ParallelResult` (also `.map()`, `__call__`)
- `iter_results(items, func=None) -> Iterator` (streaming)
- `with runner as r:` to reuse one pool/executor across runs, `close()`
- `workers`, `chunksize`, `timeout`, `error_policy`, `retries`, `retry_delay`,
  `star`, `ordered`, `on_error`, `verbose`

`ParallelResult` has `values`, `errors: list[TaskError]`, `elapsed`, `submitted`,
`metadata` (incl. `primitive`), `ok`, `succeeded`, `failed`, `unwrap()`,
`__iter__`, `__len__`, `__getitem__`.

---

## 3. Quick start

```python
from run_parallel import run_parallel, RunParallel, ExecutorRunner, MpPoolRunner

def work(x):              # module-level = picklable under spawn/forkserver
    return x * x

# one shot, backend chosen from the benchmark results
result = run_parallel(work, range(100), backend="auto", workload="hashing")
print(result.unwrap(), result.primitive, result.elapsed)

# class-based, reusable
with RunParallel("executor", executor_kind="thread", workers=4, func=work) as r:
    first = r.run(range(10))
    second = r.map(range(10, 20))
    for value in r.imap(range(20, 25)):
        print(value)

# explicit backends
MpPoolRunner(work, method="imap_unordered", ordered=True, workers=4).run(range(100))
ExecutorRunner(work, kind="process", mode="submit", max_pending=16).run(range(100))
JoblibRunner(work, backend="loky", n_jobs=4, mmap_mode="r").run(range(100))
```

**Always** put the multiprocessing entry point behind
`if __name__ == "__main__":` and define workers at module level. See
`examples/example_usage.py`.

---

## 4. Class 1: `MpPoolRunner`

```python
MpPoolRunner(func=None, method=MpMethod.MAP, *, start_method=None,
             initializer=None, initargs=(), maxtasksperchild=None,
             max_pending=None, workers=None, chunksize=None, timeout=None,
             error_policy="raise", retries=0, retry_delay=0.0,
             star=False, ordered=True, on_error=None, verbose=0)
```

| method | underlying call | use when |
|---|---|---|
| `map` | `Pool.map` | small, ordered inputs; simplest |
| `imap` | `Pool.imap` | want a lazy ordered iterator (`iter_results()`) |
| `imap_unordered` | `Pool.imap_unordered` | heterogeneous task times; `ordered=True` restores input order via index tagging |
| `starmap` | `Pool.starmap` | items are argument tuples (`star=True`) |
| `map_async` | `Pool.map_async().get()` | need `timeout` around the whole batch |
| `starmap_async` | `Pool.starmap_async().get()` | star args + timeout |
| `apply_async` | bounded `Pool.apply_async` + `AsyncResult.get` | per-task results/timeouts with a **bounded** in-flight window |

Notes:

- `start_method=None` uses the interpreter default — **`forkserver` on CPython
  3.14/Linux**. Pass `"fork"` explicitly for speed when you know the parent is
  single-threaded; pass `"spawn"` for maximum isolation.
- `maxtasksperchild` recycles workers (returns RSS to the OS on long runs).
- `workers` is capped at `len(items)`; `chunksize` defaults to
  `ceil(n / (4 * workers))`.
- `timeout` applies to `AsyncResult.get` / `IMapIterator.next` and is
  normalised to the builtin `TimeoutError`.
- With `error_policy="collect"/"skip"`, every worker is wrapped in a
  picklable `_Guard`, so one bad item does not abort the batch; `TaskError`
  records the index, item and exception.

---

## 5. Class 2: `ExecutorRunner`

```python
ExecutorRunner(func=None, kind=ExecutorKind.PROCESS, *, mode="map",
               max_pending=None, start_method=None, mp_context=None,
               max_tasks_per_child=None, initializer=None, initargs=(),
               unordered=False, **shared_options)
```

- `kind="thread" | "process" | "interpreter"`. On Python < 3.14,
  `interpreter` raises `BackendUnavailable` (optional as required). The
  `INTERPRETER_AVAILABLE` flag is exported.
- `mode="map"` uses `Executor.map` with `chunksize` for process/interpreter
  executors — the fastest primitive in the benchmark.
- `mode="submit"` uses bounded `submit()` + `wait(FIRST_COMPLETED)`, supports
  per-run deadlines, `unordered=True` and streaming `iter_results()`.
- `max_tasks_per_child` recycles process workers (3.12+; ignored with a warning
  on 3.11 because CPython 3.11 can deadlock when all workers are recycled).
- `InterpreterPoolExecutor` (3.14) gives sub-interpreter isolation without
  process start-up; its callables **must** be importable module-level objects
  (no lambdas/closures, nothing defined in `__main__`).

---

## 6. Class 3: `JoblibRunner`

```python
JoblibRunner(func=None, backend=JoblibBackend.LOKY, *, n_jobs=None,
             pre_dispatch="2 * n_jobs", batch_size="auto", mmap_mode=None,
             temp_folder=None, max_nbytes="1M", prefer=None, require=None,
             return_as="list", **shared_options)
```

- Raises `BackendUnavailable("pip install joblib")` when joblib is missing;
  `JoblibRunner.available()` and `JOBLIB_AVAILABLE` let you check first.
- `Parallel` is used as a context manager (pool reuse + deterministic release).
- `pre_dispatch` bounds dispatched tasks → bounded peak memory; `batch_size`
  amortises dispatch overhead.
- `mmap_mode="r"` + `max_nbytes` + `temp_folder` transparently memmap large
  numpy arrays instead of copying them into every worker.
- `return_as="generator"` / `"generator_unordered"` streams via
  `iter_results()` (joblib >= 1.5; older versions fall back to a list).
- Unsupported kwargs are filtered by signature, so older joblib releases work.

---

## 7. `recommend()` — benchmark-driven defaults

```python
from run_parallel import recommend, make_runner

rec = recommend("hashing")
print(rec.config, rec.rationale)
runner = make_runner(**{k: v for k, v in rec.config.items() if k != "primitive"})
```

| workload | backend | why |
|---|---|---|
| `hashing`, `io_bound` | executor + **thread** + `map` | GIL-releasing hashlib/I/O; measured 0.073 s vs 0.105 s processes, 0.126 s mp.Pool |
| `cpu_bound` | executor + **process** + `map` | pure Python is GIL-bound; `max_tasks_per_child` bounds RSS |
| `numpy`, `large_array` | joblib + `loky` + memmap | avoids copying big arrays per task |
| `isolated` | executor + **interpreter** (3.14), else process | per-interpreter isolation without process start-up |
| `ordered_stream` | mp + `imap` | ordered streaming with tuned chunksize |
| `low_memory` | executor + thread + `submit` | cap workers/in-flight futures; see `termux/` |

`prefer_memory=True` caps workers at 2 and in-flight futures at 4.
`family_stats()` / `load_results()` expose the raw measured data.
Run `python3 -m run_parallel` for a full printed report.

---

## 8. Best practices baked in

1. **Picklable, top-level workers.** All wrappers (`_Guard`, `_Retry`, `_Star`,
   `_Indexed`, `_CollectArgs`) are module-level classes. No lambdas cross a
   process/interpreter boundary.
2. **`if __name__ == "__main__":` guard** in every example; required by
   `spawn`/`forkserver` (3.14 Linux default).
3. **Explicit mp context**, never an implicit `fork` from a threaded parent.
4. **Bounded memory**: `max_pending` for `apply_async`/`submit`,
   `pre_dispatch` for joblib, `iter_results()` generators for streaming,
   `maxtasksperchild` / `max_tasks_per_child` worker recycling.
5. **Chunksizing** computed as ~4 chunks per worker; avoids IPC overhead for
   many small tasks.
6. **Order control**: `imap_unordered(ordered=True)` tags indices; SKIP/COLLECT
   policies preserve positional semantics.
7. **Unified error handling**: `raise` / `collect` / `skip` behave identically
   on all three backends; `TaskError` keeps the failing item and index.
8. **Timeouts normalised** to builtin `TimeoutError` (mp, futures, joblib).
9. **Pool/executor reuse** through context managers instead of re-creating
   workers per call.
10. **Results carry provenance**: `result.metadata["primitive"]` names the
    function actually used — the same idea as the benchmark report's top 5.

---

## 9. Tests

```bash
python3 -m pip install pytest joblib      # joblib optional
python3 -m pytest -q                       # 103 passed, 1 skipped on 3.11
python3 -m pytest tests/test_mp_pool.py -v
```

The suite (`tests/`) covers:

- all **7 mp methods** × normal/star args, ordering, streaming, error policies,
  timeouts, initializers, empty input, pool reuse, and a real `spawn` run in a
  subprocess (`tests/spawn_script.py`);
- **all 3 executors** (interpreter test auto-skips on < 3.14), `map`/`submit`,
  bounded window, unordered, timeouts, worker recycling, context managers;
- **joblib** backends, star args, generators, memmap options, error policies and
  the missing-dependency path;
- facade/factory/recommendations, including `make_runner(**rec.config)` for
  every workload and the no-results-file fallback;
- the **Termux lite** helper: streaming, bounded reorder buffer, error
  policies, file walker, block-size-independent digests, CLI, home default.

---

## 10. Termux / low-memory variant

`termux/run_parallel_lite.py` is a **standalone, stdlib-only** file (separate
folder, as requested). See `termux/README.md`. Highlights: 2 threads by
default, generator-based file discovery, fixed 1 MiB hash blocks, bounded
in-flight futures, bounded reorder buffer, optional `gc.collect()`, no
joblib/interpreter requirement, peak-RSS reporting.

```bash
python3 termux/run_parallel_lite.py /sdcard/Download --workers 2 --gc-every 50
python3 termux/example_usage.py
```

---

## 11. Compatibility notes (target 3.14, tested on 3.11)

| feature | 3.14 (target) | here (3.11) |
|---|---|---|
| `InterpreterPoolExecutor` | available; `recommend("isolated")` uses it | flag is `False`, methods skipped / fallback to process |
| default mp start method on Linux | `forkserver` | `fork` (tests pass `"fork"` explicitly) |
| `max_tasks_per_child` | supported | ignored with a `RuntimeWarning` (3.11 deadlock bug) |
| `joblib return_as="generator"` | supported (joblib ≥ 1.5) | supported with joblib 1.6; filtered out on older versions |
| `type` aliases / PEP 695 | not used (kept 3.9-compatible) | imports cleanly |

## 12. Files

```
run_parallel/
    __init__.py       public API
    core.py           enums, ParallelResult, picklable wrappers, BaseRunner
    mp_pool.py        class 1 - MpPoolRunner (7 methods)
    executors.py      class 2 - ExecutorRunner (3 executors)
    joblib_runner.py  class 3 - JoblibRunner
    facade.py         RunParallel, run_parallel(), make_runner()
    recommend.py      benchmark-driven recommendations (+ __main__.py CLI)
examples/
    example_usage.py          all 3 classes, facade, error policies
    hash_folder_example.py    the benchmark's hashing workload end-to-end
    README.md
tests/                       pytest suite + picklable workers + spawn script
termux/                      standalone low-memory version + example + README
pytest.ini, README_RUN_PARALLEL.md
```

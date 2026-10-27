#!/usr/bin/env python3
"""Example usage of the unified, class-based ``run_parallel`` helper.

Run from the repository root:

    python3 examples/example_usage.py

All worker functions are defined at module level and ``main()`` is guarded by
``if __name__ == "__main__"``.  That is mandatory for ``spawn`` / ``forkserver``
(Linux default on CPython 3.14) and for ``InterpreterPoolExecutor``.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys
import tempfile
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_parallel import (  # noqa: E402
    INTERPRETER_AVAILABLE,
    JOBLIB_AVAILABLE,
    ErrorPolicy,
    ExecutorKind,
    ExecutorRunner,
    JoblibRunner,
    MpMethod,
    MpPoolRunner,
    RunParallel,
    make_runner,
    recommend,
    run_parallel,
)


# ---------------------------------------------------------------------------
# picklable, top-level workers
# ---------------------------------------------------------------------------
def square(value: int) -> int:
    return value * value


def add(a: int, b: int) -> int:
    return a + b


def fail_on_three(value: int) -> int:
    if value == 3:
        raise ValueError("3 is not allowed")
    return value * 10


def sha256_file(path: str, block_size: int = 1 << 20) -> tuple[str, str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(block_size), b""):
            digest.update(block)
            size += len(block)
    return path, digest.hexdigest(), size


def make_files(count: int = 8, size: int = 256 * 1024) -> list[str]:
    folder = Path(tempfile.mkdtemp(prefix="run_parallel_example_"))
    files = []
    for index in range(count):
        path = folder / f"file_{index:03d}.bin"
        path.write_bytes(bytes([index % 256]) * size)
        files.append(str(path))
    return files


def show(label: str, result: Any) -> None:
    print(f"  {label:<46} {result.elapsed:7.4f}s  "
          f"ok={result.ok}  primitive={result.primitive}")


# ---------------------------------------------------------------------------
def main() -> int:
    print("=" * 78)
    print("run_parallel - unified class-based helper (target: CPython 3.14 / Linux)")
    print("=" * 78)
    print(f"InterpreterPoolExecutor available: {INTERPRETER_AVAILABLE}")
    print(f"joblib available                : {JOBLIB_AVAILABLE}\n")

    # 1) class 1: all seven multiprocessing.Pool methods -------------------
    print("1) MpPoolRunner - all seven mp.Pool methods (squares of 0..7)")
    numbers = list(range(8))
    for method in MpMethod:
        runner = MpPoolRunner(square, method=method, workers=2,
                              start_method="fork",  # faster in this demo; 3.14 default is forkserver
                              maxtasksperchild=None)
        result = runner.run(numbers)
        assert result.unwrap() == [x * x for x in numbers]
        show(method.value, result)

    # star-args work for every method
    starred = MpPoolRunner(add, method=MpMethod.STARMAP, star=True,
                           workers=2, start_method="fork").run([(1, 2), (3, 4)])
    print(f"  starmap with star=True                      -> {starred.unwrap()}")

    # streaming (no full result list)
    streamed = list(MpPoolRunner(square, method=MpMethod.IMAP, workers=2,
                                 start_method="fork").iter_results(range(5)))
    print(f"  iter_results(imap)                         -> {streamed}\n")

    # 2) class 2: the three concurrent.futures executors -------------------
    print("2) ExecutorRunner - Process / Thread / Interpreter PoolExecutor")
    for kind in (ExecutorKind.THREAD, ExecutorKind.PROCESS, ExecutorKind.INTERPRETER):
        if kind is ExecutorKind.INTERPRETER and not INTERPRETER_AVAILABLE:
            print(f"  {kind.value:<46} skipped (needs CPython >= 3.14)")
            continue
        for mode in ("map", "submit"):
            runner = ExecutorRunner(square, kind=kind, mode=mode, workers=2,
                                    max_tasks_per_child=50 if kind is ExecutorKind.PROCESS else None)
            result = runner.run(numbers)
            assert result.unwrap() == [x * x for x in numbers]
            show(f"{kind.value}/{mode}", result)
    print()

    # 3) class 3: joblib ---------------------------------------------------
    print("3) JoblibRunner - 3rd-party joblib.Parallel / delayed")
    if JOBLIB_AVAILABLE:
        for backend in ("threading", "loky", "sequential"):
            runner = JoblibRunner(square, backend=backend, n_jobs=2,
                                  pre_dispatch="2 * n_jobs", batch_size="auto")
            result = runner.run(numbers)
            assert result.unwrap() == [x * x for x in numbers]
            show(backend, result)
        streaming = JoblibRunner(square, backend="threading", n_jobs=2,
                                 return_as="generator")
        print(f"  return_as='generator' iter_results          -> "
              f"{list(streaming.iter_results(range(5)))}")
    else:
        print("  joblib is not installed (pip install joblib)")
    print()

    # 4) unified facade + benchmark-driven auto selection ------------------
    print("4) RunParallel / run_parallel / recommend")
    files = make_files()
    expected = {sha256_file(path) for path in files}

    for workload in ("hashing", "cpu_bound", "numpy", "ordered_stream"):
        rec = recommend(workload)
        print(f"  recommend({workload!r}) -> {rec.config}")
        print(f"    {rec.rationale[:110]}...")

    auto = run_parallel(sha256_file, files, backend="auto", workload="hashing")
    assert set(auto.unwrap()) == expected
    show("run_parallel(auto, hashing)", auto)

    with RunParallel("auto", workload="hashing", workers=2) as parallel:
        first = parallel.run(files, sha256_file)
        second = parallel.map(files, sha256_file)  # same executor reused
        assert set(first.unwrap()) == expected == set(second.unwrap())
        show("RunParallel reused executor (1st)", first)
        show("RunParallel reused executor (2nd)", second)
    print()

    # 5) error policies ----------------------------------------------------
    print("5) error_policy: raise / collect / skip (same on all backends)")
    for policy in (ErrorPolicy.RAISE, ErrorPolicy.COLLECT, ErrorPolicy.SKIP):
        runner = make_runner("executor", executor_kind="thread",
                             error_policy=policy, workers=2)
        try:
            result = runner.run(range(1, 6), fail_on_three)
            print(f"  {policy.value:<8} values={result.values} "
                  f"errors={[str(e.exception) for e in result.errors]}")
        except ValueError as exc:
            print(f"  {policy.value:<8} raised {type(exc).__name__}: {exc}")
    print()

    print("Done. See README_RUN_PARALLEL.md for the full API and the pytest suite.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

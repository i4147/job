"""Backend recommendations derived from this repo's measured benchmark results.

The benchmark in ``benchmark/`` hashed 64 files / 128 MiB with 5 repeats on a
2-core Linux box.  Median seconds by primitive family:

    ThreadPoolExecutor  ~0.073s   <- fastest (hashlib releases the GIL)
    ProcessPoolExecutor ~0.105s
    joblib.Parallel     ~0.102s
    multiprocessing.Pool~0.126s
    serial              ~0.172s

This module reads ``benchmark/results/benchmark_results.json`` at runtime, so
the advice stays in sync if the benchmark is re-run with different data.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import json
from pathlib import Path
import statistics
import sys
from typing import Any

from .core import INTERPRETER_AVAILABLE, JOBLIB_AVAILABLE

DEFAULT_RESULTS_PATH = (
    Path(__file__).resolve().parents[1] / "benchmark" / "results" / "benchmark_results.json"
)

WORKLOADS = (
    "hashing",
    "io_bound",
    "cpu_bound",
    "numpy",
    "isolated",
    "ordered_stream",
    "low_memory",
)

__all__ = [
    "FamilyStats",
    "Recommendation",
    "load_results",
    "family_stats",
    "recommend",
    "print_report",
    "WORKLOADS",
]


@dataclass(frozen=True, slots=True)
class FamilyStats:
    family: str
    count: int
    median_seconds: float
    min_seconds: float
    best_id: str
    best_seconds: float


@dataclass(frozen=True, slots=True)
class Recommendation:
    workload: str
    backend: str
    config: dict[str, Any]
    rationale: str
    measured: bool
    family: dict[str, FamilyStats] = field(default_factory=dict)

    @property
    def primitive(self) -> str:
        return str(self.config.get("primitive", self.backend))


def load_results(path: str | Path | None = None) -> dict[str, Any] | None:
    """Load the benchmark JSON, or ``None`` if it is not available."""
    target = Path(path) if path is not None else DEFAULT_RESULTS_PATH
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError):  # pragma: no cover - missing/corrupt file
        return None


_UNSET = object()  # distinguish "not supplied" from "explicitly None"


def family_stats(results: dict[str, Any] | None | object = _UNSET
                 ) -> dict[str, FamilyStats]:
    """Median wall time per primitive family, ignoring failed/skipped methods.

    Passing ``None`` explicitly means "no results available" (no fallback to
    the default results file); omitting the argument loads the default file.
    """
    data = load_results() if results is _UNSET else results
    if not data:
        return {}
    grouped: dict[str, list[tuple[float, str]]] = defaultdict(list)
    for method in data.get("methods", []):
        seconds = method.get("median_seconds")
        if method.get("status") != "ok" or not seconds:
            continue
        grouped[method.get("primitive_family", "unknown")].append((seconds, method["id"]))
    out: dict[str, FamilyStats] = {}
    for family, entries in grouped.items():
        times = [value for value, _ in entries]
        best_seconds, best_id = min(entries)
        out[family] = FamilyStats(
            family=family,
            count=len(entries),
            median_seconds=statistics.median(times),
            min_seconds=min(times),
            best_id=best_id,
            best_seconds=best_seconds,
        )
    return out


def _fmt(stats: dict[str, FamilyStats], family: str) -> str:
    entry = stats.get(family)
    return f"{entry.median_seconds:.3f}s" if entry else "n/a"


def recommend(
    workload: str = "hashing",
    *,
    results_path: str | Path | None = None,
    prefer_memory: bool = False,
    python_version: tuple[int, int] | None = None,
) -> Recommendation:
    """Return a ready-to-use runner config plus the reasoning behind it.

    ``config`` can be splatted into :func:`run_parallel.make_runner` /
    :func:`run_parallel.run_parallel`.
    """
    workload = workload.strip().lower().replace("-", "_")
    results = load_results(results_path)
    stats = family_stats(results)
    measured = bool(stats)
    version = python_version or sys.version_info[:2]
    interpreter_available = version >= (3, 14) and INTERPRETER_AVAILABLE

    thread = _fmt(stats, "concurrent.futures.ThreadPoolExecutor")
    process = _fmt(stats, "concurrent.futures.ProcessPoolExecutor")
    pool = _fmt(stats, "multiprocessing.Pool")
    joblib = _fmt(stats, "joblib.Parallel")
    serial = _fmt(stats, "serial (no pool)")
    measured_note = (
        "Measured medians on the repo's 128 MiB hashing benchmark: "
        f"threads {thread}, processes {process}, joblib {joblib}, mp.Pool {pool}, "
        f"serial {serial}."
    )

    if workload in ("hashing", "io_bound", "gil_releasing", "auto"):
        config: dict[str, Any] = {
            "backend": "executor",
            "executor_kind": "thread",
            "mode": "map",
            "primitive": "concurrent.futures.ThreadPoolExecutor.map",
        }
        rationale = (
            "hashlib releases the GIL while hashing and file reads are I/O, so a "
            "thread pool parallelises without process start-up or pickling. "
            + measured_note
            + " For pure-Python CPU-bound work use workload='cpu_bound' instead."
        )
    elif workload == "cpu_bound":
        config = {
            "backend": "executor",
            "executor_kind": "process",
            "mode": "map",
            "primitive": "concurrent.futures.ProcessPoolExecutor.map",
        }
        if sys.version_info >= (3, 12):
            # 3.11 can deadlock when ProcessPoolExecutor recycles all workers.
            config["max_tasks_per_child"] = 100
        rationale = (
            "Pure-Python CPU-bound work is serialised by the GIL; processes are "
            "the only way to use all cores. max_tasks_per_child recycles workers "
            "to bound RSS on long runs. " + measured_note
        )
    elif workload in ("numpy", "large_array"):
        config = {
            "backend": "joblib",
            "joblib_backend": "loky",
            "mmap_mode": "r",
            "max_nbytes": "1M",
            "pre_dispatch": "2 * n_jobs",
            "primitive": "joblib.Parallel + joblib.delayed",
        }
        rationale = (
            "joblib memmaps large numpy arrays (mmap_mode/max_nbytes) instead of "
            "pickling a copy per task, and loky reuses its worker pool. "
            + measured_note
        )
    elif workload in ("isolated", "untrusted"):
        config = {
            "backend": "executor",
            "executor_kind": "interpreter" if interpreter_available else "process",
            "mode": "map",
            "primitive": (
                "concurrent.futures.InterpreterPoolExecutor.map"
                if interpreter_available
                else "concurrent.futures.ProcessPoolExecutor.map"
            ),
        }
        rationale = (
            "On CPython 3.14 InterpreterPoolExecutor gives per-interpreter "
            "isolation without full process start-up; worker callables must be "
            "importable module-level objects. "
            + (
                "InterpreterPoolExecutor is available here."
                if interpreter_available
                else "Not available on this interpreter (<3.14): falling back to processes."
            )
        )
    elif workload in ("ordered_stream", "streaming"):
        config = {
            "backend": "mp",
            "method": "imap",
            "primitive": "multiprocessing.Pool.imap",
        }
        rationale = (
            "Pool.imap streams ordered results with a tuned chunksize; use "
            "imap_unordered(ordered=True) if completion order is preferable. "
            + measured_note
        )
    elif workload in ("low_memory", "termux", "memory"):
        config = {
            "backend": "executor",
            "executor_kind": "thread",
            "mode": "submit",
            "max_pending": 4,
            "workers": 2,
            "primitive": "concurrent.futures.ThreadPoolExecutor.submit",
        }
        rationale = (
            "Constrained devices (Termux/Android, no swap) should avoid process "
            "pools and bound in-flight futures; use the standalone lite helper in "
            "termux/ for a fully streaming, stdlib-only implementation."
        )
    else:
        raise ValueError(f"unknown workload {workload!r}; expected one of {WORKLOADS}")

    if prefer_memory and workload not in ("low_memory", "termux", "memory"):
        config["workers"] = min(2, config.get("workers") or 2)
        config["max_pending"] = 4
        rationale += " prefer_memory=True: workers capped at 2, in-flight futures capped at 4."

    # ``primitive`` is descriptive metadata; make_runner() strips it before
    # constructing the backend class.
    return Recommendation(
        workload=workload,
        backend=config["backend"],
        config=config,
        rationale=rationale,
        measured=measured,
        family=stats,
    )


def print_report(results_path: str | Path | None = None) -> None:
    """Human-readable summary of the benchmark + all recommendations."""
    stats = family_stats(load_results(results_path))
    print("Measured median wall time by primitive family (repo benchmark):")
    if not stats:
        print("  (benchmark_results.json not found - using documented defaults)")
    for name, entry in sorted(stats.items(), key=lambda item: item[1].median_seconds):
        print(f"  {name:<48} n={entry.count:<3} median={entry.median_seconds:.4f}s "
              f"best={entry.best_seconds:.4f}s ({entry.best_id})")
    print(f"\nInterpreterPoolExecutor available: {INTERPRETER_AVAILABLE}")
    print(f"joblib available: {JOBLIB_AVAILABLE}")
    for workload in WORKLOADS:
        rec = recommend(workload, results_path=results_path)
        print(f"\n[{workload}] -> {rec.config}")
        print(f"  {rec.rationale}")


if __name__ == "__main__":  # pragma: no cover
    print_report()

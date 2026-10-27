"""Unified, class-based ``run_parallel`` helper (target: CPython 3.14 / Linux).

Three backend classes share one API:

* :class:`MpPoolRunner`   - ``multiprocessing.Pool``: map, imap, starmap,
  apply_async, map_async, starmap_async, imap_unordered.
* :class:`ExecutorRunner` - ``concurrent.futures`` Process / Thread /
  Interpreter PoolExecutor.
* :class:`JoblibRunner`   - 3rd-party ``joblib.Parallel`` / ``delayed``.

:class:`RunParallel` and :func:`run_parallel` provide a single entry point;
:func:`recommend` picks a backend using this repo's measured benchmark results.

    from run_parallel import run_parallel, ExecutorRunner

    run_parallel(str.upper, ["a", "b"]).unwrap()
"""
from __future__ import annotations

from .core import (
    INTERPRETER_AVAILABLE,
    JOBLIB_AVAILABLE,
    BackendUnavailable,
    BaseRunner,
    ErrorPolicy,
    ExecutorKind,
    JoblibBackend,
    MpMethod,
    ParallelResult,
    TaskError,
    auto_chunksize,
    default_start_method,
    normalize_items,
    resolve_workers,
)
from .executors import ExecutorRunner
from .facade import BACKENDS, RunParallel, make_runner, run_parallel
from .joblib_runner import JoblibRunner
from .mp_pool import MpPoolRunner
from .recommend import (
    WORKLOADS,
    FamilyStats,
    Recommendation,
    family_stats,
    load_results,
    print_report,
    recommend,
)

__version__ = "1.0.0"

__all__ = [
    # the three required classes
    "MpPoolRunner",
    "ExecutorRunner",
    "JoblibRunner",
    # unified facade
    "RunParallel",
    "run_parallel",
    "make_runner",
    "BACKENDS",
    # shared types
    "BaseRunner",
    "ParallelResult",
    "TaskError",
    "MpMethod",
    "ExecutorKind",
    "JoblibBackend",
    "ErrorPolicy",
    "BackendUnavailable",
    "resolve_workers",
    "auto_chunksize",
    "normalize_items",
    "default_start_method",
    # benchmark-driven advice
    "recommend",
    "Recommendation",
    "FamilyStats",
    "family_stats",
    "load_results",
    "print_report",
    "WORKLOADS",
    # capability flags
    "INTERPRETER_AVAILABLE",
    "JOBLIB_AVAILABLE",
    "__version__",
]

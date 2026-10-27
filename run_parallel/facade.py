"""Unified, class-based facade over the three backend classes."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any

from .core import BaseRunner, ParallelResult
from .executors import ExecutorRunner
from .joblib_runner import JoblibRunner
from .mp_pool import MpPoolRunner
from .recommend import Recommendation, recommend

BACKENDS = ("auto", "mp", "executor", "joblib")

# Options each concrete class accepts (base options + backend-specific ones).
_BASE_KEYS = {
    "func", "workers", "chunksize", "timeout", "error_policy", "retries",
    "retry_delay", "star", "ordered", "on_error", "verbose",
}
_MP_KEYS = _BASE_KEYS | {
    "method", "start_method", "initializer", "initargs", "maxtasksperchild",
    "max_pending",
}
_EXECUTOR_KEYS = _BASE_KEYS | {
    "kind", "mode", "max_pending", "start_method", "mp_context",
    "max_tasks_per_child", "initializer", "initargs", "unordered",
}
_JOBLIB_KEYS = _BASE_KEYS | {
    "n_jobs", "pre_dispatch", "batch_size", "mmap_mode", "temp_folder",
    "max_nbytes", "prefer", "require", "return_as",
}


def _build(cls: type[BaseRunner], allowed: set[str], options: dict[str, Any],
           forced: dict[str, Any]) -> BaseRunner:
    unknown = set(options) - allowed
    if unknown:
        raise TypeError(f"unsupported options for {cls.__name__}: {sorted(unknown)}")
    kwargs = {key: value for key, value in options.items() if key in allowed}
    kwargs.update(forced)
    return cls(**kwargs)


def make_runner(
    backend: str = "auto",
    *,
    workload: str = "hashing",
    executor_kind: str | None = None,
    joblib_backend: str | None = None,
    prefer_memory: bool = False,
    options: dict[str, Any] | None = None,
    **kwargs: Any,
) -> BaseRunner:
    """Instantiate one of the three backend classes.

    ``backend='auto'`` uses :func:`~run_parallel.recommend.recommend` (which is
    driven by ``benchmark/results/benchmark_results.json``) and then applies any
    explicit keyword overrides you supplied.
    """
    backend = backend.strip().lower()
    aliases = {
        "multiprocessing": "mp", "pool": "mp", "mp_pool": "mp",
        "concurrent.futures": "executor", "cf": "executor", "futures": "executor",
    }
    backend = aliases.get(backend, backend)
    merged = {**(options or {}), **kwargs}
    merged.pop("primitive", None)  # descriptive metadata from recommend()

    if backend == "auto":
        rec = recommend(workload, prefer_memory=prefer_memory)
        defaults = {
            key: value for key, value in rec.config.items()
            if key not in ("backend", "primitive")
        }
        merged = {**defaults, **merged}  # explicit user options win
        backend = rec.backend

    # generic recommendation keys -> concrete constructor keywords
    kind = merged.pop("executor_kind", None) or executor_kind
    joblib_kind = merged.pop("joblib_backend", None) or joblib_backend

    if backend == "mp":
        return _build(MpPoolRunner, _MP_KEYS, merged, {})
    if backend == "executor":
        forced: dict[str, Any] = {"kind": kind} if kind else {}
        return _build(ExecutorRunner, _EXECUTOR_KEYS, merged, forced)
    if backend == "joblib":
        forced = {"backend": joblib_kind} if joblib_kind else {}
        return _build(JoblibRunner, _JOBLIB_KEYS, merged, forced)
    raise ValueError(f"unknown backend {backend!r}; expected one of {BACKENDS}")


class RunParallel:
    """The unified, class-based helper.

    Holds one backend runner and exposes a common ``run`` / ``map`` / ``imap``
    API.  Use it as a context manager to reuse a pool/executor across calls.
    """

    def __init__(self, backend: str = "auto", *, workload: str = "hashing",
                 prefer_memory: bool = False, **kwargs: Any) -> None:
        self.requested_backend = backend
        self.workload = workload
        self.recommendation: Recommendation | None = (
            recommend(workload, prefer_memory=prefer_memory)
            if backend.strip().lower() == "auto" else None
        )
        self.runner = make_runner(
            backend, workload=workload, prefer_memory=prefer_memory, **kwargs
        )

    @property
    def backend(self) -> str:
        return self.runner.backend_name

    @property
    def primitive(self) -> str:
        return self.runner.primitive_label

    def run(self, items: Iterable[Any], func: Callable[..., Any] | None = None,
            **overrides: Any) -> ParallelResult:
        return self.runner.run(items, func, **overrides)

    def map(self, items: Iterable[Any], func: Callable[..., Any] | None = None,
            **overrides: Any) -> ParallelResult:
        return self.run(items, func, **overrides)

    def imap(self, items: Iterable[Any], func: Callable[..., Any] | None = None
             ) -> Iterator[Any]:
        return self.runner.iter_results(items, func)

    def __call__(self, items: Iterable[Any], func: Callable[..., Any] | None = None,
                 **overrides: Any) -> ParallelResult:
        return self.run(items, func, **overrides)

    def __enter__(self) -> "RunParallel":
        self.runner.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.runner.__exit__(exc_type, exc, tb)

    def close(self) -> None:
        self.runner.close()


def run_parallel(
    func: Callable[..., Any],
    items: Iterable[Any],
    *,
    backend: str = "auto",
    workload: str = "hashing",
    **kwargs: Any,
) -> ParallelResult:
    """One-shot convenience wrapper around :class:`RunParallel`.

    >>> result = run_parallel(str.upper, ["a", "b"], backend="executor",
    ...                       executor_kind="thread")
    >>> result.unwrap()
    ['A', 'B']
    """
    return RunParallel(backend, workload=workload, func=func, **kwargs).run(items)

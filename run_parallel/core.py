"""Shared building blocks for the three run_parallel backends.

Everything that crosses a process/interpreter boundary is defined at module
level (classes, not lambdas/closures): ``spawn``/``forkserver`` on Linux and
``InterpreterPoolExecutor`` on 3.14 can only pickle importable callables.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from enum import Enum
import math
import os
import sys
import time
from typing import Any, ClassVar, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")

_SKIP = object()  # sentinel: a failed item in SKIP mode is not emitted

__all__ = [
    "ErrorPolicy",
    "MpMethod",
    "ExecutorKind",
    "JoblibBackend",
    "BackendUnavailable",
    "TaskError",
    "ParallelResult",
    "BaseRunner",
    "resolve_workers",
    "auto_chunksize",
    "default_start_method",
    "INTERPRETER_AVAILABLE",
    "JOBLIB_AVAILABLE",
]


# ---------------------------------------------------------------------------
# optional capabilities (target runtime: CPython 3.14 / Linux)
# ---------------------------------------------------------------------------
def _interpreter_pool_class():
    """Import InterpreterPoolExecutor without ever breaking an older Python."""
    try:
        from concurrent.futures import InterpreterPoolExecutor

        return InterpreterPoolExecutor
    except ImportError:
        try:  # 3.14 module layout fallback
            from concurrent.futures.interpreter import InterpreterPoolExecutor

            return InterpreterPoolExecutor
        except ImportError:
            return None


INTERPRETER_EXECUTOR = _interpreter_pool_class()
INTERPRETER_AVAILABLE = sys.version_info >= (3, 14) and INTERPRETER_EXECUTOR is not None

try:
    import joblib as _joblib  # noqa: F401

    JOBLIB_AVAILABLE = True
except ImportError:  # pragma: no cover - depends on environment
    _joblib = None
    JOBLIB_AVAILABLE = False


class BackendUnavailable(RuntimeError):
    """Raised when a backend needs an optional dependency/runtime feature."""


# ---------------------------------------------------------------------------
# enums
# ---------------------------------------------------------------------------
class ErrorPolicy(str, Enum):
    """What to do when a worker raises."""

    RAISE = "raise"        # fail fast, propagate the exception (default)
    COLLECT = "collect"    # keep an exception object in the result values
    SKIP = "skip"          # drop the failed item, record it in ``errors``

    @classmethod
    def parse(cls, value: "ErrorPolicy | str") -> "ErrorPolicy":
        return value if isinstance(value, cls) else cls(str(value).strip().lower())


class MpMethod(str, Enum):
    """The seven ``multiprocessing.Pool`` methods required by the task."""

    MAP = "map"
    IMAP = "imap"
    STARMAP = "starmap"
    APPLY_ASYNC = "apply_async"
    MAP_ASYNC = "map_async"
    STARMAP_ASYNC = "starmap_async"
    IMAP_UNORDERED = "imap_unordered"

    @classmethod
    def parse(cls, value: "MpMethod | str") -> "MpMethod":
        return value if isinstance(value, cls) else cls(str(value).strip().lower())

    @property
    def is_star(self) -> bool:
        return self in (MpMethod.STARMAP, MpMethod.STARMAP_ASYNC)

    @property
    def is_lazy(self) -> bool:
        return self in (MpMethod.IMAP, MpMethod.IMAP_UNORDERED)

    @property
    def is_unordered(self) -> bool:
        return self is MpMethod.IMAP_UNORDERED


class ExecutorKind(str, Enum):
    PROCESS = "process"
    THREAD = "thread"
    INTERPRETER = "interpreter"

    @classmethod
    def parse(cls, value: "ExecutorKind | str") -> "ExecutorKind":
        return value if isinstance(value, cls) else cls(str(value).strip().lower())

    @property
    def executor_name(self) -> str:
        return {
            ExecutorKind.PROCESS: "ProcessPoolExecutor",
            ExecutorKind.THREAD: "ThreadPoolExecutor",
            ExecutorKind.INTERPRETER: "InterpreterPoolExecutor",
        }[self]


class JoblibBackend(str, Enum):
    LOKY = "loky"                          # processes, reusable executor (default)
    THREADING = "threading"                # threads
    MULTIPROCESSING = "multiprocessing"    # legacy backend
    SEQUENTIAL = "sequential"              # no parallelism, for debugging

    @classmethod
    def parse(cls, value: "JoblibBackend | str") -> "JoblibBackend":
        return value if isinstance(value, cls) else cls(str(value).strip().lower())


# ---------------------------------------------------------------------------
# result objects
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class TaskError:
    index: int
    item: Any
    exception: BaseException


@dataclass(slots=True)
class ParallelResult(Generic[R]):
    """Rich return value shared by all three backends.

    ``values`` is ordered for ordered methods and for ``ordered=True``
    unordered methods.  With :attr:`ErrorPolicy.COLLECT` failed slots hold the
    exception instance; with :attr:`ErrorPolicy.SKIP` they are omitted.
    """

    values: list[R]
    errors: list[TaskError]
    elapsed: float
    submitted: int
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def succeeded(self) -> int:
        return self.submitted - len(self.errors)

    @property
    def failed(self) -> int:
        return len(self.errors)

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def primitive(self) -> str:
        return str(self.metadata.get("primitive", ""))

    def unwrap(self) -> list[R]:
        """Return values, or raise the first worker error."""
        if self.errors:
            raise self.errors[0].exception
        return self.values

    def __iter__(self) -> Iterator[R]:
        return iter(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def __getitem__(self, index: int) -> R:
        return self.values[index]


# ---------------------------------------------------------------------------
# picklable worker wrappers (top-level classes on purpose)
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class _Failure:
    type_name: str
    message: str


class _Guard:
    """Catch worker errors as picklable ``_Failure`` sentinels."""

    __slots__ = ("func",)

    def __init__(self, func: Callable[..., Any]):
        self.func = func

    def __call__(self, item: Any) -> Any:
        try:
            return self.func(item)
        except Exception as exc:  # noqa: BLE001 - deliberate, reported to caller
            return _Failure(type(exc).__name__, str(exc))


class _Retry:
    """Exponential-backoff retries around a worker."""

    __slots__ = ("delay", "func", "retries")

    def __init__(self, func: Callable[..., Any], retries: int, delay: float):
        self.func = func
        self.retries = retries
        self.delay = delay

    def __call__(self, item: Any) -> Any:
        for attempt in range(self.retries + 1):
            try:
                return self.func(item)
            except Exception:
                if attempt >= self.retries:
                    raise
                if self.delay > 0:
                    time.sleep(self.delay * 2**attempt)
        raise AssertionError("unreachable")


class _Star:
    """Unpack ``(arg1, arg2, ...)`` into ``func(*args)``."""

    __slots__ = ("func",)

    def __init__(self, func: Callable[..., Any]):
        self.func = func

    def __call__(self, args: Any) -> Any:
        if isinstance(args, tuple):
            return self.func(*args)
        if isinstance(args, list):
            return self.func(*args)
        return self.func(args)


class _Indexed:
    """Tag an unordered result with its input index so order can be restored."""

    __slots__ = ("func",)

    def __init__(self, func: Callable[..., Any]):
        self.func = func

    def __call__(self, pair: tuple[int, Any]) -> tuple[int, Any]:
        index, item = pair
        return index, self.func(item)


class _CollectArgs:
    """Adapt ``func(item_tuple)`` to ``pool.starmap``'s ``f(*args)`` protocol."""

    __slots__ = ("func",)

    def __init__(self, func: Callable[..., Any]):
        self.func = func

    def __call__(self, *args: Any) -> Any:
        return self.func(args)


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
def resolve_workers(workers: int | None = None, *, n_items: int | None = None,
                    cap: int | None = None) -> int:
    """Resolve ``None`` -> all cores, ``-1`` -> all cores, ``-2`` -> cores-1."""
    cpu = os.cpu_count() or 1
    if workers is None or workers <= 0:
        if workers is not None and workers < 0:
            resolved = max(1, cpu + 1 + workers)  # joblib convention
        else:
            resolved = cpu
    else:
        resolved = workers
    if cap is not None:
        resolved = min(resolved, cap)
    if n_items is not None:
        resolved = min(resolved, max(1, n_items))
    return max(1, int(resolved))


def auto_chunksize(n_items: int, workers: int, *, multiplier: int = 4,
                   cap: int | None = None) -> int:
    """A conservative chunk size: ~4 chunks per worker."""
    if n_items <= 0:
        return 1
    size = max(1, math.ceil(n_items / (max(1, workers) * multiplier)))
    return min(size, cap) if cap else size


def default_start_method() -> str:
    """Python 3.14 on Linux defaults to ``forkserver``; return the real default."""
    import multiprocessing as mp

    return mp.get_start_method(allow_none=False)


def normalize_items(items: Iterable[Any], *, star: bool) -> list[Any]:
    """Materialise once and normalise star-args into tuples.

    Strings/bytes are treated as *single* arguments, never as arg sequences.
    """
    payload = list(items)
    if not star:
        return payload
    normalised = []
    for item in payload:
        if isinstance(item, tuple):
            normalised.append(item)
        elif isinstance(item, list):
            normalised.append(tuple(item))
        else:
            normalised.append((item,))
    return normalised


# ---------------------------------------------------------------------------
# base runner
# ---------------------------------------------------------------------------
class BaseRunner(ABC):
    """Common configuration, worker wrapping, error policy and result object."""

    backend_name: ClassVar[str] = "base"
    primitive_label: ClassVar[str] = ""

    def __init__(
        self,
        func: Callable[..., Any] | None = None,
        *,
        workers: int | None = None,
        chunksize: int | None = None,
        timeout: float | None = None,
        error_policy: ErrorPolicy | str = ErrorPolicy.RAISE,
        retries: int = 0,
        retry_delay: float = 0.0,
        star: bool = False,
        ordered: bool = True,
        on_error: Callable[[Any, BaseException], Any] | None = None,
        verbose: int = 0,
    ) -> None:
        if func is not None and not callable(func):
            raise TypeError("func must be callable")
        if retries < 0:
            raise ValueError("retries must be >= 0")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0")
        if chunksize is not None and chunksize < 1:
            raise ValueError("chunksize must be >= 1")
        self.func = func
        self.workers_arg = workers
        self.chunksize_arg = chunksize
        self.timeout = timeout
        self.error_policy = ErrorPolicy.parse(error_policy)
        self.retries = retries
        self.retry_delay = retry_delay
        self.star = bool(star)
        self.ordered = bool(ordered)
        self.on_error = on_error
        self.verbose = int(verbose)
        self._persistent = False
        self._resource: Any = None

    # -- configuration helpers ---------------------------------------------
    def _resolve_func(self, func: Callable[..., Any] | None) -> Callable[..., Any]:
        chosen = func if func is not None else self.func
        if chosen is None or not callable(chosen):
            raise TypeError("a callable worker `func` is required")
        return chosen

    def _resolve_workers(self, n_items: int) -> int:
        return resolve_workers(self.workers_arg, n_items=n_items)

    def _resolve_chunksize(self, n_items: int, workers: int) -> int:
        return self.chunksize_arg or auto_chunksize(n_items, workers)

    def _wrap_call(
        self,
        func: Callable[..., Any],
        *,
        star_unpack: bool = False,
        tagged: bool = False,
    ) -> Callable[..., Any]:
        """Compose retry -> error guard -> star unpack -> index tagging."""
        call: Callable[..., Any] = func
        if self.retries:
            call = _Retry(call, self.retries, self.retry_delay)
        if self.error_policy is not ErrorPolicy.RAISE:
            call = _Guard(call)
        if star_unpack:
            call = _Star(call)
        if tagged:
            call = _Indexed(call)
        return call

    def _coerce(self, value: Any) -> Any:
        """Turn a ``_Failure`` sentinel into an exception / ``_SKIP`` marker."""
        if isinstance(value, _Failure):
            exc: BaseException = RuntimeError(f"{value.type_name}: {value.message}")
            if self.error_policy is ErrorPolicy.RAISE:
                raise exc
            if self.error_policy is ErrorPolicy.SKIP:
                return _SKIP
            return exc
        return value

    def _finalize(
        self,
        raw: Iterable[Any],
        items: Sequence[Any],
        elapsed: float,
        **metadata: Any,
    ) -> ParallelResult:
        values: list[Any] = []
        errors: list[TaskError] = []
        for index, value in enumerate(raw):
            if isinstance(value, _Failure):
                exc: BaseException = RuntimeError(f"{value.type_name}: {value.message}")
                errors.append(TaskError(index, items[index] if index < len(items) else None, exc))
                if self.error_policy is ErrorPolicy.COLLECT:
                    values.append(exc)
            elif isinstance(value, BaseException):
                errors.append(TaskError(index, items[index] if index < len(items) else None, value))
                if self.error_policy is ErrorPolicy.COLLECT:
                    values.append(value)
            else:
                values.append(value)
        if self.on_error is not None:
            for error in errors:
                self.on_error(error.item, error.exception)
        if errors and self.error_policy is ErrorPolicy.RAISE:
            raise errors[0].exception
        meta = {
            "backend": self.backend_name,
            "primitive": self.primitive_label,
            "python": sys.version_info[:3],
            "workers": None,
            "chunksize": None,
            "error_policy": self.error_policy.value,
            "ordered": self.ordered,
            "star": self.star,
            **metadata,
        }
        return ParallelResult(
            values=values,
            errors=errors,
            elapsed=elapsed,
            submitted=len(items),
            metadata=meta,
        )

    def _empty(self, n_items: int = 0) -> ParallelResult:
        return self._finalize([], [], 0.0, workers=0)

    # -- public API ---------------------------------------------------------
    @abstractmethod
    def run(self, items: Iterable[Any], func: Callable[..., Any] | None = None,
            **overrides: Any) -> ParallelResult:
        """Execute ``func`` over ``items`` and return a :class:`ParallelResult`."""

    def iter_results(self, items: Iterable[Any], func: Callable[..., Any] | None = None
                     ) -> Iterator[Any]:
        """Streaming fallback: materialise with :meth:`run`, then yield."""
        yield from self.run(items, func).values

    def map(self, items: Iterable[Any], func: Callable[..., Any] | None = None,
            **overrides: Any) -> ParallelResult:
        return self.run(items, func, **overrides)

    def __call__(self, items: Iterable[Any], func: Callable[..., Any] | None = None,
                 **overrides: Any) -> ParallelResult:
        return self.run(items, func, **overrides)

    # -- optional pool reuse ------------------------------------------------
    def __enter__(self) -> "BaseRunner":
        self._persistent = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._persistent = False
        self.close()

    def close(self) -> None:
        """Release any persistent pool/executor. Overridden by subclasses."""
        self._resource = None

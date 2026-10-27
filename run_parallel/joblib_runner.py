"""Class 3/3: 3rd-party ``joblib`` backend behind the same API."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
import inspect
import time
from typing import Any

from .core import (
    JOBLIB_AVAILABLE,
    BackendUnavailable,
    BaseRunner,
    JoblibBackend,
    ParallelResult,
    _SKIP,
    _joblib,
    normalize_items,
    resolve_workers,
)


class JoblibRunner(BaseRunner):
    """`joblib.Parallel` / `joblib.delayed` behind the shared runner API.

    ``backend=``: ``loky`` (default, reusable process executor), ``threading``,
    ``multiprocessing`` (legacy) or ``sequential`` (debug).

    Best practices applied
    ----------------------
    * ``Parallel`` is used as a context manager so its worker pool is reused
      across tasks and released deterministically;
    * ``pre_dispatch`` (default ``2 * n_jobs``) bounds how many tasks are
      dispatched, which bounds peak memory for large inputs;
    * ``batch_size='auto'`` lets joblib amortise dispatch overhead;
    * ``mmap_mode='r'`` / ``max_nbytes`` / ``temp_folder`` enable transparent
      memmapping of large numpy arrays instead of copying them per worker;
    * ``return_as='generator'`` / ``'generator_unordered'`` streams results
      (joblib >= 1.5) through :meth:`iter_results`;
    * cloudpickle means joblib tolerates objects the stdlib executors cannot
      pickle, but the same ``error_policy`` / ``retries`` / ``star`` options
      still apply.
    """

    backend_name = "joblib"
    primitive_label = "joblib.Parallel + joblib.delayed"

    def __init__(
        self,
        func: Callable[..., Any] | None = None,
        backend: JoblibBackend | str = JoblibBackend.LOKY,
        *,
        n_jobs: int | None = None,
        pre_dispatch: str | int = "2 * n_jobs",
        batch_size: str | int = "auto",
        mmap_mode: str | None = None,
        temp_folder: str | None = None,
        max_nbytes: str | int | None = "1M",
        prefer: str | None = None,
        require: str | None = None,
        return_as: str = "list",
        **base_kwargs: Any,
    ) -> None:
        super().__init__(func, **base_kwargs)
        if not JOBLIB_AVAILABLE:
            raise BackendUnavailable(
                "JoblibRunner requires the 3rd-party joblib package "
                "(`pip install joblib`)"
            )
        self.backend = JoblibBackend.parse(backend)
        self.n_jobs = n_jobs
        self.pre_dispatch = pre_dispatch
        self.batch_size = batch_size
        self.mmap_mode = mmap_mode
        self.temp_folder = temp_folder
        self.max_nbytes = max_nbytes
        self.prefer = prefer
        self.require = require
        if return_as not in ("list", "generator", "generator_unordered"):
            raise ValueError("return_as must be list/generator/generator_unordered")
        self.return_as = return_as

    @staticmethod
    def available() -> bool:
        return JOBLIB_AVAILABLE

    # -- joblib lifecycle ---------------------------------------------------
    def _parallel_kwargs(self, n_jobs: int) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "n_jobs": n_jobs,
            "backend": self.backend.value,
            "pre_dispatch": self.pre_dispatch,
            "batch_size": self.batch_size,
            "verbose": self.verbose,
        }
        if self.timeout is not None:
            kwargs["timeout"] = self.timeout
        if self.return_as != "list":
            kwargs["return_as"] = self.return_as
        if self.mmap_mode is not None:
            kwargs["mmap_mode"] = self.mmap_mode
        if self.temp_folder is not None:
            kwargs["temp_folder"] = self.temp_folder
        if self.max_nbytes is not None:
            kwargs["max_nbytes"] = self.max_nbytes
        if self.prefer is not None:
            kwargs["prefer"] = self.prefer
        if self.require is not None:
            kwargs["require"] = self.require
        # Be friendly to joblib < 1.5 (no return_as / timeout parameters).
        supported = set(inspect.signature(_joblib.Parallel.__init__).parameters)
        return {key: value for key, value in kwargs.items() if key in supported}

    @contextmanager
    def _parallel_scope(self, n_jobs: int) -> Iterator[Any]:
        if self._resource is not None:
            yield self._resource
            return
        parallel = _joblib.Parallel(**self._parallel_kwargs(n_jobs))
        entered = parallel.__enter__()
        if self._persistent:
            self._resource = entered
            try:
                yield entered
            finally:
                pass  # released by close()/__exit__
        else:
            try:
                yield entered
            finally:
                parallel.__exit__(None, None, None)

    def close(self) -> None:
        parallel, self._resource = self._resource, None
        if parallel is not None:
            parallel.__exit__(None, None, None)

    # -- task building ------------------------------------------------------
    def _tasks(self, call: Callable[..., Any], payload: list[Any]) -> Iterable[Any]:
        delayed = _joblib.delayed(call)
        if self.star:
            return (delayed(*item) for item in payload)
        return (delayed(item) for item in payload)

    # -- public API ---------------------------------------------------------
    def run(self, items: Iterable[Any], func: Callable[..., Any] | None = None,
            **overrides: Any) -> ParallelResult:
        if overrides:
            raise TypeError(
                f"unknown overrides: {sorted(overrides)} "
                "(configure backend/return_as on the constructor)"
            )
        payload = normalize_items(items, star=self.star)
        if not payload:
            return self._empty()
        worker = self._resolve_func(func)
        n_jobs = resolve_workers(self.n_jobs if self.n_jobs is not None else self.workers_arg,
                                 n_items=len(payload))
        call = self._wrap_call(worker)  # joblib's delayed() unpacks for star=True
        started = time.perf_counter()
        with self._parallel_scope(n_jobs) as parallel:
            tasks = self._tasks(call, payload)
            output = parallel(tasks)
            raw = list(output)  # consume a generator while the pool is alive
        elapsed = time.perf_counter() - started
        return self._finalize(
            raw,
            payload,
            elapsed,
            joblib_backend=self.backend.value,
            return_as=self.return_as,
            n_jobs=n_jobs,
            primitive=self.primitive_label,
        )

    def iter_results(self, items: Iterable[Any], func: Callable[..., Any] | None = None
                     ) -> Iterator[Any]:
        if self.return_as == "list":
            yield from super().iter_results(items, func)
            return
        payload = normalize_items(items, star=self.star)
        if not payload:
            return
        worker = self._resolve_func(func)
        n_jobs = resolve_workers(self.n_jobs if self.n_jobs is not None else self.workers_arg,
                                 n_items=len(payload))
        call = self._wrap_call(worker)
        with self._parallel_scope(n_jobs) as parallel:
            for value in parallel(self._tasks(call, payload)):
                converted = self._coerce(value)
                if converted is not _SKIP:
                    yield converted

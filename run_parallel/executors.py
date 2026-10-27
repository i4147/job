"""Class 2/3: ``concurrent.futures`` executors behind one API.

Targets CPython 3.14 on Linux, where ``InterpreterPoolExecutor`` is part of the
standard library.  On older interpreters that kind raises
:class:`~run_parallel.core.BackendUnavailable` and the other two keep working.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from contextlib import contextmanager
import inspect
import multiprocessing as mp
import sys
import time
import warnings
from typing import Any

from .core import (
    INTERPRETER_AVAILABLE,
    INTERPRETER_EXECUTOR,
    BackendUnavailable,
    BaseRunner,
    ExecutorKind,
    ParallelResult,
    _SKIP,
    normalize_items,
)


class ExecutorRunner(BaseRunner):
    """Process / Thread / Interpreter pool executors behind one API.

    ``kind=``
        ``ExecutorKind.PROCESS``      -> ``ProcessPoolExecutor``
        ``ExecutorKind.THREAD``       -> ``ThreadPoolExecutor``
        ``ExecutorKind.INTERPRETER``  -> ``InterpreterPoolExecutor`` (3.14+)

    ``mode=``
        ``"map"``    -> ``Executor.map`` (ordered, chunked, fastest for most
        workloads; see benchmark results)
        ``"submit"`` -> bounded in-flight futures + ``wait(FIRST_COMPLETED)``;
        supports timeouts, streaming via ``iter_results()`` and unordered yield.

    Best practices applied
    ----------------------
    * ``map`` mode uses ``chunksize`` for process/interpreter executors to cut
      IPC overhead; thread executors skip it (ignored by CPython anyway);
    * ``submit`` mode caps in-flight futures at ``max_pending`` (default
      ``4 * workers``) so memory stays bounded for huge iterables;
    * ``max_tasks_per_child`` recycles process workers (memory hygiene);
    * explicit ``mp_context`` / ``start_method`` avoids unsafe ``fork``;
    * workers and callables are top-level importable objects because
      ``InterpreterPoolExecutor`` cannot run ``__main__`` closures or lambdas;
    * ``with ExecutorRunner(...) as runner:`` reuses one executor.
    """

    backend_name = "executor"

    def __init__(
        self,
        func: Callable[..., Any] | None = None,
        kind: ExecutorKind | str = ExecutorKind.PROCESS,
        *,
        mode: str = "map",
        max_pending: int | None = None,
        start_method: str | None = None,
        mp_context: Any = None,
        max_tasks_per_child: int | None = None,
        initializer: Callable[..., Any] | None = None,
        initargs: tuple[Any, ...] = (),
        unordered: bool = False,
        **base_kwargs: Any,
    ) -> None:
        super().__init__(func, **base_kwargs)
        self.kind = ExecutorKind.parse(kind)
        self.mode = mode.strip().lower()
        if self.mode not in ("map", "submit"):
            raise ValueError("mode must be 'map' or 'submit'")
        if self.kind is ExecutorKind.INTERPRETER and not INTERPRETER_AVAILABLE:
            raise BackendUnavailable(
                "InterpreterPoolExecutor requires CPython >= 3.14 "
                f"(running {sys.version_info.major}.{sys.version_info.minor})"
            )
        if max_pending is not None and max_pending < 1:
            raise ValueError("max_pending must be >= 1")
        if mode == "map" and unordered:
            raise ValueError("unordered requires mode='submit'")
        self.max_pending = max_pending
        self.start_method = start_method
        self.mp_context = mp_context
        self.max_tasks_per_child = max_tasks_per_child
        self.max_tasks_per_child_active = False
        self.initializer = initializer
        self.initargs = tuple(initargs)
        self.unordered = bool(unordered)
        self.primitive_label = (
            f"concurrent.futures.{self.kind.executor_name}."
            f"{'map' if self.mode == 'map' else 'submit'}"
        )

    # -- executor lifecycle -------------------------------------------------
    def _make_executor(self, workers: int) -> Executor:
        if self.kind is ExecutorKind.THREAD:
            return ThreadPoolExecutor(
                max_workers=workers,
                initializer=self.initializer,
                initargs=self.initargs,
                thread_name_prefix="run_parallel",
            )
        if self.kind is ExecutorKind.INTERPRETER:
            assert INTERPRETER_EXECUTOR is not None
            kwargs: dict[str, Any] = {"max_workers": workers}
            if self.initializer is not None:
                kwargs["initializer"] = self.initializer
                kwargs["initargs"] = self.initargs
            try:
                return INTERPRETER_EXECUTOR(**kwargs)
            except TypeError:  # pragma: no cover - 3.14 signature difference
                return INTERPRETER_EXECUTOR(max_workers=workers)
        kwargs = {"max_workers": workers}
        if self.initializer is not None:
            kwargs["initializer"] = self.initializer
            kwargs["initargs"] = self.initargs
        if self.mp_context is not None:
            kwargs["mp_context"] = self.mp_context
        elif self.start_method is not None:
            kwargs["mp_context"] = mp.get_context(self.start_method)
        if self.max_tasks_per_child is not None:
            # CPython 3.11's ProcessPoolExecutor can deadlock when all workers
            # are recycled at once (fixed in 3.12).  Target runtime is 3.14,
            # where this is a first-class, supported option.
            if sys.version_info >= (3, 12):
                kwargs["max_tasks_per_child"] = self.max_tasks_per_child
                self.max_tasks_per_child_active = True
            else:  # pragma: no cover - 3.11 compatibility path
                self.max_tasks_per_child_active = False
                warnings.warn(
                    "max_tasks_per_child is ignored on CPython < 3.12 "
                    "(ProcessPoolExecutor worker recycling can deadlock there)",
                    RuntimeWarning,
                    stacklevel=2,
                )
        return ProcessPoolExecutor(**kwargs)

    @contextmanager
    def _executor_scope(self, workers: int) -> Iterator[Executor]:
        if self._resource is not None:
            yield self._resource
            return
        executor = self._make_executor(workers)
        if self._persistent:
            self._resource = executor
        try:
            yield executor
        finally:
            if not self._persistent:
                executor.shutdown(wait=True)

    def close(self) -> None:
        executor, self._resource = self._resource, None
        if executor is not None:
            executor.shutdown(wait=True)

    # -- dispatch -----------------------------------------------------------
    def _executor_map(self, executor: Executor, call: Callable[..., Any],
                      payload: list[Any], chunksize: int) -> Iterator[Any]:
        if self.kind is ExecutorKind.THREAD:
            # ThreadPoolExecutor.map accepts but ignores chunksize.
            yield from executor.map(call, payload, timeout=self.timeout)
            return
        # Inspect once, before submitting anything: catching a TypeError during
        # iteration below could otherwise re-submit tasks that already ran.
        try:
            parameters = inspect.signature(executor.map).parameters
        except (TypeError, ValueError):  # pragma: no cover - exotic executor
            parameters = {}
        if "chunksize" in parameters:
            yield from executor.map(call, payload, chunksize=chunksize,
                                    timeout=self.timeout)
        else:  # pragma: no cover - 3.14 InterpreterPoolExecutor variant
            yield from executor.map(call, payload, timeout=self.timeout)

    def _cap(self, workers: int) -> int:
        return self.max_pending or max(4 * workers, workers)

    def _deadline(self) -> float | None:
        return time.monotonic() + self.timeout if self.timeout else None

    @staticmethod
    def _remaining(deadline: float | None) -> float | None:
        return None if deadline is None else max(0.0, deadline - time.monotonic())

    def _submit_bounded(self, executor: Executor, call: Callable[..., Any],
                        payload: list[Any], workers: int,
                        unordered: bool = False) -> list[Any]:
        cap = self._cap(workers)
        collected: list[tuple[int, Any]] = []
        pending: dict[Any, int] = {}
        source = iter(enumerate(payload))
        exhausted = False
        deadline = self._deadline()
        while True:
            while not exhausted and len(pending) < cap:
                try:
                    index, item = next(source)
                except StopIteration:
                    exhausted = True
                    break
                pending[executor.submit(call, item)] = index
            if not pending:
                break
            done, _ = wait(tuple(pending), timeout=self._remaining(deadline),
                           return_when=FIRST_COMPLETED)
            if not done:
                for future in pending:
                    future.cancel()
                raise TimeoutError(f"tasks did not finish within {self.timeout}s")
            for future in done:
                index = pending.pop(future)
                collected.append((index, future.result()))
        if unordered:
            return [value for _, value in collected]
        results: list[Any] = [None] * len(payload)
        for index, value in collected:
            results[index] = value
        return results

    def _submit_stream(self, executor: Executor, call: Callable[..., Any],
                       payload: Iterable[Any], workers: int) -> Iterator[Any]:
        cap = self._cap(workers)
        pending: set[Any] = set()
        source = iter(payload)
        exhausted = False
        deadline = self._deadline()
        while True:
            while not exhausted and len(pending) < cap:
                try:
                    item = next(source)
                except StopIteration:
                    exhausted = True
                    break
                pending.add(executor.submit(call, item))
            if not pending:
                break
            done, _ = wait(tuple(pending), timeout=self._remaining(deadline),
                           return_when=FIRST_COMPLETED)
            if not done:
                for future in pending:
                    future.cancel()
                raise TimeoutError(f"tasks did not finish within {self.timeout}s")
            for future in done:
                pending.discard(future)
                value = self._coerce(future.result())
                if value is not _SKIP:
                    yield value

    # -- public API ---------------------------------------------------------
    def run(self, items: Iterable[Any], func: Callable[..., Any] | None = None,
            **overrides: Any) -> ParallelResult:
        mode = overrides.pop("mode", self.mode)
        unordered = bool(overrides.pop("unordered", self.unordered))
        if overrides:
            raise TypeError(f"unknown overrides: {sorted(overrides)}")
        if mode not in ("map", "submit"):
            raise ValueError("mode must be 'map' or 'submit'")
        if mode == "map" and unordered:
            raise ValueError("unordered requires mode='submit'")
        payload = normalize_items(items, star=self.star)
        if not payload:
            return self._empty()
        worker = self._resolve_func(func)
        workers = self._resolve_workers(len(payload))
        chunksize = self._resolve_chunksize(len(payload), workers)
        call = self._wrap_call(worker, star_unpack=self.star)
        started = time.perf_counter()
        with self._executor_scope(workers) as executor:
            if mode == "map":
                raw = list(self._executor_map(executor, call, payload, chunksize))
            else:
                raw = self._submit_bounded(executor, call, payload, workers, unordered)
        elapsed = time.perf_counter() - started
        primitive = (
            f"concurrent.futures.{self.kind.executor_name}."
            f"{'map' if mode == 'map' else 'submit'}"
        )
        return self._finalize(
            raw,
            payload,
            elapsed,
            kind=self.kind.value,
            mode=mode,
            workers=workers,
            chunksize=chunksize if mode == "map" else None,
            max_tasks_per_child_active=self.max_tasks_per_child_active,
            primitive=primitive,
        )

    def iter_results(self, items: Iterable[Any], func: Callable[..., Any] | None = None
                     ) -> Iterator[Any]:
        payload = normalize_items(items, star=self.star)
        if not payload:
            return
        worker = self._resolve_func(func)
        workers = self._resolve_workers(len(payload))
        chunksize = self._resolve_chunksize(len(payload), workers)
        call = self._wrap_call(worker, star_unpack=self.star)
        with self._executor_scope(workers) as executor:
            if self.mode == "map":
                for value in self._executor_map(executor, call, payload, chunksize):
                    converted = self._coerce(value)
                    if converted is not _SKIP:
                        yield converted
            else:
                yield from self._submit_stream(executor, call, payload, workers)

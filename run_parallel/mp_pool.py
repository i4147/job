"""Class 1/3: all seven ``multiprocessing.Pool`` methods behind one API."""
from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
import multiprocessing as mp
from multiprocessing import TimeoutError as MpTimeoutError
import time
from typing import Any

from .core import (
    BaseRunner,
    MpMethod,
    ParallelResult,
    _CollectArgs,
    _SKIP,
    default_start_method,
    normalize_items,
)


class MpPoolRunner(BaseRunner):
    """`multiprocessing.Pool` with a uniform ``run()`` / ``iter_results()`` API.

    Methods (``method=``):
        ``map``, ``imap``, ``starmap``, ``apply_async``, ``map_async``,
        ``starmap_async``, ``imap_unordered``.

    Best practices applied
    ----------------------
    * explicit ``mp.get_context(...)`` (3.14 Linux defaults to ``forkserver``),
      so a threaded parent never forks accidentally;
    * workers capped at ``len(items)`` plus an automatic ~4-chunks-per-worker
      ``chunksize``;
    * ``apply_async`` uses a **bounded** in-flight window (no unbounded result
      queue); ``imap`` is exposed as a generator through ``iter_results()``;
    * ``imap_unordered(ordered=True)`` restores input order via index tagging;
    * ``maxtasksperchild`` recycles workers and returns memory to the OS;
    * ``timeout`` applies to ``AsyncResult.get`` / ``IMapIterator.next``;
    * ``error_policy`` (raise/collect/skip) and ``retries`` work for all methods;
    * ``with MpPoolRunner(...) as runner:`` reuses one pool across runs.
    """

    backend_name = "mp"
    primitive_label = "multiprocessing.Pool"

    def __init__(
        self,
        func: Callable[..., Any] | None = None,
        method: MpMethod | str = MpMethod.MAP,
        *,
        start_method: str | None = None,
        initializer: Callable[..., Any] | None = None,
        initargs: tuple[Any, ...] = (),
        maxtasksperchild: int | None = None,
        max_pending: int | None = None,
        **base_kwargs: Any,
    ) -> None:
        super().__init__(func, **base_kwargs)
        self.method = MpMethod.parse(method)
        self.start_method = start_method
        self.initializer = initializer
        self.initargs = tuple(initargs)
        self.maxtasksperchild = maxtasksperchild
        self.max_pending = max_pending
        if start_method is not None:
            mp.get_context(start_method)  # fail early on an unknown method
        if max_pending is not None and max_pending < 1:
            raise ValueError("max_pending must be >= 1")

    # -- pool lifecycle -----------------------------------------------------
    def _start_method_name(self) -> str:
        return self.start_method or default_start_method()

    def _make_pool(self, workers: int) -> Any:
        context = mp.get_context(self.start_method) if self.start_method else mp.get_context()
        return context.Pool(
            processes=workers,
            initializer=self.initializer,
            initargs=self.initargs,
            maxtasksperchild=self.maxtasksperchild,
        )

    @contextmanager
    def _pool_scope(self, workers: int) -> Iterator[Any]:
        if self._resource is not None:
            yield self._resource
            return
        pool = self._make_pool(workers)
        if self._persistent:
            self._resource = pool
        try:
            yield pool
        finally:
            if not self._persistent:
                pool.close()
                pool.join()

    def close(self) -> None:
        pool, self._resource = self._resource, None
        if pool is not None:
            try:
                pool.close()
                pool.join()
            except (ValueError, OSError):  # already closed
                pass

    # -- dispatch -----------------------------------------------------------
    def _next(self, next_: Callable[..., Any]) -> Any:
        """``IMapIterator.next``, normalising mp.TimeoutError -> TimeoutError."""
        try:
            return next_(self.timeout) if self.timeout else next_()
        except MpTimeoutError as exc:
            raise TimeoutError(f"pool result exceeded {self.timeout}s") from exc

    def _get(self, async_result: Any) -> Any:
        """``AsyncResult.get``/``MapResult.get`` with a unified TimeoutError."""
        try:
            return async_result.get(timeout=self.timeout)
        except MpTimeoutError as exc:
            raise TimeoutError(f"pool result exceeded {self.timeout}s") from exc

    def _drain(self, iterator: Any) -> list[Any]:
        """Drain an IMapIterator, honouring ``self.timeout`` per ``next``."""
        next_ = getattr(iterator, "next", None)
        if next_ is None:
            return list(iterator)
        out: list[Any] = []
        while True:
            try:
                out.append(self._next(next_))
            except StopIteration:
                return out

    def _drain_iter(self, iterator: Any) -> Iterator[Any]:
        next_ = getattr(iterator, "next", None)
        if next_ is None:
            yield from iterator
            return
        while True:
            try:
                yield self._next(next_)
            except StopIteration:
                return

    def _apply_async(self, pool: Any, call: Callable[..., Any], payload: list[Any],
                     workers: int) -> list[Any]:
        window = self.max_pending or max(4 * workers, workers)
        results: list[Any] = [None] * len(payload)
        pending: deque[tuple[int, Any]] = deque()
        source = iter(enumerate(payload))
        exhausted = False
        while True:
            while not exhausted and len(pending) < window:
                try:
                    index, item = next(source)
                except StopIteration:
                    exhausted = True
                    break
                pending.append((index, pool.apply_async(call, (item,))))
            if not pending:
                break
            index, async_result = pending.popleft()
            results[index] = self._get(async_result)
        return results

    def _dispatch(self, pool: Any, method: MpMethod, call: Callable[..., Any],
                  payload: list[Any], workers: int, chunksize: int,
                  ordered: bool) -> list[Any]:
        if method is MpMethod.MAP:
            return list(pool.map(call, payload, chunksize=chunksize))
        if method is MpMethod.MAP_ASYNC:
            return list(self._get(pool.map_async(call, payload, chunksize=chunksize)))
        if method is MpMethod.STARMAP:
            return list(pool.starmap(_CollectArgs(call), payload, chunksize=chunksize))
        if method is MpMethod.STARMAP_ASYNC:
            return list(
                self._get(pool.starmap_async(_CollectArgs(call), payload, chunksize=chunksize))
            )
        if method is MpMethod.IMAP:
            return self._drain(pool.imap(call, payload, chunksize=chunksize))
        if method is MpMethod.IMAP_UNORDERED:
            if ordered:
                slots: list[Any] = [None] * len(payload)
                for index, value in pool.imap_unordered(call, enumerate(payload), chunksize=chunksize):
                    slots[index] = value
                return slots
            return self._drain(pool.imap_unordered(call, payload, chunksize=chunksize))
        if method is MpMethod.APPLY_ASYNC:
            return self._apply_async(pool, call, payload, workers)
        raise AssertionError(f"unhandled mp method: {method}")

    # -- public API ---------------------------------------------------------
    def run(self, items: Iterable[Any], func: Callable[..., Any] | None = None,
            **overrides: Any) -> ParallelResult:
        method = MpMethod.parse(overrides.pop("method", self.method))
        ordered = bool(overrides.pop("ordered", self.ordered))
        if overrides:
            raise TypeError(f"unknown overrides: {sorted(overrides)}")
        star = self.star or method.is_star
        payload = normalize_items(items, star=star)
        if not payload:
            return self._empty()
        worker = self._resolve_func(func)
        workers = self._resolve_workers(len(payload))
        chunksize = self._resolve_chunksize(len(payload), workers)
        tagged = method.is_unordered and ordered
        call = self._wrap_call(worker, star_unpack=star, tagged=tagged)
        started = time.perf_counter()
        with self._pool_scope(workers) as pool:
            raw = self._dispatch(pool, method, call, payload, workers, chunksize, ordered)
        elapsed = time.perf_counter() - started
        return self._finalize(
            raw,
            payload,
            elapsed,
            method=method.value,
            workers=workers,
            chunksize=chunksize,
            start_method=self._start_method_name(),
            primitive=f"multiprocessing.Pool.{method.value}",
        )

    def iter_results(self, items: Iterable[Any], func: Callable[..., Any] | None = None
                     ) -> Iterator[Any]:
        """Stream results without accumulating a full list (imap style)."""
        if not self.method.is_lazy:
            yield from super().iter_results(items, func)
            return
        star = self.star or self.method.is_star
        payload = normalize_items(items, star=star)
        if not payload:
            return
        worker = self._resolve_func(func)
        workers = self._resolve_workers(len(payload))
        chunksize = self._resolve_chunksize(len(payload), workers)
        tagged = self.method is MpMethod.IMAP_UNORDERED and self.ordered
        call = self._wrap_call(worker, star_unpack=star, tagged=tagged)
        source: Iterable[Any] = list(enumerate(payload)) if tagged else payload
        with self._pool_scope(workers) as pool:
            if self.method is MpMethod.IMAP:
                for value in self._drain_iter(pool.imap(call, source, chunksize=chunksize)):
                    converted = self._coerce(value)
                    if converted is not _SKIP:
                        yield converted
            else:
                stream = pool.imap_unordered(call, source, chunksize=chunksize)
                if not tagged:
                    for value in self._drain_iter(stream):
                        converted = self._coerce(value)
                        if converted is not _SKIP:
                            yield converted
                else:
                    expected = 0
                    buffer: dict[int, Any] = {}
                    for index, value in self._drain_iter(stream):
                        buffer[index] = self._coerce(value)
                        while expected in buffer:
                            item = buffer.pop(expected)
                            if item is not _SKIP:
                                yield item
                            expected += 1

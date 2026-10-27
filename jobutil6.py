from __future__ import annotations
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
    as_completed,
    wait,
)
from enum import Enum
import math
import multiprocessing as mp
import multiprocessing.dummy
import sys
import time

# InterpreterPoolExecutor was added in CPython 3.14; make it optional so this
# module is importable (and benchmarkable) on older interpreters.
_HAS_INTERPRETER_POOL = sys.version_info >= (3, 14)
InterpreterPoolExecutor = None
if _HAS_INTERPRETER_POOL:
    try:
        from concurrent.futures import InterpreterPoolExecutor
    except ImportError:  # pragma: no cover - 3.14 layout changed
        try:
            from concurrent.futures.interpreter import InterpreterPoolExecutor
        except ImportError:
            _HAS_INTERPRETER_POOL = False
__all__ = ["Backend", "Parallel", "PoolMethod", "MAX_WORKERS"]
# These modules are plain top-level scripts, not a package: a relative import
# (``from .const import ...``) fails here.  Keep the shared constant local.
MAX_WORKERS = 8


class PoolMethod(str, Enum):
    """Pool call styles. Star methods unpack each item as ``fn(*item)``."""

    MAP = "map"
    MAP_ASYNC = "map_async"
    STARMAP = "starmap"
    STARMAP_ASYNC = "starmap_async"
    IMAP = "imap"
    IMAP_UNORDERED = "imap_unordered"
    APPLY_ASYNC = "apply_async"

    @classmethod
    def parse(cls, value):
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower().replace("-", "_"))
        except ValueError as exc:
            valid = ", ".join(member.value for member in cls)
            msg = f"unknown method {value!r}; expected one of: {valid}"
            raise ValueError(msg) from exc

    @property
    def starred(self):
        return self in {PoolMethod.STARMAP, PoolMethod.STARMAP_ASYNC, PoolMethod.APPLY_ASYNC}


class Backend(str, Enum):
    THREAD = "thread"
    PROCESS = "process"
    CF_THREAD = "cf_thread"
    CF_PROCESS = "cf_process"
    CF_INTERPRETER = "cf_interpreter"
    SERIAL = "serial"

    @classmethod
    def parse(cls, value):
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower().replace("-", "_"))
        except ValueError as exc:
            valid = ", ".join(member.value for member in cls)
            msg = f"unknown backend {value!r}; expected one of: {valid}"
            raise ValueError(msg) from exc


def _star(fn, item):
    return fn(*item)


def _chunksize(n_items, workers):
    return max(1, min(500, math.ceil(n_items / (workers * 4))))


def _check_star_items(items):
    for index, item in enumerate(items):
        if not isinstance(item, (tuple, list)):
            msg = f"items[{index}] must be a tuple or list for a star method; got {type(item).__name__}"
            raise TypeError(msg)


class Parallel:
    def __init__(self, workers=MAX_WORKERS, backend=Backend.PROCESS):
        if workers < 1:
            msg = f"workers must be >= 1, got {workers!r}"
            raise ValueError(msg)
        self.workers = workers
        self.backend = Backend.parse(backend)

    def map(
        self,
        func,
        items,
        method=PoolMethod.MAP,
        timeout=None,
    ):
        if not callable(func):
            msg = "func must be callable"
            raise TypeError(msg)
        if timeout is not None and timeout <= 0:
            msg = "timeout must be > 0"
            raise ValueError(msg)
        pool_method = PoolMethod.parse(method)
        payload = list(items)
        n_items = len(payload)
        if n_items == 0:
            return []
        if pool_method.starred:
            _check_star_items(payload)
        workers = min(self.workers, n_items)
        if workers == 1 or self.backend is Backend.SERIAL:
            return self._serial(func, payload, pool_method)
        chunksize = _chunksize(n_items, workers)
        if self.backend in {Backend.THREAD, Backend.PROCESS}:
            return self._mp(func, payload, pool_method, workers, chunksize, timeout)
        return self._cf(func, payload, pool_method, workers, chunksize, timeout)

    def imap(
        self,
        func,
        items,
        ordered=True,
    ):
        if not callable(func):
            msg = "func must be callable"
            raise TypeError(msg)
        if self.backend not in {Backend.THREAD, Backend.PROCESS, Backend.SERIAL}:
            msg = "imap() only supports the thread, process, and serial backends"
            raise ValueError(msg)
        payload = list(items)
        if not payload:
            return
        workers = min(self.workers, len(payload))
        if workers == 1 or self.backend is Backend.SERIAL:
            yield from self._serial(func, payload, PoolMethod.MAP)
            return
        chunksize = _chunksize(len(payload), workers)
        pool = self._mp_pool(workers)
        failed = False
        try:
            stream = pool.imap(func, payload, chunksize) if ordered else pool.imap_unordered(func, payload, chunksize)
            yield from stream
        except BaseException:
            failed = True
            raise
        finally:
            self._close_pool(pool, terminate=failed)

    def _serial(
        self,
        func,
        items,
        method,
    ):
        if method.starred:
            return [func(*item) for item in items]
        return [func(item) for item in items]

    def _mp_pool(self, workers):
        if self.backend is Backend.THREAD:
            return multiprocessing.dummy.Pool(processes=workers)
        return mp.get_context("spawn").Pool(processes=workers)

    @staticmethod
    def _close_pool(pool, *, terminate):
        if terminate:
            pool.terminate()
        else:
            pool.close()
        pool.join()

    def _mp(
        self,
        func,
        items,
        method,
        workers,
        chunksize,
        timeout,
    ):
        pool = self._mp_pool(workers)
        failed = False
        try:
            if method is PoolMethod.MAP:
                return list(pool.map(func, items, chunksize))
            if method is PoolMethod.STARMAP:
                return list(pool.starmap(func, items, chunksize))
            if method is PoolMethod.IMAP:
                return self._drain(pool.imap(func, items, chunksize), timeout)
            if method is PoolMethod.IMAP_UNORDERED:
                return self._drain(pool.imap_unordered(func, items, chunksize), timeout)
            window = max(1, workers * 4)
            results = []
            for start in range(0, len(items), window):
                batch = items[start : start + window]
                if method is PoolMethod.MAP_ASYNC:
                    results.extend(pool.map_async(func, batch, chunksize).get(timeout))
                elif method is PoolMethod.STARMAP_ASYNC:
                    results.extend(pool.starmap_async(func, batch, chunksize).get(timeout))
                elif method is PoolMethod.APPLY_ASYNC:
                    pending = [pool.apply_async(_star, (func, args)) for args in batch]
                    results.extend(job.get(timeout) for job in pending)
                else:
                    msg = f"unhandled method: {method}"
                    raise AssertionError(msg)
            return results
        except BaseException:
            failed = True
            raise
        finally:
            self._close_pool(pool, terminate=failed)

    @staticmethod
    def _drain(iterator, timeout):
        pull = getattr(iterator, "next", None)
        if pull is None:
            return list(iterator)
        results = []
        while True:
            try:
                results.append(pull() if timeout is None else pull(timeout))
            except StopIteration:
                return results

    def _executor(self, workers):
        if self.backend is Backend.CF_THREAD:
            return ThreadPoolExecutor(max_workers=workers)
        if self.backend is Backend.CF_PROCESS:
            return ProcessPoolExecutor(
                max_workers=workers,
                mp_context=mp.get_context("spawn"),
            )
        if self.backend is Backend.CF_INTERPRETER:
            if not _HAS_INTERPRETER_POOL or InterpreterPoolExecutor is None:
                msg = "cf_interpreter requires Python 3.14+ (concurrent.futures.InterpreterPoolExecutor)"
                raise ImportError(msg)
            return InterpreterPoolExecutor(max_workers=workers)
        msg = f"backend {self.backend} is not an executor"
        raise AssertionError(msg)

    def _cf(
        self,
        func,
        items,
        method,
        workers,
        chunksize,
        timeout,
    ):
        executor = self._executor(workers)
        ordered = method is not PoolMethod.IMAP_UNORDERED
        streaming = method in {PoolMethod.MAP, PoolMethod.IMAP} and timeout is None
        failed = False
        try:
            if streaming:
                return list(executor.map(func, items, chunksize=chunksize))
            window = max(1, workers * 4)
            results = []
            deadline = None if timeout is None else time.monotonic() + timeout
            for start in range(0, len(items), window):
                batch = items[start : start + window]
                futures = self._submit(executor, func, batch, method)
                remaining = self._remaining(deadline)
                results.extend(self._collect(futures, remaining, ordered=ordered))
            return results
        except BaseException:
            failed = True
            raise
        finally:
            try:
                executor.shutdown(wait=not failed, cancel_futures=failed)
            except TypeError:
                executor.shutdown(wait=not failed)

    @staticmethod
    def _submit(
        executor,
        func,
        batch,
        method,
    ):
        if method.starred:
            return [executor.submit(_star, func, args) for args in batch]
        return [executor.submit(func, item) for item in batch]

    @staticmethod
    def _remaining(deadline):
        if deadline is None:
            return None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            msg = "timed out before the next batch started"
            raise FutureTimeoutError(msg)
        return remaining

    @staticmethod
    def _collect(
        futures,
        timeout,
        *,
        ordered,
    ):
        if not ordered:
            return [item.result() for item in as_completed(futures, timeout=timeout)]
        if timeout is None:
            return [item.result() for item in futures]
        _done, pending = wait(futures, timeout=timeout)
        if pending:
            for item in pending:
                item.cancel()
            msg = f"{len(pending)} of {len(futures)} tasks did not complete"
            raise FutureTimeoutError(msg)
        return [item.result() for item in futures]

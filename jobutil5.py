from __future__ import annotations
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
    wait,
)
from enum import Enum
from functools import partial
import logging
import multiprocessing as mp
import sys
import time

__all__ = [
    "DEFAULT_CHUNKSIZE",
    "DEFAULT_EXECUTOR_CHUNKSIZE",
    "MAX_WORKERS",
    "PoolMethod",
    "mpf",
]
logger = logging.getLogger(__name__)
MAX_WORKERS = 8
DEFAULT_CHUNKSIZE = None
DEFAULT_EXECUTOR_CHUNKSIZE = 1
ExecutorType = type[ProcessPoolExecutor] | type[ThreadPoolExecutor]


class PoolMethod(str, Enum):
    MAP = "map"
    MAP_ASYNC = "map_async"
    STARMAP = "starmap"
    STARMAP_ASYNC = "starmap_async"
    IMAP = "imap"
    IMAP_UNORDERED = "imap_unordered"
    APPLY = "apply"
    APPLY_ASYNC = "apply_async"
    JOBLIB = "joblib"
    PROCESS_POOL = "process_pool"
    THREAD_POOL = "thread_pool"

    @classmethod
    def parse(cls, value):
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).lower())
        except ValueError as exc:
            valid = ", ".join(m.value for m in cls)
            msg = f"unknown method {value!r}; expected one of: {valid}"
            raise ValueError(msg) from exc

    @property
    def is_mp_pool(self):
        return self.value in _MP_POOL_METHODS


_MP_POOL_METHODS = {
    "map",
    "map_async",
    "starmap",
    "starmap_async",
    "imap",
    "imap_unordered",
    "apply",
    "apply_async",
}


def _dispatch_mp_pool(
    pool,
    method,
    func,
    items,
    chunksize,
):
    if method is PoolMethod.MAP:
        return pool.map(func, items, chunksize=chunksize)
    if method is PoolMethod.MAP_ASYNC:
        return pool.map_async(func, items, chunksize=chunksize).get()
    if method in (PoolMethod.STARMAP, PoolMethod.STARMAP_ASYNC):
        args = [item if isinstance(item, tuple) else (item,) for item in items]
        if method is PoolMethod.STARMAP:
            return pool.starmap(func, args, chunksize=chunksize)
        return pool.starmap_async(func, args, chunksize=chunksize).get()
    if method is PoolMethod.IMAP:
        return list(pool.imap(func, items, chunksize=chunksize))
    if method is PoolMethod.IMAP_UNORDERED:
        return list(pool.imap_unordered(func, items, chunksize=chunksize))
    if method is PoolMethod.APPLY:
        return [pool.apply(func, args=(item,)) for item in items]
    if method is PoolMethod.APPLY_ASYNC:
        futs = [pool.apply_async(func, args=(item,)) for item in items]
        return [f.get() for f in futs]
    msg = f"unhandled mp.pool method: {method}"
    raise AssertionError(msg)


def _run_mp_pool(
    method,
    func,
    items,
    *,
    workers,
    chunksize,
    initializer,
    initargs,
    maxtasksperchild,
    context,
):
    items_list = list(items)
    if not items_list:
        return []
    ctx = mp.get_context(context)
    pool_kwargs = {"processes": workers}
    if initializer is not None:
        pool_kwargs["initializer"] = initializer
        pool_kwargs["initargs"] = tuple(initargs)
    if maxtasksperchild is not None:
        pool_kwargs["maxtasksperchild"] = maxtasksperchild
    with ctx.Pool(**pool_kwargs) as pool:
        return _dispatch_mp_pool(pool, method, func, items_list, chunksize)


def _submit_args(item, unpack):
    if unpack:
        return item if isinstance(item, tuple) else (item,)
    return (item,)


def _run_executor(
    executor_cls,
    func,
    items,
    *,
    workers,
    chunksize,
    unpack,
    initializer,
    initargs,
    maxtasksperchild,
    timeout,
    return_exceptions,
    max_pending,
    on_progress,
):
    is_process = executor_cls is ProcessPoolExecutor
    kwargs = {"max_workers": workers}
    if initializer is not None:
        kwargs["initializer"] = initializer
        kwargs["initargs"] = tuple(initargs)
    if maxtasksperchild is not None:
        if not is_process:
            msg = "maxtasksperchild is only supported by ProcessPoolExecutor"
            raise ValueError(msg)
        if sys.version_info < (3, 11):
            msg = "maxtasksperchild requires Python 3.11+"
            raise ValueError(msg)
        kwargs["max_tasks_per_child"] = maxtasksperchild
    if not unpack and timeout is None and on_progress is None and not return_exceptions and max_pending is None:
        with executor_cls(**kwargs) as ex:
            if is_process:
                cs = chunksize if chunksize is not None else DEFAULT_EXECUTOR_CHUNKSIZE
                return list(ex.map(func, items, chunksize=cs))
            return list(ex.map(func, items))
    items_list = list(items)
    n = len(items_list)
    if n == 0:
        return []
    cap = max_pending if max_pending is not None else max(4 * workers, workers)
    cap = max(cap, 1)
    results = [None] * n
    item_iter = iter(enumerate(items_list))
    pending = {}
    exhausted = False

    def _fill(ex):
        nonlocal exhausted
        while len(pending) < cap and not exhausted:
            try:
                idx, item = next(item_iter)
            except StopIteration:
                exhausted = True
                return
            fut = ex.submit(func, *_submit_args(item, unpack))
            pending[fut] = (idx, time.monotonic())

    with executor_cls(**kwargs) as ex:
        _fill(ex)
        while pending:
            if timeout is not None:
                earliest = min(t for _, t in pending.values())
                wait_for = max(0.0, timeout - (time.monotonic() - earliest))
            else:
                wait_for = None
            done, _ = wait(
                pending.keys(),
                return_when=FIRST_COMPLETED,
                timeout=wait_for,
            )
            for fut in done:
                info = pending.pop(fut, None)
                if info is None:
                    continue
                idx = info[0]
                try:
                    results[idx] = fut.result()
                except Exception as exc:
                    logger.debug("task %d failed: %r", idx, exc)
                    results[idx] = exc
                if on_progress is not None:
                    on_progress(idx, results[idx])
            if timeout is not None:
                now = time.monotonic()
                timed_out = [f for f, (_, t) in pending.items() if now - t >= timeout]
                for fut in timed_out:
                    idx = pending.pop(fut)[0]
                    fut.cancel()
                    err = FutureTimeoutError(f"task {idx} exceeded {timeout}s")
                    logger.debug("task %d timed out after %.3fs", idx, timeout)
                    results[idx] = err
                    if on_progress is not None:
                        on_progress(idx, err)
            _fill(ex)
    if not return_exceptions:
        for r in results:
            if isinstance(r, BaseException):
                raise r
    return results


def _run_joblib(
    func,
    items,
    workers,
    unpack,
):
    try:
        from joblib import Parallel, delayed
    except ImportError as exc:
        msg = "the 'joblib' backend requires joblib — `pip install joblib`"
        raise ImportError(msg) from exc
    n_jobs = -1 if workers is None else workers
    items_list = list(items)
    if unpack:
        return Parallel(n_jobs=n_jobs)(
            delayed(func)(*(item if isinstance(item, tuple) else (item,))) for item in items_list
        )
    return Parallel(n_jobs=n_jobs)(delayed(func)(item) for item in items_list)


def mpf(
    func,
    items,
    method=PoolMethod.IMAP_UNORDERED,
    *,
    workers=MAX_WORKERS,
    chunksize=DEFAULT_CHUNKSIZE,
    unpack=False,
    func_args=(),
    func_kwargs=None,
    initializer=None,
    initargs=(),
    maxtasksperchild=None,
    context=None,
    timeout=None,
    return_exceptions=False,
    max_pending=None,
    on_progress=None,
):
    if not callable(func):
        msg = "`func` must be callable"
        raise TypeError(msg)
    pool_method = PoolMethod.parse(method)
    if func_args or func_kwargs:
        func = partial(func, *func_args, **(func_kwargs or {}))
    if pool_method.is_mp_pool:
        if timeout is not None:
            msg = (
                "timeout is only supported by the executor backends "
                "('process_pool' / 'thread_pool'); use one of those or "
                "handle deadlines inside your worker"
            )
            raise ValueError(msg)
        return _run_mp_pool(
            pool_method,
            func,
            items,
            workers=workers,
            chunksize=chunksize,
            initializer=initializer,
            initargs=initargs,
            maxtasksperchild=maxtasksperchild,
            context=context,
        )
    if pool_method is PoolMethod.JOBLIB:
        if initializer is not None or maxtasksperchild is not None:
            msg = "the 'joblib' backend does not support initializer/maxtasksperchild; use 'process_pool' instead"
            raise ValueError(msg)
        if timeout is not None:
            msg = "the 'joblib' backend does not support timeout"
            raise ValueError(msg)
        return _run_joblib(func, items, workers, unpack)
    if pool_method is PoolMethod.PROCESS_POOL:
        return _run_executor(
            ProcessPoolExecutor,
            func,
            items,
            workers=workers,
            chunksize=chunksize,
            unpack=unpack,
            initializer=initializer,
            initargs=initargs,
            maxtasksperchild=maxtasksperchild,
            timeout=timeout,
            return_exceptions=return_exceptions,
            max_pending=max_pending,
            on_progress=on_progress,
        )
    if pool_method is PoolMethod.THREAD_POOL:
        if maxtasksperchild is not None:
            msg = "ThreadPoolExecutor does not support maxtasksperchild"
            raise ValueError(msg)
        return _run_executor(
            ThreadPoolExecutor,
            func,
            items,
            workers=workers,
            chunksize=chunksize,
            unpack=unpack,
            initializer=initializer,
            initargs=initargs,
            maxtasksperchild=None,
            timeout=timeout,
            return_exceptions=return_exceptions,
            max_pending=max_pending,
            on_progress=on_progress,
        )
    msg = f"unhandled method: {pool_method}"
    raise AssertionError(msg)

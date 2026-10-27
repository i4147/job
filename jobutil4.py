from __future__ import annotations
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from enum import Enum
from functools import partial
import multiprocessing as mp

__all__ = ["DEFAULT_CHUNKSIZE", "MAX_WORKERS", "PoolMethod", "run_parallel"]
MAX_WORKERS = 8
DEFAULT_CHUNKSIZE = None
DEFAULT_EXECUTOR_CHUNKSIZE = 1


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


_MP_POOL_METHODS = frozenset({
    "map",
    "map_async",
    "starmap",
    "starmap_async",
    "imap",
    "imap_unordered",
    "apply",
    "apply_async",
})


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
    workers,
    chunksize,
    initializer,
    initargs,
    maxtasksperchild,
    context,
):
    ctx = mp.get_context(context) if context else mp.get_context()
    with ctx.Pool(
        processes=workers,
        initializer=initializer,
        initargs=tuple(initargs),
        maxtasksperchild=maxtasksperchild,
    ) as pool:
        return _dispatch_mp_pool(pool, method, func, items, chunksize)


def _run_executor(
    executor_cls,
    func,
    items,
    workers,
    chunksize,
    unpack,
    initializer,
    initargs,
    maxtasksperchild,
):
    kwargs = {"max_workers": workers}
    if initializer is not None:
        kwargs["initializer"] = initializer
        kwargs["initargs"] = tuple(initargs)
    if maxtasksperchild is not None and executor_cls is ProcessPoolExecutor:
        kwargs["max_tasks_per_child"] = maxtasksperchild
    with executor_cls(**kwargs) as ex:
        if unpack:
            futs = [ex.submit(func, *(item if isinstance(item, tuple) else (item,))) for item in items]
            return [f.result() for f in futs]
        if executor_cls is ProcessPoolExecutor:
            return list(ex.map(func, items, chunksize=chunksize or DEFAULT_EXECUTOR_CHUNKSIZE))
        return list(ex.map(func, items))


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
    if unpack:
        return Parallel(n_jobs=n_jobs)(delayed(func)(*(item if isinstance(item, tuple) else (item,))) for item in items)
    return Parallel(n_jobs=n_jobs)(delayed(func)(item) for item in items)


def run_parallel(
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
):
    if not callable(func):
        msg = "`func` must be callable"
        raise TypeError(msg)
    pool_method = PoolMethod.parse(method)
    if func_args or func_kwargs:
        func = partial(func, *func_args, **(func_kwargs or {}))
    items = list(items)
    if pool_method.is_mp_pool:
        return _run_mp_pool(
            pool_method,
            func,
            items,
            workers,
            chunksize,
            initializer,
            initargs,
            maxtasksperchild,
            context,
        )
    if pool_method is PoolMethod.JOBLIB:
        if initializer is not None or maxtasksperchild is not None:
            msg = (
                "the 'joblib' backend does not support initializer/"
                "maxtasksperchild; use 'process_pool' or 'map' instead"
            )
            raise ValueError(msg)
        return _run_joblib(func, items, workers, unpack)
    if pool_method is PoolMethod.PROCESS_POOL:
        return _run_executor(
            ProcessPoolExecutor,
            func,
            items,
            workers,
            chunksize,
            unpack,
            initializer,
            initargs,
            maxtasksperchild,
        )
    if pool_method is PoolMethod.THREAD_POOL:
        if maxtasksperchild is not None:
            msg = "ThreadPoolExecutor does not support maxtasksperchild"
            raise ValueError(msg)
        return _run_executor(
            ThreadPoolExecutor,
            func,
            items,
            workers,
            chunksize,
            unpack,
            initializer,
            initargs,
            None,
        )
    msg = f"unhandled method: {pool_method}"
    raise AssertionError(msg)

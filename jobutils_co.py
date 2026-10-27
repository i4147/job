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
from pathlib import Path
import time
from typing import TYPE_CHECKING
logger = logging.getLogger(__name__)
MAX_WORKERS = 8
DEFAULT_CHUNKSIZE = 4
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
class _PoolDispatcher:
    def __init__(
        self,
        pool,
        method,
        func,
        items,
        chunksize,
    ):
        self.pool = pool
        self.method = method
        self.func = func
        self.items = items
        self.chunksize = chunksize
    def dispatch(self):
        if self.method is PoolMethod.MAP:
            return self.pool.map(self.func, self.items, chunksize=self.chunksize)
        if self.method is PoolMethod.MAP_ASYNC:
            return self.pool.map_async(self.func, self.items, chunksize=self.chunksize).get()
        if self.method in (PoolMethod.STARMAP, PoolMethod.STARMAP_ASYNC):
            args = self._normalize_starmap_args()
            if self.method is PoolMethod.STARMAP:
                return self.pool.starmap(self.func, args, chunksize=self.chunksize)
            return self.pool.starmap_async(self.func, args, chunksize=self.chunksize).get()
        if self.method is PoolMethod.IMAP:
            return list(self.pool.imap(self.func, self.items, chunksize=self.chunksize))
        if self.method is PoolMethod.IMAP_UNORDERED:
            return list(self.pool.imap_unordered(self.func, self.items, chunksize=self.chunksize))
        if self.method is PoolMethod.APPLY:
            return self._apply_sequential()
        if self.method is PoolMethod.APPLY_ASYNC:
            return self._apply_async_sequential()
        msg = f"unhandled mp.pool method: {self.method}"
        raise AssertionError(msg)
    def _normalize_starmap_args(self):
        return [item if isinstance(item, tuple) else (item,) for item in self.items]
    def _apply_sequential(self):
        return [self.pool.apply(self.func, args=(item,)) for item in self.items]
    def _apply_async_sequential(self):
        futs = [self.pool.apply_async(self.func, args=(item,)) for item in self.items]
        return [f.get() for f in futs]
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
    pool_kwargs = _build_pool_kwargs(workers, initializer, initargs, maxtasksperchild)
    with ctx.Pool(**pool_kwargs) as pool:
        dispatcher = _PoolDispatcher(pool, method, func, items_list, chunksize)
        return dispatcher.dispatch()
def _build_pool_kwargs(
    workers,
    initializer,
    initargs,
    maxtasksperchild,
):
    kwargs = {"processes": workers}
    if initializer is not None:
        kwargs["initializer"] = initializer
        kwargs["initargs"] = tuple(initargs)
    if maxtasksperchild is not None:
        kwargs["maxtasksperchild"] = maxtasksperchild
    return kwargs
def _submit_args(item, unpack):
    if unpack:
        return item if isinstance(item, tuple) else (item,)
    return (item,)
class _ExecutorFutureManager:
    def __init__(
        self,
        executor_cls,
        func,
        items_list,
        workers,
        chunksize,
        unpack,
        timeout,
        return_exceptions,
        max_pending,
        on_progress,
    ):
        self.executor_cls = executor_cls
        self.func = func
        self.items_list = items_list
        self.workers = workers
        self.chunksize = chunksize
        self.unpack = unpack
        self.timeout = timeout
        self.return_exceptions = return_exceptions
        self.max_pending = max_pending
        self.on_progress = on_progress
        self.n_items = len(items_list)
        self.results = [None] * self.n_items
        self.pending = {}
        self.item_iter = iter(enumerate(items_list))
        self.exhausted = False
        self.cap = max(max_pending if max_pending is not None else max(4 * workers, workers), 1)
    def execute(self):
        kwargs = self._build_executor_kwargs()
        with self.executor_cls(**kwargs) as ex:
            self._fill_queue(ex)
            self._process_futures(ex)
        return self.results
    def _build_executor_kwargs(self):
        return {"max_workers": self.workers}
    def _fill_queue(self, executor):
        while len(self.pending) < self.cap and not self.exhausted:
            try:
                idx, item = next(self.item_iter)
            except StopIteration:
                self.exhausted = True
                return
            args = _submit_args(item, self.unpack)
            fut = executor.submit(self.func, *args)
            self.pending[fut] = (idx, time.monotonic())
    def _process_futures(self, executor):
        while self.pending:
            wait_time = self._calculate_wait_time()
            done, _ = wait(self.pending.keys(), return_when=FIRST_COMPLETED, timeout=wait_time)
            self._handle_completed(done)
            self._handle_timeouts()
            self._fill_queue(executor)
    def _calculate_wait_time(self):
        if self.timeout is None:
            return None
        earliest = min(t for _, t in self.pending.values())
        return max(0.0, self.timeout - (time.monotonic() - earliest))
    def _handle_completed(self, done):
        for fut in done:
            info = self.pending.pop(fut, None)
            if info is None:
                continue
            idx = info[0]
            try:
                self.results[idx] = fut.result()
            except Exception as exc:
                logger.debug("task %d failed: %r", idx, exc)
                self.results[idx] = exc
            if self.on_progress is not None:
                self.on_progress(idx, self.results[idx])
    def _handle_timeouts(self):
        if self.timeout is None:
            return
        now = time.monotonic()
        timed_out = [f for f, (_, t) in self.pending.items() if now - t >= self.timeout]
        for fut in timed_out:
            idx = self.pending.pop(fut)[0]
            fut.cancel()
            err = FutureTimeoutError(f"task {idx} exceeded {self.timeout}s")
            logger.debug("task %d timed out after %.3fs", idx, self.timeout)
            self.results[idx] = err
            if self.on_progress is not None:
                self.on_progress(idx, err)
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
        kwargs["max_tasks_per_child"] = maxtasksperchild
    should_use_simple_path = (
        not unpack and timeout is None and on_progress is None and not return_exceptions and max_pending is None
    )
    if should_use_simple_path:
        with executor_cls(**kwargs) as ex:
            if is_process:
                cs = chunksize if chunksize is not None else DEFAULT_EXECUTOR_CHUNKSIZE
                return list(ex.map(func, items, chunksize=cs))
            return list(ex.map(func, items))
    items_list = list(items)
    if not items_list:
        return []
    manager = _ExecutorFutureManager(
        executor_cls,
        func,
        items_list,
        workers,
        chunksize,
        unpack,
        timeout,
        return_exceptions,
        max_pending,
        on_progress,
    )
    results = manager.execute()
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
        return _handle_mp_pool_method(
            pool_method,
            func,
            items,
            workers,
            chunksize,
            initializer,
            initargs,
            maxtasksperchild,
            context,
            timeout,
        )
    if pool_method is PoolMethod.JOBLIB:
        return _handle_joblib_method(func, items, workers, unpack, initializer, timeout, maxtasksperchild)
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
def _handle_mp_pool_method(
    pool_method,
    func,
    items,
    workers,
    chunksize,
    initializer,
    initargs,
    maxtasksperchild,
    context,
    timeout,
):
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
def _handle_joblib_method(
    func,
    items,
    workers,
    unpack,
    initializer,
    timeout,
    maxtasksperchild,
):
    if initializer is not None or maxtasksperchild is not None:
        msg = "the 'joblib' backend does not support initializer/maxtasksperchild; use 'process_pool' instead"
        raise ValueError(msg)
    if timeout is not None:
        msg = "the 'joblib' backend does not support timeout"
        raise ValueError(msg)
    return _run_joblib(func, items, workers, unpack)

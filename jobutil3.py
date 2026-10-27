from __future__ import annotations
from collections.abc import Sized
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
    wait,
)
from contextlib import suppress
from enum import Enum
from functools import partial
import logging
import multiprocessing as mp
import os
import time

__all__ = [
    "DEFAULT_CHUNKSIZE",
    "DEFAULT_EXECUTOR_CHUNKSIZE",
    "MAX_WORKERS",
    "ExecutorType",
    "PoolMethod",
    "mpf",
    "mpf_iter",
]
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
    SERIAL = "serial"

    @classmethod
    def parse(cls, value):
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower().replace("-", "_"))
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


class _Star:
    __slots__ = ("func",)

    def __init__(self, func):
        self.func = func

    def __call__(self, item):
        return self.func(*item) if isinstance(item, tuple) else self.func(item)


class _Retry:
    __slots__ = ("delay", "func", "retries")

    def __init__(self, func, retries, delay):
        self.func = func
        self.retries = retries
        self.delay = delay

    def __call__(self, *args, **kwargs):
        attempt = 0
        while True:
            try:
                return self.func(*args, **kwargs)
            except Exception:
                if attempt >= self.retries:
                    raise
                if self.delay > 0:
                    time.sleep(self.delay * (2**attempt))
                attempt += 1


class _Catch:
    __slots__ = ("func",)

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        try:
            return self.func(*args, **kwargs)
        except Exception as exc:
            return exc


class _Indexed:
    __slots__ = ("func",)

    def __init__(self, func):
        self.func = func

    def __call__(self, pair):
        idx, item = pair
        return idx, self.func(item)


def _resolve_workers(workers, n):
    resolved = workers if workers and workers > 0 else (os.cpu_count() or 1)
    if n is not None:
        resolved = min(resolved, max(n, 1))
    return max(resolved, 1)


def _resolve_chunksize(chunksize, n, workers):
    if chunksize is None:
        return None
    if chunksize > 0:
        return chunksize
    if n is None:
        return 1
    return max(1, n // (workers * 4))


def _build_call(
    func,
    func_args,
    func_kwargs,
    unpack,
    retries,
    retry_delay,
):
    call = func
    if func_args or func_kwargs:
        call = partial(call, *func_args, **(func_kwargs or {}))
    if unpack:
        call = _Star(call)
    if retries > 0:
        call = _Retry(call, retries, retry_delay)
    return call


def _pool_kwargs(
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


def _run_mp_pool(
    method,
    call,
    items,
    *,
    workers,
    chunksize,
    initializer,
    initargs,
    maxtasksperchild,
    context,
    return_exceptions,
    on_progress,
    ordered,
):
    payload = list(items)
    n = len(payload)
    if n == 0:
        return []
    w = _resolve_workers(workers, n)
    cs = _resolve_chunksize(chunksize, n, w)
    if method in (PoolMethod.STARMAP, PoolMethod.STARMAP_ASYNC):
        call = _Star(call)
        method = PoolMethod.MAP if method is PoolMethod.STARMAP else PoolMethod.MAP_ASYNC
    runner = _Catch(call) if return_exceptions else call
    tagged = method is PoolMethod.IMAP_UNORDERED and (ordered or on_progress is not None)
    if tagged:
        runner = _Indexed(runner)
        payload = list(enumerate(payload))
    ctx = mp.get_context(context)
    with ctx.Pool(**_pool_kwargs(w, initializer, initargs, maxtasksperchild)) as pool:
        if method in (PoolMethod.IMAP, PoolMethod.IMAP_UNORDERED):
            stream = (
                pool.imap(runner, payload, cs or 1)
                if method is PoolMethod.IMAP
                else pool.imap_unordered(runner, payload, cs or 1)
            )
            slots = [None] * n if (tagged and ordered) else []
            for pos, raw in enumerate(stream):
                idx, value = raw if tagged else (pos, raw)
                if tagged and ordered:
                    slots[idx] = value
                else:
                    slots.append(value)
                if on_progress is not None:
                    on_progress(idx, value)
            return slots
        if method is PoolMethod.MAP:
            out = pool.map(runner, payload, cs)
        elif method is PoolMethod.MAP_ASYNC:
            out = pool.map_async(runner, payload, cs).get()
        elif method is PoolMethod.APPLY:
            out = [pool.apply(runner, (x,)) for x in payload]
        elif method is PoolMethod.APPLY_ASYNC:
            futs = [pool.apply_async(runner, (x,)) for x in payload]
            out = [f.get() for f in futs]
        else:
            msg = f"unhandled mp.pool method: {method}"
            raise AssertionError(msg)
    if on_progress is not None:
        for idx, value in enumerate(out):
            on_progress(idx, value)
    return out


def _kill_workers(executor):
    processes = getattr(executor, "_processes", None) or {}
    for proc in list(processes.values()):
        with suppress(Exception):
            proc.terminate()


def _run_executor(
    executor_cls,
    call,
    items,
    *,
    workers,
    chunksize,
    n,
    initializer,
    initargs,
    maxtasksperchild,
    context,
    timeout,
    return_exceptions,
    max_pending,
    on_progress,
    fail_fast,
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
    if is_process and context is not None:
        kwargs["mp_context"] = mp.get_context(context)
    if timeout is None and on_progress is None and max_pending is None:
        runner = _Catch(call) if return_exceptions else call
        with executor_cls(**kwargs) as pool:
            if is_process:
                cs = _resolve_chunksize(chunksize, n, workers) or DEFAULT_EXECUTOR_CHUNKSIZE
                return list(pool.map(runner, items, chunksize=cs))
            return list(pool.map(runner, items))
    if max_pending is not None:
        cap = max(1, max_pending)
    elif timeout is not None:
        cap = workers
    else:
        cap = max(4 * workers, 1)
    source = iter(items)
    pending = {}
    results = {}
    errors = {}
    submitted = 0
    exhausted = False
    hung = False
    failed = False

    def record(idx, value, is_error):
        results[idx] = value
        if is_error:
            errors[idx] = value
        if on_progress is not None:
            on_progress(idx, value)

    ex = executor_cls(**kwargs)
    try:
        while True:
            while not exhausted and len(pending) < cap:
                try:
                    item = next(source)
                except StopIteration:
                    exhausted = True
                    break
                pending[ex.submit(call, item)] = (submitted, time.monotonic())
                submitted += 1
            if not pending:
                break
            wait_for = None
            if timeout is not None:
                earliest = min(started for _, started in pending.values())
                wait_for = max(0.0, timeout - (time.monotonic() - earliest))
            done, _ = wait(list(pending), timeout=wait_for, return_when=FIRST_COMPLETED)
            for fut in done:
                idx = pending.pop(fut)[0]
                try:
                    value = fut.result()
                except Exception as exc:
                    logger.debug("task %d failed: %r", idx, exc)
                    if fail_fast and not return_exceptions:
                        raise
                    record(idx, exc, True)
                else:
                    record(idx, value, False)
            if timeout is not None and pending:
                now = time.monotonic()
                expired = [f for f, (_, started) in pending.items() if now - started >= timeout and not f.done()]
                for fut in expired:
                    idx = pending.pop(fut)[0]
                    if not fut.cancel():
                        hung = True
                    err = FutureTimeoutError(f"task {idx} exceeded {timeout}s")
                    logger.debug("task %d timed out after %.3fs", idx, timeout)
                    if fail_fast and not return_exceptions:
                        raise err
                    record(idx, err, True)
    except BaseException:
        failed = True
        raise
    finally:
        abandon = hung or failed
        ex.shutdown(wait=not abandon, cancel_futures=True)
        if abandon and is_process:
            _kill_workers(ex)
    if errors and not return_exceptions:
        raise errors[min(errors)]
    return [results[i] for i in range(submitted)]


def _run_serial(
    call,
    items,
    *,
    initializer,
    initargs,
    return_exceptions,
    on_progress,
):
    if initializer is not None:
        initializer(*initargs)
    out = []
    for idx, item in enumerate(items):
        try:
            value = call(item)
        except Exception as exc:
            if not return_exceptions:
                raise
            value = exc
        out.append(value)
        if on_progress is not None:
            on_progress(idx, value)
    return out


def _run_joblib(
    call,
    items,
    workers,
    return_exceptions,
    on_progress,
):
    try:
        from joblib import Parallel, delayed
    except ImportError as exc:
        msg = "the 'joblib' backend requires joblib — `pip install joblib`"
        raise ImportError(msg) from exc
    runner = _Catch(call) if return_exceptions else call
    out = list(Parallel(n_jobs=workers)(delayed(runner)(item) for item in items))
    if on_progress is not None:
        for idx, value in enumerate(out):
            on_progress(idx, value)
    return out


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
    ordered=False,
    retries=0,
    retry_delay=0.0,
    fail_fast=True,
):
    if not callable(func):
        msg = "`func` must be callable"
        raise TypeError(msg)
    if retries < 0:
        msg = "retries must be >= 0"
        raise ValueError(msg)
    if timeout is not None and timeout <= 0:
        msg = "timeout must be > 0"
        raise ValueError(msg)
    pool_method = PoolMethod.parse(method)
    call = _build_call(func, func_args, func_kwargs, unpack, retries, retry_delay)
    n = len(items) if isinstance(items, Sized) else None
    if n == 0:
        return []
    w = _resolve_workers(workers, n)
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
            call,
            items,
            workers=workers,
            chunksize=chunksize,
            initializer=initializer,
            initargs=initargs,
            maxtasksperchild=maxtasksperchild,
            context=context,
            return_exceptions=return_exceptions,
            on_progress=on_progress,
            ordered=ordered,
        )
    if pool_method is PoolMethod.SERIAL:
        if timeout is not None:
            msg = "the 'serial' backend does not support timeout"
            raise ValueError(msg)
        return _run_serial(
            call,
            items,
            initializer=initializer,
            initargs=initargs,
            return_exceptions=return_exceptions,
            on_progress=on_progress,
        )
    if pool_method is PoolMethod.JOBLIB:
        if initializer is not None or maxtasksperchild is not None:
            msg = "the 'joblib' backend does not support initializer/maxtasksperchild; use 'process_pool' instead"
            raise ValueError(msg)
        if timeout is not None:
            msg = "the 'joblib' backend does not support timeout"
            raise ValueError(msg)
        return _run_joblib(call, items, w, return_exceptions, on_progress)
    if pool_method is PoolMethod.PROCESS_POOL:
        executor_cls = ProcessPoolExecutor
    elif pool_method is PoolMethod.THREAD_POOL:
        if maxtasksperchild is not None:
            msg = "ThreadPoolExecutor does not support maxtasksperchild"
            raise ValueError(msg)
        executor_cls = ThreadPoolExecutor
    else:
        msg = f"unhandled method: {pool_method}"
        raise AssertionError(msg)
    return _run_executor(
        executor_cls,
        call,
        items,
        workers=w,
        chunksize=chunksize,
        n=n,
        initializer=initializer,
        initargs=initargs,
        maxtasksperchild=maxtasksperchild,
        context=context,
        timeout=timeout,
        return_exceptions=return_exceptions,
        max_pending=max_pending,
        on_progress=on_progress,
        fail_fast=fail_fast,
    )


def mpf_iter(
    func,
    items,
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
    ordered=True,
    return_exceptions=False,
    retries=0,
    retry_delay=0.0,
):
    if not callable(func):
        msg = "`func` must be callable"
        raise TypeError(msg)
    if retries < 0:
        msg = "retries must be >= 0"
        raise ValueError(msg)
    call = _build_call(func, func_args, func_kwargs, unpack, retries, retry_delay)
    runner = _Catch(call) if return_exceptions else call
    n = len(items) if isinstance(items, Sized) else None
    if n == 0:
        return
    w = _resolve_workers(workers, n)
    cs = _resolve_chunksize(chunksize, n, w) or 1
    ctx = mp.get_context(context)
    with ctx.Pool(**_pool_kwargs(w, initializer, initargs, maxtasksperchild)) as pool:
        stream = pool.imap(runner, items, cs) if ordered else pool.imap_unordered(runner, items, cs)
        yield from stream

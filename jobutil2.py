from __future__ import annotations
from collections import deque
from collections.abc import Callable
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
    wait,
)
from functools import partial
from multiprocessing import Pool, get_context
import os
import time
from typing import ParamSpec, TypeVar

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")
MAX_WORKERS = 8
DEFAULT_TIMEOUT = 30
get_context("spawn")
ExecutorFactory = Callable[..., Executor]


def _call_with_index(func, indexed_file):
    idx, file = indexed_file
    return idx, func(file)


def _split_result(value):
    """Workers return their result directly; keep legacy (result, error) support.

    Only an exception instance in the second slot marks an error, so ordinary
    tuple results such as ``(path, digest)`` are not swallowed as failures.
    """
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[1], BaseException):
        return value
    return value, None


def mpf_async(
    func,
    files,
    workers=MAX_WORKERS,
    timeout=DEFAULT_TIMEOUT,
    *args,
    **kwargs,
):
    files_list = list(files)
    if not files_list:
        return []
    with Pool(workers) as pool:
        async_results = [pool.apply_async(func, (file, *args), kwargs) for file in files_list]
        results = []
        for i, async_result in enumerate(async_results):
            try:
                results.append(async_result.get(timeout=timeout))
            except Exception as e:
                print(f"File {i} ({files_list[i]}) failed: {e}")
                results.append(None)
    return results


def mpf_map(
    func,
    files,
    workers=MAX_WORKERS,
    chunksize=1,
):
    files_list = list(files)
    if not files_list:
        return []
    with Pool(workers) as pool:
        try:
            return list(pool.map(func, files_list, chunksize=chunksize))
        except Exception as e:
            print(f"Pool.map failed: {e}")
            return [None] * len(files_list)


def mpf_imap(
    func,
    files,
    workers=MAX_WORKERS,
    chunksize=1,
    ordered=True,
):
    files_list = list(files)
    if not files_list:
        return []
    results = []
    with Pool(workers) as pool:
        imap_func = pool.imap if ordered else pool.imap_unordered
        for result in imap_func(func, files_list, chunksize=chunksize):
            results.append(result)
    return results


def mpf_imapu(
    func,
    files,
    workers=MAX_WORKERS,
    chunksize=1,
):
    files_list = list(files)
    if not files_list:
        return []
    results = [None] * len(files_list)
    bound = partial(_call_with_index, func)
    indexed = list(enumerate(files_list))
    with Pool(workers) as pool:
        for idx, result in pool.imap_unordered(bound, indexed, chunksize=chunksize):
            results[idx] = result
    return results


def mpf_starmap(
    func,
    files,
    extra_args=None,
    workers=MAX_WORKERS,
):
    files_list = list(files)
    if not files_list:
        return []
    if extra_args is None:
        extra_args = [() for _ in files_list]
    elif len(extra_args) != len(files_list):
        msg = "extra_args length must match files length"
        raise ValueError(msg)
    task_args = [(file, *args) for file, args in zip(files_list, extra_args)]
    with Pool(workers) as pool:
        try:
            return list(pool.starmap(func, task_args))
        except Exception as e:
            print(f"Pool.starmap failed: {e}")
            return [None] * len(files_list)


def mpf_ppe(
    func,
    files,
    workers=MAX_WORKERS,
    timeout=DEFAULT_TIMEOUT,
    use_as_completed=False,
):
    files_list = list(files)
    if not files_list:
        return []
    results = [None] * len(files_list)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(func, file): i for i, file in enumerate(files_list)}
        if use_as_completed:
            from concurrent.futures import as_completed

            iterator = as_completed(futures, timeout=timeout)
        else:
            iterator = iter(futures)
        for future in iterator:
            idx = futures[future]
            try:
                results[idx] = future.result(timeout=timeout)
            except Exception as e:
                print(f"File {idx} ({files_list[idx]}) failed: {e}")
                results[idx] = None
    return results


def mpf_tpe(
    func,
    files,
    workers=MAX_WORKERS,
    timeout=DEFAULT_TIMEOUT,
):
    files_list = list(files)
    if not files_list:
        return []
    results = [None] * len(files_list)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(func, file): i for i, file in enumerate(files_list)}
        for future in futures:
            idx = futures[future]
            try:
                results[idx] = future.result(timeout=timeout)
            except Exception as e:
                print(f"File {idx} ({files_list[idx]}) failed: {e}")
                results[idx] = None
    return results


def mpf(
    worker_fn,
    items,
    num_jobs=1,
    *worker_args,
    **worker_kwargs,
):
    if not items:
        return []
    with Pool(num_jobs) as pool:
        async_results = [pool.apply_async(worker_fn, args=(item, *worker_args), kwds=worker_kwargs) for item in items]
        return [result.get() for result in async_results]


def mpf2(
    worker_fn,
    items,
    num_jobs=1,
    *worker_args,
    **worker_kwargs,
):
    return mpf(worker_fn, items, num_jobs, *worker_args, **worker_kwargs)


def mpf_joblib(
    process_function,
    files,
    n_jobs=-1,
    **kwargs,
):
    from joblib import Parallel, delayed

    file_strings = [str(f) for f in files]
    return Parallel(n_jobs=n_jobs)(delayed(process_function)(file_str, **kwargs) for file_str in file_strings)


def mpf_ppe_ac(
    func,
    files,
    workers=MAX_WORKERS,
    timeout=30,
):
    from concurrent.futures import as_completed

    files_list = list(files)
    if not files_list:
        return []
    results = [None] * len(files_list)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(func, file): i for i, file in enumerate(files_list)}
        for future in as_completed(futures, timeout=timeout):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                print(f"File {idx} ({files_list[idx]}) failed: {e}")
                results[idx] = None
    return results


def mpf3(
    func,
    items,
    *,
    max_workers=MAX_WORKERS,
    max_pending_factor=8,
    timeout=DEFAULT_TIMEOUT,
    on_error=None,
    on_timeout=None,
    executor_type=ProcessPoolExecutor,
    executor_kwargs=None,
):
    items_list = list(items)
    if not items_list:
        return []
    if executor_kwargs is None:
        executor_kwargs = {}
    max_pending = max(max_pending_factor * max_workers, max_workers)
    results = [None] * len(items_list)
    item_iterator = iter(enumerate(items_list))
    pending = {}
    with executor_type(max_workers=max_workers, **executor_kwargs) as executor:
        exhausted = False
        while len(pending) < max_pending and not exhausted:
            try:
                idx, item = next(item_iterator)
                future = executor.submit(func, item)
                pending[future] = (idx, item, time.time())
            except StopIteration:
                exhausted = True
        while pending:
            done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                idx, item, start_time = pending.pop(future)
                try:
                    if timeout is not None:
                        elapsed = time.time() - start_time
                        remaining_timeout = max(0.0, timeout - elapsed)
                        if remaining_timeout == 0:
                            msg = f"Task timed out after {timeout}s"
                            raise FutureTimeoutError(msg)
                        result, error = _split_result(future.result(timeout=remaining_timeout))
                    else:
                        result, error = _split_result(future.result())
                    if error is not None:
                        if on_error:
                            on_error(item, error)
                    else:
                        results[idx] = result
                except FutureTimeoutError:
                    future.cancel()
                    if on_timeout:
                        on_timeout(item)
                    elif on_error:
                        on_error(item, f"Timeout after {timeout}s")
                except Exception as exc:
                    error_msg = f"{type(exc).__name__}: {exc}"
                    if on_error:
                        on_error(item, error_msg)
            if not exhausted:
                while len(pending) < max_pending:
                    try:
                        idx, item = next(item_iterator)
                        future = executor.submit(func, item)
                        pending[future] = (idx, item, time.time())
                    except StopIteration:
                        exhausted = True
                        break
    return results


def mpf_as(func, files):
    results = []
    with Pool(8) as pool:
        pending = deque()
        for f in files:
            pending.append(pool.apply_async(func, (f,)))
            if len(pending) > 32:
                results.append(pending.popleft().get())
        while pending:
            results.append(pending.popleft().get())
    return results


def mpf_bounded(
    func,
    files,
    *,
    processes=None,
    max_inflight=None,
):
    processes = processes or os.cpu_count() or 1
    max_inflight = max_inflight or 2 * processes
    results = []
    with Pool(processes) as pool:
        pending = deque()
        for f in files:
            if len(pending) >= max_inflight:
                results.append(pending.popleft().get())
            pending.append(pool.apply_async(func, (f,)))
        while pending:
            results.append(pending.popleft().get())
    return results


def mpf_stream(
    func,
    files,
    *,
    processes=None,
    chunksize=1,
):
    processes = processes or os.cpu_count() or 1
    with Pool(processes) as pool:
        yield from pool.imap(func, files, chunksize=chunksize)


def mpf_imap2(
    func,
    files,
    *,
    processes=None,
    chunksize=1,
):
    processes = processes or os.cpu_count() or 1
    with Pool(processes) as pool:
        return list(pool.imap(func, files, chunksize=chunksize))

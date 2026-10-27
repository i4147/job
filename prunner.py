from __future__ import annotations
import argparse
import functools
import hashlib
import itertools
import multiprocessing as mp
import operator
import os
import sys
import time
import warnings
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass
from multiprocessing.pool import AsyncResult
from typing import Any, Literal

# InterpreterPoolExecutor is only available on CPython 3.14+.  Keep the import
# optional so this module also imports (and benchmarks) on older interpreters.
# PEP 695 ``type X = ...`` aliases are deliberately avoided: they need 3.12+.
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
Backend = Literal[
    "auto",
    "thread",
    "process",
    "interpreter",
    "mp_imap",
    "mp_imap_unordered",
    "mp_apply_async",
]
ErrorPolicy = Literal["raise", "skip", "collect"]
__all__ = ["ParallelRunner", "Stats", "pmap", "Backend", "ErrorPolicy"]
_BACKENDS = (
    "auto",
    "thread",
    "process",
    "interpreter",
    "mp_imap",
    "mp_imap_unordered",
    "mp_apply_async",
)
_ERROR_POLICIES = ("raise", "skip", "collect")
@dataclass(slots=True)
class Stats:
    submitted: int = 0
    completed: int = 0
    errors: int = 0
    skipped: int = 0
    elapsed: float = 0.0
def _gil_disabled() -> bool:
    probe = getattr(sys, "_is_gil_enabled", None)
    if probe is None:
        return False
    try:
        return not probe()
    except Exception:
        return False
def _cpu_count() -> int:
    # os.process_cpu_count() is 3.13+ and may return None.
    probe = getattr(os, "process_cpu_count", None)
    try:
        count = probe() if probe is not None else None
    except Exception:
        count = None
    return max(1, count or os.cpu_count() or 1)
def _auto_chunksize(n_items: int, workers: int) -> int:
    if n_items <= 0:
        return 1
    chunks, remainder = divmod(n_items, workers * 4)
    return max(1, chunks + bool(remainder))
def _batched(items: Iterator[Any], size: int) -> Iterator[list[Any]]:
    while True:
        batch = list(itertools.islice(items, size))
        if not batch:
            return
        yield batch
def _batch_call(
    payload: tuple[Callable[[Any], Any], list[Any], str],
) -> tuple[list[Any], list[Exception], int, int]:
    func, items, policy = payload
    if policy == "raise":
        return [func(item) for item in items], [], 0, 0
    successes: list[Any] = []
    collected: list[Exception] = []
    errors = skipped = 0
    for item in items:
        try:
            successes.append(func(item))
        except Exception as exc:
            errors += 1
            if policy == "skip":
                skipped += 1
            else:
                collected.append(exc)
    return successes, collected, errors, skipped
def _star_call(payload: tuple[Callable[..., Any], tuple[Any, ...]]) -> Any:
    func, args = payload
    return func(*args)
class ParallelRunner:
    def __init__(
        self,
        backend: Backend = "auto",
        *,
        max_workers: int | None = None,
        chunksize: int | None = None,
        window: int | None = None,
        cpu_bound: bool = True,
        mp_start_method: str | None = None,
        initializer: Callable[..., Any] | None = None,
        initargs: tuple[Any, ...] = (),
        maxtasksperchild: int | None = None,
        on_error: ErrorPolicy = "raise",
        progress: Callable[[int, int | None], Any] | None = None,
    ) -> None:
        if backend not in _BACKENDS:
            raise ValueError(f"backend must be one of {_BACKENDS}, got {backend!r}")
        if on_error not in _ERROR_POLICIES:
            raise ValueError(f"on_error must be one of {_ERROR_POLICIES}, got {on_error!r}")
        self.backend: Backend = self._resolve_backend(backend, cpu_bound)
        self.max_workers = max(1, max_workers or _cpu_count())
        self.chunksize = chunksize
        self.window = max(2, window or max(8, self.max_workers * 4))
        self.mp_start_method = mp_start_method
        self.initializer = initializer
        self.initargs = initargs
        self.maxtasksperchild = maxtasksperchild
        self.on_error: ErrorPolicy = on_error
        self.progress = progress
        self.stats = Stats()
        self._persistent = False
        self._pool: mp.Pool | None = None
        self._executor: Executor | None = None
        self._total: int | None = None
    def _resolve_backend(self, backend: Backend, cpu_bound: bool) -> Backend:
        if backend != "auto":
            if backend == "interpreter" and not _HAS_INTERPRETER_POOL:
                warnings.warn(
                    "InterpreterPoolExecutor requires CPython 3.14+; falling back to 'process'",
                    RuntimeWarning,
                    stacklevel=3,
                )
                return "process"
            return backend
        if _gil_disabled():
            return "thread"
        return "process" if cpu_bound else "thread"
    def __enter__(self) -> ParallelRunner:
        self._persistent = True
        return self
    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
    def close(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None
        if self._pool is not None:
            self._pool.terminate()
            self._pool.join()
            self._pool = None
    def _get_executor(self) -> Executor:
        if self._executor is not None:
            return self._executor
        if self.backend == "thread":
            self._executor = ThreadPoolExecutor(
                max_workers=self.max_workers,
                initializer=self.initializer,
                initargs=self.initargs,
                thread_name_prefix="parallel",
            )
        elif self.backend == "process":
            kwargs: dict[str, Any] = {
                "max_workers": self.max_workers,
                "initializer": self.initializer,
                "initargs": self.initargs,
            }
            if self.mp_start_method is not None:
                kwargs["mp_context"] = mp.get_context(self.mp_start_method)
            self._executor = ProcessPoolExecutor(**kwargs)
        else:
            if not _HAS_INTERPRETER_POOL or InterpreterPoolExecutor is None:
                msg = "InterpreterPoolExecutor requires CPython 3.14+"
                raise ImportError(msg)
            self._executor = InterpreterPoolExecutor(
                max_workers=self.max_workers,
                initializer=self.initializer,
                initargs=self.initargs,
            )
        return self._executor
    def _get_pool(self) -> mp.Pool:
        if self._pool is not None:
            return self._pool
        context = mp.get_context(self.mp_start_method) if self.mp_start_method else mp.get_context()
        self._pool = context.Pool(
            processes=self.max_workers,
            initializer=self.initializer,
            initargs=self.initargs,
            maxtasksperchild=self.maxtasksperchild,
        )
        return self._pool
    def map(
        self,
        func: Callable[..., Any],
        iterable: Iterable[Any],
        *,
        chunksize: int | None = None,
        unordered: bool = False,
        star: bool = False,
    ) -> Iterator[Any]:
        size = chunksize if chunksize is not None else self.chunksize
        if size is None:
            size = _auto_chunksize(operator.length_hint(iterable, 0), self.max_workers)
        size = max(1, int(size))
        self._total = operator.length_hint(iterable, 0) or None
        self.stats = Stats()
        started = time.perf_counter()
        try:
            if self.backend in ("mp_imap", "mp_imap_unordered"):
                stream = self._imap(func, iterable, size, unordered or self.backend == "mp_imap_unordered", star)
            elif self.backend == "mp_apply_async":
                if unordered:
                    stream = self._imap(func, iterable, size, True, star)
                else:
                    stream = self._apply_async(func, iterable, star)
            else:
                stream = self._futures(func, iterable, size, unordered, star)
            yield from stream
        finally:
            self.stats.elapsed = time.perf_counter() - started
            if not self._persistent:
                self.close()
    def _futures(
        self, func: Callable[..., Any], iterable: Iterable[Any], size: int, unordered: bool, star: bool
    ) -> Iterator[Any]:
        executor = self._get_executor()
        submit = executor.submit
        source: Iterator[Any] = iter(iterable)
        batched = size > 1 and not star
        units: Iterator[Any] = _batched(source, size) if batched else source
        def submit_unit(unit: Any) -> Future:
            if batched:
                return submit(_batch_call, (func, unit, self.on_error))
            if star:
                return submit(func, *unit)
            return submit(func, unit)
        exhausted = False
        if unordered:
            in_flight: set[Future] = set()
            while in_flight or not exhausted:
                while not exhausted and len(in_flight) < self.window:
                    try:
                        unit = next(units)
                    except StopIteration:
                        exhausted = True
                    else:
                        in_flight.add(submit_unit(unit))
                        self.stats.submitted += len(unit) if batched else 1
                if not in_flight:
                    break
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    yield from self._collect(future, batched)
        else:
            ordered: deque[Future] = deque()
            while True:
                while not exhausted and len(ordered) < self.window:
                    try:
                        unit = next(units)
                    except StopIteration:
                        exhausted = True
                    else:
                        ordered.append(submit_unit(unit))
                        self.stats.submitted += len(unit) if batched else 1
                if not ordered:
                    break
                yield from self._collect(ordered.popleft(), batched)
    def _collect(self, future: Future, batched: bool) -> Iterator[Any]:
        try:
            result = future.result()
        except Exception as exc:
            self.stats.errors += 1
            if self.on_error == "raise":
                raise
            if self.on_error == "skip":
                self.stats.skipped += 1
                return
            yield exc
            return
        if batched:
            successes, collected, n_errors, n_skipped = result
            self.stats.errors += n_errors
            self.stats.skipped += n_skipped
            for value in successes:
                self.stats.completed += 1
                if self.progress is not None:
                    self.progress(self.stats.completed, self._total)
                yield value
            yield from collected
        else:
            self.stats.completed += 1
            if self.progress is not None:
                self.progress(self.stats.completed, self._total)
            yield result
    def _imap(
        self, func: Callable[..., Any], iterable: Iterable[Any], size: int, unordered: bool, star: bool
    ) -> Iterator[Any]:
        pool = self._get_pool()
        if star:
            call, source = _star_call, ((func, args) for args in iterable)
        else:
            call, source = func, iterable
        results = pool.imap_unordered(call, source, size) if unordered else pool.imap(call, source, size)
        while True:
            try:
                value = next(results)
            except StopIteration:
                return
            except Exception as exc:
                self.stats.errors += 1
                if self.on_error == "raise":
                    raise
                if self.on_error == "skip":
                    self.stats.skipped += 1
                    continue
                yield exc
                continue
            self.stats.completed += 1
            if self.progress is not None:
                self.progress(self.stats.completed, self._total)
            yield value
    def _apply_async(self, func: Callable[..., Any], iterable: Iterable[Any], star: bool) -> Iterator[Any]:
        pool = self._get_pool()
        source = iter(iterable)
        exhausted = False
        pending: deque[AsyncResult] = deque()
        while True:
            while not exhausted and len(pending) < self.window:
                try:
                    item = next(source)
                except StopIteration:
                    exhausted = True
                else:
                    pending.append(pool.apply_async(func, item if star else (item,)))
                    self.stats.submitted += 1
            if not pending:
                return
            async_result = pending.popleft()
            try:
                value = async_result.get()
            except Exception as exc:
                self.stats.errors += 1
                if self.on_error == "raise":
                    raise
                if self.on_error == "skip":
                    self.stats.skipped += 1
                    continue
                yield exc
                continue
            self.stats.completed += 1
            if self.progress is not None:
                self.progress(self.stats.completed, self._total)
            yield value
def pmap(
    func: Callable[..., Any],
    iterable: Iterable[Any],
    *,
    backend: Backend = "auto",
    unordered: bool = False,
    star: bool = False,
    chunksize: int | None = None,
    **runner_kwargs: Any,
) -> Iterator[Any]:
    runner = ParallelRunner(backend, **runner_kwargs)
    try:
        yield from runner.map(func, iterable, chunksize=chunksize, unordered=unordered, star=star)
    finally:
        runner.close()
def _hash_file(path: str) -> tuple[str, int, str]:
    digest = hashlib.blake2b()
    size = 0
    with open(path, "rb") as handle:
        for block in iter(functools.partial(handle.read, 1 << 20), b""):
            digest.update(block)
            size += len(block)
    return path, size, digest.hexdigest()
def _collect_files(root: str) -> list[str]:
    files: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
        for name in filenames:
            path = os.path.join(dirpath, name)
            if not name.startswith(".") and os.path.isfile(path) and not os.path.islink(path):
                files.append(path)
    return files
def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="parallel_runner")
    parser.add_argument("root", nargs="?", default=".")
    parser.add_argument(
        "-b",
        "--backends",
        default="auto",
        help="comma separated or 'all': thread,process,interpreter,mp_imap,mp_imap_unordered,mp_apply_async",
    )
    parser.add_argument("-w", "--workers", type=int, default=None)
    parser.add_argument("-c", "--chunksize", type=int, default=None)
    parser.add_argument("-e", "--on-error", choices=_ERROR_POLICIES, default="skip")
    args = parser.parse_args(argv)
    files = _collect_files(args.root)
    if not files:
        print(f"no readable files under {args.root!r}", file=sys.stderr)
        return 1
    if args.backends.strip() == "all":
        backends = list(_BACKENDS[1:])
    else:
        backends = [b.strip() for b in args.backends.split(",") if b.strip()]
    for backend in backends:
        if backend == "interpreter" and not _HAS_INTERPRETER_POOL:
            print(f"{backend:>18}: skipped (requires CPython 3.14)")
            continue
        try:
            runner = ParallelRunner(
                backend,
                max_workers=args.workers,
                chunksize=args.chunksize,
                on_error=args.on_error,
            )
        except ValueError as exc:
            print(f"{backend:>18}: {exc}")
            continue
        started = time.perf_counter()
        count = hashed_bytes = 0
        try:
            for _path, size, _digest in runner.map(_hash_file, files, unordered=True):
                count += 1
                hashed_bytes += size
        except Exception as exc:
            print(f"{backend:>18}: failed ({exc!r})")
            runner.close()
            continue
        elapsed = time.perf_counter() - started
        rate = (hashed_bytes / 1e6) / elapsed if elapsed > 0 else 0.0
        stats = runner.stats
        print(
            f"{backend:>18}: {count:>6} files, {hashed_bytes / 1e6:>9.1f} MB, "
            f"{elapsed:>7.3f}s, {rate:>8.1f} MB/s, "
            f"errors={stats.errors}, skipped={stats.skipped}, workers={runner.max_workers}"
        )
    return 0
if __name__ == "__main__":
    raise SystemExit(main())

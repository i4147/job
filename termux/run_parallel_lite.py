#!/usr/bin/env python3
"""Memory-friendly ``run_parallel`` for Termux / Android (stdlib only).

This is intentionally a *separate, standalone file*: copy just this file to a
phone and it works with no package, no joblib and no pytest.  Target runtime is
CPython 3.14 / Linux, but it also runs on 3.9+.

Why a lite version?  Termux typically has 1-4 GB RAM, no swap, and Android
restricts ``fork``/POSIX semaphores.  The main ``run_parallel`` package creates
process pools, joblib/loky executors and result lists; this one:

* defaults to a **2-thread** pool (no forked interpreters, no pickling);
* never materialises the input list: file discovery is a generator;
* keeps at most ``max_pending`` (default ``2 * workers``) futures alive;
* hashes files in fixed-size blocks (~1 MiB) instead of ``read()``;
* yields results as they complete and drops each future immediately;
* optionally recycles process workers (``max_tasks_per_child``) and calls
  ``gc.collect()`` every N items;
* reports peak RSS via ``resource``/``/proc`` for easy memory budgeting.

Usage
-----
    from termux.run_parallel_lite import LiteParallel, hash_folder

    for outcome in hash_folder("/sdcard/Download"):
        print(outcome.value)          # (path, sha256, size)

CLI
---
    python3 termux/run_parallel_lite.py /sdcard/Download --workers 2 --gc-every 50
"""
from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from functools import partial
import gc
import hashlib
import os
from pathlib import Path
import sys
import time
from typing import Any

__all__ = [
    "LiteParallel",
    "LiteResult",
    "Outcome",
    "iter_files",
    "hash_file",
    "iter_file_digests",
    "hash_folder",
    "peak_rss_kb",
    "main",
]

DEFAULT_WORKERS = min(2, os.cpu_count() or 1)
DEFAULT_BLOCK_SIZE = 1 << 20  # 1 MiB
SKIP_DIRS = frozenset(
    {".git", ".cache", ".gradle", ".npm", "node_modules", "__pycache__",
     "Android", "DCIM", "lost.dir"}
)
ERROR_POLICIES = ("raise", "collect", "skip")
_SKIP = object()  # failed item in skip mode: advance order but emit nothing


# ---------------------------------------------------------------------------
# top-level helpers (picklable for the optional process backend)
# ---------------------------------------------------------------------------
def _safe_call(func: Callable[[Any], Any], item: Any) -> tuple[Any, str | None]:
    """Return ``(value, None)`` or ``(None, 'TypeName: message')``.

    A string is used instead of an exception object so no traceback frames are
    retained (they can pin large buffers in memory) or pickled.
    """
    try:
        return func(item), None
    except Exception as exc:  # noqa: BLE001 - reported to the caller
        return None, f"{type(exc).__name__}: {exc}"


def hash_file(path: str, algorithm: str = "sha256",
              block_size: int = DEFAULT_BLOCK_SIZE) -> tuple[str, str, int]:
    """Hash one file in fixed-size blocks; peak memory ~= ``block_size``."""
    digest = hashlib.new(algorithm)
    size = 0
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(block_size), b""):
            digest.update(block)
            size += len(block)
    return path, digest.hexdigest(), size


def peak_rss_kb() -> int | None:
    """Peak resident set size in KiB (Linux), else ``None``."""
    try:
        import resource

        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except (ImportError, OSError, ValueError):  # pragma: no cover
        pass
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:  # pragma: no cover
        pass
    return None


def iter_files(
    root: str | os.PathLike[str] | None = None,
    *,
    max_file_bytes: int = 8 * 1024 * 1024,
    include_hidden: bool = False,
    skip_dirs: frozenset[str] = SKIP_DIRS,
    limit: int | None = None,
) -> Iterator[str]:
    """Stream file paths (depth-first, sorted) without building a full list."""
    base = Path(root).expanduser() if root is not None else Path.home()
    stack: list[Path] = [base]
    produced = 0
    while stack:
        directory = stack.pop()
        try:
            with os.scandir(directory) as scan:
                entries = sorted(scan, key=lambda entry: entry.name, reverse=True)
        except (OSError, ValueError):
            continue
        for entry in entries:
            if not include_hidden and entry.name.startswith("."):
                continue
            try:
                if entry.is_dir(follow_symlinks=False):
                    if entry.name not in skip_dirs:
                        stack.append(Path(entry.path))
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
                if entry.stat(follow_symlinks=False).st_size > max_file_bytes:
                    continue
                with open(entry.path, "rb") as handle:
                    handle.read(1)
            except OSError:
                continue
            yield entry.path
            produced += 1
            if limit is not None and produced >= limit:
                return


def iter_file_digests(
    root: str | os.PathLike[str] | None = None,
    *,
    algorithm: str = "sha256",
    block_size: int = DEFAULT_BLOCK_SIZE,
    **walker_kwargs: Any,
) -> Iterator[tuple[str, str, int]]:
    """Serial streaming hash of a folder (no parallelism, minimal memory)."""
    for path in iter_files(root, **walker_kwargs):
        try:
            yield hash_file(path, algorithm, block_size)
        except OSError:
            continue


# ---------------------------------------------------------------------------
# result objects
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Outcome:
    item: Any
    value: Any
    error: BaseException | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass(slots=True)
class LiteResult:
    values: list[Any] = field(default_factory=list)
    errors: list[Outcome] = field(default_factory=list)
    elapsed: float = 0.0
    submitted: int = 0
    peak_rss_kb: int | None = None

    @property
    def ok(self) -> bool:
        return not self.errors

    def unwrap(self) -> list[Any]:
        if self.errors:
            raise self.errors[0].error  # type: ignore[misc]
        return self.values


# ---------------------------------------------------------------------------
# inline executor (lowest-memory debugging backend)
# ---------------------------------------------------------------------------
class _InlineExecutor:
    def __init__(self) -> None:
        self._shutdown = False

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Future:
        future: Future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except BaseException as exc:  # pragma: no cover - defensive
            future.set_exception(exc)
        return future

    def shutdown(self, wait: bool = True) -> None:  # noqa: A002 - API compatibility
        self._shutdown = True

    def __enter__(self) -> "_InlineExecutor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.shutdown()


# ---------------------------------------------------------------------------
# the class
# ---------------------------------------------------------------------------
class LiteParallel:
    """Small, streaming, bounded-memory parallel map.

    Parameters
    ----------
    workers:
        Thread/process count. Default ``min(2, os.cpu_count())``.
    max_pending:
        Upper bound on in-flight futures; default ``2 * workers``. This is the
        main memory knob.
    backend:
        ``"thread"`` (default, recommended on Termux), ``"process"`` or
        ``"serial"`` (debug, zero extra memory).
    on_error:
        ``"collect"`` (default; yield the error), ``"skip"`` (drop the item) or
        ``"raise"`` (fail fast).
    gc_every:
        Call ``gc.collect()`` every N completed items (0 disables).
    max_tasks_per_child:
        Process backend only: recycle a worker after N tasks to release RSS.
    timeout:
        Seconds to wait for *any* future to complete; ``None`` waits forever.
    """

    def __init__(
        self,
        workers: int | None = None,
        *,
        max_pending: int | None = None,
        backend: str = "thread",
        on_error: str = "collect",
        gc_every: int = 0,
        max_tasks_per_child: int | None = None,
        timeout: float | None = None,
        ordered: bool = True,
        max_reorder: int | None = None,
    ) -> None:
        if backend not in ("thread", "process", "serial"):
            raise ValueError("backend must be thread/process/serial")
        if on_error not in ERROR_POLICIES:
            raise ValueError(f"on_error must be one of {ERROR_POLICIES}")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be > 0")
        self.workers = max(1, int(workers or DEFAULT_WORKERS))
        self.max_pending = max(1, int(max_pending or 2 * self.workers))
        self.backend = backend
        self.on_error = on_error
        self.gc_every = max(0, int(gc_every))
        self.max_tasks_per_child = max_tasks_per_child
        self.timeout = timeout
        self.ordered = bool(ordered)
        # Bounded reorder buffer: never hold more than this many out-of-order
        # results, even if one task is very slow.
        self.max_reorder = max(1, int(max_reorder or 2 * self.max_pending))

    def _executor(self) -> Any:
        if self.backend == "serial":
            return _InlineExecutor()
        if self.backend == "thread":
            return ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="lite")
        kwargs: dict[str, Any] = {"max_workers": self.workers}
        # CPython 3.11 can deadlock when all workers are recycled at once;
        # 3.12+ (and the 3.14 target) supports this safely.
        if self.max_tasks_per_child is not None and sys.version_info >= (3, 12):
            kwargs["max_tasks_per_child"] = self.max_tasks_per_child
        return ProcessPoolExecutor(**kwargs)

    def imap(self, func: Callable[[Any], Any], items: Iterable[Any]) -> Iterator[Outcome]:
        """Yield :class:`Outcome` objects with a bounded in-flight window.

        ``ordered=True`` keeps input order using a **bounded** reorder buffer;
        if one task is very slow the oldest buffered result is emitted out of
        order once the buffer exceeds ``max_reorder`` (default
        ``2 * max_pending``), so memory can never grow without limit.
        """
        source = iter(items)
        pending: dict[Future, tuple[int, Any]] = {}
        exhausted = False
        completed = 0
        next_index = 0
        expected = 0
        buffer: dict[int, Any] = {}
        with self._executor() as executor:
            while True:
                while not exhausted and len(pending) < self.max_pending:
                    try:
                        item = next(source)
                    except StopIteration:
                        exhausted = True
                        break
                    index = next_index
                    next_index += 1
                    pending[executor.submit(_safe_call, func, item)] = (index, item)
                if not pending:
                    break
                done, _ = wait(tuple(pending), timeout=self.timeout,
                               return_when=FIRST_COMPLETED)
                if not done:
                    for future in pending:
                        future.cancel()
                    exc: BaseException = TimeoutError(
                        f"no task finished within {self.timeout}s"
                    )
                    if self.on_error == "raise":
                        raise exc
                    if self.on_error != "skip":
                        for _index, item in sorted(pending.values()):
                            yield Outcome(item, None, exc)
                    return
                for future in done:
                    index, item = pending.pop(future)
                    value, error_text = future.result()
                    completed += 1
                    if self.gc_every and completed % self.gc_every == 0:
                        gc.collect()
                    error: BaseException | None = (
                        RuntimeError(error_text) if error_text else None
                    )
                    if error is not None:
                        if self.on_error == "raise":
                            raise error
                        if self.on_error == "skip":
                            outcome: Any = _SKIP
                        else:
                            outcome = Outcome(item, value, error)
                    else:
                        outcome = Outcome(item, value, None)
                    if not self.ordered:
                        if outcome is not _SKIP:
                            yield outcome
                        continue
                    buffer[index] = outcome
                    while expected in buffer:
                        buffered = buffer.pop(expected)
                        expected += 1
                        if buffered is not _SKIP:
                            yield buffered
                    while len(buffer) > self.max_reorder:
                        oldest = min(buffer)
                        buffered = buffer.pop(oldest)
                        expected = max(expected, oldest + 1)
                        if buffered is not _SKIP:
                            yield buffered
            if self.ordered and buffer:
                for index in sorted(buffer):
                    buffered = buffer[index]
                    if buffered is not _SKIP:
                        yield buffered

    def map(self, func: Callable[[Any], Any], items: Iterable[Any]) -> Iterator[Any]:
        """Yield values only (errors are skipped unless ``on_error='raise'``)."""
        for outcome in self.imap(func, items):
            if outcome.error is None:
                yield outcome.value

    def run(self, func: Callable[[Any], Any], items: Iterable[Any]) -> LiteResult:
        """Collect results. Prefer :meth:`imap` on a memory-constrained device."""
        started = time.perf_counter()
        result = LiteResult()
        for outcome in self.imap(func, items):
            result.submitted += 1
            if outcome.error is not None:
                result.errors.append(outcome)
            else:
                result.values.append(outcome.value)
        result.elapsed = time.perf_counter() - started
        result.peak_rss_kb = peak_rss_kb()
        return result

    def __call__(self, func: Callable[[Any], Any], items: Iterable[Any]) -> LiteResult:
        return self.run(func, items)

    def __enter__(self) -> "LiteParallel":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


def hash_folder(
    root: str | os.PathLike[str] | None = None,
    *,
    workers: int | None = None,
    max_pending: int | None = None,
    block_size: int = DEFAULT_BLOCK_SIZE,
    algorithm: str = "sha256",
    on_error: str = "collect",
    gc_every: int = 0,
    limit: int | None = None,
    **walker_kwargs: Any,
) -> Iterator[Outcome]:
    """Streaming parallel hash of a folder (default: home folder).

    File discovery, hashing and result consumption are all generators, so peak
    memory is roughly ``workers * block_size`` plus the bounded future window.
    """
    worker = partial(hash_file, algorithm=algorithm, block_size=block_size)
    paths = iter_files(root, limit=limit, **walker_kwargs)
    runner = LiteParallel(
        workers=workers,
        max_pending=max_pending,
        on_error=on_error,
        gc_every=gc_every,
    )
    yield from runner.imap(worker, paths)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="run_parallel_lite",
        description="Streaming, low-memory parallel file hasher for Termux.",
    )
    parser.add_argument("folder", nargs="?", default=None,
                        help="folder to hash (default: home folder)")
    parser.add_argument("-w", "--workers", type=int, default=None)
    parser.add_argument("-p", "--max-pending", type=int, default=None)
    parser.add_argument("-b", "--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    parser.add_argument("-a", "--algorithm", default="sha256")
    parser.add_argument("--gc-every", type=int, default=0)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--on-error", choices=ERROR_POLICIES, default="collect")
    args = parser.parse_args(argv)

    started = time.perf_counter()
    count = total = errors = 0
    for outcome in hash_folder(
        args.folder,
        workers=args.workers,
        max_pending=args.max_pending,
        block_size=args.block_size,
        algorithm=args.algorithm,
        on_error=args.on_error,
        gc_every=args.gc_every,
        limit=args.limit,
    ):
        if outcome.error is not None:
            errors += 1
            print(f"ERROR  {outcome.item}: {outcome.error}", file=sys.stderr)
            continue
        path, digest, size = outcome.value
        count += 1
        total += size
        print(f"{digest}  {size:>10}  {path}")
    elapsed = time.perf_counter() - started
    rss = peak_rss_kb()
    print(
        f"\n{count} files, {total / 1e6:.2f} MB, {errors} errors, "
        f"{elapsed:.3f}s, {total / 1e6 / elapsed if elapsed else 0:.1f} MB/s, "
        f"peak RSS {rss / 1024 if rss else 0:.1f} MiB",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

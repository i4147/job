#!/usr/bin/env python3
"""The benchmark's hashing workload, run through all three classes + lite.

    python3 examples/hash_folder_example.py /path/to/folder
    python3 examples/hash_folder_example.py            # uses $HOME

Creates no files: it hashes whatever folder you point it at (bounded by the
same safety limits as ``benchmark/workers.py``).
"""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_parallel import (  # noqa: E402
    JOBLIB_AVAILABLE,
    ExecutorRunner,
    JoblibRunner,
    MpPoolRunner,
    family_stats,
    recommend,
)

BLOCK = 1 << 20


def hash_file(path: str) -> tuple[str, str]:
    """The exact workload used by the repo benchmark (sha256 of file bytes)."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(BLOCK), b""):
            digest.update(block)
    return path, digest.hexdigest()


def collect(folder: str, limit: int = 200) -> list[str]:
    root = Path(folder).expanduser()
    skip = {".git", ".cache", ".local", "node_modules", "__pycache__", "site-packages"}
    found: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if not d.startswith(".") and d not in skip)
        for name in sorted(filenames):
            if name.startswith("."):
                continue
            path = Path(dirpath) / name
            try:
                if path.is_file() and not path.is_symlink() and path.stat().st_size <= 8 << 20:
                    found.append(str(path))
            except OSError:
                continue
            if len(found) >= limit:
                return found
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", nargs="?", default=str(Path.home()))
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()

    files = collect(args.folder, args.limit)
    if not files:
        print("no files found", file=sys.stderr)
        return 1
    total = sum(Path(p).stat().st_size for p in files)
    expected = {hash_file(path) for path in files}
    print(f"folder={args.folder}  files={len(files)}  bytes={total / 1e6:.2f} MB")
    print(f"workers={args.workers}  python={sys.version.split()[0]}\n")

    stats = family_stats()
    if stats:
        print("Benchmark medians (repo benchmark/results/benchmark_results.json):")
        for name, entry in sorted(stats.items(), key=lambda item: item[1].median_seconds):
            print(f"  {name:<48} {entry.median_seconds:.4f}s")
        print()

    runners: list[tuple[str, object]] = [
        ("MpPoolRunner / imap_unordered(ordered=True)",
         MpPoolRunner(hash_file, method="imap_unordered", workers=args.workers,
                      start_method="fork", ordered=True)),
        ("ExecutorRunner / ThreadPoolExecutor.map",
         ExecutorRunner(hash_file, kind="thread", mode="map", workers=args.workers)),
        ("ExecutorRunner / ProcessPoolExecutor.map",
         ExecutorRunner(hash_file, kind="process", mode="map", workers=args.workers)),
    ]
    if JOBLIB_AVAILABLE:
        runners.append(("JoblibRunner / loky",
                        JoblibRunner(hash_file, backend="loky", n_jobs=args.workers,
                                     mmap_mode="r", max_nbytes="1M")))
    for label, runner in runners:
        started = time.perf_counter()
        result = runner.run(files)  # type: ignore[attr-defined]
        elapsed = time.perf_counter() - started
        ok = set(result.unwrap()) == expected
        print(f"  {label:<48} {elapsed:7.4f}s  "
              f"{total / 1e6 / elapsed:8.1f} MB/s  correct={ok}")

    print("\nLite (Termux) streaming version:")
    from termux.run_parallel_lite import LiteParallel, hash_file as lite_hash

    started = time.perf_counter()
    seen = set()
    for outcome in LiteParallel(workers=args.workers).imap(lite_hash, files):
        path, digest, _size = outcome.value
        seen.add((path, digest))
    elapsed = time.perf_counter() - started
    print(f"  {'LiteParallel.imap (2-thread, bounded window)':<48} {elapsed:7.4f}s  "
          f"{total / 1e6 / elapsed:8.1f} MB/s  correct={seen == expected}")

    print(f"\nRecommendation for this workload: {recommend('hashing').config}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

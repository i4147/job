#!/usr/bin/env python3
"""Minimal Termux example: stream sha256 digests of a folder, low memory.

    python3 termux/example_usage.py                 # hashes $HOME
    python3 termux/example_usage.py /sdcard/DCIM   # hashes a folder

Only ``run_parallel_lite.py`` is needed on the phone - this file just shows how.
"""
from __future__ import annotations

from pathlib import Path
import sys

try:  # running as a standalone script: python3 termux/example_usage.py
    from run_parallel_lite import LiteParallel, hash_folder, iter_file_digests, peak_rss_kb
except ImportError:  # imported as termux.example_usage
    from termux.run_parallel_lite import (
        LiteParallel,
        hash_folder,
        iter_file_digests,
        peak_rss_kb,
    )


def main() -> int:
    folder = sys.argv[1] if len(sys.argv) > 1 else str(Path.home())
    print(f"streaming sha256 of {folder} (workers=2, max_pending=4, 64 KiB blocks)")

    count = total = 0
    for outcome in hash_folder(
        folder,
        workers=2,
        max_pending=4,
        block_size=64 * 1024,
        gc_every=25,
        limit=100,
    ):
        if not outcome.ok:
            print(f"  ERROR {outcome.item}: {outcome.error}")
            continue
        path, digest, size = outcome.value
        count += 1
        total += size
        print(f"  {digest[:16]}…  {size:>9} B  {path}")

    print(f"\n{count} files, {total / 1e6:.2f} MB, "
          f"peak RSS {peak_rss_kb() / 1024 if peak_rss_kb() else 0:.1f} MiB")

    # The same helper works for any CPU/I/O task, not just hashing:
    results = LiteParallel(workers=2, backend="thread").run(abs, [-3, -1, 0, 2])
    print(f"LiteParallel(abs, ...).unwrap() -> {results.unwrap()}")

    # Serial streaming fallback with zero workers (useful for debugging):
    first = next(iter_file_digests(folder), None)
    print(f"first serial digest -> {first[1][:16] + '…' if first else 'n/a'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

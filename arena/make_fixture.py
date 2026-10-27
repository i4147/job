#!/usr/bin/env python3
"""Helper used during this session: create a deterministic file tree to hash.

    python3 arena/make_fixture.py --files 24 --size 65536
    python3 arena/make_fixture.py --dir ./fixture --keep --files 40

The default location is a temporary directory outside the repo, so no generated
data is committed.  Content is derived from the seed, so repeated runs are
byte-for-byte identical (useful for reproducible digests).
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import shutil
import tempfile


def chunk_for(seed: int, index: int, size: int) -> bytes:
    block = hashlib.sha512(f"{seed}:{index}".encode()).digest()
    return (block * (size // len(block) + 1))[:size]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", default=None, help="target directory (default: temp dir)")
    parser.add_argument("--files", type=int, default=24, help="number of files")
    parser.add_argument("--size", type=int, default=64 * 1024, help="bytes per file")
    parser.add_argument("--subdirs", type=int, default=4, help="subdirectories")
    parser.add_argument("--seed", type=int, default=20261027)
    parser.add_argument("--keep", action="store_true", help="keep an existing directory's files")
    args = parser.parse_args()

    target = Path(args.dir) if args.dir else Path(tempfile.mkdtemp(prefix="job_bench_fixture_"))
    if target.exists() and not args.keep:
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    subdirs = [target / f"dir_{i:02d}" for i in range(max(1, args.subdirs))]
    for directory in subdirs:
        directory.mkdir(exist_ok=True)
    total = 0
    for index in range(args.files):
        path = subdirs[index % len(subdirs)] / f"file_{index:04d}.bin"
        path.write_bytes(chunk_for(args.seed, index, args.size))
        total += args.size
    print(f"{target}\t{args.files}\t{total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

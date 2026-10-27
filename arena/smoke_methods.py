#!/usr/bin/env python3
"""Helper used during this session: run every registered method exactly once.

This is a fast pre-flight check for the full benchmark: it catches signature
mismatches, unpicklable callables, spawn-context problems and wrong result
shapes before any timings are recorded.

    python3 arena/smoke_methods.py /tmp/job_bench_fixture_xxx
    python3 arena/smoke_methods.py --methods jobutil1,jobutil6
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
from pathlib import Path
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]
BENCH = ROOT / "benchmark"
for path in (str(ROOT), str(BENCH)):
    if path not in sys.path:
        sys.path.insert(0, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folderpath", nargs="?", default=None, help="folder to hash")
    parser.add_argument("--workers", type=int, default=min(4, __import__("os").cpu_count() or 1))
    parser.add_argument("--chunksize", type=int, default=1)
    parser.add_argument("--methods", default="", help="comma separated id filters")
    parser.add_argument("--max-files", type=int, default=32)
    parser.add_argument("--verbose-traceback", action="store_true")
    args = parser.parse_args()

    from workers import collect_files, hash_file
    from methods import build_registry, interpreter_pool_available

    folder = args.folderpath or str(Path.home())
    files = collect_files(folder, max_files=args.max_files)
    if not files:
        print(f"no files under {folder}", file=sys.stderr)
        return 2
    expected = set(hash_file(path) for path in files)
    try:
        from importlib.metadata import version

        joblib_version = version("joblib")
    except Exception:
        joblib_version = None
    env = {"joblib": joblib_version, "interpreter_pool": interpreter_pool_available()}
    print(f"folder={folder} files={len(files)} workers={args.workers} "
          f"joblib={joblib_version} IPE={env['interpreter_pool']}")

    filters = [part.strip().lower() for part in args.methods.split(",") if part.strip()]
    registry = build_registry()
    for error in registry.import_errors:
        print(f"IMPORT ERROR: {error}")
    selected = [m for m in registry.methods
                if not filters or any(part in m.id.lower() for part in filters)]

    ok = skipped = failed = 0
    for method in selected:
        reasons = []
        for requirement in method.requires:
            if requirement == "joblib" and not env["joblib"]:
                reasons.append("joblib not installed")
            if requirement in ("python>=3.14", "interpreter_pool") and not env["interpreter_pool"]:
                reasons.append("InterpreterPoolExecutor needs CPython 3.14+")
        if reasons:
            print(f"SKIP  {method.id:<48} {'; '.join(sorted(set(reasons)))}")
            skipped += 1
            continue
        items = [(p,) for p in files] if method.input_style == "tuples" else list(files)
        started = time.perf_counter()
        try:
            result = method.runner(hash_file, items, args.workers, args.chunksize)
            values = list(result)
            pairs = {(str(v[0]), str(v[1])) for v in values}
            if pairs != expected:
                raise AssertionError(
                    f"wrong results: {len(pairs)} pairs vs {len(expected)} expected; "
                    f"first={values[:1]!r}"
                )
            elapsed = time.perf_counter() - started
            print(f"PASS  {method.id:<48} {elapsed:7.4f}s  ({method.primitive})")
            ok += 1
        except Exception as exc:
            print(f"FAIL  {method.id:<48} {type(exc).__name__}: {exc}")
            if args.verbose_traceback:
                traceback.print_exc()
            failed += 1
    print(f"\nok={ok} skipped={skipped} failed={failed} total={len(selected)}")
    return 1 if failed or registry.import_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())

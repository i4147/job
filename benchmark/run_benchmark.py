#!/usr/bin/env python3
"""Benchmark the multiprocessing implementations in this repository.

Workload: calculate a cryptographic hash (default sha256) of every file in a
user supplied folder.  If no folder is given the user's home folder is used.

    python3 benchmark/run_benchmark.py                       # hashes $HOME
    python3 benchmark/run_benchmark.py /path/to/folder -v
    python3 benchmark/run_benchmark.py --list-methods
    python3 benchmark/run_benchmark.py /tmp/fixture --repeats 5 --output results.json

Outputs (all inside benchmark/results/ by default):
    benchmark_results.json   full machine readable results, incl. top 5
    report.md                human readable top-5 report

The benchmark is verbose by default (use --quiet to tone it down), always runs a
warmup phase before timing, and InterpreterPoolExecutor based methods are
optional/skipped automatically on Python < 3.14.
"""
from __future__ import annotations

# NOTE: keep top-level side effects minimal.  With the "spawn" start method the
# child interpreter re-imports this file as __mp_main__, so everything that
# creates pools/timers must live behind the __main__ guard below.
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import partial
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import random
import re
import signal
import statistics
import sys
import time
from typing import Any

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _path in (str(_ROOT), str(_HERE)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import multiprocessing as mp  # noqa: E402

from workers import DEFAULT_ALGORITHM, collect_files, hash_file  # noqa: E402

VERBOSE = True
LOG_LINES: list[str] = []
_HEX_RE = re.compile(r"[0-9a-fA-F]{16,128}")


def log(message: str, *, force: bool = False) -> None:
    stamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{stamp}] {message}"
    LOG_LINES.append(line)
    if VERBOSE or force:
        print(line, flush=True)


class MethodTimeout(Exception):
    """Raised by the per-method watchdog."""


@contextmanager
def watchdog(seconds: float):
    if seconds <= 0 or not hasattr(signal, "SIGALRM"):
        yield
        return

    def _handler(_signum, _frame):
        raise MethodTimeout(f"exceeded the {seconds:g}s per-method timeout")

    previous = signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def pairs_of(results: Any) -> tuple[set[tuple[str, str]] | None, str]:
    """Normalise a method's output to a set of ``(path, digest)`` pairs."""
    pairs: set[tuple[str, str]] = set()
    try:
        values = list(results)
    except Exception as exc:  # pragma: no cover - defensive
        return None, f"result is not iterable: {type(exc).__name__}: {exc}"
    for value in values:
        if (
            isinstance(value, (tuple, list))
            and len(value) == 2
            and isinstance(value[0], str)
            and isinstance(value[1], str)
            and _HEX_RE.fullmatch(value[1])
        ):
            pairs.add((value[0], value[1]))
        else:
            snippet = repr(value)[:100]
            return None, f"unexpected result element {snippet}"
    return pairs, ""


def environment_info() -> dict[str, Any]:
    try:
        from importlib.metadata import PackageNotFoundError, version

        try:
            joblib_version: str | None = version("joblib")
        except PackageNotFoundError:
            joblib_version = None
    except ImportError:  # pragma: no cover
        joblib_version = None
    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    return {
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "default_start_method": mp.get_start_method(),
        "available_start_methods": list(mp.get_all_start_methods()),
        "gil_enabled": bool(gil_enabled),
        "joblib_version": joblib_version,
        "interpreter_pool_available": _interpreter_pool_available(),
    }


def _interpreter_pool_available() -> bool:
    from methods import interpreter_pool_available

    return interpreter_pool_available()


def check_requires(method, args, env) -> tuple[bool, str]:
    for requirement in method.requires:
        if requirement == "joblib":
            if args.no_joblib:
                return False, "joblib methods disabled with --no-joblib"
            if env["joblib_version"] is None:
                return False, "joblib is not installed (pip install joblib)"
        elif requirement == "python>=3.14":
            if args.interpreter == "off":
                return False, "interpreter methods disabled with --interpreter off"
            if sys.version_info < (3, 14):
                return False, (
                    f"optional: requires CPython >= 3.14 (running {platform.python_version()}); "
                    "InterpreterPoolExecutor was added in 3.14"
                )
        elif requirement == "interpreter_pool":
            if args.interpreter == "off":
                return False, "interpreter methods disabled with --interpreter off"
            if not env["interpreter_pool_available"]:
                return False, (
                    "optional: concurrent.futures.InterpreterPoolExecutor is not importable "
                    f"on {platform.python_version()} (--interpreter force cannot install it)"
                )
    return True, ""


def run_once(method, items, func, workers, chunksize, timeout):
    with watchdog(timeout):
        result = method.runner(func, items, workers, chunksize)
        if isinstance(result, (list, tuple)):
            return list(result), None
        return list(result), None  # generators / iterators are drained here


def select_files(args) -> tuple[Path, list[str], int]:
    folder = Path(args.folderpath).expanduser() if args.folderpath else Path.home()
    limits = dict(
        max_files=args.max_files,
        max_file_bytes=args.max_file_bytes,
        max_total_bytes=args.max_total_bytes,
        include_hidden=args.include_hidden,
        exclude=tuple(args.exclude),
    )
    files = collect_files(folder, **limits)
    total_bytes = 0
    for path in files:
        try:
            total_bytes += os.path.getsize(path)
        except OSError:
            pass
    return folder, files, total_bytes


def stats_for(times: list[float]) -> dict[str, float]:
    return {
        "min_seconds": min(times),
        "median_seconds": statistics.median(times),
        "mean_seconds": statistics.mean(times),
        "max_seconds": max(times),
        "stdev_seconds": statistics.pstdev(times) if len(times) > 1 else 0.0,
    }


def render_report(payload: dict[str, Any]) -> str:
    env = payload["environment"]
    work = payload["workload"]
    lines = [
        "# Multiprocessing benchmark report",
        "",
        f"Generated: {payload['generated_at']}",
        "",
        "## Workload",
        "",
        f"- Function under test: `hash_file(path)` -> `(path, {work['algorithm']} hexdigest)`",
        f"- Folder: `{work['folderpath']}` (default is the home folder)",
        f"- Files: {work['file_count']} ({work['total_bytes'] / 1e6:.3f} MB total)",
        f"- Workers: {work['workers']}, chunksize: {work['chunksize']}",
        f"- Warmup: {work['warmup_runs']} run(s) per method; timed runs: {work['repeats']}",
        f"- Python: {env['python']} ({env['python_implementation']}), CPUs: {env['cpu_count']}, "
        f"default start method: `{env['default_start_method']}`",
        f"- joblib: {env['joblib_version'] or 'not installed'}, "
        f"InterpreterPoolExecutor available: {env['interpreter_pool_available']}",
        "",
        "## Top 5 methods",
        "",
        "Ranked by median wall time (lower is better). The **function actually used**",
        "is the primitive each implementation dispatches to.",
        "",
        "| # | Method | Function used | Module / entrypoint | Median (s) | Speedup vs serial | MB/s |",
        "|---|--------|---------------|---------------------|-----------:|------------------:|-----:|",
    ]
    for row in payload["top5"]:
        lines.append(
            f"| {row['rank']} | `{row['id']}` | `{row['primitive']}` "
            f"(family: `{row['primitive_family']}`) | `{row['module']}.{row['entrypoint']}` "
            f"| {row['median_seconds']:.4f} | {row['speedup_vs_serial']:.2f}x "
            f"| {row['throughput_mb_s']:.2f} |"
        )
    lines += [
        "",
        "### Which function does each top-5 method use?",
        "",
    ]
    for row in payload["top5"]:
        lines.append(
            f"- **{row['rank']}. {row['id']}** uses `{row['primitive']}` "
            f"from `{row['primitive_family']}`. {row.get('notes', '')}".rstrip()
        )
    families: dict[str, int] = {}
    for row in payload["top5"]:
        families[row["primitive_family"]] = families.get(row["primitive_family"], 0) + 1
    family_summary = ", ".join(f"{name} x{count}" for name, count in
                               sorted(families.items(), key=lambda kv: -kv[1]))
    lines += [
        "",
        "### Interpretation",
        "",
        f"- Top-5 primitive families: {family_summary or 'n/a'}.",
        "- `hashlib` releases the GIL while hashing a block, so thread based "
        "pools can genuinely run in parallel here; they also avoid process "
        "creation, which is why thread based methods can beat process pools on "
        "this workload. Fork based pools are cheaper than spawn based pools.",
        "- Times include pool creation and teardown (each run starts fresh). "
        "Larger files / more repeats will shift the balance toward processes.",
        f"- Reproduce: `python3 benchmark/run_benchmark.py "
        f"{work['folderpath']} --repeats {work['repeats']} "
        f"--workers {work['workers']}`",
        "",
        "## Reference times",
        "",
        "| Reference | Median (s) |",
        "|-----------|-----------:|",
        f"| serial `hash_file` loop | {payload['serial_reference_seconds']:.4f} |",
    ]
    for row in payload["ranking_baselines"]:
        lines.append(f"| `{row['id']}` (`{row['primitive']}`) | {row['median_seconds']:.4f} |")
    lines += [
        "",
        "## Full ranking (all successful parallel methods)",
        "",
        "| # | Method | Function used | Median (s) | Min (s) | Speedup |",
        "|---|--------|---------------|-----------:|--------:|--------:|",
    ]
    for i, row in enumerate(payload["ranking"], 1):
        lines.append(
            f"| {i} | `{row['id']}` | `{row['primitive']}` | {row['median_seconds']:.4f} "
            f"| {row['min_seconds']:.4f} | {row['speedup_vs_serial']:.2f}x |"
        )
    skipped = [m for m in payload["methods"] if m["status"] == "skipped"]
    failed = [m for m in payload["methods"] if m["status"] == "failed"]
    lines += ["", "## Skipped methods", ""]
    if skipped:
        for m in skipped:
            lines.append(f"- `{m['id']}` ({m['primitive']}): {m['skip_reason']}")
    else:
        lines.append("- none")
    lines += ["", "## Failed methods", ""]
    if failed:
        for m in failed:
            lines.append(f"- `{m['id']}`: {m['error']}")
    else:
        lines.append("- none")
    lines += [
        "",
        "## Notes",
        "",
        "- `InterpreterPoolExecutor` is optional: methods that need it are skipped "
        "automatically on Python < 3.14 (`--interpreter force|off` to override).",
        "- joblib based methods are skipped when joblib is not installed.",
        "- Every measured run is verified against the serial digests; a method that "
        "returns wrong/missing hashes is reported as failed, not ranked.",
        "- A warmup phase runs before timing so pool/import/page-cache costs are not "
        "charged to the first timed run only.",
        "- Raw machine readable results: `benchmark_results.json`.",
        "",
    ]
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_benchmark.py",
        description="Benchmark the multiprocessing implementations in this repo by hashing files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("folderpath", nargs="?", default=None,
                        help="folder to hash (default: home folder)")
    parser.add_argument("--output", default=str(_HERE / "results" / "benchmark_results.json"),
                        help="path of the JSON results file")
    parser.add_argument("--report", default=str(_HERE / "results" / "report.md"),
                        help="path of the markdown report")
    parser.add_argument("--repeats", type=int, default=3,
                        help="timed runs per method")
    parser.add_argument("--warmup-runs", type=int, default=1,
                        help="warmup runs per method (results discarded)")
    parser.add_argument("--warmup-files", type=int, default=0,
                        help="files used during warmup (0 = all files)")
    parser.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1),
                        help="worker processes/threads where the API allows it")
    parser.add_argument("--chunksize", type=int, default=1,
                        help="chunksize passed to implementations that support it")
    parser.add_argument("--algorithm", default=DEFAULT_ALGORITHM,
                        help="hashlib algorithm used by the test function")
    parser.add_argument("--max-files", type=int, default=256)
    parser.add_argument("--max-file-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--max-total-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--include-hidden", action="store_true",
                        help="also walk hidden directories and files")
    parser.add_argument("--exclude", action="append", default=[],
                        help="skip paths containing this substring (repeatable)")
    parser.add_argument("--methods", default="",
                        help="comma separated id substrings to benchmark (e.g. jobutil1,jobutil2)")
    parser.add_argument("--interpreter", choices=("auto", "force", "off"), default="auto",
                        help="InterpreterPoolExecutor handling on Python < 3.14")
    parser.add_argument("--no-joblib", action="store_true", help="skip joblib based methods")
    parser.add_argument("--timeout", type=float, default=60.0,
                        help="per-run watchdog in seconds (0 disables it)")
    parser.add_argument("--seed", type=int, default=0,
                        help="shuffle method order per repeat using this seed (0 = no shuffle)")
    parser.add_argument("--list-methods", action="store_true",
                        help="print the method registry and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="collect files and list selected methods, do not time anything")
    parser.add_argument("--quiet", action="store_true",
                        help="reduce verbose progress output (the final report is still printed)")
    return parser


def main(argv: list[str] | None = None) -> int:
    global VERBOSE
    args = build_parser().parse_args(argv)
    VERBOSE = not args.quiet

    # Keep our own results out of the workload on repeated runs.
    results_dir = Path(args.output).resolve().parent
    if str(results_dir) not in args.exclude:
        args.exclude.append(str(results_dir))

    from methods import build_registry

    registry = build_registry()
    if registry.import_errors:
        for error in registry.import_errors:
            log(f"IMPORT ERROR: {error}", force=True)

    if args.list_methods:
        print(f"{'METHOD ID':<48} {'FUNCTION USED':<62} REQUIRES")
        for method in registry.methods:
            print(f"{method.id:<48} {method.primitive:<62} {','.join(method.requires) or '-'}")
        print(f"\n{len(registry.methods)} methods registered.")
        return 0

    if args.algorithm not in hashlib.algorithms_available:
        log(f"ERROR: unknown hashlib algorithm {args.algorithm!r}", force=True)
        return 2
    if args.repeats < 1 or args.warmup_runs < 0 or args.workers < 1 or args.chunksize < 1:
        log("ERROR: --repeats/--workers/--chunksize must be >= 1 and --warmup-runs >= 0",
            force=True)
        return 2

    folder, files, total_bytes = select_files(args)
    if not files:
        log(f"ERROR: no readable files found under {folder} with the current limits", force=True)
        return 1

    env = environment_info()
    log("=" * 78, force=True)
    log("multiprocessing benchmark - hash files of a folder", force=True)
    log("=" * 78, force=True)
    log(f"folderpath        : {folder} (default = home folder)"
        + (" [DEFAULT]" if args.folderpath is None else ""))
    log(f"files / bytes     : {len(files)} files, {total_bytes / 1e6:.3f} MB")
    log(f"algorithm         : {args.algorithm}")
    log(f"workers/chunksize : {args.workers} / {args.chunksize}")
    log(f"warmup/timed runs : {args.warmup_runs} / {args.repeats}")
    log(f"python / cpus     : {env['python']} / {env['cpu_count']} "
        f"(start method: {env['default_start_method']})")
    log(f"joblib / IPE      : {env['joblib_version'] or 'missing'} / "
        f"available={env['interpreter_pool_available']}")
    log(f"limits            : max_files={args.max_files}, "
        f"max_file_bytes={args.max_file_bytes}, max_total_bytes={args.max_total_bytes}")
    log(f"output            : {args.output} + {args.report}")
    log("-" * 78)

    # Only hash functions are timed: build the exact worker callable once.
    worker = hash_file if args.algorithm == DEFAULT_ALGORITHM else partial(
        hash_file, algorithm=args.algorithm
    )
    expected = set(worker(path) for path in files)
    log(f"baseline          : {len(expected)} (path, digest) pairs computed serially")

    filters = [part.strip().lower() for part in args.methods.split(",") if part.strip()]
    selected = [m for m in registry.methods
                if not filters or any(part in m.id.lower() for part in filters)]
    log(f"methods selected  : {len(selected)} / {len(registry.methods)}"
        + (f" (filters: {filters})" if filters else ""))

    records: dict[str, dict[str, Any]] = {}
    for method in selected:
        available, reason = check_requires(method, args, env)
        records[method.id] = {
            "id": method.id,
            "module": method.module,
            "entrypoint": method.entrypoint,
            "primitive": method.primitive,
            "primitive_family": method.primitive_family,
            "input_style": method.input_style,
            "notes": method.notes,
            "baseline": method.baseline,
            "requires": list(method.requires),
            "status": "pending" if available else "skipped",
            "skip_reason": reason,
            "error": None,
            "correctness": None,
            "runs_seconds": [],
            "warmup_seconds": [],
        }
    for rec in records.values():
        if rec["status"] == "skipped":
            log(f"SKIP  {rec['id']}: {rec['skip_reason']}")

    if args.dry_run:
        log(f"DRY RUN: {len(selected)} method(s) selected, "
            f"{sum(r['status'] == 'pending' for r in records.values())} runnable; "
            "no warmup, no timings, no files written.", force=True)
        return 0

    def items_for(method, paths):
        return [(p,) for p in paths] if method.input_style == "tuples" else list(paths)

    active = [m for m in selected if records[m.id]["status"] == "pending"]
    warmup_paths = files[: args.warmup_files] if args.warmup_files else files
    warmup_expected = set(worker(path) for path in warmup_paths)

    # ---------------------------- warmup phase -----------------------------
    log("-" * 78)
    log(f"WARMUP PHASE: {args.warmup_runs} run(s) x {len(active)} method(s) "
        f"on {len(warmup_paths)} file(s) (results discarded)", force=True)
    for index, method in enumerate(active, 1):
        rec = records[method.id]
        for warm in range(args.warmup_runs):
            try:
                started = time.perf_counter()
                result, _ = run_once(method, items_for(method, warmup_paths), worker,
                                     args.workers, args.chunksize, args.timeout)
                elapsed = time.perf_counter() - started
                pairs, why = pairs_of(result)
                if pairs != warmup_expected:
                    raise RuntimeError(
                        f"warmup produced {len(pairs) if pairs is not None else 'invalid'} "
                        f"of {len(warmup_expected)} expected digests; {why}"
                    )
                rec["warmup_seconds"].append(elapsed)
                log(f"  warmup [{index:>3}/{len(active)}] {method.id:<46} "
                    f"run {warm + 1}/{args.warmup_runs}: {elapsed:.4f}s OK")
            except Exception as exc:
                rec["status"] = "failed"
                rec["error"] = f"warmup: {type(exc).__name__}: {exc}"
                log(f"  warmup [{index:>3}/{len(active)}] {method.id:<46} "
                    f"FAILED: {rec['error']}", force=True)
                break

    # ---------------------------- timed phase ------------------------------
    active = [m for m in active if records[m.id]["status"] == "pending"]
    log("-" * 78)
    log(f"TIMED PHASE: {args.repeats} run(s) x {len(active)} method(s) "
        f"on {len(files)} file(s)", force=True)
    for repeat in range(args.repeats):
        order = list(active)
        if args.seed:
            random.Random(args.seed + repeat).shuffle(order)
        log(f"--- repeat {repeat + 1}/{args.repeats} "
            f"({'shuffled' if args.seed else 'registry order'}) ---")
        for index, method in enumerate(order, 1):
            rec = records[method.id]
            if rec["status"] != "pending":
                continue
            try:
                started = time.perf_counter()
                result, _ = run_once(method, items_for(method, files), worker,
                                     args.workers, args.chunksize, args.timeout)
                elapsed = time.perf_counter() - started
                pairs, why = pairs_of(result)
                if pairs != expected:
                    raise RuntimeError(
                        f"result mismatch: got {len(pairs) if pairs is not None else 'invalid'} "
                        f"of {len(expected)} digests; {why}"
                    )
                rec["runs_seconds"].append(elapsed)
                rec["correctness"] = True
                log(f"  timed  [{index:>3}/{len(order)}] {method.id:<46} "
                    f"run {repeat + 1}/{args.repeats}: {elapsed:.4f}s  "
                    f"{total_bytes / 1e6 / elapsed:.1f} MB/s  OK")
            except Exception as exc:
                rec["status"] = "failed"
                rec["correctness"] = False
                rec["error"] = f"{type(exc).__name__}: {exc}"
                log(f"  timed  [{index:>3}/{len(order)}] {method.id:<46} "
                    f"FAILED: {rec['error']}", force=True)

    # ------------------------- serial reference ----------------------------
    serial_times = []
    for _ in range(args.repeats):
        started = time.perf_counter()
        serial_pairs = set(worker(path) for path in files)
        serial_times.append(time.perf_counter() - started)
        if serial_pairs != expected:  # pragma: no cover - files changed mid-run
            log("WARNING: files changed during the benchmark; serial digests differ", force=True)
            break
    serial_median = statistics.median(serial_times)
    log("-" * 78)
    log(f"serial reference  : median {serial_median:.4f}s over {len(serial_times)} run(s)")

    # ---------------------------- aggregation ------------------------------
    for rec in records.values():
        times = rec.pop("runs_seconds")
        if rec["status"] == "pending":
            rec["status"] = "ok" if times else "failed"
            if not times:
                rec["error"] = rec["error"] or "no successful timed runs"
        if times:
            rec.update(stats_for(times))
            rec["runs_seconds"] = times
            rec["throughput_mb_s"] = (total_bytes / 1e6) / statistics.median(times)
            rec["speedup_vs_serial"] = serial_median / statistics.median(times)
        else:
            rec.update(
                {
                    "min_seconds": None, "median_seconds": None, "mean_seconds": None,
                    "max_seconds": None, "stdev_seconds": None,
                    "runs_seconds": [], "throughput_mb_s": None,
                    "speedup_vs_serial": None,
                }
            )

    def ranking_row(rec, rank=None):
        row = {
            "id": rec["id"], "module": rec["module"], "entrypoint": rec["entrypoint"],
            "primitive": rec["primitive"], "primitive_family": rec["primitive_family"],
            "notes": rec["notes"], "median_seconds": rec["median_seconds"],
            "min_seconds": rec["min_seconds"], "mean_seconds": rec["mean_seconds"],
            "stdev_seconds": rec["stdev_seconds"],
            "throughput_mb_s": rec["throughput_mb_s"],
            "speedup_vs_serial": rec["speedup_vs_serial"],
        }
        if rank is not None:
            row = {"rank": rank, **row}
        return row

    ok = [r for r in records.values() if r["status"] == "ok"]
    rankable = sorted((r for r in ok if not r["baseline"]),
                      key=lambda r: r["median_seconds"])
    baselines = sorted((r for r in ok if r["baseline"]),
                       key=lambda r: r["median_seconds"])
    ranking = [ranking_row(rec) for rec in rankable]
    top5 = [ranking_row(rec, rank) for rank, rec in enumerate(rankable[:5], 1)]

    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "environment": env,
        "workload": {
            "folderpath": str(folder),
            "folderpath_is_default_home": args.folderpath is None,
            "file_count": len(files),
            "total_bytes": total_bytes,
            "algorithm": args.algorithm,
            "workers": args.workers,
            "chunksize": args.chunksize,
            "warmup_runs": args.warmup_runs,
            "warmup_files": len(warmup_paths),
            "repeats": args.repeats,
            "per_method_timeout_seconds": args.timeout,
            "limits": {
                "max_files": args.max_files,
                "max_file_bytes": args.max_file_bytes,
                "max_total_bytes": args.max_total_bytes,
                "include_hidden": args.include_hidden,
                "exclude": list(args.exclude),
            },
            "method_filters": filters,
        },
        "serial_reference_seconds": serial_median,
        "serial_reference_runs_seconds": serial_times,
        "methods": list(records.values()),
        "ranking": ranking,
        "ranking_baselines": [ranking_row(rec) for rec in baselines],
        "top5": top5,
        "summary": {
            "methods_registered": len(registry.methods),
            "methods_selected": len(selected),
            "ok": len(ok),
            "skipped": sum(r["status"] == "skipped" for r in records.values()),
            "failed": sum(r["status"] == "failed" for r in records.values()),
            "fastest": rankable[0]["id"] if rankable else None,
            "fastest_function": rankable[0]["primitive"] if rankable else None,
        },
        "log": LOG_LINES,
    }

    output_path = Path(args.output).resolve()
    report_path = Path(args.report).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    report_path.write_text(render_report(payload), encoding="utf-8")

    log("=" * 78, force=True)
    log("TOP 5 METHODS (by median wall time)", force=True)
    for row in top5:
        log(f"  {row['rank']}. {row['id']:<46} {row['median_seconds']:.4f}s  "
            f"{row['speedup_vs_serial']:.2f}x  uses {row['primitive']}", force=True)
    if not top5:
        log("  (no successful methods)", force=True)
    log(f"ok={payload['summary']['ok']} skipped={payload['summary']['skipped']} "
        f"failed={payload['summary']['failed']}", force=True)
    log(f"JSON results : {output_path}", force=True)
    log(f"Markdown     : {report_path}", force=True)
    log("=" * 78, force=True)
    return 0 if top5 else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Helper used during this session: sanity-check and summarise results JSON.

    python3 arena/verify_results.py benchmark/results/benchmark_results.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="?",
                        default="benchmark/results/benchmark_results.json")
    args = parser.parse_args()
    path = Path(args.results)
    data = json.loads(path.read_text(encoding="utf-8"))

    problems = []
    methods = data["methods"]
    if data["summary"]["ok"] + data["summary"]["skipped"] + data["summary"]["failed"] != len(methods):
        problems.append("status counts do not add up")
    for method in methods:
        if method["status"] == "ok":
            if not method.get("correctness"):
                problems.append(f"{method['id']}: ok but correctness is not true")
            if not method.get("runs_seconds"):
                problems.append(f"{method['id']}: ok but no timings")
            if not method.get("primitive"):
                problems.append(f"{method['id']}: missing primitive metadata")
    ranking = [m["median_seconds"] for m in data["ranking"]]
    if ranking != sorted(ranking):
        problems.append("ranking is not sorted by median time")
    if data["top5"] and len(data["top5"]) > 5:
        problems.append("top5 has more than 5 entries")

    print(f"results file     : {path}")
    print(f"generated        : {data['generated_at']}")
    print(f"files / bytes    : {data['workload']['file_count']} / {data['workload']['total_bytes']}")
    print(f"warmup / repeats : {data['workload']['warmup_runs']} / {data['workload']['repeats']}")
    print(f"status           : ok={data['summary']['ok']} "
          f"skipped={data['summary']['skipped']} failed={data['summary']['failed']}")
    print("\nTOP 5 (function actually used):")
    for row in data["top5"]:
        print(f"  {row['rank']}. {row['id']:<46} {row['median_seconds']:.4f}s  "
              f"{row['primitive']}")
    if problems:
        print("\nPROBLEMS:")
        for problem in problems:
            print(f"  - {problem}")
    else:
        print("\nAll consistency checks passed.")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())

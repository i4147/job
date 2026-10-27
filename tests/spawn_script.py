#!/usr/bin/env python3
"""Standalone spawn-context check, invoked as a subprocess by test_mp_pool.py."""
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_parallel import MpPoolRunner  # noqa: E402
from tests.workers import square  # noqa: E402


def main() -> int:
    runner = MpPoolRunner(square, method="map", workers=2, start_method="spawn")
    result = runner.run(range(4))
    print(f"spawn-ok {result.unwrap()}")
    return 0 if result.unwrap() == [0, 1, 4, 9] else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Helper used during this session: compile + import every repo module.

This is how the pre-existing breakages were found (prunner.py PEP 695 syntax,
jobutil6.py relative import, etc.).  Run before benchmarking:

    python3 arena/check_imports.py
"""
from __future__ import annotations

import importlib
from pathlib import Path
import py_compile
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    failures = 0
    for path in sorted(ROOT.glob("*.py")):
        try:
            py_compile.compile(str(path), doraise=True)
            status = "syntax OK"
        except py_compile.PyCompileError as exc:
            status = f"SYNTAX ERROR: {exc.msg.strip().splitlines()[-1]}"
            failures += 1
        try:
            importlib.import_module(path.stem)
            imported = "import OK"
        except Exception as exc:
            imported = f"IMPORT ERROR: {type(exc).__name__}: {exc}"
            failures += 1
        print(f"{path.name:<24} {status:<28} {imported}")
    print(f"\n{failures} problem(s) found.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

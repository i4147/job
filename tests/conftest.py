"""Shared pytest fixtures and import path setup."""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_parallel import INTERPRETER_AVAILABLE, JOBLIB_AVAILABLE  # noqa: E402

requires_joblib = pytest.mark.skipif(not JOBLIB_AVAILABLE, reason="joblib is not installed")
requires_interpreter = pytest.mark.skipif(
    not INTERPRETER_AVAILABLE, reason="InterpreterPoolExecutor needs CPython >= 3.14"
)
linux_only = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux only")


@pytest.fixture
def numbers() -> list[int]:
    return [-3, -1, 0, 1, 2, 3, 4, 5]


@pytest.fixture
def pairs() -> list[tuple[int, int]]:
    return [(1, 2), (3, 4), (5, 6), (7, 8)]


@pytest.fixture
def small_files(tmp_path: Path) -> list[str]:
    """Deterministic 8 KiB files in two subdirectories."""
    paths: list[str] = []
    for sub in ("a", "b"):
        (tmp_path / sub).mkdir()
        for index in range(4):
            path = tmp_path / sub / f"file_{index}.bin"
            path.write_bytes(bytes([(index + ord(sub)) % 256]) * 8192)
            paths.append(str(path))
    return paths

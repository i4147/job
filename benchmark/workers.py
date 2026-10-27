"""Hashing workload used by the multiprocessing benchmark.

Everything in this module is defined at module level on purpose: several of the
implementations under test use the ``spawn`` start method, which can only pickle
importable top-level callables (no lambdas, no closures, no __main__ locals).
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

DEFAULT_ALGORITHM = "sha256"
_READ_SIZE = 1 << 20  # 1 MiB

# Directories that are skipped while walking the user supplied folder.  They are
# excluded so that benchmarking $HOME by default stays fast and side-effect free.
DEFAULT_SKIP_DIRS = frozenset(
    {
        ".git",
        ".hg",
        ".svn",
        ".cache",
        ".local",
        ".npm",
        ".venv",
        "venv",
        "node_modules",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".tox",
        "site-packages",
    }
)


def hash_file(path: str, algorithm: str = DEFAULT_ALGORITHM) -> tuple[str, str]:
    """Return ``(path, hexdigest)`` for one file.

    A plain two-element tuple keeps every implementation under test on exactly
    the same workload/return shape (including ``starmap`` style dispatchers).
    """
    digest = hashlib.new(algorithm)
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(_READ_SIZE), b""):
            digest.update(block)
    return os.fspath(path), digest.hexdigest()


def collect_files(
    folderpath: str | os.PathLike[str] | None = None,
    *,
    max_files: int = 256,
    max_file_bytes: int = 8 * 1024 * 1024,
    max_total_bytes: int = 64 * 1024 * 1024,
    include_hidden: bool = False,
    skip_dirs: frozenset[str] = DEFAULT_SKIP_DIRS,
    exclude: tuple[str, ...] = (),
) -> list[str]:
    """Collect regular, readable files under ``folderpath`` (default: $HOME).

    The limits are safety rails so that pointing this at a home directory cannot
    accidentally hash hundreds of gigabytes.  Files are returned sorted so every
    implementation sees an identical, deterministic workload.
    """
    root = Path(folderpath).expanduser() if folderpath is not None else Path.home()
    root = root.resolve()
    found: list[str] = []
    total = 0
    for dirpath, dirnames, filenames in os.walk(root, onerror=lambda _err: None):
        dirnames[:] = sorted(
            name
            for name in dirnames
            if (include_hidden or not name.startswith("."))
            and name not in skip_dirs
            and not any(part in str(Path(dirpath) / name) for part in exclude)
        )
        for name in sorted(filenames):
            if not include_hidden and name.startswith("."):
                continue
            path = Path(dirpath) / name
            candidate = str(path)
            if any(part in candidate for part in exclude):
                continue
            try:
                if not path.is_file() or path.is_symlink():
                    continue
                size = path.stat().st_size
                if size > max_file_bytes or total + size > max_total_bytes:
                    continue
                with open(path, "rb") as handle:
                    handle.read(1)
            except (OSError, ValueError):
                continue
            found.append(candidate)
            total += size
            if len(found) >= max_files:
                return found
    return found


def hash_folder(
    folderpath: str | os.PathLike[str] | None = None,
    *,
    algorithm: str = DEFAULT_ALGORITHM,
    **limits,
) -> list[tuple[str, str]]:
    """Calculate the hash of every file in ``folderpath`` (default: home folder).

    This is the reference (serial) implementation of the function under test.
    """
    return [hash_file(path, algorithm) for path in collect_files(folderpath, **limits)]

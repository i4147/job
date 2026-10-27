"""Top-level, picklable worker functions used by the tests.

``spawn`` / ``forkserver`` / ``InterpreterPoolExecutor`` children import this
module by name, so no worker may be a lambda, closure or ``__main__`` local.
"""
from __future__ import annotations

import hashlib
import os
import time
from typing import Any


def square(value: int) -> int:
    return value * value


def add(a: int, b: int) -> int:
    return a + b


def add_triple(a: int, b: int, c: int) -> int:
    return a + b + c


def slow_square(value: int, delay: float = 0.005) -> int:
    time.sleep(delay)
    return value * value


def fail_on(value: int, bad: int = 3) -> int:
    if value == bad:
        raise ValueError(f"bad value: {value}")
    return value * 10


def always_fail(value: Any) -> Any:
    raise RuntimeError(f"boom: {value}")


def digest_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def file_digest(path: str, block_size: int = 1 << 16) -> tuple[str, str]:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(block_size), b""):
            digest.update(block)
    return path, digest.hexdigest()


def identity_with_pid(value: int) -> tuple[int, int]:
    return os.getpid(), value


MARKER = "unset"


def set_marker(value: str = "ready") -> None:
    global MARKER
    MARKER = value


def marker_value(value: int) -> tuple[str, int]:
    return MARKER, value

"""Shared types, helpers and validation."""
from __future__ import annotations

import pytest

from run_parallel import (
    ErrorPolicy,
    ExecutorKind,
    JoblibBackend,
    MpMethod,
    ParallelResult,
    TaskError,
    auto_chunksize,
)
from run_parallel.core import normalize_items, resolve_workers
from tests.workers import square


def test_mp_method_enum_has_all_seven_required_methods():
    assert {m.value for m in MpMethod} == {
        "map", "imap", "starmap", "apply_async",
        "map_async", "starmap_async", "imap_unordered",
    }
    assert MpMethod.parse("IMAP_UNORDERED") is MpMethod.IMAP_UNORDERED
    assert MpMethod.STARMAP.is_star
    assert MpMethod.IMAP_UNORDERED.is_lazy
    assert MpMethod.IMAP_UNORDERED.is_unordered
    with pytest.raises(ValueError):
        MpMethod.parse("nope")


def test_executor_and_joblib_enums():
    assert {k.executor_name for k in ExecutorKind} == {
        "ProcessPoolExecutor", "ThreadPoolExecutor", "InterpreterPoolExecutor"
    }
    assert ExecutorKind.parse("THREAD") is ExecutorKind.THREAD
    assert JoblibBackend.parse("loky") is JoblibBackend.LOKY
    assert ErrorPolicy.parse("COLLECT") is ErrorPolicy.COLLECT


def test_resolve_workers_and_chunksize():
    assert resolve_workers(None) >= 1
    assert resolve_workers(3, n_items=2) == 2
    assert resolve_workers(-1) == resolve_workers(None)
    assert resolve_workers(-2) == max(1, resolve_workers(None) - 1)
    assert resolve_workers(8, cap=2) == 2
    assert auto_chunksize(100, 4) == 7  # ceil(100 / 16)
    assert auto_chunksize(0, 4) == 1
    assert auto_chunksize(100, 4, cap=3) == 3


@pytest.mark.parametrize(
    ("items", "star", "expected"),
    [
        ([1, 2], False, [1, 2]),
        ([(1, 2), [3, 4]], True, [(1, 2), (3, 4)]),
        (["abc", 5], True, [("abc",), (5,)]),  # strings are single args
    ],
)
def test_normalize_items(items, star, expected):
    assert normalize_items(items, star=star) == expected


def test_parallel_result_container_and_unwrap():
    ok = ParallelResult([1, 2], [], 0.1, 2, {"primitive": "test"})
    assert ok.ok and ok.succeeded == 2 and ok.failed == 0
    assert list(ok) == [1, 2] and len(ok) == 2 and ok[1] == 2
    assert ok.unwrap() == [1, 2] and ok.primitive == "test"

    err = ValueError("bad")
    bad = ParallelResult([1, err], [TaskError(1, 3, err)], 0.1, 2)
    assert not bad.ok and bad.failed == 1 and bad.succeeded == 1
    with pytest.raises(ValueError, match="bad"):
        bad.unwrap()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"retries": -1},
        {"timeout": 0},
        {"chunksize": 0},
    ],
)
def test_base_validation(kwargs):
    from run_parallel import ExecutorRunner

    with pytest.raises((TypeError, ValueError)):
        ExecutorRunner(square, kind="thread", **kwargs)


def test_non_callable_func_rejected():
    from run_parallel import ExecutorRunner

    with pytest.raises(TypeError):
        ExecutorRunner(42, kind="thread")

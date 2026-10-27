"""Class 1: multiprocessing.Pool - all seven methods and their options."""
from __future__ import annotations

from functools import partial
from pathlib import Path
import subprocess
import sys

import pytest

from run_parallel import ErrorPolicy, MpMethod, MpPoolRunner
from tests.conftest import linux_only
from tests.workers import add, fail_on, marker_value, set_marker, slow_square, square

FORK = {"start_method": "fork"}  # fast + safe in this test process on Linux


@pytest.mark.parametrize("method", list(MpMethod))
def test_all_seven_mp_methods(method, numbers):
    runner = MpPoolRunner(square, method=method, workers=2, **FORK)
    result = runner.run(numbers)
    assert result.unwrap() == [x * x for x in numbers]
    assert result.ok and result.submitted == len(numbers)
    assert result.primitive == f"multiprocessing.Pool.{method.value}"
    assert result.metadata["method"] == method.value
    assert result.metadata["start_method"] == "fork"
    assert result.metadata["workers"] == 2


@pytest.mark.parametrize("method", list(MpMethod))
def test_star_args_work_for_every_method(method, pairs):
    runner = MpPoolRunner(add, method=method, star=True, workers=2, **FORK)
    assert runner.run(pairs).unwrap() == [a + b for a, b in pairs]


def test_starmap_auto_normalizes_scalar_items(numbers):
    runner = MpPoolRunner(square, method=MpMethod.STARMAP, workers=2, **FORK)
    assert runner.run(numbers).unwrap() == [x * x for x in numbers]


def test_method_override_at_run_time(numbers):
    runner = MpPoolRunner(square, method=MpMethod.MAP, workers=2, **FORK)
    assert runner.run(numbers, method="imap_unordered", ordered=True).unwrap() == [
        x * x for x in numbers
    ]
    with pytest.raises(TypeError):
        runner.run(numbers, unknown_option=1)


def test_imap_unordered_order_restoration(numbers):
    ordered = MpPoolRunner(square, method="imap_unordered", ordered=True,
                           workers=2, **FORK).run(numbers)
    unordered = MpPoolRunner(square, method="imap_unordered", ordered=False,
                             workers=2, **FORK).run(numbers)
    assert ordered.unwrap() == [x * x for x in numbers]
    assert sorted(unordered.unwrap()) == sorted(x * x for x in numbers)


def test_iter_results_streams_without_materializing(numbers):
    runner = MpPoolRunner(square, method="imap", workers=2, **FORK)
    assert list(runner.iter_results(numbers)) == [x * x for x in numbers]
    unordered = MpPoolRunner(square, method="imap_unordered", ordered=False,
                             workers=2, **FORK)
    assert sorted(unordered.iter_results(numbers)) == sorted(x * x for x in numbers)
    tagged = MpPoolRunner(square, method="imap_unordered", ordered=True,
                          workers=2, **FORK)
    assert list(tagged.iter_results(numbers)) == [x * x for x in numbers]


@pytest.mark.parametrize("policy,expected_len", [
    (ErrorPolicy.COLLECT, 5), (ErrorPolicy.SKIP, 4),
])
def test_error_policies(policy, expected_len):
    runner = MpPoolRunner(fail_on, method="map", workers=2,
                          error_policy=policy, **FORK)
    result = runner.run([1, 2, 3, 4, 5])
    assert len(result.values) == expected_len
    assert result.failed == 1 and not result.ok
    assert result.errors[0].item == 3
    assert "bad value: 3" in str(result.errors[0].exception)
    if policy is ErrorPolicy.COLLECT:
        assert isinstance(result.values[2], RuntimeError)


def test_raise_policy_propagates():
    runner = MpPoolRunner(fail_on, method="map", workers=2,
                          error_policy="raise", **FORK)
    with pytest.raises(ValueError, match="bad value"):
        runner.run([1, 2, 3])


def test_streaming_skip_policy_drops_failed_items():
    runner = MpPoolRunner(fail_on, method="imap", error_policy="skip",
                          workers=2, **FORK)
    assert list(runner.iter_results([1, 2, 3, 4])) == [10, 20, 40]


def test_timeout_on_map_async():
    slow = partial(slow_square, delay=0.4)
    runner = MpPoolRunner(slow, method="map_async", workers=2,
                          timeout=0.05, **FORK)
    with pytest.raises(TimeoutError):
        runner.run(range(4))


def test_empty_input_returns_empty_result():
    runner = MpPoolRunner(square, workers=2, **FORK)
    result = runner.run([])
    assert result.values == [] and result.submitted == 0 and result.ok
    assert list(runner.iter_results([])) == []


def test_context_manager_reuses_one_pool(numbers):
    with MpPoolRunner(square, method="map", workers=2, **FORK) as runner:
        first = runner.run(numbers[:4])
        assert runner._resource is not None  # pool kept alive between runs
        second = runner.run(numbers[4:])
    assert runner._resource is None
    assert first.unwrap() + second.unwrap() == [x * x for x in numbers]


def test_initializer_runs_in_workers(numbers):
    runner = MpPoolRunner(marker_value, method="map", workers=2,
                          initializer=set_marker, initargs=("ready",), **FORK)
    assert runner.run(numbers).unwrap() == [("ready", x) for x in numbers]


def test_invalid_configuration():
    with pytest.raises(ValueError):
        MpPoolRunner(square, method="does_not_exist")
    with pytest.raises(ValueError):
        MpPoolRunner(square, start_method="not_a_start_method")
    with pytest.raises(ValueError):
        MpPoolRunner(square, max_pending=0)


@linux_only
def test_spawn_context_with_module_level_worker():
    """spawn is exercised in a real subprocess (never from pytest's __main__)."""
    script = Path(__file__).with_name("spawn_script.py")
    completed = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, timeout=120,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    assert completed.returncode == 0, completed.stderr
    assert "spawn-ok [0, 1, 4, 9]" in completed.stdout

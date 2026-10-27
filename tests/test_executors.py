"""Class 2: concurrent.futures Process/Thread/Interpreter PoolExecutor."""
from __future__ import annotations

from functools import partial
import sys

import pytest

from run_parallel import (
    INTERPRETER_AVAILABLE,
    BackendUnavailable,
    ErrorPolicy,
    ExecutorKind,
    ExecutorRunner,
)
from tests.conftest import requires_interpreter
from tests.workers import add, fail_on, slow_square, square


@pytest.mark.parametrize("kind", [ExecutorKind.THREAD, ExecutorKind.PROCESS])
@pytest.mark.parametrize("mode", ["map", "submit"])
def test_process_and_thread_executors(kind, mode, numbers):
    runner = ExecutorRunner(square, kind=kind, mode=mode, workers=2)
    result = runner.run(numbers)
    assert result.unwrap() == [x * x for x in numbers]
    assert result.metadata["kind"] == kind.value
    assert result.metadata["mode"] == mode
    assert result.primitive == (
        f"concurrent.futures.{kind.executor_name}.{'map' if mode == 'map' else 'submit'}"
    )


@requires_interpreter
def test_interpreter_executor(numbers):
    runner = ExecutorRunner(square, kind="interpreter", mode="map", workers=2)
    assert runner.run(numbers).unwrap() == [x * x for x in numbers]


@pytest.mark.skipif(INTERPRETER_AVAILABLE, reason="only on Python < 3.14")
def test_interpreter_is_optional_on_older_python():
    with pytest.raises(BackendUnavailable, match="3.14"):
        ExecutorRunner(square, kind="interpreter")


def test_star_args(numbers, pairs):
    runner = ExecutorRunner(add, kind="thread", star=True, workers=2)
    assert runner.run(pairs).unwrap() == [a + b for a, b in pairs]
    assert ExecutorRunner(square, kind="thread", star=True, workers=2).run(
        numbers
    ).unwrap() == [x * x for x in numbers]


def test_submit_unordered_yields_completion_order(numbers):
    runner = ExecutorRunner(square, kind="thread", mode="submit",
                            unordered=True, workers=2)
    result = runner.run(numbers)
    assert sorted(result.unwrap()) == sorted(x * x for x in numbers)
    with pytest.raises(ValueError):
        runner.run(numbers, mode="map", unordered=True)


def test_bounded_in_flight_window(numbers):
    runner = ExecutorRunner(square, kind="thread", mode="submit",
                            workers=2, max_pending=3)
    assert runner._cap(2) == 3
    assert runner.run(numbers).unwrap() == [x * x for x in numbers]


def test_iter_results_streams(numbers):
    map_runner = ExecutorRunner(square, kind="thread", mode="map", workers=2)
    assert list(map_runner.iter_results(numbers)) == [x * x for x in numbers]
    submit_runner = ExecutorRunner(square, kind="thread", mode="submit", workers=2)
    assert sorted(submit_runner.iter_results(numbers)) == sorted(x * x for x in numbers)


@pytest.mark.parametrize("policy,expected_len", [
    (ErrorPolicy.COLLECT, 5), (ErrorPolicy.SKIP, 4),
])
def test_error_policies(policy, expected_len):
    runner = ExecutorRunner(fail_on, kind="thread", workers=2,
                            error_policy=policy)
    result = runner.run([1, 2, 3, 4, 5])
    assert len(result.values) == expected_len and result.failed == 1


def test_raise_policy_propagates():
    runner = ExecutorRunner(fail_on, kind="thread", workers=2)
    with pytest.raises(ValueError, match="bad value"):
        runner.run([1, 2, 3])


def test_submit_timeout():
    slow = partial(slow_square, delay=0.4)
    runner = ExecutorRunner(slow, kind="process", mode="submit",
                            workers=2, timeout=0.05)
    with pytest.raises(TimeoutError):
        runner.run(range(4))


def test_empty_input():
    result = ExecutorRunner(square, kind="thread").run([])
    assert result.values == [] and result.ok and result.submitted == 0
    assert list(ExecutorRunner(square, kind="thread").iter_results([])) == []


def test_context_manager_reuses_executor(numbers):
    with ExecutorRunner(square, kind="thread", workers=2) as runner:
        first = runner.run(numbers[:4])
        assert runner._resource is not None
        second = runner.run(numbers[4:])
    assert runner._resource is None
    assert first.unwrap() + second.unwrap() == [x * x for x in numbers]


@pytest.mark.filterwarnings("ignore:max_tasks_per_child is ignored:RuntimeWarning")
def test_process_recycles_workers(numbers):
    # CPython 3.11 deadlocks when ProcessPoolExecutor recycles workers; the
    # helper ignores the option there and warns. 3.12+ (target: 3.14) uses it.
    runner = ExecutorRunner(square, kind="process", workers=2,
                            max_tasks_per_child=2)
    result = runner.run(numbers)
    assert result.unwrap() == [x * x for x in numbers]
    assert result.metadata["max_tasks_per_child_active"] is (
        sys.version_info >= (3, 12)
    )


def test_invalid_configuration():
    with pytest.raises(ValueError):
        ExecutorRunner(square, mode="wat")
    with pytest.raises(ValueError):
        ExecutorRunner(square, kind="nope")
    with pytest.raises(ValueError):
        ExecutorRunner(square, max_pending=0)

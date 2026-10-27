"""Class 3: 3rd-party joblib.Parallel / delayed."""
from __future__ import annotations

import pytest

from run_parallel import ErrorPolicy, JoblibBackend, JoblibRunner
from run_parallel import joblib_runner
from tests.conftest import requires_joblib
from tests.workers import add, fail_on, square

pytestmark = requires_joblib


@pytest.mark.parametrize("backend", [
    JoblibBackend.LOKY, JoblibBackend.THREADING, JoblibBackend.SEQUENTIAL,
])
def test_all_joblib_backends(backend, numbers):
    runner = JoblibRunner(square, backend=backend, n_jobs=2,
                          pre_dispatch="2 * n_jobs", batch_size="auto")
    result = runner.run(numbers)
    assert result.unwrap() == [x * x for x in numbers]
    assert result.primitive == "joblib.Parallel + joblib.delayed"
    assert result.metadata["joblib_backend"] == backend.value


def test_available_and_memmap_options():
    assert JoblibRunner.available() is True
    runner = JoblibRunner(square, n_jobs=2, mmap_mode="r",
                          max_nbytes="1M", temp_folder=None)
    kwargs = runner._parallel_kwargs(2)
    assert kwargs["mmap_mode"] == "r"
    assert kwargs["max_nbytes"] == "1M"
    assert kwargs["pre_dispatch"] == "2 * n_jobs"
    assert runner.run([1, 2, 3]).unwrap() == [1, 4, 9]


def test_star_args(pairs):
    runner = JoblibRunner(add, backend="threading", star=True, n_jobs=2)
    assert runner.run(pairs).unwrap() == [a + b for a, b in pairs]


@pytest.mark.parametrize("policy,expected_len", [
    (ErrorPolicy.COLLECT, 5), (ErrorPolicy.SKIP, 4),
])
def test_error_policies(policy, expected_len):
    runner = JoblibRunner(fail_on, backend="threading", n_jobs=2,
                          error_policy=policy)
    result = runner.run([1, 2, 3, 4, 5])
    assert len(result.values) == expected_len and result.failed == 1


def test_raise_policy_propagates():
    runner = JoblibRunner(fail_on, backend="threading", n_jobs=2)
    with pytest.raises(ValueError, match="bad value"):
        runner.run([1, 2, 3])


def test_return_as_generator_streams(numbers):
    runner = JoblibRunner(square, backend="threading", n_jobs=2,
                          return_as="generator")
    assert list(runner.iter_results(numbers)) == [x * x for x in numbers]
    assert runner.run(numbers).unwrap() == [x * x for x in numbers]


def test_empty_and_context_manager(numbers):
    assert JoblibRunner(square, n_jobs=2).run([]).values == []
    with JoblibRunner(square, backend="threading", n_jobs=2) as runner:
        first = runner.run(numbers[:4])
        assert runner._resource is not None
        second = runner.run(numbers[4:])
    assert runner._resource is None
    assert first.unwrap() + second.unwrap() == [x * x for x in numbers]


def test_invalid_configuration():
    with pytest.raises(ValueError):
        JoblibRunner(square, backend="not-a-backend")
    with pytest.raises(ValueError):
        JoblibRunner(square, return_as="tuple")
    with pytest.raises(TypeError):
        JoblibRunner(square).run([1, 2], unknown=1)


def test_missing_joblib_raises_backend_unavailable(monkeypatch):
    from run_parallel import BackendUnavailable

    monkeypatch.setattr(joblib_runner, "JOBLIB_AVAILABLE", False)
    with pytest.raises(BackendUnavailable, match="pip install joblib"):
        JoblibRunner(square)

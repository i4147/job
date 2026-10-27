"""Unified facade, factory and benchmark-driven recommendations."""
from __future__ import annotations

import pytest

from run_parallel import (
    BACKENDS,
    JOBLIB_AVAILABLE,
    ExecutorRunner,
    JoblibRunner,
    MpPoolRunner,
    RunParallel,
    family_stats,
    make_runner,
    recommend,
    run_parallel,
    WORKLOADS,
)
from tests.conftest import requires_joblib
from tests.workers import square


@pytest.mark.parametrize(("backend", "expected"), [
    ("mp", MpPoolRunner), ("multiprocessing", MpPoolRunner),
    ("executor", ExecutorRunner), ("concurrent.futures", ExecutorRunner),
    ("joblib", JoblibRunner),
])
def test_make_runner_backends(backend, expected):
    runner = make_runner(backend, workers=2)
    assert isinstance(runner, expected)


def test_make_runner_auto_uses_benchmark_result():
    runner = make_runner("auto", workload="hashing", workers=2)
    assert isinstance(runner, ExecutorRunner)
    assert runner.kind.value == "thread"
    assert runner.mode == "map"


def test_unknown_backend_and_option():
    with pytest.raises(ValueError):
        make_runner("wat")
    with pytest.raises(TypeError, match="unsupported options"):
        make_runner("mp", not_an_option=1)


def test_run_parallel_one_shot(numbers):
    result = run_parallel(square, numbers, backend="auto", workload="hashing")
    assert result.unwrap() == [x * x for x in numbers]
    assert result.metadata["backend"] == "executor"


def test_run_parallel_explicit_backends(numbers):
    assert run_parallel(square, numbers, backend="executor",
                        executor_kind="thread").unwrap() == [x * x for x in numbers]
    assert run_parallel(square, numbers, backend="mp", method="map",
                        start_method="fork").unwrap() == [x * x for x in numbers]


@requires_joblib
def test_run_parallel_joblib(numbers):
    assert run_parallel(square, numbers, backend="joblib",
                        n_jobs=2).unwrap() == [x * x for x in numbers]


def test_run_parallel_class_context_manager(numbers):
    with RunParallel("auto", workload="hashing", workers=2, func=square) as parallel:
        assert parallel.backend == "executor"
        assert parallel.primitive == "concurrent.futures.ThreadPoolExecutor.map"
        assert parallel.run(numbers).unwrap() == [x * x for x in numbers]
        assert parallel.map(numbers).unwrap() == [x * x for x in numbers]
        assert list(parallel.imap(numbers[:4])) == [x * x for x in numbers[:4]]
        assert parallel(numbers[:2]).unwrap() == [9, 1]


def test_benchmark_results_are_loaded_and_threads_win():
    stats = family_stats()
    assert stats, "benchmark/results/benchmark_results.json should be present"
    thread = stats["concurrent.futures.ThreadPoolExecutor"]
    serial = stats["serial (no pool)"]
    assert thread.median_seconds < serial.median_seconds
    assert thread.count > 0


@pytest.mark.parametrize("workload", WORKLOADS)
def test_recommendations_are_constructible(workload):
    rec = recommend(workload)
    if rec.backend == "joblib" and not JOBLIB_AVAILABLE:
        pytest.skip("joblib is not installed")
    assert rec.backend in BACKENDS
    assert rec.config["backend"] == rec.backend
    assert rec.rationale
    assert rec.measured is True  # repo ships benchmark_results.json
    # every recommendation can be turned into a real runner
    config = dict(rec.config)
    config.pop("primitive", None)
    if rec.backend == "mp":
        config["start_method"] = "fork"
    runner = make_runner(**config)
    assert runner.run([1, 2, 3], square).unwrap() == [1, 4, 9]


def test_isolated_workload_uses_interpreter_on_target_runtime(monkeypatch):
    """Simulate the 3.14 target: interpreter executor must be selected."""
    import importlib

    # `run_parallel.recommend` is also a function exported by __init__, so load
    # the submodule through sys.modules explicitly.
    recommend_module = importlib.import_module("run_parallel.recommend")

    monkeypatch.setattr(recommend_module, "INTERPRETER_AVAILABLE", True)
    rec = recommend_module.recommend("isolated", python_version=(3, 14))
    assert rec.config["executor_kind"] == "interpreter"
    assert "InterpreterPoolExecutor" in rec.config["primitive"]

    fallback = recommend_module.recommend("isolated", python_version=(3, 11))
    assert fallback.config["executor_kind"] == "process"


def test_recommend_prefer_memory_and_invalid_workload():
    rec = recommend("hashing", prefer_memory=True)
    assert rec.config["workers"] == 2 and rec.config["max_pending"] == 4
    with pytest.raises(ValueError, match="unknown workload"):
        recommend("quantum_computing")


def test_fallback_recommendation_without_results(tmp_path):
    rec = recommend("hashing", results_path=tmp_path / "missing.json")
    assert rec.measured is False
    assert rec.backend == "executor"
    assert "n/a" in rec.rationale

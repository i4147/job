"""Registry of every multiprocessing implementation/method under test.

Each :class:`Method` records the *underlying primitive* it really calls, e.g.
``multiprocessing.Pool.imap_unordered`` or
``concurrent.futures.ProcessPoolExecutor.map``.  That metadata is what the JSON
results and the report use to say which function the top 5 methods use.
"""
from __future__ import annotations

import importlib
import multiprocessing as mp
import sys
from dataclasses import dataclass, field
from typing import Any, Callable

# Primitive "families" used for grouping in the report.
POOL = "multiprocessing.Pool"
PPE = "concurrent.futures.ProcessPoolExecutor"
TPE = "concurrent.futures.ThreadPoolExecutor"
IPE = "concurrent.futures.InterpreterPoolExecutor"
JOBLIB = "joblib.Parallel"
SERIAL = "serial (no pool)"


@dataclass(frozen=True)
class Method:
    id: str
    module: str
    entrypoint: str
    primitive: str
    primitive_family: str
    runner: Callable[[Callable, list, int, int], Any] = field(repr=False)
    requires: tuple[str, ...] = ()
    input_style: str = "paths"  # "paths" or "tuples" (for *star* dispatchers)
    notes: str = ""
    baseline: bool = False  # timed, but excluded from the top-5 ranking


@dataclass
class Registry:
    methods: list[Method] = field(default_factory=list)
    import_errors: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------
# availability helpers
# --------------------------------------------------------------------------
def interpreter_pool_class():
    """Return InterpreterPoolExecutor when importable, else None (3.14+ only)."""
    try:
        from concurrent.futures import InterpreterPoolExecutor

        return InterpreterPoolExecutor
    except ImportError:
        try:
            from concurrent.futures.interpreter import InterpreterPoolExecutor

            return InterpreterPoolExecutor
        except ImportError:
            return None


def interpreter_pool_available() -> bool:
    return sys.version_info >= (3, 14) and interpreter_pool_class() is not None


# --------------------------------------------------------------------------
# generic runners
# --------------------------------------------------------------------------
def _fn(module, name: str, kwargs_map: dict[str, Any] | None = None):
    """Adapter for the ``func, items, ...`` style helpers in jobutil1/2."""

    def run(func, items, workers, chunksize):
        kwargs: dict[str, Any] = {}
        for key, value in (kwargs_map or {}).items():
            if value == "@workers":
                kwargs[key] = workers
            elif value == "@chunksize":
                kwargs[key] = chunksize
            else:
                kwargs[key] = value
        return getattr(module, name)(func, items, **kwargs)

    return run


def _generic(module, entry: str, method: str | None = None):
    """Adapter for ``mpf`` / ``run_parallel`` style dispatchers (jobutil3-5, co)."""

    def run(func, items, workers, chunksize):
        dispatcher = getattr(module, entry)
        if method is None:
            return dispatcher(func, items, workers=workers, chunksize=chunksize)
        return dispatcher(func, items, method, workers=workers, chunksize=chunksize)

    return run


def _raise_worker_error(item, error):
    raise RuntimeError(f"worker failed for {item!r}: {error!r}")


def _mpf3_jobutil1(module):
    """jobutil1.mpf3 returns only a summary; collect results via on_success."""

    def run(func, items, workers, chunksize):
        collected: list[Any] = []

        def on_success(item, result):  # parent-side callback, never pickled
            collected.append(result)

        summary = module.mpf3(func, items, max_workers=workers, on_success=on_success)
        failed = getattr(summary, "failed", 0)
        if failed or len(collected) != len(items):
            raise RuntimeError(
                f"mpf3 reported failed={failed}, collected={len(collected)}/{len(items)}"
            )
        return collected

    return run


def _mpf3_jobutil2(module):
    def run(func, items, workers, chunksize):
        return module.mpf3(
            func,
            items,
            max_workers=workers,
            timeout=None,
            on_error=_raise_worker_error,
        )

    return run


def _joblib_j1(module):
    """jobutil1.mpf_joblib hardcodes n_jobs=-1 and forwards kwargs to workers."""

    def run(func, items, workers, chunksize):
        return module.mpf_joblib(func, items)

    return run


def _joblib_j2(module):
    def run(func, items, workers, chunksize):
        return module.mpf_joblib(func, items, n_jobs=workers)

    return run


def _job_helper_parallel(module, backend: str | None):
    def run(func, items, workers, chunksize):
        parallel = module.Parallel(n_jobs=workers, backend=backend)
        return parallel(module.delayed(func)(item) for item in items)

    return run


def _jobutil6_parallel(module, backend: str, method: str | None = None, *, imap: bool = False,
                       ordered: bool = True):
    def run(func, items, workers, chunksize):
        parallel = module.Parallel(workers=workers, backend=backend)
        if imap:
            return list(parallel.imap(func, items, ordered=ordered))
        if method is None:
            return parallel.map(func, items)
        return parallel.map(func, items, method=method)

    return run


def _prunner_parallel(module, backend: str, *, unordered: bool = False):
    def run(func, items, workers, chunksize):
        runner = module.ParallelRunner(
            backend,
            max_workers=workers,
            chunksize=chunksize,
            on_error="raise",
        )
        try:
            return list(runner.map(func, items, chunksize=chunksize, unordered=unordered))
        finally:
            runner.close()

    return run


def _serial(func, items, workers, chunksize):
    return [func(item) for item in items]


def _stdlib_ppe_map(func, items, workers, chunksize):
    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(func, items, chunksize=chunksize))


def _stdlib_tpe_map(func, items, workers, chunksize):
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(func, items))


def _stdlib_pool_imapu(func, items, workers, chunksize):
    with mp.Pool(workers) as pool:
        return list(pool.imap_unordered(func, items, chunksize))


def _stdlib_ipe_map(func, items, workers, chunksize):
    executor_cls = interpreter_pool_class()
    if executor_cls is None:
        raise ImportError("InterpreterPoolExecutor requires CPython 3.14+")
    with executor_cls(max_workers=workers) as executor:
        return list(executor.map(func, items, chunksize=1))


# --------------------------------------------------------------------------
# registry construction
# --------------------------------------------------------------------------
GENERIC_METHODS: dict[str, tuple[str, str]] = {
    "map": ("multiprocessing.Pool.map", POOL),
    "map_async": ("multiprocessing.Pool.map_async", POOL),
    "starmap": ("multiprocessing.Pool.starmap", POOL),
    "starmap_async": ("multiprocessing.Pool.starmap_async", POOL),
    "imap": ("multiprocessing.Pool.imap", POOL),
    "imap_unordered": ("multiprocessing.Pool.imap_unordered", POOL),
    "apply": ("multiprocessing.Pool.apply", POOL),
    "apply_async": ("multiprocessing.Pool.apply_async", POOL),
    "process_pool": ("concurrent.futures.ProcessPoolExecutor.map", PPE),
    "thread_pool": ("concurrent.futures.ThreadPoolExecutor.map", TPE),
    "joblib": ("joblib.Parallel + joblib.delayed", JOBLIB),
    "serial": ("builtins.map (serial loop)", SERIAL),
}

# jobutil4/5/jobutils_co have no 'serial' method.
GENERIC_MODULES = {
    "jobutil3": "mpf",
    "jobutil4": "run_parallel",
    "jobutil5": "mpf",
    "jobutils_co": "mpf",
}

J1_METHODS: list[tuple[str, str, str, str, dict[str, Any]]] = [
    ("mpf_async", "multiprocessing.Pool.apply_async", POOL, "paths", {"workers": "@workers"}),
    ("mpf_map", "multiprocessing.Pool.map", POOL, "paths", {"workers": "@workers"}),
    ("mpf_imap", "multiprocessing.Pool.imap_unordered", POOL, "paths",
     {"workers": "@workers", "chunksize": "@chunksize"}),
    ("mpf_imapu", "multiprocessing.Pool.imap_unordered", POOL, "paths",
     {"workers": "@workers", "chunksize": "@chunksize"}),
    ("mpf_starmap", "multiprocessing.Pool.starmap", POOL, "paths", {"workers": "@workers"}),
    ("mpf_ppe", "concurrent.futures.ProcessPoolExecutor.submit", PPE, "paths",
     {"workers": "@workers"}),
    ("mpf_ppe_ac", "concurrent.futures.ProcessPoolExecutor.submit + as_completed", PPE,
     "paths", {"workers": "@workers"}),
    ("mpf_tpe", "concurrent.futures.ThreadPoolExecutor.submit", TPE, "paths",
     {"workers": "@workers"}),
    ("mpf", "multiprocessing.Pool.apply_async", POOL, "paths", {"num_jobs": "@workers"}),
    ("mpf2", "multiprocessing.Pool.apply_async", POOL, "paths", {"num_jobs": "@workers"}),
]

J2_METHODS: list[tuple[str, str, str, str, dict[str, Any]]] = [
    ("mpf_async", "multiprocessing.Pool.apply_async", POOL, "paths", {"workers": "@workers"}),
    ("mpf_map", "multiprocessing.Pool.map", POOL, "paths",
     {"workers": "@workers", "chunksize": "@chunksize"}),
    ("mpf_imap", "multiprocessing.Pool.imap", POOL, "paths",
     {"workers": "@workers", "chunksize": "@chunksize"}),
    ("mpf_imapu", "multiprocessing.Pool.imap_unordered", POOL, "paths",
     {"workers": "@workers", "chunksize": "@chunksize"}),
    ("mpf_starmap", "multiprocessing.Pool.starmap", POOL, "paths", {"workers": "@workers"}),
    ("mpf_ppe", "concurrent.futures.ProcessPoolExecutor.submit", PPE, "paths",
     {"workers": "@workers"}),
    ("mpf_ppe_ac", "concurrent.futures.ProcessPoolExecutor.submit + as_completed", PPE,
     "paths", {"workers": "@workers"}),
    ("mpf_tpe", "concurrent.futures.ThreadPoolExecutor.submit", TPE, "paths",
     {"workers": "@workers"}),
    ("mpf", "multiprocessing.Pool.apply_async", POOL, "paths", {"num_jobs": "@workers"}),
    ("mpf2", "multiprocessing.Pool.apply_async", POOL, "paths", {"num_jobs": "@workers"}),
    ("mpf_ppe_ac_use_as_completed", "concurrent.futures.ProcessPoolExecutor.submit + as_completed",
     PPE, "paths", {"workers": "@workers", "use_as_completed": True}),
    ("mpf_as", "multiprocessing.Pool.apply_async (bounded deque)", POOL, "paths", {}),
    ("mpf_bounded", "multiprocessing.Pool.apply_async (bounded in-flight)", POOL, "paths",
     {"processes": "@workers"}),
    ("mpf_stream", "multiprocessing.Pool.imap", POOL, "paths",
     {"processes": "@workers", "chunksize": "@chunksize"}),
    ("mpf_imap2", "multiprocessing.Pool.imap", POOL, "paths",
     {"processes": "@workers", "chunksize": "@chunksize"}),
]


def build_registry() -> Registry:
    reg = Registry()
    modules: dict[str, Any] = {}
    names = [
        "job_helper",
        "jobutil1",
        "jobutil2",
        "jobutil3",
        "jobutil4",
        "jobutil5",
        "jobutil6",
        "jobutils_co",
        "prunner",
    ]
    for name in names:
        try:
            modules[name] = importlib.import_module(name)
        except Exception as exc:  # keep the rest of the benchmark usable
            reg.import_errors.append(f"{name}: {type(exc).__name__}: {exc}")

    add = reg.methods.append

    # ---- serial reference -------------------------------------------------
    add(Method("baseline.serial", "benchmark.workers", "hash_file (serial loop)",
               "builtins.map (serial reference)", SERIAL, _serial, baseline=True,
               notes="Reference implementation, no pool at all."))

    # ---- jobutil1 ---------------------------------------------------------
    j1 = modules.get("jobutil1")
    if j1:
        for name, primitive, family, style, kw in J1_METHODS:
            real_name = "mpf_ppe" if name == "mpf_ppe_ac_use_as_completed" else name
            add(Method(f"jobutil1.{name}", "jobutil1", real_name, primitive, family,
                       _fn(j1, real_name, kw), input_style=style,
                       notes="spawn context" if primitive.startswith("multiprocessing.Pool") else ""))
        add(Method("jobutil1.mpf3", "jobutil1", "mpf3",
                   "concurrent.futures.ProcessPoolExecutor.submit + wait(FIRST_COMPLETED)",
                   PPE, _mpf3_jobutil1(j1),
                   notes="bounded in-flight futures; results collected via on_success"))
        add(Method("jobutil1.mpf_joblib", "jobutil1", "mpf_joblib",
                   "joblib.Parallel + joblib.delayed", JOBLIB, _joblib_j1(j1),
                   requires=("joblib",), notes="n_jobs=-1 hardcoded in this implementation"))

    # ---- jobutil2 ---------------------------------------------------------
    j2 = modules.get("jobutil2")
    if j2:
        for name, primitive, family, style, kw in J2_METHODS:
            real_name = "mpf_ppe" if name == "mpf_ppe_ac_use_as_completed" else name
            add(Method(f"jobutil2.{name}", "jobutil2", real_name, primitive, family,
                       _fn(j2, real_name, kw), input_style=style,
                       notes="Pool(8) hardcoded" if name == "mpf_as" else ""))
        add(Method("jobutil2.mpf3", "jobutil2", "mpf3",
                   "concurrent.futures.ProcessPoolExecutor.submit + wait(FIRST_COMPLETED)",
                   PPE, _mpf3_jobutil2(j2), notes="bounded in-flight futures"))
        add(Method("jobutil2.mpf_joblib", "jobutil2", "mpf_joblib",
                   "joblib.Parallel + joblib.delayed", JOBLIB, _joblib_j2(j2),
                   requires=("joblib",)))

    # ---- generic dispatchers ---------------------------------------------
    for mod_name, entry in GENERIC_MODULES.items():
        module = modules.get(mod_name)
        if not module:
            continue
        for method, (primitive, family) in GENERIC_METHODS.items():
            if method == "serial" and mod_name != "jobutil3":
                continue
            add(Method(f"{mod_name}.{entry}.{method}", mod_name, entry, primitive, family,
                       _generic(module, entry, method),
                       requires=("joblib",) if method == "joblib" else (),
                       baseline=(method == "serial"),
                       notes=f"{entry}(method={method!r})"))
    j3 = modules.get("jobutil3")
    if j3:
        add(Method("jobutil3.mpf_iter.imap", "jobutil3", "mpf_iter",
                   "multiprocessing.Pool.imap", POOL,
                   lambda func, items, workers, chunksize: list(
                       j3.mpf_iter(func, items, workers=workers, chunksize=chunksize)),
                   notes="generator-based streaming API"))

    # ---- jobutil6 ---------------------------------------------------------
    j6 = modules.get("jobutil6")
    if j6:
        # (suffix, backend, method, primitive, family, input_style, requires, use_imap)
        j6_entries = [
            ("process.map", "process", "map", "multiprocessing.Pool.map (spawn)",
             POOL, "paths", (), False),
            ("process.imap_unordered", "process", None,
             "multiprocessing.Pool.imap_unordered (spawn)", POOL, "paths", (), True),
            ("process.starmap", "process", "starmap",
             "multiprocessing.Pool.starmap (spawn)", POOL, "tuples", (), False),
            ("process.apply_async", "process", "apply_async",
             "multiprocessing.Pool.apply_async (spawn)", POOL, "tuples", (), False),
            ("thread.map", "thread", "map", "multiprocessing.dummy.Pool.map (threads)",
             POOL, "paths", (), False),
            ("cf_process.map", "cf_process", "map",
             "concurrent.futures.ProcessPoolExecutor.map (spawn)", PPE, "paths", (), False),
            ("cf_process.imap_unordered", "cf_process", "imap_unordered",
             "concurrent.futures.ProcessPoolExecutor.submit + as_completed",
             PPE, "paths", (), False),
            ("cf_thread.map", "cf_thread", "map",
             "concurrent.futures.ThreadPoolExecutor.map", TPE, "paths", (), False),
            ("cf_interpreter.map", "cf_interpreter", "map",
             "concurrent.futures.InterpreterPoolExecutor.map", IPE, "paths",
             ("python>=3.14", "interpreter_pool"), False),
            ("serial.map", "serial", "map", "builtins.map (serial loop)",
             SERIAL, "paths", (), False),
        ]
        for suffix, backend, method, primitive, family, style, requires, use_imap in j6_entries:
            add(Method(f"jobutil6.Parallel.{suffix}", "jobutil6", "Parallel.map",
                       primitive, family,
                       _jobutil6_parallel(j6, backend, method,
                                          imap=use_imap, ordered=False),
                       requires=requires, input_style=style,
                       baseline=(backend == "serial"),
                       notes=f"Parallel(backend={backend!r})"))

    # ---- prunner ----------------------------------------------------------
    pr = modules.get("prunner")
    if pr:
        pr_entries = [
            ("thread", "concurrent.futures.ThreadPoolExecutor.submit", TPE, (), False),
            ("process", "concurrent.futures.ProcessPoolExecutor.submit", PPE, (), False),
            ("interpreter", "concurrent.futures.InterpreterPoolExecutor.submit", IPE,
             ("python>=3.14", "interpreter_pool"), False),
            ("mp_imap", "multiprocessing.Pool.imap", POOL, (), False),
            ("mp_imap_unordered", "multiprocessing.Pool.imap_unordered", POOL, (), True),
            ("mp_apply_async", "multiprocessing.Pool.apply_async", POOL, (), False),
            ("auto", "concurrent.futures.ProcessPoolExecutor.submit (auto backend)", PPE,
             (), False),
        ]
        for backend, primitive, family, requires, unordered in pr_entries:
            add(Method(f"prunner.ParallelRunner.{backend}", "prunner", "ParallelRunner.map",
                       primitive, family, _prunner_parallel(pr, backend, unordered=unordered),
                       requires=requires,
                       notes="streaming generator API; auto resolves to 'process' on GIL builds"))

    # ---- job_helper (in-repo joblib shim) ---------------------------------
    jh = modules.get("job_helper")
    if jh:
        add(Method("job_helper.Parallel.process", "job_helper", "Parallel.__call__",
                   "concurrent.futures.ProcessPoolExecutor.map (job_helper shim)", PPE,
                   _job_helper_parallel(jh, None)))
        add(Method("job_helper.Parallel.thread", "job_helper", "Parallel.__call__",
                   "concurrent.futures.ThreadPoolExecutor.map (job_helper shim)", TPE,
                   _job_helper_parallel(jh, "threading")))

    # ---- raw stdlib references (not repo implementations) -----------------
    add(Method("stdlib.ProcessPoolExecutor.map", "concurrent.futures",
               "ProcessPoolExecutor.map", "concurrent.futures.ProcessPoolExecutor.map",
               PPE, _stdlib_ppe_map, baseline=True, notes="raw stdlib reference"))
    add(Method("stdlib.ThreadPoolExecutor.map", "concurrent.futures",
               "ThreadPoolExecutor.map", "concurrent.futures.ThreadPoolExecutor.map",
               TPE, _stdlib_tpe_map, baseline=True, notes="raw stdlib reference"))
    add(Method("stdlib.Pool.imap_unordered", "multiprocessing", "Pool.imap_unordered",
               "multiprocessing.Pool.imap_unordered", POOL, _stdlib_pool_imapu,
               baseline=True, notes="raw stdlib reference"))
    add(Method("stdlib.InterpreterPoolExecutor.map", "concurrent.futures",
               "InterpreterPoolExecutor.map",
               "concurrent.futures.InterpreterPoolExecutor.map", IPE, _stdlib_ipe_map,
               requires=("python>=3.14", "interpreter_pool"), baseline=True,
               notes="raw stdlib reference; optional on Python < 3.14"))
    return reg

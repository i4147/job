from __future__ import annotations
import functools
import gzip
import hashlib
import os
import pickle
import shutil
import tempfile
__all__ = [
    "dump",
    "load",
    "hash",
    "Parallel",
    "delayed",
    "Memory",
    "MemorizedFunc",
    "cpu_count",
    "__version__",
]
__version__ = "0.0.0+job_helper"
def cpu_count(only_physical_cores: bool = False) -> int:
    return os.cpu_count() or 1
def hash(obj, hash_name: str = "md5", **kwargs) -> str:
    try:
        hasher = hashlib.new(hash_name)
    except ValueError:
        hasher = hashlib.md5()
    hasher.update(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))
    return hasher.hexdigest()
def dump(value, filename, compress=0, protocol=None, **kwargs):
    protocol = protocol if protocol is not None else pickle.HIGHEST_PROTOCOL
    filename = os.fspath(filename)
    if compress:
        level = compress[1] if isinstance(compress, (tuple, list)) else int(compress)
        level = 9 if level <= 0 else min(level, 9)
        with gzip.open(filename, "wb", compresslevel=level) as fh:
            pickle.dump(value, fh, protocol=protocol)
    else:
        with open(filename, "wb") as fh:
            pickle.dump(value, fh, protocol=protocol)
    return [filename]
def load(filename, mmap_mode=None, **kwargs):
    filename = os.fspath(filename)
    with open(filename, "rb") as fh:
        magic = fh.read(2)
    if magic == b"\x1f\x8b":
        with gzip.open(filename, "rb") as fh:
            return pickle.load(fh)
    with open(filename, "rb") as fh:
        return pickle.load(fh)
def delayed(func, *args, **kwargs):
    return functools.partial(func, *args, **kwargs)
def _call(task):
    return task()
class Parallel:
    def __init__(self, n_jobs=1, backend=None, verbose=0, timeout=None, **kwargs):
        self.n_jobs = n_jobs
        self.backend = backend
        self.verbose = verbose
        self.timeout = timeout
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False
    def _resolve_n_jobs(self) -> int:
        n = self.n_jobs
        if n is None or n == 0:
            return 1
        if n < 0:
            return max(1, cpu_count() + 1 + n)
        return int(n)
    def __call__(self, iterable):
        tasks = list(iterable)
        n = self._resolve_n_jobs()
        if n == 1 or len(tasks) <= 1:
            return [_call(t) for t in tasks]
        return self._run_parallel(tasks, n)
    def _run_parallel(self, tasks, n):
        from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
        if self.backend not in ("threading", "threads"):
            try:
                with ProcessPoolExecutor(max_workers=n) as ex:
                    return list(ex.map(_call, tasks))
            except Exception:
                pass
        with ThreadPoolExecutor(max_workers=n) as ex:
            return list(ex.map(_call, tasks))
class MemorizedFunc:
    def __init__(self, func, location, ignore=None, verbose=None):
        self.func = func
        self.location = location
        self.ignore = tuple(ignore or ())
        self.verbose = verbose
        functools.update_wrapper(self, func)
    def _key(self, args, kwargs) -> str:
        filtered = {k: v for k, v in sorted(kwargs.items()) if k not in self.ignore}
        payload = pickle.dumps((args, filtered), protocol=pickle.HIGHEST_PROTOCOL)
        return hashlib.md5(payload).hexdigest()
    def __call__(self, *args, **kwargs):
        try:
            key = self._key(args, kwargs)
        except Exception:
            return self.func(*args, **kwargs)
        path = os.path.join(self.location, key + ".pkl")
        try:
            return load(path)
        except Exception:
            pass
        result = self.func(*args, **kwargs)
        try:
            tmp = path + ".tmp"
            dump(result, tmp)
            os.replace(tmp, path)
        except Exception:
            pass
        return result
class Memory:
    def __init__(self, location=None, verbose=0, bytes_limit=None, **kwargs):
        self.location = os.fspath(location) if location else tempfile.mkdtemp(prefix="job_helper_cache_")
        self.verbose = verbose
        os.makedirs(self.location, exist_ok=True)
    def cache(self, func=None, ignore=None, verbose=None, **kwargs):
        if func is None:
            return functools.partial(self.cache, ignore=ignore, verbose=verbose, **kwargs)
        return MemorizedFunc(func, self.location, ignore=ignore, verbose=self.verbose if verbose is None else verbose)
    def clear(self, warn=True):
        if os.path.isdir(self.location):
            shutil.rmtree(self.location, ignore_errors=True)
        os.makedirs(self.location, exist_ok=True)

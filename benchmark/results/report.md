# Multiprocessing benchmark report

Generated: 2026-10-27T19:48:39+00:00

## Workload

- Function under test: `hash_file(path)` -> `(path, sha256 hexdigest)`
- Folder: `/tmp/job_bench_fixture_ual_xd5v` (default is the home folder)
- Files: 64 (134.218 MB total)
- Workers: 2, chunksize: 1
- Warmup: 1 run(s) per method; timed runs: 5
- Python: 3.11.2 (CPython), CPUs: 2, default start method: `fork`
- joblib: 1.6.0, InterpreterPoolExecutor available: False

## Top 5 methods

Ranked by median wall time (lower is better). The **function actually used**
is the primitive each implementation dispatches to.

| # | Method | Function used | Module / entrypoint | Median (s) | Speedup vs serial | MB/s |
|---|--------|---------------|---------------------|-----------:|------------------:|-----:|
| 1 | `jobutil4.run_parallel.thread_pool` | `concurrent.futures.ThreadPoolExecutor.map` (family: `concurrent.futures.ThreadPoolExecutor`) | `jobutil4.run_parallel` | 0.0707 | 2.41x | 1898.31 |
| 2 | `jobutil3.mpf.thread_pool` | `concurrent.futures.ThreadPoolExecutor.map` (family: `concurrent.futures.ThreadPoolExecutor`) | `jobutil3.mpf` | 0.0713 | 2.39x | 1882.81 |
| 3 | `jobutils_co.mpf.thread_pool` | `concurrent.futures.ThreadPoolExecutor.map` (family: `concurrent.futures.ThreadPoolExecutor`) | `jobutils_co.mpf` | 0.0718 | 2.37x | 1868.46 |
| 4 | `jobutil5.mpf.thread_pool` | `concurrent.futures.ThreadPoolExecutor.map` (family: `concurrent.futures.ThreadPoolExecutor`) | `jobutil5.mpf` | 0.0725 | 2.35x | 1851.86 |
| 5 | `jobutil1.mpf_tpe` | `concurrent.futures.ThreadPoolExecutor.submit` (family: `concurrent.futures.ThreadPoolExecutor`) | `jobutil1.mpf_tpe` | 0.0729 | 2.33x | 1841.43 |

### Which function does each top-5 method use?

- **1. jobutil4.run_parallel.thread_pool** uses `concurrent.futures.ThreadPoolExecutor.map` from `concurrent.futures.ThreadPoolExecutor`. run_parallel(method='thread_pool')
- **2. jobutil3.mpf.thread_pool** uses `concurrent.futures.ThreadPoolExecutor.map` from `concurrent.futures.ThreadPoolExecutor`. mpf(method='thread_pool')
- **3. jobutils_co.mpf.thread_pool** uses `concurrent.futures.ThreadPoolExecutor.map` from `concurrent.futures.ThreadPoolExecutor`. mpf(method='thread_pool')
- **4. jobutil5.mpf.thread_pool** uses `concurrent.futures.ThreadPoolExecutor.map` from `concurrent.futures.ThreadPoolExecutor`. mpf(method='thread_pool')
- **5. jobutil1.mpf_tpe** uses `concurrent.futures.ThreadPoolExecutor.submit` from `concurrent.futures.ThreadPoolExecutor`.

### Interpretation

- Top-5 primitive families: concurrent.futures.ThreadPoolExecutor x5.
- `hashlib` releases the GIL while hashing a block, so thread based pools can genuinely run in parallel here; they also avoid process creation, which is why thread based methods can beat process pools on this workload. Fork based pools are cheaper than spawn based pools.
- Times include pool creation and teardown (each run starts fresh). Larger files / more repeats will shift the balance toward processes.
- Reproduce: `python3 benchmark/run_benchmark.py /tmp/job_bench_fixture_ual_xd5v --repeats 5 --workers 2`

## Reference times

| Reference | Median (s) |
|-----------|-----------:|
| serial `hash_file` loop | 0.1702 |
| `stdlib.ThreadPoolExecutor.map` (`concurrent.futures.ThreadPoolExecutor.map`) | 0.0735 |
| `stdlib.ProcessPoolExecutor.map` (`concurrent.futures.ProcessPoolExecutor.map`) | 0.1069 |
| `stdlib.Pool.imap_unordered` (`multiprocessing.Pool.imap_unordered`) | 0.1292 |
| `jobutil3.mpf.serial` (`builtins.map (serial loop)`) | 0.1707 |
| `baseline.serial` (`builtins.map (serial reference)`) | 0.1721 |
| `jobutil6.Parallel.serial.map` (`builtins.map (serial loop)`) | 0.1871 |

## Full ranking (all successful parallel methods)

| # | Method | Function used | Median (s) | Min (s) | Speedup |
|---|--------|---------------|-----------:|--------:|--------:|
| 1 | `jobutil4.run_parallel.thread_pool` | `concurrent.futures.ThreadPoolExecutor.map` | 0.0707 | 0.0694 | 2.41x |
| 2 | `jobutil3.mpf.thread_pool` | `concurrent.futures.ThreadPoolExecutor.map` | 0.0713 | 0.0703 | 2.39x |
| 3 | `jobutils_co.mpf.thread_pool` | `concurrent.futures.ThreadPoolExecutor.map` | 0.0718 | 0.0696 | 2.37x |
| 4 | `jobutil5.mpf.thread_pool` | `concurrent.futures.ThreadPoolExecutor.map` | 0.0725 | 0.0703 | 2.35x |
| 5 | `jobutil1.mpf_tpe` | `concurrent.futures.ThreadPoolExecutor.submit` | 0.0729 | 0.0703 | 2.33x |
| 6 | `prunner.ParallelRunner.thread` | `concurrent.futures.ThreadPoolExecutor.submit` | 0.0731 | 0.0715 | 2.33x |
| 7 | `jobutil6.Parallel.thread.map` | `multiprocessing.dummy.Pool.map (threads)` | 0.0735 | 0.0695 | 2.31x |
| 8 | `job_helper.Parallel.thread` | `concurrent.futures.ThreadPoolExecutor.map (job_helper shim)` | 0.0738 | 0.0712 | 2.31x |
| 9 | `jobutil2.mpf_tpe` | `concurrent.futures.ThreadPoolExecutor.submit` | 0.0738 | 0.0721 | 2.31x |
| 10 | `jobutil6.Parallel.cf_thread.map` | `concurrent.futures.ThreadPoolExecutor.map` | 0.0742 | 0.0711 | 2.29x |
| 11 | `jobutils_co.mpf.process_pool` | `concurrent.futures.ProcessPoolExecutor.map` | 0.1000 | 0.0948 | 1.70x |
| 12 | `jobutil5.mpf.process_pool` | `concurrent.futures.ProcessPoolExecutor.map` | 0.1018 | 0.0994 | 1.67x |
| 13 | `jobutil3.mpf.process_pool` | `concurrent.futures.ProcessPoolExecutor.map` | 0.1020 | 0.0958 | 1.67x |
| 14 | `jobutil4.run_parallel.process_pool` | `concurrent.futures.ProcessPoolExecutor.map` | 0.1022 | 0.0976 | 1.67x |
| 15 | `jobutil2.mpf_joblib` | `joblib.Parallel + joblib.delayed` | 0.1023 | 0.1020 | 1.66x |
| 16 | `jobutil4.run_parallel.joblib` | `joblib.Parallel + joblib.delayed` | 0.1023 | 0.1022 | 1.66x |
| 17 | `jobutil3.mpf.joblib` | `joblib.Parallel + joblib.delayed` | 0.1023 | 0.1021 | 1.66x |
| 18 | `jobutil1.mpf3` | `concurrent.futures.ProcessPoolExecutor.submit + wait(FIRST_COMPLETED)` | 0.1024 | 0.1015 | 1.66x |
| 19 | `jobutil1.mpf_joblib` | `joblib.Parallel + joblib.delayed` | 0.1024 | 0.1022 | 1.66x |
| 20 | `jobutil1.mpf_ppe_ac` | `concurrent.futures.ProcessPoolExecutor.submit + as_completed` | 0.1025 | 0.1004 | 1.66x |
| 21 | `jobutils_co.mpf.joblib` | `joblib.Parallel + joblib.delayed` | 0.1025 | 0.1022 | 1.66x |
| 22 | `jobutil5.mpf.joblib` | `joblib.Parallel + joblib.delayed` | 0.1030 | 0.1023 | 1.65x |
| 23 | `jobutil2.mpf3` | `concurrent.futures.ProcessPoolExecutor.submit + wait(FIRST_COMPLETED)` | 0.1036 | 0.1027 | 1.64x |
| 24 | `jobutil1.mpf_ppe` | `concurrent.futures.ProcessPoolExecutor.submit` | 0.1047 | 0.0970 | 1.63x |
| 25 | `prunner.ParallelRunner.auto` | `concurrent.futures.ProcessPoolExecutor.submit (auto backend)` | 0.1053 | 0.0971 | 1.62x |
| 26 | `job_helper.Parallel.process` | `concurrent.futures.ProcessPoolExecutor.map (job_helper shim)` | 0.1065 | 0.1022 | 1.60x |
| 27 | `prunner.ParallelRunner.process` | `concurrent.futures.ProcessPoolExecutor.submit` | 0.1066 | 0.1021 | 1.60x |
| 28 | `jobutil2.mpf_ppe_ac_use_as_completed` | `concurrent.futures.ProcessPoolExecutor.submit + as_completed` | 0.1074 | 0.0997 | 1.58x |
| 29 | `jobutil2.mpf_ppe` | `concurrent.futures.ProcessPoolExecutor.submit` | 0.1090 | 0.1033 | 1.56x |
| 30 | `jobutil2.mpf_starmap` | `multiprocessing.Pool.starmap` | 0.1107 | 0.1056 | 1.54x |
| 31 | `jobutil2.mpf_ppe_ac` | `concurrent.futures.ProcessPoolExecutor.submit + as_completed` | 0.1137 | 0.1010 | 1.50x |
| 32 | `jobutil3.mpf.starmap` | `multiprocessing.Pool.starmap` | 0.1180 | 0.1105 | 1.44x |
| 33 | `jobutil5.mpf.imap_unordered` | `multiprocessing.Pool.imap_unordered` | 0.1193 | 0.1157 | 1.43x |
| 34 | `jobutil3.mpf.imap` | `multiprocessing.Pool.imap` | 0.1202 | 0.1132 | 1.42x |
| 35 | `jobutil3.mpf.map` | `multiprocessing.Pool.map` | 0.1204 | 0.1153 | 1.41x |
| 36 | `jobutil2.mpf_bounded` | `multiprocessing.Pool.apply_async (bounded in-flight)` | 0.1207 | 0.1187 | 1.41x |
| 37 | `jobutil4.run_parallel.imap` | `multiprocessing.Pool.imap` | 0.1214 | 0.1169 | 1.40x |
| 38 | `jobutil2.mpf2` | `multiprocessing.Pool.apply_async` | 0.1216 | 0.1178 | 1.40x |
| 39 | `jobutil5.mpf.apply_async` | `multiprocessing.Pool.apply_async` | 0.1219 | 0.1181 | 1.40x |
| 40 | `jobutil5.mpf.starmap` | `multiprocessing.Pool.starmap` | 0.1222 | 0.1213 | 1.39x |
| 41 | `jobutil4.run_parallel.map_async` | `multiprocessing.Pool.map_async` | 0.1223 | 0.1143 | 1.39x |
| 42 | `jobutil5.mpf.imap` | `multiprocessing.Pool.imap` | 0.1224 | 0.1151 | 1.39x |
| 43 | `jobutils_co.mpf.imap` | `multiprocessing.Pool.imap` | 0.1229 | 0.1219 | 1.38x |
| 44 | `jobutils_co.mpf.apply_async` | `multiprocessing.Pool.apply_async` | 0.1231 | 0.1177 | 1.38x |
| 45 | `jobutil4.run_parallel.imap_unordered` | `multiprocessing.Pool.imap_unordered` | 0.1235 | 0.1154 | 1.38x |
| 46 | `jobutil3.mpf.map_async` | `multiprocessing.Pool.map_async` | 0.1235 | 0.1099 | 1.38x |
| 47 | `jobutil5.mpf.map_async` | `multiprocessing.Pool.map_async` | 0.1243 | 0.1137 | 1.37x |
| 48 | `jobutils_co.mpf.imap_unordered` | `multiprocessing.Pool.imap_unordered` | 0.1245 | 0.1107 | 1.37x |
| 49 | `jobutil4.run_parallel.starmap_async` | `multiprocessing.Pool.starmap_async` | 0.1247 | 0.1158 | 1.36x |
| 50 | `jobutil2.mpf_as` | `multiprocessing.Pool.apply_async (bounded deque)` | 0.1248 | 0.1228 | 1.36x |
| 51 | `jobutil1.mpf2` | `multiprocessing.Pool.apply_async` | 0.1248 | 0.1171 | 1.36x |
| 52 | `jobutil5.mpf.map` | `multiprocessing.Pool.map` | 0.1250 | 0.1202 | 1.36x |
| 53 | `jobutil2.mpf_imap2` | `multiprocessing.Pool.imap` | 0.1251 | 0.1226 | 1.36x |
| 54 | `jobutil4.run_parallel.starmap` | `multiprocessing.Pool.starmap` | 0.1255 | 0.1234 | 1.36x |
| 55 | `jobutil1.mpf` | `multiprocessing.Pool.apply_async` | 0.1256 | 0.1210 | 1.36x |
| 56 | `jobutil3.mpf.starmap_async` | `multiprocessing.Pool.starmap_async` | 0.1258 | 0.1153 | 1.35x |
| 57 | `prunner.ParallelRunner.mp_imap_unordered` | `multiprocessing.Pool.imap_unordered` | 0.1259 | 0.1147 | 1.35x |
| 58 | `jobutil2.mpf_imapu` | `multiprocessing.Pool.imap_unordered` | 0.1260 | 0.1200 | 1.35x |
| 59 | `jobutil4.run_parallel.map` | `multiprocessing.Pool.map` | 0.1261 | 0.1207 | 1.35x |
| 60 | `jobutil3.mpf.imap_unordered` | `multiprocessing.Pool.imap_unordered` | 0.1263 | 0.1193 | 1.35x |
| 61 | `jobutil2.mpf_stream` | `multiprocessing.Pool.imap` | 0.1264 | 0.1175 | 1.35x |
| 62 | `jobutil3.mpf.apply_async` | `multiprocessing.Pool.apply_async` | 0.1265 | 0.1213 | 1.35x |
| 63 | `prunner.ParallelRunner.mp_imap` | `multiprocessing.Pool.imap` | 0.1268 | 0.1100 | 1.34x |
| 64 | `jobutils_co.mpf.map_async` | `multiprocessing.Pool.map_async` | 0.1273 | 0.1193 | 1.34x |
| 65 | `jobutils_co.mpf.starmap` | `multiprocessing.Pool.starmap` | 0.1275 | 0.1180 | 1.33x |
| 66 | `jobutil4.run_parallel.apply_async` | `multiprocessing.Pool.apply_async` | 0.1277 | 0.1146 | 1.33x |
| 67 | `jobutil5.mpf.starmap_async` | `multiprocessing.Pool.starmap_async` | 0.1286 | 0.1192 | 1.32x |
| 68 | `prunner.ParallelRunner.mp_apply_async` | `multiprocessing.Pool.apply_async` | 0.1294 | 0.1171 | 1.31x |
| 69 | `jobutils_co.mpf.starmap_async` | `multiprocessing.Pool.starmap_async` | 0.1297 | 0.1201 | 1.31x |
| 70 | `jobutils_co.mpf.map` | `multiprocessing.Pool.map` | 0.1299 | 0.1257 | 1.31x |
| 71 | `jobutil2.mpf_map` | `multiprocessing.Pool.map` | 0.1311 | 0.1181 | 1.30x |
| 72 | `jobutil2.mpf_async` | `multiprocessing.Pool.apply_async` | 0.1348 | 0.1294 | 1.26x |
| 73 | `jobutil3.mpf_iter.imap` | `multiprocessing.Pool.imap` | 0.1351 | 0.1250 | 1.26x |
| 74 | `jobutil2.mpf_imap` | `multiprocessing.Pool.imap` | 0.1360 | 0.1207 | 1.25x |
| 75 | `jobutil2.mpf` | `multiprocessing.Pool.apply_async` | 0.1398 | 0.1237 | 1.22x |
| 76 | `jobutil1.mpf_map` | `multiprocessing.Pool.map` | 0.1607 | 0.1567 | 1.06x |
| 77 | `jobutil6.Parallel.cf_process.map` | `concurrent.futures.ProcessPoolExecutor.map (spawn)` | 0.1617 | 0.1571 | 1.05x |
| 78 | `jobutil1.mpf_starmap` | `multiprocessing.Pool.starmap` | 0.1628 | 0.1534 | 1.05x |
| 79 | `jobutil6.Parallel.process.imap_unordered` | `multiprocessing.Pool.imap_unordered (spawn)` | 0.1646 | 0.1637 | 1.03x |
| 80 | `jobutil6.Parallel.process.starmap` | `multiprocessing.Pool.starmap (spawn)` | 0.1674 | 0.1624 | 1.02x |
| 81 | `jobutil6.Parallel.process.map` | `multiprocessing.Pool.map (spawn)` | 0.1733 | 0.1623 | 0.98x |
| 82 | `jobutil1.mpf_async` | `multiprocessing.Pool.apply_async` | 0.1736 | 0.1643 | 0.98x |
| 83 | `jobutil6.Parallel.cf_process.imap_unordered` | `concurrent.futures.ProcessPoolExecutor.submit + as_completed` | 0.1782 | 0.1754 | 0.95x |
| 84 | `jobutil1.mpf_imapu` | `multiprocessing.Pool.imap_unordered` | 0.1832 | 0.1780 | 0.93x |
| 85 | `jobutil1.mpf_imap` | `multiprocessing.Pool.imap_unordered` | 0.1841 | 0.1646 | 0.92x |
| 86 | `jobutil3.mpf.apply` | `multiprocessing.Pool.apply` | 0.2042 | 0.1946 | 0.83x |
| 87 | `jobutil6.Parallel.process.apply_async` | `multiprocessing.Pool.apply_async (spawn)` | 0.2060 | 0.1943 | 0.83x |
| 88 | `jobutil5.mpf.apply` | `multiprocessing.Pool.apply` | 0.2067 | 0.1991 | 0.82x |
| 89 | `jobutil4.run_parallel.apply` | `multiprocessing.Pool.apply` | 0.2106 | 0.1897 | 0.81x |
| 90 | `jobutils_co.mpf.apply` | `multiprocessing.Pool.apply` | 0.2107 | 0.1954 | 0.81x |

## Skipped methods

- `jobutil6.Parallel.cf_interpreter.map` (concurrent.futures.InterpreterPoolExecutor.map): optional: requires CPython >= 3.14 (running 3.11.2); InterpreterPoolExecutor was added in 3.14
- `prunner.ParallelRunner.interpreter` (concurrent.futures.InterpreterPoolExecutor.submit): optional: requires CPython >= 3.14 (running 3.11.2); InterpreterPoolExecutor was added in 3.14
- `stdlib.InterpreterPoolExecutor.map` (concurrent.futures.InterpreterPoolExecutor.map): optional: requires CPython >= 3.14 (running 3.11.2); InterpreterPoolExecutor was added in 3.14

## Failed methods

- none

## Notes

- `InterpreterPoolExecutor` is optional: methods that need it are skipped automatically on Python < 3.14 (`--interpreter force|off` to override).
- joblib based methods are skipped when joblib is not installed.
- Every measured run is verified against the serial digests; a method that returns wrong/missing hashes is reported as failed, not ranked.
- A warmup phase runs before timing so pool/import/page-cache costs are not charged to the first timed run only.
- Raw machine readable results: `benchmark_results.json`.

# Examples

Run from the repository root (Python 3.14 / Linux target; 3.11 works too):

```bash
python3 -m pip install joblib          # optional; examples degrade gracefully
python3 examples/example_usage.py
python3 examples/hash_folder_example.py /path/to/folder
python3 examples/hash_folder_example.py            # $HOME
python3 -m run_parallel                            # benchmark-derived advice
```

| file | shows |
|---|---|
| `example_usage.py` | all 7 `MpPoolRunner` methods (incl. star args and `iter_results`), all 3 `ExecutorRunner` kinds × `map`/`submit`, all `JoblibRunner` backends + `return_as="generator"`, `RunParallel`/`run_parallel`/`make_runner`, `recommend()` and the three error policies |
| `hash_folder_example.py` | the exact hashing workload from `benchmark/`, executed with the three classes plus the Termux lite helper, printing throughput and correctness |

Both scripts define their workers at **module level** and guard `main()` with
`if __name__ == "__main__":`. That is mandatory for `spawn` / `forkserver`
(the CPython 3.14 Linux default) and for `InterpreterPoolExecutor`: child
processes re-import the main module and pickle worker callables by name.

`example_usage.py` uses `start_method="fork"` for the mp.Pool demo purely to
keep it fast; production code on 3.14 should leave `start_method=None` to get
`forkserver`, or choose `"spawn"` deliberately.

Expected output highlights (2-core sandbox): the hashing example prints the
benchmark's family medians and shows `ThreadPoolExecutor.map` fastest, matching
the top-5 report in `benchmark/results/report.md`.

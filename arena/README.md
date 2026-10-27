# arena/ — helper scripts used during this session

Small, dependency-free scripts that were actually used while preparing the
multiprocessing benchmark. They are kept here (instead of deleted) as requested
by `prompt.md` so the work is reproducible. Run them from the repository root.

| script | what it does | how it was used |
|---|---|---|
| `check_imports.py` | `py_compile` + import every top-level repo module | found the pre-existing errors: `prunner.py` PEP 695 `type` alias syntax (needs 3.12+, repo runs 3.11), `jobutil6.py` `from .const import MAX_WORKERS` (no such module), plus verified the fixes |
| `make_fixture.py` | creates a deterministic, seeded tree of binary files in a temp dir | generates the reproducible hash workload used for the smoke test and the measured run |
| `smoke_methods.py` | runs every registered benchmark method once and verifies digests | pre-flight check for signature/pickling/result-shape problems before timing anything |
| `verify_results.py` | reads the results JSON, checks consistency, prints the top 5 with their primitives | validates the final `benchmark_results.json` |

Typical session:

```bash
python3 arena/check_imports.py
FIXTURE=$(python3 arena/make_fixture.py --files 24 --size 65536 | cut -f1)
python3 arena/smoke_methods.py "$FIXTURE" --methods jobutil1,jobutil2
python3 arena/smoke_methods.py "$FIXTURE"
python3 benchmark/run_benchmark.py "$FIXTURE" --repeats 3
python3 arena/verify_results.py
```

Generated fixture data is written to a temp directory outside the repo unless
`--dir` is passed, so no large test data is added to git.

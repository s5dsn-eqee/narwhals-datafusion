---
name: sync-narwhals
description: Bump the vendored narwhals submodule to a new release, regenerate the TESTS_THAT_NEED_FIX skip list, triage every newly failing narwhals test, and update the README version markers. Use after a narwhals release, when the weekly sync PR or a `compat` issue arrives, or when run_tests.py stops being green.
argument-hint: "[target] (e.g., \"v2.26.0\", \"latest\", or omit to re-triage the current submodule tag)"
---

# Sync the narwhals submodule

The backend targets narwhals' private `_sql` layer and the pin is a floor, so
every narwhals release reaches users at once. The `narwhals/` submodule is the
tag last tested; `run_tests.py` runs narwhals' suite with a curated skip list.
This skill keeps submodule, skip list and code consistent with the newest
release.

## 1. Bump the submodule

```bash
git -C narwhals fetch --tags
git -C narwhals checkout vX.Y.Z      # a v* tag, never main
uv sync --group tests --extra extra-functions
```

The weekly workflow (`sync-narwhals.yml`) does this and opens a PR. A large
`run_tests.py` diff or an import/attribute error in its CI means a
private hook moved: fix the code and cut a patch release, users are already on
the new narwhals. Scheduled workflows do not run on forks; use
`workflow_dispatch` or these steps.

## 2. Full unfiltered run

```bash
mkdir -p tmp    # gitignored
uv run --group tests python -m pytest narwhals/tests \
  -c narwhals/pyproject.toml -p narwhals_datafusion.testing -p env \
  --use-external-constructor --tb=short -q --color=no 2>&1 | tee tmp/full-run.txt
```

`-c narwhals/pyproject.toml` applies narwhals' pytest config (`TZ=UTC`,
warning filters); `narwhals_datafusion.testing` substitutes a DataFusion frame
for the `constructor` fixture and skips eager-only tests. Reference at narwhals
2.25.0 with pandas and polars installed:

| Run | Result |
|---|---|
| unfiltered | 151 failed, 2,853 passed, 943 skipped, 21 xfailed |
| unique failing names | 62 |
| `run_tests.py` | 2,782 passed, 902 skipped, 264 deselected, 20 xfailed |

`TESTS_THAT_NEED_FIX` holds names, so its length tracks the 62.

## 3. Regenerate the skip list

```bash
uv run --group tests python update_run_tests.py
git diff run_tests.py
```

The script parses `FAILED`/`ERROR` lines and rewrites `TESTS_THAT_NEED_FIX`;
`ALWAYS_DESELECTED` and `TESTS_NEED_EXTRA` it leaves alone. It refuses to write
when pytest reports failures but none were parsed; then the output format
changed and the regex needs updating. Never commit an emptied list.

## 4. Triage each new name

| Bucket | Evidence | Action |
|---|---|---|
| Known limitation | matches a README coverage caveat or "Known limitations" bullet | keep |
| New narwhals API | `AttributeError`/`not_implemented` on a method this backend lacks | implement, or add to the README unsupported column with the reason |
| Changed internal contract | failure inside `narwhals/_sql` or `_compliant` | fix to the new contract; read the DuckDB backend's diff for that release |
| Engine bug or gap | DataFusion error text | workaround plus `datafusion-workarounds` entry, or `NotImplementedError` |
| Order-dependent or CI-only | passes on rerun, asserts row order without sort, or asserts only under `CI=true` | `ALWAYS_DESELECTED` in `run_tests.py` with a comment; the regenerator leaves it alone |
| Needs the extra | passes with the shim, `NotImplementedError` naming the extra without it | `TESTS_NEED_EXTRA` in `run_tests.py`; deselected only when the shim is absent |

For each name that left the list, confirm it passes for the right reason and
move its README cell if a caveat no longer applies. One failure in full:

```bash
uv run --group tests python -m pytest narwhals/tests -c narwhals/pyproject.toml \
  -p narwhals_datafusion.testing -p env --use-external-constructor -k test_name -x
```

## 5. Verify and record

```bash
uvx pre-commit run --all-files
uv run --group tests pytest tests
uv run --group tests python run_tests.py    # must be green
```

Same commit: README Architecture version, coverage matrix header and moved
rows; the reference table in step 2. Commit message, e.g.
`bump narwhals to 2.26, retriage skip list`.

## Gotchas

- CI sets `PY_COLORS=1`; `update_run_tests.py` passes `--color=no` to parse.
- `pytest-randomly` is installed; a failure under one seed is order-dependent,
  confirm with `-p no:randomly`.
- A skip-list name matches every `constructor` parametrization.

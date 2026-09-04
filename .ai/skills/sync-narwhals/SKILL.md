---
name: sync-narwhals
description: Bump the vendored narwhals submodule to a new release, regenerate the TESTS_THAT_NEED_FIX skip list, triage every newly failing narwhals test into a known-limitation bucket or a bug, and move the pyproject pin. Use after a narwhals release, when the weekly auto-update PR arrives, or when run_tests.py stops being green.
argument-hint: "[target] (e.g., \"v2.26.0\", \"latest\", or omit to re-triage the current pin)"
---

# Sync the narwhals submodule

This backend targets narwhals' private `_sql` layer, so every narwhals
release can move the ground under it. The submodule at `narwhals/` is the
exact tag `pyproject.toml` pins, and `run_tests.py` runs narwhals' own suite
against this backend with a curated skip list. Keeping those three things
consistent is this skill.

## Step 1 — bump the submodule and the pin together

```bash
git -C narwhals fetch --tags
git -C narwhals checkout vX.Y.Z
git -C narwhals describe --tags          # confirm
```

Always check out a `v*` tag. `git submodule update --remote` tracks narwhals'
`main` branch, which drifts past the release the pin names; the weekly
workflow picks the newest tag for the same reason.

Then edit `pyproject.toml`: `narwhals>=X.Y,<X.(Y+1)`. The upper bound is one
minor above the lower bound on purpose; widen it only after the suite passes
on every release in the range. `uv sync --group tests` picks up the editable
submodule through `[tool.uv.sources]`.

The weekly GitHub workflow (`update_submodule_and_tests.yml`) does the
submodule bump and the skip-list regeneration but **not** the pin. A PR from
it that fails CI on an import error usually means the pin needs moving, not
that the code is wrong.

## Step 2 — run the full, unfiltered suite

```bash
uv run --group tests python -m pytest narwhals/tests \
  -c narwhals/pyproject.toml -p narwhals_datafusion.testing -p env \
  --use-external-constructor --tb=short -q --color=no 2>&1 | tee /tmp/full.txt
```

`-c narwhals/pyproject.toml` matters: it applies narwhals' own pytest config
(`TZ=UTC`, warning filters). `narwhals_datafusion.testing` is the plugin that
substitutes a DataFusion frame for the `constructor` fixture and skips
eager-only tests. Expect on the order of 1,400 passes and 100-plus failures;
the number is in `docs/backlog.md` if that file exists locally, else in the
last commit that touched `run_tests.py`.

## Step 3 — regenerate the skip list

```bash
uv run --group tests python update_run_tests.py
git diff run_tests.py
```

The script reruns the suite with `--color=no`, parses `FAILED`/`ERROR` lines,
and rewrites `TESTS_THAT_NEED_FIX`. It refuses to write when pytest reports
failures but none were parsed; if you see that message, the output format
changed and the regex in `update_run_tests.py` needs updating. Never commit an
emptied list.

## Step 4 — triage the diff, one name at a time

For each test that is **new** in the list, decide which it is:

| Bucket | Evidence | Action |
|---|---|---|
| Existing known limitation | matches a README "Known limitations" entry | keep in list, no other change |
| New narwhals API not yet implemented | `AttributeError`/`not_implemented` on a method this backend lacks | implement it, or add it to the README matrix's unsupported column with the reason |
| narwhals changed an internal contract | failure inside `narwhals/_sql` or `_compliant` calling this backend | fix the backend to the new contract; read the DuckDB backend's diff for the same release |
| Engine bug or missing feature | DataFusion error text | add a `datafusion-workarounds` entry if worked around, or a `NotImplementedError` with a clear message |
| Flaky / ordering | passes on rerun, or asserts row order without a sort | check whether the test is wrong for lazy backends; upstream tests usually sort first |

For each test that **left** the list, confirm it passes for the right reason
and move its method in the README matrix if a caveat no longer applies.

A convenient way to see one failure in full:

```bash
uv run --group tests python -m pytest narwhals/tests -c narwhals/pyproject.toml \
  -p narwhals_datafusion.testing -p env --use-external-constructor -k test_name -x
```

## Step 5 — verify and record

```bash
uvx ruff check .
uv run --group tests pytest tests
uv run --group tests python run_tests.py    # must be green
```

Update in the same commit:

- `pyproject.toml` pin and the "targets narwhals X.Y internals" comment.
- `README.md` header of the coverage matrix (`narwhals==X.Y`) and any rows
  that moved.
- `docs/backlog.md` failure counts and buckets, if that local file is in use.

Commit message stays one line, e.g. `bump narwhals to 2.26, retriage skip list`.

## Gotchas

- `PY_COLORS=1` is set in CI. The parser in `update_run_tests.py` passes
  `--color=no` to defeat it; keep that flag if you edit the command.
- `pytest-randomly` is in the test group. A failure that only appears under
  one seed is order-dependent; rerun with `-p no:randomly` to confirm.
- The narwhals suite parametrizes on `constructor`; a test name in the skip
  list matches every parametrization, which is what you want.

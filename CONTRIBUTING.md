# Contributing

Clone this repository with the `--recursive` flag so the narwhals submodule
comes along.

```console
git clone git@github.com:s5dsn-eqee/narwhals-datafusion.git --recursive
cd narwhals-datafusion
```

If you already cloned without it:

```console
git submodule update --init --recursive
```

Create the environment. `uv sync` builds a virtual environment, installs the
package in editable mode, and installs narwhals editable from the submodule
(configured in `pyproject.toml` under `[tool.uv.sources]`).

```console
uv sync --group tests
```

## Running checks

Lint (this is exactly what CI runs; formatting is not enforced):

```console
uvx ruff check .
```

This package's own tests:

```console
uv run --group tests pytest tests
```

Narwhals' full test suite against this backend, with known failures deselected:

```console
uv run --group tests python run_tests.py
```

Any additional arguments are passed down to pytest. Avoid `-k`, which would
replace the script's own `-k` deselect expression; narrow by path instead:

```console
uv run --group tests python run_tests.py -x narwhals/tests/frame/join_test.py
```

All three must pass before you open a pull request.

## Adding or fixing functionality

1. Read [`.ai/skills/datafusion-workarounds/SKILL.md`](.ai/skills/datafusion-workarounds/SKILL.md)
   first. DataFusion diverges from the SQL semantics narwhals expects in a
   fixed set of ways, and each already has a settled workaround to reuse.
2. Add a regression test to `tests/test_smoke.py`.
3. Regenerate the deselect list so newly passing narwhals tests are picked up:

   ```console
   uv run --group tests python update_run_tests.py
   ```

   Review the diff to `run_tests.py`. Tests that pass locally but fail only in
   CI belong in `ALWAYS_DESELECTED`, which the regenerator leaves alone.
4. Move the affected method in the README's API coverage table and adjust the
   "Known limitations" list in the same commit.

## Updating the narwhals submodule

The submodule is pinned to a release tag, never to `main`, and
`pyproject.toml` pins narwhals to the same minor. Bump both together:

```console
git -C narwhals fetch --tags
git -C narwhals checkout vX.Y.Z
# edit pyproject.toml: narwhals>=X.Y,<X.(Y+1)
uv sync --group tests
uv run --group tests python update_run_tests.py
```

Then run the checks, triage anything new in `run_tests.py`, and open a PR.
[`.ai/skills/sync-narwhals/SKILL.md`](.ai/skills/sync-narwhals/SKILL.md)
walks through the triage. A weekly GitHub Actions job does the tag bump and
regeneration automatically and opens a PR when something changed.

## Commits and pull requests

- One-line commit messages, roughly ten words, imperative mood, no body.
- CI runs lint plus both test suites on Python 3.10, 3.12 and 3.13.
- Releases are cut by pushing a `v*` tag; see
  [`.ai/skills/release/SKILL.md`](.ai/skills/release/SKILL.md) before
  touching any of the version pins.

AI coding assistants working on this repo should read [`AGENTS.md`](AGENTS.md).

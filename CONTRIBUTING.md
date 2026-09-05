# Contributing

## Setup

```console
git clone --recursive https://github.com/s5dsn-eqee/narwhals-datafusion.git
cd narwhals-datafusion
uv sync --group tests --extra extra-functions
uvx pre-commit install
```

`uv sync` installs the package editable and narwhals editable from the
`narwhals/` submodule (`[tool.uv.sources]`). The extra is the shim behind
`mode`/`skew`/`kurtosis`; the narwhals suite expects it. Already cloned without
`--recursive`: `git submodule update --init --recursive`.

## Checks

All four must pass before a pull request. CI runs the same commands.

```console
uvx pre-commit run --all-files              # ruff format + check, codespell, typos
uv run --group typing pyright               # package and test_plugin_protocol.py
uv run --group tests pytest tests           # this package's tests
uv run --group tests python run_tests.py    # narwhals' suite, known failures deselected
```

`test_plugin_protocol.py` is a static assertion that the package satisfies
narwhals' `Plugin` protocol; pyright runs it. `run_tests.py` passes extra
arguments to pytest; a path narrows the run, `-k` is combined with the skip
list.

## Adding or fixing functionality

1. Read [`.ai/skills/datafusion-workarounds/SKILL.md`](.ai/skills/datafusion-workarounds/SKILL.md).
2. Add a regression test to `tests/test_smoke.py`.
3. `uv run --group tests python update_run_tests.py` and review the diff to
   `run_tests.py`. Tests that pass locally but fail only in CI go in
   `ALWAYS_DESELECTED`; tests that need the extra go in `TESTS_NEED_EXTRA`.
4. Update the README coverage table in the same commit.

## Updating the narwhals submodule

```console
git -C narwhals fetch --tags
git -C narwhals checkout vX.Y.Z              # a release tag, never main
uv sync --group tests --extra extra-functions
uv run --group tests python update_run_tests.py
```

Triage per [`.ai/skills/sync-narwhals/SKILL.md`](.ai/skills/sync-narwhals/SKILL.md).
`sync-narwhals.yml` does this weekly and opens a PR; `compat.yml` runs both
suites on the newest narwhals release.

## Commits and releases

- One-line commit messages, about ten words, imperative, no body.
- Releases: push a `v*` tag; see
  [`.ai/skills/release/SKILL.md`](.ai/skills/release/SKILL.md) before moving a
  version pin.

Agents: read [`AGENTS.md`](AGENTS.md).

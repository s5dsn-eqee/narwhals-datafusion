# Agent Instructions for Contributors

This file is for agents working **on** narwhals-datafusion (developing,
testing, reviewing, releasing). To **use** the package, read the
[README](README.md): it has the usage snippet, the API coverage matrix and the
known limitations.

## What this project is

A [narwhals](https://github.com/narwhals-dev/narwhals) backend plugin for
[Apache DataFusion](https://github.com/apache/datafusion-python), registered
through the `narwhals.plugins` entry point. It is a thin subclass of narwhals'
private SQL layer (`narwhals._sql`, `narwhals._compliant`), the same shape as
the built-in DuckDB backend and the `narwhals-daft` plugin. Every file in
`src/narwhals_datafusion/` overrides only what DataFusion does differently.

Because it targets private narwhals internals, `pyproject.toml` pins narwhals
to one minor release and the exact tag is vendored as the `narwhals/` git
submodule. Develop and test against that submodule, never against a
site-packages narwhals.

## Skills

Task-specific instructions live in `.ai/skills/`, one directory per skill with
a `SKILL.md` (YAML frontmatter: `name`, `description`, `argument-hint`, then
the instructions). `.claude/skills` is a symlink to that directory so Claude
Code discovers them; other agents should list `.ai/skills/` and read each
`SKILL.md`.

Descriptions beginning with `TRIGGER —` are conventions to read *before*
writing code that meets the stated condition, not tasks to run on request.

| Skill | Use it when |
|---|---|
| [`datafusion-workarounds`](.ai/skills/datafusion-workarounds/SKILL.md) | TRIGGER: before touching any expression, window, join, or dtype code |
| [`sync-narwhals`](.ai/skills/sync-narwhals/SKILL.md) | bumping the narwhals submodule or regenerating the skip list |
| [`check-coverage`](.ai/skills/check-coverage/SKILL.md) | auditing which narwhals APIs this backend supports and keeping the README matrix honest |
| [`release`](.ai/skills/release/SKILL.md) | cutting a version, moving the datafusion pin, or re-pinning the FFI shim |

## Setup and checks

```sh
git submodule update --init          # narwhals at the pinned tag
uv sync --group tests
uvx ruff check .                     # CI runs exactly this; formatting is not enforced
uv run --group tests pytest tests    # this package's own suite (fast)
uv run --group tests python run_tests.py   # narwhals' suite, known failures deselected
```

All three checks must pass before a commit. The full unfiltered narwhals run
and how to interpret it are described in the `sync-narwhals` skill.

## Conventions

- **Unsupported operations** are declared, not discovered at runtime. A
  method DataFusion cannot express at all is `not_implemented()` at class
  level. A method that works only for some arguments raises
  `NotImplementedError` with a message naming the backend and the argument.
  Never return a wrong result silently; the `n_unique` window path is the
  model for refusing.
- **Every DataFusion workaround carries a comment** saying which engine
  behaviour it works around. The `datafusion-workarounds` skill is the index
  of those; add to it when you add a new one.
- **The README coverage matrix is part of the change.** Moving a method
  between the supported, partial and unsupported columns, or changing a
  caveat, happens in the same commit as the code.
- **Pins move in lockstep.** `datafusion`, `datafusion-extra-functions-ffi`
  and the narwhals submodule each have a reason for their bound; see the
  `release` skill before loosening any of them.
- **Commit messages are one line**, roughly ten words, imperative, no body
  and no trailers.

## Layout

```
src/narwhals_datafusion/
  __init__.py       entry point: __narwhals_namespace__, is_native, __version__
  namespace.py      DataFusionNamespace: lit, len, concat, horizontal ops, IO
  dataframe.py      DataFusionLazyFrame: select/join/unique/explode/unpivot/...
  expr.py           DataFusionExpr: operators, aggregates, windows, casts
  expr_{str,dt,list,struct}.py   sub-namespaces
  utils.py          col() quoting, FUNCTION_REMAP, window_expression, errors
  extra_functions.py  mode/skew/kurtosis via the FFI shim wheel
  testing.py        pytest plugin that feeds narwhals' suite a DataFusion frame
tests/              this package's own smoke and regression tests
run_tests.py        curated narwhals-suite runner (TESTS_THAT_NEED_FIX)
update_run_tests.py regenerates TESTS_THAT_NEED_FIX from a full run
narwhals/           git submodule, pinned narwhals release
```

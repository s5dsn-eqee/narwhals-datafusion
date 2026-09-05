# Agent Instructions

For agents working on narwhals-datafusion. Usage and API coverage are in the
[README](README.md); the human workflow is in [CONTRIBUTING.md](CONTRIBUTING.md).

## What this is

A [narwhals](https://github.com/narwhals-dev/narwhals) backend plugin for
[Apache DataFusion](https://github.com/apache/datafusion-python): a thin
subclass of narwhals' private SQL layer (`narwhals._sql`, `narwhals._compliant`)
that overrides only what DataFusion does differently. The release last tested
against is the `narwhals/` submodule; develop and test against it, never
against a site-packages narwhals.

## Skills

`.ai/skills/<name>/SKILL.md`, YAML frontmatter (`name`, `description`,
`argument-hint`) then instructions. `.claude/skills` is a symlink to that
directory; on a Windows checkout without `core.symlinks` it is a text file,
read `.ai/skills/` directly. A description starting with `TRIGGER —` is a
convention to read before writing the code it names.

| Skill | When |
|---|---|
| [`datafusion-workarounds`](.ai/skills/datafusion-workarounds/SKILL.md) | TRIGGER: before touching expression, window, join, cast or dtype code |
| [`sync-narwhals`](.ai/skills/sync-narwhals/SKILL.md) | bumping the narwhals submodule or regenerating the skip list |
| [`check-coverage`](.ai/skills/check-coverage/SKILL.md) | auditing API support against the README matrix |
| [`release`](.ai/skills/release/SKILL.md) | cutting a version, moving a pin, a new datafusion major or shim |

Setup and the four checks: [CONTRIBUTING.md](CONTRIBUTING.md). All four pass
before a commit.

## Conventions

- **Unsupported operations are declared.** A method DataFusion cannot express
  is `not_implemented()` at class level; one that works for some arguments
  raises `NotImplementedError` naming the backend and the argument. Never
  return a wrong result silently (`n_unique` over a window is the model).
- **Every workaround carries a one-line comment** naming the engine behaviour,
  and an entry in the `datafusion-workarounds` skill.
- **The README matrix changes in the same commit** as the code that moves a
  method or a caveat.
- **Pins.** `narwhals>=2.25`: floor only; a break is a code fix and a patch
  release. `datafusion>=54,<55`: capped at the tested major; moves after both
  suites pass on the next one. The shim pins its own datafusion major.
- **Prose is minimal.** A comment, doc line or skill sentence exists only if a
  reader needs it to act; one reason per rule, no justification paragraphs, no
  comparisons to other projects, no judgement of upstream.
- **Commit messages**: one line, about ten words, imperative, no body, no
  trailers.

## Layout

```
src/narwhals_datafusion/
  __init__.py       entry point: __narwhals_namespace__, is_native, __version__
  namespace.py      DataFusionNamespace: lit, len, concat, horizontal ops, IO
  dataframe.py      DataFusionLazyFrame: select/join/unique/explode/unpivot/...
  expr.py           DataFusionExpr: operators, aggregates, windows, casts
  expr_{str,dt,list,struct}.py   sub-namespaces
  utils.py          col() quoting, FUNCTION_REMAP, window_expression, errors
  extra_functions.py  mode/skew/kurtosis via the FFI shim (optional extra)
  testing.py        pytest plugin feeding narwhals' suite a DataFusion frame
tests/              this package's tests
run_tests.py        narwhals-suite runner with TESTS_THAT_NEED_FIX
update_run_tests.py regenerates TESTS_THAT_NEED_FIX
narwhals/           submodule, tested narwhals release
```

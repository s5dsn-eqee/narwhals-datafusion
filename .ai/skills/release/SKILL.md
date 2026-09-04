---
name: release
description: Cut a narwhals-datafusion release to PyPI, or move one of the three coupled version pins (datafusion, datafusion-extra-functions-ffi, narwhals). Explains which pin constrains which, what the tag-push workflow does, and the checks that must be green first. Use for "release", "bump datafusion", "re-pin the ffi shim", or "publish".
argument-hint: "[what] (e.g., \"0.2.0\", \"datafusion 55\", \"ffi shim\", or omit for the checklist)"
---

# Release and pins

## The three pins and why each exists

| Pin (`pyproject.toml`) | Bound by | Loosen only when |
|---|---|---|
| `narwhals>=X.Y,<X.(Y+1)` | private `narwhals._sql` internals this backend subclasses | the `sync-narwhals` skill has been run against every release in the wider range |
| `datafusion>=54,<55` | (a) the FFI shim wheel is compiled against `datafusion-ffi` 54's ABI; (b) arithmetic over two aggregates inside `aggregate()` needs 54 or newer | the shim has been rebuilt against the new major, see below |
| `datafusion-extra-functions-ffi>=0.1,<0.2` | each shim minor tracks one datafusion major | a new shim minor exists for the new datafusion major |

The datafusion and shim pins **must move together**. An aggregate UDF capsule
built for one `datafusion-ffi` major loaded into another is undefined
behaviour, not an error: datafusion-python's version check on imported
capsules covers table providers, codecs and query planners, but not
aggregate UDFs.

## Moving to a new datafusion major (e.g. 55)

1. In the shim repo (`s5dsn-eqee/datafusion-extra-functions-ffi`):
   `Cargo.toml` bumps `datafusion-expr`, `datafusion-ffi` and re-pins the
   `datafusion-extra-functions` git rev to one that targets the same major
   (upstream `main` tends to run ahead; find the last compatible commit).
   The `__datafusion_aggregate_udf__` getter signature has not changed
   through datafusion-python 55; check `docs/source/user-guide/upgrade-guides.md`
   upstream for the release you are moving to before assuming that holds.
   Release the shim as the next minor.
2. Here: bump both pins, `uv lock`, run the full checklist below, and run
   `check-coverage` since a new engine major often unblocks a
   `not_implemented()` (the README "Known limitations" heading carries the
   engine version).
3. Watch for the `mode` GROUP BY xfail in `tests/test_extra_functions.py`: it
   is `strict=True`, so if the upstream crate fixed it the test fails as
   XPASS and the marker must come off.

## Cutting a release

Version lives in exactly one place: `__version__` in
`src/narwhals_datafusion/__init__.py`. `pyproject.toml` reads it via hatch's
dynamic version, and `uv.lock` deliberately carries no version for this
package.

```bash
# 1. checks
uvx pre-commit run --all-files
uv run --group tests pytest tests
uv run --group tests python run_tests.py
uv build && uvx twine check dist/*

# 2. bump and tag
#    edit __version__ -> "0.2.0"
git commit -am "release 0.2.0"
git tag v0.2.0
git push origin main v0.2.0
```

Pushing a `v*` tag runs `.github/workflows/release.yml`: a test job (both
suites) gates a build job, which uploads the sdist and wheel; a publish job
pushes to PyPI via trusted publishing (OIDC, no secrets); a final job signs
with Sigstore and creates the GitHub release with generated notes. If the test
job fails nothing is published; fix, delete the tag locally and remotely, and
re-tag.

## After publishing

- Install from PyPI in a fresh venv and run a one-liner that exercises
  `mode` or `skew`, which proves the shim wheel resolved for that platform.
- Badges in the README are proxied by GitHub's Camo cache. If a badge shows
  stale or empty right after a release, purge it with `curl -X PURGE <camo url>`
  or wait a day.
- Pushes from this machine must use the GitHub noreply author email; the
  account blocks pushes that expose the private address.

## sdist contents

`[tool.hatch.build.targets.sdist].exclude` keeps the submodule, tests,
workflows, docs and lock file out of the sdist. Add any new top-level
development file to that list.

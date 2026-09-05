---
name: release
description: Cut a narwhals-datafusion release to PyPI, or touch one of the three dependency floors (datafusion, the datafusion-extra-functions-ffi extra, narwhals). Explains why each is a floor with no ceiling, what guards newer releases, what the tag-push workflow does, and the checks that must be green first. Use for "release", "datafusion 55 shipped", "rebuild the ffi shim", or "publish".
argument-hint: "[what] (e.g., \"0.2.0\", \"datafusion 55\", \"ffi shim\", or omit for the checklist)"
---

# Release and pins

Who can do what: anyone can prepare a release PR (version bump, checklist
green). Pushing to `main` and pushing a `v*` tag need write access to this
repository; PyPI trusted publishing is bound to this repository's
`release.yml`, so a fork cannot publish. The shim lives in a separate
repository; if you do not have write access there, open an issue in it and
finish the steps here that do not depend on the shim.

## The three pins and why each exists

All three are floors, daft-style (`narwhals-daft` ships `daft>=0.5.18,
narwhals>=2.10.0`). What was actually tested is recorded elsewhere: the
`narwhals/` submodule tag and `uv.lock`.

| Pin (`pyproject.toml`) | Why the floor | What guards newer releases |
|---|---|---|
| `narwhals>=2.25` | first release whose private `_sql` layer this code was written against | weekly workflow: submodule bumped to the newest tag, suite rerun, PR opened |
| `datafusion>=54` | arithmetic over two aggregates inside `aggregate()` fails on 53 | weekly workflow `latest-datafusion` job: both suites on the newest release, without the extra |
| `datafusion-extra-functions-ffi>=0.1` (the `extra-functions` optional extra) | first shim release | the shim's own `datafusion>=N,<N+1` pin per shim minor; the resolver pairs shim and engine |

Newer narwhals or datafusion can break this backend at **runtime**, not at
resolve time, since the private-API and Python-API changes are invisible to a
resolver. That is the accepted trade for not conflicting with whatever narwhals
a user already has installed. The response to a break is a code fix and a
patch release, never a ceiling.

The shim's own pin is load-bearing: an aggregate UDF capsule built for one
`datafusion-ffi` major loaded into another is undefined behaviour, not an
error, because datafusion-python's version check on imported capsules covers
table providers, codecs and query planners, but not aggregate UDFs. Never loosen
the pin in the shim repo, and keep one shim minor per datafusion major.

## When a new datafusion major ships (e.g. 55)

Users get it as soon as it resolves. The `extra-functions` extra keeps
resolving to the old major until a matching shim minor exists (the resolver
backtracks datafusion to satisfy the shim's pin), so `pip install
narwhals-datafusion[extra-functions]` stays on 54 while a bare install moves to
55 with `mode`/`skew`/`kurtosis` raising `NotImplementedError`.

1. In the shim repo (`s5dsn-eqee/datafusion-extra-functions-ffi`):
   `Cargo.toml` bumps `datafusion-expr`, `datafusion-ffi` and re-pins the
   `datafusion-extra-functions` git rev to one that targets the same major
   (upstream `main` tends to run ahead; find the last compatible commit).
   The `__datafusion_aggregate_udf__` getter signature has not changed
   through datafusion-python 55; check `docs/source/user-guide/upgrade-guides.md`
   upstream for the release you are moving to before assuming that holds.
   Release the shim as the next minor.
2. Here: `uv lock --upgrade-package datafusion` (and the shim once its minor
   exists) so the lockfile records the new tested version, run the full
   checklist below, and run `check-coverage` since a new engine major often
   unblocks a `not_implemented()` (the README "Known limitations" heading
   carries the engine version). If the `latest-datafusion` weekly job already
   went red, its log is the list of what to fix.
3. Watch for the `mode` GROUP BY xfail in `tests/test_extra_functions.py`: it
   is `strict=True`, so if the upstream crate fixed it the test fails as
   XPASS and the marker must come off.

## Cutting a release

Version lives in exactly one place: `__version__` in
`src/narwhals_datafusion/__init__.py`. `pyproject.toml` reads it via hatch's
dynamic version, and `uv.lock` deliberately carries no version for this
package.

```bash
# 1. checks (CI also runs `pytest tests` without the extra)
uv sync --group tests --extra extra-functions
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

**PEP 740 attestations** are generated and uploaded by the publish action
automatically, but only while the publish job keeps both `id-token: write`
and `environment: pypi`. Removing either silently drops them; the README's
"PEP 740 attested" badge would then be a lie. Verify after each release:

```bash
curl -s https://pypi.org/integrity/narwhals-datafusion/<version>/<wheel filename>/provenance \
  | jq '.attestation_bundles | length'     # expect 1
```

## After publishing

- Install `narwhals-datafusion[extra-functions]` from PyPI in a fresh venv
  and run a one-liner that exercises `mode` or `skew`, which proves the shim
  wheel resolved for that platform. Then install the bare package in another
  venv and check `mode` raises `NotImplementedError` naming the extra.
- Confirm the PyPI project page shows "Trusted publishing, provides
  attestations" under the new files (see the curl check above).
- Badges in the README are proxied by GitHub's Camo cache. If a badge shows
  stale or empty right after a release, purge it with `curl -X PURGE <camo url>`
  or wait a day.

## sdist contents

`[tool.hatch.build.targets.sdist].exclude` keeps the submodule, tests,
workflows, docs and lock file out of the sdist. Add any new top-level
development file to that list.

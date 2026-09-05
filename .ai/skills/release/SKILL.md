---
name: release
description: Cut a narwhals-datafusion release to PyPI, or move a dependency pin (datafusion cap, narwhals floor, the FFI shim extra). Use for "release", "publish", "datafusion 55 shipped", "rebuild the ffi shim".
argument-hint: "[what] (e.g., \"0.2.0\", \"datafusion 55\", \"ffi shim\", or omit for the checklist)"
---

# Release and pins

Pushing the `v*` tag and publishing need write access to this repository; a
fork cannot publish. The shim is a separate repository; without access there,
open an issue.

## Pins

| Pin | Policy |
|---|---|
| `narwhals>=2.25` | Floor only. A newer narwhals can break the private `_sql` layer at runtime; the `compat` workflow catches it, the fix is code plus a patch release. |
| `datafusion>=54,<55` | Capped at the tested major; moves after both suites pass on the next one. Floor: aggregate arithmetic inside `aggregate()` fails on 53. |
| `datafusion-extra-functions-ffi>=0.1` (extra) | Unbounded; each shim minor pins its own datafusion major. |

The shim's pin is what prevents loading an aggregate UDF capsule into a
different `datafusion-ffi` major, which is undefined behaviour, not an error
(datafusion-python's capsule version check does not cover aggregate UDFs as
of 54). One shim minor per datafusion major.

## New datafusion major (e.g. 55)

Until the cap moves nobody installs 55. With the cap at `<56` and no shim 0.2,
the extra resolves to 54 and a bare install gets 55 with `mode`/`skew`/
`kurtosis` raising `NotImplementedError`.

1. Shim repo (`s5dsn-eqee/datafusion-extra-functions-ffi`): bump
   `datafusion-expr` and `datafusion-ffi` in `Cargo.toml`, re-pin the
   `datafusion-extra-functions` git rev to a commit on the same major, check
   upstream `docs/source/user-guide/upgrade-guides.md` for changes to the
   `__datafusion_aggregate_udf__` getter, release the next shim minor.
2. Here: cap to `<56`, `uv lock --upgrade-package datafusion` (and the shim
   once released), run the checklist, fix what changed, run `check-coverage`,
   update the engine version in the README and the `datafusion-workarounds`
   skill.
3. The `mode` GROUP BY xfail in `tests/test_extra_functions.py` is strict; if
   the new crate fixed it, remove the marker.

## Cutting a release

`__version__` in `src/narwhals_datafusion/__init__.py` is the only version
source (hatch dynamic version; `uv.lock` carries none for this package).

```bash
uv sync --group tests --extra extra-functions
uvx pre-commit run --all-files
uv run --group tests pytest tests
uv run --group tests python run_tests.py
uv build && uvx twine check dist/*

# edit __version__, then
git commit -am "release 0.2.0"
git tag v0.2.0
git push origin main v0.2.0
```

The tag runs `.github/workflows/release.yml`: test (both suites) gates build,
publish (trusted publishing, OIDC), then Sigstore signing and a GitHub release.
A failed test job publishes nothing: fix, delete the tag locally and remotely,
re-tag.

PEP 740 attestations require the publish job to keep both `id-token: write`
and `environment: pypi`; removing either drops them silently and the README
badge lies. Verify:

```bash
curl -s https://pypi.org/integrity/narwhals-datafusion/<version>/<wheel filename>/provenance \
  | jq '.attestation_bundles | length'     # expect 1
```

## After publishing

- Fresh venv, `pip install "narwhals-datafusion[extra-functions]==<version>"`,
  run `mode`: proves the shim wheel resolves. Second venv, bare install: `mode`
  raises `NotImplementedError` naming the extra.
- Stale README badges: GitHub's Camo cache; `curl -X PURGE <camo url>` or wait
  a day.

## sdist

`[tool.hatch.build.targets.sdist].exclude` lists every top-level development
file; add new ones there.

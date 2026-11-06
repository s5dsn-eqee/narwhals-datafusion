---
name: release
description: Cut a narwhals-datafusion release to PyPI, or move a dependency pin (datafusion cap, narwhals floor, the extra-functions extra). Use for "release", "publish", "datafusion 56 shipped".
argument-hint: "[what] (e.g., \"0.2.0\", \"datafusion 56\", or omit for the checklist)"
---

# Release and pins

Pushing the `v*` tag and publishing need write access to this repository; a
fork cannot publish.

## Pins

| Pin | Policy |
|---|---|
| `narwhals>=2.27` | Floor only. A newer narwhals can break the private `_sql` layer at runtime; the `compat` workflow catches it, the fix is code plus a patch release. |
| `datafusion>=55,<56` | Capped at the tested major; moves after both suites pass on the next one. Floor: `array_avg`/`array_sum` and `datafusion-extra-functions` need 55. |
| `datafusion-extra-functions>=0.5.5` (extra) | Unbounded; each upstream release pins its own datafusion major. |

That package's pin is what prevents loading an aggregate UDF capsule into a
different `datafusion-ffi` major, which is undefined behaviour, not an error
(datafusion-python checks only the capsule name for aggregate UDFs as of 55).

## New datafusion major (e.g. 56)

1. Wait for a `datafusion-extra-functions` release on PyPI that pins the new
   major; until then the extra resolves the old one.
2. Move the cap; move the floor too if the code starts using the new major's
   functions. `uv lock --upgrade-package datafusion --upgrade-package
   datafusion-extra-functions`, run the checklist, fix what changed,
   `update_run_tests.py`, run `check-coverage`, update the engine version in
   `docs/api-coverage.md` and the `datafusion-workarounds` skill and the pins
   in the README and `AGENTS.md`.
3. Re-probe the gaps the workarounds skill names (`array_median`, integer
   `array_sum`, `mode` input types and nulls).

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
  run `mode`: proves the extra's wheel resolves. Second venv, bare install: `mode`
  raises `NotImplementedError` naming the extra.
- Stale README badges: GitHub's Camo cache; `curl -X PURGE <camo url>` or wait
  a day.

## sdist

`[tool.hatch.build.targets.sdist].exclude` lists every top-level development
file; add new ones there.

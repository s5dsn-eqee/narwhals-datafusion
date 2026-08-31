# Backlog

Status as of 2026-08-31. Built against narwhals 2.25 internals + datafusion 54;
pinned `narwhals>=2.25,<3`.

**Test standing**

- Own suite (`pytest tests/`): 32 passed, 1 xfailed (upstream `mode` group-by bug, see below).
- Narwhals full suite via `pytest tests/ --use-external-constructor -p narwhals_datafusion.testing`
  (run from a narwhals checkout): 2,804 / 3,972 passed — 200 failed, 947 skipped
  (lazy-only), 21 xfailed. Effective pass rate ~90.5%. Core DataFrame operations
  (select, filter, join, group_by, sort, aggregate) and the expression system
  all pass.

## Release / process

- [x] Set up a standalone venv for this repo (`uv sync`) and rebuild the FFI
      wheel here (now a uv path source: `uv sync --group ffi`).
- [ ] Add CI: own tests + narwhals suite via `--use-external-constructor`.
- [ ] Publish `narwhals-datafusion` to PyPI.
- [ ] Post the design + working plugin on narwhals-dev/narwhals#3225; ask about
      in-tree adoption (docs/design.md phase 4).
- [ ] Clean up the narwhals clone: delete stale `packages/narwhals-datafusion/`,
      revert its `uv.lock` change.
- [ ] File the `mode` state-schema bug upstream at
      datafusion-contrib/datafusion-extra-functions (details below).

## Narwhals-suite failure categories (200 failures)

All represent documented limitations or advanced features, not core defects.

### Datetime operations — 57 failures (largest bucket)

- [ ] `dt.offset_by` — not implemented (15).
- [ ] `dt.timestamp` — format/timezone edge cases (15).
- [ ] `dt.truncate` with non-unit multiples (e.g. `"2mo"`, `"3h"`) (9).
- [ ] Duration attributes (`total_minutes` etc.) (10).

### Aggregates without engine support — 15 failures

- [x] `skew`, `kurtosis`, `mode` — now available via the optional
      `extra-functions-ffi` wheel; without it they raise `NotImplementedError`.
      Narwhals-suite runs without the wheel still count these as failures
      (skew 5, quantile 5, kurtosis 5).
- [ ] Exact `quantile` — engine only has `approx_percentile_cont`; shipping
      approximate semantics needs maintainer sign-off.

### String operations — 8 failures

- [ ] `str.replace` with `n=`/count parameter (4).
- [ ] `str.to_datetime` format inference (3).

### Expression features — 8 failures

- [ ] `is_close` (4).
- [ ] `mode` with `keep="all"` — backend supports `keep="any"` only; the FFI
      crate returns a single mode value (2).
- [ ] `median` on strings (2).
- [ ] `cum_prod` — DataFusion `Expr` lacks a product window path (2).

### Enum / categorical — 7 failures

- [ ] Enum casting and categorical selectors — DataFusion rejects dictionary
      types in this path; candidates for `UNSUPPORTED_DTYPES` messaging.

### I/O — 10 failures

- [ ] `scan_csv` / `scan_parquet` parameter variants (Path vs str vs PathLike,
      option pass-through).

### Joins — 6 failures

- [ ] `join_asof` (3 variants) — no ASOF join in DataFusion; permanently
      `not_implemented()` unless the engine grows support.

### Misc

- [ ] v1 stable API: `cast_to_enum`, `with_row_index` edge case (2).
- [ ] `explode` with multiple columns — documented limitation (2).
- [ ] Namespace tests: `Implementation.UNKNOWN` prevents
      `to_native_namespace()`; plugin implementations can't register a real
      `Implementation` member — upstream plugin-integration gap to raise with
      narwhals maintainers.
- [ ] `Expr.__pow__` — `datafusion.Expr` has no `__pow__`/`__rpow__`; needs
      mapping to `f.pow` in the backend (breaks `is_nan` test *setup*).
- [ ] `is_nan` on non-numeric columns — DataFusion's `isnan` requires Numeric;
      narwhals expects graceful handling. Guard by dtype before calling.

## Known upstream issues

- **datafusion-extra-functions `mode` breaks multi-partition group-by**
  (strict xfail in `tests/test_extra_functions.py`): `state_fields` declares
  scalar columns but the accumulator serializes list state
  (`ScalarValue::List`), so schema validation fails with
  `expected Int64 but found List(Int64)`. Broken at pinned rev `9ba4d80` and
  on upstream `main`. Works with `target_partitions=1`. Fix belongs upstream;
  this repo keeps the FFI crate a thin provider (no patching of upstream Rust).
- **datafusion-extra-functions version coupling**: crates.io 0.5.2 targets
  DataFusion 53, `main` targets 55; the FFI crate pins git rev `9ba4d80` (last
  DataFusion 54-compatible commit, 2026-08-09). Re-pin when bumping
  datafusion-python.
- See the narwhals repo memory note "datafusion-python-quirks" for engine
  gotchas the implementation works around (identifier quoting, window modifier
  drops, frame limits).

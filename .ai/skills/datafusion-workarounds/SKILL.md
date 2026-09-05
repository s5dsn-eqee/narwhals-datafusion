---
name: datafusion-workarounds
description: "TRIGGER — read before adding or changing any expression, aggregate, window, join, cast, or dtype code in src/narwhals_datafusion/. DataFusion diverges from the SQL semantics narwhals' _sql layer assumes in a fixed set of ways; each has a settled workaround here. Reuse it, and never let an engine quirk produce a silently wrong result."
argument-hint: "[area] (e.g., \"windows\", \"joins\", \"column names\", \"aggregates\", \"dtypes\", or omit for the whole list)"
---

# DataFusion workarounds

Verified on datafusion 54, the major `pyproject.toml` caps at. Each entry: the
quirk, where it is handled, the rule. New quirk: add it here in the same commit
as the fix. When the cap moves to a major that fixes one, keep the workaround
until the floor passes the affected release and note the version here.

Grep first; every workaround carries a comment naming the engine behaviour:

```bash
grep -rn "datafusion" src/narwhals_datafusion/*.py | grep -i "#"
```

## Column names

- `datafusion.col("x")` parses a SQL identifier: unquoted names are lower-cased,
  keywords rejected. Always use `utils.col()`, which quotes and escapes.
- Engine errors embed the quoted name; `catch_datafusion_exception` in
  `utils.py` strips it before building `ColumnNotFoundError`. Wrap native
  calls that can fail on a missing column in the same `try/except ... raise
  catch_datafusion_exception(e, self) from None` the frame methods use.

## Literals and operators

- `Expr` has no unary minus and no `**`; integer `/` truncates. `expr.py` maps
  `__neg__` to `lit(-1) * expr`, `__pow__` to `F.pow`, `__truediv__` to a
  `float64` cast first; `utils._floordiv` floors for mixed signs.
- Reflected operators (`__rpow__`, `__rtruediv__`) `.alias("literal")`, as
  narwhals expects for a literal on the left.
- Wrap scalars in `lit()` explicitly; 54's auto-wrapping is not uniform.
- Remap lambdas receive raw Python values through `FUNCTION_REMAP`; use
  `utils._ensure_expr`.

## Aggregates

- `count_distinct` ignores nulls; narwhals `n_unique` counts one. `expr.n_unique`
  adds `max(isnull)`.
- Arithmetic over two aggregates inside `aggregate()` fails on 53 (`Invalid
  aggregate expression`): the reason for the `datafusion>=54` floor.
- `mode`, `skew`, `kurtosis` come from the `extra-functions` shim via
  `extra_functions.extra_udaf`; without it they raise `NotImplementedError`
  naming the extra (`tests/test_extra_functions.py` covers both). The crate's
  `skewness` is bias-adjusted; `_correct` in `expr.skew` undoes it and pins the
  n=0/1/2 cases. `mode` in a multi-partition GROUP BY is broken upstream
  (strict xfail); do not work around it in Python.
- `median` on strings, exact `quantile`, `product`: no engine support,
  `not_implemented()`. No approximate substitutes without an explicit decision.

## Windows

- **Modifiers declared inside an aggregate (`ORDER BY`, `IGNORE NULLS`,
  `DISTINCT`) are dropped when it runs as a window function**, returning a
  plausible wrong answer. In `expr.py`: `first`/`last`/`any_value` move them
  onto the `Window` (`utils.window_expression` takes `ignore_nulls=`,
  `frame_full=`); `n_unique` over a window raises. Keep it raising.
- `last_value` with an inner `order_by` misbehaves (rewritten to `first`
  internally); non-window `_last` uses `first_value` over the reversed sort.
- Mixing sort directions across windows in one projection trips an optimizer
  assertion; `last`'s window path keeps ascending order with `frame_full=True`.
- With `order_by` the default frame is cumulative. A whole-partition count must
  not pass the order (`dataframe.unique(keep="none")` had this bug).
- Bounded frames with `first_value`/`last_value` need `retract_batch`, not
  implemented: `fill_null(strategy=..., limit=n)` raises. Unlimited fills are
  `last_value` over a cumulative frame with `ignore_nulls=True`.
- Only aggregate and window functions may appear in `.over(...)`: window each
  aggregate, combine outside (`namespace.cov`, `expr.skew` `window_f`).

## Joins

- A column name on both sides gives an ambiguous-schema error. `dataframe.join`
  renames every right-hand column to a temporary name, joins, then re-selects
  with narwhals' suffix rules. Do not alias a single key instead.
- Semi/anti joins keep left columns only; cross join is `join_on(rhs, lit(True))`.
- `join_asof`: no engine support, `not_implemented()`.

## Frames

- `unnest_columns` drops empty lists and needs `preserve_nulls` for null
  lists; `explode` routes both through a literal-null branch and unions
  positionally. Multi-column explode raises (element counts unverifiable).
- No native unpivot: one projection per value column, unioned, after promoting
  mixed int/float value columns to a common type (`union` needs equal schemas).
- `union` is positional: `concat(how="vertical")` re-selects every frame in the
  first frame's column order.
- `union` output order is nondeterministic (one partition per input, coalesced
  as they arrive). Tests asserting order after `concat` without a sort live in
  `ALWAYS_DESELECTED` in `run_tests.py`.
- `write_parquet` takes a path; a file object would be `str()`-ed into a
  filename. `sink_parquet` raises for non-path inputs.
- `with_row_index` requires `order_by`.

## Dtypes and time

- Schemas are Arrow schemas; dtype mapping reuses `narwhals._arrow.utils` both
  ways. `Enum`/dictionary casts are rejected by the engine.
- `date_part('millisecond')` and friends include whole seconds; `dt` subtracts
  `second * 1000` etc. `dow` is 0=Sunday; narwhals wants 1=Monday.
- `truncate` with a multiple uses `date_bin` anchored at the epoch (polars
  semantics); single units use `date_trunc`.
- `replace_time_zone`: plain cast for `None` and `"UTC"`, other zones raise.
  `convert_time_zone`: cast to a tz-aware timestamp.
- `str.to_datetime` cannot infer a format; require one. A `%z`/`%Z` format
  parses to UTC but returns a naive timestamp; re-attach the zone.
- `str.to_time` with a format parses against the epoch day via `to_timestamp`
  and casts down; `concat` turns a null into a parse error, hence the null
  branch.
- `btrim` takes one argument; `strip_chars` with a character set is a regex.
  `re.escape` per character is safe inside a Rust regex class.

## Where the truth is

- `README.md` coverage matrix: same commit as any change here.
- `tests/test_smoke.py`: a regression test per entry; add one with a new entry.
- `narwhals/src/narwhals/_duckdb/`: the reference backend for what narwhals
  expects.

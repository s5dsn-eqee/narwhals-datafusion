---
name: datafusion-workarounds
description: "TRIGGER — read before adding or changing any expression, aggregate, window, join, cast, or dtype code in src/narwhals_datafusion/. DataFusion 54 diverges from the SQL semantics narwhals' _sql layer assumes in a fixed set of ways, and each one already has a settled workaround in this codebase. Reuse the existing pattern; do not design a new one, and never let an engine quirk produce a silently wrong result."
argument-hint: "[area] (e.g., \"windows\", \"joins\", \"column names\", \"aggregates\", \"dtypes\", or omit to review the whole list)"
---

# DataFusion workarounds

narwhals' `_sql` layer generates one logical shape and expects every SQL
backend to honour it. DataFusion 54 does not in the cases below. Each entry
names the quirk, the file that handles it, and the rule to follow. If you hit
a new one, add it here in the same commit as the fix.

## Rule 0 — grep before you write

```bash
grep -rn "datafusion" src/narwhals_datafusion/*.py | grep -i "#"
```

Every workaround in the codebase carries a comment naming the engine
behaviour. If the comment for your problem already exists, the pattern next to
it is the one to copy.

## Column names

- `datafusion.col("x")` parses its argument as a SQL identifier: unquoted
  names are lower-cased and reserved words are rejected. **Always go through
  `utils.col()`**, which double-quotes and escapes the name. Never call
  `datafusion.col` directly.
- Error messages from the engine embed the quoted form. `catch_datafusion_exception`
  in `utils.py` strips the quotes back off before building narwhals'
  `ColumnNotFoundError`; wrap native calls that can fail on a missing column
  in the same `try/except ... raise catch_datafusion_exception(e, self) from None`
  block the frame methods use.

## Literals and operators

- `Expr` has no unary minus, no `**`, and integer `/` truncates. `expr.py`
  maps `__neg__` to `lit(-1) * expr`, `__pow__` to `F.pow`, and `__truediv__`
  to a `float64` cast first. `utils._floordiv` does flooring (not truncating)
  division for mixed signs.
- Reflected operators (`__rpow__`, `__rtruediv__`) must `.alias("literal")`,
  matching what narwhals expects for a literal-on-the-left result.
- Wrap scalars with `lit()` explicitly everywhere. datafusion-python is adding
  auto-wrapping for some function arguments, but the versions this project
  pins do not have it uniformly, so explicit `lit()` is the version-robust
  form.
- `_ensure_expr` in `utils.py` exists because narwhals' `_function` helper
  passes raw Python values through `FUNCTION_REMAP`; use it in remap lambdas
  rather than assuming an `Expr`.

## Aggregates

- `count_distinct` ignores nulls, but narwhals `n_unique` counts a null as a
  value. `expr.n_unique` adds `max(isnull)`.
- Arithmetic that wraps two aggregates inside `aggregate()` (as `n_unique` and
  `skew` do) fails on datafusion-python 53 and earlier with `Invalid aggregate
  expression`. **This is why the floor is `datafusion>=54`.**
- `mode`, `skew`, `kurtosis` come from the `datafusion-extra-functions-ffi`
  wheel via `extra_functions.extra_udaf`. `skew` there is bias-adjusted; the
  `_correct` helper in `expr.skew` undoes that and pins the n=0/1/2 edge cases
  to match narwhals. `mode` in a multi-partition GROUP BY is broken upstream
  (strict xfail in `tests/test_extra_functions.py`); do not work around it in
  Python.
- `median` on strings, exact `quantile`, and `product` have no engine
  support and are declared `not_implemented()`; do not emulate them with
  approximate functions without an explicit decision.

## Windows

- **DataFusion silently drops `ORDER BY`, `IGNORE NULLS` and `DISTINCT`
  declared inside an aggregate when that aggregate is used as a window
  function.** It returns a plausible wrong answer, not an error. Consequences,
  all in `expr.py`:
  - `first`/`last`/`any_value` move those modifiers onto the `Window` itself
    in their `window_f` paths (`utils.window_expression` takes
    `ignore_nulls=` and `frame_full=` for this).
  - `n_unique` over a window raises `NotImplementedError`. Do not remove that.
- `last_value` with an inner `order_by` is rewritten to `first` internally and
  misbehaves; the non-window `_last` uses `first_value` over the reversed
  sort instead.
- Mixing sort directions across windows in one projection trips an optimizer
  assertion. `last`'s window path keeps ascending order and takes the whole
  partition as the frame (`frame_full=True`) rather than reversing.
- With an `order_by`, the default frame is cumulative (unbounded preceding to
  current row). Anything that needs a whole-partition count must **not** pass
  the order (see `dataframe.unique(keep="none")`, which had exactly this bug).
- Bounded frames with `first_value`/`last_value` need `retract_batch`, which
  the engine does not implement. `fill_null(strategy=..., limit=n)` raises for
  that reason; forward/backward fill without a limit is `last_value` over a
  cumulative frame with `ignore_nulls=True`.
- Only aggregate and window functions may appear inside `.over(...)`. A
  compound expression must window each aggregate individually and combine
  outside (see `namespace.cov` and `expr.skew` `window_f`).

## Joins

- Joining when a column name exists on both sides yields an ambiguous
  qualified/unqualified schema error. `dataframe.join` renames **every**
  right-hand column to a collision-free temporary name first, joins, then
  re-selects with narwhals' suffix rules. Keep that structure; do not alias a
  single key to match the other side.
- Semi and anti joins keep left columns only; cross join is `join_on(rhs, lit(True))`.
- `join_asof` has no engine support and is `not_implemented()`.

## Frames

- `unnest_columns` drops rows whose list is empty and needs `preserve_nulls`
  for null lists. `explode` routes empty and null lists through a
  literal-null branch and unions positionally. Multi-column explode raises
  because element counts cannot be checked.
- There is no native unpivot; `unpivot` builds one projection per value
  column and unions them, promoting mixed integer/float value columns to a
  common type first because `union` requires identical schemas.
- `union` is positional, so `concat(how="vertical")` re-selects every frame
  in the first frame's column order before unioning.
- `write_parquet` takes a path; a file-like object would be `str()`-ed into
  a filename. `sink_parquet` raises for non-path inputs.
- `with_row_index` requires `order_by`; row numbers without an order are not
  meaningful in a SQL engine.

## Dtypes and time

- DataFusion schemas are Arrow schemas, so dtype mapping reuses
  `narwhals._arrow.utils` in both directions. `Enum`/dictionary types are
  rejected by the engine on the cast path.
- `date_part('millisecond')` and friends include the whole seconds; the
  `dt` namespace subtracts `second * 1000` etc. `dow` is 0=Sunday, narwhals
  wants 1=Monday.
- `truncate` with a multiple uses `date_bin` anchored at the epoch, which is
  what polars does. Single units use `date_trunc`.
- `replace_time_zone` is a plain cast for `None` and `"UTC"` only; other zones
  raise. `convert_time_zone` is a cast to a tz-aware timestamp.
- `str.to_datetime` cannot infer a format; require one. A `%z`/`%Z` format
  parses to UTC but returns a naive timestamp, so re-attach the zone.
- `str.to_time` with a format parses against the epoch day via `to_timestamp`
  and casts down; `concat` turns a null into a parse error, hence the explicit
  null branch.
- `btrim` takes a single argument, so `strip_chars` with a character set is a
  regex. `re.escape` per character is safe inside a Rust regex class
  (verified for whitespace, `]-^\`, and punctuation).

## Where the truth is

- `README.md` "Known limitations" and the API coverage matrix, which must be
  updated in the same commit as any change here.
- `tests/test_smoke.py` has a regression test for most of the entries above;
  add one when you add an entry.
- `narwhals/src/narwhals/_duckdb/` is the closest reference backend: when in
  doubt about what narwhals expects, read how DuckDB does it.

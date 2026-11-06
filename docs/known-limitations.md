# Known limitations

Limits outside the [coverage table](api-coverage.md): engine-wide behaviour,
and gaps in narwhals' own lazy layers. Every row links the test that shows it;
a row goes in the same commit as the change that lifts it.

| Name | Description | Test | Link |
|---|---|---|---|
| Extra functions | `mode`, `skew` and `kurtosis` need the `extra-functions` extra, which installs the [datafusion-extra-functions][crate] wheels. Without it the three raise `NotImplementedError` naming the extra. | [`test_without_extra_points_at_it`][t-extra] | [`extra_functions.py`][extra_functions] |
| Lazy-expression gaps | narwhals' `LazyExpr` does not implement `Expr.ewm_mean` or `Expr.map_batches`; they raise `NotImplementedError` on every lazy backend. | [`test_lazy_layer_gaps_raise`][t-limits] | [`LazyExpr`][nw-lazyexpr] |
| SQL-layer gaps | narwhals' `SQLExpr` does not implement `Expr.filter`, `Expr.drop_nulls` or `Expr.unique`; they raise `NotImplementedError` on every SQL backend. | [`test_lazy_layer_gaps_raise`][t-limits] | [`SQLExpr`][nw-sqlexpr] |
| `stable.v1` row selection | `LazyFrame.tail` and `LazyFrame.gather_every` exist only in `narwhals.stable.v1`, and no SQL backend implements them; they raise `NotImplementedError`. | [`test_v1_row_selection_is_declared`][t-limits] | [`stable.v1.LazyFrame`][nw-v1], [`dataframe.py`][dataframe] |
| Row order | Row order is guaranteed only after `sort`; `concat` may interleave its inputs. | [`test_concat_returns_every_row_in_some_order`][t-frames] | [`namespace.concat`][namespace] |
| Scan by plugin name | `nw.scan_csv`/`nw.scan_parquet` take `backend="narwhals-datafusion"` on narwhals 2.28+. On 2.27, read with a `SessionContext` and pass the frame to `nw.from_native`. | [`test_scan_through_plugin_name`][t-smoke] | [`namespace.py`][namespace] |

[t-extra]: ../tests/test_extra_functions.py
[t-limits]: ../tests/test_known_limitations.py
[t-frames]: ../tests/workarounds/test_frames.py
[t-smoke]: ../tests/test_smoke.py
[extra_functions]: ../src/narwhals_datafusion/extra_functions.py
[dataframe]: ../src/narwhals_datafusion/dataframe.py
[namespace]: ../src/narwhals_datafusion/namespace.py
[crate]: https://github.com/datafusion-contrib/datafusion-extra-functions
[nw-lazyexpr]: https://github.com/narwhals-dev/narwhals/blob/v2.28.0/src/narwhals/_compliant/expr.py#L941-L942
[nw-sqlexpr]: https://github.com/narwhals-dev/narwhals/blob/v2.28.0/src/narwhals/_sql/expr.py#L984-L1001
[nw-v1]: https://github.com/narwhals-dev/narwhals/blob/v2.28.0/src/narwhals/stable/v1/__init__.py#L292-L296

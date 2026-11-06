# Known limitations

- `mode`, `skew` and `kurtosis` need the `extra-functions` extra. It installs
  [datafusion-extra-functions](https://github.com/datafusion-contrib/datafusion-extra-functions),
  prebuilt wheels of the crate. Without it the three raise
  `NotImplementedError`.
- Not in the [coverage table](api-coverage.md); narwhals' lazy layers do not
  implement them: `Expr.ewm_mean`, `Expr.map_batches` (`LazyExpr`),
  `Expr.filter`, `Expr.drop_nulls`, `Expr.unique` (`SQLExpr`).
  `LazyFrame.tail` and `LazyFrame.gather_every` exist only in
  `narwhals.stable.v1`, and no SQL backend implements them. All seven raise
  `NotImplementedError`.
- Row order is guaranteed only after `sort`; `concat` may interleave inputs.
- `nw.scan_csv`/`nw.scan_parquet` take `backend="narwhals-datafusion"` on
  narwhals 2.28+. On 2.27, read with a `SessionContext` and pass the frame to
  `nw.from_native`.

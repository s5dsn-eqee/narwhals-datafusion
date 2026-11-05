# Known limitations

- `mode`, `skew` and `kurtosis` need the `extra-functions` extra. It installs
  [datafusion-extra-functions-ffi](https://github.com/s5dsn-eqee/datafusion-extra-functions-ffi),
  a prebuilt wheel of the `datafusion-extra-functions` crate. Without it the
  three raise `NotImplementedError`.
- Not in the [coverage table](api-coverage.md): `Expr.ewm_mean`,
  `Expr.map_batches`, `LazyFrame.tail`, `LazyFrame.gather_every`: narwhals
  does not support them on lazy frames. `Expr.filter`, `Expr.drop_nulls`,
  `Expr.unique`: narwhals' SQL layer does not support them.
- Row order is guaranteed only after `sort`; `concat` may interleave inputs.
- `nw.scan_csv`/`nw.scan_parquet` take `backend="narwhals-datafusion"` on
  narwhals 2.28+. On 2.27, read with a `SessionContext` and pass the frame to
  `nw.from_native`.

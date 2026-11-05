# Architecture

A subclass of narwhals' SQL layer (`narwhals._sql`, `narwhals._compliant`),
the layer DuckDB, Ibis and Spark also use.

| Module | Class |
|---|---|
| `dataframe.py` | `DataFusionLazyFrame`: frame verbs over `datafusion.DataFrame` |
| `expr.py` | `DataFusionExpr`: the `SQLExpr` hooks, aggregates, windows, casts |
| `namespace.py` | `DataFusionNamespace`: `SQLNamespace` primitives, IO, horizontal functions |
| `group_by.py`, `selectors.py`, `expr_str/dt/list/struct.py` | supporting surface |
| `utils.py` | function-name remapping, window/sort builders, dtype bridge via `narwhals._arrow` |

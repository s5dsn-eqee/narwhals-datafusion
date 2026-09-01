# narwhals-datafusion

[![CI](https://github.com/s5dsn-eqee/narwhals-datafusion/actions/workflows/ci.yml/badge.svg)](https://github.com/s5dsn-eqee/narwhals-datafusion/actions/workflows/ci.yml)

[Apache DataFusion](https://datafusion.apache.org/python/) backend for
[Narwhals](https://github.com/narwhals-dev/narwhals), implemented as an
out-of-tree plugin via the `narwhals.plugins` entry-point system.

## Usage

```python
import narwhals as nw
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()
df = ctx.from_arrow(pa.table({"a": [1, 2, 3], "b": ["x", "y", "x"]}))

lf = nw.from_native(df)  # -> nw.LazyFrame, dispatched to this plugin
result = (
    lf.group_by("b")
    .agg(nw.col("a").sum())
    .sort("b")
    .collect(backend="pyarrow")
)
```

Everything stays lazy until `.collect()`: narwhals expressions are translated to
`datafusion.Expr`, frame verbs to `datafusion.DataFrame` methods, and the plan
executes in DataFusion's Rust engine. Dtypes are pyarrow end-to-end.

## Architecture

The backend sits on narwhals' shared SQL layer (`narwhals._sql`), the same
abstraction DuckDB, Ibis, and Spark use. It targets `narwhals==2.25` internals
(`narwhals._sql`, `narwhals._compliant`), vendored as the `narwhals/` git
submodule pinned to that release. The plugin provides:

| Module | Class |
|---|---|
| `dataframe.py` | `DataFusionLazyFrame` — frame verbs over `datafusion.DataFrame` |
| `expr.py` | `DataFusionExpr` — the six `SQLExpr` hooks + backend specifics |
| `namespace.py` | `DataFusionNamespace` — the four `SQLNamespace` primitives, IO, horizontal fns |
| `group_by.py`, `selectors.py`, `expr_str/dt/list/struct.py` | supporting surface |
| `utils.py` | function-name remapping, window/sort builders, dtype bridge (delegates to `narwhals._arrow`) |

## `mode`, `skew`, `kurtosis` via `datafusion-extra-functions`

DataFusion core deliberately keeps its function library lean, so aggregates
like `mode`/`skewness`/`kurtosis` live in the contrib
[`datafusion-extra-functions`](https://github.com/datafusion-contrib/datafusion-extra-functions)
crate (Rust-only, no wheel on PyPI). The `extra-functions-ffi/` directory
contains an FFI shim that exposes its aggregate UDFs to datafusion-python via
the `__datafusion_aggregate_udf__` PyCapsule protocol; they back `Expr.mode`,
`Expr.skew`, and `Expr.kurtosis`. It is a required dependency, built from the
local path by `uv sync`, or directly:

```sh
pip install maturin
cd extra-functions-ffi && maturin develop --release
```

The shim's `datafusion-ffi` major version must match the installed
`datafusion` release (currently 54).

## Known limitations (as of datafusion 54)

- `join_asof`, exact `quantile`, `ewm_mean`,
  `cum_prod`, `list.sum/mean/median`, `dt.total_*`, `dt.offset_by`,
  `dt.timestamp`, `str.replace(n=...)`, `Enum` casts — no engine support;
  raise `NotImplementedError`.
- `n_unique().over(...)` raises: DataFusion silently ignores `DISTINCT` inside
  window aggregates, which would return wrong results. (The same engine quirk
  drops `ORDER BY`/`IGNORE NULLS` declared inside window aggregates — this
  backend moves those modifiers onto the window itself.)
- `fill_null(strategy=..., limit=n)` raises: bounded window frames with
  `first_value`/`last_value` need `retract_batch`, unimplemented engine-side.
- `replace_time_zone` supports `None` (strip) and `"UTC"` only; use
  `convert_time_zone` for instant-preserving conversions.
- `str.to_datetime`/`to_date` require an explicit `format`;
  `str.to_time` parses `"HH:MM:SS"`-style strings via Arrow's cast, ignoring
  custom formats.
- `replace_strict` requires an explicit `default`.
- `str.to_titlecase` uses `initcap`, which doesn't break words on digits.
- No row-order guarantees except after `sort` (standard for SQL engines);
  backward `fill_null` may reorder rows.

## Development

A Rust toolchain is required to build this project correctly: the FFI shim in
`extra-functions-ffi/` is compiled from source (via maturin) when the
environment is set up.

```sh
git submodule update --init      # narwhals, pinned to the targeted release
uv sync --group tests            # installs deps and builds extra-functions-ffi
```

## Running narwhals' own test suite against this backend

The narwhals repo is vendored as a git submodule pinned to the targeted
release:

```sh
git submodule update --init
uv run --group tests python run_tests.py    # known failures deselected
```

For the full, unfiltered run:

```sh
uv run --group tests pytest narwhals/tests -c narwhals/pyproject.toml \
    -p narwhals_datafusion.testing -p env --use-external-constructor
```

Regenerate the deselect list after fixing tests or bumping the submodule with
`uv run --group tests python update_run_tests.py`.

# narwhals-datafusion

[![CI](https://github.com/s5dsn-eqee/narwhals-datafusion/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/s5dsn-eqee/narwhals-datafusion/actions/workflows/ci.yml)
[![compat](https://img.shields.io/github/actions/workflow/status/s5dsn-eqee/narwhals-datafusion/compat.yml?branch=main&label=latest%20narwhals)](https://github.com/s5dsn-eqee/narwhals-datafusion/actions/workflows/compat.yml)
[![PyPI version](https://badge.fury.io/py/narwhals-datafusion.svg)](https://badge.fury.io/py/narwhals-datafusion)
[![PEP 740](https://img.shields.io/badge/PEP%20740-attested-3775A9?logo=pypi&logoColor=white)](https://pypi.org/project/narwhals-datafusion/#files)
[![Downloads](https://static.pepy.tech/badge/narwhals-datafusion/month)](https://pepy.tech/project/narwhals-datafusion)

[Apache DataFusion](https://datafusion.apache.org/python/) backend for
[Narwhals](https://github.com/narwhals-dev/narwhals), registered through the
`narwhals.plugins` entry point.

```sh
pip install narwhals-datafusion                    # core
pip install "narwhals-datafusion[extra-functions]" # + mode, skew, kurtosis
```

## Usage

```python
import narwhals as nw
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()
df = ctx.from_arrow(pa.table({"a": [1, 2, 3], "b": ["x", "y", "x"]}))

lf = nw.from_native(df)  # nw.LazyFrame on this backend
result = (
    lf.group_by("b")
    .agg(nw.col("a").sum())
    .sort("b")
    .collect(backend="pyarrow")
)
```

Everything is lazy until `.collect()`: expressions become `datafusion.Expr`,
frame verbs become `datafusion.DataFrame` methods, and DataFusion executes the
plan. Dtypes are pyarrow end-to-end.

- [Playground](https://github.com/s5dsn-eqee/narwhals-datafusion/blob/main/docs/playground.ipynb): a tour of the API, extra included
- [API coverage](https://github.com/s5dsn-eqee/narwhals-datafusion/blob/main/docs/api-coverage.md): supported, partial and unsupported methods
- [Known limitations](https://github.com/s5dsn-eqee/narwhals-datafusion/blob/main/docs/known-limitations.md)
- [Architecture](https://github.com/s5dsn-eqee/narwhals-datafusion/blob/main/docs/architecture.md)

## Used by

- [pandera](https://pandera.readthedocs.io/en/stable/datafusion.html):
  `pip install 'pandera[datafusion]'` validates `datafusion.DataFrame`
  through this plugin. `tests/pandera/` pins the `narwhals.stable.v1` calls
  it makes.

## Compatibility

- Python `>=3.10`; tested on 3.12.
- `datafusion>=55,<56`: capped at the tested major; a release moves the cap.
- `narwhals>=2.27`: floor only; tested on 2.28, the `narwhals/` submodule.
- [`datafusion-extra-functions`](https://github.com/datafusion-contrib/datafusion-extra-functions)
  `>=0.5.5` (extra): each release pins its datafusion major.
- Weekly, `compat.yml` (the badge): both suites on the newest narwhals release
  inside the datafusion cap, extra included.

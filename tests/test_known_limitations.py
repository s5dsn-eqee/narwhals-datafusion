"""Tests for the rows of docs/known-limitations.md that live outside a feature's own tests."""

from __future__ import annotations

import narwhals.stable.v1 as nw_v1
import pyarrow as pa
import pytest
from datafusion import SessionContext

import narwhals as nw


def native():
    return SessionContext().from_arrow(pa.table({"a": [1, 2, None, 2]}))


@pytest.mark.parametrize(
    ("expr", "layer"),
    [
        (nw.col("a").ewm_mean(com=1), "LazyExpr"),
        (nw.col("a").map_batches(lambda s: s), "LazyExpr"),
        (nw.col("a").filter(nw.col("a") > 1).sum(), "SQLExpr"),
        (nw.col("a").drop_nulls().sum(), "SQLExpr"),
        (nw.col("a").unique().sum(), "SQLExpr"),
    ],
    ids=["ewm_mean", "map_batches", "filter", "drop_nulls", "unique"],
)
def test_lazy_layer_gaps_raise(expr: nw.Expr, layer: str) -> None:
    """Lazy-layer gaps: narwhals declares these unsupported for every lazy or SQL backend."""
    with pytest.raises(NotImplementedError, match=layer):
        nw.from_native(native()).select(expr)


@pytest.mark.parametrize("method", ["tail", "gather_every"])
def test_v1_row_selection_is_declared(method: str) -> None:
    """`stable.v1` row selection: no SQL backend implements `tail`/`gather_every`."""
    with pytest.raises(NotImplementedError, match=method):
        getattr(nw_v1.from_native(native()), method)(2)

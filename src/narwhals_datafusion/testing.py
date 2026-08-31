"""Pytest plugin to run narwhals' own test suite against the DataFusion backend.

From a narwhals checkout, with `narwhals-datafusion` installed:

    pytest tests/ --use-external-constructor -p narwhals_datafusion.testing
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import datafusion
    import pytest


def datafusion_lazy_constructor(obj: dict[str, Any]) -> datafusion.DataFrame:
    import pyarrow as pa
    from datafusion import SessionContext

    return SessionContext().from_arrow(pa.table(obj))


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    try:
        use_external = metafunc.config.getoption("use_external_constructor")
    except ValueError:  # narwhals' conftest not loaded
        return
    if not use_external:
        return
    if "constructor" in metafunc.fixturenames:
        metafunc.parametrize("constructor", [datafusion_lazy_constructor], ids=["datafusion"])
    if "constructor_eager" in metafunc.fixturenames:
        # DataFusion is lazy-only: eager-constructor tests are skipped.
        metafunc.parametrize("constructor_eager", [], ids=[])
    if "constructor_pandas_like" in metafunc.fixturenames:
        metafunc.parametrize("constructor_pandas_like", [], ids=[])

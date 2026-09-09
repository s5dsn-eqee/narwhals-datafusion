"""Pytest plugin that runs narwhals' own suite against a DataFusion frame.

Usage::

    pytest narwhals/tests --use-external-constructor -p narwhals_datafusion.testing
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import datafusion
    import pytest


def datafusion_lazy_constructor(obj: dict[str, Any]) -> datafusion.DataFrame:
    """Build a ``datafusion.DataFrame`` from a column-name-to-values mapping.

    Has the signature of narwhals' ``constructor`` fixture.
    """
    import pyarrow as pa
    from datafusion import SessionContext

    return SessionContext().from_arrow(pa.table(obj))


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize narwhals' constructor fixtures with this backend.

    Active only under ``--use-external-constructor``. ``constructor`` gets the
    DataFusion frame; the eager and pandas-like fixtures get no values, so
    their tests are skipped.
    """
    try:
        use_external = metafunc.config.getoption("use_external_constructor")
    except ValueError:  # narwhals' conftest not loaded
        return
    if not use_external:
        return
    if "constructor" in metafunc.fixturenames:
        metafunc.parametrize("constructor", [datafusion_lazy_constructor], ids=["datafusion"])
    if "constructor_eager" in metafunc.fixturenames:
        metafunc.parametrize("constructor_eager", [], ids=[])
    if "constructor_pandas_like" in metafunc.fixturenames:
        metafunc.parametrize("constructor_pandas_like", [], ids=[])

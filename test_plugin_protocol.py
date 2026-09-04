"""Static check that the package satisfies narwhals' `Plugin` protocol.

Not a pytest test: it is verified by `pyright test_plugin_protocol.py` in CI,
mirroring narwhals-daft. A protocol mismatch (missing `NATIVE_PACKAGE`,
wrong `__narwhals_namespace__` signature, ...) fails type-checking here
before it fails at plugin-discovery time for users.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import narwhals_datafusion

if TYPE_CHECKING:
    from narwhals.plugins import Plugin

    from narwhals_datafusion.dataframe import DataFusionLazyFrame

plugin: Plugin[DataFusionLazyFrame, DataFusionLazyFrame] = narwhals_datafusion

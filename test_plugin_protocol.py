"""Static check that the package satisfies narwhals' `Plugin` protocol.

Not a pytest test: pyright checks the assignment below. A protocol mismatch
fails here before it fails at plugin discovery for users.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import narwhals_datafusion

if TYPE_CHECKING:
    from narwhals.plugins import Plugin

    from narwhals_datafusion.dataframe import DataFusionLazyFrame

plugin: Plugin[DataFusionLazyFrame, DataFusionLazyFrame] = narwhals_datafusion

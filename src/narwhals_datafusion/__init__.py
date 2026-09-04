"""Apache DataFusion backend for Narwhals, registered via the `narwhals.plugins` entry point."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datafusion
    from narwhals._utils import Version
    from typing_extensions import TypeIs

    from narwhals_datafusion.namespace import DataFusionNamespace

__version__ = "0.1.0"

NATIVE_PACKAGE = "datafusion"


def __narwhals_namespace__(version: Version) -> DataFusionNamespace:
    from narwhals_datafusion.namespace import DataFusionNamespace

    return DataFusionNamespace(version=version)


def is_native(native_object: object) -> TypeIs[datafusion.DataFrame]:
    import datafusion

    return isinstance(native_object, datafusion.DataFrame)

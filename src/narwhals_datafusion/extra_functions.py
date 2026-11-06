"""Aggregates from the optional ``extra-functions`` extra.

``mode``, ``skewness`` and ``kurtosis`` come from the
``datafusion-extra-functions`` wheel through the
``__datafusion_aggregate_udf__`` capsule protocol. Without it ``extra_udaf``
returns ``None`` and callers raise ``NotImplementedError``.
"""

from __future__ import annotations

from functools import cache
from importlib.util import find_spec
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.user_defined import AggregateUDF

INSTALL_HINT = (
    "it needs the `datafusion-extra-functions` package "
    "(FFI bindings for the `datafusion-extra-functions` Rust crate). "
    'Install the extra: `pip install "narwhals-datafusion[extra-functions]"`.'
)


@cache
def extra_udaf(name: str) -> AggregateUDF | None:
    """The aggregate UDF called ``name``, or ``None`` if the package or the name is missing."""
    if find_spec("datafusion_extra_functions") is None:
        return None
    import datafusion_extra_functions as extra
    from datafusion import udaf

    # the extension module ships no stubs, so its attributes are unknown
    if name not in extra.list_functions():  # pyright: ignore[reportAttributeAccessIssue]
        return None
    return udaf(extra.udaf_by_name(name))  # pyright: ignore[reportAttributeAccessIssue]

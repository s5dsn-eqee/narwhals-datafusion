"""Aggregates from `datafusion-extra-functions` (mode, skewness, kurtosis).

The upstream crate is Rust-only; the companion
[`datafusion-extra-functions-ffi`](https://github.com/s5dsn-eqee/datafusion-extra-functions-ffi)
wheel — a required dependency — exposes its aggregate UDFs through
datafusion-python's `__datafusion_aggregate_udf__` PyCapsule protocol. The
lookup still degrades gracefully (raising `NotImplementedError` from the
narwhals operations) if the wheel is missing or lacks a function.
"""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.user_defined import AggregateUDF

INSTALL_HINT = (
    "it needs the `datafusion-extra-functions-ffi` package "
    "(FFI bindings for the `datafusion-extra-functions` Rust crate), "
    "which is a required dependency -- reinstall `narwhals-datafusion`."
)


@cache
def extra_udaf(name: str) -> AggregateUDF | None:
    try:
        import datafusion_extra_functions_ffi as ffi
    except ImportError:
        return None
    from datafusion import udaf

    try:
        # the extension module ships no stubs, so its attributes are unknown
        return udaf(ffi.udaf_by_name(name))  # pyright: ignore[reportAttributeAccessIssue]
    except KeyError:  # pragma: no cover - future crate versions
        return None

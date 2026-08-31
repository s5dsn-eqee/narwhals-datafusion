"""Optional aggregates from `datafusion-extra-functions` (mode, skewness, kurtosis).

The upstream crate is Rust-only; the companion `datafusion-extra-functions-ffi`
wheel (see `extra-functions-ffi/` in this repo) exposes its aggregate UDFs
through datafusion-python's `__datafusion_aggregate_udf__` PyCapsule protocol.
When that wheel is installed, the backend picks the functions up automatically;
without it, the corresponding narwhals operations raise `NotImplementedError`.
"""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.user_defined import AggregateUDF

INSTALL_HINT = (
    "it needs the optional `datafusion-extra-functions-ffi` package "
    "(FFI bindings for the `datafusion-extra-functions` Rust crate)."
)


@cache
def extra_udaf(name: str) -> AggregateUDF | None:
    try:
        import datafusion_extra_functions_ffi as ffi
    except ImportError:
        return None
    from datafusion import udaf

    try:
        return udaf(ffi.udaf_by_name(name))
    except KeyError:  # pragma: no cover - future crate versions
        return None

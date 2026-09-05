"""`mode`, `skewness`, `kurtosis` from the `datafusion-extra-functions-ffi` wheel
(the `extra-functions` extra), via the `__datafusion_aggregate_udf__` capsule
protocol. Without it `extra_udaf` returns `None` and callers raise
`NotImplementedError`.
"""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.user_defined import AggregateUDF

INSTALL_HINT = (
    "it needs the `datafusion-extra-functions-ffi` package "
    "(FFI bindings for the `datafusion-extra-functions` Rust crate). "
    'Install the extra: `pip install "narwhals-datafusion[extra-functions]"`.'
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

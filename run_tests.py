"""Run narwhals' own test suite (the `narwhals/` submodule) against this backend.

Known failures are deselected via `TESTS_THAT_NEED_FIX`; regenerate it with
`update_run_tests.py`. Without the shim, `TESTS_NEED_EXTRA` is deselected too.
Extra arguments pass through to pytest.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

TESTS_THAT_NEED_FIX: list[str] = [
    "test_cast_datetime_tz_aware",
    "test_cast_datetime_utc",
    "test_cast_to_enum_v1",
    "test_cast_to_enum_vmain",
    "test_convert_time_zone_from_none",
    "test_datetime",
    "test_drop_nulls_agg",
    "test_duration_attributes",
    "test_duration_attributes_nano",
    "test_enum",
    "test_enum_distinct_from_categorical",
    "test_explode_multiple_cols",
    "test_fill_null_limits",
    "test_fill_null_strategies_with_limit_as_none",
    "test_is_finite_expr",
    "test_joinasof_by",
    "test_joinasof_numeric",
    "test_joinasof_suffix",
    "test_joinasof_time",
    "test_lazy_cum_prod_grouped",
    "test_mean_expr",
    "test_median_expr",
    "test_median_expr_raises_on_str",
    "test_mode_expr_keep_all_lazy",
    "test_mode_group_by_multimodal",
    "test_mode_group_by_multiple_cols",
    "test_mode_group_by_unimodal",
    "test_n_unique_over",
    "test_namespace_from_native_object",
    "test_nan_non_float",
    "test_native_namespace_frame",
    "test_offset_by",
    "test_offset_by_3471",
    "test_offset_by_dst",
    "test_offset_by_invalid_interval",
    "test_offset_by_tz",
    "test_over_quantile",
    "test_parse_weight",
    "test_quantile_expr",
    "test_quantile_expr_group_by",
    "test_rank_with_order_by",
    "test_rank_with_order_by_and_partition_by",
    "test_replace_strict_expr_basic",
    "test_replace_time_zone",
    "test_scan_csv",
    "test_scan_parquet",
    "test_str_replace_errors_expr",
    "test_str_replace_expr_multivalue",
    "test_str_replace_expr_scalar",
    "test_str_to_titlecase_expr",
    "test_sum_expr",
    "test_timestamp_dates",
    "test_timestamp_datetimes",
    "test_timestamp_datetimes_tz_aware",
    "test_timestamp_invalid_date",
    "test_to_datetime_infer_fmt",
    "test_to_datetime_infer_fmt_from_date",
    "test_to_datetime_tz_aware",
    "test_tz_aware",
    "test_unique_expr_agg",
    "test_unique_maintain_order_expr",
    "test_with_row_index",
]

# pass locally, fail only in CI; `update_run_tests.py` leaves this list alone
ALWAYS_DESELECTED: list[str] = [
    # assert row order after `concat` without sorting; union output order is
    # nondeterministic across partitions
    "test_concat_diagonal",
    "test_concat_vertical",
    # greps the current directory's pyproject.toml for a static narwhals version
    "test_package_version",
]

# pass with the `extra-functions` extra, fail without it; `unary` calls skew
TESTS_NEED_EXTRA: list[str] = ["test_kurtosis", "test_mode", "test_skew", "test_unary"]

DESELECTED = [*TESTS_THAT_NEED_FIX, *ALWAYS_DESELECTED]
if importlib.util.find_spec("datafusion_extra_functions_ffi") is None:
    DESELECTED += TESTS_NEED_EXTRA

# extra arguments pass through to pytest; paths replace the default target
extra = sys.argv[1:]
targets = [arg for arg in extra if Path(arg).exists()] or ["narwhals/tests"]
options = [arg for arg in extra if not Path(arg).exists()]

command = [
    "pytest",
    *targets,
    # narwhals' own pytest config (TZ=UTC, warning filters)
    "-c",
    "narwhals/pyproject.toml",
    "-p",
    "narwhals_datafusion.testing",
    "-p",
    "env",
    "--use-external-constructor",
]
# pytest keeps only the last `-k`: combine a caller's with the skip list
keyword = None
if "-k" in options:
    at = options.index("-k")
    keyword = options.pop(at + 1)
    options.pop(at)
expressions = []
if DESELECTED:
    expressions.append(f"not ({' or '.join(DESELECTED)})")
if keyword:
    expressions.append(f"({keyword})")
if expressions:
    command += ["-k", " and ".join(expressions)]

command.extend(options)

try:
    subprocess.run(command, check=True)
except subprocess.CalledProcessError as e:
    print("Exit code:", e.returncode)
    sys.exit(e.returncode)

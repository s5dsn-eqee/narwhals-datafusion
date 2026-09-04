"""Run narwhals' own test suite (from the `narwhals/` submodule) against this backend.

Mirrors narwhals-daft's `run_tests.py`: known-failing tests are deselected via
`TESTS_THAT_NEED_FIX` so the run is green; regenerate that list with
`python update_run_tests.py` after fixing things or bumping the submodule.

Usage:
    git submodule update --init
    uv run --group tests python run_tests.py       # extra pytest args pass through
"""

from __future__ import annotations

import subprocess
import sys

TESTS_THAT_NEED_FIX: list[str] = [
    "test_cast_datetime_tz_aware",
    "test_cast_datetime_utc",
    "test_cast_to_enum_v1",
    "test_cast_to_enum_vmain",
    "test_convert_time_zone_from_none",
    "test_datetime",
    "test_drop_nulls_agg",
    "test_duration_attributes",
    "test_enum",
    "test_enum_distinct_from_categorical",
    "test_explode_multiple_cols",
    "test_fill_null_limits",
    "test_fill_null_strategies_with_limit_as_none",
    "test_float16",
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
    "test_package_version",
    "test_parse_weight",
    "test_quantile_expr",
    "test_quantile_expr_group_by",
    "test_rank_with_order_by",
    "test_rank_with_order_by_and_partition_by",
    "test_replace_strict_expr_basic",
    "test_replace_time_zone",
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

# Always deselected, independent of `update_run_tests.py`, because they pass
# locally and fail only in CI, so the regenerator would keep dropping them.
ALWAYS_DESELECTED: list[str] = [
    # assert row order after `concat` without sorting; DataFusion's union
    # output order is not deterministic under multi-partition execution
    "test_concat_diagonal",
    "test_concat_vertical",
    # greps the *current directory's* pyproject.toml for narwhals' version and
    # asserts only under GitHub Actions; ours has no static version line
    "test_package_version",
]

DESELECTED = [*TESTS_THAT_NEED_FIX, *ALWAYS_DESELECTED]

command = [
    "pytest",
    "narwhals/tests",
    # use narwhals' own pytest config (env vars like TZ=UTC, warning filters)
    "-c",
    "narwhals/pyproject.toml",
    "-p",
    "narwhals_datafusion.testing",
    "-p",
    "env",
    "--use-external-constructor",
]
if DESELECTED:
    command += ["-k", f"not ({' or '.join(DESELECTED)})"]

command.extend(sys.argv[1:])

try:
    subprocess.run(command, check=True)
except subprocess.CalledProcessError as e:
    print("Exit code:", e.returncode)
    sys.exit(e.returncode)

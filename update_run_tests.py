"""Regenerate `TESTS_THAT_NEED_FIX` in run_tests.py from a full narwhals-suite run.

Mirrors narwhals-daft's `update_run_tests.py`. Usage:

    uv run --group tests python update_run_tests.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


def update_run_tests() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "narwhals/tests",
            "-c",
            "narwhals/pyproject.toml",
            "-p",
            "narwhals_datafusion.testing",
            "-p",
            "env",
            "--use-external-constructor",
            "--tb",
            "no",
            "-v",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    print(result.stdout[-3000:])

    failed_tests = re.findall(
        r"(?:FAILED|ERROR) narwhals/tests/.*\.py::(\w+)\[?", result.stdout
    )

    formatted_tests = ",\n    ".join(f'"{t}"' for t in sorted(set(failed_tests)))

    run_tests_path = Path(__file__).parent / "run_tests.py"
    content = run_tests_path.read_text(encoding="utf-8")
    new_content = re.sub(
        r"TESTS_THAT_NEED_FIX(?:: list\[str\])?\s*=\s*\[.*?\]",
        f"TESTS_THAT_NEED_FIX: list[str] = [\n    {formatted_tests},\n]",
        content,
        count=1,
        flags=re.DOTALL,
    )
    run_tests_path.write_text(new_content, encoding="utf-8")
    print(f"\nRecorded {len(set(failed_tests))} failing test names in run_tests.py")


if __name__ == "__main__":
    update_run_tests()

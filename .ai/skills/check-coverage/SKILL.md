---
name: check-coverage
description: Audit which parts of the narwhals lazy API this backend implements, partially supports, or declares unsupported, and reconcile that with the README coverage matrix and the narwhals-suite skip list. Use after a narwhals bump, before a release, or when asked "does this backend support X".
argument-hint: "[namespace] (e.g., \"Expr\", \"Expr.str\", \"Expr.dt\", \"Expr.list\", \"LazyFrame\", or omit for all)"
---

# Check narwhals API coverage

The README's "API coverage" table is the public statement of what works. It
is maintained by hand, so it drifts. This skill rebuilds the truth from three
sources and reports the differences.

## Source 1 — what narwhals asks a lazy backend to provide

The contract is the abstract surface in the vendored submodule:

```bash
# methods every SQL backend inherits or must supply
grep -nE "^\s+def [a-z_]+\(" narwhals/src/narwhals/_sql/expr.py
grep -nE "^\s+def [a-z_]+\(" narwhals/src/narwhals/_sql/dataframe.py
grep -nE "^\s+def [a-z_]+\(" narwhals/src/narwhals/_compliant/any_namespace.py   # str/dt/list/struct
# what the closest peer backend implements, as a checklist
grep -nE "^\s+def [a-z_]+\(|not_implemented\(\)" narwhals/src/narwhals/_duckdb/*.py
```

A method defined in `_sql/*` with a body is inherited for free; one that is
`not_implemented()` or abstract there must be provided here. The DuckDB backend
is the reference for which of those are realistically implementable in SQL.

## Source 2 — what this backend declares

```bash
grep -rn "not_implemented()" src/narwhals_datafusion/          # absolute gaps
grep -rn -B3 "raise NotImplementedError" src/narwhals_datafusion/  # conditional gaps
```

An absolute gap belongs in the README's "Not supported" column. A conditional
gap belongs in "Partial" with the condition in parentheses, worded the way the
error message words it.

## Source 3 — what the narwhals suite says

`TESTS_THAT_NEED_FIX` in `run_tests.py` is the list of narwhals tests that
fail. Each name should be explainable by an entry from Source 2 or by a
"Known limitations" bullet in the README. A skipped test with no explanation
is either an undocumented limitation or a bug.

```bash
grep -c '^    "test_' run_tests.py       # how many are deselected
```

## Producing the report

For the requested namespace (or all of them), emit:

```
## <Namespace> coverage

### Implemented and documented as supported (N)
- names, only if you spot-checked at least the ones added since the last audit

### Mismatches
- `method` — code says <supported|partial|unsupported> (file:line), README says <...>. Proposed cell text: "..."
- `method` — in TESTS_THAT_NEED_FIX but no limitation explains it. Likely cause: ...

### New in narwhals since the pin, not yet handled
- `method` — added upstream in X.Y; DuckDB backend implements it via ...; feasible here because ...

### Unchanged
- namespaces that reconcile cleanly
```

Rank mismatches by user impact: a method the README calls supported that
actually raises is the most severe; a caveat that is stale in the other
direction is the least.

## If asked to fix

- Edit `README.md` table cells in place. Cells are space-separated backticked
  names; partial entries are `name (caveat)` joined by ` · `.
- Keep the "Not listed" paragraph under the table accurate: it names methods
  narwhals does not support on *any* lazy backend, which must not be counted
  as gaps here.
- When implementing a missing method, read the `datafusion-workarounds` skill
  first, add a regression test to `tests/test_smoke.py`, remove the test name
  from `TESTS_THAT_NEED_FIX` (or rerun `update_run_tests.py`), and move the
  README cell, all in one commit.

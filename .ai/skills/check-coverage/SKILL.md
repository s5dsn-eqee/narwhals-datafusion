---
name: check-coverage
description: Audit which parts of the narwhals lazy API this backend implements, partially supports, or declares unsupported, and reconcile that with the README coverage matrix and the narwhals-suite skip list. Use after a narwhals bump, before a release, or when asked "does this backend support X".
argument-hint: "[namespace] (e.g., \"Expr\", \"Expr.str\", \"Expr.dt\", \"Expr.list\", \"LazyFrame\", or omit for all)"
---

# Check narwhals API coverage

The README "API coverage" table is hand-maintained. Rebuild the truth from
three sources and report the differences.

## 1. What narwhals asks of a lazy backend

```bash
grep -nE "^\s+def [a-z_]+\(" narwhals/src/narwhals/_sql/expr.py
grep -nE "^\s+def [a-z_]+\(" narwhals/src/narwhals/_sql/dataframe.py
grep -nE "^\s+def [a-z_]+\(" narwhals/src/narwhals/_compliant/any_namespace.py   # str/dt/list/struct
grep -nE "^\s+def [a-z_]+\(|not_implemented\(\)" narwhals/src/narwhals/_duckdb/*.py  # peer checklist
```

A method with a body in `_sql/*` is inherited; one that is `not_implemented()`
or abstract there must be provided here. DuckDB is the reference for what is
implementable in SQL.

## 2. What this backend declares

```bash
grep -rn "not_implemented()" src/narwhals_datafusion/               # unsupported column
grep -rn -B3 "raise NotImplementedError" src/narwhals_datafusion/   # partial column, caveat in parentheses
```

## 3. What the suite says

Every name in `TESTS_THAT_NEED_FIX` (`run_tests.py`) must be explained by
source 2 or a README "Known limitations" bullet; otherwise it is an
undocumented limitation or a bug.

## Report

```
## <Namespace> coverage

### Mismatches
- `method` — code says <supported|partial|unsupported> (file:line), README says <...>. Proposed cell: "..."
- `method` — in TESTS_THAT_NEED_FIX, no limitation explains it. Likely cause: ...

### New in narwhals since the submodule tag
- `method` — added in X.Y; DuckDB implements it via ...; feasible here because ...

### Unchanged
- namespaces that reconcile
```

Rank by user impact: README says supported but it raises is worst.

## Fixing

- README cells: space-separated backticked names; partial entries
  `name (caveat)` joined by ` · `. Keep the "Not listed" paragraph accurate.
- Implementing a method: read `datafusion-workarounds` first, add a test to
  `tests/test_smoke.py`, rerun `update_run_tests.py`, move the README cell, one
  commit.

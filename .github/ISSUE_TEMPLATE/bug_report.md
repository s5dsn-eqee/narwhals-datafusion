---
name: Bug report
about: A wrong result, an error, or a method the README says is supported but is not
labels: bug
---

## Versions

```
narwhals-datafusion:
narwhals:
datafusion:
extra-functions installed: yes / no
```

## Reproduction

```python
import narwhals as nw
import pyarrow as pa
from datafusion import SessionContext

lf = nw.from_native(SessionContext().from_arrow(pa.table({"a": [1, 2]})))
# ...
```

## Expected

<!-- what polars or another narwhals backend returns -->

## Got

```
<!-- output or full error text -->
```

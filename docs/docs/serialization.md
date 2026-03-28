# Serialization

Values passed between executions (arguments and results) are automatically serialized and deserialized. This page describes the types that are supported and how more complex types are handled.

## Supported types

The following Python types are supported natively:

| Type | Notes |
|------|-------|
| `None`, `bool`, `int`, `float`, `str` | Basic types |
| `list` | Elements serialized recursively |
| `dict` | Supports non-string keys |
| `set`, `frozenset` | |
| `tuple` | Type preserved (not converted to list) |
| `datetime`, `date`, `time` | |
| `timedelta` | |
| `Decimal` | Precision preserved |
| `UUID` | |
| `bytes`, `bytearray` | Stored as fragments (see [Blobs](./blobs.md)) |

## Pydantic models

Pydantic models are automatically detected and serialized. Pydantic models can be passed between executions (even across different workers) as long as the model class is available in both environments.

```python
from pydantic import BaseModel

class Order(BaseModel):
    item: str
    quantity: int

@cf.task()
def create_order(item: str, quantity: int) -> Order:
    return Order(item=item, quantity=quantity)

@cf.task()
def process_order(order: Order):
    print(f"Processing {order.quantity}x {order.item}")
```

## Pandas DataFrames

Pandas DataFrames are serialized using the Parquet format, which preserves column types and is efficient for large datasets. Pandas must be installed in the worker environment.

## Pickle fallback

Any type not explicitly handled above is serialized using Python's `pickle` module. This provides broad compatibility but comes with the usual caveats:

- Both sides must have the same class definition available.
- Pickle is Python-specific and not portable across languages.
- Be cautious about unpickling data from untrusted sources.

The type name is recorded in metadata, which is displayed in Studio to help identify pickled values.

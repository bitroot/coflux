"""Validating values against a handle's declared type.

``Catalog[T]`` and ``Checkpoint[T]`` both hold values that leave the
process and come back, so both check them against ``T`` where pydantic is
available. This is what they share: how the type is named, and what
validates it.
"""

from __future__ import annotations

import typing as t

try:
    import pydantic
except ImportError:  # pragma: no cover - exercised by monkeypatching
    pydantic = None  # type: ignore[assignment]


def type_name(item: t.Any) -> str:
    """``item`` as it was written: ``Churn``, ``dict[int, str]``, ``Any``."""
    if isinstance(item, type) and t.get_origin(item) is None:
        return item.__name__
    return repr(item).replace("typing.", "")


def type_adapter(item: t.Any) -> t.Any:
    """A validator for values of type ``item``, or ``None`` without pydantic.

    Any type pydantic can validate is accepted. A class it doesn't know —
    an asset, an execution handle — is checked by ``isinstance``, which is
    what a bare annotation holding one needs. A model, dataclass or
    TypedDict carries its own config, and pydantic refuses another.
    """
    if pydantic is None:
        return None
    config = pydantic.ConfigDict(arbitrary_types_allowed=True)
    try:
        return pydantic.TypeAdapter(item, config=config)
    except pydantic.errors.PydanticUserError as error:
        if error.code != "type-adapter-config-unused":
            raise
        return pydantic.TypeAdapter(item)

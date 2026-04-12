"""Input factory for requesting user input during execution."""

from __future__ import annotations

import datetime as dt
import json
import typing as t

from .models import Input, ModelSchema
from .serialization import serialize_value
from .state import get_context

T = t.TypeVar("T")
M = t.TypeVar("M", bound=ModelSchema)

_PRIMITIVE_SCHEMAS: dict[type, dict[str, str]] = {
    str: {"type": "string"},
    int: {"type": "integer"},
    float: {"type": "number"},
    bool: {"type": "boolean"},
    dt.date: {"type": "string", "format": "date"},
    dt.time: {"type": "string", "format": "time"},
    dt.datetime: {"type": "string", "format": "date-time"},
}


class InputFactory(t.Generic[T]):
    """Factory for requesting user input.

    Usage:
        # Blocking — waits for response (suspends if needed)
        value = cf.input("Approve deployment?")

        # With a type — subscript binds the schema
        result = cf.input[MyModel]("Configure {name}", name="job")
        result = cf.input[str]("Enter your name:")

        # With a raw JSON Schema
        value = cf.input("Configure:", schema={"type": "object", ...})

        # Non-blocking — returns Input handle
        inp = cf.input.submit("Review {item}", item="order-123")
        inp = cf.input[MyModel].submit("Review {item}", item="order-123")
        value = inp.result()  # blocks until response

        # When placeholder names conflict with parameters (key, title, etc.)
        cf.input("Enter {title}:", values={"title": "Manager"}, title="Role input")
    """

    def __init__(
        self,
        model: type[T] | None = None,
        model_schema: str | None = None,
    ):
        self._model = model
        self._model_schema = model_schema

    @t.overload
    def __getitem__(self, model: type[M]) -> InputFactory[M]: ...

    @t.overload
    def __getitem__(self, model: type[T]) -> InputFactory[T]: ...

    def __getitem__(self, model: type) -> InputFactory:
        """Bind a type: ``cf.input[MyModel](...)`` or ``cf.input[str](...)``."""
        primitive = _PRIMITIVE_SCHEMAS.get(model)
        if primitive is not None:
            schema = json.dumps(primitive)
        else:
            schema = json.dumps(model.model_json_schema())
        return InputFactory(model, model_schema=schema)

    def __call__(
        self,
        template: str,
        *,
        key: str | None = None,
        schema: str | dict | None = None,
        title: str | None = None,
        actions: tuple[str, str] | None = None,
        initial: t.Any = None,
        values: dict[str, t.Any] | None = None,
        **kwargs: t.Any,
    ) -> T:
        """Request input and block until a response is available.

        Args:
            template: Markdown prompt with {placeholder} syntax.
            key: Memoization key for suspension/replay. Auto-generated from
                the input configuration if not provided.
            schema: JSON Schema for validating the response (dict or JSON string).
                Ignored when a type is bound via subscript.
            title: Short title for the input (shown in UI).
            actions: Tuple of (respond_label, dismiss_label) for button text.
            initial: Initial values to pre-populate the form (plain JSON).
            values: Explicit values dict for template substitution. Use this
                when value names conflict with other parameters (key, title, etc.).
            **kwargs: Placeholder values to substitute into the template.
                Merged with ``values`` if both are provided.
        """
        return self.submit(
            template,
            key=key,
            schema=schema,
            title=title,
            actions=actions,
            initial=initial,
            values=values,
            **kwargs,
        ).result()

    def submit(
        self,
        template: str,
        *,
        key: str | None = None,
        schema: str | dict | None = None,
        title: str | None = None,
        actions: tuple[str, str] | None = None,
        initial: t.Any = None,
        values: dict[str, t.Any] | None = None,
        **kwargs: t.Any,
    ) -> Input[T]:
        """Create an input request and return a handle without blocking.

        Args:
            template: Markdown prompt with {placeholder} syntax.
            key: Memoization key for suspension/replay. Auto-generated from
                the input configuration if not provided.
            schema: JSON Schema for validating the response (dict or JSON string).
                Ignored when a type is bound via subscript.
            title: Short title for the input (shown in UI).
            actions: Tuple of (respond_label, dismiss_label) for button text.
            initial: Initial values to pre-populate the form (plain JSON).
            values: Explicit values dict for template substitution. Use this
                when value names conflict with other parameters (key, title, etc.).
            **kwargs: Placeholder values to substitute into the template.
                Merged with ``values`` if both are provided.

        Returns:
            An Input handle that can be used to wait for or poll the response.
        """
        merged = {**(values or {}), **kwargs}
        ctx = get_context()
        schema_str = self._model_schema
        if schema_str is None and schema is not None:
            schema_str = json.dumps(schema) if isinstance(schema, dict) else schema
        serialized = _serialize_placeholders(merged)
        initial_value = _serialize_initial(initial)
        input_id = ctx.submit_input(
            template,
            placeholders=serialized,
            schema=schema_str,
            key=key,
            title=title,
            actions=actions,
            initial=initial_value,
        )
        if self._model is not None:
            return Input[self._model](input_id)
        return Input(input_id)


def _serialize_placeholders(placeholders: dict) -> dict | None:
    """Serialize placeholder values for the protocol."""
    if not placeholders:
        return None
    return {key: serialize_value(value) for key, value in placeholders.items()}


def _serialize_initial(initial: t.Any) -> t.Any:
    """Convert initial value to plain JSON for the protocol.

    Pydantic models are converted via model_dump(); other values are
    passed through as-is (assumed to be JSON-serializable).
    """
    if initial is None:
        return None
    if hasattr(initial, "model_dump"):
        return initial.model_dump(mode="json")
    return initial

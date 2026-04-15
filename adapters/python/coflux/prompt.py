"""Prompt class for requesting user input during execution."""

from __future__ import annotations

import datetime as dt
import json
import typing as t

from .models import Input, ModelSchema
from .serialization import serialize_value
from .state import get_context

T = t.TypeVar("T")

_PRIMITIVE_SCHEMAS: dict[type, dict[str, str]] = {
    str: {"type": "string"},
    int: {"type": "integer"},
    float: {"type": "number"},
    bool: {"type": "boolean"},
    dt.date: {"type": "string", "format": "date"},
    dt.time: {"type": "string", "format": "time"},
    dt.datetime: {"type": "string", "format": "date-time"},
}


class Prompt(t.Generic[T]):
    """A prompt definition for requesting user input.

    Define a prompt statically, then submit it (possibly multiple times)
    with different placeholder values.

    Usage:
        # Simple approval (no model — approve/dismiss only)
        deploy = cf.Prompt("Deploy {service} to production?", title="Deployment")
        deploy(service="api")  # blocks until approved; raises InputDismissed

        # With a model — collects structured input
        review = cf.Prompt("Review {item}", model=ReviewForm, title="Review")
        handle = review.submit(item="order-123")  # non-blocking
        result = handle.result()

        # Fluent interface for per-submission overrides
        review.with_initial(ReviewForm(approved=True)).submit(item="order-456")
        review.with_key("custom-key")(item="order-789")
        review.with_actions("Approve", "Reject").submit(item="order-012")

        # Primitive types as model
        name_prompt = cf.Prompt("Enter your name:", model=str)
        name = name_prompt()
    """

    def __init__(
        self,
        template: str,
        *,
        model: type[T] | None = None,
        title: str | None = None,
        actions: tuple[str, str] | None = None,
        schema: str | dict | None = None,
        # Internal — set via fluent methods
        _key: str | None = None,
        _initial: t.Any = None,
        _model_schema: str | None = None,
    ):
        self._template = template
        self._model = model
        self._title = title
        self._actions = actions
        self._key = _key
        self._initial = _initial

        if _model_schema is not None:
            self._model_schema = _model_schema
        elif model is not None:
            primitive = _PRIMITIVE_SCHEMAS.get(model)
            if primitive is not None:
                self._model_schema = json.dumps(primitive)
            else:
                self._model_schema = json.dumps(model.model_json_schema())
        elif schema is not None:
            self._model_schema = json.dumps(schema) if isinstance(schema, dict) else schema
        else:
            self._model_schema = None

    def _copy(self, **overrides: t.Any) -> Prompt[T]:
        """Create a shallow copy with overrides."""
        return Prompt(
            self._template,
            model=overrides.get("model", self._model),
            title=overrides.get("title", self._title),
            actions=overrides.get("actions", self._actions),
            _key=overrides.get("_key", self._key),
            _initial=overrides.get("_initial", self._initial),
            _model_schema=overrides.get("_model_schema", self._model_schema),
        )

    def with_key(self, key: str) -> Prompt[T]:
        """Return a new Prompt with an explicit memoization key."""
        return self._copy(_key=key)

    def with_initial(self, initial: t.Any) -> Prompt[T]:
        """Return a new Prompt with pre-populated initial values."""
        return self._copy(_initial=initial)

    def with_actions(self, respond: str, dismiss: str) -> Prompt[T]:
        """Return a new Prompt with custom button labels."""
        return self._copy(actions=(respond, dismiss))

    def __call__(self, **kwargs: t.Any) -> T:
        """Submit the prompt and block until a response is available.

        Args:
            **kwargs: Placeholder values to substitute into the template.
        """
        return self.submit(**kwargs).result()

    def submit(self, **kwargs: t.Any) -> Input[T]:
        """Submit the prompt and return a handle without blocking.

        Args:
            **kwargs: Placeholder values to substitute into the template.

        Returns:
            An Input handle that can be used to wait for or poll the response.
        """
        ctx = get_context()
        placeholders = _serialize_placeholders(kwargs) if kwargs else None
        initial_value = _serialize_initial(self._initial)
        input_id = ctx.submit_input(
            self._template,
            placeholders=placeholders,
            schema=self._model_schema,
            key=self._key,
            title=self._title,
            actions=self._actions,
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

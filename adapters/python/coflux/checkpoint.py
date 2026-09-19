"""Step-scoped durable state."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar, Generic, TypeVar, overload

from .state import get_context
from .validation import type_adapter, type_name

T = TypeVar("T")

# Distinguishes ``Checkpoint("x")`` from ``Checkpoint("x", default=None)``.
# The two behave the same — an unset checkpoint reads as ``None`` either
# way — but only the second is a claim that ``None`` is a ``T``.
_UNSET = object()

# Checkpoint names starting with this are the adapter's own. Reserved as a
# namespace rather than name by name, so later internal state doesn't need
# another round of this. Enforced here in ``Checkpoint`` rather than in the
# context's checkpoint_get/set/reset, which are the path the adapter's own
# cursors go through.
RESERVED_PREFIX = "_"


class Checkpoint(Generic[T]):
    """A named value that survives across executions of a step.

    Created via ``cf.Checkpoint(...)``, usually at module level. The name
    identifies storage scoped to the current step and workspace, so the value
    written by one attempt is what the next attempt reads — across retries,
    suspends, recurrences and manual re-runs.

    ::

        cursor = cf.Checkpoint("cursor", default=0)

        @cf.workflow(recurrent=True, delay=60)
        def poll_orders():
            since = cursor.get()
            orders, next_since = fetch_orders(since)
            for order in orders:
                process_order.submit(order)
            cursor.set(next_since)

    Writes are throttled, so a crash can lose up to one throttle window: treat
    a checkpoint as at-least-once and make the code that follows a read safe
    to re-run from it. ``cf.flush()`` gives an explicit boundary where that
    isn't good enough. Whatever is written before an execution suspends,
    returns or fails is delivered.

    Writes are cut into deltas only where the execution could resume from
    them, so a checkpoint is always read back as part of a state the step was
    actually in: one written while a stream item is in a loop body's hands is
    published with the cursor advance that consumes it, and dropped if that
    iteration never finishes.

    A checkpoint is not part of any cache, memo or defer key, and a step that
    resolves from the cache never runs and never sees one.

    ``T`` is the type ``get()`` returns. It's inferred from ``default`` when
    one is given, so ``cursor`` above is a ``Checkpoint[int]``. Spelling it
    out with pydantic installed validates it on both sides: ``set()`` checks
    the value against it and stores plain data (a model's fields, say), and
    ``get()`` validates what it reads with it, so an attempt that reads what
    an older version of the code wrote finds out here. Any type pydantic can
    validate works — a model, a dataclass, a ``dict[str, int]``::

        cursor = cf.Checkpoint[int]("cursor", default=0)
        seen = cf.Checkpoint[set[str]]("seen", default=set())

    A checkpoint with no default reads as ``None`` until it's set, so a
    declared type has to admit that — ``Checkpoint[int | None]("cursor")`` —
    or give a default that it does admit. Without pydantic, ``T`` only
    informs type checkers, as with task arguments and results: the value is
    stored and returned as it is.

    Args:
        name: Checkpoint name, unique within the step. Can't start with
            ``_`` — that prefix is reserved for adapter-managed state.
        default: Value returned when the checkpoint has never been set, or has
            been reset. Client-side only — the server never sees it. With a
            declared type, validated as one here.
    """

    _type: ClassVar[Any] = None
    _adapter: ClassVar[Any] = None

    def __class_getitem__(cls, item: Any) -> type:
        # A subclass that remembers the type argument and what validates it,
        # built here so a type pydantic can't handle fails where the
        # checkpoint is declared. Mirrors ``Catalog[T]``.
        attrs = {"_type": item, "_adapter": type_adapter(item)}
        return type(f"Checkpoint[{type_name(item)}]", (cls,), attrs)

    @overload
    def __init__(self, name: str, *, default: T) -> None: ...

    @overload
    def __init__(self, name: str) -> None: ...

    # ``Any`` rather than ``T | None``: the stored default has to satisfy the
    # ``-> T`` on ``default`` and ``get()``, which it can't when ``T`` is
    # non-optional and no default was given.
    def __init__(self, name: str, *, default: Any = _UNSET) -> None:
        if name.startswith(RESERVED_PREFIX):
            raise ValueError(
                f"checkpoint name {name!r} is reserved: names starting with"
                f" {RESERVED_PREFIX!r} are used for adapter-managed state,"
                " such as the cursors behind stream suspension"
            )
        self._name = name
        self._default = self._validate_default(name, default)

    @classmethod
    def _validate_default(cls, name: str, default: Any) -> Any:
        """The default this checkpoint falls back to, checked against ``T``.

        Checked at declaration rather than at the read it would be returned
        from, since it's part of what makes ``get()`` a ``T``. That includes
        the implicit ``None`` of a checkpoint declared without one: nothing
        has to be stored for a read to return it.
        """
        adapter = cls._adapter
        if adapter is None:
            return None if default is _UNSET else default
        if default is _UNSET:
            try:
                return adapter.validate_python(None)
            except ValueError:
                raise ValueError(
                    f"checkpoint {name!r} has no default, so it reads as None"
                    f" until it is set — which the declared type"
                    f" ({type_name(cls._type)}) doesn't allow: give a default,"
                    " or declare the type as optional"
                ) from None
        return adapter.validate_python(default)

    @property
    def name(self) -> str:
        return self._name

    @property
    def default(self) -> T:
        return self._default

    def get(self) -> T:
        """The current value, or the declared default if it isn't set.

        A checkpoint explicitly set to ``None`` reads back as ``None``; only
        an unset or reset checkpoint falls back to the default.

        With a declared type and pydantic, the stored value is validated as
        a ``T`` — a model comes back as an instance. The default was checked
        when the checkpoint was declared, and is returned as it is.
        """
        try:
            value = get_context().checkpoint_get(self._name)
        except KeyError:
            return self._default
        adapter = self._adapter
        if adapter is not None:
            return adapter.validate_python(value)
        return value

    def is_set(self) -> bool:
        """Whether the checkpoint has a value (including an explicit ``None``)."""
        return get_context().checkpoint_has(self._name)

    def set(self, value: T) -> None:
        """Set the value, replacing anything already there.

        With a declared type and pydantic, the value is validated as a ``T``
        first and stored as plain data — a model becomes its fields — so what
        the next attempt reads doesn't depend on this particular class.
        """
        adapter = self._adapter
        if adapter is not None:
            value = adapter.dump_python(adapter.validate_python(value))
        get_context().checkpoint_set(self._name, value)

    def update(self, fn: Callable[[T], T]) -> T:
        """Set the value to ``fn(current)``, and return what was stored.

        The read-modify-write that most checkpoints do — advancing a
        cursor, accumulating a total — without naming the old value::

            n = count.update(lambda x: x + 1)

        ``fn`` receives the declared default when the checkpoint isn't set,
        exactly as ``get()`` would return it.

        This is a read followed by a write, not an atomic swap: two threads
        of one execution updating the same checkpoint can still lose one of
        the updates. That only arises if you share a checkpoint across
        threads — a task body and a ``cf.stream`` generator, say — in which
        case guard it yourself.
        """
        value = fn(self.get())
        self.set(value)
        return value

    def reset(self) -> None:
        """Clear the checkpoint, so ``get()`` returns the declared default.

        Distinct from ``set(None)``, which stores ``None`` as a value.
        """
        get_context().checkpoint_reset(self._name)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._name!r})"

    def __reduce__(self):
        # Unlike cf.Metric, a checkpoint handle names step-scoped storage
        # rather than describing itself. Serialising one and passing it to
        # another execution would silently rebind it to that execution's step.
        raise TypeError(
            "Checkpoint handles can't be passed between executions — "
            "declare cf.Checkpoint(...) in the target that uses it"
        )


def flush() -> None:
    """Block until buffered state has reached the server.

    Checkpoint writes are throttled and metrics and logs are batched; this
    delivers whatever is outstanding and returns once the server has
    acknowledged it. Useful for pinning a checkpoint before a side effect that
    shouldn't be repeated::

        cursor.set(next_cursor)
        cf.flush()
        send_notification()

    Not needed before suspending, returning or raising — those are flushed
    automatically. Inside a stream loop body it also publishes what is being
    held for the current item, ahead of the cursor advance that would
    normally carry it.
    """
    get_context().flush()


__all__ = ["Checkpoint", "flush"]

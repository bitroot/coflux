"""Step-scoped durable state."""

from __future__ import annotations

from typing import Any, Generic, TypeVar, overload

from .state import get_context

T = TypeVar("T")


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
    returns or fails is always delivered.

    A checkpoint is not part of any cache, memo or defer key, and a step that
    resolves from the cache never runs and never sees one.

    ``T`` is the type ``get()`` returns. It's inferred from ``default`` when
    one is given, so ``cursor`` above is a ``Checkpoint[int]``. Without a
    default the checkpoint can also read as ``None``, so spell that out::

        cursor = cf.Checkpoint[int | None]("cursor")

    As with task arguments and results, this only informs type checkers —
    nothing is enforced at runtime.

    Args:
        name: Checkpoint name, unique within the step.
        default: Value returned when the checkpoint has never been set, or has
            been reset. Client-side only — the server never sees it.
    """

    @overload
    def __init__(self, name: str, *, default: T) -> None: ...

    @overload
    def __init__(self, name: str) -> None: ...

    # ``Any`` rather than ``T | None``: the stored default has to satisfy the
    # ``-> T`` on ``default`` and ``get()``, which it can't when ``T`` is
    # non-optional and no default was given.
    def __init__(self, name: str, *, default: Any = None) -> None:
        self._name = name
        self._default = default

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
        """
        try:
            return get_context().checkpoint_get(self._name)
        except KeyError:
            return self._default

    def is_set(self) -> bool:
        """Whether the checkpoint has a value (including an explicit ``None``)."""
        return get_context().checkpoint_has(self._name)

    def set(self, value: T) -> None:
        """Set the value, replacing anything already there."""
        get_context().checkpoint_set(self._name, value)

    def reset(self) -> None:
        """Clear the checkpoint, so ``get()`` returns the declared default.

        Distinct from ``set(None)``, which stores ``None`` as a value.
        """
        get_context().checkpoint_reset(self._name)

    def __repr__(self) -> str:
        return f"Checkpoint({self._name!r})"

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
    automatically.
    """
    get_context().flush()


__all__ = ["Checkpoint", "flush"]

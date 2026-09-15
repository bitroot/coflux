"""Declared handles to paths in the catalog."""

from __future__ import annotations

import re
import string
import typing as t

from .state import get_context
from .validation import type_adapter, type_name

T = t.TypeVar("T")

# Mirrors the server's rule (`Catalog.validate_path`): slash-separated
# segments of `[A-Za-z0-9_.-]`, none empty, none `.` or `..`, at most 512
# characters. Checked here so a typo fails where the handle is declared or
# bound rather than as a refused request or a wait that never ends.
_PATH_SEGMENT = re.compile(r"^[A-Za-z0-9_.-]+$")
_PATH_MAX_LENGTH = 512


def validate_catalog_path(path: t.Any) -> str:
    """The path, if it is one the catalog accepts; ``ValueError`` if not."""
    if not isinstance(path, str) or not path:
        raise ValueError("catalog path must be a non-empty string")
    if len(path) > _PATH_MAX_LENGTH:
        raise ValueError(
            f"catalog path is too long ({len(path)} > {_PATH_MAX_LENGTH} characters)"
        )
    for segment in path.split("/"):
        if segment in ("", ".", "..") or not _PATH_SEGMENT.match(segment):
            raise ValueError(f"invalid catalog path: {path!r}")
    return path


def _placeholders(template: str) -> list[str]:
    """The placeholder names in ``template``, in order of first appearance.

    A placeholder is a plain ``{name}``: no positional fields, conversions
    or format specs, since a value is substituted as-is.
    """
    names: list[str] = []
    for _, field, spec, conversion in string.Formatter().parse(template):
        if field is None:
            continue
        if not field.isidentifier() or spec or conversion:
            raise ValueError(f"invalid placeholder in catalog path: {template!r}")
        if field not in names:
            names.append(field)
    return names


class _Partial(dict):
    """A ``format_map`` mapping that leaves unbound placeholders in place."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class Catalog(t.Generic[T]):
    """A handle to a path in the catalog, which holds versioned values.

    Declared once, usually at module level, and used from any target::

        models = cf.Catalog("models/{name}")

        @cf.task()
        def train(name: str) -> int:
            return models.at(name=name).publish(fit(name))

        @cf.workflow()
        def evaluate(name: str):
            churn = models.at(name=name)
            score(churn.current())   # the value, as of this execution's snapshot
            churn.next()             # suspend until there's a newer one

    A path is a slash-separated name. A ``{placeholder}`` stands for a
    segment (or part of one) to be filled in later with ``at()``, which
    returns another handle with the same type. Nothing round-trips until a
    handle is used, and a handle with placeholders still unbound refuses to
    be.

    ``T`` is the type of the values at the path. With pydantic installed
    it's validated on both sides: ``publish()`` checks the value against
    it and stores plain data (a model's fields, say), and ``current()``
    validates what it reads with it — this type, in this code, whatever
    the publisher used. So a reader declares the shape it expects and
    finds out at the read if a version doesn't match. Any type pydantic
    can validate works — a model, a dataclass, a ``dict[int, str]``::

        class Churn(BaseModel):
            auc: float
            threshold: float

        models = cf.Catalog[Churn]("models/{name}")
        counts = cf.Catalog[dict[str, int]]("counts/{name}")

    Without pydantic, ``T`` only informs type checkers, as with task
    arguments and results: the value is stored and returned as it is.

    A handle is also a handle for ``cf.select``: it resolves when the path
    has a version this execution hasn't seen — on an empty path, the first
    — so it is ``next()`` in select form. It isn't a value: pass the path
    or the value it holds to a task, not the handle.
    """

    _type: t.ClassVar[t.Any] = None
    _adapter: t.ClassVar[t.Any] = None

    def __class_getitem__(cls, item: t.Any) -> type:
        # A subclass that remembers the type argument and what validates
        # it, built here so a type pydantic can't handle fails where the
        # handle is declared. Mirrors ``Input[T]``.
        attrs = {"_type": item, "_adapter": type_adapter(item)}
        return type(f"Catalog[{type_name(item)}]", (cls,), attrs)

    def __init__(self, path: str):
        if not isinstance(path, str) or not path:
            raise ValueError("catalog path must be a non-empty string")
        self._placeholders = _placeholders(path)
        # The literal parts have to be valid wherever they are, so check the
        # template with each placeholder standing in for itself. With none,
        # this is the path itself.
        validate_catalog_path(
            path.format(**{name: name for name in self._placeholders})
        )
        self._template = path

    @property
    def template(self) -> str:
        """The path as declared, placeholders included."""
        return self._template

    @property
    def path(self) -> str:
        """The path, once every placeholder is bound; ``ValueError`` before."""
        if self._placeholders:
            unbound = ", ".join(self._placeholders)
            raise ValueError(
                f"catalog path {self._template!r} has unbound placeholders "
                f"({unbound}); bind them with .at()"
            )
        return self._template

    def at(self, **placeholders: t.Any) -> Catalog[T]:
        """A handle to this path with the given placeholders filled in.

        Values are substituted as strings and the result has to be a valid
        path, so a value can't be empty or hold anything a path segment
        can't. Placeholders left out stay unbound, to be filled in by a
        later ``at()``.
        """
        unknown = [name for name in placeholders if name not in self._placeholders]
        if unknown:
            raise ValueError(
                f"unknown placeholder(s) for catalog path {self._template!r}: "
                + ", ".join(unknown)
            )
        values: dict[str, str] = {}
        for name, value in placeholders.items():
            text = str(value)
            if "{" in text or "}" in text:
                raise ValueError(
                    f"invalid value for catalog placeholder {name}: {text!r}"
                )
            values[name] = text
        return type(self)(self._template.format_map(_Partial(values)))

    def publish(self, value: T) -> int:
        """Publish ``value`` at this path and return the version's number.

        ``value`` is anything that can be passed to a task: an asset, a
        data structure holding assets, a reference to something external,
        a plain number. Facts about a publish — a metric, what it was built
        from — go in the value too, alongside the thing itself. Publishing
        what is already the visible head — the same value — writes nothing
        and returns the existing version's number, which is what makes a
        publish safe to re-run.

        With pydantic, the value is validated as a ``T`` first and stored
        as plain data — a model becomes its fields — so what's in the
        catalog doesn't depend on this particular class.

        The catalog pins the value, not what the value points at: a handle
        (an execution, an input) resolves to whatever it resolves to when
        read, and a locator for external data is only as stable as that
        data.
        """
        path = self.path
        adapter = self._adapter
        if adapter is not None:
            value = adapter.dump_python(adapter.validate_python(value))
        return get_context().catalog_publish(path, value)

    def current(self) -> T:
        """The value at this path as of the execution's snapshot.

        An execution sees the catalog as it was when it was assigned, plus
        anything its own run has published since, so repeated calls agree,
        and a version published after that is not current here — ``next()``
        is how to get an execution that sees it. With nothing published
        yet this waits for the first publish — blocking outside a
        ``cf.suspense`` scope, suspending inside one.

        With pydantic, the value is validated as a ``T`` — a model comes
        back as an instance — so a version that doesn't fit the type this
        reader declares is an error here rather than later.
        """
        value = get_context().catalog_current(self.path)
        adapter = self._adapter
        if adapter is not None:
            return adapter.validate_python(value)
        return value

    def next(self) -> t.NoReturn:
        """Suspend until this path has a version newer than the execution
        can see, then re-run.

        Always suspends, inside a ``cf.suspense`` scope or not: what comes
        next is by definition outside this execution's snapshot, so only
        a new execution can see it. If a newer version already exists the
        execution resumes straight away. Never returns.
        """
        get_context().catalog_next(self.path)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Catalog)
            and other._template == self._template
            and other._type == self._type
        )

    def __hash__(self) -> int:
        return hash(("catalog", self._template))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._template!r})"

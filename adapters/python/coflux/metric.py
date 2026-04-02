"""Metric classes for recording metric data points."""

from __future__ import annotations

from typing import Any, Iterator, TypeVar

from . import protocol
from .state import get_context

T = TypeVar("T")


class MetricGroup:
    """Configuration for a metric group (shared x-axis / chart).

    Args:
        name: Group name (required). Metrics in the same group are
            displayed together.
        units: Optional units label for the x-axis (e.g., "epoch").
        lower: Optional lower bound for the x-axis.
        upper: Optional upper bound for the x-axis.
    """

    def __init__(
        self,
        name: str,
        *,
        units: str | None = None,
        lower: float | None = None,
        upper: float | None = None,
    ):
        self.name = name
        self.units = units
        self.lower = lower
        self.upper = upper

    def _config(self) -> dict:
        return {"units": self.units, "lower": self.lower, "upper": self.upper}


class MetricScale:
    """Configuration for a metric scale (shared y-axis).

    Args:
        name: Optional scale name. Metrics sharing a scale name within a
            group share a y-axis. Without a name, the scale is private to
            the metric.
        units: Optional units label for the y-axis (e.g., "%", "ms").
        progress: Whether this scale represents progress toward ``upper``.
        lower: Optional lower bound for the y-axis.
        upper: Optional upper bound for the y-axis.
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        units: str | None = None,
        progress: bool = False,
        lower: float | None = None,
        upper: float | None = None,
    ):
        self.name = name
        self.units = units
        self.progress = progress
        self.lower = lower
        self.upper = upper

    def _config(self) -> dict:
        return {
            "units": self.units,
            "progress": self.progress,
            "lower": self.lower,
            "upper": self.upper,
        }


class Metric:
    """A metric descriptor that can record values against the current execution.

    Created via ``cf.Metric(...)``. The metric definition (metadata) is lazily
    sent to the server on the first ``record()`` call in each execution.
    The object is serialisable and can be passed between executions.

    Args:
        key: Metric key name (e.g., "loss", "accuracy").
        group: Optional group (str or MetricGroup). Metrics in the same group
            are displayed together and share an x-axis.
        scale: Optional scale (str or MetricScale). Metrics sharing a named
            scale within a group share a y-axis. Without a scale, each metric
            gets its own y-axis.
    """

    def __init__(
        self,
        key: str,
        *,
        group: str | MetricGroup | None = None,
        scale: str | MetricScale | None = None,
    ):
        self._key = key
        if isinstance(group, str):
            self._group = MetricGroup(group)
        else:
            self._group = group
        if isinstance(scale, str):
            self._scale = MetricScale(scale)
        else:
            self._scale = scale

    @property
    def key(self) -> str:
        return self._key

    def record(self, value: float, *, at: float | None = None) -> None:
        """Record a metric data point.

        Args:
            value: Metric value (float).
            at: Optional x-axis value. If omitted, defaults to seconds since
                execution start.
        """
        ctx = get_context()
        self._ensure_defined(ctx)
        protocol.send_metric(ctx.execution_id, self._key, value, at=at)

    def _build_definition(self) -> dict[str, Any]:
        definition: dict[str, Any] = {}
        if self._group is not None:
            definition["group"] = self._group.name
            if self._group.units is not None:
                definition["group_units"] = self._group.units
            if self._group.lower is not None:
                definition["group_lower"] = self._group.lower
            if self._group.upper is not None:
                definition["group_upper"] = self._group.upper
        if self._scale is not None:
            definition["scale"] = self._scale.name
            if self._scale.units is not None:
                definition["units"] = self._scale.units
            if self._scale.progress:
                definition["progress"] = True
            if self._scale.lower is not None:
                definition["lower"] = self._scale.lower
            if self._scale.upper is not None:
                definition["upper"] = self._scale.upper
        return definition

    def _build_config(self) -> dict:
        return {
            "group_name": self._group.name if self._group else None,
            "group": self._group._config() if self._group else None,
            "scale_name": self._scale.name if self._scale else None,
            "scale": self._scale._config() if self._scale else None,
        }

    def _ensure_defined(self, ctx: Any) -> None:
        config = self._build_config()
        existing = ctx._defined_metrics.get(self._key)
        if existing is not None:
            if existing != config:
                raise ValueError(
                    f"Metric '{self._key}' already defined in this execution "
                    f"with different configuration"
                )
            return

        if self._group is not None:
            group_config = self._group._config()
            existing_group = ctx._defined_groups.get(self._group.name)
            if existing_group is not None and existing_group != group_config:
                raise ValueError(
                    f"Group '{self._group.name}' already defined in this execution "
                    f"with inconsistent configuration"
                )
            ctx._defined_groups[self._group.name] = group_config

        if self._scale is not None and self._scale.name is not None:
            scale_config = self._scale._config()
            existing_scale = ctx._defined_scales.get(self._scale.name)
            if existing_scale is not None and existing_scale != scale_config:
                raise ValueError(
                    f"Scale '{self._scale.name}' already defined in this execution "
                    f"with inconsistent configuration"
                )
            ctx._defined_scales[self._scale.name] = scale_config

        protocol.send_define_metric(
            ctx.execution_id, self._key, self._build_definition()
        )
        ctx._defined_metrics[self._key] = config


def progress(
    iterable: Any,
    key: str = "progress",
    *,
    group: str | MetricGroup | None = None,
) -> Iterator[T]:
    """Wrap an iterable and auto-record progress as items are consumed.

    Creates a progress metric and yields each item from the iterable.
    After each item, records the count of items consumed so far.

    Args:
        iterable: Any iterable (or sized collection) to wrap.
        key: Metric key name. Defaults to "progress".
        group: Optional group (str or MetricGroup).

    Yields:
        Items from the iterable.
    """
    if not hasattr(iterable, "__len__"):
        iterable = list(iterable)
    total = len(iterable)
    m = Metric(key, group=group, scale=MetricScale(progress=True, upper=total))
    for i, item in enumerate(iterable):
        yield item
        m.record(i + 1)

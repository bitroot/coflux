# Metrics

You can record metrics from within tasks and workflows. Metrics are streamed in real-time and rendered as charts in Studio.

## Recording metrics

Create a `Metric` and call `record()`:

```python
import coflux as cf

loss = cf.Metric("loss", group="training")
accuracy = cf.Metric("accuracy", group="training")

@cf.task()
def train(epochs):
    for epoch in range(epochs):
        loss_val, acc_val = run_epoch(epoch)
        loss.record(loss_val, at=epoch)
        accuracy.record(acc_val, at=epoch)
```

By default, values are associated with the time since the execution started. The `at=` parameter lets you specify an explicit x-value — useful when plotting against epochs, steps, or any other dimension.

## Groups and scales

Metrics within the same _group_ are displayed together on a shared chart. You can configure the axes with `MetricGroup` and `MetricScale`:

```python
group = cf.MetricGroup("training", units="epoch")

loss = cf.Metric("loss", group=group, scale=cf.MetricScale(units=""))
accuracy = cf.Metric("accuracy", group=group, scale=cf.MetricScale(units="%", lower=0, upper=100))
```

`MetricGroup` configures the x-axis (shared across metrics in the group):

| Parameter | Description |
|-----------|-------------|
| `name` | Group name (required) |
| `units` | X-axis unit label |
| `lower` | X-axis lower bound |
| `upper` | X-axis upper bound |

`MetricScale` configures the y-axis. Metrics sharing a scale name within a group share a y-axis:

| Parameter | Description |
|-----------|-------------|
| `name` | Scale name (metrics with the same name share a y-axis) |
| `units` | Y-axis unit label |
| `lower` | Y-axis lower bound |
| `upper` | Y-axis upper bound |
| `progress` | Whether to render as a progress bar |

## Progress

The `progress()` helper wraps an iterable and automatically records a progress metric as items are consumed:

```python
@cf.task()
def process_batch(items):
    for item in cf.progress(items):
        handle(item)
```

This renders as a progress bar in Studio. You can customise the metric key and group:

```python
for item in cf.progress(items, key="items_processed", group="processing"):
    handle(item)
```

## Throttling

Metrics are throttled client-side to avoid flooding the server in tight loops. By default, each metric reports up to 10 values per second. This can be adjusted per metric:

```python
metric = cf.Metric("fast_metric", throttle=100)   # 100 values/sec
metric = cf.Metric("exact_metric", throttle=None)  # No throttling
```

## Metric store

Currently a built-in metric store is supported. The intention is to support integrating with external stores in future.

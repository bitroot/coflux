# Logging

Log messages can be recorded from tasks using `log_debug`, `log_info`, `log_warning` and `log_error`. In each case the function accepts a 'template' and a set of values. Values will be substituted into the template, or shown alongside if a placeholder isn't present in the template. For example:

```python
import coflux as cf

cf.log_info(
  "{count} bottles of {drink} on the wall",
  count=99, drink='beer', sunny=True, temperature=12.3
)
```

These messages will appear in Studio like this:

<img src="/img/beer_logs.png" alt="Log messages" width="500" />

Logs can be viewed aggregated across all steps within the run, or for a specific step execution.


:::note
Apart from the convenience of separating templates from labels in code, doing so also has the benefit that log messages take up less disk space. But it's also fine to dynamically construct the log message yourself.
:::

## Stdout/stderr

Tasks are run in sub-processes, and Coflux makes a best effort to capture output that's sent to stdout/stderr (e.g., via `print`) streams and send it back to the server. These appear amongst logs.

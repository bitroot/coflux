<p align="center">
  <img src="logo.svg" width="250" alt="Coflux" />
</p>

Coflux is an open-source workflow engine. Use it to orchestrate and observe computational workflows defined in plain Python. Suitable for data pipelines, background tasks, agentic systems.

```python
import coflux as cf
import requests

@cf.task(retries=2)
def fetch_splines(url: str) -> list:
    return requests.get(url).json()

@cf.task()
def reticulate(splines: list) -> list:
    return list(reversed(splines))

@cf.workflow()
def my_workflow(url: str) -> None:
    reticulate(fetch_splines(url))
```

Refer to the [docs](https://docs.coflux.com) for more details.

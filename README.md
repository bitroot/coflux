<p align="center">
  <img src="logo.svg" width="250" alt="Coflux" />
</p>

<p align="center">
  <strong>Open-source workflow engine for Python</strong><br>
  Orchestrate and observe computational workflows defined in plain Python.<br>
  Suitable for data pipelines, background tasks, AI agents, etc.
</p>

<p align="center">
  <a href="https://docs.coflux.com">Docs</a> &middot;
  <a href="https://studio.coflux.com">Studio</a> &middot;
  <a href="https://pypi.org/project/coflux/">PyPI</a> &middot;
  <a href="https://coflux.com">Website</a>
</p>

## Why Coflux?

- **Plain Python** — Workflows are regular Python functions with decorators. No DSLs, no YAML, no static DAGs.
- **Low latency** — Millisecond task startup using warm executor processes.
- **Real-time observability** — Watch workflows execute live in [Coflux Studio](https://studio.coflux.com), with graph visualisation, logs, and results.
- **Self-hosted** — You run the server; your data stays in your infrastructure.
- **Workspace inheritance** — Branch production into development workspaces and re-run individual steps with real data.

## Quick example

A **workflow** is the entry point for a run. It can call **tasks**, which are the individual operations that Coflux orchestrates — scheduling them across workers and tracking results, dependencies, retries, etc. When you call a task from a workflow, Coflux intercepts the call and executes it remotely:

```python
import coflux as cf

@cf.task(retries=cf.Retries(3, when=ConnectionError))
def fetch_data(url: str) -> dict:
    return requests.get(url).json()

@cf.task(cache=True)
def transform(data: dict) -> list:
    return sorted(data["items"], key=lambda x: x["score"], reverse=True)

@cf.workflow()
def my_pipeline(url: str):
    transform(fetch_data(url))
```

Tasks and workflows are just functions — call them directly in tests, or let Coflux orchestrate them across workers.

## Features

### Caching

Reuse results across runs, with TTLs, parameter filtering, and cross-workspace cache sharing:

```python
# cache indefinitely
@cf.task(cache=True)
def get_user(user_id): ...

# cache for 10 minutes
@cf.task(cache=600)
def fetch_prices(): ...

# cache by specific params only
@cf.task(cache=cf.Cache(params=["product_id"]))
def get_product(product_id, include_reviews=False): ...
```

### Retries with conditions

Retry on specific exceptions with configurable backoff:

```python
@cf.task(retries=cf.Retries(5, backoff=(1, 60), when=TransientError))
def call_api():
    ...
```

### Parallel execution

Submit tasks concurrently and collect results:

```python
@cf.workflow()
def process_order(user_id, product_id):
    user = load_user.submit(user_id)              # starts immediately
    product = load_product.submit(product_id)     # starts immediately
    create_order(user.result(), product.result()) # waits for results before calling
```

### Assets

Share files and directories between tasks, with glob filtering and composition:

```python
@cf.task()
def generate_report() -> cf.Asset:
    Path("report.csv").write_text(build_csv())
    return cf.asset(match="*.csv")

@cf.workflow()
def my_workflow():
    report = generate_report()
    paths = report.restore()  # download files locally
```

### Memoisation

Memoise task calls within a single run (unlike caching, which works across runs). Useful for debugging workflows without repeating side effects — re-run steps within the workflow and memoised steps return their previous result:

```python
@cf.task(memo=True)
def send_email(recipient):
    mailer.send(recipient.email, ...)

@cf.workflow()
def notify(campaign_id):
    for r in get_recipients(campaign_id):
        send_email.submit(r)
```

### Groups

Organise parallel tasks into groups:

```python
@cf.workflow()
def map_reduce(n: int):
    with cf.group("Process chapters"):
        results = [process.submit(i) for i in range(n)]
    return merge([r.result() for r in results])
```

### Structured logging

```python
cf.log_info("Processing {count} items for {user}", count=42, user="alice")
```

### And more

- **Debouncing**: defer execution until a task stops being called (`defer=True`)
- **Recurrence**: automatically re-execute workflows for polling (`recurrent=True`)
- **Suspense**: pause a task and free resources while waiting (`cf.suspend()`)
- **Worker pools**: auto-launch and manage workers with Docker or process launchers

## Getting started

### 1. Install the CLI

```bash
curl -fsSL https://coflux.com/install.sh | sh
```

Or download a binary from the [releases page](https://github.com/bitroot/coflux/releases).

### 2. Start the server

Use the CLI to start the server:

```bash
coflux server --no-auth --project myproject
```

Or run it with Docker:

```bash
docker run -p 7777:7777 ghcr.io/bitroot/coflux --no-auth --project myproject
```

### 3. Create a workflow

```python
# myapp/workflows.py
import coflux as cf

@cf.task()
def greet(name: str) -> str:
    return f"Hello, {name}!"

@cf.workflow()
def hello(name: str):
    print(greet(name))
```

### 4. Start a worker

```bash
pip install coflux
coflux worker --dev myapp.workflows
```

The `--dev` flag watches for code changes and automatically restarts the worker.

### 5. Submit a run

Arguments are JSON values. The module/target path uses `/` as a separator:

```bash
coflux submit myapp/hello '"world"'
```

### 6. Open Studio

Visit [studio.coflux.com](https://studio.coflux.com), enter your server address (`localhost:7777`), and watch your workflow execute in real time.

## License

Apache 2.0

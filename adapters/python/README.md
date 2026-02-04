# Coflux Python SDK

The Coflux Python SDK provides decorators and utilities for defining workflows and tasks.

## Installation

```bash
pip install coflux
```

Or for development:

```bash
cd adapters/python
poetry install
```

## Usage

### Defining Tasks and Workflows

```python
from coflux import task, workflow, log_info

@task()
def process_item(item_id: int) -> dict:
    log_info("Processing item {id}", id=item_id)
    return {"id": item_id, "status": "done"}

@workflow()
def batch_process(items: list[int]) -> list[dict]:
    results = []
    for item_id in items:
        result = process_item(item_id)
        results.append(result)
    return results
```

### CLI Commands

The SDK is invoked by the Go CLI via subprocess:

```bash
# Discovery - find all @task/@workflow targets
python -m coflux discover myapp.workflows myapp.tasks

# Execution - run as executor process
python -m coflux execute
```

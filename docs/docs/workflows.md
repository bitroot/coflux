# Defining workflows

Workflows are defined in Python using decorators provided by the `coflux` package. This page covers how to define workflows, tasks, and modules in more detail.

## Workflows and tasks

A function decorated with `@cf.workflow()` is the entry point for a run. A function decorated with `@cf.task()` is an operation that can be called from a workflow or another task.

```python
import coflux as cf

@cf.task()
def fetch_data(url: str):
    ...

@cf.workflow()
def process(url: str):
    data = fetch_data(url)
    ...
```

Workflows can call tasks, tasks can call other tasks, and tasks can call workflows (which will submit a separate run). Workflows and tasks are collectively referred to as _targets_.

You can think of the distinction between workflows and tasks like the distinction between public and private functions — workflows are the entry points that can be submitted, while tasks are internal operations.

### Docstrings

The docstring of a workflow is displayed in Studio when submitting a run. This is a good place to explain what the workflow does and what arguments it expects.

```python
@cf.workflow()
def process(url: str):
    """
    Fetches data from `url` and processes it.
    """
    ...
```

### Running outside Coflux

The decorators are designed to be unimposing — decorated functions can be called directly outside of a Coflux context (e.g., in tests or scripts). When called outside of an execution context, tasks execute directly rather than being scheduled as steps.

## Modules

Targets are defined in _modules_. Typically these correspond to Python modules (i.e., `.py` files).

Modules are specified when starting a worker:

```bash
coflux worker --dev myapp.workflows myapp.tasks
```

Or in `coflux.toml`:

```toml
modules = ["myapp.workflows", "myapp.tasks"]
```

Each module's targets (workflows and tasks) are declared by the worker when it connects to the server so that the server knows what targets the worker is able to handle. The workflows can also be registered with the server so they appear in Studio and can be submitted.

## Stubs

A _stub_ allows you to reference a target in another module without importing it. This is useful for separating dependencies between modules that may run on different workers or have different package requirements.

For example, given an `other.workflows` module with a task:

```python
# other/workflows.py

@cf.task()
def random_int(max: int) -> int:
    return random.randint(1, max)
```

Another module can reference this task with a stub:

```python
# example/workflows.py

@cf.stub("other.workflows")
def random_int(max: int) -> int:
    ...

@cf.workflow()
def roll_die():
    if random_int(6).result() == 6:
        print("You won")
    else:
        print("You lost")
```

When called in the context of a workflow, the stub schedules the real target for execution — the stub's function body is not executed. However, the body can be useful for providing a dummy implementation when running outside of Coflux (e.g., in tests):

```python
@cf.stub("other.workflows")
def random_int(max: int) -> int:
    return 4  # Dummy value for testing
```

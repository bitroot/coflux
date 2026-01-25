# 3. Defining workflows

Workflows are defined in code, using Python functions, which are decorated to indicate their operation type and configuration. A function decorated as a _workflow_ is the entry point for a run, and a function decorated as a _task_ is an operation to be executed within the run. Workflows can call tasks, tasks can call other tasks, and tasks can also call workflows (which will submit a separate run).

The decorators are intended to be unimposing so that functions can be executed outside of Coflux.

## An example

Here's a simple example:

```python title="hello.py"
import coflux as cf

@cf.task()
def build_greeting(name: str):
    return f"Hello, {name}"

@cf.workflow()
def print_greeting(name: str):
    """
    Prints a greeting for the specified `name`.
    """
    print(build_greeting(name))
```

This defines a `print_greeting` workflow, which takes a `name` as an argument. When run, it calls the `build_greeting` task, passing through the name argument. Once it has the result from the task, the result gets printed.

Workflows are defined in _modules_. Typically these are Python modules, but they can alternatively be loaded from a Python script, which this guide will demonstrate.

Put the workflow above into `hello.py`.

:::tip
The docstring of a workflow will be available in the UI when running a workflow. This is a great place to explain what the workflow does.
:::

Before coming back to more advanced features, let's see how to get this workflow running...

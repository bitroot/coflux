# Stubs

A stub allows you to define a reference to a step that's in another module (in a separate codebase) by its name. Using stubs can make it easier to separate 3rd-party dependencies since you don't need to `import` the target module into into the module you're calling it from.

For example, given an `other.workflows` module with a random number generator:

```python
# other/workflows.py

@cf.task()
def random_int(max: int) -> int:
    return random.randint(1, max)
```

Another module could reference this function with a stub, and then call it:

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

## Stub implementations

When you call the stub in the context of a workflow, the function itself won't be executed, so the body of the function isn't important. However, being able to implement the function is useful when you want to be able to run your code outside of the context of a workflow. For example, as part of a test, you could return some dummy data.

```python
@cf.stub("other.workflows")
def random_int(max: int) -> int:
    return 4  # Dummy value for testing
```

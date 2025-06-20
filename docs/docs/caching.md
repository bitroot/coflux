# Caching results

Caching can be used to:

- Avoid re-computing something that we can assume hasn't changed.
- Avoid making an external request too frequently (e.g., to an API that implements rate limiting).

:::note
Caching can also be used to avoid executing some side-effect too frequently by including a time-based argument (e.g., today's date), although [deferring](/deferring) may be a more effective approach.
:::

The simplest way to enable caching is by setting the `cache` option to `True`:

```python
@cf.workflow(cache=True)
def add_numbers(a, b):
    return a + b
```

If this step has previously run with the same arguments, the step won't be re-executed - instead, the result will be taken from that previous execution.

## Maximum age

Alternatively, instead of passing `True`, a _maximum age_ can be specified, either with a numeric value, corresponding to seconds; or as a `datetime.timedelta`. In this case, a previous result (for the same arguments) will only be used if it's within this age:

```python
import datetime as dt

@cf.task(cache=dt.timedelta(minutes=10))
def fetch_latest_posts(user_id):
    ...
```

This will re-use a (successful) request (for a user) as long as it's within the last ten minutes. Alternatively this could have been specified in seconds as `cache=600`.

## Expiration

Unlike a typical cache, results themselves don't expire. This gives more flexibility, as the age can be changed retrospectively. However, you can achieve an expiration-like mechanism by adding time-based task arguments. For example, adding a date parameter (and having the caller determine the current date) means you automatically stop using the cached result at the end of each day (subject to timezone considerations):

```python
import datetime as dt

@cf.task(cache=True)
def get_headlines(date):
    ...

@cf.workflow()
def my_workflow():
    headlines = get_headlines(dt.date.today())
    ...
```

## Pending executions

In the case of tasks being delayed (either intentionally, or while waiting to be assigned), pending executions with caching enabled will become linked together _before_ execution has completed (or started). For example, with a recursive Fibonacci task, multiple tasks for the same _n_ value that are queued at the same time will be linked together:

```python
@cf.task(cache=True)
def fib(n: int) -> int:
    if n == 0 or n == 1:
        return n
    else:
        return fib(n - 1) + fib(n - 2)
```

This property is generally desirable, but it means that if the first task fails, they both fail.

## Cache parameters

By default, all parameters are considered for a cache match, but if needed specific parameters can be specified. For example:

```python
@cf.task(cache=True, cache_params=["product_id"])
def fetch_product(product_id, url):
    ...
```

In this case, only the `product_id` parameter will be used - a different `url` won't affect the cache lookup.

:::note
The names of arguments can be changed without affecting the cache - this is because the names are translated to indexes.

Additionally, if the order of parameters needs to be changed, the cache can be maintained by specifying (or rearranging) the `cache_params`. In the following three versions of `my_task` the addition of a parameter, and then rearranging, won't effect the cache:

```python
# before change
@cf.task(cache=True)
def my_task(a, b):
    #...
```

```python
@cf.task(cache=True, cache_params=["a", "b"])
def my_task(a, b, c):
    # ...
```

```python
@cf.task(cache=True, cache_params=["a", "b2"])
def my_task(c, b2, a):
    # ...
```
:::

## Cache namespaces

Each cache key is considered within a namespace. By default this namespace consists of the module name and the task name (in the format `module:target`). In some cases it might be necessary to override this namespace. For example, if you need to rename a function (or module), but you want to retain the cache:

```python
@cf.task(cache=True, cache_namespace="example1.workflows:task_name")
def new_task_name():
    ...
```

Two task can share the same namespace. In this case, calling either task (with the same argument) will resolve to the same cached result (if present):

```python
@cf.task(cache=True, cache_namespace="my_namespace")
def task_a(a):
    ...

@cf.task(cache=True, cache_namespace="my_namespace")
def task_b(b):
    ...
```

## Cache versions

When the implementation of a task changes, it may be desirable to reset the cache. This can be achieved by setting a `cache_version`:

```python
@cf.task(cache=True, cache_version="v2")
def my_task():
    # ...
```

## Space inheritance

The inheritance of spaces affects caching - refer to [the explanation on the concepts page](/concepts#space-inheritance).

## Forcing execution

If you need to re-evaluate a task that's cached, you can do so by 're-running' the cached step. The cache settings will be ignored. Any subsequent cache hits for that task will use the result from this re-run.

## Cache hit requirements

To summarise, the requirements for a cache hit (i.e., for a previous result to be reused, instead of executing a step) are that:

1. The result must be in the same space, or an ancestral space, within the same project.
2. The result must also have had caching enabled.
3. The cache key and namespace (as described above) must match.
4. The result must not have failed (i.e., either it was successful, it's scheduled, or it's in progress).
5. The time of the result must be within the maximum age, if specified.

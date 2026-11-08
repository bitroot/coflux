import os
import platform
import time

import coflux as cf


@cf.task()
def describe_host():
    return {
        "platform": platform.platform(),
        "function": os.environ.get("AWS_LAMBDA_FUNCTION_NAME"),
        "memory": os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE"),
    }


@cf.task()
def square(n: int) -> int:
    return n * n


@cf.workflow()
def nap(seconds: float) -> float:
    # Longer than the function's timeout, this is cut short at what's left
    # of the invocation, and times out rather than being lost with it.
    time.sleep(seconds)
    return seconds


@cf.workflow()
def hello_lambda(name: str = "world"):
    return [f"Hello, {name}!", describe_host()]


@cf.workflow()
def fan_out(count: int = 5):
    return sum(r.result() for r in [square.submit(i) for i in range(count)])

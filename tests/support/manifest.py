def _target(
    module,
    name,
    type,
    parameters=None,
    *,
    retries=None,
    cache=None,
    memo=None,
    delay=None,
    recurrent=False,
    wait_for=None,
    requires=None,
    timeout=None,
    streams=None,
):
    target = {
        "module": module,
        "name": name,
        "type": type,
        "parameters": [{"name": p} for p in (parameters or [])],
    }
    if retries is not None:
        target["retries"] = retries
    if cache is not None:
        target["cache"] = cache
    if memo is not None:
        target["memo"] = memo
    if delay is not None:
        target["delay"] = delay
    if recurrent:
        target["recurrent"] = True
    if wait_for is not None:
        target["wait_for"] = wait_for
    if requires is not None:
        target["requires"] = requires
    if timeout is not None:
        target["timeout"] = timeout
    if streams is not None:
        target["streams"] = streams
    return target


def workflow(module, name, parameters=None, **kwargs):
    return _target(module, name, "workflow", parameters, **kwargs)


def task(module, name, parameters=None, **kwargs):
    return _target(module, name, "task", parameters, **kwargs)


def manifest(targets):
    return {"targets": targets}

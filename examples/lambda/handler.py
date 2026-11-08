"""A Lambda handler that runs a Coflux worker for a Lambda pool.

The server invokes the function with the worker's arguments (the modules
to host) and environment (what it needs to connect back). This runs
``coflux worker`` with them and returns when it exits - normally when the
server asks it to, once it has done what it was launched for.
"""

import os
import subprocess

# Seconds before the function's timeout to stop a worker that hasn't
# already left, so that it's gone - along with the adapter processes it
# runs - before Lambda freezes the environment.
STOP_MARGIN = 5
KILL_AFTER = 3


def handler(event, context):
    env = {**os.environ, **event.get("env", {})}
    proc = subprocess.Popen(["coflux", "worker", *event.get("args", [])], env=env)

    timeout = max(0, context.get_remaining_time_in_millis() / 1000 - STOP_MARGIN)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=KILL_AFTER)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    if proc.returncode != 0:
        # Raising makes it a function error, which is reported as the
        # worker's.
        raise RuntimeError(f"worker exited with code {proc.returncode}")
    return {"exitCode": 0}

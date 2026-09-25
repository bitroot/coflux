"""Runs the reference Lambda handler for one invocation of the fake Lambda.

Reads the event from stdin, calls the handler with a context counting
down to the fake's deadline, and writes what came of it - a value, or an
error in the shape Lambda reports one - to the result file.
"""

import json
import os
import sys
import time
import traceback


class Context:
    def __init__(self, deadline_ms):
        self._deadline_ms = deadline_ms

    def get_remaining_time_in_millis(self):
        return max(0, self._deadline_ms - int(time.time() * 1000))


def main():
    event = json.load(sys.stdin)
    context = Context(int(os.environ["FAKE_LAMBDA_DEADLINE_MS"]))
    result_path = os.environ["FAKE_LAMBDA_RESULT"]
    sys.path.insert(0, os.environ["FAKE_LAMBDA_HANDLER_DIR"])

    try:
        if os.environ.get("FAKE_LAMBDA_HANDLER_ERROR"):
            raise RuntimeError(os.environ["FAKE_LAMBDA_HANDLER_ERROR"])
        import handler

        result = {"value": handler.handler(event, context)}
    except Exception as e:  # noqa: BLE001 - Lambda reports whatever the handler raised
        traceback.print_exc()
        result = {"error": {"errorType": type(e).__name__, "errorMessage": str(e)}}

    with open(result_path, "w") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main()

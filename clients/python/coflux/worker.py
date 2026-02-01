import asyncio
import base64
import random
import traceback
import typing as t
import urllib.parse

import websockets

from . import config, execution, models, server, types
from .version import API_VERSION, check_server


def _parse_reference(reference: t.Any) -> types.Reference:
    match reference:
        case ["execution", execution_id, run_id, step_id, attempt, module, target]:
            metadata = models.ExecutionMetadata(
                run_id=run_id,
                step_id=step_id,
                attempt=attempt,
                module=module,
                target=target,
            )
            return ("execution", execution_id, metadata)
        case ["asset", external_id, name, total_count, total_size]:
            metadata = models.AssetMetadata(
                name=name,
                total_count=total_count,
                total_size=total_size,
            )
            return ("asset", external_id, metadata)
        case ["fragment", serialiser, blob_key, size, metadata]:
            return ("fragment", serialiser, blob_key, size, metadata)
        case other:
            raise ValueError(f"Unknown reference format: {other}")


def _parse_references(references: list[t.Any]) -> list[types.Reference]:
    return [_parse_reference(r) for r in references]


# TODO: remove duplication in execution.py
def _parse_value(value: list) -> types.Value:
    match value:
        case ["raw", content, references]:
            return ("raw", content, _parse_references(references))
        case ["blob", key, size, references]:
            return ("blob", key, size, _parse_references(references))
    raise Exception(f"unexpected value: {value}")


class SessionExpiredError(Exception):
    """Raised when the worker's session has expired."""


class Worker:
    def __init__(
        self,
        workspace_name: str,
        server_host: str,
        secure: bool,
        serialiser_configs: list[config.SerialiserConfig],
        blob_threshold: int,
        blob_store_configs: list[config.BlobStoreConfig],
        log_store_config: config.LogStoreConfig,
        session_id: str,
        targets: dict[str, dict[str, tuple[models.Target, t.Callable]]],
    ):
        self._workspace_name = workspace_name
        self._server_host = server_host
        self._secure = secure
        self._session_id = session_id
        self._targets = targets
        self._connection = server.Connection(
            {"execute": self._handle_execute, "abort": self._handle_abort}
        )
        self._execution_manager = execution.Manager(
            self._connection, serialiser_configs, blob_threshold, blob_store_configs, log_store_config
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._execution_manager.abort_all()

    async def _handle_execute(self, *args) -> None:
        (execution_id, module_name, target_name, arguments, run_id, workspace_id) = args
        print(f"Handling execute '{target_name}' ({execution_id})...")
        target = self._targets[module_name][target_name][1].__name__
        arguments = [_parse_value(a) for a in arguments]
        loop = asyncio.get_running_loop()
        self._execution_manager.execute(
            execution_id, module_name, target, arguments, self._server_host, self._secure, loop,
            run_id, workspace_id
        )

    async def _handle_abort(self, *args) -> None:
        (execution_id,) = args
        print(f"Aborting execution ({execution_id})...")
        if not self._execution_manager.abort(execution_id):
            print(f"Ignored abort for unrecognised execution ({execution_id}).")

    def _url(self, scheme: str, path: str, params: dict[str, str]) -> str:
        params_ = {k: v for k, v in params.items() if v is not None} if params else None
        query_string = f"?{urllib.parse.urlencode(params_)}" if params_ else ""
        return f"{scheme}://{self._server_host}/{path}{query_string}"

    def _params(self):
        params = {
            "workspace": self._workspace_name,
        }
        if API_VERSION:
            params["version"] = API_VERSION
        return params

    def _subprotocols(self) -> list[str]:
        """Build WebSocket subprotocols for authentication."""
        encoded_token = base64.urlsafe_b64encode(self._session_id.encode()).decode().rstrip("=")
        return [f"session.{encoded_token}", "v1"]

    async def run(self) -> None:
        """Run the worker. Raises SessionExpiredError if session expires."""
        check_server(self._server_host, self._secure)
        while True:
            print(f"Connecting ({self._server_host}, {self._workspace_name})...")
            scheme = "wss" if self._secure else "ws"
            url = self._url(scheme, "worker", self._params())
            try:
                async with websockets.connect(url, subprotocols=self._subprotocols()) as websocket:
                    print("Connected.")
                    targets: dict[str, dict[types.TargetType, list[str]]] = {}
                    for module, module_targets in self._targets.items():
                        for target_name, (target, _) in module_targets.items():
                            targets.setdefault(module, {}).setdefault(
                                target.type, []
                            ).append(target_name)
                    coros = [
                        asyncio.create_task(self._connection.run(websocket)),
                        asyncio.create_task(self._execution_manager.run(targets)),
                    ]
                    done, pending = await asyncio.wait(
                        coros, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in pending:
                        task.cancel()
                    for task in done:
                        task.result()
            except websockets.ConnectionClosedError as e:
                reason = e.rcvd.reason if e.rcvd else None
                if reason == "project_not_found":
                    raise Exception("Project not found")
                elif reason == "session_invalid":
                    raise SessionExpiredError("Session invalid")
                elif reason == "workspace_mismatch":
                    raise Exception(
                        f"Workspace mismatch: session does not belong to workspace '{self._workspace_name}'"
                    )
                else:
                    delay = 1 + 3 * random.random()  # TODO: exponential backoff
                    print(f"Disconnected (reconnecting in {delay:.1f} seconds).")
                    await asyncio.sleep(delay)
            except OSError:
                traceback.print_exc()
                delay = 1 + 3 * random.random()  # TODO: exponential backoff
                print(f"Can't connect (retrying in {delay:.1f} seconds).")
                await asyncio.sleep(delay)

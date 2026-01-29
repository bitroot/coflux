import asyncio
import functools
import os
import random
import subprocess
import sys
import time
import types
import typing as t
from importlib.metadata import PackageNotFoundError, version as pkg_version
from pathlib import Path

import click
import httpx
import tomlkit
import watchfiles

from . import Worker, config, decorators, loader, models, utils
from .worker import SessionExpiredError
from .blobs import Manager as BlobManager
from .version import API_VERSION, VersionMismatchError

T = t.TypeVar("T")


def _is_localhost(host: str) -> bool:
    """
    Check if a host is localhost-like.

    Handles localhost, *.localhost, 127.0.0.1, [::1], all with optional port.
    """
    # Handle IPv6 addresses in brackets (e.g., [::1]:7777)
    if host.startswith("["):
        bracket_end = host.find("]")
        if bracket_end != -1:
            return host[1:bracket_end] == "::1"
        return False

    # Remove port suffix for regular hosts
    hostname = host.rsplit(":", 1)[0] if ":" in host else host

    # Check for localhost, subdomains of localhost, or IPv4 loopback
    return hostname == "localhost" or hostname.endswith(".localhost") or hostname == "127.0.0.1"


def _should_use_secure(host: str, secure: bool | None) -> bool:
    """
    Determine whether to use secure connections (HTTPS/WSS).

    If secure is explicitly set, use that value.
    Otherwise, infer from hostname: insecure for localhost, secure otherwise.
    """
    if secure is not None:
        return secure
    return not _is_localhost(host)


def _callback(_changes: set[tuple[watchfiles.Change, str]]) -> None:
    print("Change detected. Reloading...")


def _get_default_image() -> str:
    try:
        v = pkg_version("coflux")
        return f"ghcr.io/bitroot/coflux:{v}"
    except PackageNotFoundError:
        return "ghcr.io/bitroot/coflux"


def _api_request(
    method: str, host: str, action: str, token: str | None, *, secure: bool, **kwargs
) -> t.Any:
    headers = kwargs.pop("headers", {})
    if API_VERSION:
        headers["X-API-Version"] = API_VERSION
    if token:
        headers["Authorization"] = f"Bearer {token}"
    scheme = "https" if secure else "http"
    with httpx.Client() as client:
        response = client.request(
            method, f"{scheme}://{host}/api/{action}", headers=headers, **kwargs
        )
        if response.status_code == 409:
            data = response.json()
            if data.get("error") == "version_mismatch":
                details = data["details"]
                raise VersionMismatchError(details["server"], details["expected"])
        # TODO: return errors
        response.raise_for_status()
        is_json = response.headers.get("Content-Type") == "application/json"
        return response.json() if is_json else None


def _create_session(
    host: str,
    project_id: str,
    workspace_name: str,
    provides: dict[str, list[str]] | None = None,
    concurrency: int | None = None,
    token: str | None = None,
    *,
    secure: bool,
) -> str:
    payload: dict[str, t.Any] = {"projectId": project_id, "workspaceName": workspace_name}
    if provides:
        payload["provides"] = provides
    if concurrency:
        payload["concurrency"] = concurrency
    result = _api_request("POST", host, "create_session", token, secure=secure, json=payload)
    return result["sessionId"]


def _encode_provides(
    provides: dict[str, list[str] | str | bool] | None,
) -> tuple[str, ...] | None:
    if not provides:
        return None
    return tuple(
        f"{k}:{','.join((str(v).lower() if isinstance(v, bool) else v) for v in (vs if isinstance(vs, list) else [vs]))}"
        for k, vs in provides.items()
    )


def _parse_provides(argument: tuple[str, ...] | None) -> dict[str, list[str]]:
    if not argument:
        return {}
    result: dict[str, list[str]] = {}
    for part in (p for a in argument for p in a.split(";") if p):
        key, values = part.split(":", 1) if ":" in part else (part, "true")
        for value in values.split(","):
            result.setdefault(key, []).append(value)
    return result


def _load_module(
    module: types.ModuleType,
) -> dict[str, tuple[models.Target, t.Callable]]:
    attrs = (getattr(module, k) for k in dir(module))
    return {
        a.name: (a.definition, a.fn)
        for a in attrs
        if isinstance(a, decorators.Target) and not a.definition.is_stub
    }


def _load_modules(
    modules: list[types.ModuleType | str],
) -> dict[str, dict[str, tuple[models.Target, t.Callable]]]:
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    targets = {}
    for module in list(modules):
        if isinstance(module, str):
            module = loader.load_module(module)
        targets[module.__name__] = _load_module(module)
    return targets


def _register_manifests(
    project_id: str,
    workspace_name: str,
    host: str,
    targets: dict[str, dict[str, tuple[models.Target, t.Callable]]],
    token: str | None = None,
    *,
    secure: bool,
) -> None:
    manifests = {
        module: {
            workflow_name: {
                "parameters": [
                    {
                        "name": p.name,
                        "annotation": p.annotation,
                        "default": p.default,
                    }
                    for p in definition.parameters
                ],
                "waitFor": list(definition.wait_for),
                "cache": (
                    definition.cache
                    and {
                        "params": definition.cache.params,
                        "maxAge": definition.cache.max_age,
                        "namespace": definition.cache.namespace,
                        "version": definition.cache.version,
                    }
                ),
                "defer": (
                    definition.defer
                    and {
                        "params": definition.defer.params,
                    }
                ),
                "delay": definition.delay,
                "retries": (
                    definition.retries
                    and {
                        "limit": definition.retries.limit,
                        "delayMin": definition.retries.delay_min,
                        "delayMax": definition.retries.delay_max,
                    }
                ),
                "recurrent": definition.recurrent,
                "requires": definition.requires,
                "instruction": definition.instruction,
            }
            for workflow_name, (definition, _) in target.items()
            if definition.type == "workflow"
        }
        for module, target in targets.items()
    }
    # TODO: handle response?
    _api_request(
        "POST",
        host,
        "register_manifests",
        token,
        secure=secure,
        json={
            "projectId": project_id,
            "workspaceName": workspace_name,
            "manifests": manifests,
        },
    )


def _get_pool(
    host: str, project_id: str, workspace_name: str, pool_name: str, token: str | None, *, secure: bool
) -> dict | None:
    try:
        return _api_request(
            "GET",
            host,
            "get_pool",
            token,
            secure=secure,
            params={
                "project": project_id,
                "workspace": workspace_name,
                "pool": pool_name,
            },
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        else:
            raise


def _truncate(text: str, max_width: int) -> str:
    if max_width is None or len(text) <= max_width:
        return text
    return text[: max_width - 1] + "…"


def _print_table(
    headers: tuple[str, ...],
    rows: list[tuple[str, ...]],
    max_width: int | None = 30,
) -> None:
    if max_width is not None:
        headers = tuple(_truncate(h, max_width) for h in headers)
        rows = [tuple(_truncate(cell, max_width) for cell in row) for row in rows]
    widths = [max(len(col_val) for col_val in col) for col in zip(headers, *rows)]
    click.echo(
        "  ".join(click.style(h.ljust(w), bold=True) for h, w in zip(headers, widths))
    )
    for row in rows:
        click.echo("  ".join(str(cell).ljust(w) for cell, w in zip(row, widths)))


def _init(
    *modules: types.ModuleType | str,
    project: str,
    workspace: str,
    host: str,
    token: str | None,
    secure: bool,
    provides: dict[str, list[str]],
    serialiser_configs: list[config.SerialiserConfig],
    blob_threshold: int,
    blob_store_configs: list[config.BlobStoreConfig],
    concurrency: int,
    session_id: str | None,
    register: bool,
) -> None:
    try:
        targets = _load_modules(list(modules))
        if register:
            _register_manifests(project, workspace, host, targets, token=token, secure=secure)

        # Track whether we created the session (vs it being provided externally)
        session_provided = session_id is not None
        session_backoff = 0

        while True:
            # Create a session if not provided
            if not session_id:
                if session_backoff > 0:
                    delay = min(session_backoff, 30) * (0.5 + random.random())
                    print(f"Waiting {delay:.1f} seconds before creating session...")
                    time.sleep(delay)
                print("Creating session...")
                session_id = _create_session(
                    host, project, workspace, provides, concurrency, token=token, secure=secure
                )
                print("Session created.")

            try:
                with Worker(
                    project,
                    workspace,
                    host,
                    secure,
                    serialiser_configs,
                    blob_threshold,
                    blob_store_configs,
                    session_id,
                    targets,
                ) as worker:
                    asyncio.run(worker.run())
                    session_backoff = 0  # Reset on clean exit
            except SessionExpiredError:
                if session_provided:
                    print("Session expired. Exiting...")
                    raise
                else:
                    print("Session expired. Recreating...")
                    session_backoff = max(1, session_backoff * 2)
                    session_id = None
                    continue
            break
    except KeyboardInterrupt:
        pass


@click.group()
def cli():
    pass


@cli.command("server")
@click.option(
    "-p",
    "--port",
    type=int,
    default=7777,
    help="Port to run server on",
)
@click.option(
    "-d",
    "--data-dir",
    type=click.Path(file_okay=False, path_type=Path, resolve_path=True),
    default="./data/",
    help="The directory to store data",
)
@click.option(
    "--image",
    default=_get_default_image(),
    help="The Docker image to run",
)
def server(port: int, data_dir: Path, image: str):
    """
    Start a local server.

    This is just a wrapper around Docker (which must be installed and running), useful for running the server in a development environment.
    """
    command = [
        "docker",
        "run",
        "--pull",
        ("missing" if image.startswith("sha256:") else "always"),
        "--publish",
        f"{port}:7777",
        "--volume",
        f"{data_dir}:/data",
        image,
    ]
    process = subprocess.run(command)
    sys.exit(process.returncode)


def _config_path():
    return Path.cwd().joinpath("coflux.toml")


def _read_config(path: Path) -> tomlkit.TOMLDocument:
    if path.exists():
        with path.open("r") as f:
            return tomlkit.load(f)
    else:
        # TODO: add instructions?
        return tomlkit.document()


def _write_config(path: Path, data: tomlkit.TOMLDocument):
    with path.open("w") as f:
        tomlkit.dump(data, f)


@functools.cache
def _load_config() -> config.Config:
    path = _config_path()
    return config.Config.model_validate(_read_config(path).unwrap())


def _parse_bool_env(name: str) -> bool | None:
    """
    Parse a boolean environment variable.

    Returns True for '1', 'true', 'yes', 'on' (case-insensitive).
    Returns False for '0', 'false', 'no', 'off' (case-insensitive).
    Returns None if not set or empty.
    Raises click.BadParameter for invalid values.
    """
    value = os.environ.get(name, "").strip().lower()
    if not value:
        return None
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise click.BadParameter(
        f"Invalid value for {name}: {value!r}. Use: 1/0, true/false, yes/no, on/off"
    )


def _get_default_secure() -> bool | None:
    """Get the default secure value from env or config."""
    env_value = _parse_bool_env("COFLUX_SECURE")
    if env_value is not None:
        return env_value
    return _load_config().server.secure


def _server_options(token: bool = True):
    """Add server options: host, secure, and optionally token."""
    def decorator(f):
        decorators = [
            click.option(
                "-h",
                "--host",
                help="Host to connect to",
                envvar="COFLUX_HOST",
                default=_load_config().server.host,
                show_default=True,
                required=True,
            ),
            click.option(
                "--secure/--no-secure",
                "secure",
                default=_get_default_secure(),
                help="Use secure connections (HTTPS/WSS). Inferred from host if not specified.",
            ),
        ]
        if token:
            decorators.append(
                click.option(
                    "--token",
                    help="Authentication token",
                    envvar="COFLUX_TOKEN",
                    default=_load_config().server.token,
                )
            )
        for d in reversed(decorators):
            f = d(f)
        return f
    return decorator


def _project_options(workspace: bool = False):
    """Add project option, and optionally workspace."""
    def decorator(f):
        decorators = [
            click.option(
                "-p",
                "--project",
                help="Project ID",
                envvar="COFLUX_PROJECT",
                default=_load_config().project,
                show_default=True,
                required=True,
            ),
        ]
        if workspace:
            decorators.append(
                click.option(
                    "-w",
                    "--workspace",
                    help="Workspace name",
                    envvar="COFLUX_WORKSPACE",
                    default=_load_config().workspace,
                    show_default=True,
                    required=True,
                )
            )
        for d in reversed(decorators):
            f = d(f)
        return f
    return decorator


@cli.command("configure")
@click.option(
    "-p",
    "--project",
    help="Project ID",
    default=_load_config().project,
    show_default=True,
    prompt=True,
)
@click.option(
    "workspace",
    "-w",
    "--workspace",
    help="Workspace name",
    default=_load_config().workspace,
    show_default=True,
    prompt=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    default=_load_config().server.host,
    show_default=True,
    prompt=True,
)
def configure(
    host: str | None,
    project: str | None,
    workspace: str | None,
):
    """
    Populate/update the configuration file.
    """
    # TODO: connect to server to check details?
    click.secho("Writing configuration...", fg="black")

    path = _config_path()
    data = _read_config(path)
    data["project"] = project
    data["workspace"] = workspace
    data.setdefault("server", {})["host"] = host
    _write_config(path, data)

    click.secho(
        f"Configuration written to '{path.relative_to(Path.cwd())}'.", fg="green"
    )


@cli.group()
def workspaces():
    """
    Manage workspaces.
    """
    pass


@workspaces.command("list")
@_project_options()
@_server_options()
def workspaces_list(
    project: str,
    host: str,
    token: str | None,
    secure: bool | None,
):
    """
    Lists workspaces.
    """
    use_secure = _should_use_secure(host, secure)
    workspaces = _api_request("GET", host, "get_workspaces", token, secure=use_secure, params={"project": project})
    if workspaces:
        # TODO: draw as tree
        _print_table(
            ("Name", "Base"),
            [
                (
                    workspace["name"],
                    workspaces[workspace["baseId"]]["name"] if workspace["baseId"] else "(None)",
                )
                for workspace in workspaces.values()
            ],
        )


@workspaces.command("create")
@_project_options()
@_server_options()
@click.option(
    "--base",
    help="The base workspace to inherit from",
)
@click.argument("name")
def workspaces_create(
    project: str,
    host: str,
    token: str | None,
    secure: bool | None,
    base: str | None,
    name: str,
):
    """
    Creates a workspace within the project.
    """
    use_secure = _should_use_secure(host, secure)
    base_id = None
    if base:
        workspaces = _api_request("GET", host, "get_workspaces", token, secure=use_secure, params={"project": project})
        workspace_ids_by_name = {w["name"]: id for id, w in workspaces.items()}
        base_id = workspace_ids_by_name.get(base)
        if not base_id:
            click.BadOptionUsage("base", "Not recognised")

    # TODO: handle response
    _api_request(
        "POST",
        host,
        "create_workspace",
        token,
        secure=use_secure,
        json={
            "projectId": project,
            "name": name,
            "baseId": base_id,
        },
    )
    click.secho(f"Created workspace '{name}'.", fg="green")


@workspaces.command("update")
@_project_options(workspace=True)
@_server_options()
@click.option(
    "--name",
    help="The new name of the workspace",
)
@click.option(
    "--base",
    help="The new base workspace to inherit from",
)
@click.option(
    "--no-base",
    is_flag=True,
    help="Unset the base workspace",
)
def workspaces_update(
    project: str,
    workspace: str,
    host: str,
    token: str | None,
    secure: bool | None,
    name: str | None,
    base: str | None,
    no_base: bool,
):
    """
    Updates a workspace within the project.
    """
    use_secure = _should_use_secure(host, secure)
    workspaces = _api_request("GET", host, "get_workspaces", token, secure=use_secure, params={"project": project})
    workspace_ids_by_name = {w["name"]: id for id, w in workspaces.items()}
    workspace_id = workspace_ids_by_name.get(workspace)
    if not workspace_id:
        raise click.BadOptionUsage("workspace", "Not recognised")

    base_id = None
    if base:
        base_id = workspace_ids_by_name.get(base)
        if not base_id:
            raise click.BadOptionUsage("base", "Not recognised")

    payload = {
        "projectId": project,
        "workspaceId": workspace_id,
    }
    if name is not None:
        payload["name"] = name

    if base is not None:
        payload["baseId"] = base_id
    elif no_base is True:
        payload["baseId"] = None

    # TODO: handle response
    _api_request("POST", host, "update_workspace", token, secure=use_secure, json=payload)

    click.secho(f"Updated workspace '{name or workspace}'.", fg="green")


@workspaces.command("archive")
@_project_options(workspace=True)
@_server_options()
def workspaces_archive(
    project: str,
    workspace: str,
    host: str,
    token: str | None,
    secure: bool | None,
):
    """
    Archives a workspace.
    """
    use_secure = _should_use_secure(host, secure)
    workspaces = _api_request("GET", host, "get_workspaces", token, secure=use_secure, params={"project": project})
    workspace_ids_by_name = {w["name"]: id for id, w in workspaces.items()}
    workspace_id = workspace_ids_by_name.get(workspace)
    if not workspace_id:
        raise click.BadOptionUsage("workspace", "Not recognised")

    _api_request(
        "POST",
        host,
        "archive_workspace",
        token,
        secure=use_secure,
        json={
            "projectId": project,
            "workspaceId": workspace_id,
        },
    )
    click.secho(f"Archived workspace '{workspace}'.", fg="green")


@cli.group()
def pools():
    """
    Manage pools.
    """
    pass


@pools.command("list")
@_project_options(workspace=True)
@_server_options()
def pools_list(project: str, workspace: str, host: str, token: str | None, secure: bool | None):
    """
    Lists pools.
    """
    use_secure = _should_use_secure(host, secure)
    pools = _api_request(
        "GET",
        host,
        "get_pools",
        token,
        secure=use_secure,
        json={"projectId": project, "workspaceName": workspace},
    )
    if pools:
        _print_table(
            ("Name", "Launcher", "Modules", "Provides"),
            [
                (
                    pool_name,
                    pool["launcherType"] or "",
                    ",".join(pool["modules"]),
                    " ".join(_encode_provides(pool["provides"]) or []),
                )
                for pool_name, pool in pools.items()
            ],
        )


@pools.command("update")
@_project_options(workspace=True)
@_server_options()
@click.option(
    "modules",
    "-m",
    "--module",
    help="Modules to be hosted by workers in the pool",
    multiple=True,
)
@click.option(
    "--provides",
    help="Features that workers in the pool provide (to be matched with features that tasks require)",
    multiple=True,
)
@click.option(
    "--docker-image",
    help="The Docker image.",
)
@click.option(
    "--docker-host",
    help="Docker daemon connection (e.g., 'unix:///var/run/docker.sock' or 'tcp://host:2375').",
)
@click.argument("name")
def pools_update(
    project: str,
    workspace: str,
    host: str,
    token: str | None,
    secure: bool | None,
    modules: tuple[str, ...] | None,
    provides: tuple[str, ...] | None,
    docker_image: str | None,
    docker_host: str | None,
    name: str,
):
    """
    Updates a pool.
    """
    use_secure = _should_use_secure(host, secure)
    pool = _get_pool(host, project, workspace, name, token, secure=use_secure) or {}

    # TODO: support explicitly unsetting 'provides' (and modules, etc?)

    if modules is not None:
        pool["modules"] = list(modules)
    if provides is not None:
        pool["provides"] = _parse_provides(provides)
    if docker_image or docker_host:
        if "launcher" not in pool or pool.get("launcher", {}).get("type") != "docker":
            pool["launcher"] = {"type": "docker"}
        if docker_image:
            pool["launcher"]["image"] = docker_image
        if docker_host:
            pool["launcher"]["dockerHost"] = docker_host

    _api_request(
        "POST",
        host,
        "update_pool",
        token,
        secure=use_secure,
        json={
            "projectId": project,
            "workspaceName": workspace,
            "poolName": name,
            "pool": pool,
        },
    )


@pools.command("delete")
@_project_options(workspace=True)
@_server_options()
@click.argument("name")
def pools_delete(project: str, workspace: str, host: str, token: str | None, secure: bool | None, name: str):
    """
    Deletes a pool.
    """
    use_secure = _should_use_secure(host, secure)
    _api_request(
        "POST",
        host,
        "update_pool",
        token,
        secure=use_secure,
        json={"projectId": project, "workspaceName": workspace, "poolName": name, "pool": None},
    )


@cli.group()
def blobs():
    """
    Manage blobs.
    """
    pass


@blobs.command("get")
@_server_options(token=False)
@click.argument("key")
def blobs_get(host: str, secure: bool | None, key: str):
    """
    Gets a blob by key and writes the content to stdout.
    """
    use_secure = _should_use_secure(host, secure)
    config = _load_config()
    if not config.blobs.stores:
        raise click.ClickException("Blob store not configured")

    out = click.get_binary_stream("stdout")
    with BlobManager(config.blobs.stores, host, secure=use_secure) as blob_manager:
        blob = blob_manager.get(key)
        for chunk in iter(lambda: blob.read(64 * 1024), b""):
            out.write(chunk)
        out.flush()


@cli.group()
def assets():
    """
    Manage assets.
    """
    pass


def _get_asset(host: str, project_id: str, asset_id: str, token: str | None, *, secure: bool) -> dict | None:
    try:
        return _api_request(
            "GET",
            host,
            "get_asset",
            token,
            secure=secure,
            params={
                "project": project_id,
                "asset": asset_id,
            },
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        else:
            raise


def _human_size(bytes: int) -> str:
    if bytes < 1024:
        return f"{bytes} bytes"
    value = bytes / 1024
    for unit in ("KiB", "MiB", "GiB"):
        if value < 1024:
            return f"{value:3.1f}{unit}"
        value /= 1024
    return f"{bytes:.1f}TiB"


@assets.command("inspect")
@_project_options()
@_server_options()
@click.option(
    "--match",
    help="Glob-style matcher to filter files",
)
@click.argument("id")
def assets_inspect(project: str, host: str, token: str | None, secure: bool | None, match: str | None, id: str):
    """
    Inspect an asset.
    """
    use_secure = _should_use_secure(host, secure)
    asset = _get_asset(host, project, id, token, secure=use_secure)
    if not asset:
        raise click.ClickException(f"Asset '{id}' not found in project")

    click.echo(f"Name: {asset['name'] or '(untitled)'}")

    entries = asset["entries"]
    if match:
        matcher = utils.GlobMatcher(match)
        entries = {k: v for k, v in entries.items() if matcher.match(k)}
        click.echo(f"Matched {len(entries)} of {len(asset['entries'])} entries.")

    _print_table(
        ("Path", "Size", "Type", "Blob key"),
        [
            (
                key,
                _human_size(value["size"]),
                value["metadata"].get("type") or "(unknown)",
                value["blobKey"],
            )
            for key, value in entries.items()
        ],
        max_width=None,
    )


@assets.command("download")
@_project_options()
@_server_options()
@click.option(
    "--to",
    type=click.Path(file_okay=False, path_type=Path, resolve_path=True),
    default=".",
    help="The local path to download the contents to",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrites any existing files if present",
)
@click.option(
    "--match",
    help="Glob-style matcher to filter files",
)
@click.argument("id")
def assets_download(
    project: str,
    host: str,
    token: str | None,
    secure: bool | None,
    to: Path,
    force: bool,
    match: str | None,
    id: str,
):
    """
    Downloads the contents of an asset.
    """
    use_secure = _should_use_secure(host, secure)
    asset = _get_asset(host, project, id, token, secure=use_secure)
    if not asset:
        raise click.ClickException(f"Asset '{id}' not found in project")

    entries = asset["entries"]
    if match:
        matcher = utils.GlobMatcher(match)
        entries = {k: v for k, v in entries.items() if matcher.match(k)}
        click.echo(f"Matched {len(entries)} of {len(asset['entries'])} entries.")

    if not entries:
        click.echo("Nothing to download")
        return

    for key in entries.keys():
        path = to.joinpath(key)
        if path.exists():
            if not force:
                raise click.ClickException(f"File already exists at path: {path}")
            elif not path.is_file():
                raise click.ClickException(f"Cannot overwrite non-file: {path}")

    config = _load_config()
    if not config.blobs.stores:
        raise click.ClickException("Blob store not configured")

    total_size = sum(v["size"] for v in entries.values())

    with BlobManager(config.blobs.stores, host, secure=use_secure) as blob_manager:
        click.echo(f"Downloading {len(entries)} files ({_human_size(total_size)})...")
        # TODO: parallelise downloads
        with click.progressbar(entries.items(), label="") as bar:
            for key, entry in bar:
                path = to.joinpath(key)
                path.parent.mkdir(exist_ok=True, parents=True)
                blob_manager.download(entry["blobKey"], path)


@cli.command("register")
@_project_options(workspace=True)
@_server_options()
@click.argument("module_name", nargs=-1)
def register(
    project: str,
    workspace: str,
    host: str,
    token: str | None,
    secure: bool | None,
    module_name: tuple[str, ...],
) -> None:
    """
    Register modules with the server.

    Paths to scripts can be passed instead of module names.

    Options will be loaded from the configuration file, unless overridden as arguments (or environment variables).
    """
    if not module_name:
        raise click.ClickException("No module(s) specified.")
    use_secure = _should_use_secure(host, secure)
    targets = _load_modules(list(module_name))
    _register_manifests(project, workspace, host, targets, token=token, secure=use_secure)
    click.secho("Manifest(s) registered.", fg="green")


@cli.command("worker")
@_project_options(workspace=True)
@_server_options()
@click.option(
    "--provides",
    help="Features that this worker provides (to be matched with features that tasks require)",
    multiple=True,
    envvar="COFLUX_PROVIDES",
    default=_encode_provides(_load_config().provides),
    show_default=True,
)
@click.option(
    "--session",
    help="Session ID (for pool-launched workers)",
    envvar="COFLUX_SESSION",
)
@click.option(
    "--concurrency",
    type=int,
    help="Limit on number of executions to process at once",
    default=_load_config().concurrency,
    show_default=True,
)
@click.option(
    "--watch",
    is_flag=True,
    default=False,
    help="Enable auto-reload when code changes",
)
@click.option(
    "--register",
    is_flag=True,
    default=False,
    help="Automatically register modules",
)
@click.option(
    "--dev",
    is_flag=True,
    default=False,
    help="Enable development mode (implies `--watch` and `--register`)",
)
@click.argument("module_name", nargs=-1)
def worker(
    project: str,
    workspace: str,
    host: str,
    token: str | None,
    secure: bool | None,
    provides: tuple[str, ...] | None,
    session: str | None,
    concurrency: int,
    watch: bool,
    register: bool,
    dev: bool,
    module_name: tuple[str, ...],
) -> None:
    """
    Starts a worker.

    Hosts the specified modules. Paths to scripts can be passed instead of module names.

    Options will be loaded from the configuration file, unless overridden as arguments (or environment variables).
    """
    if not module_name:
        raise click.ClickException("No module(s) specified.")
    provides_ = _parse_provides(provides)
    config = _load_config()
    use_secure = _should_use_secure(host, secure)
    args = (*module_name,)
    kwargs = {
        "project": project,
        "workspace": workspace,
        "host": host,
        "token": token,
        "secure": use_secure,
        "provides": provides_,
        "serialiser_configs": config and config.serialisers,
        "blob_threshold": config and config.blobs and config.blobs.threshold,
        "blob_store_configs": config and config.blobs and config.blobs.stores,
        "concurrency": concurrency,
        "session_id": session,
        "register": register or dev,
    }
    if watch or dev:
        filter = watchfiles.PythonFilter()
        watchfiles.run_process(
            ".",
            target=_init,
            args=args,
            kwargs=kwargs,
            callback=_callback,
            watch_filter=filter,
        )
    else:
        _init(*args, **kwargs)


@cli.command("submit")
@_project_options(workspace=True)
@_server_options()
@click.argument("module")
@click.argument("target")
@click.argument("argument", nargs=-1)
def submit(
    project: str,
    workspace: str,
    host: str,
    token: str | None,
    secure: bool | None,
    module: str,
    target: str,
    argument: tuple[str, ...],
) -> None:
    """
    Submit a workflow to be run.
    """
    use_secure = _should_use_secure(host, secure)
    # TODO: support overriding options?
    workflow = _api_request(
        "GET",
        host,
        "get_workflow",
        token,
        secure=use_secure,
        params={
            "project": project,
            "workspace": workspace,
            "module": module,
            "target": target,
        },
    )
    execute_after = (
        int((time.time() + workflow["delay"]) * 1000) if workflow["delay"] else None
    )
    # TODO: handle response
    _api_request(
        "POST",
        host,
        "submit_workflow",
        token,
        secure=use_secure,
        json={
            "projectId": project,
            "workspaceName": workspace,
            "module": module,
            "target": target,
            "arguments": [["json", a] for a in argument],
            "waitFor": workflow["waitFor"],
            "cache": workflow["cache"],
            "defer": workflow["defer"],
            "executeAfter": execute_after,
            "retries": workflow["retries"],
            "requires": workflow["requires"],
        },
    )
    click.secho("Workflow submitted.", fg="green")
    # TODO: follow logs?
    # TODO: wait for result?


if __name__ == "__main__":
    cli()

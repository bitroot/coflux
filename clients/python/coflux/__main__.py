import asyncio
import functools
import os
import random
import subprocess
import sys
import time
import types
import typing as t
import datetime as dt
from importlib.metadata import PackageNotFoundError, version as pkg_version
from pathlib import Path

import click
import httpx
import tomlkit
import watchfiles

from . import Worker, auth, config, decorators, loader, models, utils
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
        if not response.is_success:
            # Try to extract error details from response body
            try:
                data = response.json()
                error_msg = data.get("error", response.reason_phrase)
                details = data.get("details")
                if details:
                    click.secho(f"Error: {error_msg}", fg="red", err=True)
                    click.secho(f"Details: {details}", fg="red", err=True)
                else:
                    click.secho(f"Error: {error_msg}", fg="red", err=True)
            except Exception:
                click.secho(f"Error: {response.text}", fg="red", err=True)
        response.raise_for_status()
        is_json = response.headers.get("Content-Type") == "application/json"
        return response.json() if is_json else None


def _create_session(
    host: str,
    workspace_name: str,
    provides: dict[str, list[str]] | None = None,
    concurrency: int | None = None,
    token: str | None = None,
    *,
    secure: bool,
) -> str:
    payload: dict[str, t.Any] = {"workspaceName": workspace_name}
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
            "workspaceName": workspace_name,
            "manifests": manifests,
        },
    )


def _get_pool(
    host: str, workspace_name: str, pool_name: str, token: str | None, *, secure: bool
) -> dict | None:
    try:
        return _api_request(
            "GET",
            host,
            "get_pool",
            token,
            secure=secure,
            params={
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
    workspace: str,
    host: str,
    token: str | None,
    secure: bool,
    provides: dict[str, list[str]],
    serialiser_configs: list[config.SerialiserConfig],
    blob_threshold: int,
    blob_store_configs: list[config.BlobStoreConfig],
    log_store_config: config.LogStoreConfig,
    concurrency: int,
    session_id: str | None,
    register: bool,
) -> None:
    try:
        targets = _load_modules(list(modules))
        if register:
            _register_manifests(workspace, host, targets, token=token, secure=secure)

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
                    host, workspace, provides, concurrency, token=token, secure=secure
                )
                print("Session created.")

            try:
                with Worker(
                    workspace,
                    host,
                    secure,
                    serialiser_configs,
                    blob_threshold,
                    blob_store_configs,
                    log_store_config,
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


@cli.command("login")
def login():
    """
    Log in to Coflux Studio.

    Opens a browser-based authentication flow. After logging in, your session
    will be saved locally for use with team-based authentication.

    Set the COFLUX_STUDIO_URL environment variable to use a custom Studio URL.
    """
    studio_url = _get_studio_url()
    try:
        # Start device flow
        flow = auth.start_device_flow(studio_url)

        # Show user instructions
        click.echo()
        click.echo("To authenticate, open this URL in your browser:")
        click.echo()
        click.secho(f"  {flow['verification_uri']}", fg="cyan")
        click.echo()
        click.echo("Waiting for authorization...")

        # Poll for token
        result = auth.poll_for_token(
            flow["device_code"],
            studio_url,
            interval=flow.get("interval", 5),
            timeout=flow.get("expires_in", 900),
        )

        # Save credentials
        auth.save_credentials(
            {
                "studio_token": result.access_token,
                "user_email": result.user_email,
                "user_id": result.user_id,
            }
        )

        click.echo()
        if result.user_email:
            click.secho(f"Logged in as {result.user_email}", fg="green")
        else:
            click.secho("Logged in successfully", fg="green")
    except auth.DeviceFlowExpired:
        click.secho("Login expired. Please try again.", fg="red")
        sys.exit(1)
    except auth.DeviceFlowError as e:
        click.secho(f"Login failed: {e}", fg="red")
        sys.exit(1)
    except httpx.HTTPStatusError as e:
        click.secho(f"Login failed: {e}", fg="red")
        sys.exit(1)


@cli.command("logout")
def logout():
    """
    Log out from Coflux Studio.

    Removes locally stored credentials.
    """
    auth.clear_credentials()
    click.secho("Logged out successfully", fg="green")


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


def _project_options(token: bool = True, workspace: bool = False, team: bool = False):
    """Add project options: host, secure, and optionally token, workspace, and team."""
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
                    help="Authentication token (for token auth mode)",
                    envvar="COFLUX_TOKEN",
                    default=_load_config().server.token,
                )
            )
        if team:
            decorators.append(
                click.option(
                    "-t",
                    "--team",
                    help="Team ID for Studio authentication",
                    envvar="COFLUX_TEAM",
                    default=_load_config().team,
                )
            )
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


def _get_studio_url() -> str:
    """Get Studio URL from environment variable or default."""
    return os.environ.get("COFLUX_STUDIO_URL") or "https://studio.coflux.com"


def _resolve_token(
    token: str | None,
    team: str | None,
    host: str,
) -> str | None:
    """
    Resolve the authentication token to use.

    If a token is provided directly, use it.
    If a team is specified, get a project token (cached or exchanged).
    Otherwise, return None.
    """
    if token:
        return token

    if team:
        # Use Studio authentication (with caching)
        studio_url = _get_studio_url()
        try:
            return auth.get_project_token(team, host, studio_url)
        except ValueError as e:
            raise click.ClickException(str(e))
        except httpx.HTTPStatusError as e:
            # Try to extract error message from JSON response
            error_desc = None
            try:
                data = e.response.json()
                error_desc = data.get("error_description")
            except Exception:
                pass

            if error_desc:
                raise click.ClickException(f"Authentication failed: {error_desc}")
            raise click.ClickException(f"Authentication failed: HTTP {e.response.status_code}")

    return None


@cli.command("configure")
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
    workspace: str | None,
):
    """
    Populate/update the configuration file.
    """
    # TODO: connect to server to check details?
    click.secho("Writing configuration...", fg="black")

    path = _config_path()
    data = _read_config(path)
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
@_project_options(team=True)
def workspaces_list(
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
):
    """
    Lists workspaces.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    workspaces = _api_request("GET", host, "get_workspaces", resolved_token, secure=use_secure)
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
@_project_options(team=True)
@click.option(
    "--base",
    help="The base workspace to inherit from",
)
@click.argument("name")
def workspaces_create(
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
    base: str | None,
    name: str,
):
    """
    Creates a workspace within the project.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    base_id = None
    if base:
        workspaces = _api_request("GET", host, "get_workspaces", resolved_token, secure=use_secure)
        workspace_ids_by_name = {w["name"]: id for id, w in workspaces.items()}
        base_id = workspace_ids_by_name.get(base)
        if not base_id:
            click.BadOptionUsage("base", "Not recognised")

    # TODO: handle response
    _api_request(
        "POST",
        host,
        "create_workspace",
        resolved_token,
        secure=use_secure,
        json={
            "name": name,
            "baseId": base_id,
        },
    )
    click.secho(f"Created workspace '{name}'.", fg="green")


@workspaces.command("update")
@_project_options(workspace=True, team=True)
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
    workspace: str,
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
    name: str | None,
    base: str | None,
    no_base: bool,
):
    """
    Updates a workspace within the project.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    workspaces = _api_request("GET", host, "get_workspaces", resolved_token, secure=use_secure)
    workspace_ids_by_name = {w["name"]: id for id, w in workspaces.items()}
    workspace_id = workspace_ids_by_name.get(workspace)
    if not workspace_id:
        raise click.BadOptionUsage("workspace", "Not recognised")

    base_id = None
    if base:
        base_id = workspace_ids_by_name.get(base)
        if not base_id:
            raise click.BadOptionUsage("base", "Not recognised")

    payload: dict[str, t.Any] = {
        "workspaceId": workspace_id,
    }
    if name is not None:
        payload["name"] = name

    if base is not None:
        payload["baseId"] = base_id
    elif no_base is True:
        payload["baseId"] = None

    # TODO: handle response
    _api_request("POST", host, "update_workspace", resolved_token, secure=use_secure, json=payload)

    click.secho(f"Updated workspace '{name or workspace}'.", fg="green")


@workspaces.command("archive")
@_project_options(workspace=True, team=True)
def workspaces_archive(
    workspace: str,
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
):
    """
    Archives a workspace.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    workspaces = _api_request("GET", host, "get_workspaces", resolved_token, secure=use_secure)
    workspace_ids_by_name = {w["name"]: id for id, w in workspaces.items()}
    workspace_id = workspace_ids_by_name.get(workspace)
    if not workspace_id:
        raise click.BadOptionUsage("workspace", "Not recognised")

    _api_request(
        "POST",
        host,
        "archive_workspace",
        resolved_token,
        secure=use_secure,
        json={
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
@_project_options(workspace=True, team=True)
def pools_list(
    workspace: str,
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
):
    """
    Lists pools.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    pools = _api_request(
        "GET",
        host,
        "get_pools",
        resolved_token,
        secure=use_secure,
        params={"workspace": workspace},
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
@_project_options(workspace=True, team=True)
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
    workspace: str,
    host: str,
    token: str | None,
    team: str | None,
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
    resolved_token = _resolve_token(token, team, host)
    pool = _get_pool(host, workspace, name, resolved_token, secure=use_secure) or {}

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
        resolved_token,
        secure=use_secure,
        json={
            "workspaceName": workspace,
            "poolName": name,
            "pool": pool,
        },
    )


@pools.command("delete")
@_project_options(workspace=True, team=True)
@click.argument("name")
def pools_delete(
    workspace: str,
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
    name: str,
):
    """
    Deletes a pool.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    _api_request(
        "POST",
        host,
        "update_pool",
        resolved_token,
        secure=use_secure,
        json={"workspaceName": workspace, "poolName": name, "pool": None},
    )


@cli.group()
def blobs():
    """
    Manage blobs.
    """
    pass


@blobs.command("get")
@_project_options(token=False)
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
    with BlobManager(config.blobs.stores, server_host=host, server_secure=use_secure) as blob_manager:
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


def _get_asset(host: str, asset_id: str, token: str | None, *, secure: bool) -> dict | None:
    try:
        return _api_request(
            "GET",
            host,
            "get_asset",
            token,
            secure=secure,
            params={
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
@_project_options(team=True)
@click.option(
    "--match",
    help="Glob-style matcher to filter files",
)
@click.argument("id")
def assets_inspect(
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
    match: str | None,
    id: str,
):
    """
    Inspect an asset.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    asset = _get_asset(host, id, resolved_token, secure=use_secure)
    if not asset:
        raise click.ClickException(f"Asset '{id}' not found")

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
@_project_options(team=True)
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
    host: str,
    token: str | None,
    team: str | None,
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
    resolved_token = _resolve_token(token, team, host)
    asset = _get_asset(host, id, resolved_token, secure=use_secure)
    if not asset:
        raise click.ClickException(f"Asset '{id}' not found")

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

    with BlobManager(config.blobs.stores, server_host=host, server_secure=use_secure) as blob_manager:
        click.echo(f"Downloading {len(entries)} files ({_human_size(total_size)})...")
        # TODO: parallelise downloads
        with click.progressbar(entries.items(), label="") as bar:
            for key, entry in bar:
                path = to.joinpath(key)
                path.parent.mkdir(exist_ok=True, parents=True)
                blob_manager.download(entry["blobKey"], path)


@cli.command("register")
@_project_options(workspace=True, team=True)
@click.argument("module_name", nargs=-1)
def register(
    workspace: str,
    host: str,
    token: str | None,
    team: str | None,
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
    resolved_token = _resolve_token(token, team, host)
    targets = _load_modules(list(module_name))
    _register_manifests(workspace, host, targets, token=resolved_token, secure=use_secure)
    click.secho("Manifest(s) registered.", fg="green")


@cli.command("worker")
@_project_options(workspace=True, team=True)
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
    workspace: str,
    host: str,
    token: str | None,
    team: str | None,
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

    For Studio authentication, use --team to specify the team ID. This requires
    running 'coflux login' first.
    """
    if not module_name:
        raise click.ClickException("No module(s) specified.")
    provides_ = _parse_provides(provides)
    config = _load_config()
    use_secure = _should_use_secure(host, secure)

    # Resolve token (either direct token or via Studio team auth)
    resolved_token = _resolve_token(token, team, host)

    args = (*module_name,)
    kwargs = {
        "workspace": workspace,
        "host": host,
        "token": resolved_token,
        "secure": use_secure,
        "provides": provides_,
        "serialiser_configs": config and config.serialisers,
        "blob_threshold": config and config.blobs and config.blobs.threshold,
        "blob_store_configs": config and config.blobs and config.blobs.stores,
        "log_store_config": config and config.logs and config.logs.store,
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


@cli.group()
def tokens():
    """
    Manage API tokens.
    """
    pass


def _format_timestamp(ts: int | None) -> str:
    """Format a Unix timestamp as a human-readable string."""
    if ts is None:
        return ""
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_workspaces(workspaces: list[str] | None) -> str:
    """Format workspaces for display."""
    if workspaces is None:
        return "(all)"
    return ", ".join(workspaces)


@tokens.command("list")
@_project_options(team=True)
def tokens_list(
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
):
    """
    Lists API tokens.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    try:
        result = _api_request("GET", host, "list_tokens", resolved_token, secure=use_secure)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            raise click.ClickException("Access denied.")
        raise

    tokens_list = result.get("tokens", [])
    if tokens_list:
        _print_table(
            ("ID", "Name", "Created", "Status", "Workspaces"),
            [
                (
                    t["externalId"],
                    t["name"] or "(unnamed)",
                    _format_timestamp(t["createdAt"]),
                    "revoked" if t["revokedAt"] else ("expired" if t.get("expiresAt") and t["expiresAt"] < int(time.time()) else "active"),
                    _format_workspaces(t.get("workspaces")),
                )
                for t in tokens_list
            ],
        )
    else:
        click.echo("No tokens found.")


@tokens.command("create")
@_project_options(team=True)
@click.option(
    "--name",
    help="A name for the token (for identification)",
)
@click.option(
    "--workspaces",
    help="Comma-separated list of workspace patterns (e.g., 'production,staging/*'). Omit for access to all workspaces.",
)
def tokens_create(
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
    name: str | None,
    workspaces: str | None,
):
    """
    Creates a new API token.

    The token value is displayed only once. Make sure to copy it.

    Use --workspaces to restrict the token to specific workspaces. Patterns can
    include wildcards (e.g., 'development/*' matches all workspaces starting with
    'development/'). If omitted, the token has access to all workspaces.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    payload: dict[str, t.Any] = {}
    if name is not None:
        payload["name"] = name
    if workspaces is not None:
        # Parse comma-separated workspace patterns
        workspace_list = [w.strip() for w in workspaces.split(",") if w.strip()]
        if workspace_list:
            payload["workspaces"] = workspace_list
    try:
        result = _api_request(
            "POST",
            host,
            "create_token",
            resolved_token,
            secure=use_secure,
            json=payload,
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            raise click.ClickException(
                "Access denied. Cannot create a token with broader workspace access than your own."
            )
        raise

    click.secho("Token created successfully.", fg="green")
    click.echo()
    click.echo("Token: " + click.style(result['token'], bold=True))
    if workspaces:
        click.echo("Workspaces: " + workspaces)
    else:
        click.echo("Workspaces: " + click.style("(all)", dim=True))
    click.echo()
    click.secho("Copy this now, it won't be shown again.", fg="yellow")


@tokens.command("revoke")
@_project_options(team=True)
@click.argument("token_id")
def tokens_revoke(
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
    token_id: str,
):
    """
    Revokes an API token.

    The TOKEN_ID is the external ID of the token.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)

    # First, get the token to find its internal ID
    try:
        result = _api_request("GET", host, "list_tokens", resolved_token, secure=use_secure)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            raise click.ClickException("Access denied.")
        raise

    tokens_list = result.get("tokens", [])
    matching = [t for t in tokens_list if t["externalId"] == token_id]

    if not matching:
        raise click.ClickException(f"Token '{token_id}' not found.")

    token_info = matching[0]
    if token_info["revokedAt"]:
        raise click.ClickException(f"Token '{token_id}' is already revoked.")

    # Revoke the token
    try:
        _api_request(
            "POST",
            host,
            "revoke_token",
            resolved_token,
            secure=use_secure,
            json={"externalId": token_id},
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise click.ClickException(f"Token '{token_id}' not found.")
        if e.response.status_code == 403:
            raise click.ClickException(
                "Access denied. You can only revoke tokens you created."
            )
        raise

    click.secho(f"Token '{token_id}' has been revoked.", fg="green")


@cli.command("submit")
@_project_options(workspace=True, team=True)
@click.argument("module")
@click.argument("target")
@click.argument("argument", nargs=-1)
def submit(
    workspace: str,
    host: str,
    token: str | None,
    team: str | None,
    secure: bool | None,
    module: str,
    target: str,
    argument: tuple[str, ...],
) -> None:
    """
    Submit a workflow to be run.
    """
    use_secure = _should_use_secure(host, secure)
    resolved_token = _resolve_token(token, team, host)
    # TODO: support overriding options?
    workflow = _api_request(
        "GET",
        host,
        "get_workflow",
        resolved_token,
        secure=use_secure,
        params={
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
        resolved_token,
        secure=use_secure,
        json={
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

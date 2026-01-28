import asyncio
import functools
import os
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


def _callback(_changes: set[tuple[watchfiles.Change, str]]) -> None:
    print("Change detected. Reloading...")


def _get_default_image() -> str:
    try:
        v = pkg_version("coflux")
        return f"ghcr.io/bitroot/coflux:{v}"
    except PackageNotFoundError:
        return "ghcr.io/bitroot/coflux"


def _api_request(
    method: str, host: str, action: str, token: str | None, **kwargs
) -> t.Any:
    headers = kwargs.pop("headers", {})
    if API_VERSION:
        headers["X-API-Version"] = API_VERSION
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with httpx.Client() as client:
        response = client.request(
            method, f"http://{host}/api/{action}", headers=headers, **kwargs
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
    space_name: str,
    provides: dict[str, list[str]] | None = None,
    concurrency: int | None = None,
    token: str | None = None,
) -> str:
    payload: dict[str, t.Any] = {"projectId": project_id, "spaceName": space_name}
    if provides:
        payload["provides"] = provides
    if concurrency:
        payload["concurrency"] = concurrency
    result = _api_request("POST", host, "create_session", token, json=payload)
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
    space_name: str,
    host: str,
    targets: dict[str, dict[str, tuple[models.Target, t.Callable]]],
    token: str | None = None,
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
        json={
            "projectId": project_id,
            "spaceName": space_name,
            "manifests": manifests,
        },
    )


def _get_pool(
    host: str, project_id: str, space_name: str, pool_name: str, token: str | None
) -> dict | None:
    try:
        return _api_request(
            "GET",
            host,
            "get_pool",
            token,
            params={
                "project": project_id,
                "space": space_name,
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
    space: str,
    host: str,
    token: str | None,
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
            _register_manifests(project, space, host, targets, token=token)

        # Track whether we created the session (vs it being provided externally)
        session_provided = session_id is not None

        while True:
            # Create a session if not provided
            if not session_id:
                print("Creating session...")
                session_id = _create_session(
                    host, project, space, provides, concurrency, token=token
                )
                print("Session created.")

            try:
                with Worker(
                    project,
                    space,
                    host,
                    serialiser_configs,
                    blob_threshold,
                    blob_store_configs,
                    session_id,
                    targets,
                ) as worker:
                    asyncio.run(worker.run())
            except SessionExpiredError:
                if session_provided:
                    print("Session expired. Exiting...")
                    raise
                else:
                    print("Session expired. Recreating...")
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
    "space",
    "-s",
    "--space",
    help="Space name",
    default=_load_config().space,
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
    space: str | None,
):
    """
    Populate/update the configuration file.
    """
    # TODO: connect to server to check details?
    click.secho("Writing configuration...", fg="black")

    path = _config_path()
    data = _read_config(path)
    data["project"] = project
    data["space"] = space
    data.setdefault("server", {})["host"] = host
    _write_config(path, data)

    click.secho(
        f"Configuration written to '{path.relative_to(Path.cwd())}'.", fg="green"
    )


@cli.group()
def spaces():
    """
    Manage spaces.
    """
    pass


@spaces.command("list")
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
def spaces_list(
    project: str,
    host: str,
    token: str | None,
):
    """
    Lists spaces.
    """
    spaces = _api_request("GET", host, "get_spaces", token, params={"project": project})
    if spaces:
        # TODO: draw as tree
        _print_table(
            ("Name", "Base"),
            [
                (
                    space["name"],
                    spaces[space["baseId"]]["name"] if space["baseId"] else "(None)",
                )
                for space in spaces.values()
            ],
        )


@spaces.command("create")
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
@click.option(
    "--base",
    help="The base space to inherit from",
)
@click.argument("name")
def spaces_create(
    project: str,
    host: str,
    token: str | None,
    base: str | None,
    name: str,
):
    """
    Creates a space within the project.
    """
    base_id = None
    if base:
        spaces = _api_request("GET", host, "get_spaces", token, params={"project": project})
        space_ids_by_name = {w["name"]: id for id, w in spaces.items()}
        base_id = space_ids_by_name.get(base)
        if not base_id:
            click.BadOptionUsage("base", "Not recognised")

    # TODO: handle response
    _api_request(
        "POST",
        host,
        "create_space",
        token,
        json={
            "projectId": project,
            "name": name,
            "baseId": base_id,
        },
    )
    click.secho(f"Created space '{name}'.", fg="green")


@spaces.command("update")
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-s",
    "--space",
    help="The (current) name of the space",
    envvar="COFLUX_SPACE",
    default=_load_config().space,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
@click.option(
    "--name",
    help="The new name of the space",
)
@click.option(
    "--base",
    help="The new base space to inherit from",
)
@click.option(
    "--no-base",
    is_flag=True,
    help="Unset the base space",
)
def spaces_update(
    project: str,
    space: str,
    host: str,
    token: str | None,
    name: str | None,
    base: str | None,
    no_base: bool,
):
    """
    Updates a space within the project.
    """
    spaces = _api_request("GET", host, "get_spaces", token, params={"project": project})
    space_ids_by_name = {w["name"]: id for id, w in spaces.items()}
    space_id = space_ids_by_name.get(space)
    if not space_id:
        raise click.BadOptionUsage("space", "Not recognised")

    base_id = None
    if base:
        base_id = space_ids_by_name.get(base)
        if not base_id:
            raise click.BadOptionUsage("base", "Not recognised")

    payload = {
        "projectId": project,
        "spaceId": space_id,
    }
    if name is not None:
        payload["name"] = name

    if base is not None:
        payload["baseId"] = base_id
    elif no_base is True:
        payload["baseId"] = None

    # TODO: handle response
    _api_request("POST", host, "update_space", token, json=payload)

    click.secho(f"Updated space '{name or space}'.", fg="green")


@spaces.command("archive")
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-s",
    "--space",
    help="Space name",
    envvar="COFLUX_SPACE",
    default=_load_config().space,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
def spaces_archive(
    project: str,
    space: str,
    host: str,
    token: str | None,
):
    """
    Archives a space.
    """
    spaces = _api_request("GET", host, "get_spaces", token, params={"project": project})
    space_ids_by_name = {w["name"]: id for id, w in spaces.items()}
    space_id = space_ids_by_name.get(space)
    if not space_id:
        raise click.BadOptionUsage("space", "Not recognised")

    _api_request(
        "POST",
        host,
        "archive_space",
        token,
        json={
            "projectId": project,
            "spaceId": space_id,
        },
    )
    click.secho(f"Archived space '{space}'.", fg="green")


@cli.group()
def pools():
    """
    Manage pools.
    """
    pass


@pools.command("list")
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-s",
    "--space",
    help="Space name",
    envvar="COFLUX_SPACE",
    default=_load_config().space,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
def pools_list(project: str, space: str, host: str, token: str | None):
    """
    Lists pools.
    """
    pools = _api_request(
        "GET",
        host,
        "get_pools",
        token,
        json={"projectId": project, "spaceName": space},
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
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-s",
    "--space",
    help="Space name",
    envvar="COFLUX_SPACE",
    default=_load_config().space,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
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
@click.argument("name")
def pools_update(
    project: str,
    space: str,
    host: str,
    token: str | None,
    modules: tuple[str, ...] | None,
    provides: tuple[str, ...] | None,
    docker_image: str | None,
    name: str,
):
    """
    Updates a pool.
    """
    pool = _get_pool(host, project, space, name, token) or {}

    # TODO: support explicitly unsetting 'provides' (and modules, etc?)

    if modules is not None:
        pool["modules"] = list(modules)
    if provides is not None:
        pool["provides"] = _parse_provides(provides)
    if docker_image:
        if "launcher" not in pool or pool["launcher"]["type"] != "docker":
            pool["launcher"] = {}
        pool["launcher"]["type"] = "docker"
        pool["launcher"]["image"] = docker_image

    _api_request(
        "POST",
        host,
        "update_pool",
        token,
        json={
            "projectId": project,
            "spaceName": space,
            "poolName": name,
            "pool": pool,
        },
    )


@pools.command("delete")
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-s",
    "--space",
    help="Space name",
    envvar="COFLUX_SPACE",
    default=_load_config().space,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
@click.argument("name")
def pools_delete(project: str, space: str, host: str, token: str | None, name: str):
    """
    Deletes a pool.
    """
    _api_request(
        "POST",
        host,
        "update_pool",
        token,
        json={"projectId": project, "spaceName": space, "poolName": name, "pool": None},
    )


@cli.group()
def blobs():
    """
    Manage blobs.
    """
    pass


@blobs.command("get")
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.argument("key")
def blobs_get(host: str, key: str):
    """
    Gets a blob by key and writes the content to stdout.
    """
    config = _load_config()
    if not config.blobs.stores:
        raise click.ClickException("Blob store not configured")

    out = click.get_binary_stream("stdout")
    with BlobManager(config.blobs.stores, host) as blob_manager:
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


def _get_asset(host: str, project_id: str, asset_id: str, token: str | None) -> dict | None:
    try:
        return _api_request(
            "GET",
            host,
            "get_asset",
            token,
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
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
@click.option(
    "--match",
    help="Glob-style matcher to filter files",
)
@click.argument("id")
def assets_inspect(project: str, host: str, token: str | None, match: str | None, id: str):
    """
    Inspect an asset.
    """

    asset = _get_asset(host, project, id, token)
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
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
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
    to: Path,
    force: bool,
    match: str | None,
    id: str,
):
    """
    Downloads the contents of an asset.
    """

    asset = _get_asset(host, project, id, token)
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

    with BlobManager(config.blobs.stores, host) as blob_manager:
        click.echo(f"Downloading {len(entries)} files ({_human_size(total_size)})...")
        # TODO: parallelise downloads
        with click.progressbar(entries.items(), label="") as bar:
            for key, entry in bar:
                path = to.joinpath(key)
                path.parent.mkdir(exist_ok=True, parents=True)
                blob_manager.download(entry["blobKey"], path)


@cli.command("register")
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-w",
    "--space",
    help="Space name",
    envvar="COFLUX_SPACE",
    default=_load_config().space,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
@click.argument("module_name", nargs=-1)
def register(
    project: str,
    space: str,
    host: str,
    token: str | None,
    module_name: tuple[str, ...],
) -> None:
    """
    Register modules with the server.

    Paths to scripts can be passed instead of module names.

    Options will be loaded from the configuration file, unless overridden as arguments (or environment variables).
    """
    if not module_name:
        raise click.ClickException("No module(s) specified.")
    targets = _load_modules(list(module_name))
    _register_manifests(project, space, host, targets, token=token)
    click.secho("Manifest(s) registered.", fg="green")


@cli.command("worker")
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-s",
    "--space",
    help="Space name",
    envvar="COFLUX_SPACE",
    default=_load_config().space,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
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
    space: str,
    host: str,
    token: str | None,
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
    args = (*module_name,)
    kwargs = {
        "project": project,
        "space": space,
        "host": host,
        "token": token,
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
@click.option(
    "-p",
    "--project",
    help="Project ID",
    envvar="COFLUX_PROJECT",
    default=_load_config().project,
    show_default=True,
    required=True,
)
@click.option(
    "-s",
    "--space",
    help="Space name",
    envvar="COFLUX_SPACE",
    default=_load_config().space,
    show_default=True,
    required=True,
)
@click.option(
    "-h",
    "--host",
    help="Host to connect to",
    envvar="COFLUX_HOST",
    default=_load_config().server.host,
    show_default=True,
    required=True,
)
@click.option(
    "--token",
    help="Authentication token",
    envvar="COFLUX_TOKEN",
    default=_load_config().server.token,
)
@click.argument("module")
@click.argument("target")
@click.argument("argument", nargs=-1)
def submit(
    project: str,
    space: str,
    host: str,
    token: str | None,
    module: str,
    target: str,
    argument: tuple[str, ...],
) -> None:
    """
    Submit a workflow to be run.
    """
    # TODO: support overriding options?
    workflow = _api_request(
        "GET",
        host,
        "get_workflow",
        token,
        params={
            "project": project,
            "space": space,
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
        json={
            "projectId": project,
            "spaceName": space,
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

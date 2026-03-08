import json
import os
import subprocess

_COFLUX_BIN = os.environ.get("COFLUX_BIN", "coflux")


def _build_env(env_vars=None):
    """Build a clean subprocess env, optionally with extra COFLUX_ variables.

    Keys like "logs_store_flush_interval" are translated to
    "COFLUX_LOGS_STORE_FLUSH_INTERVAL". PATH is always included.
    """
    env = {"PATH": os.environ["PATH"]}
    if env_vars:
        for key, value in env_vars.items():
            env[f"COFLUX_{key.upper()}"] = value
    return env


def _coflux(
    *args, host=None, workspace="default", output="json", env_vars=None, timeout=30
):
    cmd = [_COFLUX_BIN]
    if host:
        cmd.extend(["--host", host])
    if workspace:
        cmd.extend(["--workspace", workspace])
    if output:
        cmd.extend(["--output", output])
    cmd.extend(args)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=_build_env(env_vars),
        check=True,
        timeout=timeout,
    )


def submit(target, *arguments, idempotency_key=None, host=None, workspace="default"):
    args = ["submit", "--no-wait"]
    if idempotency_key:
        args.extend(["--idempotency-key", idempotency_key])
    args.append(target)
    args.extend(arguments)
    result = _coflux(*args, host=host, workspace=workspace)
    return json.loads(result.stdout)


def runs_result(run_id, host=None, workspace="default"):
    result = _coflux("runs", "result", run_id, host=host, workspace=workspace)
    return json.loads(result.stdout)


def runs_inspect(run_id, host=None, workspace="default"):
    result = _coflux(
        "runs", "inspect", "--no-wait", run_id, host=host, workspace=workspace
    )
    return json.loads(result.stdout)


def runs_rerun(step_id, host=None, workspace="default"):
    result = _coflux(
        "runs", "rerun", "--no-wait", step_id, host=host, workspace=workspace
    )
    return json.loads(result.stdout)


def runs_cancel(execution_id, host=None, workspace="default"):
    _coflux(
        "runs", "cancel", str(execution_id), host=host, workspace=workspace, output=None
    )


def workspaces_create(name, base=None, host=None, workspace="default"):
    args = ["workspaces", "create", name]
    if base is not None:
        args.extend(["--base", base])
    _coflux(*args, host=host, workspace=workspace, output=None)


def workspaces_pause(host=None, workspace="default"):
    _coflux("workspaces", "pause", host=host, workspace=workspace, output=None)


def workspaces_resume(host=None, workspace="default"):
    _coflux("workspaces", "resume", host=host, workspace=workspace, output=None)


def manifests_discover(*modules, adapter=None, host=None, workspace="default"):
    args = ["manifests"]
    if adapter:
        args.extend(["--adapter", adapter])
    args.append("discover")
    args.extend(modules)
    result = _coflux(*args, host=host, workspace=workspace)
    return json.loads(result.stdout)


def manifests_archive(module, host=None, workspace="default"):
    _coflux("manifests", "archive", module, host=host, workspace=workspace, output=None)


def manifests_inspect(host=None, workspace="default"):
    result = _coflux("manifests", "inspect", host=host, workspace=workspace)
    return json.loads(result.stdout)


def assets_inspect(asset_id, host=None, workspace="default"):
    result = _coflux("assets", "inspect", str(asset_id), host=host, workspace=workspace)
    return json.loads(result.stdout)


def assets_download(asset_id, dest_dir, host=None, workspace="default"):
    _coflux(
        "assets",
        "download",
        str(asset_id),
        "--to",
        dest_dir,
        host=host,
        workspace=workspace,
        output=None,
    )


def blobs_get(key, output_path, host=None, workspace="default"):
    _coflux(
        "blobs",
        "get",
        key,
        "-o",
        output_path,
        host=host,
        workspace=workspace,
        output=None,
    )


def logs_get(
    run_id,
    step_attempt=None,
    from_ts=None,
    host=None,
    workspace="default",
    json_output=True,
):
    args = ["logs", run_id]
    if step_attempt:
        args.append(step_attempt)
    if from_ts is not None:
        args.extend(["--from", str(from_ts)])
    result = _coflux(
        *args, host=host, workspace=workspace, output="json" if json_output else None
    )
    if json_output:
        return json.loads(result.stdout)
    return result.stdout


def worker(
    modules,
    adapter,
    concurrency=1,
    provides=None,
    host=None,
    workspace="default",
    env_vars=None,
    log_level=None,
):
    cmd = [_COFLUX_BIN]
    if host:
        cmd.extend(["--host", host])
    if workspace:
        cmd.extend(["--workspace", workspace])
    cmd.extend(
        [
            "worker",
            "--register",
            "--adapter",
            adapter,
            "--concurrency",
            str(concurrency),
        ]
    )
    if log_level:
        cmd.extend(["--log-level", log_level])
    if provides:
        for key, values in provides.items():
            cmd.extend(["--provides", ",".join(f"{key}:{v}" for v in values)])
    cmd.extend(modules)
    return subprocess.Popen(
        cmd, env=_build_env(env_vars), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

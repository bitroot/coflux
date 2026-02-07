import json
import os
import subprocess

_COFLUX_BIN = os.environ.get("COFLUX_BIN", "coflux")


def _coflux(*args, json_output=False, env=None):
    cmd = [_COFLUX_BIN]
    if json_output:
        cmd.append("--json")
    cmd.extend(args)
    return subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)


def submit(target, *arguments, env=None):
    result = _coflux("submit", target, *arguments, json_output=True, env=env)
    return json.loads(result.stdout)


def runs_result(run_id, env=None):
    result = _coflux("runs", "result", run_id, json_output=True, env=env)
    return json.loads(result.stdout)


def runs_inspect(run_id, env=None):
    result = _coflux("runs", "inspect", run_id, json_output=True, env=env)
    return json.loads(result.stdout)


def runs_rerun(step_id, env=None):
    result = _coflux("runs", "rerun", step_id, json_output=True, env=env)
    return json.loads(result.stdout)


def runs_cancel(execution_id, env=None):
    _coflux("runs", "cancel", str(execution_id), env=env)


def workspaces_create(name, base=None, env=None):
    args = ["workspaces", "create", name]
    if base is not None:
        args.extend(["--base", base])
    _coflux(*args, env=env)


def workspaces_pause(env=None):
    _coflux("workspaces", "pause", env=env)


def workspaces_resume(env=None):
    _coflux("workspaces", "resume", env=env)


def manifests_discover(*modules, adapter=None, env=None):
    args = ["manifests"]
    if adapter:
        args.extend(["--adapter", adapter])
    args.append("discover")
    args.extend(modules)
    result = _coflux(*args, json_output=True, env=env)
    return json.loads(result.stdout)


def manifests_archive(module, env=None):
    _coflux("manifests", "archive", module, env=env)


def manifests_inspect(env=None):
    result = _coflux("manifests", "inspect", json_output=True, env=env)
    return json.loads(result.stdout)


def assets_inspect(asset_id, env=None):
    result = _coflux("assets", "inspect", str(asset_id), json_output=True, env=env)
    return json.loads(result.stdout)


def assets_download(asset_id, dest_dir, env=None):
    _coflux("assets", "download", str(asset_id), "--to", dest_dir, env=env)


def blobs_get(key, output_path, env=None):
    _coflux("blobs", "get", key, "-o", output_path, env=env)


def logs_get(run_id, step_attempt=None, env=None, json_output=True):
    args = ["logs", run_id]
    if step_attempt:
        args.append(step_attempt)
    result = _coflux(*args, json_output=json_output, env=env)
    if json_output:
        return json.loads(result.stdout)
    return result.stdout


def worker(modules, adapter, concurrency=1, provides=None, env=None):
    cmd = [_COFLUX_BIN, "worker", "--register", "--adapter", adapter,
           "--concurrency", str(concurrency)]
    if provides:
        for key, values in provides.items():
            cmd.extend(["--provides", f"{key}={','.join(values)}"])
    cmd.extend(modules)
    return subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

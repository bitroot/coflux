#!/usr/bin/env python3
"""Build a worker image, publish it where a pool's launcher will find it,
and bring the pool's configuration up to date.

Driven by a deploy file (by default `deploy.toml` in the current
directory). Its `[pools.<name>]` tables are pool configurations in the
shape `coflux pools export` writes, and an `[image]` table says how to
build the image they run:

    [image]
    dockerfile = "Dockerfile"            # relative to the deploy file
    context = "."
    platform = "linux/amd64"
    contexts = { cli = ".build" }        # extra named build contexts
    args = { KEY = "value" }             # build args
    before = ["make something"]          # shell commands run first
    ecr = "my-repo"                      # push to this ECR repository...
    # repository = "ghcr.io/org/app"     # ...or to any registry
    # (neither: the image is only tagged locally, for a docker pool)

    [pools.fn]
    modules = ["myapp"]
    [pools.fn.launcher]
    type = "lambda"
    function = "my-function"
    region = "eu-west-2"
    [pools.fn.deploy]                    # the target's own settings
    memory = 1024
    timeout = "5m"

    [manifests]                          # optional: register the modules
    adapter = ["python", "-m", "coflux"]

What deploying does depends on the launcher:

- lambda: the function's code is pointed at the new image (the function,
  and an execution role for it, are created if they don't exist). With a
  `qualifier` that's an alias, a version is published and the alias moved
  to it; with one that's a version number, the pool is moved to the new
  version.
- ecs: a new revision of the task definition is registered, with the
  container's image replaced. A pool that names a revision is moved to the
  new one; one that names only the family picks it up as the latest.
- docker, kubernetes: the pool's `image` is set to the new image.
- process: nothing is built; only the pool is updated.

Then the pools are imported (`coflux pools import --only ...`), so other
pools in the workspace are left alone, and, with a `[manifests]` table,
the pools' modules are registered.

Connection options (--host, --token, --workspace, --project) are passed
through to `coflux`; otherwise it reads its usual configuration. The CLI
used is `coflux` on the PATH, or $COFLUX_BIN.
"""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import NoReturn

# Lambda's own policy for a function that only needs to write its logs.
LAMBDA_BASIC_POLICY = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"

LAMBDA_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

# Fields describe-task-definition returns that register-task-definition
# doesn't take.
ECS_READ_ONLY_FIELDS = [
    "taskDefinitionArn",
    "revision",
    "status",
    "requiresAttributes",
    "compatibilities",
    "registeredAt",
    "registeredBy",
    "deregisteredAt",
]

DURATION_UNITS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}

dry_run = False


def log(message):
    print(f"==> {message}", file=sys.stderr)


def fail(message) -> NoReturn:
    print(f"error: {message}", file=sys.stderr)
    sys.exit(1)


def run(args, *, cwd=None, input=None, capture=False, mutates=True, check=True):
    """Runs a command, echoing it. In a dry run, a command that changes
    something is only echoed."""
    print(f"$ {shlex.join(str(a) for a in args)}", file=sys.stderr)
    if dry_run and mutates:
        return subprocess.CompletedProcess(args, 0, "", "")
    result = subprocess.run(
        [str(a) for a in args],
        cwd=cwd,
        input=input,
        text=True,
        check=False,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if check and result.returncode != 0:
        if capture:
            sys.stderr.write(result.stderr)
        fail(f"command failed with exit code {result.returncode}")
    return result


def aws_command(service, args, region):
    command = ["aws", service, *args, "--output", "json"]
    return command + ["--region", region] if region else command


def aws(service, *args, region=None, mutates=True):
    """Runs an AWS CLI command, and returns its parsed output (empty in a
    dry run)."""
    command = aws_command(service, args, region)
    result = run(command, capture=True, mutates=mutates)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def aws_try(service, *args, region=None, mutates=False):
    """Runs an AWS CLI command that's allowed to fail - a lookup of
    something that might not exist - and returns the completed process."""
    command = aws_command(service, args, region)
    return run(command, capture=True, mutates=mutates, check=False)


def parse_duration_seconds(value):
    if not isinstance(value, str):
        fail(f'duration {value!r} needs a unit (e.g. "5m")')
    total = 0.0
    parts = re.findall(r"(\d+(?:\.\d+)?)(ms|s|m|h)", value)
    if not parts or "".join(n + u for n, u in parts) != value:
        fail(f'invalid duration {value!r} (expected e.g. "30s", "5m")')
    for number, unit in parts:
        total += float(number) * DURATION_UNITS[unit]
    return int(total)


def field(table, camel, snake=None):
    """Reads a launcher field written either way `pools import` accepts."""
    snake = snake or re.sub(r"([A-Z])", lambda m: "_" + m.group(1).lower(), camel)
    return table.get(snake, table.get(camel))


def set_field(table, camel, value):
    snake = re.sub(r"([A-Z])", lambda m: "_" + m.group(1).lower(), camel)
    table.pop(camel, None)
    table[snake] = value


def parse_set(assignments):
    """Parses --set key=value into launcher overrides, the way `coflux
    pools update --set` does: JSON if it parses, otherwise a string."""
    overrides = {}
    for assignment in assignments:
        key, sep, value = assignment.partition("=")
        if not sep:
            fail(f"--set expects key=value, not {assignment!r}")
        try:
            overrides[key] = json.loads(value)
        except json.JSONDecodeError:
            overrides[key] = value
    return overrides


# Images


def build_image(image, base, tag):
    for command in image.get("before", []):
        print(f"$ {command}", file=sys.stderr)
        if dry_run:
            continue
        if subprocess.run(command, shell=True, cwd=base, check=False).returncode:
            fail("'before' command failed")

    command = [
        "docker",
        "build",
        "--provenance=false",
        "-f",
        base / image.get("dockerfile", "Dockerfile"),
        "-t",
        tag,
    ]
    if platform := image.get("platform"):
        command += ["--platform", platform]
    for name, path in image.get("contexts", {}).items():
        command += ["--build-context", f"{name}={base / path}"]
    for name, value in image.get("args", {}).items():
        command += ["--build-arg", f"{name}={value}"]
    command.append(base / image.get("context", "."))
    run(command)


def ecr_registry(region):
    account = aws("sts", "get-caller-identity", "--query", "Account", mutates=False)
    return f"{account}.dkr.ecr.{region}.amazonaws.com"


def ensure_ecr_repository(name, region):
    result = aws_try(
        "ecr", "describe-repositories", "--repository-names", name, region=region
    )
    if result.returncode == 0:
        return
    if "RepositoryNotFoundException" not in result.stderr:
        sys.stderr.write(result.stderr)
        fail(f"couldn't look up ECR repository {name!r}")
    log(f"Creating ECR repository {name}")
    aws("ecr", "create-repository", "--repository-name", name, region=region)


def push_image(image, local_tag, pool_region):
    """Pushes the image, if it has somewhere to go, and returns the
    reference a launcher should use: by digest, so that a deploy pins
    exactly what was built."""
    if repo := image.get("ecr"):
        region = image.get("region") or pool_region
        if not region:
            fail("an ECR image needs a region ([image] region, or a pool's)")
        ensure_ecr_repository(repo, region)
        registry = ecr_registry(region)
        password = run(
            ["aws", "ecr", "get-login-password", "--region", region],
            capture=True,
            mutates=False,
        ).stdout
        run(
            ["docker", "login", "--username", "AWS", "--password-stdin", registry],
            input=password,
            capture=True,
        )
        repository = f"{registry}/{repo}"
    elif repository := image.get("repository"):
        pass
    else:
        return local_tag

    tag = local_tag.rsplit(":", 1)[1]
    remote = f"{repository}:{tag}"
    run(["docker", "tag", local_tag, remote])
    run(["docker", "push", remote])
    if dry_run:
        return remote
    digests = json.loads(
        run(
            ["docker", "inspect", "--format", "{{json .RepoDigests}}", remote],
            capture=True,
            mutates=False,
        ).stdout
    )
    for digest in digests:
        if digest.startswith(repository + "@"):
            return digest
    return remote


# Launchers


def deploy_lambda(name, launcher, settings, image_ref):
    function = field(launcher, "function")
    region = field(launcher, "region")
    if not function:
        fail(f"pool {name!r}: a lambda launcher needs a function")

    config_args = []
    if "memory" in settings:
        config_args += ["--memory-size", settings["memory"]]
    if "timeout" in settings:
        config_args += ["--timeout", parse_duration_seconds(settings["timeout"])]
    if "ephemeral_storage" in settings:
        config_args += [
            "--ephemeral-storage",
            f"Size={settings['ephemeral_storage']}",
        ]

    existing = aws_try(
        "lambda",
        "get-function-configuration",
        "--function-name",
        function,
        region=region,
    )
    if existing.returncode != 0 and "ResourceNotFoundException" not in existing.stderr:
        sys.stderr.write(existing.stderr)
        fail(f"couldn't look up function {function!r}")

    if existing.returncode != 0:
        role = settings.get("role") or ensure_lambda_role(f"{function}-execution")
        log(f"Creating function {function}")
        create_lambda_function(function, region, image_ref, role, settings, config_args)
    else:
        log(f"Updating function {function}")
        aws(
            "lambda",
            "update-function-code",
            "--function-name",
            function,
            "--image-uri",
            image_ref,
            region=region,
        )
        wait_for_lambda(function, region)
        if config_args or "role" in settings:
            if "role" in settings:
                config_args += ["--role", settings["role"]]
            aws(
                "lambda",
                "update-function-configuration",
                "--function-name",
                function,
                *config_args,
                region=region,
            )
            wait_for_lambda(function, region)

    qualifier = field(launcher, "qualifier")
    if qualifier and qualifier != "$LATEST":
        published = aws(
            "lambda", "publish-version", "--function-name", function, region=region
        )
        version = published.get("Version", "<new version>")
        if qualifier.isdigit():
            log(f"Moving pool {name} to version {version}")
            launcher["qualifier"] = version
        else:
            point_lambda_alias(function, region, qualifier, version)


def create_lambda_function(function, region, image_ref, role, settings, config_args):
    command = [
        "create-function",
        "--function-name",
        function,
        "--package-type",
        "Image",
        "--code",
        f"ImageUri={image_ref}",
        "--role",
        role,
        "--architectures",
        settings.get("architecture", "x86_64"),
        *config_args,
    ]
    # A role that has only just been created can take a few seconds before
    # Lambda is allowed to assume it.
    for attempt in range(10):
        result = aws_try("lambda", *command, region=region, mutates=True)
        if result.returncode == 0:
            break
        if "cannot be assumed" not in result.stderr or attempt == 9:
            sys.stderr.write(result.stderr)
            fail(f"couldn't create function {function!r}")
        log("Waiting for the execution role to be usable...")
        time.sleep(5)
    wait_for_lambda(function, region, "function-active-v2")


def wait_for_lambda(function, region, condition="function-updated-v2"):
    run(
        ["aws", "lambda", "wait", condition, "--function-name", function]
        + (["--region", region] if region else [])
    )


def point_lambda_alias(function, region, alias, version):
    exists = aws_try(
        "lambda",
        "get-alias",
        "--function-name",
        function,
        "--name",
        alias,
        region=region,
    )
    action = "update-alias" if exists.returncode == 0 else "create-alias"
    log(f"Pointing alias {alias} at version {version}")
    aws(
        "lambda",
        action,
        "--function-name",
        function,
        "--name",
        alias,
        "--function-version",
        version,
        region=region,
    )


def ensure_lambda_role(role_name):
    result = aws_try("iam", "get-role", "--role-name", role_name)
    if result.returncode == 0:
        return json.loads(result.stdout)["Role"]["Arn"]
    if "NoSuchEntity" not in result.stderr:
        sys.stderr.write(result.stderr)
        fail(f"couldn't look up role {role_name!r}")
    log(f"Creating execution role {role_name}")
    created = aws(
        "iam",
        "create-role",
        "--role-name",
        role_name,
        "--assume-role-policy-document",
        json.dumps(LAMBDA_TRUST_POLICY),
    )
    aws(
        "iam",
        "attach-role-policy",
        "--role-name",
        role_name,
        "--policy-arn",
        LAMBDA_BASIC_POLICY,
    )
    return created.get("Role", {}).get("Arn", f"<{role_name}>")


def deploy_ecs(name, launcher, settings, image_ref):
    task_definition = field(launcher, "taskDefinition")
    region = field(launcher, "region")
    if not task_definition:
        fail(f"pool {name!r}: an ecs launcher needs a task definition")

    described = aws(
        "ecs",
        "describe-task-definition",
        "--task-definition",
        task_definition,
        "--include",
        "TAGS",
        region=region,
        mutates=False,
    )
    definition = described["taskDefinition"]
    containers = definition["containerDefinitions"]
    container_name = field(launcher, "containerName")
    container = (
        next((c for c in containers if c["name"] == container_name), None)
        if container_name
        else containers[0]
    )
    if container is None:
        fail(f"task definition has no container {container_name!r}")
    container["image"] = image_ref

    for key in ECS_READ_ONLY_FIELDS:
        definition.pop(key, None)
    if described.get("tags"):
        definition["tags"] = described["tags"]

    log(f"Registering a new revision of {definition['family']}")
    registered = aws(
        "ecs",
        "register-task-definition",
        "--cli-input-json",
        json.dumps(definition),
        region=region,
    )
    # A pool that names a revision (or a revision's ARN) stays on it until
    # it's moved; one that names only the family runs the latest.
    if re.search(r":\d+$", task_definition):
        revision = registered.get("taskDefinition", {}).get("revision", "<new>")
        new = f"{definition['family']}:{revision}"
        log(f"Moving pool {name} to {new}")
        set_field(launcher, "taskDefinition", new)


def deploy_image_field(name, launcher, settings, image_ref):
    launcher["image"] = image_ref


LAUNCHERS = {
    "lambda": deploy_lambda,
    "ecs": deploy_ecs,
    "docker": deploy_image_field,
    "kubernetes": deploy_image_field,
}


# Pools


def toml_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(toml_value(v) for v in value) + "]"
    if isinstance(value, dict):
        items = ", ".join(
            f"{json.dumps(k)} = {toml_value(v)}" for k, v in value.items()
        )
        return "{ " + items + " }" if items else "{}"
    fail(f"can't write {value!r} to TOML")


def pools_toml(pools):
    lines = []
    for name, pool in pools.items():
        lines.append(f"[pools.{json.dumps(name)}]")
        for key, value in pool.items():
            lines.append(f"{key} = {toml_value(value)}")
        lines.append("")
    return "\n".join(lines)


def coflux(args, coflux_args, **kwargs):
    binary = os.environ.get("COFLUX_BIN", "coflux")
    return run([binary, *coflux_args, *args], **kwargs)


def main():
    global dry_run

    parser = argparse.ArgumentParser(
        description="Build and publish a pool's worker image, and update the pool.",
        epilog="See the top of this script for the deploy file's format.",
    )
    parser.add_argument("-f", "--file", default="deploy.toml", help="deploy file")
    parser.add_argument(
        "-p",
        "--pool",
        action="append",
        help="only deploy these pools (default: all in the file)",
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="override a launcher field, as for `coflux pools update --set`",
    )
    parser.add_argument("--tag", help="image tag (default: a timestamp)")
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="only update the pools (and register manifests)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show what would be done, without changing anything",
    )
    parser.add_argument("--host")
    parser.add_argument("--token")
    parser.add_argument("--project")
    parser.add_argument("-w", "--workspace")
    options = parser.parse_args()
    dry_run = options.dry_run

    path = Path(options.file).resolve()
    base = path.parent
    try:
        with open(path, "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        fail(f"no deploy file at {path}")

    all_pools = config.get("pools", {})
    names = options.pool or list(all_pools)
    for name in names:
        if name not in all_pools:
            fail(f"no pool {name!r} in {path.name}")
    if not names:
        fail(f"{path.name} defines no pools")

    coflux_args = []
    for option in ("host", "token", "project", "workspace"):
        if value := getattr(options, option):
            coflux_args += [f"--{option}", value]

    overrides = parse_set(options.set)
    pools = {}
    deploy_settings = {}
    for name in names:
        pool = dict(all_pools[name])
        deploy_settings[name] = pool.pop("deploy", {})
        launcher = dict(pool.get("launcher", {}))
        for key, value in overrides.items():
            set_field(launcher, key, value)
        pool["launcher"] = launcher
        pools[name] = pool

    image = config.get("image")
    needs_image = any(
        pool["launcher"].get("type") in LAUNCHERS for pool in pools.values()
    )
    if needs_image and not options.skip_build:
        if not image:
            fail(f"{path.name} has no [image] to build")
        stem = image.get("ecr") or image.get("repository") or base.name
        tag = options.tag or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        local_tag = f"{stem.rsplit('/', 1)[-1]}:{tag}"
        log(f"Building {local_tag}")
        build_image(image, base, local_tag)

        regions = {field(p["launcher"], "region") for p in pools.values()} - {None}
        image_ref = push_image(image, local_tag, next(iter(regions), None))
        log(f"Image: {image_ref}")

        for name, pool in pools.items():
            launcher = pool["launcher"]
            if deploy := LAUNCHERS.get(launcher.get("type")):
                deploy(name, launcher, deploy_settings[name], image_ref)
    elif needs_image:
        log("Skipping the build; launchers keep the image they have")

    log(f"Updating pools: {', '.join(names)}")
    with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as f:
        f.write(pools_toml(pools))
    try:
        # A dry run of the import is safe, and shows the diff.
        import_args = ["pools", "import", f.name, "--only", ",".join(names)]
        coflux(
            import_args + ["--dry-run" if dry_run else "--yes"],
            coflux_args,
            mutates=False,
        )
    finally:
        os.unlink(f.name)

    if manifests := config.get("manifests"):
        modules = sorted(
            {m for pool in pools.values() for m in pool.get("modules", [])}
        )
        register = ["manifests", "register"]
        if adapter := manifests.get("adapter"):
            register += ["--adapter", ",".join(adapter)]
        register += modules or ["--all-modules"]
        log("Registering manifests")
        coflux(register, coflux_args, cwd=base / manifests.get("directory", "."))


if __name__ == "__main__":
    main()

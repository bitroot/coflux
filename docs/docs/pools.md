# Pools

A _pool_ defines a configuration for automatically launching workers in a workspace. Instead of manually starting workers, you can configure a pool and the server will launch and manage workers on your behalf.

## Configuring a pool

Pools are configured using the CLI. Use `--type` to specify the launcher type and `--set` to configure launcher-specific fields:

```bash
coflux pools create mypool --type process \
  --set directory=/path/to/project \
  --modules myapp.workflows myapp.tasks
```

### Launcher types

Each pool has a _launcher_ that determines how workers are started. The server must be configured to allow the relevant launcher type (`COFLUX_LAUNCHER_TYPES`).

#### Docker launcher

Launches workers as Docker containers:

```bash
coflux pools create mypool --type docker \
  --set image=myorg/myapp:latest \
  --set dockerHost=tcp://docker:2375 \
  --modules myapp.workflows
```

| Field | Description |
|-------|-------------|
| `image` | Docker image to run |
| `dockerHost` | Docker host (default: local socket) |
| `networkMode` | Container network mode (default: `host`) |

The default `host` network mode lets workers reach a server running on the same
machine without further configuration. It isn't available on Docker Desktop,
where containers run inside a VM — there, set `networkMode` to `bridge` and
point `serverHost` at `host.docker.internal`.

#### Process launcher

Launches workers as local processes:

```bash
coflux pools create mypool --type process \
  --set directory=/path/to/project \
  --modules myapp.workflows
```

| Field | Description |
|-------|-------------|
| `directory` | Working directory for the worker process |

#### Kubernetes launcher

:::note
The Kubernetes launcher is experimental — the API may change based on feedback.
:::

Launches workers as Kubernetes Jobs:

```bash
coflux pools create mypool --type kubernetes \
  --set image=myorg/myapp:latest \
  --set namespace=coflux-workers \
  --set serverHost=coflux-server.coflux.svc:7777 \
  --modules myapp.workflows
```

When the Coflux server runs inside Kubernetes, it automatically uses in-cluster authentication. For external servers, provide the API server URL and name a [secret](#secrets) holding a bearer token:

```bash
coflux secrets set k8s-token < /path/to/token

coflux pools create mypool --type kubernetes \
  --set image=myorg/myapp:latest \
  --set apiServer=https://my-cluster.example.com:6443 \
  --set tokenSecret=k8s-token \
  --set serverHost=coflux.example.com:7777 \
  --modules myapp.workflows
```

| Field | Description |
|-------|-------------|
| `image` | Container image to run |
| `namespace` | Kubernetes namespace (default: `default`) |
| `apiServer` | Kubernetes API server URL (default: in-cluster) |
| `tokenSecret` | Name of the secret holding the bearer token |
| `caCert` | Path, on the server's host, to a CA certificate file for TLS verification |
| `insecure` | Skip TLS verification |
| `serviceAccount` | Service account for launched pods |
| `imagePullPolicy` | Image pull policy (`Always`, `IfNotPresent`, `Never`) |
| `imagePullSecrets` | Image pull secret names |
| `nodeSelector` | Node selection labels |
| `tolerations` | Pod tolerations |
| `hostAliases` | Host aliases for pods |
| `resources` | CPU/memory/GPU requests and limits |
| `labels` | Custom pod labels |
| `annotations` | Custom pod annotations |
| `activeDeadlineSeconds` | Job timeout in seconds |
| `volumes` | Kubernetes volume definitions |
| `volumeMounts` | Volume mounts in container |

#### ECS launcher

:::note
The ECS launcher is experimental — the API may change based on feedback.
:::

Launches workers as Amazon ECS tasks, on Fargate by default:

```bash
coflux pools create mypool --type ecs \
  --set cluster=workers \
  --set taskDefinition=myapp-worker \
  --set region=eu-west-2 \
  --set subnets='["subnet-0123", "subnet-4567"]' \
  --set securityGroups=sg-0123 \
  --set serverHost=coflux.example.com \
  --modules myapp.workflows
```

The pool refers to an existing task definition, which is where the image,
CPU and memory, IAM roles and log configuration are set. For each worker,
one task is run from it, with the container's command overridden to the
modules to host and its environment to the worker's connection details.
So the container should run the Coflux worker with the modules left to
the command — an `ENTRYPOINT` of `coflux worker --adapter ...`, say.

Workers connect out to the server, so a task needs a route to it and
nothing needs to reach the task: a public IP (`assignPublicIp`) in a
public subnet, or a NAT gateway from a private one.

Credentials come from the [secret](#secrets) named by `credentialsSecret`,
whose value is JSON in the shape the AWS CLI produces, so a profile's
credentials can be stored directly:

```bash
aws configure export-credentials --profile sandbox | coflux secrets set aws-sandbox
coflux pools update mypool --set credentialsSecret=aws-sandbox
```

Without one, credentials come from the server's surroundings the way the
AWS SDKs look: `AWS_ACCESS_KEY_ID` and friends in its environment, its ECS
task role, or its EC2 instance profile. Either way they need `ecs:RunTask`,
`ecs:DescribeTasks` and `ecs:StopTask` on the cluster,
`ecs:DescribeTaskDefinition` unless `containerName` is set, and
`iam:PassRole` for the roles the task definition names.

ECS doesn't expose container output through its API, so a worker's log
tail isn't shown; a task that fails to start reports its reason in its
place. Give the task definition a log configuration (`awslogs`, say) to
see what workers print.

| Field | Description |
|-------|-------------|
| `cluster` | ECS cluster name or ARN |
| `taskDefinition` | Task definition family, `family:revision`, or ARN |
| `region` | AWS region |
| `containerName` | Container to override (default: the task definition's first) |
| `launchType` | `FARGATE` (default), `EC2`, or `EXTERNAL` |
| `capacityProvider` | Capacity provider to use instead of a launch type (e.g. `FARGATE_SPOT`) |
| `subnets` | Subnet IDs for the task (required on Fargate) |
| `securityGroups` | Security group IDs for the task |
| `assignPublicIp` | Give the task a public IP |
| `platformVersion` | Fargate platform version |
| `credentialsSecret` | Name of the secret holding AWS credentials as JSON (`AccessKeyId`, `SecretAccessKey`, optional `SessionToken`) |
| `endpoint` | ECS API endpoint override (e.g. a VPC endpoint) |

### Common fields

These fields apply to all launcher types:

| Field / Flag | Description |
|--------------|-------------|
| `--modules`, `-m` | Modules to host (can be specified multiple times). Module names only — wildcards aren't supported, since this is also what launched workers are told to import |
| `--provides` | Features that workers provide (e.g., `gpu:A100`) |
| `--accepts` | Tags that executions must have to be assigned to this pool |
| `idleTimeout` | Seconds the pool keeps an idle worker before stopping it (default: 5). Worth raising for launchers with slow starts, such as ECS |
| `serverHost` | Server host override for launched workers |
| `serverSecure` | Use TLS for server connection |
| `adapter` | Adapter command |
| `concurrency` | Maximum concurrent executions per worker |
| `env` | Environment variables (e.g., `--set env.KEY=VALUE`) |
| `envSecrets` | Environment variables set from secrets (e.g., `--set envSecrets.API_KEY=api-key`) |

## Secrets

Anything a pool needs that mustn't be written down — an API key for workers,
a cluster token, cloud credentials — is a _secret_: stored by the server,
encrypted, and referred to by name. A pool's configuration, `pools get`, and
`pools export` only ever carry the name.

The value is read from stdin, so it never appears on the command line:

```bash
printf '%s' "$OPENAI_API_KEY" | coflux secrets set openai
coflux pools update mypool --set envSecrets.OPENAI_API_KEY=openai
```

`--from-env` and `--from-file` read it from an environment variable or a file
instead. `secrets list` shows names, scopes and versions, never values, and
`secrets delete` removes one.

A secret applies to a _scope_: a workspace name, or a prefix of one. A secret
set for `development` applies to `development/joe`, and the nearest scope wins,
so `development/joe` can override it. By default the scope is the current
workspace; `--scope` names another, and `--global` applies it to every
workspace. Scopes follow workspace names, not what a workspace inherits from:
a workspace that inherits from `production` doesn't see production's secrets.

Setting a secret takes operator access to its scope, and `--global` takes
access to every workspace. Setting one again replaces its value and bumps its
version; workers already running keep the value they were launched with.
Values are encrypted with a key derived from `COFLUX_SECRET`, which must be
configured for secrets to be used.

A pool that names a secret its workspace can't see is refused when it is
created, updated, or imported.

## Managing pools

```bash
# List pools in a workspace
coflux pools list

# Get pool configuration
coflux pools get mypool

# Update pool configuration
coflux pools update mypool --set image=myorg/myapp:v2

# View launched workers
coflux pools launches mypool

# Watch launches in real-time
coflux pools launches mypool --watch

# Disable a pool (drains workers, stops new assignments)
coflux pools disable mypool

# Re-enable a disabled pool
coflux pools enable mypool

# Delete a pool
coflux pools delete mypool
```

### Exporting and importing

Pool configurations can be exported to TOML and imported back, making it easy to replicate setups across environments or manage configurations declaratively:

```bash
# Export all pools
coflux pools export -o pools.toml

# Export specific pools
coflux pools export --only mypool --only gpu-pool -o pools.toml

# Import pools
coflux pools import pools.toml
```

Pools refer to secrets by name, so an export never contains a value. Import it
somewhere else and the same secrets must exist there, or the import is refused
with the names that are missing.

## Provides, accepts, and requires

Workers can declare features they _provide_, and targets can _require_ specific features. This allows routing executions to appropriate workers — for example, GPU-intensive tasks to GPU-equipped workers.

On the worker side, configure `provides` on the pool:

```bash
coflux pools create gpu-pool --type docker \
  --set image=myorg/gpu-worker:latest \
  --provides gpu:A100 \
  --modules myapp.workflows
```

On the task side, specify `requires` in the decorator:

```python
@cf.task(requires={"gpu": "A100"})
def train_model(data): ...
```

The `requires` parameter accepts a dictionary where keys are feature names and values can be a specific value (`"A100"`), a list of acceptable values (`["A100", "H100"]`), or `True` to require the feature with any value.

Setting `requires` on a `@workflow` applies to the entire run — child tasks are automatically routed to matching workers unless explicitly overridden per-task.

### Accepts tags

Pools can also declare what they _accept_. When a pool defines `accepts` tags, only executions whose `requires` tags match will be assigned to it. This prevents general work from being scheduled to specialised workers:

```bash
coflux pools create gpu-pool --type docker \
  --set image=myorg/gpu-worker:latest \
  --accepts gpu:A100,H100 \
  --modules myapp.workflows
```

In this example, only tasks that require `gpu:A100` or `gpu:H100` will be assigned to `gpu-pool`. Tasks without a matching `requires` tag will not be sent to this pool, even if there are idle workers.

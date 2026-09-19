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

When the Coflux server runs inside Kubernetes, it automatically uses in-cluster authentication. For external servers, provide the API server URL and a bearer token:

```bash
coflux pools create mypool --type kubernetes \
  --set image=myorg/myapp:latest \
  --set apiServer=https://my-cluster.example.com:6443 \
  --set token="$(cat /path/to/token)" \
  --set serverHost=coflux.example.com:7777 \
  --modules myapp.workflows
```

Note that the token is stored in the orchestration database.

| Field | Description |
|-------|-------------|
| `image` | Container image to run |
| `namespace` | Kubernetes namespace (default: `default`) |
| `apiServer` | Kubernetes API server URL (default: in-cluster) |
| `token` | Bearer token for API authentication |
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

### Common fields

These fields apply to all launcher types:

| Field / Flag | Description |
|--------------|-------------|
| `--modules`, `-m` | Modules to host (can be specified multiple times). Module names only — wildcards aren't supported, since this is also what launched workers are told to import |
| `--provides` | Features that workers provide (e.g., `gpu:A100`) |
| `--accepts` | Tags that executions must have to be assigned to this pool |
| `serverHost` | Server host override for launched workers |
| `serverSecure` | Use TLS for server connection |
| `adapter` | Adapter command |
| `concurrency` | Maximum concurrent executions per worker |
| `env` | Environment variables (e.g., `--set env.KEY=VALUE`) |

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

Launcher secrets — currently the Kubernetes `token` — are redacted on export
unless `--include-secrets` is given. Importing a redacted file is refused rather
than silently clearing the secrets it omits, so use `--include-secrets` when the
exported file is meant to be imported again:

```bash
coflux pools export --include-secrets -o pools.toml
```

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

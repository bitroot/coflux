# Pools

:::note
Pools are an early preview feature. The concept is functional but will become more useful as additional launcher types are added.
:::

A _pool_ defines a configuration for automatically launching workers in a workspace. Instead of manually starting workers, you can configure a pool and the server will launch and manage workers on your behalf.

## Configuring a pool

Pools are configured using the CLI:

```bash
coflux pools update mypool \
  --module myapp.workflows \
  --module myapp.tasks \
  --docker-image myorg/myapp:latest
```

### Launcher types

Each pool has a _launcher_ that determines how workers are started. The server must be configured to allow the relevant launcher type (`COFLUX_LAUNCHER_TYPES`).

#### Docker launcher

Launches workers as Docker containers:

```bash
coflux pools update mypool \
  --docker-image myorg/myapp:latest \
  --docker-host tcp://docker:2375
```

| Option | Description |
|--------|-------------|
| `--docker-image` | Docker image to run |
| `--docker-host` | Docker host (default: local socket) |

#### Process launcher

Launches workers as local processes:

```bash
coflux pools update mypool \
  --process-dir /path/to/project
```

| Option | Description |
|--------|-------------|
| `--process-dir` | Working directory for the worker process |

#### Kubernetes launcher

Launches workers as Kubernetes Jobs:

```bash
coflux pools update mypool \
  --k8s-image myorg/myapp:latest \
  --k8s-namespace coflux-workers \
  --server-host coflux-server.coflux.svc:7777
```

When the Coflux server runs inside Kubernetes, it automatically uses in-cluster authentication. For external servers, provide the API server URL and a bearer token:

```bash
coflux pools update mypool \
  --k8s-image myorg/myapp:latest \
  --k8s-api-server https://my-cluster.example.com:6443 \
  --k8s-token "$(cat /path/to/token)" \
  --server-host coflux.example.com:7777
```

| Option | Description |
|--------|-------------|
| `--k8s-image` | Container image to run |
| `--k8s-namespace` | Kubernetes namespace (default: `default`) |
| `--k8s-api-server` | Kubernetes API server URL (default: in-cluster) |
| `--k8s-token` | Bearer token for API authentication |
| `--k8s-service-account` | Service account for launched pods |

### Common options

These options apply to all launcher types:

| Option | Description |
|--------|-------------|
| `--module`, `-m` | Modules to host (can be specified multiple times) |
| `--provides` | Features that workers provide (e.g., `gpu:A100`) |
| `--server-host` | Server host override for launched workers |
| `--adapter` | Adapter command |
| `--concurrency` | Maximum concurrent executions per worker |
| `--env` | Environment variables (e.g., `--env KEY=VALUE`) |

## Managing pools

```bash
# List pools in a workspace
coflux pools list

# Get pool configuration
coflux pools get mypool

# View launched workers
coflux pools launches mypool

# Watch launches in real-time
coflux pools launches mypool --watch

# Delete a pool
coflux pools delete mypool
```

## Provides and requires

Workers can declare features they _provide_, and targets can _require_ specific features. This allows routing executions to appropriate workers — for example, GPU-intensive tasks to GPU-equipped workers.

On the worker side, configure `provides` on the pool:

```bash
coflux pools update gpu-pool \
  --docker-image myorg/gpu-worker:latest \
  --provides "gpu:A100"
```

On the task side, specify `requires` in the decorator:

```python
@cf.task(requires={"gpu": "A100"})
def train_model(data):
    ...
```

The `requires` parameter accepts a dictionary where keys are feature names and values can be a specific value (`"A100"`), a list of acceptable values (`["A100", "H100"]`), or `True` to require the feature with any value.

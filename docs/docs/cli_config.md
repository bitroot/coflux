# CLI configuration

The CLI is configured through a combination of a configuration file, environment variables, and command-line flags. The priority order is: flags > environment variables > config file > defaults.

## Configuration file

The default configuration file is `coflux.toml` in the current directory. Use `coflux setup` to create one interactively:

```bash
coflux setup
```

A typical configuration file:

```toml
host = "localhost:7777"
workspace = "default"
modules = ["myapp.workflows", "myapp.tasks"]

[worker]
concurrency = 8
adapter = ["python", "-m", "coflux"]

[blobs]
threshold = 100

[[blobs.stores]]
type = "http"
url = "http://localhost:7777/blobs"
```

### Reference

| Key | Default | Description |
|-----|---------|-------------|
| `host` | `localhost:7777` | Server host |
| `token` | _(none)_ | Authentication token |
| `workspace` | `default` | Workspace name |
| `modules` | `[]` | Modules to load |
| `secure` | _(auto)_ | Use TLS (defaults to `true` for non-localhost hosts) |
| `team` | _(none)_ | Team ID for Studio authentication |
| `output` | _(none)_ | Output format (`json` for machine-readable output) |
| `log_level` | `info` | Log level (`debug`, `info`, `warn`, `error`) |

### Worker settings

```toml
[worker]
concurrency = 8
adapter = ["python", "-m", "coflux"]
provides = ["gpu:A100", "region:eu"]
```

| Key | Default | Description |
|-----|---------|-------------|
| `worker.concurrency` | _(CPU count + 4, max 32)_ | Maximum concurrent executions |
| `worker.adapter` | `[]` | Adapter command for executing Python code |
| `worker.provides` | `[]` | Features this worker provides (for pool matching) |

### Blob storage

See [Blobs](./blobs.md) for detailed blob store configuration.

### Log storage

```toml
[logs]
type = "http"
url = "http://localhost:7777/logs"
batch_size = 100
flush_interval = 0.5
```

## Environment variables

All configuration keys can be set via environment variables with the `COFLUX_` prefix:

```bash
export COFLUX_HOST=localhost:7777
export COFLUX_TOKEN=my-token
export COFLUX_WORKSPACE=production
```

Nested keys use underscores: `COFLUX_WORKER_CONCURRENCY`, `COFLUX_BLOBS_THRESHOLD`.

## Global flags

These flags are available on all commands:

| Flag | Description |
|------|-------------|
| `--config`, `-c` | Path to configuration file (default: `coflux.toml`) |
| `--host` | Server host |
| `--token` | Authentication token |
| `--workspace`, `-w` | Workspace name |
| `--team`, `-t` | Team ID |
| `--output`, `-o` | Output format (`json`) |
| `--log-level` | Log level |

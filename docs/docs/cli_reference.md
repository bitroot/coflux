---
sidebar_label: CLI
---

# CLI reference

## Global flags

These flags are available on all commands:

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--host` | | `localhost:7777` | Server host |
| `--project` | | | Project ID |
| `--token` | | | Authentication token |
| `--team` | `-t` | | Team ID (for Studio auth) |
| `--workspace` | `-w` | `default` | Workspace name |
| `--output` | `-o` | | Output format (`json`) |
| `--config` | `-c` | `coflux.toml` | Configuration file path |
| `--log-level` | | `info` | Log level (`debug`, `info`, `warn`, `error`) |

## `coflux submit`

Submit a workflow run.

```bash
coflux submit <module/target> [arguments...]
```

Arguments are passed as JSON strings.

| Flag | Description |
|------|-------------|
| `--no-wait` | Submit and exit without waiting for completion |
| `--idempotency-key` | Deduplication key |
| `--catalog` | Run as of a [catalog](./catalog.md) version (`path@n`): every execution in the run sees the catalog as it was then. `latest` pins the run to the catalog as of submission |
| `--requires` | Override requires tags (can be repeated) |
| `--no-requires` | Clear requires |
| `--memo` / `--no-memo` | Override memoisation |
| `--delay` | Override delay (seconds) |
| `--retries` | Override retry limit (0 = no retries) |

```bash
coflux submit myapp/process '"arg1"' '42'
coflux submit --no-wait myapp/long_job '"data"'
coflux submit --requires gpu:A100 --memo myapp/train '"config"'
```

## `coflux runs`

### `coflux runs inspect <run-id>`

Inspect a run (target, status, timestamps, step/execution counts).

| Flag | Description |
|------|-------------|
| `--no-wait` | Return snapshot without waiting |

### `coflux runs result <target>`

Get the result of a run, step, or execution.

Target format: `<run-id>`, `<run-id>:<step>`, or `<run-id>:<step>:<attempt>`.

### `coflux runs rerun <step-id>`

Re-run a step. Step ID format: `<run-id>:<step>`.

| Flag | Description |
|------|-------------|
| `--no-wait` | Re-run and exit without waiting |
| `--catalog` | The [catalog](./catalog.md) snapshot for the new attempt: a version (`path@n`) or `latest`. By default it sees what the previous attempt saw |

### `coflux runs cancel <execution-id>`

Cancel an execution and all its descendants.

## `coflux logs`

```bash
coflux logs <run-id> [<run-id>:<step>:<attempt>]
```

Fetch logs for a run or specific execution.

| Flag | Short | Description |
|------|-------|-------------|
| `--follow` | `-f` | Stream logs in real-time |
| `--from` | | Only include logs after this timestamp (unix ms) |

## `coflux worker`

```bash
coflux worker [modules...]
```

Start a worker.

| Flag | Description |
|------|-------------|
| `--dev` | Development mode (implies `--watch` and `--register`) |
| `--watch` | Watch for file changes and reload |
| `--register` | Register modules with server |
| `--concurrency` | Max concurrent executions (default: CPU count + 4, max 32) |
| `--provides` | Features worker provides (e.g., `gpu:A100`) |
| `--accepts` | Tags executions must have |
| `--adapter` | Adapter command |
| `--session` | Session ID (for pool-launched workers) |
| `--drain-timeout` | How long to wait for in-flight executions to finish on shutdown or reload (default: `2m`; `0` = wait indefinitely) |

On `SIGINT`/`SIGTERM` the worker stops accepting new executions and drains the in-flight ones (up to `--drain-timeout`). Sending the signal a second time aborts the drain early; a third time forces an immediate exit. The same drain runs between reloads in `--watch` / `--dev` mode.

## `coflux setup`

Interactive configuration wizard. Creates `coflux.toml`.

| Flag | Short | Description |
|------|-------|-------------|
| `--host` | `-H` | Server host |
| `--workspace` | `-w` | Workspace name |
| `--adapter` | | Adapter command |
| `--detect` | | Auto-detect adapter |

## `coflux server`

Start a local server using Docker.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--port` | `-p` | `7777` | Server port |
| `--data-dir` | `-d` | `./data` | Data directory |
| `--image` | | | Docker image |
| `--project` | | | Single-project mode |
| `--public-host` | | | Public host (use `%` prefix for subdomain routing) |
| `--no-auth` | | | Disable authentication |
| `--super-token` | | | Super token |
| `--super-token-hash` | | | Pre-hashed super token (SHA-256 hex) |
| `--secret` | | | Server secret for signing service tokens |
| `--team` | | | Allowed team IDs for Studio auth |
| `--launcher` | | | Allowed launcher types (`docker`, `process`, `kubernetes`) |

## `coflux login` / `coflux logout`

Authenticate with Coflux Studio using a device authorization flow.

| Flag | Description |
|------|-------------|
| `--no-browser` | Don't open browser automatically (`login` only) |

## `coflux workspaces`

| Command | Description |
|---------|-------------|
| `workspaces list` | List workspaces |
| `workspaces create <name>` | Create a workspace (`--base` for inheritance) |
| `workspaces update` | Update workspace (`--name`, `--base`, `--no-base`) |
| `workspaces pause` | Pause a workspace |
| `workspaces resume` | Resume a paused workspace |
| `workspaces archive` | Archive a workspace |

## `coflux manifests`

| Command | Description |
|---------|-------------|
| `manifests discover <modules...>` | Discover targets without registering |
| `manifests register <modules...>` | Register targets with the server |
| `manifests archive <module>` | Archive a module |
| `manifests inspect` | List registered modules and targets (`--watch`) |

All manifest commands accept `--adapter` to specify the adapter command.

## `coflux pools`

| Command | Description |
|---------|-------------|
| `pools list` | List pools |
| `pools get <name>` | Get pool configuration |
| `pools create <name>` | Create a pool |
| `pools update <name>` | Update a pool |
| `pools delete <name>` | Delete a pool |
| `pools disable <name>` | Disable a pool (drain workers) |
| `pools enable <name>` | Re-enable a pool |
| `pools launches <pool> [worker-id]` | View launched workers (`--watch`) |
| `pools export` | Export pool configs as TOML (`-o`, `--only`) |
| `pools import [file]` | Import pool configs from TOML or stdin. Declarative: pools absent from the file are deleted. Flags: `--dry-run`, `--yes`, `--only` |

### Pool creation flags

| Flag | Description |
|------|-------------|
| `--type` | Launcher type: `kubernetes`, `docker`, `process` (required) |
| `--set` | Set a field (e.g., `--set image=myapp:latest`, `--set env.KEY=VALUE`) |
| `--modules` | Modules to host |
| `--provides` | Features workers provide |
| `--accepts` | Tags executions must have |

### Pool update flags

| Flag | Description |
|------|-------------|
| `--set` | Set a field |
| `--unset` | Unset a field |
| `--modules` | Modules to host |
| `--provides` / `--no-provides` | Set or clear provides |
| `--accepts` / `--no-accepts` | Set or clear accepts |

See [pools](./pools.md) for launcher-specific fields.

## `coflux tokens`

| Command | Description |
|---------|-------------|
| `tokens list` | List service tokens |
| `tokens create` | Create a token (`--name`, `--workspaces`) |
| `tokens revoke <id>` | Revoke a token |

## `coflux inputs`

| Command | Description |
|---------|-------------|
| `inputs list` | List pending inputs (`--run` to filter by run ID) |
| `inputs inspect <input-id>` | Show a prompt's template, schema, and current state |
| `inputs respond <input-id> <value>` | Submit a response (value as JSON) |
| `inputs dismiss <input-id>` | Dismiss a prompt |

See [inputs](./inputs.md) for the workflow side of the API.

## `coflux assets`

| Command | Description |
|---------|-------------|
| `assets inspect <id>` | List asset entries (`--match` to filter) |
| `assets download <id>` | Download asset files (`--to`, `--match`, `--force`) |

## `coflux catalog`

| Command | Description |
|---------|-------------|
| `catalog list [prefix]` | Paths and their latest versions, optionally under a prefix |
| `catalog inspect <path>` | The versions at a path, newest first (`--limit`) |
| `catalog get <path>[@<number>]` | Print the value at a path, or of a specific version |
| `catalog publish <path> <json>` | Publish a JSON value at a path |
| `catalog publish <path> --asset <asset-id>` | Publish an existing asset at a path |
| `catalog download <path>[@<number>]` | Download the assets held by the value at a path; several restore into subdirectories by key (`--to`, `--match`, `--force`) |

## `coflux blobs`

### `coflux blobs get <key>`

Retrieve a blob by key. Use `-o` to write to a file (default: stdout).

## `coflux sessions`

### `coflux sessions list`

List active sessions (`--watch` to watch for changes).

## `coflux queue`

Show the execution queue. The dependencies column lists what each queued or suspended execution is waiting on: other executions, inputs, or stream items. Use `--no-watch` for a snapshot, and `-o json` to dump the raw topic.

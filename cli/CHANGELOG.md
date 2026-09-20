## 0.13.0

Enhancements:

- Adds `--type ecs` support for `pools create` and `pools update`.
- Adds the `idleTimeout` pool field, for how long a pool keeps an idle worker before stopping it.
- Adds `secrets set`, `secrets list` and `secrets delete`. Pools refer to secrets by name (`tokenSecret`, `credentialsSecret`, `envSecrets`) instead of holding credentials, so `pools export` no longer needs `--include-secrets`. Each secret is set for one or more workspace patterns, given as a required `--workspaces`, in the same language `tokens create --workspaces` uses.

## 0.12.0

Enhancements:

- Supports streams: runs producer generators against server-granted demand (with idle timers), and routes stream items to consumers.
- Supports checkpoints: throttles and orders checkpoint writes, flushing them before an execution suspends or exits.
- Supports partial blob reads (byte ranges) from HTTP and S3 stores.
- `submit` fills in omitted trailing arguments from the workflow's declared defaults, so the cache, memo and defer keys match a call that spells them out.
- `queue` shows the inputs and stream items that queued executions are waiting on.
- In `--dev`/`--watch` mode, a module that fails to import on reload is logged and the worker keeps running (serving whatever loaded) rather than exiting. The first run is still strict.

## 0.11.0

Enhancements:

- Adds `--drain-timeout` flag to `worker` (default: 2 minutes) for gracefully draining in-flight executions on shutdown or reload. A second signal aborts the drain early; a third forces exit.
- Adds `inputs list`, `inputs inspect`, `inputs respond` and `inputs dismiss` commands for managing input requests from the CLI.

## 0.10.0

Enhancements:

- Updates `server` command to set default project ("default"), and improve Docker lifecycle handling.
- Updates `worker` command to infer adapter (to avoid running `setup`).
- Adds `--type kubernetes` support for `pools create` and `pools update`.
- Adds `pools disable` and `pools enable` commands.
- Adds `pools export` and `pools import` commands.
- Adds `--accepts` flag for pool commands.
- Adds `--requires`, `--memo`/`--no-memo`, `--delay`, and `--retries` flags to `submit`.

## 0.9.0

First release of the Go CLI, replacing the previous Python-based CLI.

Enhancements:

- Rewritten in Go for faster startup and standalone distribution (no Python dependency).
- Adds `submit`, `runs inspect`, `runs result`, `runs rerun`, `runs cancel` and `logs` commands.
- Adds `manifests inspect`, `manifests discover` and `manifests register` commands.
- Adds `assets inspect`, `assets download` and `blobs get` commands.
- Adds `workspaces list`, `workspaces create`, `workspaces pause` and `workspaces resume` commands.
- Adds `tokens list`, `tokens create` and `tokens revoke` commands.
- Supports real-time log streaming with `logs --follow`.
- Supports JSON output (`--output json`) across all commands.
- Workers automatically create workspaces on first connection.
- Workers gracefully handle server restarts and reconnect automatically.
- Supports authenticated connections to the server (token-based and Studio auth).
- Validates API version compatibility with the server.
- Validates protocol version compatibility with language adapters.

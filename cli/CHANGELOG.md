## 0.9.2

No changes.

## 0.9.1

Enhancements:

- Updates `server` command to set default project ("default"), and improve Docker lifecycle handling.
- Updates `worker` command to infer adapter (to avoid running `setup`).

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

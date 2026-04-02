# Server configuration

The Coflux server is distributed as a Docker image and can be started using the CLI or directly with Docker.

## Starting the server

```bash
coflux server
```

This is a convenience wrapper around `docker run`. Docker must be installed and running.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--project` | `default` | Restrict the server to a single project |
| `--public-host` | _(none)_ | Public-facing host (use `%` prefix for subdomain routing) |
| `--port` | `7777` | Port to run the server on |
| `--data-dir` | `./data` | Directory for persistent data |
| `--no-auth` | `false` | Disable authentication |
| `--super-token` or `--super-token-hash` | _(none)_ | Set a super token (plain text or pre-hashed SHA-256 hex) |
| `--secret` | _(none)_ | Server secret for signing service tokens |
| `--team` | _(none)_ | Team IDs allowed for Studio auth (repeatable) |
| `--launcher` | _(none)_ | Allowed launcher types (repeatable, e.g. `docker`, `process`) |
| `--image` | _(auto)_ | Docker image to use |

## Projects

The server can operate in two modes:

### Single-project mode

By default, the server runs in single-project mode with a project called "default". Use `--project` to choose a different name:

```bash
coflux server --project myproject
```

All requests are routed to this project regardless of the hostname used to connect.

### Multi-project mode

Configure `COFLUX_PUBLIC_HOST` with a `%` placeholder to enable subdomain-based routing, where the project is extracted from the subdomain:

```bash
docker run \
  -e COFLUX_PUBLIC_HOST=%.localhost:7777 \
  ghcr.io/bitroot/coflux
```

Workers and Studio connect using the project as a subdomain (e.g., `myproject.localhost:7777`).

## Environment variables

The server is configured via environment variables. When using `coflux server`, CLI flags are mapped to these variables automatically.

| Variable | Default | Description |
|----------|---------|-------------|
| `COFLUX_PROJECT` | _(none)_ | Restrict to a single project (the CLI defaults to `default`) |
| `COFLUX_PUBLIC_HOST` | `localhost:PORT` | Public host (use `%` prefix for subdomain routing) |
| `COFLUX_REQUIRE_AUTH` | `true` | Whether authentication is required |
| `COFLUX_SUPER_TOKEN_HASH` | _(none)_ | SHA-256 hex hash of the super token |
| `COFLUX_SECRET` | _(none)_ | Server secret for signing service tokens |
| `COFLUX_STUDIO_TEAMS` | _(none)_ | Comma-separated team IDs for Studio auth |
| `COFLUX_STUDIO_URL` | `https://studio.coflux.com` | Studio URL |
| `COFLUX_DATA_DIR` | `./data` | Data directory path |
| `COFLUX_ALLOW_ORIGINS` | `https://studio.coflux.com` | Comma-separated CORS origins |
| `COFLUX_LAUNCHER_TYPES` | _(none)_ | Allowed launcher types (e.g., `docker,process`) |
| `COFLUX_CLI_PATH` | `coflux` | CLI binary path for process launcher |

## Data storage

The server stores data in the configured data directory. Each project gets its own SQLite database. Data is managed in rotating epochs, which allows the server to manage data growth without losing access to historical runs.

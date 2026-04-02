# 2. Starting the server

Use the CLI to start the server locally:

```bash
coflux server --no-auth
```

The `--no-auth` flag disables authentication, which simplifies getting started. See the [authentication documentation](/authentication) for options for setting up authentication for production use.

By default, the server uses a project called "default". Use `--project` to choose a different name — a _project_ is a top-level unit of isolation with its own data, orchestration process, and set of workspaces. See [server configuration](/server_config) for more details.

:::note
The command is a wrapper around `docker run`, so you'll need to have Docker installed and running.

Alternatively you can start the server with Docker directly:

```bash
docker run \
  --pull always \
  -p 7777:7777 \
  -v $(pwd):/data \
  -e COFLUX_REQUIRE_AUTH=false \
  ghcr.io/bitroot/coflux
```
:::

The server is now running on `localhost:7777`.

Next, we can define a workflow...

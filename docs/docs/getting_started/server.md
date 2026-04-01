# 2. Starting the server

Use the CLI to start the server locally:

```bash
coflux server --no-auth --project myproject
```

The `--project` flag configures the server for single-project mode. A _project_ is a top-level unit of isolation — it has its own data, orchestration process, and set of workspaces. The `--no-auth` flag disables authentication, which simplifies getting started. See the [authentication documentation](/authentication) for options for setting up authentication for production use.

:::note
The command is a wrapper around `docker run`, so you'll need to have Docker installed and running.

Alternatively you can start the server with Docker directly:

```bash
docker run \
  --pull always \
  -p 7777:7777 \
  -v $(pwd):/data \
  -e COFLUX_PROJECT=myproject \
  -e COFLUX_REQUIRE_AUTH=false \
  ghcr.io/bitroot/coflux
```
:::

The server is now running on `localhost:7777`.

Next, we can define a workflow...

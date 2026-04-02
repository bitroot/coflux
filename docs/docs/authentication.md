# Authentication

By default, the Coflux server requires authentication. There are several authentication methods available, which can be used independently or combined.

## Disabling authentication

For local development, authentication can be disabled by starting the server with the `--no-auth` flag:

```bash
coflux server --no-auth
```

This allows anonymous access to all endpoints. This is not recommended for production use.

## Super token

A super token provides full access to the server. It is configured when starting the server:

```bash
coflux server --super-token "my-secret-token"
```

The token can then be used with the CLI:

```bash
coflux submit --token "my-secret-token" mymodule/my_workflow
```

Or in `coflux.toml`:

```toml
token = "my-secret-token"
```

If configuring the server via environment variables or a configuration file, you can provide the SHA-256 hash of the token instead of the plaintext token using `COFLUX_SUPER_TOKEN_HASH`. This is recommended for production environments where the token shouldn't appear in configuration files.

## Service tokens

Service tokens can be created and scoped to specific workspaces, making them suitable for CI/CD pipelines, production workers, or as an alternative to Studio authentication for individual users:

```bash
coflux tokens create --name "CI" --workspaces "production/*"
```

Service tokens require `COFLUX_SECRET` to be configured on the server. This secret is used to derive per-project signing keys.

To manage tokens:

```bash
coflux tokens list
coflux tokens revoke <token-id>
```

## Studio authentication

Studio authentication uses a device authorization flow. Run `coflux login`, approve in the browser, and the CLI manages token refresh automatically:

```bash
coflux login
```

This requires `COFLUX_STUDIO_TEAMS` to be configured on the server with the allowed team IDs:

```bash
coflux server --team team-id-1 --team team-id-2
```

To log out:

```bash
coflux logout
```

## Worker authentication

Workers authenticate using the same token mechanisms. When a token is configured (via `coflux.toml`, the `--token` flag, or Studio login), it is used for all connections to the server, including WebSocket connections and blob/log uploads.

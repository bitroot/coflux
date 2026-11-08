# Lambda pool

Runs Coflux workers as AWS Lambda invocations. Each invocation runs one
`coflux worker`, which by default takes one execution and leaves.

- `handler.py` - the function's handler: runs `coflux worker` with the
  arguments and environment the invocation carries, and returns when it
  exits. Any function that does the same will do.
- `Dockerfile` - an image with the handler, the CLI, the adapter and
  `lambda_demo.py`.
- `deploy.toml` - how to build the image, the function's settings, and the
  pool, for `scripts/deploy-pool.py`.

## Deploying

`deploy.toml` describes the image and the pool, and
`scripts/deploy-pool.py` builds the image, pushes it to ECR, points the
function at it (creating the function, and an execution role for it, the
first time), then creates or updates the pool and registers
`lambda_demo`:

```bash
# From this directory, with AWS credentials for the account in your
# environment, and coflux configured for the server and workspace
../../scripts/deploy-pool.py
../../scripts/deploy-pool.py --dry-run   # show what it would do
```

`--set` overrides launcher fields for this deploy, as `coflux pools
update --set` does - for a server host that changes between sessions,
say. The pool is imported as the file (plus any `--set`) describes it, so
an override left off a later deploy is removed from the pool. Run it
again whenever the code changes.

The server needs `lambda:InvokeFunction` and
`lambda:GetFunctionConfiguration` on the function. The worker connects
out to the server, so the server has to be reachable from the function.

## Trying it against a local server

The function has to reach the server, so expose it with a tunnel. A dev
server restricted to one project (`COFLUX_PROJECT`) answers on any host,
so a quick Cloudflare tunnel will do:

```bash
cloudflared tunnel --url http://localhost:7777
# ... prints https://<something>.trycloudflare.com
```

The server calls Lambda with its own credentials. It doesn't read AWS
profiles, so export the profile's credentials into a secret (these are
temporary - repeat this when they expire):

```bash
aws configure export-credentials | coflux secrets set aws-sandbox --workspaces development/joe
```

Then deploy, pointing the workers at the tunnel:

```bash
../../scripts/deploy-pool.py \
  --set serverHost=<something>.trycloudflare.com --set serverSecure=true \
  --set credentialsSecret=aws-sandbox

coflux submit lambda_demo/fan_out 10
coflux pools launches lambda --watch
```

`nap` shows what happens to work that would outlast the function: with a
45 second timeout, `coflux submit lambda_demo/nap 300` times out after 35
seconds, and the worker leaves cleanly, rather than being lost when Lambda
ends the invocation.

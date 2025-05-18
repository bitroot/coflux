# Coflux → Server

Coflux is an open-source workflow engine. Use it to orchestrate and observe computational workflows, defined in plain Python. Suitable for data pipelines, background tasks, chat bots.

This is the server component, which includes the orchestrator and the frontend. It gets packaged up into a Docker image.

Refer to the [docs](https://docs.coflux.com) for more details.

## Development

Use Nix (and direnv) to set up the environment. Then install dependencies:

```bash
$ mix do deps.get, deps.compile
```

Then start the server (with IEx):

```bash
$ iex -S mix
```

And build the frontend with:

``` bash
$ cd ../frontend
$ npm run dev
```

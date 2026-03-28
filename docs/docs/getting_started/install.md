# 1. Installing the CLI

The quickest way to install the Coflux CLI on macOS or Linux:

```bash
curl -fsSL https://coflux.com/install.sh | sh
```

This detects your OS and architecture, downloads the latest release, and installs the binary to `/usr/local/bin`.

:::tip
To install a specific version, set the `VERSION` environment variable:

```bash
VERSION=0.9.0 curl -fsSL https://coflux.com/install.sh | sh
```
:::

Alternatively, you can download the binary directly from the [GitHub releases](https://github.com/bitroot/coflux/releases) page.

Once installed, verify it's working:

```bash
coflux --help
```

Next, we'll start the server...

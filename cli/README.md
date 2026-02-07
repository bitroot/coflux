# Coflux CLI

Go-based CLI for Coflux workflow orchestration with multi-language support.

## Building

```bash
cd cli
go build -o coflux ./cmd/coflux
```

## Usage

### Setup

Configure a project interactively:

```bash
coflux setup
```

This creates a `coflux.toml` with server host, workspace, adapter, and modules.

### Worker

Start a worker that connects to the Coflux server:

```bash
# Uses modules from coflux.toml
coflux worker

# Override modules
coflux worker myapp.workflows myapp.tasks

# Development mode (watch for changes + auto-register)
coflux worker --dev
```

### Register

Register module manifests with the server (without starting a worker):

```bash
coflux register
coflux register myapp.workflows myapp.tasks
coflux register --dry-run
```

### Submit

Submit a workflow to be run:

```bash
coflux submit mymodule.my_workflow '["arg1", "arg2"]'
```

### Server

Start a local Coflux server using Docker:

```bash
coflux server
coflux server --port 8080
coflux server --data-dir ./my-data
```

### Authentication

```bash
coflux login    # Authenticate with Coflux Studio
coflux logout   # Remove stored credentials
```

### Management Commands

```bash
coflux workspaces list
coflux pools list
coflux tokens list
coflux assets inspect <asset-id>
coflux blobs get <key>
```

## Configuration

Create a `coflux.toml` file (or use `coflux setup`):

```toml
workspace = "default"
modules = ["myapp.workflows", "myapp.tasks"]
concurrency = 8

[server]
host = "localhost:7777"
# token = "your-token"  # or use --token flag

[blobs]
threshold = 200  # bytes, values larger stored as blobs

[[blobs.stores]]
type = "http"
url = "http://localhost:7777/blobs"
```

## Architecture

The CLI uses a plugin-style architecture:

```
┌─────────────────────────────────────────────────────────────────────┐
│   CLI (Go)                                                          │
│   ┌───────────────┬──────────────┬─────────────────┬──────────────┐│
│   │ Server API    │ Studio Auth  │ Executor Pool   │ Blob/Log     ││
│   │ Client        │ (device flow)│ Manager         │ Stores       ││
│   └───────────────┴──────────────┴────────┬────────┴──────────────┘│
└───────────────────────────────────────────┼─────────────────────────┘
                                            │ stdio (JSON Lines)
                    ┌───────────────────────┼───────────────────────┐
                    ▼                       ▼                       ▼
             ┌────────────┐         ┌────────────┐          ┌────────────┐
             │ Discovery  │         │ Executor 1 │          │ Executor N │
             │ Adapter    │         │ (Python)   │          │ (Python)   │
             └────────────┘         └────────────┘          └────────────┘
```

## Package Structure

- `cmd/coflux/` - CLI entry point and commands
- `internal/config/` - Configuration parsing
- `internal/auth/` - Studio authentication (device flow)
- `internal/api/` - Server HTTP/WebSocket client
- `internal/worker/` - Worker session management
- `internal/pool/` - Executor pool management
- `internal/adapter/` - Language adapter protocol
- `internal/blob/` - Blob store implementations
- `internal/log/` - Log store implementations

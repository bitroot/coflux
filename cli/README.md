# Coflux CLI

Go-based CLI for Coflux workflow orchestration with multi-language support.

## Building

```bash
cd cli
go build -o coflux ./cmd/coflux
```

## Usage

### Discovery

Discover workflow and task targets in Python modules:

```bash
# Basic discovery
coflux discover myapp.workflows myapp.tasks

# With explicit Python interpreter
coflux discover --python /path/to/python --module myapp.workflows
```

### Worker

Start a worker that connects to the Coflux server:

```bash
# Using command-line options
coflux worker --project myproject --workspace dev myapp.workflows

# Using coflux.toml configuration
coflux worker myapp.workflows myapp.tasks
```

### Authentication

Login to Coflux Studio:

```bash
coflux login
```

Logout:

```bash
coflux logout
```

## Configuration

Create a `coflux.toml` file:

```toml
project = "myproject"
workspace = "default"
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

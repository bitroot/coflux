## 0.10.1

No changes.

## 0.10.0

Enhancements:

- Adds support for writing metrics, and writing progress.
- Adds `timeout` parameter to `@task` and `@workflow` decorators.
- Adds `ExecutionCancelled` and `ExecutionTimeout` exceptions.
- Adds `.poll()` method to `Execution` for checking execution results without blocking (or suspending).
- Adds `memo` parameter to `@task`, `@workflow`, and `@stub` decorators.
- Adds `requires` parameter to `@task` and `@workflow` decorators.

## 0.9.0

Enhancements:

- Communicates with the new Go CLI over JSON Lines (replacing the previous all-in-one Python package).
- Adds support for conditional retries (`@task(retries=Retries(3, when=TransientError))`).
- Supports serialisation of additional types (datetime, UUID, Decimal, bytes, frozenset, etc.).
- Spawns a fresh process for each execution, improving isolation and memory management.
- Improved error reconstruction for remote exceptions.
- Reports protocol version during worker handshake for compatibility validation.

Changes:

- The `coflux` package is now a pure Python SDK — the CLI is a separate Go binary.
- Replaces 'sensors' and 'checkpoints' with recurrent targets.
- Renames 'spaces' back to 'workspaces'.

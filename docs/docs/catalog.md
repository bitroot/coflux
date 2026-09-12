# Catalog

The catalog holds versioned values under paths. A task publishes a value at a path, other tasks read the current version at that path, and an execution can be re-run when a newer version lands.

A value is anything that can be passed to a task: an [asset](./assets.md), a data structure holding assets, a reference to data in an external system, a plain number.

```python
import coflux as cf
from pathlib import Path


@cf.task()
def build_dataset() -> int:
    Path("customers.parquet").write_bytes(fetch_customers())
    return cf.publish("datasets/customers", cf.asset("customers.parquet"))


@cf.task(memo=True)
def train(dataset: cf.Asset) -> int:
    paths = dataset.restore()
    model = fit(paths["customers.parquet"])
    return cf.publish("models/churn", {"weights": cf.asset(model), "auc": model.auc})
```

A path is a slash-separated name like `datasets/customers`. Each publish appends a *version*: which value, who published it, and when. Versions are never modified or deleted.

## Publishing

`cf.publish(path, value)` publishes a value and returns the new version's number.

```python
number = cf.publish("configs/training", {"threshold": 0.7, "epochs": 20})  # 6
```

Facts about a publish — a metric, what it was built from — go in the value, alongside the thing itself. There is no separate metadata: a dict holding an asset and its scores is one value, rendered as such in Studio, and a reader gets both from `current()`.

Publishing what is already the latest version — the same value — writes nothing and returns the existing version. Values are content-addressed, so this is exact, and it's what makes a publish safe to run again after a [suspension](./suspense.md): a body that publishes and then suspends republishes on resumption without creating a duplicate. A change to any part of the value *is* a new version.

Versions are numbered per path, once, across every workspace: `models/churn@6` names exactly one version wherever you are.

### What a version pins

The catalog pins the value, not what the value points at. A data structure or an asset is immutable by construction. A handle — an execution, an input — resolves to whatever it resolves to when it's read, which can change as the execution is retried or the input answered. A locator for external data (a table name, a URI) is only as stable as the data behind it, so publish a snapshot identifier or a content hash alongside it when lineage needs to be exact:

```python
cf.publish("tables/customers", {"table": "analytics.customers", "snapshot": 4182})
```

A value can also be published from outside a run, with the CLI — a JSON document, or an existing asset:

```bash
coflux catalog publish configs/training '{"threshold": 0.7}'
coflux catalog publish models/churn --asset <asset-id>
```

## Reading

`cf.catalog(path)` is a handle to a path. Nothing round-trips until it's used.

```python
entry = cf.catalog("models/churn")
entry.current()    # the current value
```

**An execution sees the catalog as it was when it started.** Every execution is pinned to the catalog at the moment it's assigned to a worker, and `current()` answers from that snapshot, so repeated reads agree with each other and with reads of other paths. There are two exceptions. Anything the execution's own run has published since is visible: a parent that waits on a child which publishes sees the child's version. And a path that had nothing when the execution started shows its first version once one lands, since the alternative is never answering.

That's why it's *current* rather than *latest*: a version published after the execution started isn't current for it. `next()`, below, is how to wait for one.

`current()` on a path with nothing published yet waits for the first publish, following the [suspense](./suspense.md) rule every read follows: it blocks outside a `cf.suspense` scope, holding the worker slot, and suspends inside one.

Reads are recorded, so Studio shows which versions each execution used alongside its other dependencies, and which it published.

### Snapshots across attempts

A step's later attempts keep the snapshot of the attempt before them. A retry, a re-run from Studio or the CLI, and the execution that resumes a suspended step all see what the previous attempt saw, so re-running reproduces a run rather than quietly picking up whatever has been published since. Two kinds of attempt start afresh instead: the successor of a `next()`, which exists to see the newer version, and each iteration of a [recurring](./recurring.md) target.

The snapshot can also be chosen. A run can be submitted as of a version — the catalog as it was when that version was published, every path included — and a step can be re-run against a version, or against the latest:

```bash
coflux submit --catalog models/churn@4 myapp/evaluate
coflux runs rerun --catalog models/churn@4 R1a2b3:1
coflux runs rerun --catalog latest R1a2b3:1
```

A run submitted as of a version gives every execution in it that snapshot, including steps it schedules. A version has to exist and be visible from the workspace the run is in.

## Re-running on a new version

`current()` is a read: what is the value, as of my snapshot? `next()` is a request to re-run: run me again when this path has something I haven't seen. It suspends the execution — always, whether or not it's inside a `cf.suspense` scope — until the path has a version newer than the snapshot, and it returns nothing. The execution that resumes the step runs from the top with a fresh snapshot (the one case a resumed step doesn't keep the snapshot it had), so its `current()` returns the new value:

```python
@cf.workflow()
def retrain_on_publish():
    datasets = cf.catalog("datasets/customers")
    train(datasets.current())   # memoised, so a re-run with the same data is a hit
    datasets.next()             # suspends until a newer version lands
```

Nothing is carried between attempts. The only thing the server holds for the suspended execution is what it's waiting for.

As with any suspension, the code before the wait runs again on resumption, so what it calls should be [memoised](./memoizing.md). Here that is what you want anyway: when a new dataset lands, `train` sees a new argument, misses the memo, and trains on the new data.

`next()` needs no `cf.suspense` scope, and the body above has none, so waiting for `train` holds the worker slot and the step runs once per version. Put the reads in a scope if the path might be empty and you'd rather suspend than hold the slot until the first publish. Put the whole body in one and a slow `train` suspends too, after the scope's timeout, and the resumed attempt runs the body again: harmless, since `train` is memoised, but a second attempt per version.

Because the wait is relative to what the execution can see, an execution's own publish never wakes it. If a newer version already exists when `next()` is called, the execution suspends and resumes straight away.

A `CatalogEntry` is also a handle for [`cf.select`](./select.md). It resolves when the path has a version this execution hasn't seen — on an empty path, the first — so it is `next()` in select form, and the thing to do when an entry wins is call `next()` on it. What makes the combination useful is the other handles:

```python
with cf.suspense():
    datasets = cf.catalog("datasets/customers")
    configs = cf.catalog("configs/training")
    retrain_now = cf.Prompt("Retrain now?").submit()
    ...
    winner, _ = cf.select([datasets, configs, retrain_now])   # whichever comes first
    if isinstance(winner, cf.CatalogEntry):
        winner.next()   # re-run on the new data or config
```

## Workspaces

A version is published into the publishing execution's [workspace](./concepts.md). A read from a workspace sees every version published in it or any of its bases, and the current version is the newest of those, whichever workspace it came from — the same way cached results are inherited.

So a `dev` workspace derived from `prod` reads `prod`'s versions, and keeps tracking `prod`'s publishes after publishing its own: a `dev` publish is current in `dev` until `prod` publishes something newer. `prod` never sees `dev`'s versions. Numbers are allocated across both, so `prod` may see `@5` followed by `@7`, with `@6` belonging to `dev`.

To keep a derived workspace off its base's publishes, use a different path prefix or a workspace with no base.

## Passing values between tasks

To make two steps agree on the same data, read it once and pass the value. A value is immutable, so the receiving task sees exactly what the reader saw, and a memoised task hits on identical content whichever version it came from. The read is recorded against the execution that made it, and Studio shows which version that was.


## Studio and the CLI

Studio's Catalog page lists every path visible from the workspace with its latest version and its value, and the versions behind each path. A run shows what each execution published and read.

```bash
coflux catalog list [prefix]                        # paths and their latest versions
coflux catalog inspect <path>                       # the versions at a path
coflux catalog get <path>[@<number>]                # print a version's value
coflux catalog publish <path> <json>                # publish a JSON value
coflux catalog publish <path> --asset <asset-id>    # publish an existing asset
coflux catalog download <path>[@<number>] --to ./dir
coflux submit --catalog <path>@<number> ...         # run as of a version
coflux runs rerun --catalog <path>@<number>|latest <step-id>
```

`download` restores the assets a value holds. A value that is a single asset restores flat into the directory. One holding several assets restores each into a subdirectory named by the keys (or indices) leading to it, so `{"train": a, "test": b}` restores into `train/` and `test/`. One holding no assets is an error; `get` shows it instead.

# Assets

An asset is a collection of files, which can be shared between tasks and inspected in the UI. Individual files are uploaded to the configured blob store. The listing and metadata are uploaded to the Coflux server.

## Creating assets

An asset is created by calling `cf.asset()`. This will return an `Asset` object, which can be passed to other tasks (or returned), and then used to restore some or all of the files.

Each execution is started in a temporary directory. By default, `cf.asset()` will collect all files in the directory.

```python
import coflux as cf
from pathlib import Path

@cf.task()
def my_task() -> cf.Asset:
    Path.cwd().joinpath("foo.txt").write_text("hello")
    return cf.asset()
```

There are several options for being more specific about which files to include:

- Persist a single file by specifying the path: `cf.asset("my_file.txt")` or `cf.asset(Path("my_file.txt"))`.
- Or a list of files: `cf.asset(["first.txt", "second.txt"])`.
- Specify an alternative base directory: `cf.asset(at=Path("images"))`.
- Filter files using a glob-style matcher: `cf.asset(match="**/*.{jpg,png}")`.
- Specify entries explicitly: `cf.asset({"img/1.jpg": "image.jpg", "css/default.css": "styles.css"})`.

Assets can also be created from other assets without downloading and re-uploading the files:

- Filtering an existing asset: `cf.asset(asset, match="*.md")`.
- Combining assets: `cf.asset({"1": asset_1, "2": asset_2})`.

## Restoring assets

An asset persisted by one task can be 'restored' by another, using the `.restore()` method. This returns a dictionary of path (in the asset) to the `Path` where the file has been restored to (`dict[str, Path]`).

```python
@cf.workflow()
def my_workflow():
    paths = my_task().restore()
    print(paths["foo.txt"].read_text())
```

An asset can be partially restored by specifying a glob-style matcher:

```python
asset.restore(match="images/*.{jpg,png}")
```

Or individual entries can be restored:

```python
path = asset["path/to/file.txt"].restore()
path.read_text()
```

By default an asset is restored to the task's temporary directory, at the same relative path that it was persisted from. To change this, the `at` argument can be specified (as a `pathlib.Path`, or string):

```python
asset.restore(to="other/dir")
```

# Blobs

Blob stores are used to store non-trivial amounts of data - this includes execution results, arguments passed to other executions, and asset data.

Separating the storage of data means that the Coflux server doesn't need to have access to your data, which can be beneficial both in terms of scalability and in terms of privacy.

By default Coflux will use a blob store embedded in the Coflux server, which saves blobs to the filesystem.

This can be configured explicitly in the CLI configuration file (which is used by workers; the configuration file can be initialised with the CLI using `coflux configure`):

```toml
[[blobs.stores]]
type = "http"
host = "localhost:7777"
protocol = "http"
```

## Blob threshold

To determine when to store data in the blob store, a blob 'threshold' is used. If the serialised data takes more than this number of bytes, the blob store will be used, and a reference to the blob is substituted - otherwise the raw data is sent to the Coflux server. The default threshold is 200 bytes. This can be specified in the configuration file:

```toml
[blobs]
threshold = 100
```

To have all values stored as blobs, set the threshold to zero.

## S3 blob store

As an alternative to the built-in blob store, AWS S3 can be used. To enable this, update the configuration file:

```toml
[[blobs.stores]]
type = "s3"
bucket = "my-bucket"
prefix = "blobs"  # optional
region = "eu-west-1"  # optional
```

Ensure that AWS credentials are available to the worker - for example by setting `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.

## Multiple stores

Multiple blob stores can be configured - the first will be considered the 'primary' store, and will be the one that blobs are written to. Subsequent stores will be tried in turn when a blob can't be found in preceding stores.

This is useful when adding a new store, as the original store can still be read from. Blobs can be manually migrated to the new store, and then the original store can be removed from configuration.

## UI

This configuration is only used by the CLI (e.g., for running workers). To support loading blobs in the UI, stores can be configured from the project settings dialog. Settings entered in the UI (including access keys) are stored in the browser in local storage. When blobs are loaded in the UI, they're cached in the browser in session storage.

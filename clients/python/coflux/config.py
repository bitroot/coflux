import os
import typing as t

import pydantic


class ServerConfig(pydantic.BaseModel):
    host: str = "localhost:7777"
    token: str | None = None
    secure: bool | None = None


class HTTPBlobStoreConfig(pydantic.BaseModel):
    type: t.Literal["http"] = "http"
    host: str | None = None
    secure: bool | None = None


class S3BlobStoreConfig(pydantic.BaseModel):
    type: t.Literal["s3"] = "s3"
    bucket: str
    prefix: str | None = None
    region: str | None = None


BlobStoreConfig = t.Annotated[
    HTTPBlobStoreConfig | S3BlobStoreConfig,
    pydantic.Field(discriminator="type"),
]


def _default_blob_stores() -> list[BlobStoreConfig]:
    return [HTTPBlobStoreConfig()]


class BlobsConfig(pydantic.BaseModel):
    threshold: int = 200
    stores: list[BlobStoreConfig] = pydantic.Field(default_factory=_default_blob_stores)


class HTTPLogStoreConfig(pydantic.BaseModel):
    """HTTP-based log store that POSTs logs to the Coflux server."""

    type: t.Literal["http"] = "http"
    host: str | None = None
    secure: bool | None = None
    batch_size: int = 100
    flush_interval: float = 0.5


LogStoreConfig = t.Annotated[
    HTTPLogStoreConfig,
    pydantic.Field(discriminator="type"),
]


def _default_log_store():
    return HTTPLogStoreConfig()


class LogsConfig(pydantic.BaseModel):
    """Configuration for log storage."""

    store: LogStoreConfig = pydantic.Field(default_factory=_default_log_store)


class PandasSerialiserConfig(pydantic.BaseModel):
    type: t.Literal["pandas"] = "pandas"


class PydanticSerialiserConfig(pydantic.BaseModel):
    type: t.Literal["pydantic"] = "pydantic"


class PickleSerialiserConfig(pydantic.BaseModel):
    type: t.Literal["pickle"] = "pickle"


SerialiserConfig = t.Annotated[
    PandasSerialiserConfig | PydanticSerialiserConfig | PickleSerialiserConfig,
    pydantic.Field(discriminator="type"),
]


def _default_concurrency():
    return min(32, (os.cpu_count() or 4) + 4)


def _default_serialisers():
    return [
        PandasSerialiserConfig(),
        PydanticSerialiserConfig(),
        PickleSerialiserConfig(),
    ]


class Config(pydantic.BaseModel):
    project: str | None = None
    workspace: str | None = None
    concurrency: int = pydantic.Field(default_factory=_default_concurrency)
    server: ServerConfig = pydantic.Field(default_factory=ServerConfig)
    provides: dict[str, list[str] | str | bool] | None = None
    serialisers: list[SerialiserConfig] = pydantic.Field(
        default_factory=_default_serialisers
    )
    blobs: BlobsConfig = pydantic.Field(default_factory=BlobsConfig)
    logs: LogsConfig = pydantic.Field(default_factory=LogsConfig)


class DockerLauncherConfig(pydantic.BaseModel):
    type: t.Literal["docker"] = "docker"
    image: str


LauncherConfig = t.Annotated[DockerLauncherConfig, pydantic.Field(discriminator="type")]

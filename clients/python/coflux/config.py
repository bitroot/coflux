import os
import typing as t

import pydantic


class ServerConfig(pydantic.BaseModel):
    host: str = "localhost:7777"
    token: str | None = None


class HTTPBlobStoreConfig(pydantic.BaseModel):
    type: t.Literal["http"] = "http"
    protocol: t.Literal["http", "https"] = "http"
    host: str | None = None


class S3BlobStoreConfig(pydantic.BaseModel):
    type: t.Literal["s3"] = "s3"
    bucket: str
    prefix: str | None = None
    region: str | None = None


BlobStoreConfig = t.Annotated[
    HTTPBlobStoreConfig | S3BlobStoreConfig,
    pydantic.Field(discriminator="type"),
]


def _default_blob_stores():
    return [HTTPBlobStoreConfig()]


class BlobsConfig(pydantic.BaseModel):
    threshold: int = 200
    stores: list[BlobStoreConfig] = pydantic.Field(default_factory=_default_blob_stores)


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
    space: str | None = None
    concurrency: int = pydantic.Field(default_factory=_default_concurrency)
    server: ServerConfig = pydantic.Field(default_factory=ServerConfig)
    provides: dict[str, list[str] | str | bool] | None = None
    serialisers: list[SerialiserConfig] = pydantic.Field(
        default_factory=_default_serialisers
    )
    blobs: BlobsConfig = pydantic.Field(default_factory=BlobsConfig)


class DockerLauncherConfig(pydantic.BaseModel):
    type: t.Literal["docker"] = "docker"
    image: str


LauncherConfig = t.Annotated[DockerLauncherConfig, pydantic.Field(discriminator="type")]

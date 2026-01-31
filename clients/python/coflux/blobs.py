import httpx
import hashlib
import typing as t
import io
import abc
from pathlib import Path

from . import config

try:
    import boto3
    import botocore
except ImportError:
    boto3 = None
    botocore = None


def _hash_file(buffer: t.BinaryIO):
    buffer.seek(0)
    hash = hashlib.sha256()
    for chunk in iter(lambda: buffer.read(4096), b""):
        hash.update(chunk)
    return hash.hexdigest()


class Store(abc.ABC):
    @abc.abstractmethod
    def __enter__(self):
        return self

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    @abc.abstractmethod
    def get(self, key: str) -> io.BytesIO | None:
        raise NotImplementedError

    @abc.abstractmethod
    def put(self, buffer: t.BinaryIO) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def download(self, key: str, path: Path) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def upload(self, path: Path) -> str:
        raise NotImplementedError


class HttpStore(Store):
    def __init__(self, host: str, *, secure: bool = False):
        self._protocol = "https" if secure else "http"
        self._host = host

    def __enter__(self):
        self._client = httpx.Client(timeout=10)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._client.close()

    def _url(self, key: str) -> str:
        return f"{self._protocol}://{self._host}/blobs/{key}"

    def _exists(self, key: str) -> bool:
        return self._client.head(self._url(key)).status_code == 200

    def get(self, key: str) -> io.BytesIO | None:
        response = self._client.get(self._url(key))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return io.BytesIO(response.content)

    def put(self, buffer: t.BinaryIO) -> str:
        assert buffer.seekable()
        key = _hash_file(buffer)
        if not self._exists(key):
            buffer.seek(0)
            self._client.put(self._url(key), content=buffer).raise_for_status()
        return key

    def download(self, key: str, path: Path) -> bool:
        with self._client.stream("GET", self._url(key)) as response:
            if response.status_code == 404:
                return False
            response.raise_for_status()
            with path.open("wb") as file:
                for chunk in response.iter_bytes():
                    file.write(chunk)
                return True

    def upload(self, path: Path) -> str:
        with open(path, "rb") as file:
            return self.put(file)


class S3Store(Store):
    def __init__(self, bucket_name: str, prefix: str | None, region: str | None):
        self._bucket_name = bucket_name
        self._prefix = prefix.strip("/") if prefix else ""
        self._region = region

    def __enter__(self):
        if not boto3:
            raise Exception("Missing 'boto3' dependency")
        self._s3 = boto3.client("s3", region_name=self._region)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def _key(self, key: str) -> str:
        prefix = f"{self._prefix}/" if self._prefix else ""
        return f"{prefix}{key[0:2]}/{key[2:4]}/{key[4:]}"

    def _exists(self, key: str) -> bool:
        assert botocore
        try:
            self._s3.head_object(Bucket=self._bucket_name, Key=self._key(key))
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise

    def get(self, key: str) -> io.BytesIO | None:
        assert botocore
        try:
            response = self._s3.get_object(Bucket=self._bucket_name, Key=self._key(key))
            return io.BytesIO(response["Body"].read())
        except botocore.exceptions.NoSuchKey:
            return None

    def put(self, buffer: t.BinaryIO) -> str:
        assert buffer.seekable()
        key = _hash_file(buffer)
        if not self._exists(key):
            buffer.seek(0)
            self._s3.put_object(
                Bucket=self._bucket_name,
                Key=self._key(key),
                Body=buffer,
            )
        return key

    def download(self, key: str, path: Path) -> bool:
        assert botocore
        try:
            with open(path, "wb") as file:
                self._s3.download_fileobj(self._bucket_name, self._key(key), file)
                return True
        except botocore.exceptions.NoSuchKey:
            return False

    def upload(self, path: Path) -> str:
        with open(path, "rb") as file:
            return self.put(file)


def _create(config_: config.BlobStoreConfig, server_host: str, server_secure: bool):
    if config_.type == "http":
        host = config_.host if config_.host is not None else server_host
        secure = config_.secure if config_.secure is not None else server_secure
        return HttpStore(host, secure=secure)
    elif config_.type == "s3":
        return S3Store(config_.bucket, config_.prefix, config_.region)
    else:
        raise ValueError("unrecognised blob store config")


class Manager:
    def __init__(self, store_configs: list[config.BlobStoreConfig], *, server_host: str, server_secure: bool):
        self._stores = [_create(c, server_host, server_secure) for c in store_configs]

    def __enter__(self):
        # TODO: ?
        for store in self._stores:
            store.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def get(self, key: str) -> io.BytesIO:
        for store in self._stores:
            result = store.get(key)
            if result is not None:
                return result
        raise Exception(f"blob not found ({key})")

    def put(self, buffer: t.BinaryIO) -> str:
        return self._stores[0].put(buffer)

    def download(self, key: str, path: Path) -> None:
        for store in self._stores:
            if store.download(key, path):
                return
        raise Exception(f"blob not found ({key})")

    def upload(self, path: Path) -> str:
        return self._stores[0].upload(path)

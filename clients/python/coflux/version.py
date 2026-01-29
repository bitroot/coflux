from importlib.metadata import PackageNotFoundError, version as pkg_version

import httpx


def get_api_version() -> str | None:
    try:
        v = pkg_version("coflux")
        parts = v.split(".")
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        if major == 0:
            return f"0.{minor}"
        else:
            return str(major)
    except (PackageNotFoundError, ValueError):
        return None


API_VERSION = get_api_version()


class VersionMismatchError(Exception):
    def __init__(self, server: str, expected: str):
        self.server = server
        self.expected = expected
        super().__init__(
            f"Version mismatch: server is {server}, client expected {expected}"
        )


class ServerConnectionError(Exception):
    def __init__(self, host: str, message: str):
        self.host = host
        super().__init__(f"Cannot connect to server at {host}: {message}")


def check_server(host: str, secure: bool) -> dict:
    """
    Check server connectivity and version compatibility.
    Returns the server metadata if successful.
    Raises ServerConnectionError if server is unreachable.
    Raises VersionMismatchError if versions are incompatible.
    """
    scheme = "https" if secure else "http"
    url = f"{scheme}://{host}/.well-known/com.coflux"
    try:
        with httpx.Client(timeout=10) as client:
            response = client.get(url)
            response.raise_for_status()
            data = response.json()
    except httpx.ConnectError:
        raise ServerConnectionError(host, "connection refused")
    except httpx.TimeoutException:
        raise ServerConnectionError(host, "connection timed out")
    except httpx.RequestError as e:
        raise ServerConnectionError(host, str(e))

    if API_VERSION:
        server_api_version = data.get("apiVersion")
        if server_api_version and server_api_version != API_VERSION:
            raise VersionMismatchError(server_api_version, API_VERSION)

    return data

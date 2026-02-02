"""
Authentication utilities for Coflux CLI.

Handles device flow login and token management for Studio authentication.
"""

import json
import time
import typing as t
from pathlib import Path

import httpx


def get_credentials_path() -> Path:
    """Get the path to the credentials file."""
    config_dir = Path.home() / ".config" / "coflux"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / "credentials.json"


def load_credentials() -> dict[str, t.Any] | None:
    """Load credentials from the credentials file."""
    path = get_credentials_path()
    if not path.exists():
        return None
    try:
        with path.open("r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def save_credentials(credentials: dict[str, t.Any]) -> None:
    """Save credentials to the credentials file."""
    path = get_credentials_path()
    with path.open("w") as f:
        json.dump(credentials, f, indent=2)
    # Set restrictive permissions (owner read/write only)
    path.chmod(0o600)


def clear_credentials() -> None:
    """Remove the credentials file and token cache."""
    path = get_credentials_path()
    if path.exists():
        path.unlink()
    clear_token_cache()


def clear_token_cache() -> None:
    """Remove the token cache file."""
    path = Path.home() / ".config" / "coflux" / "token_cache.json"
    if path.exists():
        path.unlink()


def get_studio_token() -> str | None:
    """Get the stored Studio CLI session token."""
    creds = load_credentials()
    if not creds:
        return None
    return creds.get("studio_token")


class DeviceFlowError(Exception):
    """Error during device flow authentication."""

    pass


class DeviceFlowExpired(DeviceFlowError):
    """The device code has expired."""

    pass


class DeviceFlowResult(t.NamedTuple):
    """Result of a successful device flow authentication."""

    access_token: str
    user_email: str | None
    user_id: str | None


def start_device_flow(studio_url: str) -> dict[str, t.Any]:
    """
    Start the device flow by requesting a device code from Studio.

    Returns a dict with:
    - device_code: Long code for polling
    - verification_uri: URL to open (includes code in query string)
    - expires_in: Seconds until expiration
    - interval: Seconds to wait between polls
    """
    url = f"{studio_url}/api/auth/device"

    with httpx.Client() as client:
        response = client.post(url)
        response.raise_for_status()
        return response.json()


def poll_for_token(
    device_code: str,
    studio_url: str,
    interval: int = 5,
    timeout: int = 900,
) -> DeviceFlowResult:
    """
    Poll for the device flow token until the user approves or it expires.

    Args:
        device_code: The device code from start_device_flow
        studio_url: Studio URL
        interval: Seconds to wait between polls
        timeout: Maximum seconds to wait

    Returns:
        DeviceFlowResult with access token and user info

    Raises:
        DeviceFlowExpired: If the device code expires
        DeviceFlowError: For other errors
    """
    url = f"{studio_url}/api/auth/device/token"
    start_time = time.time()

    with httpx.Client() as client:
        while time.time() - start_time < timeout:
            response = client.post(url, json={"device_code": device_code})

            if response.status_code == 200:
                data = response.json()
                return DeviceFlowResult(
                    access_token=data["access_token"],
                    user_email=data.get("user_email"),
                    user_id=data.get("user_id"),
                )

            if response.status_code == 400:
                data = response.json()
                error = data.get("error")

                if error == "authorization_pending":
                    # User hasn't approved yet, keep polling
                    time.sleep(interval)
                    continue
                elif error == "expired_token":
                    raise DeviceFlowExpired("Device code has expired")
                else:
                    raise DeviceFlowError(
                        data.get("error_description", f"Unknown error: {error}")
                    )
            else:
                response.raise_for_status()

    raise DeviceFlowExpired("Timeout waiting for user approval")


def exchange_for_project_token(
    team: str,
    host: str,
    studio_url: str,
    studio_token: str | None = None,
) -> dict[str, t.Any]:
    """
    Exchange a device session token for a project access JWT.

    Args:
        team: Team external ID
        host: Server host
        studio_url: Studio URL
        studio_token: Device session token (uses stored token if not provided)

    Returns:
        Dict with:
        - token: Project access JWT
        - expiresIn: Seconds until expiration
        - projectName: Project name (if available)
        - teamName: Team name (if available)

    Raises:
        ValueError: If no studio token is available
        httpx.HTTPStatusError: On API errors
    """
    token = studio_token or get_studio_token()
    if not token:
        raise ValueError(
            "No Studio session token available. Run 'coflux login' first."
        )

    url = f"{studio_url}/api/auth/project-token"

    with httpx.Client() as client:
        response = client.post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json={"team": team, "host": host},
        )

        if response.status_code == 401:
            raise ValueError(
                "Studio session token is invalid or expired. Run 'coflux login' again."
            )

        response.raise_for_status()
        return response.json()


def _get_token_cache_path() -> Path:
    """Get the path to the token cache file."""
    config_dir = Path.home() / ".config" / "coflux"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / "token_cache.json"


def _load_token_cache() -> dict[str, t.Any]:
    """Load the token cache from disk."""
    path = _get_token_cache_path()
    if not path.exists():
        return {}
    try:
        with path.open("r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_token_cache(cache: dict[str, t.Any]) -> None:
    """Save the token cache to disk."""
    path = _get_token_cache_path()
    with path.open("w") as f:
        json.dump(cache, f)
    path.chmod(0o600)


def get_project_token(
    team: str,
    host: str,
    studio_url: str,
    refresh_buffer: int = 60,
) -> str:
    """
    Get a project access token, using cache if available.

    Args:
        team: Team external ID
        host: Server host
        studio_url: Studio URL
        refresh_buffer: Seconds before expiry to consider token stale

    Returns:
        Project access JWT

    Raises:
        ValueError: If no studio token is available
        httpx.HTTPStatusError: On API errors
    """
    cache = _load_token_cache()
    key = f"{team}:{host}"

    # Check cache for valid token
    if key in cache:
        entry = cache[key]
        expires_at = entry.get("expires_at", 0)
        if time.time() < expires_at - refresh_buffer:
            return entry["token"]

    # Exchange for new token
    result = exchange_for_project_token(team, host, studio_url)

    # Cache the result
    cache[key] = {
        "token": result["token"],
        "expires_at": time.time() + result["expiresIn"],
    }
    _save_token_cache(cache)

    return result["token"]



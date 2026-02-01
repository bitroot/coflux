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
    """Remove the credentials file."""
    path = get_credentials_path()
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
    url = f"{studio_url}/api/cli/device"

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
    url = f"{studio_url}/api/cli/device/token"
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


def exchange_for_server_token(
    team: str,
    host: str,
    studio_url: str,
    studio_token: str | None = None,
) -> dict[str, t.Any]:
    """
    Exchange a CLI session token for a server access JWT.

    Args:
        team: Team external ID
        host: Server host
        studio_url: Studio URL
        studio_token: CLI session token (uses stored token if not provided)

    Returns:
        Dict with:
        - token: Server access JWT
        - expires_in: Seconds until expiration
        - project_name: Project name (if available)
        - team_name: Team name (if available)

    Raises:
        ValueError: If no studio token is available
        httpx.HTTPStatusError: On API errors
    """
    token = studio_token or get_studio_token()
    if not token:
        raise ValueError(
            "No Studio session token available. Run 'coflux login' first."
        )

    url = f"{studio_url}/api/cli/token"

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



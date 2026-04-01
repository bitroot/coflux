"""Helpers for testing Studio JWT authentication.

Provides an in-process JWKS HTTP server and JWT minting utilities so that
E2E tests can exercise the server's Studio auth path without a real Studio
instance.
"""

import base64
import json
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

import jwt as pyjwt
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat


def _b64url(data: bytes) -> str:
    """Base64url-encode without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def generate_keypair(kid="test-key-1"):
    """Generate an Ed25519 keypair and return ``(private_key, jwk_dict)``."""
    private_key = Ed25519PrivateKey.generate()
    raw_public = private_key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    jwk = {
        "kty": "OKP",
        "crv": "Ed25519",
        "x": _b64url(raw_public),
        "kid": kid,
        "alg": "EdDSA",
        "use": "sig",
    }
    return private_key, jwk


def mint_jwt(
    private_key,
    *,
    kid="test-key-1",
    issuer,
    subject="test-user-1",
    team_id,
    host,
    workspaces=None,
    expires_in=3600,
    not_before=None,
    extra_claims=None,
):
    """Create a signed JWT matching the format the Coflux server expects.

    Returns the encoded JWT string.
    """
    now = int(time.time())
    claims = {
        "iss": issuer,
        "sub": subject,
        "aud": f"{team_id}:{host}",
        "exp": now + expires_in,
        "iat": now,
    }
    if workspaces is not None:
        claims["workspaces"] = workspaces
    if not_before is not None:
        claims["nbf"] = not_before
    if extra_claims:
        claims.update(extra_claims)
    return pyjwt.encode(
        claims, private_key, algorithm="EdDSA", headers={"kid": kid}
    )


class JWKSHandler(BaseHTTPRequestHandler):
    """Serves ``/.well-known/jwks.json``."""

    def do_GET(self):
        if self.path == "/.well-known/jwks.json":
            body = json.dumps(self.server.jwks_data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, format, *args):
        pass  # Silence request logging during tests


class JWKSServer:
    """Tiny HTTP server that serves a JWKS document.

    Usage::

        srv = JWKSServer([jwk_dict])
        srv.start()
        # ... run tests against srv.url ...
        srv.stop()

    The ``keys`` list can be mutated between requests to simulate key
    rotation.
    """

    def __init__(self, keys):
        self._httpd = HTTPServer(("127.0.0.1", 0), JWKSHandler)
        self._httpd.jwks_data = {"keys": keys}
        self.port = self._httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self._thread = None

    def start(self):
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, daemon=True
        )
        self._thread.start()

    def set_keys(self, keys):
        """Replace the served keys (takes effect on next request)."""
        self._httpd.jwks_data = {"keys": keys}

    def stop(self):
        self._httpd.shutdown()
        if self._thread:
            self._thread.join(timeout=5)

"""[sc] Space Channel Mission Control connection-token verification.

The Space Channel auth Lambda mints tokens; this module verifies them at the
WebSocket `hello`. Contract (do NOT change one-sided — fixture vectors live in
the space-os repo at lambda/auth-api/src/__tests__/fixtures/):

    token   = b64url(payloadJson) + "." + b64url(HMAC-SHA256(secret, "mc-token.v1." + b64url(payloadJson)))
    payload = { v:1, sub, cid, ent:{voice}, env, iat, exp, jti }

Stdlib only. Auth is active only when MISSION_CONTROL_TOKEN_SECRET is set,
so upstream/local development without the secret behaves exactly as before.
"""

from __future__ import annotations

import base64
import hmac
import hashlib
import json
import os
import re
import time

TOKEN_DOMAIN = b"mc-token.v1."
CLOCK_SKEW_SECONDS = 30
# Load-bearing: cid becomes part of a filesystem path (agent memory files).
CID_RE = re.compile(r"^[A-Za-z0-9-]{1,64}$")


class TokenError(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def enabled() -> bool:
    return bool(os.environ.get("MISSION_CONTROL_TOKEN_SECRET"))


def _b64url_decode(data: str) -> bytes:
    pad = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + pad)


def verify(token: str, *, secret: str | None = None, env: str | None = None,
           now: float | None = None) -> dict:
    """Return validated claims or raise TokenError(reason)."""
    secret = secret if secret is not None else os.environ.get("MISSION_CONTROL_TOKEN_SECRET", "")
    env = env if env is not None else os.environ.get("MISSION_CONTROL_ENV", "dev")
    if not secret:
        raise TokenError("no_secret")
    if not isinstance(token, str) or token.count(".") != 1:
        raise TokenError("malformed")

    encoded, sig = token.split(".")
    expected = hmac.new(secret.encode("utf-8"), TOKEN_DOMAIN + encoded.encode("ascii"),
                        hashlib.sha256).digest()
    try:
        provided = _b64url_decode(sig)
    except Exception:
        raise TokenError("malformed")
    if not hmac.compare_digest(provided, expected):
        raise TokenError("bad_signature")

    try:
        claims = json.loads(_b64url_decode(encoded))
    except Exception:
        raise TokenError("malformed")
    if not isinstance(claims, dict) or claims.get("v") != 1:
        raise TokenError("malformed")
    if not isinstance(claims.get("sub"), str) or not claims["sub"]:
        raise TokenError("malformed")
    if not CID_RE.match(str(claims.get("cid", ""))):
        raise TokenError("bad_cid")
    if claims.get("env") != env:
        raise TokenError("wrong_env")
    now_s = now if now is not None else time.time()
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)) or now_s > exp + CLOCK_SKEW_SECONDS:
        raise TokenError("expired")
    return claims

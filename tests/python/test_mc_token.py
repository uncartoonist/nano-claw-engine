"""[sc] Connection-token verification tests, including the shared Space
Channel contract fixtures (identical vectors run in the space-os repo —
never change one-sided)."""

import base64
import hashlib
import hmac
import json
from pathlib import Path

import pytest

from voice.mission_control import token as mc_token

FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "mission-control-contract.json").read_text()
)

SECRET = "unit-test-secret"
NOW = 1_784_500_000


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def mint(payload: dict, secret: str = SECRET) -> str:
    encoded = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    sig = _b64url(
        hmac.new(secret.encode(), mc_token.TOKEN_DOMAIN + encoded.encode(), hashlib.sha256).digest()
    )
    return f"{encoded}.{sig}"


def claims(**over) -> dict:
    base = {
        "v": 1,
        "sub": "u1",
        "cid": "3f2c8a90-1111-4222-8333-444455556666",
        "ent": {"voice": False},
        "env": "dev",
        "iat": NOW,
        "exp": NOW + 90,
        "jti": "j-1",
    }
    base.update(over)
    return base


def test_valid_token_roundtrip():
    out = mc_token.verify(mint(claims()), secret=SECRET, env="dev", now=NOW + 10)
    assert out["sub"] == "u1"
    assert out["cid"] == "3f2c8a90-1111-4222-8333-444455556666"


def test_tampered_payload_rejected():
    token = mint(claims())
    encoded, sig = token.split(".")
    forged_payload = _b64url(json.dumps(claims(ent={"voice": True}), separators=(",", ":")).encode())
    with pytest.raises(mc_token.TokenError) as err:
        mc_token.verify(f"{forged_payload}.{sig}", secret=SECRET, env="dev", now=NOW)
    assert err.value.reason == "bad_signature"


def test_wrong_secret_rejected():
    with pytest.raises(mc_token.TokenError) as err:
        mc_token.verify(mint(claims(), secret="other"), secret=SECRET, env="dev", now=NOW)
    assert err.value.reason == "bad_signature"


def test_expired_beyond_skew_rejected():
    with pytest.raises(mc_token.TokenError) as err:
        mc_token.verify(mint(claims()), secret=SECRET, env="dev", now=NOW + 90 + 31)
    assert err.value.reason == "expired"


def test_within_skew_accepted():
    assert mc_token.verify(mint(claims()), secret=SECRET, env="dev", now=NOW + 90 + 29)


def test_wrong_env_rejected():
    with pytest.raises(mc_token.TokenError) as err:
        mc_token.verify(mint(claims()), secret=SECRET, env="prod", now=NOW)
    assert err.value.reason == "wrong_env"


@pytest.mark.parametrize("bad", ["", "abc", "a.b.c", "notb64.!!!"])
def test_malformed_rejected(bad):
    with pytest.raises(mc_token.TokenError):
        mc_token.verify(bad, secret=SECRET, env="dev", now=NOW)


@pytest.mark.parametrize("cid", ["../../etc/passwd", "a/b", "a" * 65, "", "spaces here"])
def test_traversal_and_invalid_cid_rejected(cid):
    with pytest.raises(mc_token.TokenError) as err:
        mc_token.verify(mint(claims(cid=cid)), secret=SECRET, env="dev", now=NOW)
    assert err.value.reason == "bad_cid"


def test_no_secret_raises():
    with pytest.raises(mc_token.TokenError) as err:
        mc_token.verify(mint(claims()), secret="", env="dev", now=NOW)
    assert err.value.reason == "no_secret"


# —— Shared contract fixtures (parity with the space-os Lambda) ——————————————


def test_contract_fixture_token_verifies():
    fx = FIXTURES["token"]
    out = mc_token.verify(
        fx["token"],
        secret=FIXTURES["secret"],
        env="dev",
        now=fx["payload"]["iat"] + 10,
    )
    assert out == fx["payload"]


def test_contract_fixture_ingest_signature():
    """Engine-side ingest signing must reproduce the Lambda's expected hex."""
    fx = FIXTURES["ingest"]
    body = fx["rawBody"].encode("utf-8")
    mac = hmac.new(
        FIXTURES["secret"].encode(),
        b"mc-ingest.v1." + str(fx["timestamp"]).encode() + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    assert mac == fx["signature"]

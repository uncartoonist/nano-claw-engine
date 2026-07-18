"""[sc] Mission Control engine smoke test.

Usage:
    MISSION_CONTROL_TOKEN_SECRET=... python deploy/spacechannel/smoke.py ws://localhost:8080/ws
    MISSION_CONTROL_TOKEN_SECRET=... python deploy/spacechannel/smoke.py wss://dev-mc.spacechannel.com/ws

Mints a real connection token, opens the WS in text mode, sends a message,
asserts streaming deltas + done. Then negative checks: no token → close 4401.
Requires: pip install httpx aiohttp (already in voice requirements).
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import uuid

import aiohttp


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def mint_token(secret: str, env: str = "dev", voice: bool = False) -> tuple[str, str]:
    cid = str(uuid.uuid4())
    now = int(time.time())
    payload = {
        "v": 1, "sub": "smoke-user", "cid": cid, "ent": {"voice": voice},
        "env": env, "iat": now, "exp": now + 90, "jti": str(uuid.uuid4()),
    }
    encoded = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    sig = _b64url(hmac.new(secret.encode(), b"mc-token.v1." + encoded.encode(), hashlib.sha256).digest())
    return f"{encoded}.{sig}", cid


async def positive_flow(url: str, secret: str) -> None:
    token, cid = mint_token(secret)
    async with aiohttp.ClientSession() as http:
        async with http.ws_connect(url, origin=os.environ.get("SMOKE_ORIGIN", "https://dev.spacechannel.com")) as ws:
            await ws.send_json({"type": "hello", "token": token, "mode": "text"})
            ack = json.loads((await ws.receive(timeout=10)).data)
            assert ack["type"] == "hello_ack", f"expected hello_ack, got {ack}"
            print(f"[smoke] authenticated (conversation {cid})")

            await ws.send_json({"type": "text_message", "text": "In one short sentence, what is Space Channel?"})
            deltas, done, t0 = [], False, time.monotonic()
            ttft = None
            while time.monotonic() - t0 < 60:
                msg = await ws.receive(timeout=60)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                obj = json.loads(msg.data)
                if obj["type"] == "agent_reply_delta":
                    if ttft is None:
                        ttft = time.monotonic() - t0
                    deltas.append(obj["text"])
                elif obj["type"] == "agent_reply_done":
                    done = True
                    break
                elif obj["type"] in ("agent_reply",):  # non-stream fallback
                    deltas.append(obj["text"])
                    done = True
                    break
                elif obj["type"] == "error":
                    raise AssertionError(f"engine error: {obj}")
            assert done and deltas, f"no completed reply (deltas={len(deltas)})"
            reply = "".join(deltas)
            assert "couldn't reach the agent" not in reply.lower(), (
                "engine fell back to its error reply — check ANTHROPIC_API_KEY / agent API"
            )
            ttft_s = f"{ttft:.2f}s" if ttft is not None else "n/a (non-stream)"
            print(f"[smoke] reply ok — ttft={ttft_s} len={len(reply)} chars")
            print(f"[smoke] reply: {reply[:160]}")


async def negative_no_token(url: str) -> None:
    async with aiohttp.ClientSession() as http:
        async with http.ws_connect(url, origin=os.environ.get("SMOKE_ORIGIN", "https://dev.spacechannel.com")) as ws:
            await ws.send_json({"type": "hello", "token": "bogus.token"})
            msg = await ws.receive(timeout=10)
            assert msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED), (
                f"expected close, got {msg.type}"
            )
            assert ws.close_code == 4401, f"expected 4401, got {ws.close_code}"
            print("[smoke] bad token correctly closed with 4401")


async def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8080/ws"
    secret = os.environ.get("MISSION_CONTROL_TOKEN_SECRET", "")
    if not secret:
        print("MISSION_CONTROL_TOKEN_SECRET required", file=sys.stderr)
        sys.exit(2)
    await positive_flow(url, secret)
    await negative_no_token(url)
    print("[smoke] ALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())

"""[sc] Turn-persistence webhook: POST completed turns to the Space Channel
Lambda so Neon (the member-facing system of record) stores every conversation
turn with usage metrics.

Contract (mc-ingest.v1 — fixtures in the space-os repo):
    headers: x-mc-timestamp: <epoch s>
             x-mc-signature: hex(HMAC-SHA256(secret, "mc-ingest.v1." + ts + "." + rawBody))

Fire-and-forget: spawned as a task, 5s timeout, one retry, then log-and-drop.
A persistence failure must NEVER break the voice loop.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from pathlib import Path

import httpx

log = logging.getLogger("mc-ingest")

INGEST_DOMAIN = b"mc-ingest.v1."

_knowledge_version_cache: dict = {"mtime": None, "version": None}


def enabled() -> bool:
    return bool(os.environ.get("SPACECHANNEL_INGEST_URL"))


def knowledge_version() -> str | None:
    """Read the current knowledge package version (mtime-cached)."""
    path = os.environ.get("MISSION_CONTROL_KNOWLEDGE_VERSION_FILE", "")
    if not path:
        return None
    try:
        p = Path(path)
        mtime = p.stat().st_mtime
        if _knowledge_version_cache["mtime"] != mtime:
            data = json.loads(p.read_text())
            _knowledge_version_cache["mtime"] = mtime
            _knowledge_version_cache["version"] = data.get("version")
        return _knowledge_version_cache["version"]
    except Exception:
        return None


def sign(timestamp: int, raw_body: bytes, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        INGEST_DOMAIN + str(timestamp).encode("ascii") + b"." + raw_body,
        hashlib.sha256,
    ).hexdigest()


async def _post_once(url: str, raw_body: bytes, headers: dict) -> bool:
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(url, content=raw_body, headers=headers)
        if resp.status_code == 200:
            return True
        log.warning("ingest POST returned %s: %s", resp.status_code, resp.text[:200])
        # 401/404 are permanent for this payload; only retry server-side blips.
        return resp.status_code not in (500, 502, 503, 504)


async def post_turn(payload: dict) -> None:
    """Deliver one turn to the Lambda. Never raises."""
    url = os.environ.get("SPACECHANNEL_INGEST_URL", "")
    secret = os.environ.get("MISSION_CONTROL_TOKEN_SECRET", "")
    if not url or not secret:
        return
    try:
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        ts = int(time.time())
        headers = {
            "content-type": "application/json",
            "x-mc-timestamp": str(ts),
            "x-mc-signature": sign(ts, raw, secret),
        }
        try:
            done = await _post_once(url, raw, headers)
        except Exception as err:
            log.warning("ingest POST failed (%s); retrying once", err)
            done = False
        if not done:
            await asyncio.sleep(2.0)
            try:
                # Re-sign: the retry may cross the timestamp window otherwise.
                ts = int(time.time())
                headers["x-mc-timestamp"] = str(ts)
                headers["x-mc-signature"] = sign(ts, raw, secret)
                await _post_once(url, raw, headers)
            except Exception as err:
                log.error("ingest POST dropped after retry (%s) turn=%s", err, payload.get("turnId"))
    except Exception:
        log.exception("ingest assembly failed")


def build_turn_payload(
    *,
    conversation_id: str,
    turn_id: str,
    asked_text: str,
    said_text: str,
    content_type: str,  # 'text' | 'transcription'
    status: str,  # 'completed' | 'interrupted' | 'failed'
    rec: dict,
    started_at_iso: str,
    audio: bool,
) -> dict:
    """Map the engine's metrics record onto the canonical ingest body."""
    return {
        "conversationId": conversation_id,
        "turnId": turn_id,
        "userMessage": {
            "content": asked_text,
            "contentType": content_type,
            "startedAt": started_at_iso,
        },
        "assistantMessage": {
            "content": said_text,
            "contentType": "voice_response" if audio else "text",
            "status": status,
            "model": rec.get("model") or None,
            "provider": rec.get("provider") or None,
        },
        "usage": {
            "inputTokens": rec.get("tokens_in"),
            "outputTokens": rec.get("tokens_out"),
            "estimatedCostUsd": rec.get("est_cost_usd"),
            "sttMs": rec.get("stt_ms"),
            "llmTtftMs": rec.get("llm_ttft_ms"),
            "llmTotalMs": rec.get("llm_total_ms"),
            "ttsMs": rec.get("tts_ms"),
            "e2eMs": rec.get("e2e_ms"),
        },
        "knowledgeVersion": knowledge_version(),
        "completedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

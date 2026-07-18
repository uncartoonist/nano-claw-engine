"""[sc] Turn-ingest webhook tests: payload mapping, signing, retry-then-drop."""

import asyncio
import hashlib
import hmac
import json

import pytest

from voice.mission_control import ingest as mc_ingest


REC = {
    "model": "anthropic/claude-haiku-4-5",
    "provider": "anthropic",
    "asked_text": "What changed in space today?",
    "said_text": "Here is what we are tracking.",
    "stt_ms": None,
    "llm_ttft_ms": 640,
    "llm_total_ms": 2100,
    "tokens_in": 1200,
    "tokens_out": 180,
    "tts_ms": None,
    "e2e_ms": 2400,
    "est_cost_usd": 0.0021,
}


def build(status="completed", content_type="text", audio=False):
    return mc_ingest.build_turn_payload(
        conversation_id="3f2c8a90-1111-4222-8333-444455556666",
        turn_id="turn-1",
        asked_text=REC["asked_text"],
        said_text=REC["said_text"],
        content_type=content_type,
        status=status,
        rec=REC,
        started_at_iso="2026-07-20T12:00:00Z",
        audio=audio,
    )


def test_payload_matches_canonical_shape():
    p = build()
    assert p["conversationId"] == "3f2c8a90-1111-4222-8333-444455556666"
    assert p["turnId"] == "turn-1"
    assert p["userMessage"] == {
        "content": REC["asked_text"],
        "contentType": "text",
        "startedAt": "2026-07-20T12:00:00Z",
    }
    assert p["assistantMessage"]["status"] == "completed"
    assert p["assistantMessage"]["contentType"] == "text"
    assert p["assistantMessage"]["model"] == REC["model"]
    assert p["usage"]["inputTokens"] == 1200
    assert p["usage"]["llmTtftMs"] == 640
    assert p["usage"]["e2eMs"] == 2400
    assert "completedAt" in p


def test_transcription_and_voice_content_types():
    p = build(content_type="transcription", audio=True)
    assert p["userMessage"]["contentType"] == "transcription"
    assert p["assistantMessage"]["contentType"] == "voice_response"


def test_interrupted_status_carried():
    assert build(status="interrupted")["assistantMessage"]["status"] == "interrupted"


def test_signature_matches_contract_scheme():
    body = b'{"a":1}'
    sig = mc_ingest.sign(1784500042, body, "secret-x")
    expected = hmac.new(
        b"secret-x", b"mc-ingest.v1.1784500042." + body, hashlib.sha256
    ).hexdigest()
    assert sig == expected


@pytest.mark.asyncio
async def test_post_turn_noop_without_config(monkeypatch):
    monkeypatch.delenv("SPACECHANNEL_INGEST_URL", raising=False)
    await mc_ingest.post_turn(build())  # must not raise


@pytest.mark.asyncio
async def test_post_turn_retries_once_then_drops(monkeypatch):
    monkeypatch.setenv("SPACECHANNEL_INGEST_URL", "https://example.invalid/ingest")
    monkeypatch.setenv("MISSION_CONTROL_TOKEN_SECRET", "s")
    calls = {"n": 0}

    async def fail_once(url, raw, headers):
        calls["n"] += 1
        raise RuntimeError("network down")

    monkeypatch.setattr(mc_ingest, "_post_once", fail_once)
    monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
    await mc_ingest.post_turn(build())  # must not raise
    assert calls["n"] == 2  # initial + one retry


async def _instant_sleep(_secs):
    return None


def test_knowledge_version_absent_by_default(monkeypatch):
    monkeypatch.delenv("MISSION_CONTROL_KNOWLEDGE_VERSION_FILE", raising=False)
    assert mc_ingest.knowledge_version() is None


def test_knowledge_version_reads_file(tmp_path, monkeypatch):
    f = tmp_path / "knowledge-version.json"
    f.write_text(json.dumps({"version": "20260720T020000Z-abc123def456"}))
    monkeypatch.setenv("MISSION_CONTROL_KNOWLEDGE_VERSION_FILE", str(f))
    assert mc_ingest.knowledge_version() == "20260720T020000Z-abc123def456"

"""Voice server — aiohttp + WebSocket bridge between browser and nano-claw API."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from aiohttp import web

from voice import metrics_db
from voice import voice_catalog
from voice.flow_session import FLOW_MODES, FlowSession, get_flow_mode, set_flow_mode
from voice.text_chunker import TextChunker
from voice.tts import synthesize as tts_synthesize
from voice.wav import pcm_to_wav
from voice import kokoro_client
from voice import lux_client
from voice.backoff import Backoff
from voice.mission_control import token as mc_token
from voice.mission_control import ingest as mc_ingest
import uuid

if TYPE_CHECKING:
    from voice.webrtc import Session

log = logging.getLogger("voice-server")


def _on_agent_task_done(task: asyncio.Task) -> None:
    """Log unexpected failures from a spawned agent-handler task.

    A cancellation is the expected outcome of a committed barge-in
    (`Session.cancel_stream` cancels this exact task), so it's silently
    swallowed here rather than logged as an error.
    """
    if task.cancelled():
        return  # committed barge-in cancels the task on purpose
    exc = task.exception()
    if exc is not None:
        log.error("Agent task failed", exc_info=exc)


NANO_CLAW_URL = os.environ.get("NANO_CLAW_URL", "http://localhost:3001")
SESSION_ID = "voice-default"
STATIC_DIR = Path(__file__).resolve().parent / "web"
BARGE_IN_ENABLED = os.environ.get("NANO_CLAW_BARGE_IN", "0") not in ("0", "false", "")
# [sc] Locked mode for the public Space Channel deployment: clients cannot
# change models, pick large STT sizes, drive tool approvals, or hit
# state-mutating/transcript-leaking admin routes.
LOCKED = os.environ.get("NANO_CLAW_LOCKED", "0") not in ("0", "false", "")
STT_ALLOWED = tuple(
    s.strip()
    for s in os.environ.get("NANO_CLAW_STT_ALLOWED", "tiny,base,small,medium").split(",")
    if s.strip()
)
METRICS = metrics_db.init_db()


# no-cache: browsers must revalidate the UI on every load, otherwise tabs
# opened before a deploy keep running the old app.js (stale controls that
# silently do nothing). FileResponse still serves 304s when unchanged.
_NO_CACHE = {"Cache-Control": "no-cache"}


async def index_handler(request: web.Request) -> web.FileResponse:
    return web.FileResponse(STATIC_DIR / "index.html", headers=_NO_CACHE)


async def static_handler(request: web.Request) -> web.FileResponse:
    filename = request.match_info["filename"]
    path = (STATIC_DIR / filename).resolve()
    if not path.is_relative_to(STATIC_DIR.resolve()) or not path.is_file():
        raise web.HTTPNotFound()
    return web.FileResponse(path, headers=_NO_CACHE)


def _init_session_state(session: "Session", session_id: str, conversation_id: str | None) -> None:
    """[sc] Shared per-session init for both text-mode and WebRTC sessions."""
    session._backoff = Backoff()
    session._resume_task = None
    session._scheduler_flow_enabled = get_flow_mode() == "scheduler"
    session._scheduler_flow_attempted = False
    session._scheduler_flow = None
    session._mc_session_id = session_id
    session._mc_conversation_id = conversation_id


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    # [sc] Origin allowlist — enforced before the upgrade completes.
    allowed_origins = os.environ.get("MISSION_CONTROL_ALLOWED_ORIGINS", "")
    if allowed_origins:
        origin = request.headers.get("Origin", "")
        if origin not in [o.strip() for o in allowed_origins.split(",") if o.strip()]:
            raise web.HTTPForbidden(text="origin not allowed")

    ws = web.WebSocketResponse()
    await ws.prepare(request)
    log.info("WebSocket connected")

    session: Session | None = None
    # [sc] Connection auth state. When MISSION_CONTROL_TOKEN_SECRET is unset,
    # auth is off and behavior matches upstream exactly.
    auth_state = {"authed": not mc_token.enabled(), "voice_allowed": True}
    mc_session_id: str = SESSION_ID
    mc_conversation_id: str | None = None

    if mc_token.enabled():
        async def _auth_deadline() -> None:
            await asyncio.sleep(5)
            if not auth_state["authed"] and not ws.closed:
                await ws.close(code=4408, message=b"auth timeout")

        asyncio.ensure_future(_auth_deadline())
    # The browser pushes its persisted set_voice/set_model/set_stt right after
    # `hello`, but the session is only created when `webrtc_offer` arrives
    # (after mic permission). Buffer early settings and apply them at session
    # creation so saved choices survive a reconnect instead of being dropped.
    pending_settings: dict = {}
    http_client = httpx.AsyncClient(timeout=120.0)

    def _spawn_agent(coro, turn_state=None):
        # One active agent reply at a time. If a reply is still in flight,
        # drop the duplicate (the browser also gates new turns behind
        # agentSpeaking) so two tasks can't race on the audio queue / WS
        # and orphan each other past barge-in's reach.
        existing = session._stream_task if session else None
        if existing is not None and not existing.done():
            log.info("Agent reply already in flight; ignoring duplicate request")
            coro.close()  # avoid 'coroutine was never awaited' warning
            return
        task = asyncio.create_task(coro)
        if turn_state is not None:
            session._turn = turn_state
        session.set_stream_task(task)
        task.add_done_callback(_on_agent_task_done)

    try:
        async for raw_msg in ws:
            if raw_msg.type != web.WSMsgType.TEXT:
                continue

            try:
                msg = json.loads(raw_msg.data)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")

            # [sc] Nothing but `hello` is processed until the connection is
            # authenticated (no-op when auth is disabled).
            if not auth_state["authed"] and msg_type != "hello":
                continue

            if msg_type == "hello":
                if mc_token.enabled():
                    try:
                        claims = mc_token.verify(msg.get("token", ""))
                    except mc_token.TokenError as err:
                        log.info("MC auth rejected: %s", err.reason)
                        await ws.close(code=4401, message=b"unauthorized")
                        break
                    auth_state["authed"] = True
                    auth_state["voice_allowed"] = bool(
                        (claims.get("ent") or {}).get("voice", False)
                    )
                    mc_conversation_id = claims["cid"]
                    mc_session_id = f"mc-{mc_conversation_id}"
                    log.info("MC session authenticated (conversation %s)", mc_conversation_id)
                    # Text-only mode: create the session eagerly (upstream only
                    # creates one on webrtc_offer) with synthesis disabled.
                    if msg.get("mode") == "text" and session is None:
                        from voice.webrtc import Session

                        session = Session()
                        _init_session_state(session, mc_session_id, mc_conversation_id)
                        session.audio_enabled = False
                await ws.send_json({"type": "hello_ack", "bargeIn": BARGE_IN_ENABLED})

            elif msg_type == "webrtc_offer":
                # [sc] Voice requires the voice entitlement when auth is on.
                if mc_token.enabled() and not auth_state["voice_allowed"]:
                    await ws.send_json({
                        "type": "voice_notice",
                        "text": "Voice is not enabled for this session.",
                    })
                    continue
                # aiortc is only needed once a browser actually starts WebRTC.
                from voice.webrtc import Session

                session = Session()
                _init_session_state(session, mc_session_id, mc_conversation_id)
                session.audio_enabled = True
                # Apply any settings the browser pushed before the session existed.
                if "voice" in pending_settings:
                    v = pending_settings["voice"]
                    session.set_voice(v["voiceId"], v["speed"])
                if "model" in pending_settings:
                    session.model = pending_settings["model"]
                    log.info("Model set (pending): %s", session.model or "(default)")
                if "stt" in pending_settings:
                    session.stt_size = pending_settings["stt"]
                    log.info("STT size set (pending): %s", session.stt_size)
                pending_settings.clear()
                answer_sdp = await session.handle_offer(msg["sdp"])
                await ws.send_json({"type": "webrtc_answer", "sdp": answer_sdp})

            elif msg_type == "mic_start":
                if session:
                    session.start_recording()

            elif msg_type == "mic_stop":
                if not session:
                    continue

                t0 = time.monotonic()
                text, duration, stt_ms = await session.stop_recording()
                if not text:
                    await ws.send_json({"type": "transcription", "text": ""})
                    continue
                turn_state = {"t0": t0, "asked": text, "stt_ms": stt_ms,
                              "stt_size": session.stt_size, "voice_id": session.voice_id,
                              "model": session.model,
                              # [sc] stable turn identity for idempotent persistence
                              "turn_id": str(uuid.uuid4()),
                              "content_type": "transcription",
                              "started_at_iso": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
                await ws.send_json({"type": "transcription", "text": text})
                _spawn_agent(_handle_agent_request(ws, session, http_client, text), turn_state)

            elif msg_type == "mic_cancel":
                if session:
                    session.cancel_recording()

            elif msg_type == "text_message":
                text = msg.get("text", "").strip()
                if not text or not session:
                    continue
                await ws.send_json({"type": "transcription", "text": text})
                turn_state = {"t0": time.monotonic(), "asked": text, "stt_ms": None,
                              "stt_size": session.stt_size, "voice_id": session.voice_id,
                              "model": session.model,
                              # [sc] stable turn identity for idempotent persistence
                              "turn_id": str(uuid.uuid4()),
                              "content_type": "text",
                              "started_at_iso": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
                _spawn_agent(_handle_agent_request(ws, session, http_client, text), turn_state)

            elif msg_type == "set_model":
                if LOCKED:
                    continue  # [sc] model policy is server-owned in locked mode
                model_id = msg.get("modelId", "") or ""
                if session:
                    session.model = model_id
                    log.info("Model set: %s", session.model or "(default)")
                else:
                    pending_settings["model"] = model_id

            elif msg_type == "set_stt":
                size = msg.get("size", "base")
                size = size if size in ("tiny", "base", "small", "medium") else "base"
                if size not in STT_ALLOWED:  # [sc] clamp to the allowed set
                    size = STT_ALLOWED[0] if STT_ALLOWED else "base"
                if session:
                    session.stt_size = size
                    log.info("STT size set: %s", session.stt_size)
                else:
                    pending_settings["stt"] = size

            elif msg_type == "set_voice":
                if not session:
                    pending_settings["voice"] = {
                        "voiceId": msg.get("voiceId", ""),
                        "speed": msg.get("speed", 1.0),
                    }
                    continue
                voice_id = msg.get("voiceId", "")
                session.set_voice(voice_id, msg.get("speed", 1.0))
                # Proactively warn if a native-service voice was picked but the
                # service is down — the reply will still work (Piper fallback).
                entry = voice_catalog.lookup(voice_id)
                if entry and entry["engine"] in ("kokoro", "luxtts"):
                    probe = (kokoro_client.is_healthy if entry["engine"] == "kokoro"
                             else lux_client.is_healthy)
                    label = "Kokoro" if entry["engine"] == "kokoro" else "LuxTTS"
                    loop = asyncio.get_running_loop()
                    healthy = await loop.run_in_executor(None, probe)
                    if not healthy:
                        await ws.send_json({
                            "type": "voice_notice",
                            "text": f"{label} voice unavailable — using the fast voice.",
                        })

            elif msg_type == "tool_approve":
                if LOCKED:
                    continue  # [sc] no tool flow in the public deployment
                request_id = msg.get("requestId", "")
                if not request_id or not session:
                    continue
                _spawn_agent(_handle_tool_decision(ws, session, http_client, "approve", request_id))

            elif msg_type == "tool_reject":
                if LOCKED:
                    continue  # [sc]
                request_id = msg.get("requestId", "")
                if not request_id or not session:
                    continue
                _spawn_agent(_handle_tool_decision(ws, session, http_client, "reject", request_id))

            elif msg_type == "stop_speaking":
                if session:
                    session.stop_speaking()

            elif msg_type == "barge_in":
                if BARGE_IN_ENABLED and session:
                    # Cancel any pending resume, then pause.
                    if getattr(session, "_resume_task", None):
                        session._resume_task.cancel()
                        session._resume_task = None
                    session.pause_speaking()

            elif msg_type == "barge_in_commit":
                if BARGE_IN_ENABLED and session:
                    if getattr(session, "_resume_task", None):
                        session._resume_task.cancel()
                        session._resume_task = None
                    session.cancel_stream()          # abort reply + clear audio
                    session._backoff.reset()
                    await ws.send_json({"type": "agent_audio_end"})   # re-arm mic for the user's turn

            elif msg_type == "barge_in_false":
                if BARGE_IN_ENABLED and session and session.is_paused():
                    delay = session._backoff.next()
                    log.info("Barge-in false alarm; resuming in %.2fs", delay)

                    async def _resume_after(d, sess=session, w=ws):
                        try:
                            await asyncio.sleep(d)
                            if sess.is_paused() and not w.closed:
                                sess.resume_speaking()
                        except asyncio.CancelledError:
                            pass

                    session._resume_task = asyncio.ensure_future(_resume_after(delay))

            elif msg_type == "ping":
                await ws.send_json({"type": "pong"})

    except Exception:
        log.exception("WebSocket error")
    finally:
        if session and session._stream_task and not session._stream_task.done():
            session._stream_task.cancel()
            try:
                await session._stream_task
            except BaseException:
                pass  # CancelledError (expected) or the task's own error — we're tearing down
        await http_client.aclose()
        if session:
            await session.close()
        log.info("WebSocket disconnected")

    return ws


async def _handle_agent_request(
    ws: web.WebSocketResponse,
    session: Session,
    client: httpx.AsyncClient,
    text: str,
) -> None:
    """Stream nano-claw's reply as SSE; synthesize + forward chunks as they arrive."""
    try:
        if await _handle_scheduler_request(ws, session, text):
            return
        req_start = time.monotonic()
        async with client.stream(
            "POST",
            f"{NANO_CLAW_URL}/api/chat",
            json={
                "message": text,
                "sessionId": getattr(session, "_mc_session_id", SESSION_ID),  # [sc]
                **({"model": session.model} if session.model else {}),
            },
            headers={"Accept": "text/event-stream"},
        ) as resp:
            ctype = resp.headers.get("content-type", "")
            if "text/event-stream" not in ctype:
                data = json.loads(await resp.aread())
                await _process_api_response(ws, session, data, req_start=req_start)
                return
            await _consume_sse(ws, session, resp, req_start=req_start)
    except Exception:
        log.exception("nano-claw streaming call failed")
        error_text = "Sorry, I couldn't reach the agent."
        await ws.send_json({"type": "agent_reply", "text": error_text})
        await _speak_with_events(ws, session, error_text)


async def _handle_scheduler_request(
    ws: web.WebSocketResponse,
    session: Session,
    text: str,
) -> bool:
    """Handle an enabled scheduler turn before the normal API route."""

    if not getattr(session, "_scheduler_flow_enabled", False):
        return False

    flow = getattr(session, "_scheduler_flow", None)
    greeting = None
    if flow is None:
        if getattr(session, "_scheduler_flow_attempted", False):
            return False
        session._scheduler_flow_attempted = True
        flow = FlowSession.create()
        if flow is None:
            session._scheduler_flow_enabled = False
            return False
        session._scheduler_flow = flow
        greeting = flow.greeting
        await ws.send_json({"type": "agent_reply", "text": greeting})
        # One audio gate covers both the greeting and the pending first reply;
        # otherwise the greeting's audio_end rearms hands-free VAD too early.
        await ws.send_json({"type": "agent_audio_start"})
        await ws.send_json(_flow_state_message(flow))

    try:
        if greeting is not None:
            await session.speak_text(greeting, session.voice_id, session.speed)

        reply = await flow.reply(text)
        log.info(
            "Scheduler flow outcome=%s slots=%s",
            reply.outcome or "continue",
            reply.slots,
        )
        if reply.done:
            # WebSocket sends and playback are cancellable during barge-in.
            # Revert before either so a completed flow cannot be stranded.
            session._scheduler_flow = None
            session._scheduler_flow_enabled = False
        await ws.send_json(_flow_state_message(flow, reply))
        await ws.send_json({"type": "agent_reply", "text": reply.text})
        if greeting is not None:
            await session.speak_text(reply.text, session.voice_id, session.speed)
        else:
            await _speak_with_events(ws, session, reply.text)
        return True
    finally:
        if greeting is not None and not ws.closed:
            await ws.send_json({"type": "agent_audio_end"})


def _flow_state_message(flow, reply=None) -> dict:
    """Build the browser's defensive, read-only goal-region snapshot."""

    slots = getattr(reply, "slots", None) if reply is not None else None
    if not isinstance(slots, dict):
        slots = getattr(flow, "slots", {})
    if not isinstance(slots, dict):
        slots = {}

    rejected = getattr(reply, "rejected", []) if reply is not None else []
    if not isinstance(rejected, (list, tuple)):
        rejected = []

    turns_used = getattr(reply, "turns_used", None) if reply is not None else None
    if not isinstance(turns_used, int) or isinstance(turns_used, bool):
        turns_used = getattr(flow, "turns_used", 0)
    if not isinstance(turns_used, int) or isinstance(turns_used, bool):
        turns_used = 0
    max_turns = getattr(reply, "max_turns", None) if reply is not None else None
    if not isinstance(max_turns, int) or isinstance(max_turns, bool):
        max_turns = getattr(flow, "max_turns", 0)
    if not isinstance(max_turns, int) or isinstance(max_turns, bool):
        max_turns = 0

    supervisor_ms = (
        getattr(reply, "supervisor_ms", None) if reply is not None else None
    )
    if not isinstance(supervisor_ms, (int, float)) or isinstance(supervisor_ms, bool):
        supervisor_ms = None

    return {
        "type": "flow_state",
        "goal": str(getattr(flow, "goal", "") or ""),
        "outcome": getattr(reply, "outcome", None) if reply is not None else None,
        "slots": dict(slots),
        "rejected": [str(item) for item in rejected],
        "turns_used": int(turns_used),
        "max_turns": int(max_turns),
        "supervisor_ms": supervisor_ms,
    }


async def _consume_sse(
    ws: web.WebSocketResponse,
    session: Session,
    resp: httpx.Response,
    req_start: float | None = None,
) -> None:
    """Parse SSE frames, speaking each chunk and forwarding text to the browser."""
    # Redundant with the spawn-time set_stream_task() in websocket_handler:
    # this coroutine now always runs inside that same spawned task, so
    # current_task() here IS the task already registered on the session.
    # Left in place as a harmless no-op / safety net.
    session.set_stream_task(asyncio.current_task())
    if req_start is None:
        req_start = time.monotonic()
    first_delta = None
    first_audio = None
    said_parts = []
    chunker = TextChunker()
    loop = asyncio.get_running_loop()
    total_bytes = 0
    event = ""
    data_lines: list[str] = []

    async def speak_chunk(chunk: str):
        nonlocal total_bytes, first_audio
        said_parts.append(chunk)
        await ws.send_json({"type": "agent_reply_delta", "text": chunk})
        if not getattr(session, "audio_enabled", True):
            return  # [sc] text-only session — no synthesis
        queued_bytes = await loop.run_in_executor(
            None, session.enqueue_chunk, chunk, session.voice_id, session.speed
        )
        total_bytes += queued_bytes
        if queued_bytes and first_audio is None:
            first_audio = time.monotonic()

    session.begin_stream()
    await ws.send_json({"type": "agent_audio_start"})
    try:
        async for raw in resp.aiter_lines():
            if raw == "":  # frame boundary
                payload = "\n".join(data_lines)
                data_lines = []
                ev, event = event, ""
                if not payload:
                    continue
                obj = json.loads(payload)
                if ev == "delta":
                    if first_delta is None:
                        first_delta = time.monotonic()
                    for chunk in chunker.push(obj.get("text", "")):
                        await speak_chunk(chunk)
                elif ev == "tool_pending":
                    tail = chunker.flush()
                    if tail:
                        await speak_chunk(tail)
                    _stash_turn_metrics(
                        session, req_start, first_delta, first_audio, said_parts, obj.get("debug") or {}
                    )
                    await ws.send_json({"type": "tool_pending", "requestId": obj["requestId"], "tools": obj["tools"]})
                    await ws.send_json({"type": "agent_audio_end"})
                    return
                elif ev == "final":
                    tail = chunker.flush()
                    if tail:
                        await speak_chunk(tail)
                    debug = obj.get("debug") or {}
                    if debug:
                        await ws.send_json({"type": "debug", **debug})
                    rec = _write_turn_metrics(session, req_start, first_delta, first_audio, said_parts, debug)
                    _spawn_ingest(session, rec, "completed")  # [sc]
                    await ws.send_json({"type": "agent_reply_done"})
                elif ev == "error":
                    await ws.send_json({"type": "agent_reply", "text": f"Error: {obj.get('error', 'agent error')}"})
                continue
            if raw.startswith("event:"):
                event = raw[6:].strip()
            elif raw.startswith("data:"):
                data_lines.append(raw[5:].strip())

        await session.end_stream(total_bytes)
        session._backoff.reset()   # clean drain — clear consecutive-false count
        if not ws.closed:
            await ws.send_json({"type": "agent_audio_end"})
    except asyncio.CancelledError:
        # [sc] Interrupted turn (barge-in commit or WS teardown): persist what
        # was actually streamed, honestly marked. No ws sends here — the
        # cancel paths handle their own signalling.
        rec = _write_turn_metrics(session, req_start, first_delta, first_audio, said_parts, {})
        _spawn_ingest(session, rec, "interrupted")
        session.stop_speaking()
        raise
    except Exception:
        session.stop_speaking()
        if not ws.closed:
            await ws.send_json({"type": "agent_audio_end"})
        raise


def _ms(a, b):
    return int((b - a) * 1000) if (a is not None and b is not None) else None


def _sum_metric_values(*values):
    numbers = [value for value in values if isinstance(value, (int, float)) and not isinstance(value, bool)]
    return sum(numbers) if numbers else None


def _generation_ms(debug):
    total_ms = debug.get("durationMs")
    first_token_ms = debug.get("firstTokenMs")
    if not isinstance(total_ms, (int, float)) or not isinstance(first_token_ms, (int, float)):
        return None
    return max(1, total_ms - first_token_ms)


def _stash_turn_metrics(session, req_start, first_delta, first_audio, said_parts, debug):
    """Best-effort accumulation for a turn paused on tool approval."""
    try:
        turn = getattr(session, "_turn", None)
        if not isinstance(turn, dict):
            return
        partial = turn.get("_metrics")
        if not isinstance(partial, dict):
            partial = {}
        usage = debug.get("tokenUsage") or {}
        tokens_out = usage.get("completion")
        current_gen_ms = _generation_ms(debug)
        prior_said = partial.get("said_parts")
        if not isinstance(prior_said, list):
            prior_said = []
        turn["_metrics"] = {
            "said_parts": [*prior_said, *said_parts],
            "tokens_in": _sum_metric_values(partial.get("tokens_in"), usage.get("prompt")),
            "tokens_out": _sum_metric_values(partial.get("tokens_out"), tokens_out),
            "llm_total_ms": _sum_metric_values(partial.get("llm_total_ms"), debug.get("durationMs")),
            "generation_ms": _sum_metric_values(partial.get("generation_ms"), current_gen_ms),
            "generation_complete": partial.get("generation_complete", True)
            and (not tokens_out or current_gen_ms is not None),
            "t0": partial.get("t0", turn.get("t0", req_start)),
            "req_start": partial.get("req_start", req_start),
            "first_delta": partial.get("first_delta") if partial.get("first_delta") is not None else first_delta,
            "first_audio": partial.get("first_audio") if partial.get("first_audio") is not None else first_audio,
        }
    except Exception:
        log.exception("metrics: failed to stash partial turn")


def _write_turn_metrics(session, req_start, first_delta, first_audio, said_parts, debug):
    try:
        turn = getattr(session, "_turn", {}) or {}
        partial = turn.get("_metrics") or {}
        usage = debug.get("tokenUsage") or {}
        current_tokens_out = usage.get("completion")
        current_gen_ms = _generation_ms(debug)
        accumulated_said = partial.get("said_parts") if isinstance(partial.get("said_parts"), list) else []
        said_parts = [*accumulated_said, *said_parts]
        req_start = partial.get("req_start", req_start)
        first_delta = partial.get("first_delta") if partial.get("first_delta") is not None else first_delta
        first_audio = partial.get("first_audio") if partial.get("first_audio") is not None else first_audio
        t0 = partial.get("t0", turn.get("t0", req_start))
        tokens_in = _sum_metric_values(partial.get("tokens_in"), usage.get("prompt"))
        tokens_out = _sum_metric_values(partial.get("tokens_out"), current_tokens_out)
        total_ms = _sum_metric_values(partial.get("llm_total_ms"), debug.get("durationMs"))
        gen_ms = _sum_metric_values(partial.get("generation_ms"), current_gen_ms)
        generation_complete = partial.get("generation_complete", True) and (
            not current_tokens_out or current_gen_ms is not None
        )
        tok_per_sec = round(tokens_out / (gen_ms / 1000), 2) if (
            tokens_out and gen_ms and generation_complete
        ) else None
        model = turn.get("model") or debug.get("model") or ""
        provider = model.split("/")[0] if "/" in model else None
        rec = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "session_id": getattr(session, "_mc_session_id", SESSION_ID),  # [sc]
            "provider": provider, "model": model,
            "model_version": debug.get("model"),
            "stt_size": turn.get("stt_size"), "voice_id": turn.get("voice_id"),
            "asked_text": turn.get("asked"), "said_text": " ".join(said_parts).strip() or None,
            "stt_ms": turn.get("stt_ms"),
            "llm_ttft_ms": _ms(req_start, first_delta),
            "llm_total_ms": total_ms,
            "tokens_in": tokens_in, "tokens_out": tokens_out, "tok_per_sec": tok_per_sec,
            "tts_ms": _ms(first_delta, first_audio),
            "e2e_ms": _ms(t0, first_audio),
            "est_cost_usd": metrics_db.estimate_cost(METRICS, model, tokens_in, tokens_out) if METRICS else None,
        }
        metrics_db.record_turn(METRICS, rec)
        turn.pop("_metrics", None)
        return rec  # [sc] consumed by the Space Channel ingest webhook
    except Exception:
        log.exception("metrics: failed to assemble turn record")
        return None


def _spawn_ingest(session, rec, status: str) -> None:
    """[sc] Fire-and-forget turn delivery to Space Channel; never raises."""
    try:
        if rec is None or not mc_ingest.enabled():
            return
        conversation_id = getattr(session, "_mc_conversation_id", None)
        turn = getattr(session, "_turn", {}) or {}
        if not conversation_id or not turn.get("turn_id"):
            return
        payload = mc_ingest.build_turn_payload(
            conversation_id=conversation_id,
            turn_id=turn["turn_id"],
            asked_text=rec.get("asked_text") or "",
            said_text=rec.get("said_text") or "",
            content_type=turn.get("content_type", "text"),
            status=status,
            rec=rec,
            started_at_iso=turn.get("started_at_iso", ""),
            audio=bool(getattr(session, "audio_enabled", True)),
        )
        asyncio.ensure_future(mc_ingest.post_turn(payload))
    except Exception:
        log.exception("ingest spawn failed")


async def _handle_tool_decision(
    ws: web.WebSocketResponse,
    session: Session,
    client: httpx.AsyncClient,
    action: str,
    request_id: str,
) -> None:
    """POST approve/reject to nano-claw API and handle response."""
    try:
        endpoint = f"{NANO_CLAW_URL}/api/chat/{action}"
        req_start = time.monotonic()
        async with client.stream(
            "POST",
            endpoint,
            json={"requestId": request_id, "sessionId": getattr(session, "_mc_session_id", SESSION_ID)},  # [sc]
            headers={"Accept": "text/event-stream"},
        ) as resp:
            ctype = resp.headers.get("content-type", "")
            if "text/event-stream" not in ctype:
                data = json.loads(await resp.aread())
                await _process_api_response(ws, session, data, req_start=req_start)
                return
            await _consume_sse(ws, session, resp, req_start=req_start)
    except Exception:
        log.exception("nano-claw API %s call failed", action)
        error_text = "Sorry, tool execution failed."
        await ws.send_json({"type": "agent_reply", "text": error_text})
        await _speak_with_events(ws, session, error_text)


async def _speak_with_events(
    ws: web.WebSocketResponse,
    session: Session,
    text: str,
) -> float | None:
    """Keep browser VAD muted until synthesized audio actually finishes."""
    await ws.send_json({"type": "agent_audio_start"})
    try:
        if not getattr(session, "audio_enabled", True):
            return None  # [sc] text-only session — no synthesis
        return await session.speak_text(text, session.voice_id, session.speed)
    finally:
        if not ws.closed:
            await ws.send_json({"type": "agent_audio_end"})


async def _process_api_response(
    ws: web.WebSocketResponse,
    session: Session,
    data: dict,
    req_start: float | None = None,
) -> None:
    """Route an API response to the browser and optionally TTS."""
    if req_start is None:
        req_start = time.monotonic()
    # Forward debug info if present
    debug = data.get("debug")
    if debug:
        log.info(
            "iter=%d msgs=%d model=%s tokens=%s duration=%dms finish=%s",
            debug.get("iteration", 0),
            debug.get("messageCount", 0),
            debug.get("model", "?"),
            debug.get("tokenUsage"),
            debug.get("durationMs", 0),
            debug.get("finishReason"),
        )
        await ws.send_json({"type": "debug", **debug})

    if data.get("type") == "final":
        reply = data.get("response", "")
        await ws.send_json({"type": "agent_reply", "text": reply})
        first_audio = None
        if reply:
            first_audio = await _speak_with_events(ws, session, reply)
        else:
            await ws.send_json({"type": "agent_audio_end"})
        rec = _write_turn_metrics(session, req_start, None, first_audio, [reply] if reply else [], debug or {})
        _spawn_ingest(session, rec, "completed")  # [sc]
    elif data.get("type") == "tool_pending":
        _stash_turn_metrics(session, req_start, None, None, [], debug or {})
        await ws.send_json({
            "type": "tool_pending",
            "requestId": data["requestId"],
            "tools": data["tools"],
        })
    elif data.get("error"):
        error_text = f"Error: {data['error']}"
        await ws.send_json({"type": "agent_reply", "text": error_text})
        await _speak_with_events(ws, session, error_text)


_PREVIEW_SAMPLES = {
    "a": "Hi, this is how I sound.",
    "b": "Hi, this is how I sound.",
    "e": "Hola, así es como sueno.",
}


async def voices_handler(request: web.Request) -> web.Response:
    return web.json_response(voice_catalog.grouped_for_ui())


async def models_handler(request: web.Request) -> web.Response:
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{NANO_CLAW_URL}/api/models")
        return web.json_response(resp.json())


async def metrics_handler(request: web.Request) -> web.Response:
    if LOCKED:
        return web.Response(status=403, text="locked")  # [sc] leaks transcripts
    if METRICS is None:
        return web.json_response({"recent": [], "byModel": []})
    return web.json_response({
        "recent": metrics_db.recent(METRICS, 50),
        "byModel": metrics_db.aggregates(METRICS),
    })


def _flow_api_payload() -> dict:
    return {
        "active": get_flow_mode(),
        "options": list(FLOW_MODES),
        "availability_ok": FlowSession.availability_ok(),
    }


async def flow_get_handler(request: web.Request) -> web.Response:
    """Report the flow used for new browser sessions and phone calls."""

    return web.json_response(_flow_api_payload())


async def flow_set_handler(request: web.Request) -> web.Response:
    """Set the flow used for new browser sessions and phone calls."""

    if LOCKED:
        return web.Response(status=403, text="locked")  # [sc] mutates global state
    try:
        body = await request.json()
    except (json.JSONDecodeError, TypeError):
        return web.Response(status=400, text="bad json")
    if not isinstance(body, dict):
        return web.Response(status=400, text="bad json")
    mode = str(body.get("mode", "")).lower()
    if not set_flow_mode(mode):
        return web.Response(status=400, text=f"unknown mode: {mode}")
    return web.json_response(_flow_api_payload())


async def preview_handler(request: web.Request) -> web.Response:
    body = await request.json()
    voice_id = body.get("voiceId", "")
    entry = voice_catalog.lookup(voice_id)
    if not entry:
        raise web.HTTPBadRequest(text="unknown voice")
    sample = _PREVIEW_SAMPLES.get(entry.get("lang", "a"), _PREVIEW_SAMPLES["a"])
    loop = asyncio.get_running_loop()
    pcm_48k = await loop.run_in_executor(None, tts_synthesize, sample, voice_id, 1.0)
    wav = pcm_to_wav(pcm_48k, 48000)
    return web.Response(body=wav, content_type="audio/wav")


def create_app() -> web.Application:
    from voice.phone import register_phone_routes
    app = web.Application()
    app.router.add_get("/", index_handler)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/api/voices", voices_handler)
    app.router.add_post("/api/preview", preview_handler)
    app.router.add_get("/api/models", models_handler)
    app.router.add_get("/api/metrics", metrics_handler)
    app.router.add_get("/api/voice/flow", flow_get_handler)
    app.router.add_post("/api/voice/flow", flow_set_handler)
    register_phone_routes(app)  # no-op unless NANO_CLAW_PHONE=1
    app.router.add_get("/{filename}", static_handler)
    return app


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-14s %(levelname)-5s %(message)s",
    )
    port = int(os.environ.get("VOICE_PORT", "8080"))
    app = create_app()
    log.info("Voice server starting on port %d", port)
    web.run_app(app, port=port, print=None)
    log.info("Voice server stopped")

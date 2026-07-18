"""WebRTC session — PeerConnection lifecycle, mic recording, TTS playback."""

from __future__ import annotations

import asyncio
from collections import deque
import logging
import os
import re
import time

import httpx
import numpy as np
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCConfiguration, RTCIceServer

from voice.types import AudioChunk
from voice.audio.audio_queue import AudioQueue
from voice.audio.webrtc_audio_source import WebRTCAudioSource

FRAME_SAMPLES = 960  # 20ms at 48kHz
SAMPLE_RATE = 48000
MIC_PREROLL_FRAMES = 30  # 600ms: preserves the first word while VAD opens

log = logging.getLogger("webrtc")


class QueuedGenerator:
    """Reads PCM from an AudioQueue FIFO in 20ms chunks."""

    def __init__(self, queue: AudioQueue):
        self.queue = queue

    def next_chunk(self) -> AudioChunk:
        pcm = self.queue.read(FRAME_SAMPLES * 2)  # 2 bytes per int16 sample
        return AudioChunk(samples=pcm, sample_rate=SAMPLE_RATE, channels=1)


class Session:
    """Manages one WebRTC peer connection and its audio track."""

    def __init__(self):
        # [sc] Env-configurable ICE servers (comma-separated URLs). Required
        # for off-LAN use: with no STUN the server only offers host candidates
        # and internet peers can never reach it. Empty env = upstream behavior.
        ice_env = os.environ.get("NANO_CLAW_ICE_SERVERS", "")
        ice_servers = [RTCIceServer(urls=u.strip()) for u in ice_env.split(",") if u.strip()]
        self._pc = RTCPeerConnection(configuration=RTCConfiguration(iceServers=ice_servers))
        self._audio_source = WebRTCAudioSource()

        self._audio_queue = AudioQueue()
        self._tts_generator = QueuedGenerator(self._audio_queue)

        # Mic recording state
        self._recording = False
        self._mic_frames: list[bytes] = []
        self._mic_preroll: deque[bytes] = deque(maxlen=MIC_PREROLL_FRAMES)
        self._mic_track = None
        self._mic_recv_task: asyncio.Task | None = None
        self._closed = False

        # Selected voice for this session (browser default: Kokoro af_heart).
        self.voice_id = "af_heart"
        self.speed = 1.0

        # Pipeline settings: model + STT (Whisper) size for this session.
        self.model = ""       # "" → server uses its default
        self.stt_size = "base"

        self._paused = False
        self._stream_task: asyncio.Task | None = None
        self._turn: dict = {}

        @self._pc.on("connectionstatechange")
        async def on_conn_state():
            log.info("Connection state: %s", self._pc.connectionState)

        @self._pc.on("track")
        async def on_track(track):
            if track.kind != "audio":
                return
            log.info("Received remote audio track from browser mic")
            self._mic_track = track
            self._mic_recv_task = asyncio.ensure_future(self._recv_mic_audio(track))

    async def handle_offer(self, sdp: str) -> str:
        """Process client SDP offer, return SDP answer."""
        self._pc.addTrack(self._audio_source)

        offer = RTCSessionDescription(sdp=sdp, type="offer")
        await self._pc.setRemoteDescription(offer)

        answer = await self._pc.createAnswer()
        await self._pc.setLocalDescription(answer)

        log.info("SDP answer created")
        return self._pc.localDescription.sdp

    def start_recording(self):
        """Start buffering mic audio, including the VAD trigger pre-roll."""
        self._mic_frames = list(self._mic_preroll)
        self._mic_preroll.clear()
        self._recording = True
        log.info(
            "Mic recording started (mic_track=%s, preroll_frames=%d)",
            "attached" if self._mic_track else "MISSING",
            len(self._mic_frames),
        )

    def cancel_recording(self):
        """Discard a partial hands-free turn without invoking STT or Claude."""
        self._recording = False
        self._mic_frames.clear()
        log.info("Mic recording cancelled")

    async def stop_recording(self) -> tuple[str, float, int | None]:
        """Stop recording and transcribe all captured audio.

        Returns:
            Tuple of (transcribed_text, audio_duration_seconds, stt_ms).
        """
        self._recording = False

        if not self._mic_frames:
            log.warning("No mic frames captured")
            return "", 0.0, None

        pcm_data = b"".join(self._mic_frames)
        self._mic_frames.clear()
        audio_duration_s = len(pcm_data) / (SAMPLE_RATE * 2)

        log.info("Mic recording stopped: %d bytes, %.2fs", len(pcm_data), audio_duration_s)

        stt_url = os.environ.get("STT_SERVICE_URL", "http://host.docker.internal:8200")
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{stt_url}/transcribe",
                    content=pcm_data,
                    headers={
                        "Content-Type": "application/octet-stream",
                        "X-Sample-Rate": str(SAMPLE_RATE),
                        "X-Model-Size": self.stt_size,
                    },
                )
                result = resp.json()
                text = result.get("text", "")
                stt_ms = result.get("processing_ms")
        except Exception:
            log.exception("STT service call failed (is stt-service running on %s?)", stt_url)
            return "", 0.0, None
        return text, audio_duration_s, stt_ms

    def stop_speaking(self):
        """Stop TTS playback — clear the audio queue."""
        self._audio_queue.clear()
        self._audio_source.clear_generator()
        self._paused = False
        log.info("TTS playback stopped")

    def set_stream_task(self, task) -> None:
        """Remember the task running the current streamed reply (for cancel)."""
        self._stream_task = task

    def is_paused(self) -> bool:
        return self._paused

    def pause_speaking(self) -> None:
        """Barge-in pause: go silent but KEEP the queued audio for resume."""
        self._paused = True
        self._audio_source.clear_generator()
        log.info("Barge-in: paused (%d bytes retained)", self._audio_queue.available)

    def resume_speaking(self) -> None:
        """Resume a paused reply from where it stopped."""
        self._paused = False
        self._audio_source.set_generator(self._tts_generator)
        log.info("Barge-in: resumed (%d bytes queued)", self._audio_queue.available)

    def cancel_stream(self) -> None:
        """Committed barge-in: discard the reply audio + abort its stream task."""
        self._paused = False
        self._audio_queue.clear()
        self._audio_source.clear_generator()
        if (self._stream_task and self._stream_task is not asyncio.current_task()
                and not self._stream_task.done()):
            self._stream_task.cancel()
        self._stream_task = None
        log.info("Barge-in: committed — reply cancelled")

    def set_voice(self, voice_id: str, speed: float):
        """Update the voice + speed used for subsequent replies."""
        if voice_id:
            self.voice_id = voice_id
        try:
            self.speed = max(0.5, min(2.0, float(speed)))
        except (TypeError, ValueError):
            pass
        log.info("Voice set: %s (speed=%.2f)", self.voice_id, self.speed)

    @staticmethod
    def _clean_for_speech(text: str) -> str:
        """Strip markdown formatting so TTS reads clean prose."""
        text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
        text = re.sub(r'^\s*[-*•]\s+', '', text, flags=re.MULTILINE)
        text = re.sub(r'^\s*\d+\.\s+', '', text, flags=re.MULTILINE)
        text = re.sub(r'\*{1,3}(.+?)\*{1,3}', r'\1', text)
        text = re.sub(r'\*{1,3}', '', text)
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'`(.+?)`', r'\1', text)
        text = re.sub(r'\n{2,}', '. ', text)
        text = re.sub(r'\n', ' ', text)
        text = re.sub(r'\s{2,}', ' ', text)
        text = re.sub(r'\.{2,}', '.', text)
        return text.strip()

    @staticmethod
    def _split_sentences(text: str) -> list[str]:
        """Split text into sentences for incremental TTS."""
        parts = re.split(r'(?<=[.!?])\s+', text.strip())
        return [p for p in parts if p.strip()]

    def begin_stream(self) -> None:
        """Start a fresh reply: discard any leftover audio, then attach the generator."""
        self._audio_queue.clear()
        self._paused = False
        self._audio_source.set_generator(self._tts_generator)

    def enqueue_chunk(self, text: str, voice_id: str = "", speed: float = 1.0) -> int:
        """Synthesize one already-clean chunk and enqueue it. Returns bytes queued."""
        from voice.tts import synthesize
        pcm_48k = synthesize(text, voice_id, speed)
        if pcm_48k:
            self._audio_queue.enqueue(pcm_48k)
        return len(pcm_48k)

    async def end_stream(self, total_bytes: int) -> None:
        """Wait for the queue to drain, then detach the generator (mirrors speak_text tail)."""
        loop = asyncio.get_running_loop()
        playback_seconds = total_bytes / (SAMPLE_RATE * 2)
        budget = max(5.0, min(120.0, playback_seconds + 5.0))
        deadline = loop.time() + budget
        while self._audio_queue.available and not self._closed:
            await asyncio.sleep(0.02)
            if self._paused:
                # Freeze the countdown while paused (extend the deadline).
                deadline += 0.02
                continue
            if loop.time() >= deadline:
                break
        if self._audio_queue.available and not self._paused:
            log.warning("TTS playback drain timed out with %d bytes queued", self._audio_queue.available)
            self._audio_queue.clear()
        # The last WebRTC frame has left the queue but can still be in the
        # browser audio buffer. Keep the mic gate closed through that tail.
        await asyncio.sleep(0.15)
        if not self._paused:
            self._audio_source.clear_generator()

    async def speak_text(self, text: str, voice_id: str = "", speed: float = 1.0) -> float | None:
        """Whole-text path (non-streaming fallback): clean, split, enqueue, drain."""
        self.begin_stream()
        text = self._clean_for_speech(text)
        sentences = self._split_sentences(text)
        loop = asyncio.get_running_loop()
        total_bytes = 0
        first_audio = None
        for sentence in sentences:
            queued_bytes = await loop.run_in_executor(None, self.enqueue_chunk, sentence, voice_id, speed)
            total_bytes += queued_bytes
            if queued_bytes and first_audio is None:
                first_audio = time.monotonic()
        await self.end_stream(total_bytes)
        return first_audio

    async def _recv_mic_audio(self, track):
        """Background task: continuously receive audio frames from browser mic."""
        logged_format = False
        while True:
            try:
                frame = await track.recv()
            except Exception:
                log.info("Mic track ended")
                break

            if not logged_format:
                arr = frame.to_ndarray()
                log.info("Mic frame format=%s rate=%d samples=%d shape=%s",
                         frame.format.name, frame.sample_rate, frame.samples, arr.shape)
                logged_format = True

            arr = frame.to_ndarray()
            if arr.dtype in (np.float32, np.float64):
                arr = (arr * 32767).clip(-32768, 32767).astype(np.int16)
            flat = arr.flatten()
            channels = flat.shape[0] // frame.samples
            if channels > 1:
                flat = flat[::channels]
            pcm = flat.astype(np.int16).tobytes()
            if self._recording:
                self._mic_frames.append(pcm)
            else:
                self._mic_preroll.append(pcm)

    async def close(self):
        """Tear down the peer connection."""
        if self._closed:
            return
        self._closed = True
        self._recording = False
        self._audio_source.clear_generator()
        if self._mic_recv_task:
            self._mic_recv_task.cancel()
        await self._pc.close()
        log.info("Session closed")

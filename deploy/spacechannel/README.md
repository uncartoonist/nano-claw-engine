# Space Channel patches (`spacechannel` branch)

Fork of [davidbmar/2026-nano-claw-voice-loop-tts-stt](https://github.com/davidbmar/2026-nano-claw-voice-loop-tts-stt)
(MIT — upstream LICENSE and attribution unchanged). `main` tracks upstream and
is never committed to; `git log main..spacechannel` is the patch inventory.

## Diff inventory vs upstream (merge cheat sheet)

| Commit | Files touched | Nature |
|---|---|---|
| P1 auth/session/text | `voice/mission_control/token.py` (new), `voice/server.py` | Guarded insertions in `websocket_handler` (hello auth, origin allowlist, auth gate), `_init_session_state` helper, `getattr(session,"_mc_session_id",...)` at 3 call sites, `audio_enabled` guards in 2 speak paths. Inert when `MISSION_CONTROL_TOKEN_SECRET` unset. |
| P3+P5 locked mode | `voice/server.py`, `src/api/server.ts`, `src/mission_control.ts` (new) | `NANO_CLAW_LOCKED` guards on set_model/set_stt/tools/flow-POST/metrics; Node: model strip, bind host, CORS env. Inert when unset. |
| P4 ingest | `voice/mission_control/ingest.py` (new), `voice/server.py` | turn ids in the two `turn_state` dicts; `_write_turn_metrics` returns rec; `_spawn_ingest` at 2 completion sites + CancelledError hook. Inert when `SPACECHANNEL_INGEST_URL` unset. |
| tests | `tests/python/test_mc_*.py`, `tests/python/fixtures/` | additive |
| deploy | `deploy/spacechannel/*` | additive |

Hot files for upstream merges: `voice/server.py` (all patches are guarded
one-liners calling into `voice/mission_control/`), `src/api/server.ts`
(3 small guarded edits).

## Upstream sync

```bash
git fetch upstream
git checkout main && git merge --ff-only upstream/main && git push origin main
git checkout spacechannel && git merge main   # resolve in the files above
.venv-test/bin/pytest tests/python && npm test
```

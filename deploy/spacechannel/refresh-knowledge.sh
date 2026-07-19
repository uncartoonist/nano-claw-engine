#!/usr/bin/env bash
# [sc] Host-side knowledge refresh for the Space Channel engine (milestone 3d).
#
# Wraps scripts/refresh_site.sh (re-crawl from the base URL + feed list already
# recorded in site_index.json, then rebuild the digest — fail-loud, previous
# knowledge.md kept on any failure), then stamps knowledge-version.json and
# archives digest history so every ingested turn in Neon records which
# knowledge package answered it (MISSION_CONTROL_KNOWLEDGE_VERSION_FILE →
# voice/mission_control/ingest.py knowledge_version()).
#
# Run by mc-knowledge-refresh.timer (see units alongside this script); safe to
# run by hand: deploy/spacechannel/refresh-knowledge.sh
set -euo pipefail

ENGINE=/opt/mission-control/engine
SITE=spacechannel
DATA="$ENGINE/data/$SITE"
DIGEST="$DATA/knowledge.md"
VERSION_FILE="$DATA/knowledge-version.json"
ARCHIVE_DIR="$DATA/knowledge-versions"
KEEP_VERSIONS=14

# refresh_site.sh falls back to bare `python3`; the host's system python has
# no httpx — put the engine venv first so that fallback resolves inside it.
export PATH="$ENGINE/.venv/bin:$PATH"

# Report a build to the gateway's signed knowledge-build ledger (Mission
# Operations shows the last 10). Best-effort: a dead Lambda must never fail
# the refresh itself. Args: status version checksum sizeChars
report_build() {
  python3 - "$@" <<'PY' || true
import hashlib, hmac, json, sys, time, urllib.request
status, version, checksum, size = sys.argv[1:5]
env = {}
for line in open("/opt/mission-control/.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v
secret, url = env.get("MISSION_CONTROL_TOKEN_SECRET"), env.get("SPACECHANNEL_INGEST_URL")
if not secret or not url:
    sys.exit(0)
body = json.dumps({
    "version": version,
    "checksum": checksum or None,
    "status": status,
    "sizeChars": int(size) if size.isdigit() else None,
}).encode()
ts = str(int(time.time()))
sig = hmac.new(secret.encode(), f"mc-ingest.v1.{ts}.".encode() + body, hashlib.sha256).hexdigest()
req = urllib.request.Request(url + "/knowledge-build", data=body, headers={
    "Content-Type": "application/json", "x-mc-timestamp": ts, "x-mc-signature": sig})
try:
    urllib.request.urlopen(req, timeout=10)
    print(f"  reported knowledge-build {status} ({version}) to gateway")
except Exception as e:
    print(f"  WARN: knowledge-build report failed: {e}", file=sys.stderr)
PY
}

if ! "$ENGINE/scripts/refresh_site.sh" "$SITE"; then
  report_build failed "$(date -u +%Y%m%dT%H%M%SZ)-failed" "" 0
  exit 1
fi

# ── Version stamp + archive (only when the digest actually changed) ────────
# The builder embeds a "Snapshot crawled <ts>" line that changes every run;
# hash the content without it or every refresh would mint a new version.
sha="$(grep -v '^Snapshot crawled ' "$DIGEST" | sha256sum | cut -c1-12)"
prev_sha=""
if [ -f "$VERSION_FILE" ]; then
  prev_sha="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('sha',''))" "$VERSION_FILE" 2>/dev/null || true)"
fi

if [ "$sha" = "$prev_sha" ]; then
  version="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['version'])" "$VERSION_FILE")"
  echo "Digest unchanged (sha $sha) — version stays $version"
  report_build success "$version" "$sha" "$(wc -c < "$DIGEST" | tr -d ' ')"
  exit 0
fi

version="$(date -u +%Y%m%dT%H%M%SZ)-$sha"
chars="$(wc -c < "$DIGEST" | tr -d ' ')"

mkdir -p "$ARCHIVE_DIR"
cp "$DIGEST" "$ARCHIVE_DIR/$version.md"

# Atomic version-file write: the engine mtime-caches this file and the ingest
# path may read it mid-write otherwise.
tmp="$VERSION_FILE.tmp"
printf '{"version": "%s", "sha": "%s", "chars": %s, "built_at": "%s"}\n' \
  "$version" "$sha" "$chars" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$tmp"
mv "$tmp" "$VERSION_FILE"

# Prune archive to the newest KEEP_VERSIONS (names sort chronologically).
ls -1 "$ARCHIVE_DIR" | sort | head -n -"$KEEP_VERSIONS" | while IFS= read -r old; do
  rm -f "$ARCHIVE_DIR/$old"
done

echo "Knowledge version: $version ($chars chars, $(ls -1 "$ARCHIVE_DIR" | wc -l | tr -d ' ') archived)"
report_build success "$version" "$sha" "$chars"

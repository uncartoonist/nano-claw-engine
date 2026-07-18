# Space Channel Mission Control — dev engine host runbook

Host: EC2 t3a.xlarge, Ubuntu 24.04, 50 GB encrypted gp3, Elastic IP,
`dev-mc.spacechannel.com` (A record at GoDaddy). Fork branch `spacechannel`
of `uncartoonist/nano-claw-engine`.

## One-time provisioning (from an operator machine with AWS creds)

```bash
# Security group: 443 (Caddy TLS/WSS), 22 (admin), UDP 40000-49999 (WebRTC ICE)
aws ec2 create-security-group --group-name mc-dev --description "Mission Control dev engine"
aws ec2 authorize-security-group-ingress --group-name mc-dev --protocol tcp --port 443 --cidr 0.0.0.0/0
aws ec2 authorize-security-group-ingress --group-name mc-dev --protocol tcp --port 22 --cidr <ADMIN_IP>/32
aws ec2 authorize-security-group-ingress --group-name mc-dev --protocol udp --port 40000-49999 --cidr 0.0.0.0/0
aws ec2 run-instances --instance-type t3a.xlarge --image-id <ubuntu-24.04-ami> \
  --key-name <keypair> --security-groups mc-dev \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":50,"VolumeType":"gp3","Encrypted":true}}]'
aws ec2 allocate-address && aws ec2 associate-address --instance-id <id> --allocation-id <alloc>
# → give the Elastic IP to the user for the GoDaddy A record: dev-mc → <EIP>
```

## Host setup (as root on the instance)

```bash
apt-get update && apt-get install -y docker.io docker-compose-v2 caddy git
# WebRTC: aioice binds OS-ephemeral UDP ports (no port-range knob in 0.10.2);
# narrow the host ephemeral range to match the security group.
echo 'net.ipv4.ip_local_port_range = 40000 49999' > /etc/sysctl.d/99-mc-webrtc.conf
sysctl --system

mkdir -p /opt/mission-control/{data,locks,logs}
git clone -b spacechannel https://github.com/uncartoonist/nano-claw-engine.git /opt/mission-control/engine

# Env (root:600) — see .env template below
install -m 600 /dev/null /opt/mission-control/.env && $EDITOR /opt/mission-control/.env

cp /opt/mission-control/engine/deploy/spacechannel/Caddyfile /etc/caddy/Caddyfile
systemctl reload caddy   # Caddy fetches the LE cert once DNS resolves

cd /opt/mission-control/engine/deploy/spacechannel
docker compose build
```

## Seed knowledge (before first start; crawls PROD content)

```bash
cd /opt/mission-control/engine
python3 -m venv .venv && .venv/bin/pip install httpx beautifulsoup4  # crawl deps (check script imports)
.venv/bin/python scripts/crawl_site.py https://www.spacechannel.com/ --name spacechannel --out /opt/mission-control/data \
  --feed https://www.spacechannel.com/data/launches.json \
  --feed https://www.spacechannel.com/data/ufo-cases.json \
  --feed https://www.spacechannel.com/data/ufo-wire.json \
  --feed https://www.spacechannel.com/data/uap-news.json \
  --feed https://www.spacechannel.com/data/maxq-podcast.json \
  --feed https://www.spacechannel.com/data/ufo-podcast.json \
  --feed https://www.spacechannel.com/data/becker-tour.json
.venv/bin/python scripts/build_knowledge.py spacechannel --data-dir /opt/mission-control/data
```
(Exact flags: check the scripts' argparse — adjust --out/--data-dir to match.)

## Start + smoke

```bash
cd /opt/mission-control/engine/deploy/spacechannel && docker compose up -d
curl -s http://127.0.0.1:8200/health          # pre-warms nothing; first /transcribe downloads whisper base
curl -s https://dev-mc.spacechannel.com/healthz
# Full protocol smoke (mints a token with the shared secret):
cd /opt/mission-control/engine && .venv/bin/python deploy/spacechannel/smoke.py wss://dev-mc.spacechannel.com/ws
```

## .env template (/opt/mission-control/.env)

```
ANTHROPIC_API_KEY=
MISSION_CONTROL_TOKEN_SECRET=        # same value as the auth-api Lambda
MISSION_CONTROL_ENV=dev
MISSION_CONTROL_ALLOWED_ORIGINS=https://dev.spacechannel.com,https://www.spacechannel.com,https://spacechannel.com
SPACECHANNEL_INGEST_URL=https://zi065h7oai.execute-api.us-east-1.amazonaws.com/api/mission-control/ingest
NANO_CLAW_DISABLE_TOOLS=1
NANO_CLAW_LOCKED=1
NANO_CLAW_BARGE_IN=0
NANO_CLAW_API_HOST=127.0.0.1
STT_SERVICE_URL=http://127.0.0.1:8200
NANO_CLAW_KNOWLEDGE=/app/sites/spacechannel/knowledge.md
MISSION_CONTROL_KNOWLEDGE_VERSION_FILE=/app/sites/spacechannel/knowledge-version.json
NANO_CLAW_STT_ALLOWED=tiny,base
# NANO_CLAW_ICE_SERVERS=stun:stun.l.google.com:19302   (milestone 3c)
```

## Deploys

```bash
cd /opt/mission-control/engine && git pull && cd deploy/spacechannel && docker compose build && docker compose up -d
```

## Notes / known limitations (dev)

- Single host, in-process session state: deploys interrupt active conversations
  (Neon keeps completed history; the client reconnects).
- Kokoro/LuxTTS not deployed — Piper voices only (CPU-fast). Voice arrives in
  milestone 3c with STUN enabled.
- Knowledge refresh timer: see refresh-knowledge.sh + systemd units (3d).

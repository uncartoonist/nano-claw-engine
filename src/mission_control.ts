// [sc] Space Channel locked-mode helpers for the public Mission Control
// deployment. When NANO_CLAW_LOCKED=1: clients cannot select models, and the
// API binds to localhost by default so only the co-located voice server (and
// nothing on the network) can reach it.

export function isLocked(): boolean {
  const v = process.env.NANO_CLAW_LOCKED ?? '0';
  return v !== '0' && v !== 'false' && v !== '';
}

export function apiHost(): string | undefined {
  return process.env.NANO_CLAW_API_HOST || undefined;
}

export function corsOrigin(): string {
  return process.env.NANO_CLAW_CORS_ORIGIN || '*';
}

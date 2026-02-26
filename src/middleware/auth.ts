import type { MiddlewareHandler } from 'hono';
import { getCookie } from 'hono/cookie';

const SESSION_COOKIE = 'asspp_session';
const SESSION_MAX_AGE = 7 * 24 * 60 * 60; // 7 days
const KV_PASSWORD_KEY = 'password_hash';
const POW_CHALLENGE_TTL = 60; // seconds

// ---------------------------------------------------------------------------
// Crypto helpers
// ---------------------------------------------------------------------------

function base64url(buf: ArrayBuffer | Uint8Array): string {
  const bytes = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  let binary = '';
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function base64urlToString(data: string): string {
  const bytes = base64urlDecode(data);
  return new TextDecoder().decode(bytes);
}

function base64urlDecode(str: string): Uint8Array {
  const padded = str.replace(/-/g, '+').replace(/_/g, '/');
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

const PBKDF2_ITERATIONS = 100_000;

export async function hashPassword(password: string): Promise<string> {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(password),
    'PBKDF2',
    false,
    ['deriveBits'],
  );
  const hash = await crypto.subtle.deriveBits(
    { name: 'PBKDF2', salt, iterations: PBKDF2_ITERATIONS, hash: 'SHA-256' },
    key,
    256,
  );
  return `${base64url(salt)}.${base64url(hash)}`;
}

async function deriveKey(passwordHash: string): Promise<CryptoKey> {
  const hashPart = passwordHash.includes('.') ? passwordHash.split('.')[1]! : passwordHash;
  return crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(hashPart),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign', 'verify'],
  );
}

export async function createToken(passwordHash: string): Promise<string> {
  const key = await deriveKey(passwordHash);
  const payload = JSON.stringify({
    exp: Math.floor(Date.now() / 1000) + SESSION_MAX_AGE,
  });
  const payloadB64 = base64url(new TextEncoder().encode(payload));
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(payloadB64));
  return `${payloadB64}.${base64url(sig)}`;
}

export async function verifyToken(token: string, passwordHash: string): Promise<boolean> {
  const parts = token.split('.');
  if (parts.length !== 2) return false;
  const [payloadB64, sigB64] = parts;
  if (!payloadB64 || !sigB64) return false;

  const key = await deriveKey(passwordHash);
  const valid = await crypto.subtle.verify(
    'HMAC',
    key,
    base64urlDecode(sigB64),
    new TextEncoder().encode(payloadB64),
  );
  if (!valid) return false;

  try {
    const payload = JSON.parse(base64urlToString(payloadB64)) as {
      exp: number;
    };
    return payload.exp > Math.floor(Date.now() / 1000);
  } catch {
    return false;
  }
}

/** Timing-safe comparison via HMAC */
async function timingSafeEqual(a: string, b: string): Promise<boolean> {
  const cmpKey = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode('asspp-compare'),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const sigA = new Uint8Array(
    await crypto.subtle.sign('HMAC', cmpKey, new TextEncoder().encode(a)),
  );
  const sigB = new Uint8Array(
    await crypto.subtle.sign('HMAC', cmpKey, new TextEncoder().encode(b)),
  );
  if (sigA.length !== sigB.length) return false;
  let result = 0;
  for (let i = 0; i < sigA.length; i++) result |= sigA[i]! ^ sigB[i]!;
  return result === 0;
}

/** Timing-safe password comparison — re-derive PBKDF2 with stored salt */
export async function verifyPassword(input: string, storedHash: string): Promise<boolean> {
  const dotIdx = storedHash.indexOf('.');
  if (dotIdx < 0) return false;

  const salt = base64urlDecode(storedHash.slice(0, dotIdx));
  const expectedHash = storedHash.slice(dotIdx + 1);

  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(input),
    'PBKDF2',
    false,
    ['deriveBits'],
  );
  const derived = await crypto.subtle.deriveBits(
    { name: 'PBKDF2', salt, iterations: PBKDF2_ITERATIONS, hash: 'SHA-256' },
    key,
    256,
  );
  const inputHash = base64url(derived);
  return timingSafeEqual(inputHash, expectedHash);
}

// ---------------------------------------------------------------------------
// Cookie helpers
// ---------------------------------------------------------------------------

export { SESSION_COOKIE, SESSION_MAX_AGE };

export function isLocalDev(url: string): boolean {
  try {
    return new URL(url).hostname === 'localhost';
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// KV password hash (with DO fallback for migration)
// ---------------------------------------------------------------------------

export async function getPasswordHash(env: Env): Promise<string | null> {
  let hash = await env.AUTH_KV.get(KV_PASSWORD_KEY);
  if (!hash) {
    // Fallback: migrate from DO storage (KV write failure is non-fatal)
    hash = await dm(env).getPasswordHash();
    if (hash)
      await env.AUTH_KV.put(KV_PASSWORD_KEY, hash).catch((e) => {
        console.error('Password hash KV migration failed:', e);
      });
  }
  return hash;
}

export async function setPasswordHash(env: Env, hash: string): Promise<void> {
  await env.AUTH_KV.put(KV_PASSWORD_KEY, hash);
}

// Note: KV check + put is not atomic (TOCTOU), but this is only called during
// first-time password setup — a rare, one-shot operation. The DO fallback check
// provides an additional layer, making a real race practically impossible.
export async function setPasswordHashIfNotExists(env: Env, hash: string): Promise<boolean> {
  const existing = await env.AUTH_KV.get(KV_PASSWORD_KEY);
  if (existing) return false;
  // Also check DO for migration case
  const doHash = await dm(env).getPasswordHash();
  if (doHash) {
    await env.AUTH_KV.put(KV_PASSWORD_KEY, doHash);
    return false;
  }
  await env.AUTH_KV.put(KV_PASSWORD_KEY, hash);
  return true;
}

// ---------------------------------------------------------------------------
// PoW (Proof of Work) — stateless challenge-response
// ---------------------------------------------------------------------------

/** Ephemeral HMAC key — lazily generated on first use, then cached.
 *  Not extractable; challenges become invalid after Worker restart.
 *  Cannot be top-level: Workers forbid crypto I/O at module init. */
let ephemeralPowKeyPromise: Promise<CryptoKey> | null = null;

/** In-memory set of used challenges to prevent replay within TTL window.
 *  Safe as module-level state: PoW is not request-scoped data — it's a
 *  global rate-limit mechanism, and stale entries are harmless. */
const usedChallenges = new Map<string, number>();
const MAX_USED_CHALLENGES = 10_000;

function powHmacKey(): Promise<CryptoKey> {
  if (!ephemeralPowKeyPromise) {
    ephemeralPowKeyPromise = crypto.subtle.generateKey({ name: 'HMAC', hash: 'SHA-256' }, false, [
      'sign',
      'verify',
    ]) as Promise<CryptoKey>;
  }
  return ephemeralPowKeyPromise;
}

/**
 * Generate a signed PoW challenge.
 * Format: `timestamp:random:signature`
 */
export async function generateChallenge(
  env: Env,
): Promise<{ challenge: string; difficulty: number }> {
  const difficulty = Math.min(24, Math.max(16, parseInt(env.POW_DIFFICULTY ?? '18', 10) || 18));
  const timestamp = Math.floor(Date.now() / 1000);
  const random = crypto.randomUUID();
  const payload = `${timestamp}:${random}`;

  const key = await powHmacKey();
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(payload));
  const challenge = `${payload}:${base64url(sig)}`;
  return { challenge, difficulty };
}

/**
 * Verify a PoW solution: check HMAC signature, timestamp freshness, and hash difficulty.
 */
export async function verifyPow(challenge: string, nonce: string, env: Env): Promise<boolean> {
  // Parse challenge: timestamp:random:signature
  const lastColon = challenge.lastIndexOf(':');
  if (lastColon < 0) return false;
  const payload = challenge.slice(0, lastColon);
  const sigB64 = challenge.slice(lastColon + 1);

  // Reject already-used challenges (one-time use, in-memory)
  if (usedChallenges.has(challenge)) return false;

  // Verify HMAC signature (prevent forged challenges)
  const key = await powHmacKey();
  const valid = await crypto.subtle.verify(
    'HMAC',
    key,
    base64urlDecode(sigB64),
    new TextEncoder().encode(payload),
  );
  if (!valid) return false;

  // Check timestamp freshness
  const firstColon = payload.indexOf(':');
  if (firstColon < 0) return false;
  const timestamp = parseInt(payload.slice(0, firstColon), 10);
  const now = Math.floor(Date.now() / 1000);
  if (now - timestamp > POW_CHALLENGE_TTL) return false;

  // Verify hash difficulty
  const difficulty = Math.min(24, Math.max(16, parseInt(env.POW_DIFFICULTY ?? '18', 10) || 18));
  const data = new TextEncoder().encode(challenge + nonce);
  const hash = new Uint8Array(await crypto.subtle.digest('SHA-256', data));
  if (!hasLeadingZeroBits(hash, difficulty)) return false;

  // Mark challenge as used; purge expired entries if map grows large
  const now2 = Math.floor(Date.now() / 1000);
  usedChallenges.set(challenge, now2);
  if (usedChallenges.size > MAX_USED_CHALLENGES) {
    for (const [c, ts] of usedChallenges) {
      if (now2 - ts > POW_CHALLENGE_TTL * 2) usedChallenges.delete(c);
    }
  }
  return true;
}

function hasLeadingZeroBits(hash: Uint8Array, bits: number): boolean {
  let remaining = bits;
  for (let i = 0; i < hash.length && remaining > 0; i++) {
    if (remaining >= 8) {
      if (hash[i] !== 0) return false;
      remaining -= 8;
    } else {
      const mask = 0xff << (8 - remaining);
      if ((hash[i]! & mask) !== 0) return false;
      remaining = 0;
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// DO stub helper
// ---------------------------------------------------------------------------

export function dm(env: Env) {
  return env.DOWNLOAD_MANAGER.get(env.DOWNLOAD_MANAGER.idFromName('singleton'));
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

export function authMiddleware(): MiddlewareHandler<{ Bindings: Env }> {
  return async (c, next) => {
    const passwordHash = await getPasswordHash(c.env);
    // No password set => fully open
    if (!passwordHash) return next();

    const cookie = getCookie(c, SESSION_COOKIE);
    if (cookie && (await verifyToken(cookie, passwordHash))) {
      return next();
    }

    return c.json({ error: 'Unauthorized' }, 401);
  };
}

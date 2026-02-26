import type { MiddlewareHandler } from 'hono';
import { getCookie } from 'hono/cookie';
import type { DownloadManager } from '../do/DownloadManager.js';

const SESSION_COOKIE = 'asspp_session';
const SESSION_MAX_AGE = 7 * 24 * 60 * 60; // 7 days

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
  // Use the hash portion (after the dot) as HMAC key material
  const hashPart = passwordHash.includes('.')
    ? passwordHash.split('.')[1]!
    : passwordHash;
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
  const sig = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(payloadB64),
  );
  return `${payloadB64}.${base64url(sig)}`;
}

export async function verifyToken(
  token: string,
  passwordHash: string,
): Promise<boolean> {
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

/** Timing-safe password comparison — re-derive PBKDF2 with stored salt */
export async function verifyPassword(
  input: string,
  storedHash: string,
): Promise<boolean> {
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

  // Timing-safe comparison via HMAC
  const cmpKey = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode('asspp-compare'),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const sigA = new Uint8Array(
    await crypto.subtle.sign('HMAC', cmpKey, new TextEncoder().encode(inputHash)),
  );
  const sigB = new Uint8Array(
    await crypto.subtle.sign('HMAC', cmpKey, new TextEncoder().encode(expectedHash)),
  );
  if (sigA.length !== sigB.length) return false;
  let result = 0;
  for (let i = 0; i < sigA.length; i++) result |= sigA[i]! ^ sigB[i]!;
  return result === 0;
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
// DO stub helper
// ---------------------------------------------------------------------------

export function dm(
  env: Env,
): DurableObjectStub & InstanceType<typeof DownloadManager> {
  return env.DOWNLOAD_MANAGER.get(
    env.DOWNLOAD_MANAGER.idFromName('singleton'),
  ) as unknown as DurableObjectStub & InstanceType<typeof DownloadManager>;
}

// ---------------------------------------------------------------------------
// Auth middleware
// ---------------------------------------------------------------------------

export function authMiddleware(): MiddlewareHandler<{ Bindings: Env }> {
  return async (c, next) => {
    const passwordHash = await dm(c.env).getPasswordHash();
    // No password set => fully open
    if (!passwordHash) return next();

    const cookie = getCookie(c, SESSION_COOKIE);
    if (cookie && (await verifyToken(cookie, passwordHash))) {
      return next();
    }

    return c.json({ error: 'Unauthorized' }, 401);
  };
}

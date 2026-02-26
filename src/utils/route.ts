import type { Context } from 'hono';
import { MIN_ACCOUNT_HASH_LENGTH } from '../config.js';

export function getIdParam(c: Context): string {
  return c.req.param('id');
}

export function requireAccountHash(c: Context): string | null {
  const hash = c.req.query('accountHash');
  if (!hash || hash.length < MIN_ACCOUNT_HASH_LENGTH) return null;
  return hash;
}

export function requireAccountHashOrBody(query: string | undefined, body: unknown): string | null {
  const hash =
    query ??
    (body && typeof body === 'object' && 'accountHash' in body
      ? String((body as Record<string, unknown>).accountHash)
      : undefined);
  if (!hash || hash.length < MIN_ACCOUNT_HASH_LENGTH) return null;
  return hash;
}

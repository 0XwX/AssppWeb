import { Hono } from 'hono';
import { setCookie, getCookie } from 'hono/cookie';
import {
  hashPassword,
  verifyPassword,
  createToken,
  verifyToken,
  dm,
  isLocalDev,
  SESSION_COOKIE,
  SESSION_MAX_AGE,
} from '../middleware/auth.js';

const auth = new Hono<{ Bindings: Env }>();

// GET /auth/status
auth.get('/auth/status', async (c) => {
  const passwordHash = await dm(c.env).getPasswordHash();

  if (!passwordHash) {
    return c.json({ required: false, setup: true, authenticated: false });
  }

  const cookie = getCookie(c, SESSION_COOKIE);
  const authenticated = cookie
    ? await verifyToken(cookie, passwordHash)
    : false;

  return c.json({ required: true, setup: false, authenticated });
});

// POST /auth/setup — first-time password setup
auth.post('/auth/setup', async (c) => {
  const body = await c.req.json<{ password: string }>();
  if (!body.password || body.password.length < 1) {
    return c.json({ error: 'Password required' }, 400);
  }

  const hash = await hashPassword(body.password);
  const ok = await dm(c.env).setPasswordHashIfNotExists(hash);
  if (!ok) {
    return c.json({ error: 'Password already set' }, 400);
  }

  const token = await createToken(hash);
  const local = isLocalDev(c.req.url);
  setCookie(c, SESSION_COOKIE, token, {
    httpOnly: true,
    secure: !local,
    sameSite: local ? 'Lax' : 'Strict',
    path: '/',
    maxAge: SESSION_MAX_AGE,
  });

  return c.json({ ok: true });
});

// POST /auth/login
auth.post('/auth/login', async (c) => {
  const passwordHash = await dm(c.env).getPasswordHash();
  if (!passwordHash) {
    return c.json({ error: 'Password not configured' }, 400);
  }

  const body = await c.req.json<{ password: string }>();
  if (!body.password) {
    return c.json({ error: 'Password required' }, 400);
  }

  const valid = await verifyPassword(body.password, passwordHash);
  if (!valid) {
    return c.json({ error: 'Invalid password' }, 401);
  }

  const token = await createToken(passwordHash);
  const local = isLocalDev(c.req.url);
  setCookie(c, SESSION_COOKIE, token, {
    httpOnly: true,
    secure: !local,
    sameSite: local ? 'Lax' : 'Strict',
    path: '/',
    maxAge: SESSION_MAX_AGE,
  });

  return c.json({ ok: true });
});

// POST /auth/logout
auth.post('/auth/logout', async (c) => {
  const local = isLocalDev(c.req.url);
  setCookie(c, SESSION_COOKIE, '', {
    httpOnly: true,
    secure: !local,
    sameSite: local ? 'Lax' : 'Strict',
    path: '/',
    maxAge: 0,
  });
  return c.json({ ok: true });
});

// POST /auth/change-password
auth.post('/auth/change-password', async (c) => {
  const passwordHash = await dm(c.env).getPasswordHash();
  if (!passwordHash) {
    return c.json({ error: 'Password not configured' }, 400);
  }

  // Verify current session
  const cookie = getCookie(c, SESSION_COOKIE);
  if (!cookie || !(await verifyToken(cookie, passwordHash))) {
    return c.json({ error: 'Unauthorized' }, 401);
  }

  const body = await c.req.json<{
    currentPassword: string;
    newPassword: string;
  }>();
  if (!body.currentPassword || !body.newPassword) {
    return c.json({ error: 'Both passwords required' }, 400);
  }

  const valid = await verifyPassword(body.currentPassword, passwordHash);
  if (!valid) {
    return c.json({ error: 'Current password incorrect' }, 401);
  }

  const newHash = await hashPassword(body.newPassword);
  await dm(c.env).setPasswordHash(newHash);

  // Issue new token with new password hash
  const token = await createToken(newHash);
  const local = isLocalDev(c.req.url);
  setCookie(c, SESSION_COOKIE, token, {
    httpOnly: true,
    secure: !local,
    sameSite: local ? 'Lax' : 'Strict',
    path: '/',
    maxAge: SESSION_MAX_AGE,
  });

  return c.json({ ok: true });
});

export default auth;

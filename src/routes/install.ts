import { Hono } from 'hono';
import { buildManifest, getWhitePng } from '../services/manifestBuilder.js';
import { dm } from '../middleware/auth.js';

const install = new Hono<{ Bindings: Env }>();

/** Derive base URL from request (matches install.ts logic in backend) */
function getBaseUrl(req: Request): string {
  const url = new URL(req.url);
  const proto = req.headers.get('x-forwarded-proto') ?? url.protocol.replace(':', '');
  const host = req.headers.get('host') ?? url.host;
  const sanitizedHost = host.replace(/[^\w.\-:]/g, '');

  if (!sanitizedHost.includes(':')) {
    const port = req.headers.get('x-forwarded-port')?.replace(/\D/g, '');
    const isDefault = (proto === 'https' && port === '443') || (proto === 'http' && port === '80');
    if (port && !isDefault) {
      return `${proto}://${sanitizedHost}:${port}`;
    }
  }
  return `${proto}://${sanitizedHost}`;
}

function joinUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, '')}/${path.replace(/^\/+/, '')}`;
}

// GET /install/:id/manifest.plist
install.get('/install/:id/manifest.plist', async (c) => {
  const id = c.req.param('id');
  // The manifest URL is unguessable (UUID), so no auth is needed.
  const task = await getTaskPublic(c.env, id);
  if (!task) return c.json({ error: 'Package not found' }, 404);

  const baseUrl = getBaseUrl(c.req.raw);
  const payloadUrl = joinUrl(baseUrl, `/api/install/${id}/payload.ipa`);
  const smallIconUrl = joinUrl(baseUrl, `/api/install/${id}/icon-small.png`);
  const largeIconUrl = joinUrl(baseUrl, `/api/install/${id}/icon-large.png`);

  const manifest = buildManifest(task.software, payloadUrl, smallIconUrl, largeIconUrl);

  return new Response(manifest, {
    headers: { 'Content-Type': 'application/xml' },
  });
});

// GET /install/:id/url?accountHash=...
install.get('/install/:id/url', async (c) => {
  const accountHash = c.req.query('accountHash');
  if (!accountHash) return c.json({ error: 'Missing accountHash' }, 400);

  const id = c.req.param('id');
  const task = await dm(c.env).getTask(id, accountHash);
  if (!task?.hasFile) return c.json({ error: 'Package not found' }, 404);

  const baseUrl = getBaseUrl(c.req.raw);
  const manifestUrl = joinUrl(baseUrl, `/api/install/${id}/manifest.plist`);
  const installUrl = `itms-services://?action=download-manifest&url=${encodeURIComponent(manifestUrl)}`;

  return c.json({ installUrl, manifestUrl });
});

// GET /install/:id/payload.ipa  (no auth — UUID is the secret)
install.get('/install/:id/payload.ipa', async (c) => {
  const id = c.req.param('id');
  const task = await getTaskPublic(c.env, id);
  if (!task?.hasFile) return c.json({ error: 'Package not found' }, 404);

  const r2key = await dm(c.env).getR2KeyPublic(id);
  if (!r2key) return c.json({ error: 'Package not found' }, 404);

  // CDN direct link: 302 redirect to R2 public bucket custom domain
  const cdnDomain = c.env.R2_CDN_DOMAIN;
  if (cdnDomain && /^[\w.-]+$/.test(cdnDomain)) {
    const encodedKey = r2key.split('/').map(encodeURIComponent).join('/');
    return c.redirect(`https://${cdnDomain}/${encodedKey}`, 302);
  }

  // Fallback: stream from R2 through Worker
  const obj = await c.env.IPA_BUCKET.get(r2key);
  if (!obj) return c.json({ error: 'Package not found' }, 404);

  return new Response(obj.body, {
    headers: {
      'Content-Type': 'application/octet-stream',
      'Content-Length': String(obj.size),
      ETag: obj.etag,
      'Last-Modified': obj.uploaded.toUTCString(),
      'Cache-Control': 'public, max-age=3600',
    },
  });
});

// GET /install/:id/icon-small.png
install.get('/install/:id/icon-small.png', (_c) => {
  const png = getWhitePng();
  return new Response(png, {
    headers: {
      'Content-Type': 'image/png',
      'Content-Length': String(png.length),
    },
  });
});

// GET /install/:id/icon-large.png
install.get('/install/:id/icon-large.png', (_c) => {
  const png = getWhitePng();
  return new Response(png, {
    headers: {
      'Content-Type': 'image/png',
      'Content-Length': String(png.length),
    },
  });
});

// ---------------------------------------------------------------------------
// Public task lookup (no accountHash required — UUID is the secret)
// ---------------------------------------------------------------------------

async function getTaskPublic(
  env: Env,
  id: string,
): Promise<{ software: Parameters<typeof buildManifest>[0]; hasFile: boolean } | null> {
  return dm(env).getTaskPublic(id);
}

export default install;

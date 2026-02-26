import { Hono } from 'hono';
import { MIN_ACCOUNT_HASH_LENGTH } from '../config.js';
import { dm } from '../middleware/auth.js';

const packages = new Hono<{ Bindings: Env }>();

function requireHash(hash: string | undefined): hash is string {
  return !!hash && hash.length >= MIN_ACCOUNT_HASH_LENGTH;
}

function sanitizeFilename(name: string): string {
  return name
    .replace(/[^\x20-\x7E]/g, '_')
    .replace(/["\\]/g, '_')
    .replace(/[\r\n]/g, '')
    .slice(0, 200);
}

// GET /packages?accountHashes=a,b,c
packages.get('/packages', async (c) => {
  const hashesParam = c.req.query('accountHashes');
  if (!hashesParam) return c.json([]);

  const hashes = hashesParam
    .split(',')
    .map((h) => h.trim())
    .filter((h) => requireHash(h));

  if (hashes.length === 0) return c.json([]);

  const pkgs = await dm(c.env).listPackages(hashes);
  const result = [];

  for (const pkg of pkgs) {
    const meta = await c.env.IPA_BUCKET.head(pkg.r2key);
    if (!meta) continue;
    result.push({
      id: pkg.id,
      software: pkg.software,
      accountHash: pkg.accountHash,
      fileSize: meta.size,
      createdAt: pkg.createdAt,
    });
  }

  return c.json(result);
});

// GET /packages/:id/file?accountHash=...
packages.get('/packages/:id/file', async (c) => {
  const accountHash = c.req.query('accountHash');
  if (!requireHash(accountHash)) {
    return c.json({ error: 'Missing or invalid accountHash' }, 400);
  }

  const id = c.req.param('id');
  const r2key = await dm(c.env).getR2Key(id, accountHash);
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

  const task = await dm(c.env).getTask(id, accountHash);
  const name = task?.software.name ?? 'app';
  const version = task?.software.version ?? '1.0';
  let safeName = sanitizeFilename(name);
  // If name is all non-ASCII (e.g. Chinese), sanitize produces only underscores — fallback to bundleID
  if (!safeName.replace(/_/g, '').trim()) {
    safeName = sanitizeFilename(task?.software.bundleID ?? 'app');
  }
  const safeVersion = sanitizeFilename(version);
  const fileName = `${safeName}_${safeVersion}.ipa`;

  return new Response(obj.body, {
    headers: {
      'Content-Type': 'application/octet-stream',
      'Content-Disposition': `attachment; filename="${fileName}"`,
      'Content-Length': String(obj.size),
    },
  });
});

// DELETE /packages/:id?accountHash=...
packages.delete('/packages/:id', async (c) => {
  const accountHash = c.req.query('accountHash');
  if (!requireHash(accountHash)) {
    return c.json({ error: 'Missing or invalid accountHash' }, 400);
  }

  const id = c.req.param('id');
  const ok = await dm(c.env).deleteTask(id, accountHash);
  if (!ok) return c.json({ error: 'Package not found' }, 404);

  return c.json({ success: true });
});

export default packages;

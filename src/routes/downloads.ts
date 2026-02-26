import { Hono } from 'hono';
import { MIN_ACCOUNT_HASH_LENGTH } from '../config.js';
import { dm } from '../middleware/auth.js';
import type { Software, Sinf } from '../types.js';

const downloads = new Hono<{ Bindings: Env }>();

function requireHash(hash: string | undefined): hash is string {
  return !!hash && hash.length >= MIN_ACCOUNT_HASH_LENGTH;
}

// POST /downloads — create a new download task
downloads.post('/downloads', async (c) => {
  let body: {
    software: Software;
    accountHash: string;
    downloadURL: string;
    sinfs: Sinf[];
    iTunesMetadata?: string;
  };
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: 'Invalid JSON body' }, 400);
  }

  const { software, accountHash, downloadURL, sinfs, iTunesMetadata } = body;

  if (!software || !accountHash || !downloadURL || !sinfs) {
    return c.json(
      { error: 'Missing required fields: software, accountHash, downloadURL, sinfs' },
      400,
    );
  }

  try {
    const task = await dm(c.env).createTask({
      software,
      accountHash,
      downloadURL,
      sinfs,
      iTunesMetadata,
    });
    return c.json(task, 201);
  } catch (err) {
    return c.json({ error: err instanceof Error ? err.message : 'Failed to create download' }, 400);
  }
});

// GET /downloads?accountHashes=a,b,c
downloads.get('/downloads', async (c) => {
  const hashesParam = c.req.query('accountHashes');
  if (!hashesParam) return c.json([]);

  const hashes = hashesParam
    .split(',')
    .map((h) => h.trim())
    .filter((h) => requireHash(h));

  if (hashes.length === 0) return c.json([]);

  const tasks = await dm(c.env).listTasks(hashes);
  return c.json(tasks);
});

// GET /downloads/:id?accountHash=...
downloads.get('/downloads/:id', async (c) => {
  const accountHash = c.req.query('accountHash');
  if (!requireHash(accountHash)) {
    return c.json({ error: 'Missing or invalid accountHash' }, 400);
  }

  const id = c.req.param('id');
  const task = await dm(c.env).getTask(id, accountHash);
  if (!task) return c.json({ error: 'Download not found' }, 404);

  return c.json(task);
});

// POST /downloads/:id/pause
downloads.post('/downloads/:id/pause', async (c) => {
  const accountHash = c.req.query('accountHash');
  if (!requireHash(accountHash)) {
    return c.json({ error: 'Missing or invalid accountHash' }, 400);
  }

  const id = c.req.param('id');
  const ok = await dm(c.env).pauseTask(id, accountHash);
  if (!ok) return c.json({ error: 'Cannot pause this download' }, 400);

  const updated = await dm(c.env).getTask(id, accountHash);
  return c.json(updated ?? { success: true });
});

// POST /downloads/:id/resume
downloads.post('/downloads/:id/resume', async (c) => {
  const accountHash = c.req.query('accountHash');
  if (!requireHash(accountHash)) {
    return c.json({ error: 'Missing or invalid accountHash' }, 400);
  }

  const id = c.req.param('id');
  const ok = await dm(c.env).resumeTask(id, accountHash);
  if (!ok) return c.json({ error: 'Cannot resume this download' }, 400);

  const updated = await dm(c.env).getTask(id, accountHash);
  return c.json(updated ?? { success: true });
});

// DELETE /downloads/:id
downloads.delete('/downloads/:id', async (c) => {
  const accountHash = c.req.query('accountHash');
  if (!requireHash(accountHash)) {
    return c.json({ error: 'Missing or invalid accountHash' }, 400);
  }

  const id = c.req.param('id');
  const ok = await dm(c.env).deleteTask(id, accountHash);
  if (!ok) return c.json({ error: 'Download not found' }, 404);

  return c.json({ success: true });
});

export default downloads;

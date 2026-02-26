import { Hono } from 'hono';
import { dm } from '../middleware/auth.js';

const settings = new Hono<{ Bindings: Env }>();

settings.get('/settings', async (c) => {
  try {
    const config = await dm(c.env).getConfig();
    const envDays = parseInt(c.env.AUTO_CLEANUP_DAYS ?? '0', 10) || 0;
    const envMaxMB = parseInt(c.env.AUTO_CLEANUP_MAX_MB ?? '0', 10) || 0;

    // Scan R2 for storage stats
    let storageSizeBytes = 0;
    let storageFileCount = 0;
    let cursor: string | undefined;
    do {
      const listed = await c.env.IPA_BUCKET.list({ cursor, limit: 500 });
      for (const obj of listed.objects) {
        storageSizeBytes += obj.size;
        storageFileCount++;
      }
      cursor = listed.truncated ? listed.cursor : undefined;
    } while (cursor);

    return c.json({
      buildCommit: c.env.BUILD_COMMIT ?? 'unknown',
      buildDate: c.env.BUILD_DATE ?? 'unknown',
      autoCleanupDays: config.autoCleanupDays ?? envDays,
      autoCleanupMaxMB: config.autoCleanupMaxMB ?? envMaxMB,
      storageSizeMB: Math.round((storageSizeBytes / (1024 * 1024)) * 100) / 100,
      storageFileCount,
      r2CdnDomain: c.env.R2_CDN_DOMAIN || undefined,
    });
  } catch (err) {
    console.error('Settings fetch failed:', err instanceof Error ? err.message : err);
    return c.json({ error: 'Failed to load settings' }, 500);
  }
});

settings.put('/settings', async (c) => {
  let body: { autoCleanupDays?: number; autoCleanupMaxMB?: number };
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: 'Invalid JSON body' }, 400);
  }

  try {
    const update: { autoCleanupDays?: number; autoCleanupMaxMB?: number } = {};

    if (body.autoCleanupDays !== undefined) {
      const v = Math.max(0, Math.floor(body.autoCleanupDays));
      if (Number.isFinite(v)) update.autoCleanupDays = v;
    }
    if (body.autoCleanupMaxMB !== undefined) {
      const v = Math.max(0, Math.floor(body.autoCleanupMaxMB));
      if (Number.isFinite(v)) update.autoCleanupMaxMB = v;
    }

    await dm(c.env).setConfig(update);

    // Return updated values
    const config = await dm(c.env).getConfig();
    const envDays = parseInt(c.env.AUTO_CLEANUP_DAYS ?? '0', 10) || 0;
    const envMaxMB = parseInt(c.env.AUTO_CLEANUP_MAX_MB ?? '0', 10) || 0;

    return c.json({
      autoCleanupDays: config.autoCleanupDays ?? envDays,
      autoCleanupMaxMB: config.autoCleanupMaxMB ?? envMaxMB,
    });
  } catch (err) {
    console.error('Settings update failed:', err instanceof Error ? err.message : err);
    return c.json({ error: 'Failed to update settings' }, 500);
  }
});

export default settings;

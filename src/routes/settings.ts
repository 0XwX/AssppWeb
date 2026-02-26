import { Hono } from 'hono';
import { dm } from '../middleware/auth.js';

const settings = new Hono<{ Bindings: Env }>();

settings.get('/settings', async (c) => {
  const config = await dm(c.env).getConfig();
  const envDays = parseInt(c.env.AUTO_CLEANUP_DAYS ?? '0', 10) || 0;
  const envMaxMB = parseInt(c.env.AUTO_CLEANUP_MAX_MB ?? '0', 10) || 0;

  return c.json({
    buildCommit: c.env.BUILD_COMMIT ?? 'unknown',
    buildDate: c.env.BUILD_DATE ?? 'unknown',
    autoCleanupDays: config.autoCleanupDays ?? envDays,
    autoCleanupMaxMB: config.autoCleanupMaxMB ?? envMaxMB,
  });
});

settings.put('/settings', async (c) => {
  const body = await c.req.json<{
    autoCleanupDays?: number;
    autoCleanupMaxMB?: number;
  }>();

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
});

export default settings;

import { Hono } from 'hono';
import { WispProxy } from './do/WispProxy.js';
import { DownloadManager } from './do/DownloadManager.js';
import { authMiddleware, dm } from './middleware/auth.js';
import authRouter from './routes/auth.js';
import bagRouter from './routes/bag.js';
import searchRouter from './routes/search.js';
import downloadsRouter from './routes/downloads.js';
import packagesRouter from './routes/packages.js';
import installRouter from './routes/install.js';
import settingsRouter from './routes/settings.js';

export { WispProxy, DownloadManager };

const app = new Hono<{ Bindings: Env }>();

// HTTPS redirect (skip in local dev where x-forwarded-proto is absent)
app.use('*', async (c, next) => {
  if (c.req.header('x-forwarded-proto') === 'http') {
    const url = new URL(c.req.url);
    url.protocol = 'https:';
    return c.redirect(url.toString(), 301);
  }
  return next();
});

// Auth middleware — protect /api/* and /wisp/*
// Skip: /api/auth/* (login/setup endpoints), /api/install/* (iOS itms-services)
app.use('/api/*', async (c, next) => {
  const path = new URL(c.req.url).pathname;
  if (path.startsWith('/api/auth/') || path.startsWith('/api/install/')) {
    return next();
  }
  return authMiddleware()(c, next);
});

app.use('/wisp/*', authMiddleware());

// Wisp WebSocket proxy — each session gets its own DO instance
app.all('/wisp/*', async (c) => {
  const id = c.env.WISP_PROXY.newUniqueId();
  const stub = c.env.WISP_PROXY.get(id);
  return stub.fetch(c.req.raw);
});

// API routes
app.route('/api', authRouter);
app.route('/api', bagRouter);
app.route('/api', searchRouter);
app.route('/api', downloadsRouter);
app.route('/api', packagesRouter);
app.route('/api', installRouter);
app.route('/api', settingsRouter);

// Frontend static assets (Workers Assets binding)
// SPA fallback: if the asset doesn't exist, serve index.html
app.get('*', async (c) => {
  const res = await c.env.ASSETS.fetch(c.req.raw);
  if (res.status === 404) {
    const url = new URL('/', c.req.url);
    return c.env.ASSETS.fetch(new Request(url, c.req.raw));
  }
  return res;
});

// ---------------------------------------------------------------------------
// Scheduled handler — R2 cleanup cron
// ---------------------------------------------------------------------------

async function scheduled(_event: ScheduledEvent, env: Env, _ctx: ExecutionContext): Promise<void> {
  try {
    const stub = dm(env);

    // DO config overrides env vars
    const config = await stub.getConfig();
    const envDays = parseInt(env.AUTO_CLEANUP_DAYS || '0');
    const envMaxMB = parseInt(env.AUTO_CLEANUP_MAX_MB || '0');
    const days = config.autoCleanupDays ?? envDays;
    const maxMB = config.autoCleanupMaxMB ?? envMaxMB;

    // Always run cleanup — even if days/maxMB are 0, orphaned R2 files need purging
    const result = await stub.cleanupExpired(days, maxMB);
    console.log('Cleanup result:', JSON.stringify(result));
  } catch (err) {
    console.error('Scheduled cleanup failed:', err instanceof Error ? err.message : err);
  }
}

export default { fetch: app.fetch, scheduled };

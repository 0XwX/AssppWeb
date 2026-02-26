import { Hono } from 'hono';
import { BAG_TIMEOUT_MS, BAG_MAX_BYTES, BAG_USER_AGENT } from '../config.js';

const bag = new Hono<{ Bindings: Env }>();

bag.get('/bag', async (c) => {
  const guid = c.req.query('guid');
  if (!guid) return c.json({ error: 'Missing guid parameter' }, 400);
  if (!/^[a-fA-F0-9]+$/.test(guid)) return c.json({ error: 'Invalid guid format' }, 400);

  const url = `https://init.itunes.apple.com/bag.xml?guid=${encodeURIComponent(guid)}`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), BAG_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        'User-Agent': BAG_USER_AGENT,
        Accept: 'application/xml',
      },
    });

    clearTimeout(timer);

    if (!response.ok) {
      return c.json({ error: `Bag upstream returned HTTP ${response.status}` }, 502);
    }

    // Read with size limit
    const reader = response.body?.getReader();
    if (!reader) return c.json({ error: 'No response body' }, 502);

    const chunks: Uint8Array[] = [];
    let totalBytes = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.length;
      if (totalBytes > BAG_MAX_BYTES) {
        reader.cancel();
        return c.json({ error: 'Bag response too large' }, 502);
      }
      chunks.push(value);
    }

    const body = new TextDecoder().decode(
      chunks.reduce((acc, chunk) => {
        const merged = new Uint8Array(acc.length + chunk.length);
        merged.set(acc);
        merged.set(chunk, acc.length);
        return merged;
      }, new Uint8Array(0)),
    );

    const plistMatch = body.match(/<plist[\s\S]*<\/plist>/);
    if (!plistMatch) return c.json({ error: 'No plist found in bag response' }, 502);

    return new Response(plistMatch[0], {
      headers: { 'Content-Type': 'text/xml' },
    });
  } catch (err) {
    clearTimeout(timer);
    if (err instanceof Error && err.name === 'AbortError') {
      return c.json({ error: 'Bag request timed out' }, 502);
    }
    console.error('Bag proxy error:', err instanceof Error ? err.message : err);
    return c.json({ error: 'Bag request failed' }, 502);
  }
});

export default bag;

import { Hono } from 'hono';
import { MAX_SEARCH_BYTES, TOP_CHARTS_MAX_BYTES } from '../config.js';

const search = new Hono<{ Bindings: Env }>();

/** Read response body as text with a size limit. Throws if exceeded. */
async function readWithLimit(response: Response, maxBytes: number): Promise<string> {
  const reader = response.body?.getReader();
  if (!reader) throw new Error('No response body');

  const chunks: Uint8Array[] = [];
  let totalBytes = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    totalBytes += value.length;
    if (totalBytes > maxBytes) {
      reader.cancel();
      throw new Error('Response too large');
    }
    chunks.push(value);
  }

  const merged = new Uint8Array(totalBytes);
  let pos = 0;
  for (const chunk of chunks) {
    merged.set(chunk, pos);
    pos += chunk.length;
  }
  return new TextDecoder().decode(merged);
}

function mapSoftware(item: Record<string, unknown>) {
  return {
    id: item['trackId'],
    bundleID: item['bundleId'],
    name: item['trackName'],
    version: item['version'],
    price: item['price'],
    artistName: item['artistName'],
    sellerName: item['sellerName'],
    description: item['description'],
    averageUserRating: item['averageUserRating'],
    userRatingCount: item['userRatingCount'],
    artworkUrl: item['artworkUrl512'],
    screenshotUrls: (item['screenshotUrls'] as string[]) ?? [],
    minimumOsVersion: item['minimumOsVersion'],
    fileSizeBytes: item['fileSizeBytes'],
    releaseDate: item['currentVersionReleaseDate'] ?? item['releaseDate'],
    releaseNotes: item['releaseNotes'],
    formattedPrice: item['formattedPrice'],
    primaryGenreName: item['primaryGenreName'],
  };
}

search.get('/search', async (c) => {
  try {
    const params = new URL(c.req.url).searchParams;
    const response = await fetch(`https://itunes.apple.com/search?${params.toString()}`);
    const text = await readWithLimit(response, MAX_SEARCH_BYTES);
    const data = JSON.parse(text) as { results?: Record<string, unknown>[] };
    return c.json((data.results ?? []).map(mapSoftware));
  } catch (err) {
    console.error('Search error:', err instanceof Error ? err.message : err);
    return c.json({ error: 'Search request failed' }, 500);
  }
});

search.get('/lookup', async (c) => {
  try {
    const params = new URL(c.req.url).searchParams;
    const response = await fetch(`https://itunes.apple.com/lookup?${params.toString()}`);
    const text = await readWithLimit(response, MAX_SEARCH_BYTES);
    const data = JSON.parse(text) as {
      resultCount?: number;
      results?: Record<string, unknown>[];
    };
    if (!data.resultCount || !data.results?.length) return c.json(null);
    return c.json(mapSoftware(data.results[0]!));
  } catch (err) {
    console.error('Lookup error:', err instanceof Error ? err.message : err);
    return c.json({ error: 'Lookup request failed' }, 500);
  }
});

const VALID_CHARTS = ['top-free'];

search.get('/top-charts', async (c) => {
  try {
    const country = c.req.query('country') || 'us';
    const chart = c.req.query('chart') || 'top-free';
    if (!/^[a-z]{2}$/i.test(country)) {
      return c.json({ error: 'Invalid country code' }, 400);
    }
    if (!VALID_CHARTS.includes(chart)) {
      return c.json({ error: 'Invalid chart type' }, 400);
    }

    // Step 1: Fetch chart IDs from Apple Marketing Tools
    const cc = country.toLowerCase();
    const chartUrl = `https://rss.marketingtools.apple.com/api/v2/${cc}/apps/${chart}/100/apps.json`;
    const chartResp = await fetch(chartUrl);
    if (!chartResp.ok) {
      return c.json({ error: 'Failed to fetch charts' }, 502);
    }
    const chartText = await readWithLimit(chartResp, TOP_CHARTS_MAX_BYTES);
    const chartData = JSON.parse(chartText) as {
      feed?: { results?: { id: string }[] };
    };
    const chartResults = chartData.feed?.results ?? [];
    if (chartResults.length === 0) return c.json([]);

    // Step 2: Batch lookup via iTunes API (supports comma-separated IDs)
    const ids = chartResults.map((r) => r.id).join(',');
    const lookupResp = await fetch(`https://itunes.apple.com/lookup?id=${ids}&country=${cc}`);
    if (!lookupResp.ok) {
      return c.json({ error: 'Failed to fetch app details' }, 502);
    }
    const lookupText = await readWithLimit(lookupResp, MAX_SEARCH_BYTES);
    const lookupData = JSON.parse(lookupText) as {
      results?: Record<string, unknown>[];
    };

    // Step 3: Map and preserve chart ranking order
    const mapped = new Map<number, ReturnType<typeof mapSoftware>>();
    for (const item of lookupData.results ?? []) {
      const sw = mapSoftware(item);
      if (sw.id != null) mapped.set(Number(sw.id), sw);
    }
    const ordered = chartResults.map((r) => mapped.get(Number(r.id))).filter(Boolean);

    return c.json(ordered);
  } catch (err) {
    console.error('Top charts error:', err instanceof Error ? err.message : err);
    return c.json({ error: 'Top charts request failed' }, 500);
  }
});

export default search;

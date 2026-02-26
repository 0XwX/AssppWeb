import { Hono } from 'hono';
import { MAX_SEARCH_BYTES } from '../config.js';

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

export default search;

import type { PlistDict } from './plist';
import { parsePlist } from './plist';
import { check401 } from '../api/client';

export interface BagOutput {
  authURL: string;
}

export const defaultAuthURL =
  'https://buy.itunes.apple.com/WebObjects/MZFinance.woa/wa/authenticate';

// Fetches the bag via the backend proxy.
// The backend fetches it using Node.js native HTTPS.
// The bag response is public data (Apple service URLs, no credentials).
export async function fetchBag(deviceId: string): Promise<BagOutput> {
  try {
    const resp = await fetch(`/api/bag?guid=${encodeURIComponent(deviceId)}`);
    if (!resp.ok) {
      check401(resp);
      const err = await resp.json().catch(() => ({ error: resp.statusText }));
      console.warn(
        `[Bag] Proxy request failed, using default auth endpoint: ${err.error || `HTTP ${resp.status}`}`,
      );
      return { authURL: defaultAuthURL };
    }

    const xml = await resp.text();
    const dict = parsePlist(xml) as PlistDict;

    // authenticateAccount may be at top level or inside a urlBag dict
    let authURL: string | undefined;
    const urlBag = dict.urlBag as PlistDict | undefined;
    if (urlBag) {
      authURL = urlBag.authenticateAccount as string | undefined;
    }
    if (!authURL) {
      authURL = dict.authenticateAccount as string | undefined;
    }

    if (!authURL) {
      console.warn('[Bag] authenticateAccount URL not found in bag, using default auth endpoint');
      return { authURL: defaultAuthURL };
    }

    return { authURL };
  } catch (error) {
    console.warn(
      `[Bag] Failed to fetch/parse bag, using default auth endpoint: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
    return { authURL: defaultAuthURL };
  }
}

import { apiGet } from './client';
import type { Software } from '../types';

export async function searchApps(
  term: string,
  country: string,
  entity: string,
  limit: number = 25,
): Promise<Software[]> {
  const params = new URLSearchParams({
    term,
    country,
    entity: entity === 'iPad' ? 'iPadSoftware' : 'software',
    limit: String(limit),
  });
  return apiGet<Software[]>(`/api/search?${params}`);
}

export async function lookupApp(bundleId: string, country: string): Promise<Software | null> {
  const params = new URLSearchParams({ bundleId, country });
  return apiGet<Software | null>(`/api/lookup?${params}`);
}

export async function lookupById(id: string, country: string): Promise<Software | null> {
  const params = new URLSearchParams({ id, country });
  return apiGet<Software | null>(`/api/lookup?${params}`);
}

export async function fetchTopCharts(
  country: string,
  chart: 'top-free' | 'top-paid' = 'top-free',
): Promise<Software[]> {
  const params = new URLSearchParams({ country, chart });
  return apiGet<Software[]>(`/api/top-charts?${params}`);
}

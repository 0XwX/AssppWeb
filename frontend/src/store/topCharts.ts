import { create } from 'zustand';
import { fetchTopCharts } from '../api/search';
import type { Software } from '../types';

interface TopChartsState {
  results: Software[];
  loading: boolean;
  error: string | null;
  fetched: boolean;
  lastCountry: string;
  fetch: (country: string, chart: 'top-free' | 'top-paid') => Promise<void>;
}

export const useTopCharts = create<TopChartsState>((set, get) => ({
  results: [],
  loading: false,
  error: null,
  fetched: false,
  lastCountry: '',
  fetch: async (country, chart) => {
    if (get().loading) return;
    if (country === get().lastCountry && get().results.length > 0) return;
    set({ loading: true, error: null });
    try {
      const results = await fetchTopCharts(country, chart);
      set({ results, fetched: true, lastCountry: country });
    } catch (e) {
      set({
        error: e instanceof Error ? e.message : 'Failed to fetch charts',
        results: [],
        fetched: true,
      });
    } finally {
      set({ loading: false });
    }
  },
}));

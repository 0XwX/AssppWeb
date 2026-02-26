import { create } from 'zustand';
import { fetchAndSolveChallenge } from '../utils/pow';

interface AuthState {
  /** null = checking, true = authenticated or no auth needed, false = need login */
  authenticated: boolean | null;
  /** Whether password auth is required */
  required: boolean;
  /** Whether first-time password setup is needed */
  setup: boolean;
  /** Whether PoW is being solved */
  solving: boolean;

  checkAuth: () => Promise<void>;
  login: (password: string) => Promise<{ ok: boolean; error?: string }>;
  setupPassword: (password: string) => Promise<{ ok: boolean; error?: string }>;
  logout: () => Promise<void>;
  changePassword: (
    currentPassword: string,
    newPassword: string,
  ) => Promise<{ ok: boolean; error?: string }>;
}

export const useAuthStore = create<AuthState>((set) => ({
  authenticated: null,
  required: false,
  setup: false,
  solving: false,

  checkAuth: async () => {
    try {
      const res = await fetch('/api/auth/status');
      if (res.ok) {
        const data = (await res.json()) as {
          required: boolean;
          setup: boolean;
          authenticated: boolean;
        };
        set({
          required: data.required,
          setup: data.setup,
          authenticated: data.authenticated,
        });
      } else {
        set({ authenticated: false, required: true, setup: false });
      }
    } catch {
      set({ authenticated: false, required: true, setup: false });
    }
  },

  login: async (password: string) => {
    set({ solving: true });
    try {
      const { challenge, nonce } = await fetchAndSolveChallenge();
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password, challenge, nonce }),
      });
      if (res.ok) {
        set({ authenticated: true });
        return { ok: true };
      }
      return { ok: false, error: 'invalid' };
    } catch {
      return { ok: false, error: 'network' };
    } finally {
      set({ solving: false });
    }
  },

  setupPassword: async (password: string) => {
    set({ solving: true });
    try {
      const { challenge, nonce } = await fetchAndSolveChallenge();
      const res = await fetch('/api/auth/setup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password, challenge, nonce }),
      });
      if (res.ok) {
        set({ authenticated: true, required: true, setup: false });
        return { ok: true };
      }
      return { ok: false, error: 'failed' };
    } catch {
      return { ok: false, error: 'network' };
    } finally {
      set({ solving: false });
    }
  },

  logout: async () => {
    await fetch('/api/auth/logout', { method: 'POST' }).catch(() => {});
    set({ authenticated: false });
  },

  changePassword: async (currentPassword: string, newPassword: string) => {
    set({ solving: true });
    try {
      const { challenge, nonce } = await fetchAndSolveChallenge();
      const res = await fetch('/api/auth/change-password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ currentPassword, newPassword, challenge, nonce }),
      });
      if (res.ok) return { ok: true };
      if (res.status === 401) return { ok: false, error: 'incorrect' };
      return { ok: false, error: 'failed' };
    } catch {
      return { ok: false, error: 'network' };
    } finally {
      set({ solving: false });
    }
  },
}));

import { useAuthStore } from '../store/auth';

const BASE_URL = '';

export function check401(res: Response): Response {
  if (res.status === 401) {
    useAuthStore
      .getState()
      .checkAuth()
      .catch(() => {});
  }
  return res;
}

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    check401(res);
    throw new Error(await res.text());
  }
  return res.json();
}

export async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    check401(res);
    throw new Error(await res.text());
  }
  return res.json();
}

export async function apiDelete(path: string): Promise<void> {
  const res = await fetch(`${BASE_URL}${path}`, { method: 'DELETE' });
  if (!res.ok) {
    check401(res);
    throw new Error(await res.text());
  }
}

import { ApiError } from '../api/errors';
import type { AuthSession } from '../types';

export interface SessionPayload {
  user: AuthSession;
  csrf_token: string;
  expires_at: string;
}

let csrfToken: string | null = null;
let refreshing: Promise<SessionPayload> | undefined;
const listeners = new Set<(session: SessionPayload | null) => void>();

export function subscribeSession(listener: (session: SessionPayload | null) => void): () => void {
  listeners.add(listener);
  return () => { listeners.delete(listener); };
}

function clearSession() {
  csrfToken = null;
  listeners.forEach(listener => listener(null));
}

async function authFetch(path: string, init: RequestInit = {}): Promise<Response> {
  try {
    return await fetch(`/auth/${path}`, {
      ...init, credentials: 'same-origin', cache: 'no-store',
      headers: { Accept: 'application/json', ...init.headers },
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error;
    throw new ApiError(503, 'Layanan Auth belum dapat dihubungi. Silakan coba lagi.');
  }
}

async function readBody(response: Response): Promise<Record<string, unknown>> {
  try {
    const body: unknown = await response.json();
    return body && typeof body === 'object' ? body as Record<string, unknown> : {};
  } catch { return {}; }
}

function sessionFrom(body: Record<string, unknown>): SessionPayload {
  const user = body.user as Partial<AuthSession> | undefined;
  if (!user || typeof user.id !== 'string' || typeof user.name !== 'string' || user.mode !== 'oidc' ||
      !['MEDIA', 'FIELD_TEAM', 'BNPB_INTERNAL'].includes(user.role ?? '') ||
      !Array.isArray(user.scopes) || !user.scopes.every(scope => typeof scope === 'string') ||
      typeof body.csrf_token !== 'string' || !body.csrf_token ||
      typeof body.expires_at !== 'string' || !Number.isFinite(Date.parse(body.expires_at))) {
    throw new ApiError(502, 'Format sesi Auth tidak valid.');
  }
  const result: SessionPayload = {
    user: { id: user.id, name: user.name, role: user.role!, scopes: [...user.scopes], mode: 'oidc' },
    csrf_token: body.csrf_token, expires_at: body.expires_at,
  };
  csrfToken = result.csrf_token;
  listeners.forEach(listener => listener(result));
  return result;
}

function authError(response: Response, body: Record<string, unknown>): ApiError {
  return new ApiError(response.status, typeof body.detail === 'string' ? body.detail :
    response.status === 401 ? 'Sesi telah berakhir. Silakan masuk kembali.' : 'Layanan Auth belum dapat memproses permintaan.');
}

/** Only profile, CSRF nonce and expiry are readable here; access/refresh tokens stay HttpOnly. */
export async function getSession(signal?: AbortSignal): Promise<SessionPayload | null> {
  const response = await authFetch('session', { signal });
  const body = await readBody(response);
  if (response.ok) return sessionFrom(body);
  if (response.status === 401) {
    if (body.refresh_required === true && typeof body.csrf_token === 'string' && body.csrf_token) {
      csrfToken = body.csrf_token;
      try { return await refreshSession(); }
      catch (error) { if (error instanceof ApiError && error.status === 401) return null; throw error; }
    }
    clearSession();
    return null;
  }
  throw authError(response, body);
}

/** CSRF nonce for API mutations; Envoy rejects cookie-authenticated POSTs without it. */
export async function getCsrfToken(): Promise<string> {
  if (!csrfToken) await getSession();
  if (!csrfToken) throw new ApiError(401, 'Sesi telah berakhir. Silakan masuk kembali.');
  return csrfToken;
}

/** Share one refresh across table, detail, status and SSE requests. Never replay a refresh token in parallel. */
export function refreshSession(): Promise<SessionPayload> {
  if (refreshing) return refreshing;
  refreshing = (async () => {
    try {
      if (!csrfToken) {
        const response = await authFetch('session');
        const body = await readBody(response);
        if (response.ok) sessionFrom(body);
        else if (response.status === 401 && body.refresh_required === true && typeof body.csrf_token === 'string' && body.csrf_token) csrfToken = body.csrf_token;
        else throw authError(response, body);
      }
      const response = await authFetch('refresh', { method: 'POST', headers: { 'X-CSRF-Token': csrfToken! } });
      const body = await readBody(response);
      if (!response.ok) throw authError(response, body);
      return sessionFrom(body);
    } catch (error) {
      clearSession();
      throw error;
    }
  })().finally(() => { refreshing = undefined; });
  return refreshing;
}

export async function logoutSession(): Promise<string> {
  // Obtain a fresh CSRF nonce if the page opened before a session was available.
  if (!csrfToken) {
    const session = await getSession();
    if (!session) return '/#/login';
  }
  if (refreshing) await refreshing;
  const response = await authFetch('logout', { method: 'POST', headers: { 'X-CSRF-Token': csrfToken! } });
  const body = await readBody(response);
  if (!response.ok) throw authError(response, body);
  if (typeof body.logout_url !== 'string') throw new ApiError(502, 'Alamat logout Auth tidak valid.');
  const url = new URL(body.logout_url, window.location.origin);
  if (!['http:', 'https:'].includes(url.protocol)) throw new ApiError(502, 'Alamat logout Auth tidak valid.');
  clearSession();
  return url.href;
}

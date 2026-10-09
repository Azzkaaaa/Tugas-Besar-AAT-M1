import type { Notification, Severity } from '../types';

/**
 * Runs inside the service worker, which shares cookies but not the page's in-memory CSRF
 * nonce. The Auth bridge hands the nonce to GET /auth/session when only the access cookie
 * expired, and serialises concurrent refreshes per session, so refreshing here is safe while
 * tabs are open.
 */
export type NotificationLookup =
  | { kind: 'found'; notification: Notification }
  /** The bridge session itself is gone: logged out, revoked, or past its absolute lifetime. */
  | { kind: 'signed-out' }
  | { kind: 'failed' };

type Fetch = typeof fetch;

const init: RequestInit = { credentials: 'same-origin', cache: 'no-store', headers: { Accept: 'application/json' } };

async function readJson(response: Response): Promise<Record<string, unknown>> {
  try {
    const body: unknown = await response.json();
    return body && typeof body === 'object' ? body as Record<string, unknown> : {};
  } catch { return {}; }
}

/** true: refreshed; false: no session left to refresh; throws on transient failures. */
async function refresh(fetcher: Fetch): Promise<boolean> {
  const session = await fetcher('/auth/session', init);
  if (session.ok) return true;
  const body = await readJson(session);
  if (session.status !== 401) throw new Error(`session ${session.status}`);
  if (body.refresh_required !== true || typeof body.csrf_token !== 'string' || !body.csrf_token) return false;
  const refreshed = await fetcher('/auth/refresh', { ...init, method: 'POST', headers: { Accept: 'application/json', 'X-CSRF-Token': body.csrf_token } });
  await refreshed.body?.cancel();
  if (refreshed.ok) return true;
  if (refreshed.status === 401) return false;
  throw new Error(`refresh ${refreshed.status}`);
}

export async function lookupNotification(apiBase: string, id: string, fetcher: Fetch = fetch): Promise<NotificationLookup> {
  const url = `${apiBase}/notifications/${encodeURIComponent(id)}?view=summary`;
  try {
    let response = await fetcher(url, init);
    if (response.status === 401) {
      await response.body?.cancel();
      if (!await refresh(fetcher)) return { kind: 'signed-out' };
      // The session exists, so a second 401 is a Gateway-side rejection, not a logout.
      response = await fetcher(url, init);
    }
    if (!response.ok) { await response.body?.cancel(); return { kind: 'failed' }; }
    const body = await readJson(response);
    const item = body.item as Notification | undefined;
    if (!item?.hazard?.area_name || !item.hazard.severity) return { kind: 'failed' };
    return { kind: 'found', notification: item };
  } catch {
    return { kind: 'failed' };
  }
}

const severityLabels: Record<Severity, string> = { NORMAL: 'Normal', WASPADA: 'Waspada', SIAGA: 'Siaga', AWAS: 'Awas' };

export interface DisplayedNotification { title: string; body: string }

export function describe(lookup: NotificationLookup): DisplayedNotification {
  if (lookup.kind === 'found') {
    const { hazard, message } = lookup.notification;
    return {
      title: `${severityLabels[hazard.severity] ?? hazard.severity} · ${hazard.area_name}`,
      body: message || `${hazard.source}: ${hazard.hazard_type === 'SEISMIC' ? 'kejadian seismik' : 'aktivitas vulkanik'}. Buka untuk detail.`,
    };
  }
  if (lookup.kind === 'signed-out') return { title: 'Peringatan bahaya baru', body: 'Masuk kembali untuk melihat detail kejadian.' };
  return { title: 'Peringatan bahaya baru', body: 'Buka dashboard untuk melihat detail kejadian.' };
}

/** Message type the worker posts to an open window when a notification is clicked. */
export const pushOpenMessage = 'bnpb.push.open';

/** Hash route the dashboard understands; the hazard id is also the notification id. */
export function notificationRoute(id: string | undefined): string {
  return id ? `/#/notifications?hazard=${encodeURIComponent(id)}` : '/#/notifications';
}

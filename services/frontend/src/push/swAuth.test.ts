import { describe as suite, expect, it, vi } from 'vitest';
import { describe, lookupNotification, notificationRoute } from './swAuth';

const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
const item = {
  notification_id: 'haz-1', created_at: '2026-10-09T05:00:00Z', message: 'Gempa M6 di Maluku.',
  hazard: { hazard_id: 'haz-1', source: 'BMKG', hazard_type: 'SEISMIC', severity: 'AWAS', area_name: 'Maluku', occurred_at: '2026-10-09T05:00:00Z', ingested_at: '2026-10-09T05:00:01Z' },
};

suite('service worker notification lookup', () => {
  it('fetches the summary view with same-origin cookies', async () => {
    const fetcher = vi.fn().mockResolvedValue(json({ item, freshness: [], partial: false }));
    const lookup = await lookupNotification('/api/v1', 'haz-1', fetcher);
    expect(lookup).toEqual({ kind: 'found', notification: item });
    expect(fetcher.mock.calls[0][0]).toBe('/api/v1/notifications/haz-1?view=summary');
    expect(fetcher.mock.calls[0][1]).toMatchObject({ credentials: 'same-origin' });
    expect(describe(lookup)).toEqual({ title: 'Awas · Maluku', body: 'Gempa M6 di Maluku.' });
  });

  it('refreshes an expired access cookie with the CSRF nonce from /auth/session, then retries once', async () => {
    const fetcher = vi.fn()
      .mockResolvedValueOnce(json({}, 401))
      .mockResolvedValueOnce(json({ refresh_required: true, csrf_token: 'csrf-sw' }, 401))
      .mockResolvedValueOnce(json({ user: {} }))
      .mockResolvedValueOnce(json({ item, freshness: [], partial: false }));
    expect((await lookupNotification('/api/v1', 'haz-1', fetcher)).kind).toBe('found');
    expect(fetcher.mock.calls.map(call => call[0])).toEqual(['/api/v1/notifications/haz-1?view=summary', '/auth/session', '/auth/refresh', '/api/v1/notifications/haz-1?view=summary']);
    expect(fetcher.mock.calls[2][1]).toMatchObject({ method: 'POST', headers: { 'X-CSRF-Token': 'csrf-sw' } });
  });

  it('reports signed-out when the bridge session is gone or refresh is rejected', async () => {
    const gone = vi.fn().mockResolvedValueOnce(json({}, 401)).mockResolvedValueOnce(json({ detail: 'session_required' }, 401));
    expect(await lookupNotification('/api/v1', 'haz-1', gone)).toEqual({ kind: 'signed-out' });
    expect(gone).toHaveBeenCalledTimes(2);
    const rejected = vi.fn()
      .mockResolvedValueOnce(json({}, 401))
      .mockResolvedValueOnce(json({ refresh_required: true, csrf_token: 'csrf-sw' }, 401))
      .mockResolvedValueOnce(json({ detail: 'session_expired' }, 401));
    const lookup = await lookupNotification('/api/v1', 'haz-1', rejected);
    expect(lookup).toEqual({ kind: 'signed-out' });
    expect(describe(lookup).body).toContain('Masuk kembali');
  });

  it('falls back to a generic notification on network, Gateway or format failures', async () => {
    expect(await lookupNotification('/api/v1', 'haz-1', vi.fn().mockRejectedValue(new TypeError('offline')))).toEqual({ kind: 'failed' });
    expect(await lookupNotification('/api/v1', 'haz-1', vi.fn().mockResolvedValue(json({}, 503)))).toEqual({ kind: 'failed' });
    expect(await lookupNotification('/api/v1', 'haz-1', vi.fn().mockResolvedValue(json({ item: {} })))).toEqual({ kind: 'failed' });
    const transient = vi.fn().mockResolvedValueOnce(json({}, 401)).mockResolvedValueOnce(json({}, 503));
    expect(await lookupNotification('/api/v1', 'haz-1', transient)).toEqual({ kind: 'failed' });
    const stillRejected = vi.fn().mockResolvedValueOnce(json({}, 401)).mockResolvedValueOnce(json({ user: {} })).mockResolvedValueOnce(json({}, 401));
    expect(await lookupNotification('/api/v1', 'haz-1', stillRejected)).toEqual({ kind: 'failed' });
    expect(describe({ kind: 'failed' }).title).toBe('Peringatan bahaya baru');
  });

  it('builds the dashboard deep link', () => {
    expect(notificationRoute('haz 1')).toBe('/#/notifications?hazard=haz%201');
    expect(notificationRoute(undefined)).toBe('/#/notifications');
  });
});

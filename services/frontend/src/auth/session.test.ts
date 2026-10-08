import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const payload = {
  user: { id: 'user-test', name: 'Pengguna Media', role: 'MEDIA', scopes: ['hazards:read:summary', 'notifications:read'], mode: 'oidc' },
  csrf_token: 'csrf-test', expires_at: new Date(Date.now() + 60_000).toISOString(),
};
const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

beforeEach(() => vi.resetModules());
afterEach(() => vi.unstubAllGlobals());

describe('Cookie Auth session', () => {
  it('keeps anonymous users logged out without any fabricated identity', async () => {
    const fetchMock = vi.fn().mockResolvedValue(json({ detail: 'Login diperlukan.' }, 401));
    vi.stubGlobal('fetch', fetchMock);
    const { getSession } = await import('./session');
    expect(await getSession()).toBeNull();
    expect(fetchMock.mock.calls[0][1].credentials).toBe('same-origin');
  });

  it('refreshes an expired access cookie using server-issued CSRF and restores the profile', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(json({ refresh_required: true, csrf_token: 'expired-csrf' }, 401))
      .mockResolvedValueOnce(json(payload));
    vi.stubGlobal('fetch', fetchMock);
    const { getSession } = await import('./session');
    expect(await getSession()).toEqual(payload);
    expect(fetchMock.mock.calls[1][0]).toBe('/auth/refresh');
    expect(fetchMock.mock.calls[1][1]).toMatchObject({ method: 'POST', credentials: 'same-origin', headers: { 'X-CSRF-Token': 'expired-csrf' } });
  });

  it('shares concurrent refresh requests and clears the profile if refresh is rejected', async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(json(payload)).mockResolvedValueOnce(json({ detail: 'Sesi kedaluwarsa.' }, 401));
    vi.stubGlobal('fetch', fetchMock);
    const { getSession, refreshSession, subscribeSession } = await import('./session');
    await getSession();
    const changed = vi.fn();
    const stop = subscribeSession(changed);
    const first = refreshSession();
    const second = refreshSession();
    expect(second).toBe(first);
    await expect(first).rejects.toMatchObject({ status: 401 });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(changed).toHaveBeenLastCalledWith(null);
    stop();
  });

  it('rejects malformed or demo sessions returned as a real authenticated session', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(json({ ...payload, user: { ...payload.user, mode: 'demo' } })));
    const { getSession } = await import('./session');
    await expect(getSession()).rejects.toMatchObject({ status: 502 });
  });

  it('posts logout with CSRF and returns the server logout destination', async () => {
    vi.stubGlobal('window', { location: { origin: 'http://localhost:8080' } });
    const fetchMock = vi.fn().mockResolvedValueOnce(json(payload)).mockResolvedValueOnce(json({ logout_url: 'http://localhost:9000/oidc/logout' }));
    vi.stubGlobal('fetch', fetchMock);
    const { getSession, logoutSession } = await import('./session');
    await getSession();
    expect(await logoutSession()).toBe('http://localhost:9000/oidc/logout');
    expect(fetchMock.mock.calls[1][1]).toMatchObject({ method: 'POST', headers: { 'X-CSRF-Token': payload.csrf_token } });
  });
});

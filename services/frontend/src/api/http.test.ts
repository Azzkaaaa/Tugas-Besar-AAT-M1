import { afterEach, describe, expect, it, vi } from 'vitest';
import { createHttpGatewayClient } from './http';
import { getCsrfToken, refreshSession } from '../auth/session';

vi.mock('../auth/session', () => ({ refreshSession: vi.fn(), getCsrfToken: vi.fn() }));

afterEach(() => vi.unstubAllGlobals());

describe('Gateway HTTP adapter', () => {
  it('rejects cross-origin cookie API configuration before a request', () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    expect(() => createHttpGatewayClient('https://another-origin.example/api/v1')).toThrow('origin yang sama');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('sends same-origin cookies and filters without exposing tokens in headers or URLs', async () => {
    const payload = { items: [], freshness: [{ source: 'BMKG', status: 'FRESH' }], partial: false };
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify(payload), { headers: { 'Content-Type': 'application/json' } }));
    vi.stubGlobal('fetch', fetchMock);
    const client = createHttpGatewayClient();
    expect(await client.listHazards({ source: 'BMKG', page_size: 8 })).toEqual(payload);
    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toContain('/hazards?source=BMKG&page_size=8');
    expect(url).not.toContain('test-only-token');
    expect(options.headers.Authorization).toBeUndefined();
    expect(options.credentials).toBe('same-origin');
  });

  it('exposes structured HTTP errors without replacing them with generic success', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ detail: 'Akses ditolak.', correlation_id: 'corr-example' }), { status: 403 })));
    const client = createHttpGatewayClient();
    await expect(client.listNotifications()).rejects.toMatchObject({ status: 403, message: 'Akses ditolak.', problem: { correlation_id: 'corr-example' } });
  });

  it('parses authenticated SSE and cancels the reader when unsubscribed', async () => {
    const event = {
      event_id: 'event-test', sequence: '1', published_at: new Date().toISOString(),
      hazard: { hazard_id: 'haz-test', source: 'BMKG', hazard_type: 'SEISMIC', severity: 'SIAGA', area_name: 'Maluku', occurred_at: new Date().toISOString(), ingested_at: new Date().toISOString() },
    };
    let streamController: ReadableStreamDefaultController<Uint8Array> | undefined;
    const cancelled = vi.fn();
    const stream = new ReadableStream<Uint8Array>({ start(controller) { streamController = controller; }, cancel: cancelled });
    const fetchMock = vi.fn().mockResolvedValue(new Response(stream, { headers: { 'Content-Type': 'text/event-stream' } }));
    vi.stubGlobal('fetch', fetchMock);
    const client = createHttpGatewayClient();
    let received: (() => void) | undefined;
    const eventReceived = new Promise<void>((resolve) => { received = resolve; });
    const onEvent = vi.fn(() => received?.());
    const unsubscribe = client.subscribe(onEvent);
    streamController?.enqueue(new TextEncoder().encode(`: heartbeat\r\nid: cursor-1\r\nevent: hazard.upsert\r\ndata: ${JSON.stringify(event)}\r\n\r\n`));
    await eventReceived;
    unsubscribe();
    // Native fetch cancels on abort; this in-memory response is closed explicitly.
    streamController?.close();
    await Promise.resolve();
    expect(onEvent).toHaveBeenCalledWith(event);
    expect(fetchMock.mock.calls[0][1].headers.Accept).toBe('text/event-stream');
    expect(fetchMock.mock.calls[0][1].signal.aborted).toBe(true);
  });

  it('refreshes on 401 then retries only once, preserving the abort signal', async () => {
    const fetchMock = vi.fn().mockImplementation(async () => new Response('{}', { status: 401 }));
    vi.stubGlobal('fetch', fetchMock);
    vi.mocked(refreshSession).mockResolvedValue({ user: { id: 'user', name: 'Media', role: 'MEDIA', scopes: [], mode: 'oidc' }, csrf_token: 'csrf', expires_at: new Date(Date.now() + 60_000).toISOString() });
    const signal = new AbortController().signal;
    await expect(createHttpGatewayClient().listHazards({}, { signal })).rejects.toMatchObject({ status: 401 });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(refreshSession).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[1][1].signal).toBe(signal);
  });

  it('registers a push token with CSRF and retries once after a refresh', async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(new Response('{}', { status: 401 })).mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal('fetch', fetchMock);
    vi.mocked(getCsrfToken).mockResolvedValue('csrf-test');
    vi.mocked(refreshSession).mockResolvedValue({ user: { id: 'user', name: 'Media', role: 'MEDIA', scopes: [], mode: 'oidc' }, csrf_token: 'csrf-test', expires_at: new Date(Date.now() + 60_000).toISOString() });
    await createHttpGatewayClient().registerPushToken('fcm-test');
    expect(fetchMock).toHaveBeenCalledTimes(2);
    const [url, init] = fetchMock.mock.calls[1];
    expect(url).toContain('/notifications/push/register');
    expect(init).toMatchObject({ method: 'POST', credentials: 'same-origin', body: JSON.stringify({ fcm_token: 'fcm-test' }) });
    expect(init.headers).toMatchObject({ 'X-CSRF-Token': 'csrf-test', 'Content-Type': 'application/json' });
  });

  it('surfaces a rejected push unregistration as a structured error', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ detail: 'csrf' }), { status: 403 })));
    vi.mocked(getCsrfToken).mockResolvedValue('csrf-test');
    await expect(createHttpGatewayClient().unregisterPushToken('fcm-test')).rejects.toMatchObject({ status: 403 });
  });

  it('performs webhook operations with proper paths, methods, and CSRF token', async () => {
    const webhookItem = {
      webhook_id: 'wh-123',
      url: 'http://host.docker.internal:9090/webhook',
      view: 'full',
      is_active: true,
      owner_client_id: 'user-1',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    const deliveriesItem = {
      items: [{
        delivery_id: 'del-1',
        webhook_id: 'wh-123',
        event_id: 'evt-1',
        hazard_id: 'haz-1',
        status_code: 200,
        status: 'SUCCESS',
        attempt: 1,
        duration_ms: 25,
        delivered_at: new Date().toISOString(),
      }],
    };
    const pingResult = { status_code: 200, duration_ms: 30, success: true, message: 'ok' };

    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ items: [webhookItem] }), { headers: { 'Content-Type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(webhookItem), { status: 201, headers: { 'Content-Type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(webhookItem), { headers: { 'Content-Type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(deliveriesItem), { headers: { 'Content-Type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(pingResult), { headers: { 'Content-Type': 'application/json' } }));

    vi.stubGlobal('fetch', fetchMock);
    vi.mocked(getCsrfToken).mockResolvedValue('csrf-token-xyz');

    const client = createHttpGatewayClient();

    // 1. List
    const list = await client.listWebhooks();
    expect(list.items).toHaveLength(1);
    expect(fetchMock.mock.calls[0][0]).toContain('/webhooks');

    // 2. Create
    const created = await client.createWebhook({ url: 'http://host.docker.internal:9090/webhook', view: 'full' });
    expect(created.webhook_id).toBe('wh-123');
    expect(fetchMock.mock.calls[1][0]).toContain('/webhooks');
    expect(fetchMock.mock.calls[1][1].method).toBe('POST');
    expect(fetchMock.mock.calls[1][1].headers['X-CSRF-Token']).toBe('csrf-token-xyz');

    // 3. Get
    const get = await client.getWebhook('wh-123');
    expect(get.webhook_id).toBe('wh-123');
    expect(fetchMock.mock.calls[2][0]).toContain('/webhooks/wh-123');

    // 4. Delete
    await client.deleteWebhook('wh-123');
    expect(fetchMock.mock.calls[3][0]).toContain('/webhooks/wh-123');
    expect(fetchMock.mock.calls[3][1].method).toBe('DELETE');
    expect(fetchMock.mock.calls[3][1].headers['X-CSRF-Token']).toBe('csrf-token-xyz');

    // 5. Deliveries
    const deliveries = await client.listWebhookDeliveries('wh-123');
    expect(deliveries.items).toHaveLength(1);
    expect(fetchMock.mock.calls[4][0]).toContain('/webhooks/wh-123/deliveries');

    // 6. Ping / Test
    const ping = await client.pingWebhook('wh-123');
    expect(ping.success).toBe(true);
    expect(fetchMock.mock.calls[5][0]).toContain('/webhooks/wh-123/test');
    expect(fetchMock.mock.calls[5][1].method).toBe('POST');
    expect(fetchMock.mock.calls[5][1].headers['X-CSRF-Token']).toBe('csrf-token-xyz');
  });
});


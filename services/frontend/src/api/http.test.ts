import { afterEach, describe, expect, it, vi } from 'vitest';
import { createHttpGatewayClient } from './http';
import { refreshSession } from '../auth/session';

vi.mock('../auth/session', () => ({ refreshSession: vi.fn() }));

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
});

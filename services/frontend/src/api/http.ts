import { apiBaseUrl } from '../config';
import type { GatewayClient, HazardFeedEvent, HazardFilters, Problem, RequestOptions } from '../types';
import { ApiError } from './errors';
import { getCsrfToken, refreshSession } from '../auth/session';

function queryString(filters: HazardFilters): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined && value !== '') params.set(key, String(value));
  }
  return params.size ? `?${params}` : '';
}

async function readProblem(response: Response): Promise<ApiError> {
  let problem: Partial<Problem> = {};
  try {
    const body: unknown = await response.json();
    if (body && typeof body === 'object') problem = body as Partial<Problem>;
  } catch { /* No internal response body is exposed. */ }
  const detail = typeof problem.detail === 'string' ? problem.detail :
    response.status === 401 ? 'Silakan masuk kembali untuk melanjutkan.' :
      response.status === 403 ? 'Akun ini tidak memiliki akses yang diperlukan.' :
        'Gateway belum dapat memproses permintaan. Silakan coba kembali.';
  return new ApiError(response.status, detail, {
    ...(typeof problem.type === 'string' ? { type: problem.type } : {}),
    ...(typeof problem.title === 'string' ? { title: problem.title } : {}),
    ...(typeof problem.correlation_id === 'string' ? { correlation_id: problem.correlation_id } : {}),
    ...(typeof problem.instance === 'string' ? { instance: problem.instance } : {}),
  });
}

/** Restrict cookie-bearing requests to the SPA origin; the reverse proxy owns API routing. */
export function createHttpGatewayClient(baseUrl = apiBaseUrl): GatewayClient {
  const origin = globalThis.location?.origin ?? 'http://localhost';
  const resolved = new URL(baseUrl, origin);
  if (resolved.origin !== origin || resolved.username || resolved.password || resolved.search || resolved.hash) {
    throw new ApiError(400, 'Alamat Gateway harus berada pada origin yang sama dengan frontend.');
  }
  const basePath = resolved.pathname.replace(/\/+$/, '');
  async function authorizedFetch(path: string, options: RequestOptions = {}, extraHeaders: Record<string, string> = {}, body?: unknown, method?: string): Promise<Response> {
    try {
      const httpMethod = method ?? (body !== undefined ? 'POST' : 'GET');
      const init: RequestInit = {
        ...(httpMethod !== 'GET' && { method: httpMethod }),
        ...(body !== undefined && { body: JSON.stringify(body) }),
        headers: { Accept: 'application/json', ...(body !== undefined && { 'Content-Type': 'application/json' }), ...extraHeaders },
        signal: options.signal,
        cache: 'no-store',
        credentials: 'same-origin',
      };
      const response = await fetch(`${basePath}${path}`, init);
      if (response.status !== 401) return response;
      options.signal?.throwIfAborted();
      await response.body?.cancel();
      await refreshSession();
      options.signal?.throwIfAborted();
      return await fetch(`${basePath}${path}`, init);
    } catch (error) {
      if (error instanceof ApiError) throw error;
      if (error instanceof DOMException && error.name === 'AbortError') throw error;
      throw new ApiError(503, 'Tidak dapat terhubung ke Gateway. Periksa koneksi dan coba kembali.');
    }
  }

  async function request<T>(path: string, options?: RequestOptions): Promise<T> {
    const response = await authorizedFetch(path, options);
    if (!response.ok) throw await readProblem(response);
    try { return await response.json() as T; }
    catch { throw new ApiError(502, 'Format respons Gateway belum sesuai kontrak.'); }
  }

  /** The CSRF nonce survives a refresh, so the single retry may reuse it. */
  async function mutate(path: string, body?: unknown, options?: RequestOptions, method = 'POST'): Promise<void> {
    const response = await authorizedFetch(path, options, { 'X-CSRF-Token': await getCsrfToken() }, body, method);
    if (!response.ok) throw await readProblem(response);
    await response.body?.cancel();
  }

  async function mutateWithResponse<T>(path: string, method = 'POST', body?: unknown, options?: RequestOptions): Promise<T> {
    const response = await authorizedFetch(path, options, { 'X-CSRF-Token': await getCsrfToken() }, body, method);
    if (!response.ok) throw await readProblem(response);
    try { return await response.json() as T; }
    catch { throw new ApiError(502, 'Format respons Gateway belum sesuai kontrak.'); }
  }

  return {
    listHazards: (filters = {}, options) => request(`/hazards${queryString(filters)}`, options),
    getHazard: (id, view = 'summary', options) => request(`/hazards/${encodeURIComponent(id)}${queryString({ view })}`, options),
    listNotifications: (filters = {}, options) => request(`/notifications${queryString(filters)}`, options),
    getSourceStatuses: (options) => request('/hazards/sources/statuses', options),
    registerPushToken: (token, options) => mutate('/notifications/push/register', { fcm_token: token }, options),
    unregisterPushToken: (token, options) => mutate('/notifications/push/unregister', { fcm_token: token }, options),
    listWebhooks: (options) => request('/webhooks', options),
    createWebhook: (payload, options) => mutateWithResponse('/webhooks', 'POST', payload, options),
    getWebhook: (id, options) => request(`/webhooks/${encodeURIComponent(id)}`, options),
    deleteWebhook: (id, options) => mutate(`/webhooks/${encodeURIComponent(id)}`, undefined, options, 'DELETE'),
    listWebhookDeliveries: (id, options) => request(`/webhooks/${encodeURIComponent(id)}/deliveries`, options),
    pingWebhook: (id, options) => mutateWithResponse(`/webhooks/${encodeURIComponent(id)}/test`, 'POST', {}, options),

    subscribe(onEvent, onStatus, onReset) {
      let stopped = false;
      let cursor = '';
      let retry: ReturnType<typeof setTimeout> | undefined;
      let controller: AbortController | undefined;
      let retryDelay = 1000;
      let resetSnapshot = false;
      const seen = new Set<string>();

      async function connect() {
        if (stopped) return;
        controller = new AbortController();
        onStatus?.('connecting');
        let stopOnAuth = false;
        try {
          const response = await authorizedFetch('/feeds/events?view=summary', { signal: controller.signal }, {
            Accept: 'text/event-stream',
            ...(cursor ? { 'Last-Event-ID': cursor } : {}),
          });
          if (!response.ok) throw await readProblem(response);
          if (!response.headers.get('content-type')?.includes('text/event-stream') || !response.body) {
            throw new ApiError(502, 'Gateway belum menyediakan aliran kejadian yang valid.');
          }
          onStatus?.('connected');
          // Reopen the live stream before asking the UI to refetch its snapshot.
          // The UI treats incoming events as invalidations during that fetch.
          if (resetSnapshot) { resetSnapshot = false; onReset?.(); }
          retryDelay = 1000;
          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';
          let eventName = '';
          let eventId = '';
          let dataLines: string[] = [];
          let needsReset = false;

          const consumeLine = (line: string) => {
            if (line === '') {
              if (eventName === 'reset_required') {
                cursor = '';
                seen.clear();
                needsReset = true;
                resetSnapshot = true;
              } else if (eventName === 'hazard.upsert' && dataLines.length) {
                let event: HazardFeedEvent;
                try { event = JSON.parse(dataLines.join('\n')) as HazardFeedEvent; }
                catch { throw new ApiError(502, 'Format event Gateway belum sesuai kontrak.'); }
                if (!event.event_id || !event.hazard?.hazard_id || !event.sequence) throw new ApiError(502, 'Format event Gateway belum sesuai kontrak.');
                if (eventId) cursor = eventId;
                if (!seen.has(event.event_id)) {
                  seen.add(event.event_id);
                  if (seen.size > 1000) seen.delete(seen.values().next().value!);
                  onEvent(event);
                }
              }
              eventName = ''; eventId = ''; dataLines = [];
              return;
            }
            if (line.startsWith(':')) return;
            const separator = line.indexOf(':');
            const field = separator < 0 ? line : line.slice(0, separator);
            let value = separator < 0 ? '' : line.slice(separator + 1);
            if (value.startsWith(' ')) value = value.slice(1);
            if (field === 'event') eventName = value;
            else if (field === 'id' && !value.includes('\0')) eventId = value;
            else if (field === 'data') dataLines.push(value);
          };

          try {
            while (!stopped && !needsReset) {
              const { done, value } = await reader.read();
              if (done) break;
              buffer += decoder.decode(value, { stream: true });
              let newline: number;
              while ((newline = buffer.indexOf('\n')) >= 0) {
                const line = buffer.slice(0, newline).replace(/\r$/, '');
                buffer = buffer.slice(newline + 1);
                consumeLine(line);
                if (needsReset) break;
              }
              if (buffer.length > 1_000_000) throw new ApiError(502, 'Ukuran event Gateway tidak valid.');
            }
          } finally {
            await reader.cancel().catch(() => undefined);
            reader.releaseLock();
          }
        } catch (error) {
          stopOnAuth = error instanceof ApiError && (error.status === 401 || error.status === 403);
        } finally {
          if (!stopped) {
            onStatus?.('disconnected');
            // Failed refresh clears the real session through the Auth subscription.
            if (!stopOnAuth) {
              retry = setTimeout(() => { void connect(); }, retryDelay);
              retryDelay = Math.min(retryDelay * 2, 15_000);
            }
          }
        }
      }

      void connect();
      return () => { stopped = true; controller?.abort(); if (retry) clearTimeout(retry); };
    },
    setScenario() {
      throw new ApiError(400, 'Skenario simulasi tidak tersedia pada koneksi Gateway.');
    },
  };
}

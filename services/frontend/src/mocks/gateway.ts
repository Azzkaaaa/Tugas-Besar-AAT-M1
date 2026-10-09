import { canReadFull } from '../auth/demo';
import { ApiError } from '../api/errors';
import type {
  AuthSession, DemoScenario, FeedStatus, Freshness, GatewayClient, Hazard, HazardFeedEvent,
  HazardFilters, HazardFull, HazardView, Notification, RequestOptions, Source,
  WebhookDelivery, WebhookSubscription,
} from '../types';
import { createFixture, createFixtures } from './fixtures';


const sources: Source[] = ['BMKG', 'PVMBG'];
const severities = ['NORMAL', 'WASPADA', 'SIAGA', 'AWAS'];
const snapshotLifetime = 5 * 60_000;

/** Constructing a new object is deliberate: summary is a strict allowlist. */
export function projectHazard(hazard: HazardFull, view: HazardView): Hazard {
  const summary = {
    hazard_id: hazard.hazard_id,
    source: hazard.source,
    hazard_type: hazard.hazard_type,
    severity: hazard.severity,
    area_name: hazard.area_name,
    occurred_at: hazard.occurred_at,
    ingested_at: hazard.ingested_at,
  };
  return view === 'full'
    ? { ...summary, source_ref_id: hazard.source_ref_id, latitude: hazard.latitude, longitude: hazard.longitude, attributes: structuredClone(hazard.attributes) }
    : summary;
}

function wait(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) { reject(new DOMException('Permintaan dibatalkan', 'AbortError')); return; }
    const abort = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', abort);
      reject(new DOMException('Permintaan dibatalkan', 'AbortError'));
    };
    const timer = setTimeout(() => { signal?.removeEventListener('abort', abort); resolve(); }, ms);
    signal?.addEventListener('abort', abort, { once: true });
  });
}

function selection(filters: HazardFilters): Source[] {
  if (filters.source !== undefined && !sources.includes(filters.source)) throw new ApiError(400, 'Sumber tidak dikenal.');
  if (filters.hazard_type !== undefined && !['SEISMIC', 'VOLCANIC'].includes(filters.hazard_type)) throw new ApiError(400, 'Jenis kejadian tidak dikenal.');
  if (filters.severity !== undefined && !severities.includes(filters.severity)) throw new ApiError(400, 'Tingkat bahaya tidak dikenal.');
  if (filters.view !== undefined && !['summary', 'full'].includes(filters.view)) throw new ApiError(400, 'Tampilan data tidak dikenal.');
  const implied: Source | undefined = filters.hazard_type === 'SEISMIC' ? 'BMKG' : filters.hazard_type === 'VOLCANIC' ? 'PVMBG' : undefined;
  if (filters.source && implied && filters.source !== implied) throw new ApiError(400, 'Sumber dan jenis kejadian tidak cocok.');
  for (const date of [filters.since, filters.until]) {
    if (date !== undefined && (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(date) || !Number.isFinite(Date.parse(date)))) {
      throw new ApiError(400, 'Rentang waktu harus memakai tanggal dan zona waktu yang valid.');
    }
  }
  if (filters.since && filters.until && Date.parse(filters.since) >= Date.parse(filters.until)) throw new ApiError(400, 'Waktu akhir harus sesudah waktu awal.');
  const size = filters.page_size ?? 20;
  if (!Number.isInteger(size) || size < 1 || size > 100) throw new ApiError(400, 'Ukuran halaman harus antara 1 dan 100.');
  if (filters.page_token !== undefined && !filters.page_token) throw new ApiError(400, 'Token halaman tidak valid.');
  return filters.source ? [filters.source] : implied ? [implied] : sources;
}

function matches(hazard: HazardFull, filters: HazardFilters, selected: Source[]): boolean {
  const occurred = Date.parse(hazard.occurred_at);
  return selected.includes(hazard.source) &&
    (!filters.severity || hazard.severity === filters.severity) &&
    (!filters.since || occurred >= Date.parse(filters.since)) &&
    (!filters.until || occurred < Date.parse(filters.until));
}

interface Snapshot {
  signature: string;
  expires: number;
  rows: (Hazard | Notification)[];
}

interface Cursor {
  snapshot: Snapshot;
  offset: number;
}

/** In-browser fixtures, not the upstream BMKG/PVMBG service or a login provider. */
export function createMockGatewayClient(session: AuthSession, latencyMs = 350): GatewayClient {
  const identity = { ...session, scopes: [...session.scopes] };
  const hazards = createFixtures();
  let scenario: DemoScenario = 'normal';
  let scenarioStartedAt = Date.now();
  let sequence = 0;
  let cursorSequence = 0;
  let interval: ReturnType<typeof setInterval> | undefined;
  const cursors = new Map<string, Cursor>();
  const listeners = new Set<{ event: (event: HazardFeedEvent) => void; status?: (status: FeedStatus) => void }>();

  function requireView(view: HazardView) {
    if (!identity.scopes.includes('hazards:read:summary') || (view === 'full' && !canReadFull(identity))) {
      throw new ApiError(403, 'Akun ini tidak memiliki akses ke tampilan yang diminta.');
    }
  }

  function freshness(selected = sources): Freshness[] {
    return selected.map((source) => {
      if (scenario === 'error') return { source, status: 'UNAVAILABLE' };
      if (scenario === 'stale' && source === 'PVMBG') {
        return { source, status: 'STALE', last_success_at: new Date(scenarioStartedAt - 13 * 60_000).toISOString(), stale_since: new Date(scenarioStartedAt).toISOString() };
      }
      return { source, status: 'FRESH', last_success_at: new Date(Date.now() - 3000).toISOString() };
    });
  }

  async function beforeRead(options?: RequestOptions) {
    await wait(latencyMs, options?.signal);
    if (scenario === 'error') throw new ApiError(503, 'Data kejadian belum tersedia. Silakan coba kembali.', { type: 'urn:bnpb:problem:source-unavailable' });
  }

  function paginate<T extends Hazard | Notification>(kind: 'hazards' | 'notifications', rows: T[], filters: HazardFilters): { items: T[]; next_page_token?: string } {
    const { page_token: token, ...query } = filters;
    const signature = JSON.stringify([kind, identity.id, query.source ?? '', query.hazard_type ?? '', query.severity ?? '', query.since ?? '', query.until ?? '', query.view ?? 'summary', query.page_size ?? 20]);
    const pageSize = query.page_size ?? 20;
    const cursor = token ? cursors.get(token) : undefined;
    if (token && (!cursor || cursor.snapshot.signature !== signature)) throw new ApiError(400, 'Token halaman tidak cocok. Muat ulang daftar dari awal.');
    if (cursor && cursor.snapshot.expires <= Date.now()) throw new ApiError(409, 'Halaman sudah kedaluwarsa. Muat ulang daftar dari awal.');
    const snapshot = cursor?.snapshot ?? { signature, expires: Date.now() + snapshotLifetime, rows: structuredClone(rows) };
    const offset = cursor?.offset ?? 0;
    const items = structuredClone(snapshot.rows.slice(offset, offset + pageSize)) as T[];
    if (offset + pageSize >= snapshot.rows.length) return { items };
    const next = `demo-page-${++cursorSequence}-${Date.now().toString(36)}`;
    cursors.set(next, { snapshot, offset: offset + pageSize });
    // Keep memory bounded during long UI development sessions.
    if (cursors.size > 250) cursors.delete(cursors.keys().next().value!);
    return { items, next_page_token: next };
  }

  function filtered(filters: HazardFilters, selected: Source[]) {
    return scenario === 'empty' ? [] : hazards.filter((hazard) => matches(hazard, filters, selected))
      .sort((left, right) => Date.parse(right.occurred_at) - Date.parse(left.occurred_at) || left.hazard_id.localeCompare(right.hazard_id));
  }

  function publish() {
    if (scenario === 'error' || scenario === 'empty') return;
    sequence++;
    // During a PVMBG outage, only BMKG continues producing updates.
    const index = scenario === 'stale' ? 25 + sequence * 2 : 24 + sequence;
    const hazard = createFixture(index, new Date(Date.now() - 2000), true);
    hazard.hazard_id = `haz-demo-live-${sequence}`;
    hazards.unshift(hazard);
    const event: HazardFeedEvent = {
      event_id: `evt-demo-${sequence}`,
      sequence: String(sequence),
      hazard: projectHazard(hazard, canReadFull(identity) ? 'full' : 'summary'),
      published_at: new Date().toISOString(),
    };
    for (const listener of listeners) listener.event(structuredClone(event));
  }

  const mockWebhooks: WebhookSubscription[] = [
    {
      webhook_id: 'wh-demo-local-01',
      url: 'http://host.docker.internal:9090/webhook',
      description: 'Local Docker Webhook Listener',
      view: canReadFull(identity) ? 'full' : 'summary',
      is_active: true,
      owner_client_id: identity.id,
      created_at: new Date(Date.now() - 3600_000).toISOString(),
      updated_at: new Date(Date.now() - 3600_000).toISOString(),
      filter: {},
    },
  ];

  const mockDeliveries = new Map<string, WebhookDelivery[]>([
    ['wh-demo-local-01', [
      {
        delivery_id: 'del-demo-01',
        webhook_id: 'wh-demo-local-01',
        event_id: 'evt-demo-1',
        hazard_id: 'haz-bmkg-1',
        status_code: 200,
        status: 'SUCCESS',
        attempt: 1,
        duration_ms: 45,
        delivered_at: new Date(Date.now() - 1800_000).toISOString(),
      },
    ]],
  ]);

  return {
    async listHazards(filters = {}, options) {
      const selected = selection(filters);
      requireView(filters.view ?? 'summary');
      await beforeRead(options);
      const status = freshness(selected);
      return {
        ...paginate('hazards', filtered(filters, selected).map((hazard) => projectHazard(hazard, filters.view ?? 'summary')), filters),
        freshness: status,
        partial: status.some((entry) => entry.status !== 'FRESH'),
      };
    },
    async getHazard(id, view = 'summary', options) {
      selection({ view });
      requireView(view);
      await beforeRead(options);
      const hazard = scenario === 'empty' ? undefined : hazards.find((candidate) => candidate.hazard_id === id);
      if (!hazard) throw new ApiError(404, 'Kejadian tidak ditemukan.');
      const status = freshness([hazard.source]);
      return { item: projectHazard(hazard, view), freshness: status, partial: status[0].status !== 'FRESH' };
    },
    async listNotifications(filters = {}, options) {
      const selected = selection(filters);
      requireView(filters.view ?? 'summary');
      if (!identity.scopes.includes('notifications:read')) throw new ApiError(403, 'Akun ini tidak memiliki akses ke notifikasi.');
      await beforeRead(options);
      const notifications: Notification[] = filtered(filters, selected)
        .filter((hazard) => hazard.severity === 'SIAGA' || hazard.severity === 'AWAS')
        .map((hazard) => ({
          notification_id: `notif-${hazard.hazard_id}`,
          hazard: projectHazard(hazard, filters.view ?? 'summary'),
          created_at: new Date(Date.parse(hazard.ingested_at) + 500).toISOString(),
          message: `${hazard.area_name} berstatus ${hazard.severity}. Pantau pembaruan dari ${hazard.source}.`,
        }))
        .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at) || left.notification_id.localeCompare(right.notification_id));
      const status = freshness(selected);
      return { ...paginate('notifications', notifications, filters), freshness: status, partial: status.some((entry) => entry.status !== 'FRESH') };
    },
    async getSourceStatuses(options) {
      requireView('summary');
      await wait(latencyMs, options?.signal);
      return { freshness: freshness() };
    },
    async listWebhooks(options) {
      if (!identity.scopes.includes('webhooks:read')) throw new ApiError(403, 'Akun ini tidak memiliki akses ke webhooks.');
      await beforeRead(options);
      return { items: structuredClone(mockWebhooks) };
    },
    async createWebhook(payload, options) {
      if (!identity.scopes.includes('webhooks:write')) throw new ApiError(403, 'Akun ini tidak memiliki akses untuk membuat webhook.');
      if (payload.view === 'full' && !canReadFull(identity)) throw new ApiError(403, 'Tampilan data lengkap memerlukan hak akses full view.');
      if (!payload.url || (!payload.url.startsWith('http://') && !payload.url.startsWith('https://'))) {
        throw new ApiError(400, 'Alamat URL webhook harus diawali dengan http:// atau https://.');
      }
      await beforeRead(options);
      const sub: WebhookSubscription = {
        webhook_id: `wh-demo-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
        url: payload.url.trim(),
        description: payload.description?.trim(),
        view: payload.view ?? (canReadFull(identity) ? 'full' : 'summary'),
        filter: payload.filter,
        is_active: true,
        owner_client_id: identity.id,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };
      mockWebhooks.unshift(sub);
      return structuredClone(sub);
    },
    async getWebhook(id, options) {
      if (!identity.scopes.includes('webhooks:read')) throw new ApiError(403, 'Akun ini tidak memiliki akses ke webhooks.');
      await beforeRead(options);
      const item = mockWebhooks.find((w) => w.webhook_id === id);
      if (!item) throw new ApiError(404, 'Webhook tidak ditemukan.');
      return structuredClone(item);
    },
    async deleteWebhook(id, options) {
      if (!identity.scopes.includes('webhooks:write')) throw new ApiError(403, 'Akun ini tidak memiliki akses untuk menghapus webhook.');
      await beforeRead(options);
      const idx = mockWebhooks.findIndex((w) => w.webhook_id === id);
      if (idx < 0) throw new ApiError(404, 'Webhook tidak ditemukan.');
      mockWebhooks.splice(idx, 1);
    },
    async listWebhookDeliveries(id, options) {
      if (!identity.scopes.includes('webhooks:read')) throw new ApiError(403, 'Akun ini tidak memiliki akses ke pengiriman webhook.');
      await beforeRead(options);
      return { items: structuredClone(mockDeliveries.get(id) ?? []) };
    },
    async pingWebhook(id, options) {
      if (!identity.scopes.includes('webhooks:write')) throw new ApiError(403, 'Akun ini tidak memiliki akses untuk menguji webhook.');
      await beforeRead(options);
      const item = mockWebhooks.find((w) => w.webhook_id === id);
      if (!item) throw new ApiError(404, 'Webhook tidak ditemukan.');
      const del: WebhookDelivery = {
        delivery_id: `del-demo-${Date.now().toString(36)}`,
        webhook_id: id,
        event_id: `ping-${Date.now()}`,
        hazard_id: 'test-ping',
        status_code: 200,
        status: 'SUCCESS',
        attempt: 1,
        duration_ms: 32 + Math.floor(Math.random() * 20),
        delivered_at: new Date().toISOString(),
      };
      const current = mockDeliveries.get(id) ?? [];
      current.unshift(del);
      mockDeliveries.set(id, current);
      return { status_code: 200, duration_ms: del.duration_ms, success: true, message: 'Endpoint HTTP 200 OK' };
    },
    subscribe(onEvent, onStatus) {
      requireView('summary');
      const listener = { event: onEvent, status: onStatus };
      listeners.add(listener);
      onStatus?.(scenario === 'error' ? 'disconnected' : 'connected');
      if (!interval) interval = setInterval(publish, 15_000);
      return () => {
        listeners.delete(listener);
        if (!listeners.size && interval) { clearInterval(interval); interval = undefined; }
      };
    },
    async registerPushToken() {
      throw new ApiError(400, 'Notifikasi push tidak tersedia pada data simulasi.');
    },
    async unregisterPushToken() {
      throw new ApiError(400, 'Notifikasi push tidak tersedia pada data simulasi.');
    },
    setScenario(next) {
      if (!['normal', 'stale', 'empty', 'error'].includes(next)) throw new ApiError(400, 'Skenario tidak dikenal.');
      if (scenario !== next) scenarioStartedAt = Date.now();
      scenario = next;
      for (const listener of listeners) listener.status?.(next === 'error' ? 'disconnected' : 'connected');
    },
  };
}


import { afterEach, describe, expect, it, vi } from 'vitest';
import type { AuthSession, HazardFeedEvent, HazardFull } from '../types';
import { createMockGatewayClient } from './gateway';

const media: AuthSession = {
  id: 'test-media', name: 'Media', role: 'MEDIA', mode: 'demo',
  scopes: ['hazards:read:summary', 'notifications:read'],
};
const internal: AuthSession = {
  id: 'test-internal', name: 'BNPB', role: 'BNPB_INTERNAL', mode: 'demo',
  scopes: [...media.scopes, 'hazards:read:full'],
};
const summaryKeys = ['hazard_id', 'source', 'hazard_type', 'severity', 'area_name', 'occurred_at', 'ingested_at'].sort();

afterEach(() => vi.useRealTimers());

describe('Gateway fixture contract', () => {
  it('Media receives exactly seven summary fields in lists, details, notifications and live events', async () => {
    const client = createMockGatewayClient(media, 0);
    const page = await client.listHazards();
    expect(Object.keys(page.items[0]).sort()).toEqual(summaryKeys);
    const detail = await client.getHazard(page.items[0].hazard_id);
    expect(Object.keys(detail.item).sort()).toEqual(summaryKeys);
    const notifications = await client.listNotifications();
    expect(Object.keys(notifications.items[0].hazard).sort()).toEqual(summaryKeys);
    expect(notifications.items[0].message).not.toMatch(/magnitude|latitude|confidence|depth/i);
    await expect(client.listHazards({ view: 'full' })).rejects.toMatchObject({ status: 403 });
    await expect(client.getHazard(page.items[0].hazard_id, 'full')).rejects.toMatchObject({ status: 403 });
    await expect(client.listNotifications({ view: 'full' })).rejects.toMatchObject({ status: 403 });
    vi.useFakeTimers();
    const event = vi.fn<(event: HazardFeedEvent) => void>();
    const unsubscribe = client.subscribe(event);
    await vi.advanceTimersByTimeAsync(15_000);
    expect(event).toHaveBeenCalledOnce();
    expect(Object.keys(event.mock.calls[0][0].hazard).sort()).toEqual(summaryKeys);
    unsubscribe();
    await vi.advanceTimersByTimeAsync(30_000);
    expect(event).toHaveBeenCalledOnce();
  });

  it('full view retains variable attributes without allowing callers to mutate cached data', async () => {
    const client = createMockGatewayClient(internal, 0);
    const page = await client.listHazards({ view: 'full', source: 'PVMBG' });
    const first = page.items[0] as HazardFull;
    expect(first.attributes.confidence_level).toEqual(expect.any(Number));
    expect(first.latitude).toEqual(expect.any(Number));
    first.attributes.confidence_level = 0;
    const detail = await client.getHazard(first.hazard_id, 'full');
    expect((detail.item as HazardFull).attributes.confidence_level).toBeGreaterThan(0);
  });

  it('type selects the matching source, rejects conflicts, and respects inclusive/exclusive time bounds', async () => {
    const client = createMockGatewayClient(media, 0);
    const volcanic = await client.listHazards({ hazard_type: 'VOLCANIC', page_size: 100 });
    expect(volcanic.items).toHaveLength(12);
    expect(volcanic.items.every((hazard) => hazard.source === 'PVMBG')).toBe(true);
    expect(volcanic.freshness.map((entry) => entry.source)).toEqual(['PVMBG']);
    await expect(client.listHazards({ source: 'BMKG', hazard_type: 'VOLCANIC' })).rejects.toMatchObject({ status: 400 });
    const [latest, previous] = volcanic.items;
    const range = await client.listHazards({ hazard_type: 'VOLCANIC', since: previous.occurred_at, until: latest.occurred_at });
    expect(range.items.map((hazard) => hazard.hazard_id)).toEqual([previous.hazard_id]);
    await expect(client.listHazards({ page_size: 0 })).rejects.toMatchObject({ status: 400 });
    await expect(client.listHazards({ since: latest.occurred_at, until: previous.occurred_at })).rejects.toMatchObject({ status: 400 });
  });

  it('pagination keeps its original snapshot when live events arrive and rejects changed filters', async () => {
    const client = createMockGatewayClient(internal, 0);
    const baseline = await client.listHazards({ page_size: 100 });
    const first = await client.listHazards({ page_size: 4 });
    expect(first.next_page_token).toBeTruthy();
    vi.useFakeTimers();
    const unsubscribe = client.subscribe(() => undefined);
    await vi.advanceTimersByTimeAsync(15_000);
    unsubscribe();
    vi.useRealTimers();
    const second = await client.listHazards({ page_size: 4, page_token: first.next_page_token });
    expect(second.items.map((item) => item.hazard_id)).toEqual(baseline.items.slice(4, 8).map((item) => item.hazard_id));
    await expect(client.listHazards({ page_size: 5, page_token: first.next_page_token })).rejects.toMatchObject({ status: 400 });
    await expect(client.listNotifications({ page_size: 4, page_token: first.next_page_token })).rejects.toMatchObject({ status: 400 });
    const newPage = await client.listHazards();
    expect(newPage.items[0].hazard_id).toMatch(/live/);
  });

  it('expires snapshots and returns 409 instead of silently paging a different dataset', async () => {
    const client = createMockGatewayClient(media, 0);
    const first = await client.listHazards({ page_size: 4 });
    vi.useFakeTimers();
    vi.setSystemTime(Date.now() + 301_000);
    const promise = client.listHazards({ page_size: 4, page_token: first.next_page_token });
    const assertion = expect(promise).rejects.toMatchObject({ status: 409 });
    await vi.advanceTimersByTimeAsync(1);
    await assertion;
  });

  it('PVMBG degradation preserves cached records and selected-source freshness semantics', async () => {
    const client = createMockGatewayClient(media, 0);
    client.setScenario('stale');
    const mixed = await client.listHazards({ page_size: 100 });
    expect(mixed.items).toHaveLength(24);
    expect(mixed.partial).toBe(true);
    expect(mixed.freshness.find((entry) => entry.source === 'PVMBG')).toMatchObject({ status: 'STALE', stale_since: expect.any(String) });
    const bmkg = await client.listHazards({ source: 'BMKG' });
    expect(bmkg.partial).toBe(false);
    const unknownRange = await client.listHazards({ source: 'PVMBG', since: new Date(Date.now() + 100_000).toISOString() });
    expect(unknownRange.items).toEqual([]);
    expect(unknownRange.freshness[0].source).toBe('PVMBG');
    expect(unknownRange.partial).toBe(true);
  });

  it('distinguishes a usable empty dataset from unavailable data and supports recovery', async () => {
    const client = createMockGatewayClient(media, 0);
    client.setScenario('empty');
    expect(await client.listHazards()).toMatchObject({ items: [], partial: false });
    client.setScenario('error');
    await expect(client.listHazards()).rejects.toMatchObject({ status: 503 });
    expect((await client.getSourceStatuses()).freshness.every((entry) => entry.status === 'UNAVAILABLE')).toBe(true);
    client.setScenario('normal');
    expect((await client.listHazards()).items).not.toHaveLength(0);
  });

  it('aborts pending requests without returning old results', async () => {
    const client = createMockGatewayClient(media);
    const controller = new AbortController();
    const pending = client.listHazards({}, { signal: controller.signal });
    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
  });
});

import { describe, expect, it } from 'vitest';
import type { HazardFull, HazardSummary } from '../types';
import { isNotificationWorthy, matchesFilters, upsertHazard } from './merge';

const hazard = (id: string, minutesAgo: number, overrides: Partial<HazardSummary> = {}): HazardSummary => ({
  hazard_id: id, source: 'BMKG', hazard_type: 'SEISMIC', severity: 'SIAGA', area_name: `Area ${id}`,
  occurred_at: new Date(Date.UTC(2026, 9, 9, 12, 0) - minutesAgo * 60_000).toISOString(),
  ingested_at: new Date(Date.UTC(2026, 9, 9, 12, 0) - minutesAgo * 60_000 + 1000).toISOString(),
  ...overrides,
});

describe('live feed merge', () => {
  it('inserts a new matching hazard in Gateway order', () => {
    const items = [hazard('a', 1), hazard('c', 10)];
    const next = upsertHazard(items, hazard('b', 5), {});
    expect(next.map(item => item.hazard_id)).toEqual(['a', 'b', 'c']);
    expect(upsertHazard(items, hazard('z', 1), {}).map(item => item.hazard_id)).toEqual(['a', 'z', 'c']);
    expect(upsertHazard(items, hazard('old', 60), {}).at(-1)?.hazard_id).toBe('old');
  });

  it('replaces an existing row with a newer version and keeps full-only fields', () => {
    const full: HazardFull = { ...hazard('a', 1), source_ref_id: 'ref', latitude: 1, longitude: 2, attributes: {} };
    const update = hazard('a', 1, { severity: 'AWAS', ingested_at: new Date(Date.parse(full.ingested_at) + 5000).toISOString() });
    const [merged] = upsertHazard([full], update, {});
    expect(merged).toMatchObject({ severity: 'AWAS', source_ref_id: 'ref' });
  });

  it('ignores an event older than the row already shown', () => {
    const items = [hazard('a', 1, { severity: 'AWAS' })];
    const stale = hazard('a', 1, { severity: 'WASPADA', ingested_at: new Date(Date.parse(items[0].ingested_at) - 1000).toISOString() });
    expect(upsertHazard(items, stale, {})).toBe(items);
  });

  it('removes a row whose update leaves the active filters and skips non-matching inserts', () => {
    const items = [hazard('a', 1)];
    const update = hazard('a', 1, { severity: 'NORMAL', ingested_at: new Date(Date.parse(items[0].ingested_at) + 1).toISOString() });
    expect(upsertHazard(items, update, { severity: 'SIAGA' })).toEqual([]);
    expect(upsertHazard(items, hazard('b', 1, { source: 'PVMBG' }), { source: 'BMKG' })).toBe(items);
  });

  it('applies time window filters to occurred_at', () => {
    const since = new Date(Date.UTC(2026, 9, 9, 11, 0)).toISOString();
    expect(matchesFilters(hazard('a', 30), { since })).toBe(true);
    expect(matchesFilters(hazard('a', 90), { since })).toBe(false);
  });

  it('marks only Siaga and Awas as notification-worthy', () => {
    expect(isNotificationWorthy(hazard('a', 1, { severity: 'AWAS' }))).toBe(true);
    expect(isNotificationWorthy(hazard('a', 1, { severity: 'WASPADA' }))).toBe(false);
  });
});

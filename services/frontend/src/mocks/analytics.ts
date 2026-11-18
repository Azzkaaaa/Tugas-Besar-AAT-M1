import type { AnalyticsFilters, AnalyticsResult, HazardFull } from '../types';

export function fixtureAnalytics(hazards: HazardFull[], filters: AnalyticsFilters): AnalyticsResult {
  const result: AnalyticsResult = { total: 0, categories: { SEISMIC: 0, TSUNAMI: 0, VOLCANIC: 0, WILDFIRE: 0, COASTAL: 0 }, severity: { NORMAL: 0, WASPADA: 0, SIAGA: 0, AWAS: 0 }, areas: [], series: [], partial: false, unavailable_units: [], as_of: new Date().toISOString() };
  const areas = new Map<string, number>();
  const series = new Map<string, Record<string, number>>();
  for (const hazard of hazards) {
    if (hazard.attributes.withdrawn === true || filters.source && hazard.source !== filters.source || filters.since && hazard.occurred_at < filters.since || filters.until && hazard.occurred_at >= filters.until) continue;
    result.total++; result.categories[hazard.hazard_type]++; result.severity[hazard.severity]++;
    const tsunami = hazard.hazard_type === 'SEISMIC' && Array.isArray(hazard.attributes.tsunami_warnings) && hazard.attributes.tsunami_warnings.length > 0;
    if (tsunami) result.categories.TSUNAMI++;
    areas.set(hazard.area_name, (areas.get(hazard.area_name) ?? 0) + 1);
    const bucket = new Date(hazard.occurred_at);
    bucket.setUTCMinutes(0, 0, 0);
    if (filters.interval !== 'hour') bucket.setUTCHours(0);
    if (filters.interval === 'month' || filters.interval === 'year') bucket.setUTCDate(1);
    if (filters.interval === 'year') bucket.setUTCMonth(0);
    const key = bucket.toISOString();
    const counts = series.get(key) ?? {};
    counts[hazard.hazard_type] = (counts[hazard.hazard_type] ?? 0) + 1;
    if (tsunami) counts.TSUNAMI = (counts.TSUNAMI ?? 0) + 1;
    series.set(key, counts);
  }
  result.areas = [...areas].map(([area_name, count]) => ({ area_name, count })).sort((a, b) => b.count - a.count || a.area_name.localeCompare(b.area_name));
  result.series = [...series].map(([bucket, counts]) => ({ bucket, counts })).sort((a, b) => a.bucket.localeCompare(b.bucket));
  return result;
}

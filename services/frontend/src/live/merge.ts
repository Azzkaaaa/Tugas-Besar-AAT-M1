import type { Hazard, HazardFilters } from '../types';

/** Mirrors the Gateway list filters the dashboard sends; until/hazard_type are not used there. */
export function matchesFilters(hazard: Hazard, filters: HazardFilters): boolean {
  if (filters.source && hazard.source !== filters.source) return false;
  if (filters.hazard_type && hazard.hazard_type !== filters.hazard_type) return false;
  if (filters.severity && hazard.severity !== filters.severity) return false;
  const occurred = Date.parse(hazard.occurred_at);
  if (filters.since && occurred < Date.parse(filters.since)) return false;
  if (filters.until && occurred >= Date.parse(filters.until)) return false;
  return true;
}

/** Same order as the Gateway: occurred_at descending, hazard_id ascending. */
function compare(left: Hazard, right: Hazard): number {
  return Date.parse(right.occurred_at) - Date.parse(left.occurred_at) || left.hazard_id.localeCompare(right.hazard_id);
}

/**
 * Apply one feed event to the first page. Returns the same array when nothing changed.
 * An event older than the row already shown (by ingested_at) never overwrites it, so a REST
 * snapshot fetched after the event was published stays authoritative. The page may grow past
 * page_size; trimming would drop rows that the frozen next_page_token snapshot does not contain.
 */
export function upsertHazard(items: Hazard[], hazard: Hazard, filters: HazardFilters): Hazard[] {
  const index = items.findIndex(item => item.hazard_id === hazard.hazard_id);
  const existing = index >= 0 ? items[index] : undefined;
  if (existing && Date.parse(hazard.ingested_at) < Date.parse(existing.ingested_at)) return items;
  const rest = existing ? items.filter((_, position) => position !== index) : items;
  if (!matchesFilters(hazard, filters)) return existing ? rest : items;
  // Keep fields from a full projection that the summary feed event does not carry.
  const next = existing ? { ...existing, ...hazard } : hazard;
  const position = rest.findIndex(item => compare(next, item) < 0);
  return position < 0 ? [...rest, next] : [...rest.slice(0, position), next, ...rest.slice(position)];
}

/** Only these levels create danger notifications (notification.proto). */
export function isNotificationWorthy(hazard: Hazard): boolean {
  return hazard.severity === 'SIAGA' || hazard.severity === 'AWAS';
}

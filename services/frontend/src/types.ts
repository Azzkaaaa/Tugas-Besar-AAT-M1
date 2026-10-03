export type Role = 'MEDIA' | 'FIELD_TEAM' | 'BNPB_INTERNAL';
export type Source = 'BMKG' | 'PVMBG';
export type HazardType = 'SEISMIC' | 'VOLCANIC';
export type Severity = 'NORMAL' | 'WASPADA' | 'SIAGA' | 'AWAS';
export type HazardView = 'summary' | 'full';
export type DemoScenario = 'normal' | 'stale' | 'empty' | 'error';
export type FeedStatus = 'connecting' | 'connected' | 'disconnected';

export interface AuthSession {
  id: string;
  name: string;
  role: Role;
  scopes: string[];
  mode: 'demo';
}

/** Keep this projection to exactly these seven fields (Gateway contract). */
export interface HazardSummary {
  hazard_id: string;
  source: Source;
  hazard_type: HazardType;
  severity: Severity;
  area_name: string;
  occurred_at: string;
  ingested_at: string;
}

export interface HazardFull extends HazardSummary {
  source_ref_id: string;
  latitude: number;
  longitude: number;
  attributes: Record<string, unknown>;
}

export type Hazard = HazardSummary | HazardFull;

export interface Freshness {
  source: Source;
  status: 'FRESH' | 'STALE' | 'UNAVAILABLE';
  last_success_at?: string;
  stale_since?: string;
}

export interface HazardPage {
  items: Hazard[];
  next_page_token?: string;
  freshness: Freshness[];
  partial: boolean;
}

export interface HazardResult {
  item: Hazard;
  freshness: Freshness[];
  partial: boolean;
}

export interface Notification {
  notification_id: string;
  hazard: Hazard;
  created_at: string;
  message: string;
  push_status?: 'PENDING' | 'DELIVERED' | 'FAILED';
}

export interface NotificationPage {
  items: Notification[];
  next_page_token?: string;
  freshness: Freshness[];
  partial: boolean;
}

export interface HazardFeedEvent {
  event_id: string;
  sequence: string;
  hazard: Hazard;
  published_at: string;
}

export interface HazardFilters {
  source?: Source;
  hazard_type?: HazardType;
  severity?: Severity;
  since?: string;
  until?: string;
  view?: HazardView;
  page_size?: number;
  page_token?: string;
}

export interface RequestOptions {
  signal?: AbortSignal;
}

export interface GatewayClient {
  listHazards(filters?: HazardFilters, options?: RequestOptions): Promise<HazardPage>;
  getHazard(id: string, view?: HazardView, options?: RequestOptions): Promise<HazardResult>;
  listNotifications(filters?: HazardFilters, options?: RequestOptions): Promise<NotificationPage>;
  getSourceStatuses(options?: RequestOptions): Promise<{ freshness: Freshness[] }>;
  /** Events invalidate displayed snapshots; do not overwrite them with older payloads. */
  subscribe(onEvent: (event: HazardFeedEvent) => void, onStatus?: (status: FeedStatus) => void, onReset?: () => void): () => void;
  /** Only the development fixture adapter supports scenario controls. */
  setScenario(scenario: DemoScenario): void;
}

export interface Problem {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance: string;
  correlation_id: string;
}

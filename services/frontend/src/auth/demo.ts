import { isDemoMode } from '../config';
import type { AuthSession, Role, WebhookView } from '../types';

export const roleLabels: Record<Role, string> = {
  MEDIA: 'Media',
  FIELD_TEAM: 'Tim Lapangan',
  BNPB_INTERNAL: 'BNPB Pusat',
};

const accounts: AuthSession[] = [
  { id: 'demo-media', name: 'Ruang Media', role: 'MEDIA', scopes: ['hazards:read:summary', 'notifications:read', 'webhooks:read'], mode: 'demo' },
  { id: 'demo-field', name: 'Tim Lapangan', role: 'FIELD_TEAM', scopes: ['hazards:read:summary', 'hazards:read:full', 'notifications:read', 'webhooks:read', 'webhooks:write'], mode: 'demo' },
  { id: 'demo-bnpb', name: 'Pusat Kendali BNPB', role: 'BNPB_INTERNAL', scopes: ['hazards:read:summary', 'hazards:read:full', 'notifications:read', 'webhooks:read', 'webhooks:write'], mode: 'demo' },
];

/** These are fixture identities, never credentials or bearer tokens. */
export const demoAccounts: AuthSession[] = isDemoMode ? accounts.map(copySession) : [];

function copySession(session: AuthSession): AuthSession {
  return { ...session, scopes: [...session.scopes] };
}

export function getDemoSession(role: Role): AuthSession {
  if (!isDemoMode) throw new Error('Mode simulasi tidak aktif.');
  const account = accounts.find((candidate) => candidate.role === role);
  if (!account) throw new Error('Identitas simulasi tidak dikenal.');
  return copySession(account);
}

export function canReadFull(session: Pick<AuthSession, 'scopes'>): boolean {
  return session.scopes.includes('hazards:read:summary') && session.scopes.includes('hazards:read:full');
}

export function canManageWebhooks(session: Pick<AuthSession, 'role' | 'scopes'>): boolean {
  return session.role === 'FIELD_TEAM' || session.role === 'BNPB_INTERNAL' || session.scopes.includes('webhooks:write');
}

export function getAllowedWebhookViews(session: Pick<AuthSession, 'role' | 'scopes'>): WebhookView[] {
  if (session.role === 'FIELD_TEAM' || session.role === 'BNPB_INTERNAL' || canReadFull(session)) {
    return ['summary', 'full'];
  }
  return ['summary'];
}

export function getDefaultWebhookView(session: Pick<AuthSession, 'role' | 'scopes'>): WebhookView {
  if (session.role === 'FIELD_TEAM' || session.role === 'BNPB_INTERNAL' || canReadFull(session)) {
    return 'full';
  }
  return 'summary';
}


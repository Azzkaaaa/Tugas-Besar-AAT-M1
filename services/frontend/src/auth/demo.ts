import { isDemoMode } from '../config';
import type { AuthSession, Role } from '../types';

export const roleLabels: Record<Role, string> = {
  MEDIA: 'Media',
  FIELD_TEAM: 'Tim Lapangan',
  BNPB_INTERNAL: 'BNPB Pusat',
};

const accounts: AuthSession[] = [
  { id: 'demo-media', name: 'Ruang Media', role: 'MEDIA', scopes: ['hazards:read:summary', 'notifications:read'], mode: 'demo' },
  { id: 'demo-field', name: 'Tim Lapangan', role: 'FIELD_TEAM', scopes: ['hazards:read:summary', 'hazards:read:full', 'notifications:read'], mode: 'demo' },
  { id: 'demo-bnpb', name: 'Pusat Kendali BNPB', role: 'BNPB_INTERNAL', scopes: ['hazards:read:summary', 'hazards:read:full', 'notifications:read'], mode: 'demo' },
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

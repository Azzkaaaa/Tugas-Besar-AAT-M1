import { createHmac, timingSafeEqual } from 'node:crypto';

// Single source of the M1 permission matrix. Zitadel appends these claims to
// userinfo, introspection and JWT access tokens through the Actions v2 target.
const scopesByRole = {
  MEDIA: ['hazards:read:summary', 'notifications:read', 'webhooks:read'],
  FIELD_TEAM: ['hazards:read:summary', 'hazards:read:full', 'notifications:read', 'webhooks:read', 'webhooks:write'],
  BNPB_INTERNAL: ['hazards:read:summary', 'hazards:read:full', 'notifications:read', 'webhooks:read', 'webhooks:write'],
};
export const roles = ['BNPB_INTERNAL', 'FIELD_TEAM', 'MEDIA'];
const activeGrant = 1; // domain.UserGrantStateActive
const tolerance = 5 * 60 * 1000;

/** Verifies `ZITADEL-Signature: t=<unix>,v1=<hex>` where v1 = HMAC-SHA256(key, "<t>." + body). */
export function verifySignature(body, header, key, now = Date.now()) {
  if (typeof header !== 'string' || !key) return false;
  let timestamp;
  const signatures = [];
  for (const part of header.split(',')) {
    const [name, value] = part.trim().split('=');
    if (name === 't') timestamp = value;
    else if (name === 'v1' && /^[a-f\d]+$/i.test(value || '')) signatures.push(Buffer.from(value, 'hex'));
  }
  if (!/^\d+$/.test(timestamp || '') || Math.abs(now - Number(timestamp) * 1000) > tolerance) return false;
  const expected = createHmac('sha256', key).update(`${timestamp}.`).update(body).digest();
  return signatures.some(actual => actual.length === expected.length && timingSafeEqual(actual, expected));
}

/** Builds the Actions v2 response for a preuserinfo/preaccesstoken function call. */
export function claimsFor(payload, oidc) {
  const clients = [oidc.client_id, oidc.introspection_client_id, oidc.gateway_client_id];
  // Executions are instance-wide; leave tokens of unrelated applications untouched.
  if (!clients.includes(payload?.application?.client_id)) return {};
  const granted = new Set((Array.isArray(payload.user_grants) ? payload.user_grants : [])
    .filter(grant => grant?.project_id === oidc.project_id && grant.resource_owner === oidc.organization_id
      && (grant.state === undefined || grant.state === activeGrant))
    .flatMap(grant => Array.isArray(grant.roles) ? grant.roles : []));
  const role = roles.find(candidate => granted.has(candidate));
  if (!role) return {};
  return { append_claims: [{ key: 'role', value: role }, { key: 'scopes', value: [...scopesByRole[role]] }] };
}

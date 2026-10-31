import { readFileSync } from 'node:fs';

export class ProvisioningPending extends Error {}

export function loadConfig(env = process.env) {
  const allowHttp = env.AUTH_ALLOW_HTTP === 'true';
  function publicUrl(value, name) {
    const url = new URL(value);
    if (url.username || url.password || url.search || url.hash || url.pathname !== '/') throw new Error(`Invalid ${name}`);
    const loopback = url.hostname === 'localhost' || url.hostname.endsWith('.localhost') || url.hostname === '127.0.0.1';
    if (url.protocol !== 'https:' && !(allowHttp && loopback && url.protocol === 'http:')) throw new Error(`${name} requires HTTPS (local HTTP needs AUTH_ALLOW_HTTP=true)`);
    return url.origin;
  }
  const appUrl = publicUrl(env.APP_PUBLIC_URL || 'http://localhost:8080', 'APP_PUBLIC_URL');
  let oidc;
  try { oidc = JSON.parse(readFileSync(env.OIDC_CONFIG_FILE || '/config/oidc.json', 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') throw new ProvisioningPending('OIDC configuration not provisioned yet'); throw error; }
  const issuer = publicUrl(env.OIDC_ISSUER || oidc.issuer, 'OIDC_ISSUER');
  if (oidc.issuer !== issuer) throw new Error('Provisioned issuer differs from OIDC_ISSUER');
  for (const key of ['client_id', 'client_secret', 'introspection_client_id', 'introspection_client_secret', 'gateway_client_id',
    'project_id', 'organization_id']) {
    if (typeof oidc[key] !== 'string' || !oidc[key]) throw new ProvisioningPending(`Missing OIDC ${key}`);
  }
  if (!/^[a-f\d]{64}$/i.test(env.AUTH_SESSION_KEY || '')) throw new Error('AUTH_SESSION_KEY must contain 64 hex characters');
  const sessionTtl = Number(env.AUTH_SESSION_TTL_SECONDS || 86400);
  if (!Number.isSafeInteger(sessionTtl) || sessionTtl < 60) throw new Error('Invalid AUTH_SESSION_TTL_SECONDS');
  const secureCookies = env.AUTH_COOKIE_SECURE !== 'false';
  if (!secureCookies && (!allowHttp || !appUrl.startsWith('http:'))) throw new Error('Insecure cookies are only supported for explicit local HTTP');
  return { appUrl, issuer, oidc, allowHttp, secureCookies, sessionTtl, sessionKey: env.AUTH_SESSION_KEY,
    databasePath: env.AUTH_DATABASE_PATH || '/data/sessions.sqlite', redisUrl: env.AUTH_REDIS_URL || '', port: Number(env.PORT || 3001) };
}

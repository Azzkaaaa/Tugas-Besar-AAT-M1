import * as oidc from 'openid-client';
import { AsyncLocalStorage } from 'node:async_hooks';

export const requestContext = new AsyncLocalStorage();
export class AuthError extends Error {
  constructor(status, code) { super(code); this.status = status; this.code = code; }
}

// Precedence and scopes live in the zitadel-claims service; this only checks the role is known.
const roles = ['BNPB_INTERNAL', 'FIELD_TEAM', 'MEDIA'];

/** Role and scopes are appended by the zitadel-claims Actions v2 target (services/zitadel-claims). */
export function identityFromClaims(claims, config) {
  const audiences = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
  if (claims.active !== true || typeof claims.sub !== 'string' || !claims.sub || !Number.isFinite(claims.exp) || claims.exp * 1000 <= Date.now()
    || !audiences.includes(config.oidc.project_id) || (claims.iss && claims.iss !== config.issuer)) throw new AuthError(401, 'invalid_token');
  const { role, scopes } = claims;
  if (!roles.includes(role) || !Array.isArray(scopes) || !scopes.length || !scopes.every(scope => typeof scope === 'string')) {
    throw new AuthError(403, 'role_required');
  }
  return { user: { id: claims.sub, name: claims.name || claims.preferred_username || claims.sub, role, scopes: [...scopes], mode: 'oidc' }, expiresAt: claims.exp * 1000 };
}

export class ZitadelProvider {
  constructor(config, logger = console.log) { this.config = config; this.logger = logger; }
  async clients() {
    if (!this.pending) this.pending = this.discover().catch(error => { this.pending = null; throw error; });
    return this.pending;
  }
  async discover() {
    const timedFetch = async (input, options) => {
      const start = performance.now();
      const url = new URL(input instanceof Request ? input.url : input);
      let status = 0;
      try {
        const response = await fetch(input, { ...options, signal: options?.signal
          ? AbortSignal.any([options.signal, AbortSignal.timeout(10000)]) : AbortSignal.timeout(10000) });
        status = response.status;
        return response;
      } finally {
        this.logger(JSON.stringify({ event: 'oidc_request', path: url.pathname, status,
          duration_ms: Math.round(performance.now() - start), correlation_id: requestContext.getStore()?.correlationId }));
      }
    };
    const cfg = this.config;
    const execute = [oidc.enableNonRepudiationChecks];
    if (cfg.allowHttp) execute.push(oidc.allowInsecureRequests);
    const web = await oidc.discovery(new URL(cfg.issuer), cfg.oidc.client_id, cfg.oidc.client_secret,
      oidc.ClientSecretBasic(cfg.oidc.client_secret), { execute, [oidc.customFetch]: timedFetch });
    const api = new oidc.Configuration(web.serverMetadata(), cfg.oidc.introspection_client_id,
      cfg.oidc.introspection_client_secret, oidc.ClientSecretBasic(cfg.oidc.introspection_client_secret));
    api[oidc.customFetch] = timedFetch;
    if (cfg.allowHttp) oidc.allowInsecureRequests(api);
    return { web, api };
  }
  async begin() {
    const { web } = await this.clients();
    const verifier = oidc.randomPKCECodeVerifier();
    const state = oidc.randomState();
    const nonce = oidc.randomNonce();
    const scope = ['openid', 'profile', 'email', 'offline_access',
      `urn:zitadel:iam:org:id:${this.config.oidc.organization_id}`,
      `urn:zitadel:iam:org:project:id:${this.config.oidc.project_id}:aud`, 'urn:zitadel:iam:org:projects:roles'].join(' ');
    const url = oidc.buildAuthorizationUrl(web, { redirect_uri: `${this.config.appUrl}/auth/callback`, scope,
      state, nonce, code_challenge: await oidc.calculatePKCECodeChallenge(verifier), code_challenge_method: 'S256' });
    return { url: url.href, state, nonce, verifier };
  }
  async inspect(token) {
    const { api } = await this.clients();
    const claims = await oidc.tokenIntrospection(api, token);
    return identityFromClaims(claims, this.config);
  }
  async complete(url, pending) {
    const { web } = await this.clients();
    const tokens = await oidc.authorizationCodeGrant(web, url, { pkceCodeVerifier: pending.verifier,
      expectedState: pending.state, expectedNonce: pending.nonce, idTokenExpected: true });
    const identity = await this.inspect(tokens.access_token);
    const claims = tokens.claims();
    if (identity.user.id !== claims.sub || !tokens.refresh_token) throw new AuthError(401, 'invalid_identity');
    identity.user.name = claims.name || claims.preferred_username || identity.user.name;
    return { ...identity, accessToken: tokens.access_token, refreshToken: tokens.refresh_token };
  }
  async refresh(session) {
    const { web } = await this.clients();
    const tokens = await oidc.refreshTokenGrant(web, session.refreshToken);
    const identity = await this.inspect(tokens.access_token);
    if (identity.user.id !== session.user.id || (tokens.claims() && tokens.claims().sub !== session.user.id)) throw new AuthError(401, 'invalid_identity');
    if (identity.user.name === identity.user.id) identity.user.name = tokens.claims()?.name || session.user.name;
    return { ...identity, accessToken: tokens.access_token, refreshToken: tokens.refresh_token || session.refreshToken };
  }
  async revoke(session) {
    const { web } = await this.clients();
    await Promise.allSettled([
      oidc.tokenRevocation(web, session.refreshToken, { token_type_hint: 'refresh_token' }),
      oidc.tokenRevocation(web, session.accessToken, { token_type_hint: 'access_token' }),
    ]);
  }
  // The gateway validates tokens at Zitadel, so a replaced access token must be revoked there too.
  async revokeAccessToken(token) {
    const { web } = await this.clients();
    await oidc.tokenRevocation(web, token, { token_type_hint: 'access_token' });
  }
  async logoutUrl() {
    const { web } = await this.clients();
    return oidc.buildEndSessionUrl(web, { post_logout_redirect_uri: `${this.config.appUrl}/` }).href;
  }
}

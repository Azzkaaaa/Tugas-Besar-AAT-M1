import { createServer } from 'node:http';
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { parseCookie, stringifySetCookie } from 'cookie';
import { setTimeout as delay } from 'node:timers/promises';
import { loadConfig, ProvisioningPending } from './config.mjs';
import { SessionStore, randomSecret, equal } from './store.mjs';
import { ZitadelProvider, AuthError, requestContext } from './provider.mjs';

const payload = session => ({ user: session.user, csrf_token: session.csrf,
  expires_at: new Date(session.expiresAt).toISOString() });

export function createAuthServer(config, { store = new SessionStore(config.databasePath, config.sessionKey),
  provider = new ZitadelProvider(config), logger = console.log } = {}) {
  const refreshes = new Map();
  function cookie(name, value, seconds) {
    return stringifySetCookie({ name, value, maxAge: Math.max(0, Math.floor(seconds)), path: '/', httpOnly: true,
      sameSite: 'lax', secure: config.secureCookies });
  }
  function sessionCookies(res, id, session) {
    if (Buffer.byteLength(session.accessToken) > 3500) throw new AuthError(502, 'token_too_large');
    res.setHeader('Set-Cookie', [cookie('bnpb_access', session.accessToken, (session.expiresAt - Date.now()) / 1000),
      cookie('bnpb_session', id, (session.sessionExpires - Date.now()) / 1000)]);
  }
  function clearCookies(res) {
    res.setHeader('Set-Cookie', ['bnpb_access', 'bnpb_session', 'bnpb_login'].map(name => cookie(name, '', 0)));
  }
  function json(res, status, body) {
    res.writeHead(status, { 'Content-Type': status >= 400 ? 'application/problem+json' : 'application/json' });
    res.end(JSON.stringify(body));
  }
  function problem(res, status, code, extras = {}) {
    json(res, status, { type: `urn:bnpb:auth:${code}`, title: code, status,
      detail: code, correlation_id: res.getHeader('X-Correlation-ID'), ...extras });
  }
  function csrf(req, session) {
    if (req.headers.origin !== config.appUrl || !equal(req.headers['x-csrf-token'], session.csrf)) throw new AuthError(403, 'csrf_failed');
  }
  async function inspect(token) {
    if (!token || store.isRevoked(token)) throw new AuthError(401, 'invalid_token');
    return provider.inspect(token);
  }
  async function handle(req, res) {
    // Construct from the configured origin, never from an untrusted Host header.
    if (!req.url?.startsWith('/') || req.url.startsWith('//')) throw new AuthError(400, 'invalid_path');
    const url = new URL(req.url, config.appUrl);
    const cookies = parseCookie(req.headers.cookie || '');
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'no-referrer');
    if (url.pathname === '/health' && req.method === 'GET') return json(res, 200, { status: 'ok' });
    if (url.pathname === '/auth/login' && req.method === 'GET') {
      const pending = await provider.begin();
      const binding = randomSecret();
      store.putLogin(pending.state, binding, pending);
      res.setHeader('Set-Cookie', cookie('bnpb_login', binding, 600));
      res.writeHead(302, { Location: pending.url });
      return res.end();
    }
    if (url.pathname === '/auth/callback' && req.method === 'GET') {
      res.setHeader('Set-Cookie', cookie('bnpb_login', '', 0));
      try {
        const pending = store.takeLogin(url.searchParams.get('state'), cookies.bnpb_login);
        if (!pending) throw new AuthError(401, 'invalid_state');
        const result = await provider.complete(url, pending);
        const session = { ...result, csrf: randomSecret(), sessionExpires: Date.now() + config.sessionTtl * 1000 };
        const id = randomSecret();
        sessionCookies(res, id, session);
        const previous = store.getSession(cookies.bnpb_session);
        if (previous) store.rotate(cookies.bnpb_session, previous, null);
        store.putSession(id, session);
        res.appendHeader('Set-Cookie', cookie('bnpb_login', '', 0));
        res.writeHead(302, { Location: `${config.appUrl}/#/dashboard` });
      } catch {
        res.writeHead(302, { Location: `${config.appUrl}/#/login?error=login_failed` });
      }
      return res.end();
    }
    if (url.pathname === '/auth/session' && req.method === 'GET') {
      const session = store.getSession(cookies.bnpb_session);
      if (!session) throw new AuthError(401, 'session_required');
      if (session.expiresAt <= Date.now() || !cookies.bnpb_access) {
        return problem(res, 401, 'refresh_required', { csrf_token: session.csrf, refresh_required: true });
      }
      if (!equal(cookies.bnpb_access, session.accessToken)) throw new AuthError(401, 'invalid_token');
      const identity = await inspect(session.accessToken);
      if (identity.user.id !== session.user.id) throw new AuthError(401, 'invalid_identity');
      if (identity.user.name === identity.user.id) identity.user.name = session.user.name;
      return json(res, 200, payload({ ...session, ...identity }));
    }
    if (url.pathname === '/auth/refresh' && req.method === 'POST') {
      const id = cookies.bnpb_session;
      const session = store.getSession(id);
      if (!session) throw new AuthError(401, 'session_required');
      csrf(req, session);
      // One provider refresh per session at a time, including concurrent tabs.
      if (!refreshes.has(id)) {
        const operation = (async () => {
          let tokens;
          try { tokens = await provider.refresh(session); }
          catch (error) {
            if (error instanceof AuthError || error.error === 'invalid_grant') {
              store.rotate(id, session, null);
              throw new AuthError(401, 'session_expired');
            }
            throw error;
          }
          if (!store.getSession(id)) { await provider.revoke(tokens); throw new AuthError(401, 'session_expired'); }
          if (Buffer.byteLength(tokens.accessToken) > 3500) throw new AuthError(502, 'token_too_large');
          const next = { ...session, ...tokens };
          store.rotate(id, session, next);
          provider.revokeAccessToken(session.accessToken).catch(() => { /* expires within the access TTL */ });
          return next;
        })().finally(() => refreshes.delete(id));
        refreshes.set(id, operation);
      }
      const next = await refreshes.get(id);
      sessionCookies(res, id, next);
      return json(res, 200, payload(next));
    }
    if (url.pathname === '/auth/logout' && req.method === 'POST') {
      const id = cookies.bnpb_session;
      let session = store.getSession(id);
      if (!session) throw new AuthError(401, 'session_required');
      csrf(req, session);
      if (refreshes.has(id)) await refreshes.get(id).catch(() => {});
      session = store.getSession(id) || session;
      store.rotate(id, session, null);
      clearCookies(res);
      // Local invalidation succeeds even if Zitadel is temporarily unreachable.
      let logoutUrl = `${config.appUrl}/`;
      try { logoutUrl = await provider.logoutUrl(); await provider.revoke(session); } catch { /* session already invalidated */ }
      return json(res, 200, { logout_url: logoutUrl });
    }
    throw new AuthError(404, 'not_found');
  }
  const server = createServer((req, res) => {
    const supplied = req.headers['x-correlation-id'];
    const correlationId = typeof supplied === 'string' && /^[\w.-]{1,100}$/.test(supplied) ? supplied : randomUUID();
    res.setHeader('X-Correlation-ID', correlationId);
    const start = performance.now();
    res.on('finish', () => logger(JSON.stringify({ event: 'http_request', method: req.method,
      path: req.url?.split('?')[0], status: res.statusCode, duration_ms: Math.round(performance.now() - start), correlation_id: correlationId })));
    requestContext.run({ correlationId }, () => handle(req, res).catch(error => {
      if (res.headersSent) return res.destroy();
      if (error instanceof AuthError) problem(res, error.status, error.code);
      else problem(res, 503, 'dependency_unavailable');
    }));
  });
  server.requestTimeout = 15000;
  server.headersTimeout = 10000;
  const cleanup = setInterval(() => store.cleanup(), 60000);
  cleanup.unref();
  server.on('close', () => clearInterval(cleanup));
  return { server, store };
}

// auth-bootstrap writes oidc.json after Zitadel is provisioned; start first and wait for the file.
async function provisionedConfig() {
  for (let attempt = 0; ; attempt++) {
    try { return loadConfig(); }
    catch (error) {
      if (!(error instanceof ProvisioningPending) || attempt >= 300) throw error;
      if (attempt === 0) console.log(JSON.stringify({ event: 'waiting_for_provisioning' }));
      await delay(2000);
    }
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const config = await provisionedConfig();
    const { server, store } = createAuthServer(config);
    server.listen(config.port, '0.0.0.0', () => console.log(JSON.stringify({ event: 'started', port: config.port })));
    for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { server.close(() => { store.close(); process.exit(0); }); server.closeAllConnections(); });
  } catch {
    console.error('Auth bridge could not start. Check private configuration and database access.');
    process.exitCode = 1;
  }
}

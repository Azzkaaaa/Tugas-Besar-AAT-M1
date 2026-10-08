import { createServer } from 'node:http';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { parseCookie, stringifySetCookie } from 'cookie';
import { loadConfig } from './config.mjs';
import { SessionStore, randomSecret, equal, hash } from './store.mjs';
import { ZitadelProvider, AuthError, requestContext } from './provider.mjs';

const payload = session => ({ user: session.user, csrf_token: session.csrf,
  expires_at: new Date(session.expiresAt).toISOString() });

export function requiredScopes(url, method) {
  const path = url.pathname;
  const views = url.searchParams.getAll('view');
  if (views.length > 1 || (views.length && !['summary', 'full'].includes(views[0]))) throw new AuthError(400, 'invalid_view');
  let scopes;
  if (method === 'GET' && (/^\/api\/v1\/hazards(?:\/[^/]+)?$/.test(path) || path === '/api/v1/hazards/sources/statuses'
    || /^\/api\/v1\/feeds\/(events|seismic|volcanic)$/.test(path))) scopes = ['hazards:read:summary'];
  else if (method === 'GET' && /^\/api\/v1\/notifications(?:\/[^/]+)?$/.test(path)) scopes = ['hazards:read:summary', 'notifications:read'];
  else if (method === 'POST' && /^\/api\/v1\/notifications\/push\/(register|unregister)$/.test(path)) scopes = ['notifications:read'];
  else if (method === 'GET' && /^\/api\/v1\/webhooks(?:\/[^/]+(?:\/deliveries)?)?$/.test(path)) scopes = ['webhooks:read'];
  else if ((method === 'POST' && (path === '/api/v1/webhooks' || /^\/api\/v1\/webhooks\/[^/]+\/test$/.test(path)))
    || (method === 'DELETE' && /^\/api\/v1\/webhooks\/[^/]+$/.test(path))) scopes = ['webhooks:write'];
  else throw new AuthError(404, 'not_found');
  if (views[0] === 'full') scopes.push('hazards:read:full');
  return scopes;
}

export function createAuthServer(config, { store = new SessionStore(config.databasePath, config.sessionKey),
  provider = new ZitadelProvider(config), logger = console.log } = {}) {
  const refreshes = new Map();
  const streams = new Map();
  function closeStreams(key) {
    for (const abort of streams.get(key) || []) abort();
    streams.delete(key);
  }
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
  async function authenticate(req, cookies) {
    const authorization = req.headers.authorization;
    if (authorization) {
      if (cookies.bnpb_session || cookies.bnpb_access) throw new AuthError(400, 'ambiguous_credentials');
      if (!/^Bearer [^\s]+$/i.test(authorization)) throw new AuthError(401, 'invalid_token');
      const token = authorization.slice(7);
      return { ...await inspect(token), token, streamKey: hash(token) };
    }
    const session = store.getSession(cookies.bnpb_session);
    if (!session || !equal(cookies.bnpb_access, session.accessToken)) throw new AuthError(401, 'session_required');
    const identity = await inspect(session.accessToken);
    if (identity.user.id !== session.user.id) throw new AuthError(401, 'invalid_identity');
    if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method)) csrf(req, session);
    return { ...identity, token: session.accessToken, streamKey: hash(cookies.bnpb_session) };
  }
  async function body(req) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > 1024 * 1024) throw new AuthError(413, 'body_too_large');
      chunks.push(chunk);
    }
    return Buffer.concat(chunks);
  }
  async function proxy(req, res, url, cookies) {
    const scopes = requiredScopes(url, req.method);
    const identity = await authenticate(req, cookies);
    const authorized = user => scopes.every(scope => user.scopes.includes(scope));
    if (!authorized(identity.user)) throw new AuthError(403, 'insufficient_scope');
    if (!config.upstream) throw new AuthError(503, 'gateway_not_configured');
    const controller = new AbortController();
    const abort = () => controller.abort();
    const key = identity.streamKey;
    if (!streams.has(key)) streams.set(key, new Set());
    streams.get(key).add(abort);
    res.on('close', abort);
    const expiryTimer = setTimeout(abort, Math.max(1, identity.expiresAt - Date.now()));
    const timeout = setTimeout(abort, 10000);
    let recheck;
    const start = performance.now();
    let status = 0;
    try {
      const headers = { Authorization: `Bearer ${identity.token}`, 'X-Service-Token': config.serviceToken,
        'X-Correlation-ID': res.getHeader('X-Correlation-ID'), Host: 'api.localhost' };
      for (const name of ['accept', 'content-type', 'last-event-id', 'traceparent', 'tracestate']) {
        if (typeof req.headers[name] === 'string') headers[name] = req.headers[name];
      }
      const response = await fetch(new URL(url.pathname + url.search, config.upstream), {
        method: req.method, headers, signal: controller.signal, redirect: 'manual',
        ...(!['GET', 'HEAD'].includes(req.method) ? { body: await body(req) } : {}),
      });
      status = response.status;
      if (status >= 300 && status < 400) throw new AuthError(502, 'invalid_upstream_redirect');
      if (status >= 400) {
        await response.body?.cancel();
        problem(res, [400, 401, 403, 404, 409, 429, 503].includes(status) ? status : 502, 'gateway_request_failed');
        return;
      }
      const contentType = response.headers.get('content-type') || '';
      const streaming = contentType.startsWith('text/event-stream');
      if (streaming) {
        clearTimeout(timeout);
        let checking = false;
        recheck = setInterval(async () => {
          if (checking) return;
          checking = true;
          try { if (!authorized((await inspect(identity.token)).user)) abort(); } catch { abort(); }
          finally { checking = false; }
        }, 15000);
      }
      res.writeHead(status, { 'Content-Type': contentType || 'application/json', ...(streaming ? { 'X-Accel-Buffering': 'no' } : {}) });
      if (streaming) res.flushHeaders();
      if (response.body) for await (const chunk of response.body) {
        if (!res.write(chunk)) await once(res, 'drain', { signal: controller.signal });
      }
      res.end();
    } finally {
      clearTimeout(timeout);
      clearTimeout(expiryTimer);
      clearInterval(recheck);
      streams.get(key)?.delete(abort);
      if (!streams.get(key)?.size) streams.delete(key);
      res.off('close', abort);
      logger(JSON.stringify({ event: 'gateway_request', path: url.pathname, status,
        duration_ms: Math.round(performance.now() - start), correlation_id: res.getHeader('X-Correlation-ID') }));
    }
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
        if (previous) { store.rotate(cookies.bnpb_session, previous, null); closeStreams(hash(cookies.bnpb_session)); }
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
              store.rotate(id, session, null); closeStreams(hash(id));
              throw new AuthError(401, 'session_expired');
            }
            throw error;
          }
          if (!store.getSession(id)) { await provider.revoke(tokens); throw new AuthError(401, 'session_expired'); }
          if (Buffer.byteLength(tokens.accessToken) > 3500) throw new AuthError(502, 'token_too_large');
          const next = { ...session, ...tokens };
          store.rotate(id, session, next);
          closeStreams(hash(id)); closeStreams(hash(session.accessToken));
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
      closeStreams(hash(id)); closeStreams(hash(session.accessToken));
      clearCookies(res);
      // Local invalidation succeeds even if Zitadel is temporarily unreachable.
      let logoutUrl = `${config.appUrl}/`;
      try { logoutUrl = await provider.logoutUrl(); await provider.revoke(session); } catch { /* session already invalidated */ }
      return json(res, 200, { logout_url: logoutUrl });
    }
    if (url.pathname.startsWith('/api/v1/')) return proxy(req, res, url, cookies);
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
  server.on('close', () => { clearInterval(cleanup); for (const key of streams.keys()) closeStreams(key); });
  return { server, store };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const config = loadConfig();
    const { server, store } = createAuthServer(config);
    server.listen(config.port, '0.0.0.0', () => console.log(JSON.stringify({ event: 'started', port: config.port })));
    for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { server.close(() => { store.close(); process.exit(0); }); server.closeAllConnections(); });
  } catch {
    console.error('Auth bridge could not start. Check private configuration and database access.');
    process.exitCode = 1;
  }
}

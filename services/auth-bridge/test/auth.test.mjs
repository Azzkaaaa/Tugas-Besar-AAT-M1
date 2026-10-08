import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { generateKeyPairSync, sign, createHash, randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createAuthServer } from '../src/server.mjs';
import { SessionStore } from '../src/store.mjs';
import { ZitadelProvider } from '../src/provider.mjs';

const { privateKey, publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
const { privateKey: wrongKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
const encoded = value => Buffer.from(JSON.stringify(value)).toString('base64url');
function jwt(claims, invalid = false) {
  const input = `${encoded({ alg: 'RS256', kid: 'test-key' })}.${encoded(claims)}`;
  return `${input}.${sign('RSA-SHA256', Buffer.from(input), invalid ? wrongKey : privateKey).toString('base64url')}`;
}
async function listen(server) { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return `http://127.0.0.1:${server.address().port}`; }
async function stop(server) { const done = new Promise(resolve => server.close(resolve)); server.closeAllConnections(); await done; }

// A controlled, signed OIDC provider exercises the real openid-client library.
// This is not a substitute for the separate Docker/Zitadel acceptance test.
async function fixture(t, options = {}) {
  const codes = new Map(), tokens = new Map(), refreshTokens = new Map();
  const state = { role: 'FIELD_TEAM', ttl: 60, ...options, exchanges: 0, refreshes: 0, upstream: [], logs: [] };
  let issuer;
  function issue(nonce) {
    const access = randomBytes(24).toString('base64url');
    const refresh = randomBytes(24).toString('base64url');
    const exp = Math.floor(Date.now() / 1000) + state.ttl;
    const claims = { active: true, iss: issuer, sub: 'user-test', aud: [state.audience || 'project-test'], exp,
      name: 'User Test', 'urn:zitadel:iam:org:project:project-test:roles': { [state.role]: { [state.organization || 'org-test']: 'test.localhost' } } };
    tokens.set(access, claims); refreshTokens.set(refresh, true);
    return { access_token: access, refresh_token: refresh, token_type: 'Bearer', expires_in: state.ttl,
      id_token: jwt({ iss: issuer, sub: 'user-test', aud: 'web-test', iat: Math.floor(Date.now() / 1000), exp,
        ...(nonce ? { nonce: state.badNonce ? 'wrong-nonce' : nonce } : {}) }, state.badSignature) };
  }
  const idp = createServer(async (req, res) => {
    const url = new URL(req.url, issuer);
    const json = (status, value) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(value)); };
    if (url.pathname === '/.well-known/openid-configuration') return json(200, {
      issuer, authorization_endpoint: `${issuer}/authorize`, token_endpoint: `${issuer}/token`,
      introspection_endpoint: `${issuer}/introspect`, revocation_endpoint: `${issuer}/revoke`,
      jwks_uri: `${issuer}/jwks`, end_session_endpoint: `${issuer}/logout`, response_types_supported: ['code'],
      subject_types_supported: ['public'], id_token_signing_alg_values_supported: ['RS256'],
      token_endpoint_auth_methods_supported: ['client_secret_basic'], code_challenge_methods_supported: ['S256'],
    });
    if (url.pathname === '/jwks') return json(200, { keys: [{ ...publicKey.export({ format: 'jwk' }), kid: 'test-key', use: 'sig', alg: 'RS256' }] });
    if (url.pathname === '/authorize') {
      if (url.searchParams.get('code_challenge_method') !== 'S256') return json(400, { error: 'invalid_request' });
      const code = randomBytes(20).toString('hex');
      codes.set(code, Object.fromEntries(url.searchParams));
      const redirect = new URL(url.searchParams.get('redirect_uri'));
      redirect.searchParams.set('code', code); redirect.searchParams.set('state', url.searchParams.get('state'));
      res.writeHead(302, { Location: redirect.href }); return res.end();
    }
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const form = new URLSearchParams(Buffer.concat(chunks).toString());
    const basic = Buffer.from(req.headers.authorization?.slice(6) || '', 'base64').toString().split(':').map(decodeURIComponent).join(':');
    if (!['web-test:web-secret', 'api-test:api-secret'].includes(basic)) { state.providerFailure = 'client_auth'; return json(401, { error: 'invalid_client' }); }
    if (url.pathname === '/token') {
      if (basic !== 'web-test:web-secret') return json(401, { error: 'invalid_client' });
      if (form.get('grant_type') === 'refresh_token') {
        if (!refreshTokens.delete(form.get('refresh_token'))) return json(400, { error: 'invalid_grant' });
        state.refreshes++;
        await delay(20);
        return json(200, issue());
      }
      const saved = codes.get(form.get('code'));
      if (!saved) { state.providerFailure = 'missing_code'; return json(400, { error: 'invalid_grant' }); }
      codes.delete(form.get('code')); state.exchanges++;
      const challenge = createHash('sha256').update(form.get('code_verifier') || '').digest('base64url');
      if (saved.code_challenge !== challenge || saved.redirect_uri !== form.get('redirect_uri')) {
        state.providerFailure = saved.code_challenge !== challenge ? 'pkce' : 'redirect_uri';
        return json(400, { error: 'invalid_grant' });
      }
      return json(200, issue(saved.nonce));
    }
    if (url.pathname === '/introspect') {
      if (basic !== 'api-test:api-secret') return json(401, { error: 'invalid_client' });
      return json(200, tokens.get(form.get('token')) || { active: false });
    }
    if (url.pathname === '/revoke') {
      tokens.delete(form.get('token')); refreshTokens.delete(form.get('token'));
      return json(200, {});
    }
    json(404, {});
  });
  issuer = await listen(idp);
  const upstream = createServer((req, res) => {
    state.upstream.push({ headers: req.headers, url: req.url });
    if (req.url.startsWith('/api/v1/feeds/')) {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' }); res.write(': connected\n\n');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ items: [] }));
  });
  const upstreamUrl = await listen(upstream);
  const config = { appUrl: 'http://127.0.0.1', issuer, allowHttp: true, secureCookies: false,
    sessionTtl: 3600, databasePath: ':memory:', sessionKey: randomBytes(32).toString('hex'),
    upstream: upstreamUrl, serviceToken: 'private-service-test', oidc: {
      client_id: 'web-test', client_secret: 'web-secret', introspection_client_id: 'api-test',
      introspection_client_secret: 'api-secret', project_id: 'project-test', organization_id: 'org-test',
    } };
  const logger = message => state.logs.push(message);
  const provider = new ZitadelProvider(config, logger);
  const complete = provider.complete.bind(provider);
  provider.complete = async (...args) => {
    try { return await complete(...args); }
    catch (error) { state.loginError = `${error.code || error.name}: ${error.message}`; throw error; }
  };
  const { server, store } = createAuthServer(config, { provider, logger });
  config.appUrl = await listen(server);
  t.after(async () => { await stop(server); store.close(); await stop(upstream); await stop(idp); });
  const jar = new Map();
  const cookieHeader = () => [...jar].map(([key, value]) => `${key}=${value}`).join('; ');
  async function request(path, init = {}) {
    const response = await fetch(new URL(path, config.appUrl), { ...init, redirect: 'manual', headers: { Cookie: cookieHeader(), ...init.headers } });
    for (const header of response.headers.getSetCookie()) {
      const [pair] = header.split(';'), index = pair.indexOf('=');
      const key = pair.slice(0, index), value = pair.slice(index + 1);
      if (header.includes('Max-Age=0')) jar.delete(key); else jar.set(key, value);
    }
    return response;
  }
  async function authorization() {
    const start = await request('/auth/login');
    assert.equal(start.status, 302);
    const external = await fetch(start.headers.get('location'), { redirect: 'manual' });
    return external.headers.get('location');
  }
  async function login() {
    const callback = await authorization();
    const response = await request(callback);
    assert.equal(response.headers.get('location'), `${config.appUrl}/#/dashboard`, `${state.loginError}; ${state.providerFailure}`);
    return response;
  }
  return { state, config, store, tokens, jar, request, login, authorization, cookieHeader };
}

test('signed code+PKCE login sets HttpOnly cookies, hides tokens, and proxies authenticated metadata', async t => {
  const f = await fixture(t);
  const login = await f.login();
  for (const value of login.headers.getSetCookie()) {
    assert.match(value, /HttpOnly/); assert.match(value, /SameSite=Lax/); assert.doesNotMatch(value, /Domain=/);
  }
  const response = await f.request('/auth/session');
  const session = await response.json();
  assert.equal(response.status, 200); assert.equal(session.user.role, 'FIELD_TEAM');
  assert.deepEqual(Object.keys(session).sort(), ['csrf_token', 'expires_at', 'user']);
  assert.ok(!JSON.stringify(session).includes(f.jar.get('bnpb_access')));
  const api = await f.request('/api/v1/hazards?view=full', { headers: { 'X-Correlation-ID': 'test-correlation', 'X-Service-Token': 'attacker' } });
  assert.equal(api.status, 200);
  const forwarded = f.state.upstream.at(-1).headers;
  assert.equal(forwarded.authorization, `Bearer ${f.jar.get('bnpb_access')}`);
  assert.equal(forwarded['x-service-token'], 'private-service-test');
  assert.equal(forwarded['x-correlation-id'], 'test-correlation'); assert.equal(forwarded.cookie, undefined);
  assert.ok(!f.state.logs.join('').includes(f.jar.get('bnpb_access')));
});

test('anonymous and Media full requests are rejected before upstream', async t => {
  const f = await fixture(t, { role: 'MEDIA' });
  assert.equal((await f.request('/api/v1/hazards')).status, 401);
  await f.login();
  assert.equal((await f.request('/api/v1/hazards?view=full')).status, 403);
  assert.equal((await f.request('/api/v1/feeds/events?view=full')).status, 403);
  assert.equal((await f.request('/api/v1/webhooks', { method: 'POST' })).status, 403);
  assert.equal(f.state.upstream.length, 0);
  assert.equal((await f.request('/api/v1/hazards?view=summary&view=full')).status, 400);
  assert.equal((await f.request('/api/v1/hazards?view=summary')).status, 200);
});

test('callback is bound to browser and single use', async t => {
  const f = await fixture(t);
  const callback = await f.authorization();
  const foreign = await fetch(callback, { redirect: 'manual' });
  assert.match(foreign.headers.get('location'), /error=login_failed/);
  assert.equal(f.state.exchanges, 0);
  assert.match((await f.request(callback)).headers.get('location'), /dashboard$/);
  assert.match((await f.request(callback)).headers.get('location'), /error=login_failed/);
  assert.equal(f.state.exchanges, 1);
});

for (const [name, option] of Object.entries({ signature: { badSignature: true }, nonce: { badNonce: true }, audience: { audience: 'foreign-project' }, organization: { organization: 'foreign-org' }, role: { role: 'UNKNOWN' } })) {
  test(`login rejects incorrect ${name}`, async t => {
    const f = await fixture(t, option);
    const response = await f.request(await f.authorization());
    assert.match(response.headers.get('location'), /error=login_failed/);
    assert.equal(f.jar.has('bnpb_session'), false);
    assert.equal((await f.request('/auth/session')).status, 401);
  });
}

test('refresh requires Origin and CSRF; simultaneous requests share rotation and old bearer is denied', async t => {
  const f = await fixture(t); await f.login();
  const session = await (await f.request('/auth/session')).json();
  const oldAccess = f.jar.get('bnpb_access');
  assert.equal((await f.request('/auth/refresh', { method: 'POST' })).status, 403);
  assert.equal((await f.request('/auth/refresh', { method: 'POST', headers: { Origin: 'http://evil.localhost', 'X-CSRF-Token': session.csrf_token } })).status, 403);
  const headers = { Origin: f.config.appUrl, 'X-CSRF-Token': session.csrf_token };
  const responses = await Promise.all([1, 2].map(() => f.request('/auth/refresh', { method: 'POST', headers })));
  assert.ok(responses.every(r => r.status === 200)); assert.equal(f.state.refreshes, 1);
  assert.notEqual(f.jar.get('bnpb_access'), oldAccess);
  assert.equal((await f.request('/api/v1/hazards', { headers: { Cookie: '', Authorization: `Bearer ${oldAccess}` } })).status, 401);
  assert.equal((await f.request('/api/v1/hazards')).status, 200);
});

test('naturally expired access cookie recovers with server refresh token without login', async t => {
  const f = await fixture(t, { ttl: 2 }); await f.login();
  await delay(2100);
  f.jar.delete('bnpb_access');
  const expired = await f.request('/auth/session'); const recovery = await expired.json();
  assert.equal(expired.status, 401); assert.equal(recovery.refresh_required, true);
  const refreshed = await f.request('/auth/refresh', { method: 'POST', headers: { Origin: f.config.appUrl, 'X-CSRF-Token': recovery.csrf_token } });
  assert.equal(refreshed.status, 200); assert.equal(f.state.exchanges, 1); assert.equal(f.state.refreshes, 1);
});

test('logout deletes persisted session, revokes bearer and closes open SSE', async t => {
  const f = await fixture(t); await f.login();
  const session = await (await f.request('/auth/session')).json();
  const oldCookie = f.cookieHeader(), oldAccess = f.jar.get('bnpb_access');
  const controller = new AbortController();
  const stream = await f.request('/api/v1/feeds/events', { signal: controller.signal });
  const reader = stream.body.getReader(); await reader.read();
  const ended = reader.read().then(() => true, () => true);
  const response = await f.request('/auth/logout', { method: 'POST', headers: { Origin: f.config.appUrl, 'X-CSRF-Token': session.csrf_token } });
  assert.equal(response.status, 200);
  assert.match((await response.json()).logout_url, /\/logout\?/);
  assert.equal(f.jar.size, 0);
  assert.equal(await Promise.race([ended, delay(1000).then(() => false)]), true); controller.abort();
  assert.equal((await f.request('/auth/session', { headers: { Cookie: oldCookie } })).status, 401);
  assert.equal((await f.request('/api/v1/hazards', { headers: { Authorization: `Bearer ${oldAccess}`, Cookie: '' } })).status, 401);
});

test('encrypted SQLite sessions survive reopening and retain token denylist', () => {
  const folder = mkdtempSync(join(tmpdir(), 'bnpb-auth-test-'));
  const path = join(folder, 'sessions.sqlite'); const key = randomBytes(32).toString('hex');
  const session = { accessToken: 'access-secret-test', refreshToken: 'refresh-secret-test', expiresAt: Date.now() + 60000, sessionExpires: Date.now() + 60000 };
  let store = new SessionStore(path, key);
  try {
    store.putSession('opaque-id-test', session); store.close();
    assert.equal(readFileSync(path).includes(Buffer.from(session.refreshToken)), false);
    assert.equal(readFileSync(path).includes(Buffer.from('opaque-id-test')), false);
    store = new SessionStore(path, key); assert.deepEqual(store.getSession('opaque-id-test'), session);
    store.rotate('opaque-id-test', session, null); assert.equal(store.isRevoked(session.accessToken), true);
    assert.equal(store.getSession('opaque-id-test'), null);
  } finally {
    store.close();
    const target = resolve(folder);
    if (!target.startsWith(resolve(tmpdir()) + '\\bnpb-auth-test-') && !target.startsWith(resolve(tmpdir()) + '/bnpb-auth-test-')) throw new Error('Invalid test cleanup path');
    rmSync(target, { recursive: true });
  }
});

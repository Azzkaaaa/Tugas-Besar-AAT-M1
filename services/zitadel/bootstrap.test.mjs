// Provisioning regression tests with a controlled HTTP provider, not a substitute
// for the real Zitadel compose/browser smoke tests documented in README.md.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const runFile = promisify(execFile);
const script = fileURLToPath(new URL('./bootstrap.mjs', import.meta.url));

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'bnpb-bootstrap-'));
  const configFile = join(directory, 'oidc.json');
  const patFile = join(directory, 'admin.pat');
  await writeFile(patFile, 'test-admin-pat');
  const state = { projects: [], apps: [], roles: [], users: [], grants: [], members: [], policies: null, oidc: {}, changes: 0 };
  const server = createServer(async (request, response) => {
    response.setHeader('Content-Type', 'application/json');
    const done = value => response.end(JSON.stringify(value));
    const path = request.url;
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
    if (request.headers.authorization !== 'Bearer test-admin-pat') {
      response.statusCode = 401;
      return done({ code: 16 });
    }
    if (path === '/management/v1/orgs/me') return done({ org: { id: 'org1' } });
    if (path === '/management/v1/projects/_search') return done({ result: state.projects });
    if (path === '/management/v1/projects') {
      state.projects.push({ ...body, id: 'project1' }); state.changes++;
      return done({ id: 'project1' });
    }
    if (path === '/admin/v1/settings/oidc') {
      if (request.method === 'PUT') { state.oidc = body; state.changes++; return done({}); }
      return done({ settings: state.oidc });
    }
    if (path === '/management/v1/policies/login') {
      if (request.method === 'POST') { state.policies = body; state.changes++; return done({}); }
      return done({ isDefault: !state.policies, policy: state.policies });
    }
    if (path === '/management/v1/projects/project1/roles/_search') return done({ result: state.roles });
    if (path === '/management/v1/projects/project1/roles') {
      state.roles.push({ key: body.roleKey }); state.changes++; return done({});
    }
    if (path === '/management/v1/projects/project1/apps/_search') return done({ result: state.apps });
    if (path === '/management/v1/projects/project1/apps/oidc' || path === '/management/v1/projects/project1/apps/api') {
      const kind = path.endsWith('/oidc') ? 'oidc' : 'api';
      const id = `app${state.apps.length}`;
      const clientId = `${id}-client`;
      state.apps.push({ id, name: body.name, [`${kind}Config`]: { ...body, clientId } }); state.changes++;
      return done({ appId: id, clientId, clientSecret: `${id}-test-secret` });
    }
    if (path === '/management/v1/users/_search') return done({ result: state.users });
    if (path === '/management/v1/users/human/_import') {
      const id = `user${state.users.length}`;
      state.users.push({ ...body, id }); state.changes++;
      return done({ userId: id });
    }
    if (path === '/management/v1/users/grants/_search') return done({ result: state.grants });
    if (/^\/management\/v1\/users\/user\d+\/grants$/.test(path)) {
      state.grants.push({ ...body, id: `grant${state.grants.length}`, userId: path.split('/')[4] }); state.changes++;
      return done({});
    }
    if (path === '/admin/v1/members/_search') return done({ result: state.members });
    if (path === '/admin/v1/members') { state.members.push(body); state.changes++; return done({}); }
    response.statusCode = 404;
    done({ code: 5 });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true });
  });
  const env = {
    ...process.env,
    OIDC_ISSUER: `http://127.0.0.1:${server.address().port}`,
    APP_PUBLIC_URL: 'http://localhost:8080',
    OIDC_CONFIG_FILE: configFile,
    ZITADEL_ADMIN_PAT_FILE: patFile,
    AUTH_MEDIA_PASSWORD: 'TestMedia1!',
    AUTH_FIELD_TEAM_PASSWORD: 'TestField1!',
    AUTH_BNPB_PASSWORD: 'TestBNPB1!',
    AUTH_ADMIN_PASSWORD: 'TestAdmin1!',
    ZITADEL_ACCESS_TOKEN_TTL_SECONDS: '60',
    AUTH_SESSION_TTL_SECONDS: '86400',
  };
  return { state, configFile, run: overrides => runFile(process.execPath, [script], { env: { ...env, ...overrides }, timeout: 15000 }) };
}

test('seed is idempotent, preserves credentials, and configures 60-second tokens', async t => {
  const f = await fixture(t);
  const firstRun = await f.run();
  const saved = await readFile(f.configFile, 'utf8');
  const changes = f.state.changes;
  assert.equal(f.state.apps.length, 2);
  assert.equal(f.state.users.length, 4);
  assert.deepEqual(f.state.grants.map(grant => grant.roleKeys), [['MEDIA'], ['FIELD_TEAM'], ['BNPB_INTERNAL']]);
  assert.deepEqual(f.state.oidc, { accessTokenLifetime: '60s', idTokenLifetime: '60s', refreshTokenIdleExpiration: '86400s', refreshTokenExpiration: '86400s' });
  assert.equal(f.state.apps[0].oidcConfig.authMethodType, 'OIDC_AUTH_METHOD_TYPE_BASIC');
  assert.equal(f.state.apps[0].oidcConfig.accessTokenType, 'OIDC_TOKEN_TYPE_BEARER');
  assert.deepEqual(f.state.apps[0].oidcConfig.redirectUris, ['http://localhost:8080/auth/callback']);
  assert.equal(f.state.members.length, 1);
  assert.doesNotMatch(firstRun.stdout + firstRun.stderr, /TestMedia1|test-secret|test-admin-pat/);
  await f.run({ AUTH_MEDIA_PASSWORD: 'DifferentPassword1!' });
  assert.equal(f.state.changes, changes);
  assert.equal(await readFile(f.configFile, 'utf8'), saved);
  assert.equal(f.state.users[0].password, 'TestMedia1!');
  await f.run({ ZITADEL_ACCESS_TOKEN_TTL_SECONDS: '90' });
  assert.equal(f.state.oidc.accessTokenLifetime, '90s');
});

test('existing application without matching saved secret fails without rotation', async t => {
  const f = await fixture(t);
  await f.run();
  const saved = JSON.parse(await readFile(f.configFile, 'utf8'));
  delete saved.client_secret;
  await writeFile(f.configFile, JSON.stringify(saved));
  const changes = f.state.changes;
  await assert.rejects(f.run(), error => /no matching persisted secret/.test(error.stderr));
  assert.equal(f.state.changes, changes);
});

test('changed seed grants and callback origins are not silently overwritten', async t => {
  const f = await fixture(t);
  await f.run();
  f.state.grants[0].roleKeys = ['BNPB_INTERNAL'];
  await assert.rejects(f.run(), error => /role grant differs/.test(error.stderr));
  assert.deepEqual(f.state.grants[0].roleKeys, ['BNPB_INTERNAL']);
  await assert.rejects(f.run({ APP_PUBLIC_URL: 'http://other.localhost:8080' }), error => /issuer\/callback differs/.test(error.stderr));
});

test('invalid TTL fails before applying token lifetime settings', async t => {
  const f = await fixture(t);
  await assert.rejects(f.run({ ZITADEL_ACCESS_TOKEN_TTL_SECONDS: '-1' }), error => /positive integer/.test(error.stderr));
  assert.deepEqual(f.state.oidc, {});
});

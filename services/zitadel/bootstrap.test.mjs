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
  const actionsFile = join(directory, 'actions', 'actions.json');
  const patFile = join(directory, 'admin.pat');
  const serviceDir = join(directory, 'service-accounts');
  await writeFile(patFile, 'test-admin-pat');
  const state = { projects: [], apps: [], roles: [], users: [], grants: [], members: [], policies: null, oidc: {},
    features: {}, targets: [], executions: {}, hosts: new Set(), changes: 0 };
  const server = createServer(async (request, response) => {
    response.setHeader('Content-Type', 'application/json');
    const done = value => response.end(JSON.stringify(value));
    const path = request.url;
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
    state.hosts.add(request.headers.host);
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
    if (path === '/management/v1/users/machine') {
      const id = `user${state.users.length}`;
      state.users.push({ ...body, id, machine: { hasSecret: false } }); state.changes++;
      return done({ userId: id });
    }
    if (/^\/management\/v1\/users\/user\d+\/secret$/.test(path) && request.method === 'PUT') {
      const user = state.users.find(item => item.id === path.split('/')[4]);
      user.machine.hasSecret = true; state.changes++;
      return done({ clientId: user.userName, clientSecret: `${user.userName}-test-secret` });
    }
    if (path === '/management/v1/users/grants/_search') return done({ result: state.grants });
    if (/^\/management\/v1\/users\/user\d+\/grants$/.test(path)) {
      state.grants.push({ ...body, id: `grant${state.grants.length}`, userId: path.split('/')[4] }); state.changes++;
      return done({});
    }
    if (path === '/v2/features/instance') {
      if (request.method === 'PUT') { state.features = body; state.changes++; return done({}); }
      return done({ oidcTokenExchange: { enabled: state.features.oidcTokenExchange === true } });
    }
    if (path === '/v2/actions/targets/search') return done({ targets: state.targets });
    if (path === '/v2/actions/targets') {
      const id = `target${state.targets.length}`;
      state.targets.push({ ...body, id }); state.changes++;
      return done({ id, signingKey: `${id}-test-signing-key` });
    }
    if (path === '/v2/actions/executions' && request.method === 'PUT') {
      const name = body.condition?.function?.name;
      if (JSON.stringify(state.executions[name]) !== JSON.stringify(body.targets)) { state.executions[name] = body.targets; state.changes++; }
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
    OIDC_ISSUER: 'http://auth.localhost:8080',
    ZITADEL_API_URL: `http://127.0.0.1:${server.address().port}`,
    ZITADEL_API_HOST: 'auth.localhost:8080',
    APP_PUBLIC_URL: 'http://localhost:8080',
    OIDC_CONFIG_FILE: configFile,
    ACTIONS_CONFIG_FILE: actionsFile,
    ZITADEL_ADMIN_PAT_FILE: patFile,
    SERVICE_ACCOUNTS_DIR: serviceDir,
    AUTH_MEDIA_PASSWORD: 'TestMedia1!',
    AUTH_FIELD_TEAM_PASSWORD: 'TestField1!',
    AUTH_BNPB_PASSWORD: 'TestBNPB1!',
    AUTH_ADMIN_PASSWORD: 'TestAdmin1!',
    ZITADEL_ACCESS_TOKEN_TTL_SECONDS: '60',
    AUTH_SESSION_TTL_SECONDS: '86400',
  };
  return { state, configFile, actionsFile, serviceDir, run: overrides => runFile(process.execPath, [script], { env: { ...env, ...overrides }, timeout: 15000 }) };
}

test('seed is idempotent, preserves credentials, and configures 60-second tokens', async t => {
  const f = await fixture(t);
  const firstRun = await f.run();
  const saved = await readFile(f.configFile, 'utf8');
  const savedActions = await readFile(f.actionsFile, 'utf8');
  const changes = f.state.changes;
  assert.equal(f.state.apps.length, 4);
  assert.deepEqual([...f.state.hosts], ['auth.localhost:8080']);
  assert.deepEqual(f.state.features, { oidcTokenExchange: true });
  const gateway = f.state.apps.find(app => app.name === 'BNPB gateway token exchange').oidcConfig;
  assert.deepEqual(gateway.grantTypes, ['OIDC_GRANT_TYPE_AUTHORIZATION_CODE', 'OIDC_GRANT_TYPE_TOKEN_EXCHANGE']);
  assert.equal(gateway.accessTokenType, 'OIDC_TOKEN_TYPE_JWT');
  const feed = f.state.apps.find(app => app.name === 'BNPB event feed token exchange').oidcConfig;
  assert.deepEqual(feed.grantTypes, ['OIDC_GRANT_TYPE_AUTHORIZATION_CODE', 'OIDC_GRANT_TYPE_TOKEN_EXCHANGE']);
  assert.equal(feed.accessTokenType, 'OIDC_TOKEN_TYPE_JWT');
  assert.equal(f.state.targets.length, 1);
  assert.equal(f.state.targets[0].endpoint, 'http://zitadel-claims:3002/claims');
  assert.deepEqual(f.state.targets[0].restCall, { interruptOnError: true });
  assert.deepEqual(f.state.executions, { preuserinfo: ['target0'], preaccesstoken: ['target0'] });
  const config = JSON.parse(await readFile(f.configFile, 'utf8'));
  assert.equal(config.gateway_client_id, 'app2-client');
  assert.equal(config.gateway_client_secret, 'app2-test-secret');
  assert.equal(config.feed_client_id, 'app3-client');
  assert.equal(config.feed_client_secret, 'app3-test-secret');
  assert.equal(config.actions_target_id, undefined);
  assert.equal(config.actions_signing_key, undefined);
  // The claims service gets only the signing key and the IDs it filters on.
  assert.deepEqual(JSON.parse(savedActions), { actions_target_id: 'target0', actions_signing_key: 'target0-test-signing-key',
    client_ids: [config.client_id, config.introspection_client_id, config.gateway_client_id, config.feed_client_id],
    project_id: config.project_id, organization_id: config.organization_id });
  assert.equal(f.state.users.length, 7);
  assert.deepEqual(f.state.grants.map(grant => grant.roleKeys),
    [['MEDIA'], ['FIELD_TEAM'], ['BNPB_INTERNAL'], ['MEDIA'], ['FIELD_TEAM'], ['BNPB_INTERNAL']]);
  const serviceUsers = f.state.users.filter(user => user.machine);
  assert.deepEqual(serviceUsers.map(user => [user.userName, user.accessTokenType]), [
    ['svc-media', 'ACCESS_TOKEN_TYPE_BEARER'], ['svc-lapangan', 'ACCESS_TOKEN_TYPE_BEARER'], ['svc-bnpb', 'ACCESS_TOKEN_TYPE_BEARER']]);
  // Raw values, one per file, so scripts can use $(cat file) directly.
  const serviceFiles = {};
  for (const name of ['project-id', 'svc-media.secret', 'svc-lapangan.secret', 'svc-bnpb.secret']) {
    serviceFiles[name] = await readFile(join(f.serviceDir, name), 'utf8');
  }
  assert.deepEqual(serviceFiles, { 'project-id': 'project1', 'svc-media.secret': 'svc-media-test-secret',
    'svc-lapangan.secret': 'svc-lapangan-test-secret', 'svc-bnpb.secret': 'svc-bnpb-test-secret' });
  assert.deepEqual(f.state.oidc, { accessTokenLifetime: '60s', idTokenLifetime: '60s', refreshTokenIdleExpiration: '86400s', refreshTokenExpiration: '86400s' });
  assert.equal(f.state.apps[0].oidcConfig.authMethodType, 'OIDC_AUTH_METHOD_TYPE_BASIC');
  assert.equal(f.state.apps[0].oidcConfig.accessTokenType, 'OIDC_TOKEN_TYPE_BEARER');
  assert.deepEqual(f.state.apps[0].oidcConfig.redirectUris, ['http://localhost:8080/auth/callback']);
  assert.equal(f.state.members.length, 1);
  assert.doesNotMatch(firstRun.stdout + firstRun.stderr, /TestMedia1|test-secret|test-signing-key|test-admin-pat/);
  await f.run({ AUTH_MEDIA_PASSWORD: 'DifferentPassword1!' });
  assert.equal(f.state.changes, changes);
  assert.equal(await readFile(f.configFile, 'utf8'), saved);
  assert.equal(await readFile(f.actionsFile, 'utf8'), savedActions);
  for (const [name, value] of Object.entries(serviceFiles)) assert.equal(await readFile(join(f.serviceDir, name), 'utf8'), value);
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

test('existing service user without saved secret fails without rotation', async t => {
  const f = await fixture(t);
  await f.run();
  await rm(join(f.serviceDir, 'svc-bnpb.secret'));
  const changes = f.state.changes;
  await assert.rejects(f.run(), error => /svc-bnpb has no persisted secret/.test(error.stderr));
  assert.equal(f.state.changes, changes);
});

test('service user created without a secret gets one on the next run', async t => {
  const f = await fixture(t);
  await f.run();
  const user = f.state.users.find(item => item.userName === 'svc-media');
  user.machine.hasSecret = false;
  await rm(join(f.serviceDir, 'svc-media.secret'));
  const changes = f.state.changes;
  await f.run();
  assert.equal(f.state.changes, changes + 1);
  assert.equal(await readFile(join(f.serviceDir, 'svc-media.secret'), 'utf8'), 'svc-media-test-secret');
});

test('fresh Zitadel replaces a stale service user secret file', async t => {
  const f = await fixture(t);
  await f.run();
  // A new database: the user exists again but has no secret, while the old file is still on disk.
  const user = f.state.users.find(item => item.userName === 'svc-bnpb');
  user.machine.hasSecret = false;
  await writeFile(join(f.serviceDir, 'svc-bnpb.secret'), 'secret-from-another-instance');
  const changes = f.state.changes;
  await f.run();
  assert.equal(f.state.changes, changes + 1);
  assert.equal(user.machine.hasSecret, true);
  assert.equal(await readFile(join(f.serviceDir, 'svc-bnpb.secret'), 'utf8'), 'svc-bnpb-test-secret');
});

test('existing claims target without saved signing key fails without rotation', async t => {
  const f = await fixture(t);
  await f.run();
  const saved = JSON.parse(await readFile(f.actionsFile, 'utf8'));
  delete saved.actions_signing_key;
  await writeFile(f.actionsFile, JSON.stringify(saved));
  const changes = f.state.changes;
  await assert.rejects(f.run(), error => /no matching persisted signing key/.test(error.stderr));
  assert.equal(f.state.changes, changes);
  assert.equal(f.state.targets.length, 1);
});

test('signing key persisted in oidc.json by earlier versions moves to actions.json', async t => {
  const f = await fixture(t);
  await f.run();
  const actions = JSON.parse(await readFile(f.actionsFile, 'utf8'));
  const config = JSON.parse(await readFile(f.configFile, 'utf8'));
  await writeFile(f.configFile, JSON.stringify({ ...config, actions_target_id: actions.actions_target_id,
    actions_signing_key: actions.actions_signing_key }));
  await rm(f.actionsFile);
  const changes = f.state.changes;
  await f.run();
  assert.equal(f.state.changes, changes);
  assert.equal(f.state.targets.length, 1);
  assert.deepEqual(JSON.parse(await readFile(f.actionsFile, 'utf8')), actions);
  assert.deepEqual(JSON.parse(await readFile(f.configFile, 'utf8')), config);
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

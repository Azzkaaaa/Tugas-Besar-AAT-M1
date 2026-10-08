// Local provisioning only. Uses the pinned Zitadel Management API v1 and the v2 feature/action APIs.
// Never log request bodies, API secrets, tokens, or initial passwords.
import { readFile, writeFile, rename, mkdir, access } from 'node:fs/promises';
import { request } from 'node:http';
import { dirname, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const issuer = process.env.OIDC_ISSUER || 'http://auth.localhost:8080';
const appUrl = process.env.APP_PUBLIC_URL || 'http://localhost:8080';
// Provisioning talks to Zitadel directly so it does not depend on the gateway,
// which itself needs the credentials written here. The Host header selects the instance.
const apiUrl = process.env.ZITADEL_API_URL || issuer;
const apiHost = process.env.ZITADEL_API_HOST || '';
const claimsEndpoint = process.env.ACTIONS_CLAIMS_ENDPOINT || 'http://zitadel-claims:3002/claims';
const claimsTargetName = 'BNPB scope claims';
const output = process.env.OIDC_CONFIG_FILE || '/bootstrap/oidc.json';
// Separate file and volume for the claims service, so it never sees client secrets.
const actionsOutput = process.env.ACTIONS_CONFIG_FILE || '/actions/actions.json';
const patPath = process.env.ZITADEL_ADMIN_PAT_FILE || '/admin/admin.pat';
// Host bind mount (.cache/auth): one raw value per file so scripts can `cat` them.
// Kept out of oidc.json, which the bridge, gateway and feed service mount.
const serviceAccountsDir = process.env.SERVICE_ACCOUNTS_DIR || '/service-accounts';
let token;
let saved = {};
let actions = {};

function seconds(name, fallback) {
  const value = process.env[name] || String(fallback);
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) < 1) {
    throw new Error(`${name} must be a positive integer number of seconds.`);
  }
  return Number(value);
}

async function write(path, data) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(`${path}.tmp`, JSON.stringify(data, null, 2) + '\n', { mode: 0o644 });
  await rename(`${path}.tmp`, path);
}

// Raw value without JSON or trailing newline, for `$(cat file)` in scripts.
async function writeRaw(path, value) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(`${path}.tmp`, value, { mode: 0o644 });
  await rename(`${path}.tmp`, path);
}

async function exists(path) {
  try { await access(path); return true; }
  catch (error) { if (error.code === 'ENOENT') return false; throw error; }
}

// Read-only mounted into the bridge; no IAM_OWNER PAT is written here.
const persist = () => write(output, saved);
// Read-only mounted into the claims service: signing key and the IDs it filters on, no client secrets.
const persistActions = () => write(actionsOutput, actions);

async function load(path, what) {
  try { return JSON.parse(await readFile(path, 'utf8')); }
  catch (error) { if (error.code !== 'ENOENT') throw new Error(`Cannot read persisted ${what}.`); return {}; }
}

// node:http instead of fetch: fetch silently drops a custom Host header.
function send(path, body, method) {
  const payload = body === undefined ? undefined : JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = request(new URL(path, apiUrl), {
      method,
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json',
        ...(apiHost ? { Host: apiHost } : {}) },
      signal: AbortSignal.timeout(20000),
    }, response => {
      const chunks = [];
      response.on('data', chunk => chunks.push(chunk));
      response.on('end', () => {
        let json = {};
        try { json = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}'); } catch { /* status is enough */ }
        resolve({ status: response.statusCode, json });
      });
      response.on('error', reject);
    });
    req.on('error', reject);
    req.end(payload);
  });
}

async function api(path, body, method = body === undefined ? 'GET' : 'POST') {
  const { status, json } = await send(path, body, method);
  if (status < 200 || status >= 300) {
    // Provider messages might echo submitted values. Only log numeric status/code.
    throw new Error(`Provisioning ${method} ${path} failed: HTTP ${status}, code ${Number(json.code) || 0}`);
  }
  return json;
}

async function all(path) {
  const rows = [];
  for (let offset = 0; ; offset += 100) {
    const page = await api(path, { query: { offset: String(offset), limit: 100 } });
    const result = page.result || [];
    rows.push(...result);
    if (result.length < 100) return rows;
  }
}

async function ensureUser(username, firstName, lastName, passwordKey) {
  const existing = (await all('/management/v1/users/_search')).find(user => user.userName === username);
  if (existing) return existing.id;
  const password = process.env[passwordKey];
  if (!password || password.startsWith('REPLACE_')) throw new Error(`Missing ${passwordKey}; run setup_auth_env.py.`);
  const created = await api('/management/v1/users/human/_import', {
    userName: username,
    profile: { firstName, lastName, displayName: `${firstName} ${lastName}`, preferredLanguage: 'en' },
    email: { email: username.includes('@') ? username : `${username}@bnpb.local`, isEmailVerified: true },
    password,
    passwordChangeRequired: false,
  });
  return created.userId;
}

// Machine user for non-browser API clients (client_credentials). The client ID is the username;
// Zitadel generates the secret and cannot return it again, so it is only created when Zitadel has
// none. That decision follows Zitadel, not the file: after a fresh database (or a switch between
// stacks sharing .cache/auth) a leftover file would otherwise be kept for a user without a secret.
async function ensureServiceUser(username, name) {
  let user = (await all('/management/v1/users/_search')).find(item => item.userName === username);
  if (!user) {
    const created = await api('/management/v1/users/machine', {
      userName: username, name, description: 'Local non-browser API client',
      accessTokenType: 'ACCESS_TOKEN_TYPE_BEARER',
    });
    user = { id: created.userId, machine: { hasSecret: false } };
  }
  const secretFile = join(serviceAccountsDir, `${username}.secret`);
  if (user.machine?.hasSecret) {
    if (await exists(secretFile)) return user.id;
    throw new Error(`Existing ${username} has no persisted secret. Restore .cache/auth/${username}.secret; secrets are not automatically rotated.`);
  }
  // Overwrites a stale file left from another Zitadel instance.
  const secret = await api(`/management/v1/users/${user.id}/secret`, {}, 'PUT');
  if (secret.clientId !== username || !secret.clientSecret) {
    throw new Error(`Zitadel did not return client credentials matching ${username}.`);
  }
  await writeRaw(secretFile, secret.clientSecret);
  return user.id;
}

async function ensureGrant(userId, username, role, projectId) {
  const grants = await all('/management/v1/users/grants/_search');
  const existing = grants.find(grant => grant.userId === userId && grant.projectId === projectId);
  if (!existing) await api(`/management/v1/users/${userId}/grants`, { projectId, roleKeys: [role] });
  else if (existing.roleKeys?.length !== 1 || existing.roleKeys[0] !== role) {
    throw new Error(`Existing ${username} role grant differs from the local seed. Review the user's project grants in Zitadel; grants are not silently overwritten.`);
  }
}

async function ensureApp(projectId, name, kind, prefix, config) {
  const found = (await all(`/management/v1/projects/${projectId}/apps/_search`)).find(app => app.name === name);
  const idKey = prefix ? `${prefix}_app_id` : 'app_id';
  const clientKey = prefix ? `${prefix}_client_id` : 'client_id';
  const secretKey = prefix ? `${prefix}_client_secret` : 'client_secret';
  if (found) {
    const clientId = kind === 'oidc' ? found.oidcConfig?.clientId : found.apiConfig?.clientId;
    if (saved[idKey] !== found.id || saved[clientKey] !== clientId || !saved[secretKey]) {
      throw new Error(`Existing ${name} has no matching persisted secret. Restore auth_client_config volume; secrets are not automatically rotated.`);
    }
    return;
  }
  const created = await api(`/management/v1/projects/${projectId}/apps/${kind}`, { name, ...config });
  if (!created.clientId || !created.clientSecret) throw new Error(`Zitadel did not return client credentials for ${name}.`);
  saved[idKey] = created.appId;
  saved[clientKey] = created.clientId;
  saved[secretKey] = created.clientSecret;
  await persist();
}

function oidcApp(overrides) {
  return {
    redirectUris: [`${appUrl}/auth/callback`],
    responseTypes: ['OIDC_RESPONSE_TYPE_CODE'],
    appType: 'OIDC_APP_TYPE_WEB',
    authMethodType: 'OIDC_AUTH_METHOD_TYPE_BASIC',
    postLogoutRedirectUris: [`${appUrl}/`],
    version: 'OIDC_VERSION_1_0',
    // HTTP is permitted only for this loopback-only development stack.
    devMode: true,
    loginVersion: { loginV2: { baseUri: `${issuer}/ui/v2/login/` } },
    ...overrides,
  };
}

// The gateway exchanges browser access tokens for JWTs (RFC 8693); beta feature in Zitadel v4.
async function ensureTokenExchange() {
  const features = await api('/v2/features/instance');
  if (features.oidcTokenExchange?.enabled !== true) {
    await api('/v2/features/instance', { oidcTokenExchange: true }, 'PUT');
  }
}

// Actions v2: Zitadel calls the claims service to append role/scopes claims to userinfo,
// introspection (preuserinfo) and JWT access tokens (preaccesstoken).
async function ensureClaimsAction() {
  const { targets = [] } = await api('/v2/actions/targets/search', { pagination: { limit: 1000 } });
  const found = targets.find(target => target.name === claimsTargetName);
  if (found) {
    if (actions.actions_target_id !== found.id || !actions.actions_signing_key) {
      throw new Error(`Existing ${claimsTargetName} target has no matching persisted signing key. Restore auth_actions_config volume; keys are not automatically rotated.`);
    }
  } else {
    const created = await api('/v2/actions/targets', {
      name: claimsTargetName,
      restCall: { interruptOnError: true },
      endpoint: claimsEndpoint,
      timeout: '5s',
      payloadType: 'PAYLOAD_TYPE_JSON',
    });
    if (!created.id || !created.signingKey) throw new Error('Zitadel did not return a target signing key.');
    actions.actions_target_id = created.id;
    actions.actions_signing_key = created.signingKey;
  }
  // Rewritten every run so the filter IDs follow the provisioned applications.
  actions = { ...actions, client_ids: [saved.client_id, saved.introspection_client_id, saved.gateway_client_id,
    saved.feed_client_id],
    project_id: saved.project_id, organization_id: saved.organization_id };
  await persistActions();
  for (const name of ['preuserinfo', 'preaccesstoken']) {
    await api('/v2/actions/executions', { condition: { function: { name } }, targets: [actions.actions_target_id] }, 'PUT');
  }
}

// Volumes provisioned before the claims service split kept the target in oidc.json.
// Move it (actions.json first, so a crash in between loses nothing); keys cannot be re-read from Zitadel.
async function migrateActions() {
  if (actions.actions_signing_key || !saved.actions_signing_key) return;
  actions = { ...actions, actions_target_id: saved.actions_target_id, actions_signing_key: saved.actions_signing_key };
  await persistActions();
  delete saved.actions_target_id;
  delete saved.actions_signing_key;
  await persist();
}

async function main() {
  token = (await readFile(patPath, 'utf8')).trim();
  if (!token) throw new Error('The bootstrap PAT file is empty.');
  saved = await load(output, 'OIDC configuration');
  actions = await load(actionsOutput, 'actions configuration');
  if (saved.issuer && (saved.issuer !== issuer || saved.app_public_url !== appUrl)) {
    throw new Error('The persisted issuer/callback differs from this deployment. Update the existing Zitadel application explicitly.');
  }
  let organization;
  for (let attempt = 0; attempt < 30; attempt++) {
    try { organization = (await api('/management/v1/orgs/me')).org; break; }
    catch (error) { if (attempt === 29) throw error; await delay(2000); }
  }
  const projects = await all('/management/v1/projects/_search');
  let project = projects.find(item => item.name === 'BNPB M1');
  if (!project) project = await api('/management/v1/projects', { name: 'BNPB M1', projectRoleAssertion: true, projectRoleCheck: true });
  const projectId = project.id;
  if (!projectId || !organization?.id) throw new Error('Zitadel did not return a project or organization identifier.');
  if (saved.project_id && saved.project_id !== projectId) throw new Error('Persisted client configuration belongs to another project/database.');
  saved = { ...saved, issuer, app_public_url: appUrl, project_id: projectId, organization_id: organization.id };
  await persist();
  await migrateActions();

  // All four durations must be supplied together to the instance OIDC settings API.
  // Reapply on restart so an explicitly changed TTL also takes effect on persisted data.
  const accessLifetime = seconds('ZITADEL_ACCESS_TOKEN_TTL_SECONDS', 60);
  const sessionLifetime = seconds('AUTH_SESSION_TTL_SECONDS', 86400);
  if (accessLifetime > sessionLifetime) throw new Error('Access token lifetime must not exceed the session lifetime.');
  const lifetimes = {
    accessTokenLifetime: `${accessLifetime}s`,
    idTokenLifetime: `${accessLifetime}s`,
    refreshTokenIdleExpiration: `${sessionLifetime}s`,
    refreshTokenExpiration: `${sessionLifetime}s`,
  };
  const current = (await api('/admin/v1/settings/oidc')).settings;
  if (Object.entries(lifetimes).some(([key, value]) => current?.[key] !== value)) {
    await api('/admin/v1/settings/oidc', lifetimes, 'PUT');
  }

  // Local classroom configuration: passwords, no public registration or MFA prompt.
  // Configure stronger policies separately before any non-local deployment.
  const policy = await api('/management/v1/policies/login');
  if (policy.isDefault || policy.policy?.isDefault) {
    await api('/management/v1/policies/login', {
      allowUsernamePassword: true, allowRegister: false, allowExternalIdp: false,
      forceMfa: false, passwordlessType: 'PASSWORDLESS_TYPE_NOT_ALLOWED',
      hidePasswordReset: true, ignoreUnknownUsernames: true,
      passwordCheckLifetime: '86400s', mfaInitSkipLifetime: '86400s',
      secondFactors: [], multiFactors: [],
    });
  }

  const accounts = [
    ['MEDIA', 'Media', 'media-demo', 'Media', 'Demo', 'AUTH_MEDIA_PASSWORD'],
    ['FIELD_TEAM', 'Tim Lapangan', 'lapangan-demo', 'Tim', 'Lapangan', 'AUTH_FIELD_TEAM_PASSWORD'],
    ['BNPB_INTERNAL', 'BNPB Pusat', 'bnpb-demo', 'BNPB', 'Pusat', 'AUTH_BNPB_PASSWORD'],
  ];
  const serviceAccounts = [
    ['MEDIA', 'svc-media', 'Media service'],
    ['FIELD_TEAM', 'svc-lapangan', 'Tim Lapangan service'],
    ['BNPB_INTERNAL', 'svc-bnpb', 'BNPB Pusat service'],
  ];
  const roles = await all(`/management/v1/projects/${projectId}/roles/_search`);
  for (const [key, displayName] of accounts) {
    if (!roles.some(role => role.key === key)) {
      await api(`/management/v1/projects/${projectId}/roles`, { roleKey: key, displayName, group: 'BNPB' });
    }
  }
  await ensureTokenExchange();
  await ensureApp(projectId, 'BNPB browser bridge', 'oidc', '', oidcApp({
    grantTypes: ['OIDC_GRANT_TYPE_AUTHORIZATION_CODE', 'OIDC_GRANT_TYPE_REFRESH_TOKEN'],
    accessTokenType: 'OIDC_TOKEN_TYPE_BEARER',
    idTokenRoleAssertion: true,
  }));
  await ensureApp(projectId, 'BNPB token introspection', 'api', 'introspection', { authMethodType: 'API_AUTH_METHOD_TYPE_BASIC' });
  // Same project as the bridge: Zitadel only exchanges tokens whose audience holds this project.
  // Zitadel requires the authorization_code grant for any app with the `code` response type.
  await ensureApp(projectId, 'BNPB gateway token exchange', 'oidc', 'gateway', oidcApp({
    grantTypes: ['OIDC_GRANT_TYPE_AUTHORIZATION_CODE', 'OIDC_GRANT_TYPE_TOKEN_EXCHANGE'],
    accessTokenType: 'OIDC_TOKEN_TYPE_JWT',
    accessTokenRoleAssertion: true,
  }));
  // event-live-feed re-exchanges the gateway JWT before it expires so open streams survive
  // token rotation; its own client keeps the gateway secret out of that service.
  await ensureApp(projectId, 'BNPB event feed token exchange', 'oidc', 'feed', oidcApp({
    grantTypes: ['OIDC_GRANT_TYPE_AUTHORIZATION_CODE', 'OIDC_GRANT_TYPE_TOKEN_EXCHANGE'],
    accessTokenType: 'OIDC_TOKEN_TYPE_JWT',
    accessTokenRoleAssertion: true,
  }));
  await ensureClaimsAction();
  for (const [role, , username, firstName, lastName, passwordKey] of accounts) {
    await ensureGrant(await ensureUser(username, firstName, lastName, passwordKey), username, role, projectId);
  }
  // Needed for the client_credentials audience scope urn:zitadel:iam:org:project:id:<id>:aud.
  await writeRaw(join(serviceAccountsDir, 'project-id'), projectId);
  for (const [role, username, name] of serviceAccounts) {
    await ensureGrant(await ensureServiceUser(username, name), username, role, projectId);
  }
  const adminId = await ensureUser('admin@bnpb.local', 'Local', 'Administrator', 'AUTH_ADMIN_PASSWORD');
  const admins = await all('/admin/v1/members/_search');
  if (!admins.some(member => member.userId === adminId)) {
    await api('/admin/v1/members', { userId: adminId, roles: ['IAM_OWNER'] });
  }
  console.log('Auth provisioning complete. Project, roles, four applications, claims action, local users, and service users are ready.');
  console.log('Existing passwords, client secrets, and signing keys were preserved. No credentials are printed.');
  console.log(`Access token lifetime: ${accessLifetime}s; maximum refresh/session lifetime: ${sessionLifetime}s.`);
}

main().catch(error => {
  // Errors created above contain paths/status only, never provider request bodies.
  console.error(error instanceof Error ? error.message : 'Auth provisioning failed.');
  process.exitCode = 1;
});

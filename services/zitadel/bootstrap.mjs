// Local provisioning only. Uses the pinned Zitadel Management API v1.
// Never log request bodies, API secrets, tokens, or initial passwords.
import { readFile, writeFile, rename, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const issuer = process.env.OIDC_ISSUER || 'http://auth.localhost:8080';
const appUrl = process.env.APP_PUBLIC_URL || 'http://localhost:8080';
const output = process.env.OIDC_CONFIG_FILE || '/bootstrap/oidc.json';
const patPath = process.env.ZITADEL_ADMIN_PAT_FILE || '/admin/admin.pat';
let token;
let saved = {};

function seconds(name, fallback) {
  const value = process.env[name] || String(fallback);
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) < 1) {
    throw new Error(`${name} must be a positive integer number of seconds.`);
  }
  return Number(value);
}

async function persist() {
  await mkdir(dirname(output), { recursive: true });
  // Read-only mounted into the bridge; no IAM_OWNER PAT is written here.
  await writeFile(`${output}.tmp`, JSON.stringify(saved, null, 2) + '\n', { mode: 0o644 });
  await rename(`${output}.tmp`, output);
}

async function api(path, body, method = body === undefined ? 'GET' : 'POST') {
  const response = await fetch(`${issuer}${path}`, {
    method,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(20000),
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    // Provider messages might echo submitted values. Only log numeric status/code.
    throw new Error(`Provisioning ${method} ${path} failed: HTTP ${response.status}, code ${Number(payload.code) || 0}`);
  }
  return response.json();
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

async function ensureApp(projectId, name, kind, prefix) {
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
  const config = kind === 'oidc' ? {
    name,
    redirectUris: [`${appUrl}/auth/callback`],
    responseTypes: ['OIDC_RESPONSE_TYPE_CODE'],
    grantTypes: ['OIDC_GRANT_TYPE_AUTHORIZATION_CODE', 'OIDC_GRANT_TYPE_REFRESH_TOKEN'],
    appType: 'OIDC_APP_TYPE_WEB',
    authMethodType: 'OIDC_AUTH_METHOD_TYPE_BASIC',
    postLogoutRedirectUris: [`${appUrl}/`],
    version: 'OIDC_VERSION_1_0',
    // HTTP is permitted only for this loopback-only development stack.
    devMode: true,
    accessTokenType: 'OIDC_TOKEN_TYPE_BEARER',
    idTokenRoleAssertion: true,
    loginVersion: { loginV2: { baseUri: `${issuer}/ui/v2/login/` } },
  } : { name, authMethodType: 'API_AUTH_METHOD_TYPE_BASIC' };
  const created = await api(`/management/v1/projects/${projectId}/apps/${kind}`, config);
  if (!created.clientId || !created.clientSecret) throw new Error(`Zitadel did not return client credentials for ${name}.`);
  saved[idKey] = created.appId;
  saved[clientKey] = created.clientId;
  saved[secretKey] = created.clientSecret;
  await persist();
}

async function main() {
  token = (await readFile(patPath, 'utf8')).trim();
  if (!token) throw new Error('The bootstrap PAT file is empty.');
  try { saved = JSON.parse(await readFile(output, 'utf8')); }
  catch (error) { if (error.code !== 'ENOENT') throw new Error('Cannot read persisted OIDC configuration.'); }
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
  const roles = await all(`/management/v1/projects/${projectId}/roles/_search`);
  for (const [key, displayName] of accounts) {
    if (!roles.some(role => role.key === key)) {
      await api(`/management/v1/projects/${projectId}/roles`, { roleKey: key, displayName, group: 'BNPB' });
    }
  }
  await ensureApp(projectId, 'BNPB browser bridge', 'oidc', '');
  await ensureApp(projectId, 'BNPB token introspection', 'api', 'introspection');
  for (const [role, , username, firstName, lastName, passwordKey] of accounts) {
    const userId = await ensureUser(username, firstName, lastName, passwordKey);
    const grants = await all('/management/v1/users/grants/_search');
    const existing = grants.find(grant => grant.userId === userId && grant.projectId === projectId);
    if (!existing) await api(`/management/v1/users/${userId}/grants`, { projectId, roleKeys: [role] });
    else if (existing.roleKeys?.length !== 1 || existing.roleKeys[0] !== role) {
      throw new Error(`Existing ${username} role grant differs from the local seed. Review the user's project grants in Zitadel; grants are not silently overwritten.`);
    }
  }
  const adminId = await ensureUser('admin@bnpb.local', 'Local', 'Administrator', 'AUTH_ADMIN_PASSWORD');
  const admins = await all('/admin/v1/members/_search');
  if (!admins.some(member => member.userId === adminId)) {
    await api('/admin/v1/members', { userId: adminId, roles: ['IAM_OWNER'] });
  }
  console.log('Auth provisioning complete. Project, roles, two applications, and local users are ready.');
  console.log('Existing passwords and client secrets were preserved. No credentials are printed.');
  console.log(`Access token lifetime: ${accessLifetime}s; maximum refresh/session lifetime: ${sessionLifetime}s.`);
}

main().catch(error => {
  // Errors created above contain paths/status only, never provider request bodies.
  console.error(error instanceof Error ? error.message : 'Auth provisioning failed.');
  process.exitCode = 1;
});

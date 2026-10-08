// Acceptance test against compose.auth.yaml, with the real hosted Zitadel UI.
// No screenshots/traces or credential values are written to the report.
import { chromium } from '../services/frontend/node_modules/playwright/index.mjs';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const origin = 'http://localhost:8080';
const values = Object.fromEntries((await readFile(resolve(root, '.env.auth'), 'utf8')).split(/\r?\n/)
  .filter(line => /^[A-Z_]+=/.test(line)).map(line => {
    const index = line.indexOf('='); return [line.slice(0, index), line.slice(index + 1).trim().replace(/^['"]|['"]$/g, '')];
  }));
const results = [];
let stage = 'launch';
function check(condition, name) {
  if (!condition) throw new Error(name);
  results.push({ name, passed: true }); console.log(`PASS ${name}`);
}
const browser = await chromium.launch({ ...(process.platform === 'win32' ? { channel: 'msedge' } : {}), headless: true });
try {
  const anonymous = await fetch(`${origin}/auth/session`);
  check(anonymous.status === 401, 'anonymous session denied');
  for (const [username, passwordKey, role] of [
    ['media-demo', 'AUTH_MEDIA_PASSWORD', 'MEDIA'],
    ['lapangan-demo', 'AUTH_FIELD_TEAM_PASSWORD', 'FIELD_TEAM'],
    ['bnpb-demo', 'AUTH_BNPB_PASSWORD', 'BNPB_INTERNAL'],
  ]) {
    const context = await browser.newContext({ locale: 'en-US' });
    const page = await context.newPage();
    page.setDefaultTimeout(20000);
    stage = `${role}: login redirect`;
    await page.goto(origin);
    await page.getByRole('button', { name: 'Masuk dengan Zitadel' }).click();
    await page.waitForURL(url => url.hostname === 'auth.localhost');
    check(new URL(page.url()).hostname === 'auth.localhost', `${role} hosted login`);
    stage = `${role}: username form`;
    await page.locator('input[autocomplete="username"], input[name="loginName"], input[type="email"], input[type="text"]').first().fill(username);
    await page.getByRole('button', { name: /next|continue|sign in|log in|masuk|berikutnya|lanjut/i }).first().click();
    stage = `${role}: password form`;
    await page.locator('input[type="password"]').fill(values[passwordKey]);
    await page.getByRole('button', { name: /next|continue|sign in|log in|masuk|berikutnya|lanjut/i }).first().click();
    stage = `${role}: OIDC callback`;
    await page.waitForURL(url => url.origin === origin && url.hash === '#/dashboard', { timeout: 30000 });
    await page.getByRole('button', { name: 'Keluar', exact: true }).waitFor();
    let response = await context.request.get(`${origin}/auth/session`);
    let session = await response.json();
    check(response.status() === 200 && session.user?.role === role, `${role} authenticated role`);
    check(!('access_token' in session) && !('refresh_token' in session), `${role} session JSON hides tokens`);
    const cookies = await context.cookies(origin);
    const access = cookies.find(cookie => cookie.name === 'bnpb_access');
    const sessionCookie = cookies.find(cookie => cookie.name === 'bnpb_session');
    check(access?.httpOnly && sessionCookie?.httpOnly && access.sameSite === 'Lax' && !access.domain.startsWith('.'), `${role} private cookies`);
    check(!await page.evaluate(() => document.cookie.includes('bnpb_access=')), `${role} access cookie hidden from JavaScript`);
    const ttl = (Date.parse(session.expires_at) - Date.now()) / 1000;
    check(ttl > 0 && ttl <= 65, `${role} 60-second token lifetime`);
    response = await context.request.get(`${origin}/api/v1/hazards?view=full`);
    check(response.status() === (role === 'MEDIA' ? 403 : 503), `${role} full-view authorization before unfinished backend`);
    response = await context.request.post(`${origin}/auth/refresh`, { headers: { Origin: origin } });
    check(response.status() === 403, `${role} missing CSRF rejected`);

    if (role === 'FIELD_TEAM') {
      stage = 'FIELD_TEAM: automatic refresh';
      console.log('Waiting for the frontend automatic refresh (about 55 seconds).');
      const refreshedResponse = await page.waitForResponse(r => new URL(r.url()).pathname === '/auth/refresh' && r.request().method() === 'POST', { timeout: 65000 });
      check(refreshedResponse.status() === 200, 'automatic browser refresh without login');
      session = await (await context.request.get(`${origin}/auth/session`)).json();
      const freshAccess = (await context.cookies(origin)).find(cookie => cookie.name === 'bnpb_access').value;
      check(access.value !== freshAccess, 'automatic refresh rotates access token');
      response = await fetch(`${origin}/api/v1/hazards`, { headers: { Authorization: `Bearer ${access.value}` } });
      check(response.status === 401, 'old bearer rejected after refresh');

      // Stop React timers so access expires naturally; do not fast-forward clocks.
      await page.goto(`${origin}/health`);
      stage = 'FIELD_TEAM: natural expiry';
      const wait = Math.max(0, Date.parse(session.expires_at) - Date.now()) + 1500;
      console.log(`Waiting ${Math.ceil(wait / 1000)} seconds for natural token expiry.`);
      await delay(wait);
      response = await context.request.get(`${origin}/auth/session`);
      const expired = await response.json();
      check(response.status() === 401 && expired.refresh_required === true, 'expired access requires refresh');
      response = await fetch(`${origin}/api/v1/hazards`, { headers: { Authorization: `Bearer ${freshAccess}` } });
      check(response.status === 401, 'naturally expired bearer rejected');
      response = await context.request.post(`${origin}/auth/refresh`, { headers: { Origin: origin, 'X-CSRF-Token': expired.csrf_token } });
      const renewed = await response.json();
      check(response.status() === 200 && renewed.user.id === session.user.id, 'expired session recovers without password');
      response = await fetch(`${origin}/api/v1/hazards`, { headers: { Authorization: `Bearer ${freshAccess}` } });
      check(response.status === 401, 'expired old bearer remains rejected after refresh');

      stage = 'FIELD_TEAM: SQLite restart';
      execFileSync('docker', ['compose', '--env-file', '.env.auth', '-f', 'compose.auth.yaml', 'restart', 'auth-bridge'], { cwd: root, stdio: 'pipe', windowsHide: true });
      for (let attempt = 0; attempt < 20; attempt++) {
        response = await context.request.get(`${origin}/auth/session`);
        if (response.status() === 200) break;
        await delay(1000);
      }
      check(response.status() === 200 && (await response.json()).user.id === renewed.user.id, 'SQLite session survives container restart');
      await page.goto(`${origin}/#/dashboard`);
      await page.getByRole('button', { name: 'Keluar', exact: true }).waitFor();
    }
    const beforeLogout = (await context.cookies(origin)).find(cookie => cookie.name === 'bnpb_access').value;
    stage = `${role}: logout`;
    await page.getByRole('button', { name: 'Keluar', exact: true }).click();
    // Zitadel V2 asks which provider session to end. Complete its hosted UI.
    stage = `${role}: hosted logout page`;
    await page.waitForURL(url => url.hostname === 'auth.localhost' && url.pathname.endsWith('/logout'), { timeout: 30000 });
    stage = `${role}: End Session confirmation`;
    await page.getByRole('button').filter({ hasText: 'End Session' }).click();
    stage = `${role}: return after End Session`;
    await page.waitForURL(url => url.origin === origin && url.hash === '#/login', { timeout: 30000 });
    check((await context.request.get(`${origin}/auth/session`)).status() === 401, `${role} logout invalidates session`);
    check(!(await context.cookies(origin)).some(cookie => ['bnpb_access', 'bnpb_session'].includes(cookie.name)), `${role} logout clears cookies`);
    response = await fetch(`${origin}/api/v1/hazards`, { headers: { Authorization: `Bearer ${beforeLogout}` } });
    check(response.status === 401, `${role} logout rejects bearer`);
    // Verify provider SSO was ended rather than merely deleting application cookies.
    stage = `${role}: provider login after logout`;
    await page.getByRole('button', { name: 'Masuk dengan Zitadel' }).click();
    await page.waitForURL(url => url.hostname === 'auth.localhost');
    await page.locator('input[autocomplete="username"], input[name="loginName"], input[type="email"], input[type="text"]').first().waitFor();
    check(true, `${role} provider requires login again`);
    await context.close();
  }
  stage = 'complete';
} catch (error) {
  // Do not print Playwright errors: they can include credential-filled actions/URLs.
  results.push({ name: stage, passed: false, error_type: error.name });
  console.error(`FAIL ${stage} (${error.name}). No credentials or request bodies were logged.`);
  const page = browser.contexts().at(-1)?.pages().at(-1);
  if (page) {
    const url = new URL(page.url());
    console.log(JSON.stringify({ origin: url.origin, path: url.pathname,
      inputs: await page.locator('input').evaluateAll(elements => elements.map(element => ({ type: element.type, name: element.name, placeholder: element.placeholder }))),
      buttons: await page.getByRole('button').evaluateAll(elements => elements.map(element => ({ text: element.textContent, label: element.getAttribute('aria-label') }))),
      headings: await page.locator('h1,h2').allTextContents(),
    }));
  }
  process.exitCode = 1;
} finally {
  await browser.close();
  await mkdir(resolve(root, '.cache'), { recursive: true });
  await writeFile(resolve(root, '.cache/auth-browser-test-results.json'), JSON.stringify({
    tested_at: new Date().toISOString(), target: origin, real_provider: true,
    complete: stage === 'complete', results,
  }, null, 2));
  console.log(`Result: ${results.filter(result => result.passed).length} checks passed; report .cache/auth-browser-test-results.json`);
}

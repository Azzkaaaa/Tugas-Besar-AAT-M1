// Shared helpers for the k6 load tests in this directory (see docs/load-testing.md).
import http from 'k6/http';
import encoding from 'k6/encoding';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

export const env = (name, fallback) => __ENV[name] || fallback;

export const HOST = env('E2E_HOST', '127.0.0.1');
export const BASE = `http://${HOST}:${env('E2E_PORT', '8080')}`;
const APP_HOST = env('E2E_APP_HOST', 'localhost:8080');
const AUTH_HOST = env('E2E_AUTH_HOST', 'auth.localhost:8080');
// open() resolves relative paths against this file, so the default points at <repo>/.cache/auth.
const SECRETS_DIR = env('E2E_SECRETS_DIR', '../../.cache/auth');
export const TIMEOUT = Number(env('E2E_TIMEOUT', '120'));
export const USER = env('LOAD_USER', 'svc-bnpb');

const PROJECT_ID = open(`${SECRETS_DIR}/project-id`).trim();
const SECRET = open(`${SECRETS_DIR}/${USER}.secret`).trim();

export const HAZARDS = '/api/v1/hazards';
const STATUSES = ['0', '400', '401', '403', '404', '409', '429', '500', '502', '503', '504'];

// A request counts as an error if it is not a 200 with the expected body.
const errors = new Rate('errors');
const tokenFailures = new Counter('token_failures');

export function seconds(duration) {
  const parts = [...String(duration).matchAll(/(\d+(?:\.\d+)?)(ms|s|m|h)/g)];
  if (!parts.length) throw new Error(`invalid duration: ${duration}`);
  const unit = { ms: 0.001, s: 1, m: 60, h: 3600 };
  return parts.reduce((total, [, value, suffix]) => total + Number(value) * unit[suffix], 0);
}

export const outPath = (name) =>
  env('LOAD_OUT', `.cache/load/${name}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);

// Zitadel issues short-lived client-credentials tokens (about 60 s), so each VU keeps its own
// and renews it 10 s before expiry, like token() in tests/e2e_support.py.
let cached = null;

function token() {
  if (cached && cached.expires > Date.now()) return cached.value;
  const basic = encoding.b64encode(`${USER}:${SECRET}`);
  const scope = `openid urn:zitadel:iam:org:project:id:${PROJECT_ID}:aud`;
  const response = http.post(`${BASE}/oauth/v2/token`,
    { grant_type: 'client_credentials', scope },
    { headers: { Host: AUTH_HOST, Authorization: `Basic ${basic}` }, tags: { name: 'token', kind: 'token' } });
  if (response.status !== 200) {
    // The body may echo request details; report only the status.
    console.warn(`token request for ${USER} failed: HTTP ${response.status}`);
    return null;
  }
  const body = response.json();
  cached = { value: body.access_token, expires: Date.now() + Math.max(0, (body.expires_in || 60) - 10) * 1000 };
  return cached.value;
}

function get(path, accessToken, name, correlationId) {
  return http.get(`${BASE}${path}`, {
    headers: { Host: APP_HOST, Authorization: `Bearer ${accessToken}`, 'X-Correlation-ID': correlationId },
    tags: { name, kind: 'api' },
  });
}

function valid(response, name) {
  if (response.status !== 200) return false;
  try {
    if (name === 'list') return Array.isArray(response.json('items'));
    if (name === 'get') return typeof response.json('item.hazard_id') === 'string';
    return Array.isArray(response.json('freshness'));
  } catch (_) {
    return false;
  }
}

// Waits until the hazard list at path is non-empty and returns its hazard ids; aborts the test on timeout.
export function waitForIds(path, runId) {
  const deadline = Date.now() + TIMEOUT * 1000;
  for (;;) {
    const accessToken = token();
    if (!accessToken) exec.test.abort(`token request for ${USER} failed`);
    const response = get(path, accessToken, 'setup', `${runId}-setup`);
    const items = response.status === 200 ? response.json('items') || [] : [];
    if (items.length) return items.map((item) => item.hazard_id);
    if (Date.now() > deadline) exec.test.abort(`no hazards at ${path} after ${TIMEOUT}s (last HTTP ${response.status})`);
    sleep(3);
  }
}

// One measured API request. Skips the iteration (counted in token_failures) if no token is available.
export function request(runId, name, path) {
  const accessToken = token();
  if (!accessToken) {
    tokenFailures.add(1);
    sleep(1);
    return;
  }
  const correlationId = `${runId}-${exec.vu.idInTest}-${exec.vu.iterationInInstance}`;
  const response = get(path, accessToken, name, correlationId);
  errors.add(!valid(response, name), { name, kind: 'api' });
}

export const randomItem = (items) => items[Math.floor(Math.random() * items.length)];

// Thresholds double as sub-metric declarations, so the summary can report numbers per tag filter.
export function declare(thresholds, filter) {
  thresholds[`http_req_duration{${filter}}`] = thresholds[`http_req_duration{${filter}}`] || ['max>=0'];
  thresholds[`http_reqs{${filter}}`] = thresholds[`http_reqs{${filter}}`] || ['count>=0'];
  thresholds[`errors{${filter}}`] = thresholds[`errors{${filter}}`] || ['rate>=0'];
}

export function declareStatuses(thresholds, filter) {
  for (const status of STATUSES) thresholds[`http_reqs{${filter},status:${status}}`] = ['count>=0'];
}

export const options = (thresholds) => ({
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
  thresholds,
});

const ms = (value) => (value === undefined ? '-' : `${value.toFixed(1)} ms`);
export const pct = (value) => (value === undefined ? '-' : `${(value * 100).toFixed(2)} %`);

export const metric = (data, key) => (data.metrics[key] || {}).values || {};

export const HEADER = ['', 'requests', 'throughput', 'p50', 'p95', 'p99', 'max', 'error rate'];

export function row(data, label, filter, windowSeconds) {
  const duration = metric(data, `http_req_duration{${filter}}`);
  const count = metric(data, `http_reqs{${filter}}`).count || 0;
  return [label, count, `${(count / windowSeconds).toFixed(1)}/s`, ms(duration.med), ms(duration['p(95)']),
    ms(duration['p(99)']), ms(duration.max), pct(metric(data, `errors{${filter}}`).rate)];
}

export function table(rows) {
  const widths = rows[0].map((_, i) => Math.max(...rows.map((cells) => String(cells[i]).length)));
  return rows.map((cells) => `  ${cells.map((cell, i) => String(cell).padEnd(widths[i])).join('  ')}`);
}

export function statusLine(data, filter) {
  const statuses = STATUSES.map((status) => [status, metric(data, `http_reqs{${filter},status:${status}}`).count || 0])
    .filter(([, count]) => count > 0)
    .map(([status, count]) => `${status === '0' ? 'network error' : `HTTP ${status}`}: ${count}`);
  return `  error responses: ${statuses.length ? statuses.join(', ') : 'none'}`;
}

export function failedThresholds(data) {
  return Object.entries(data.metrics)
    .filter(([, value]) => value.thresholds && Object.values(value.thresholds).some((t) => !t.ok))
    .map(([key]) => key);
}

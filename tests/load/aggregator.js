// Load test for the aggregator through the public edge: Envoy -> token exchange -> api-gateway -> aggregator gRPC.
// Run from the repo root: k6 run tests/load/aggregator.js (see docs/load-testing.md).
import exec from 'k6/execution';
import {
  BASE, HAZARDS, HEADER, TIMEOUT, USER, declare, declareStatuses, env, failedThresholds, metric, options as baseOptions,
  outPath, pct, randomItem, request, row, seconds, statusLine, table, waitForIds,
} from './lib.js';

// LOAD_PROFILE=m2 applies P2.6: 10,000 VUs sustained for 120 s, judged on
// uncontrolled errors (<1 %); controlled 429 shedding is reported separately.
// Pair it with infrastructure m2/scale.py for the 3->5->2 replica sequence.
const M2 = env('LOAD_PROFILE', '') === 'm2';
const VUS = Number(env('LOAD_VUS', M2 ? '10000' : '50'));
const DURATION = env('LOAD_DURATION', M2 ? '120s' : '60s');
const RAMP = env('LOAD_RAMP', '10s');
const OUT = outPath('aggregator');

const ENDPOINTS = ['list', 'get', 'statuses'];
// Token requests are excluded: only requests tagged kind:api are reported.
const LOAD = 'scenario:load,kind:api';

const thresholds = {
  [`http_req_duration{${LOAD}}`]: ['p(95)<2000'],
  [`http_reqs{${LOAD}}`]: ['count>0'],
  ...(M2
    ? { [`uncontrolled_errors{${LOAD}}`]: ['rate<0.01'], [`errors{${LOAD}}`]: ['rate>=0'] }
    : { [`http_req_failed{${LOAD}}`]: ['rate<0.01'], [`errors{${LOAD}}`]: ['rate<0.01'] }),
  'token_failures{scenario:load}': ['count==0'],
};
for (const name of ENDPOINTS) declare(thresholds, `${LOAD},name:${name}`);
declareStatuses(thresholds, LOAD);

export const options = {
  ...baseOptions(thresholds),
  setupTimeout: `${TIMEOUT + 30}s`,
  scenarios: {
    // Opens the connections and warms caches; its samples are excluded from the reported numbers.
    warmup: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [{ duration: RAMP, target: VUS }],
      gracefulRampDown: '0s',
      gracefulStop: '0s',
    },
    load: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      startTime: RAMP,
      gracefulStop: '10s',
    },
  },
};

export function setup() {
  const runId = `load-${Date.now()}`;
  const ids = waitForIds(`${HAZARDS}?page_size=100`, runId);
  console.log(`run ${runId}: ${ids.length} hazard ids for detail requests`);
  return { runId, ids };
}

export default function ({ runId, ids }) {
  const roll = Math.random();
  if (roll < 0.6) {
    const source = ['', '&source=BMKG', '&source=PVMBG'][exec.vu.iterationInInstance % 3];
    request(runId, 'list', `${HAZARDS}?page_size=20${source}`);
  } else if (roll < 0.9) {
    request(runId, 'get', `${HAZARDS}/${encodeURIComponent(randomItem(ids))}`);
  } else {
    request(runId, 'statuses', `${HAZARDS}/sources/statuses`);
  }
}

export function handleSummary(data) {
  const window = seconds(DURATION);
  const failed = failedThresholds(data);
  const text = [
    '',
    `aggregator load test: ${VUS} VUs for ${DURATION} (after ${RAMP} warm-up) via ${BASE} as ${USER}`,
    '',
    ...table([['endpoint', ...HEADER.slice(1)], row(data, 'all', LOAD, window),
      ...ENDPOINTS.map((name) => row(data, name, `${LOAD},name:${name}`, window))]),
    '',
    statusLine(data, LOAD),
    `  token failures: ${metric(data, 'token_failures{scenario:load}').count || 0} (iterations skipped, not counted above)`,
    `  http_req_failed: ${pct(metric(data, `http_req_failed{${LOAD}}`).rate)} (status >= 400 or network error)`,
    `  controlled rejections (429 + Retry-After): ${pct(metric(data, `controlled_rejections{${LOAD}}`).rate)}`,
    `  uncontrolled errors: ${pct(metric(data, `uncontrolled_errors{${LOAD}}`).rate)}${M2 ? ' (P2.6 target < 1 %)' : ''}`,
    `  thresholds: ${failed.length ? `FAILED ${failed.join(', ')}` : 'ok'}`,
    `  full summary: ${OUT}`,
    '',
  ].join('\n');

  return { stdout: text, [OUT]: JSON.stringify(data, null, 2) };
}

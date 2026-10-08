// P2 isolation test: BMKG-only requests keep p95 < 300 ms while PVMBG requests run at the same time
// and the PVMBG mock answers in 3 s. Both phases run the same number of VUs: a measured BMKG group plus
// a second group that sends BMKG requests in the baseline and PVMBG requests in the contention phase,
// so the only difference between the phases is the PVMBG traffic.
// Run from the repo root: k6 run tests/load/aggregator-isolation.js (see docs/load-testing.md).
import http from 'k6/http';
import exec from 'k6/execution';
import { Gauge } from 'k6/metrics';
import {
  BASE, HAZARDS, HEADER, HOST, TIMEOUT, USER, declare, declareStatuses, env, failedThresholds, metric,
  options as baseOptions, outPath, randomItem, request, row, seconds, statusLine, table, waitForIds,
} from './lib.js';

const BMKG_VUS = Number(env('ISO_BMKG_VUS', '10'));
const PVMBG_VUS = Number(env('ISO_PVMBG_VUS', '10'));
const RAMP = env('ISO_RAMP', '5s');
const BASELINE = env('ISO_BASELINE', '30s');
const DURATION = env('ISO_DURATION', '60s');
const P95_MS = Number(env('ISO_P95_MS', '300'));
const MIN_DELAY_MS = Number(env('ISO_MIN_PVMBG_DELAY_MS', '2900'));
const OUT = outPath('aggregator-isolation');

// The PVMBG mock's credentials come from the repo's .env, as for the compose stack; never printed.
function dotenv(name) {
  if (__ENV[name]) return __ENV[name];
  let text = '';
  try {
    text = open('../../.env');
  } catch (_) {
    return '';
  }
  const line = text.split('\n').map((l) => l.trim()).find((l) => l.startsWith(`${name}=`));
  return line ? line.slice(name.length + 1).trim().replace(/^["']|["']$/g, '') : '';
}
const PVMBG_TOKEN = dotenv('PVMBG_BEARER_TOKEN');
const PVMBG_URL = `http://${HOST}:${dotenv('PVMBG_HTTP_PORT') || '8082'}/volcanic-reports`;

const BASELINE_START = seconds(RAMP);
const CONTENTION_START = BASELINE_START + seconds(BASELINE);

const PHASES = {
  baseline: 'scenario:baseline,kind:api',
  filler: 'scenario:baseline_filler,kind:api',
  bmkg: 'scenario:contention_bmkg,kind:api',
  pvmbg: 'scenario:contention_pvmbg,kind:api',
};

const thresholds = {
  // The P2 criterion; everything else is reported, not gated.
  [`http_req_duration{${PHASES.bmkg}}`]: [`p(95)<${P95_MS}`],
  [`errors{${PHASES.bmkg}}`]: ['rate<0.01'],
  token_failures: ['count==0'],
  'http_req_duration{kind:probe}': ['max>=0'],
};
for (const filter of Object.values(PHASES)) {
  declare(thresholds, filter);
  declareStatuses(thresholds, filter);
}

// Setup samples land in the summary, which is how the probe result and the start time reach handleSummary.
const runStartedAt = new Gauge('run_started_at');

const bmkgScenario = (executor) => ({ executor: 'constant-vus', exec: 'bmkg', vus: BMKG_VUS, ...executor });
const BASELINE_PHASE = { duration: BASELINE, startTime: `${BASELINE_START}s`, gracefulStop: '0s' };

export const options = {
  ...baseOptions(thresholds),
  setupTimeout: `${TIMEOUT + 60}s`,
  scenarios: {
    warmup: bmkgScenario({ vus: BMKG_VUS + PVMBG_VUS, duration: RAMP, gracefulStop: '0s' }),
    baseline: bmkgScenario(BASELINE_PHASE),
    // Stands in for the PVMBG group with BMKG requests, so the baseline carries the same total load.
    baseline_filler: bmkgScenario({ vus: PVMBG_VUS, ...BASELINE_PHASE }),
    contention_bmkg: bmkgScenario({ duration: DURATION, startTime: `${CONTENTION_START}s`, gracefulStop: '10s' }),
    contention_pvmbg: {
      executor: 'constant-vus', exec: 'pvmbg', vus: PVMBG_VUS, duration: DURATION,
      startTime: `${CONTENTION_START}s`, gracefulStop: '10s',
    },
  },
};

function probePvmbgDelay() {
  if (!PVMBG_TOKEN) exec.test.abort('PVMBG_BEARER_TOKEN not found in the environment or .env');
  const durations = [];
  for (let i = 0; i < 3; i += 1) {
    const response = http.get(PVMBG_URL, {
      headers: { Authorization: `Bearer ${PVMBG_TOKEN}` }, timeout: '15s', tags: { name: 'pvmbg-probe', kind: 'probe' },
    });
    if (response.status !== 200) exec.test.abort(`PVMBG mock probe returned HTTP ${response.status} (outage enabled?)`);
    durations.push(response.timings.duration);
  }
  const median = durations.sort((a, b) => a - b)[1];
  if (median < MIN_DELAY_MS) {
    exec.test.abort(`PVMBG mock delay is ${Math.round(median)} ms, expected >= ${MIN_DELAY_MS} ms; `
      + 'recreate pvmbg-mock with PVMBG_RESPONSE_DELAY_MIN_MS=PVMBG_RESPONSE_DELAY_MAX_MS=3000');
  }
  return durations;
}

export function setup() {
  const durations = probePvmbgDelay();
  const runId = `iso-${Date.now()}`;
  const ids = {
    bmkg: waitForIds(`${HAZARDS}?source=BMKG&page_size=100`, runId),
    pvmbg: waitForIds(`${HAZARDS}?source=PVMBG&page_size=100`, runId),
  };
  console.log(`run ${runId}: PVMBG mock delay ${durations.map(Math.round).join('/')} ms; `
    + `${ids.bmkg.length} BMKG and ${ids.pvmbg.length} PVMBG hazard ids`);
  // Scenario start times count from the end of setup.
  runStartedAt.add(Date.now());
  return { runId, ids };
}

function mix(runId, ids, source, hazardType) {
  const roll = Math.random();
  if (roll < 0.4) request(runId, 'list', `${HAZARDS}?page_size=20&source=${source}`);
  else if (roll < 0.7) request(runId, 'list', `${HAZARDS}?page_size=20&hazard_type=${hazardType}`);
  else request(runId, 'get', `${HAZARDS}/${encodeURIComponent(randomItem(ids))}`);
}

export function bmkg({ runId, ids }) {
  mix(runId, ids.bmkg, 'BMKG', 'SEISMIC');
}

export function pvmbg({ runId, ids }) {
  mix(runId, ids.pvmbg, 'PVMBG', 'VOLCANIC');
}

const iso = (ms) => new Date(ms).toISOString();
const signed = (ms) => {
  const rounded = Math.round(ms * 10) / 10;
  return `${rounded >= 0 ? '+' : '-'}${Math.abs(rounded).toFixed(1)} ms`;
};

export function handleSummary(data) {
  const p95 = (filter) => metric(data, `http_req_duration{${filter}}`)['p(95)'];
  const contention = p95(PHASES.bmkg);
  const baseline = p95(PHASES.baseline);
  const pass = contention !== undefined && contention < P95_MS;
  const delay = metric(data, 'http_req_duration{kind:probe}');
  const started = metric(data, 'run_started_at').value;
  const window = (start, duration) => (started
    ? `${iso(started + start * 1000)} – ${iso(started + (start + seconds(duration)) * 1000)}`
    : '-');
  const failed = failedThresholds(data);

  const text = [
    '',
    `BMKG/PVMBG isolation test via ${BASE} as ${USER}`,
    `  config: ${BMKG_VUS} measured BMKG VUs + ${PVMBG_VUS} second-group VUs (BMKG in baseline, PVMBG in contention); `
      + `warm-up ${RAMP}, baseline ${BASELINE}, contention ${DURATION}`,
    `  PVMBG mock delay (probe, 3 requests): min ${Math.round(delay.min)} ms, median ${Math.round(delay.med)} ms, `
      + `max ${Math.round(delay.max)} ms`,
    `  baseline window:   ${window(BASELINE_START, BASELINE)}`,
    `  contention window: ${window(CONTENTION_START, DURATION)}`,
    '',
    ...table([['phase', ...HEADER.slice(1)],
      row(data, 'baseline BMKG', PHASES.baseline, seconds(BASELINE)),
      row(data, 'baseline 2nd group BMKG', PHASES.filler, seconds(BASELINE)),
      row(data, 'contention BMKG', PHASES.bmkg, seconds(DURATION)),
      row(data, 'contention PVMBG', PHASES.pvmbg, seconds(DURATION))]),
    '',
    `  ${pass ? 'PASS' : 'FAIL'}: BMKG-only p95 under PVMBG contention = ${contention === undefined ? '-' : contention.toFixed(1)} ms `
      + `(target < ${P95_MS} ms, ${metric(data, `http_reqs{${PHASES.bmkg}}`).count || 0} samples)`,
    `  BMKG p95 change vs baseline: ${contention === undefined || baseline === undefined ? '-' : signed(contention - baseline)}`,
    statusLine(data, PHASES.bmkg).replace('error responses', 'contention BMKG errors'),
    statusLine(data, PHASES.pvmbg).replace('error responses', 'contention PVMBG errors'),
    `  token failures: ${metric(data, 'token_failures').count || 0} (iterations skipped, not counted above)`,
    `  thresholds: ${failed.length ? `FAILED ${failed.join(', ')}` : 'ok'}`,
    `  full summary: ${OUT}`,
    '',
  ].join('\n');

  return { stdout: text, [OUT]: JSON.stringify(data, null, 2) };
}

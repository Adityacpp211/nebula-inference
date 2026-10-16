// Phase 7 exit test: sustained overload at three times capacity.
//
// The stack is sized so capacity is known: one mock worker with 4 slots generating
// 16 tokens at 100 tokens/s (about 160 ms a request, so about 25 requests/s), and a
// gateway that admits 4 at a time with a queue of 16 (scripts/load-overload.sh).
// This script offers RATE (default 75) requests/s for DURATION and asserts what
// the admission queue promises (docs/architecture.md §6.3):
//
//   - every response is a success, a 429 with Retry-After (shed), or a 504
//     queue_timeout — never a 5xx from the serving path, never a hang;
//   - the queue never exceeds its bound;
//   - the gateway's heap stays flat: memory does not grow with offered load.
//
// A sampler reads the gateway's /debug/queues once a second for depth and heap.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Gauge, Trend } from 'k6/metrics';

const GATEWAY = __ENV.GATEWAY_URL;
const KEY = __ENV.API_KEY;
const RATE = parseInt(__ENV.RATE || '75', 10);
const DURATION = __ENV.DURATION || '60s';
const MAX_DEPTH = parseInt(__ENV.MAX_DEPTH || '16', 10);

const served = new Counter('served');
const shed = new Counter('shed_429');
const queueTimeouts = new Counter('queue_timeout_504');
const unexpected = new Counter('unexpected_status');
const missingRetryAfter = new Counter('shed_without_retry_after');
const depth = new Gauge('queue_depth');
const depthTrend = new Trend('queue_depth_samples');
const heapMB = new Trend('gateway_heap_mb');
const heapGrowth = new Gauge('gateway_heap_growth_mb');
const queuedMs = new Trend('gateway_queue_ms', true);

export const options = {
  scenarios: {
    overload: {
      executor: 'constant-arrival-rate',
      exec: 'chat',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
    sampler: {
      executor: 'constant-vus',
      exec: 'sample',
      vus: 1,
      duration: DURATION,
    },
  },
  thresholds: {
    unexpected_status: ['count==0'],
    shed_without_retry_after: ['count==0'],
    queue_depth_samples: [`max<=${MAX_DEPTH}`],
    // Flat memory: the heap at the end is within 16 MiB of the heap after warm-up.
    gateway_heap_growth_mb: ['value<16'],
    served: ['count>0'],
    shed_429: ['count>0'],
  },
  summaryTrendStats: ['min', 'med', 'avg', 'p(95)', 'p(99)', 'max'],
};

const headers = { Authorization: `Bearer ${KEY}`, 'Content-Type': 'application/json' };
const payload = JSON.stringify({
  model: 'nebula-mock',
  messages: [{ role: 'user', content: 'hello' }],
  max_tokens: 16,
  nebula: { timeout_ms: 3000 },
});

export function chat() {
  const res = http.post(`${GATEWAY}/v1/chat/completions`, payload, { headers, timeout: '10s' });
  if (res.status === 200) {
    served.add(1);
    const q = res.json('nebula.gateway_queue_ms');
    if (q) queuedMs.add(q);
    return;
  }
  if (res.status === 429) {
    shed.add(1);
    if (!res.headers['Retry-After']) missingRetryAfter.add(1);
    return;
  }
  if (res.status === 504 && res.body && res.body.indexOf('queue_timeout') >= 0) {
    queueTimeouts.add(1);
    return;
  }
  unexpected.add(1);
  check(res, { [`unexpected status ${res.status}`]: () => false });
}

let warmHeap = null;
let started = Date.now();

export function sample() {
  const res = http.get(`${GATEWAY}/debug/queues`);
  if (res.status === 200) {
    const body = res.json();
    let d = 0;
    for (const k of Object.keys(body.queues || {})) {
      d = Math.max(d, body.queues[k].depth);
    }
    depth.add(d);
    depthTrend.add(d);
    const mb = body.heap_alloc_bytes / (1 << 20);
    heapMB.add(mb);
    if (warmHeap === null && Date.now() - started > 10000) {
      warmHeap = mb;
    }
    if (warmHeap !== null) {
      heapGrowth.add(Math.max(0, mb - warmHeap));
    }
  }
  sleep(1);
}

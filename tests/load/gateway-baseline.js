// Phase 4 baseline: the gateway's serving path under steady load.
//
// What this measures: authentication (Redis-cached), rate limiting (the Redis Lua
// script), validation, templating, dispatch to one mock worker, and the response —
// non-streamed and streamed. What it does not measure: model speed. The mock worker
// generates at a fixed, configured rate so the numbers describe NEBULA's overhead
// and are repeatable; Phase 16 re-measures with the real runtime.
//
// Thresholds are deliberately loose in Phase 4: this run ESTABLISHES the baseline
// (docs/roadmap.md, Phase 4 tests). Phase 16 commits tighter ones as SLOs.
//
// Run through scripts/load-gateway.sh, which starts the stack and passes
// GATEWAY_URL and API_KEY.

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const GATEWAY = __ENV.GATEWAY_URL;
const KEY = __ENV.API_KEY;
const RATE = parseInt(__ENV.RATE || '40', 10);
const DURATION = __ENV.DURATION || '60s';

const streamTotal = new Trend('stream_duration', true);
const rejected = new Counter('rejected_429');

export const options = {
  discardResponseBodies: false,
  scenarios: {
    chat: {
      executor: 'constant-arrival-rate',
      exec: 'chat',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
    chat_stream: {
      executor: 'constant-arrival-rate',
      exec: 'chatStream',
      rate: Math.max(1, Math.floor(RATE / 2)),
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    'http_req_failed{scenario:chat}': ['rate<0.01'],
    'http_req_failed{scenario:chat_stream}': ['rate<0.01'],
    'http_req_duration{scenario:chat}': ['p(99)<2000'],
  },
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const headers = { Authorization: `Bearer ${KEY}`, 'Content-Type': 'application/json' };

function body(stream) {
  return JSON.stringify({
    model: 'nebula-mock',
    messages: [{ role: 'user', content: 'load test' }],
    max_tokens: 16,
    temperature: 0,
    stream,
    ...(stream ? { stream_options: { include_usage: true } } : {}),
  });
}

export function chat() {
  const res = http.post(`${GATEWAY}/v1/chat/completions`, body(false), { headers });
  if (res.status === 429) rejected.add(1);
  check(res, {
    'status 200': (r) => r.status === 200,
    'has usage': (r) => r.status !== 200 || r.json('usage.completion_tokens') > 0,
  });
}

export function chatStream() {
  const res = http.post(`${GATEWAY}/v1/chat/completions`, body(true), { headers });
  if (res.status === 429) rejected.add(1);
  streamTotal.add(res.timings.duration);
  check(res, {
    'status 200': (r) => r.status === 200,
    'ends with [DONE]': (r) => r.status !== 200 || r.body.trimEnd().endsWith('data: [DONE]'),
    'no error frame': (r) => r.status !== 200 || !r.body.includes('"error"'),
  });
}

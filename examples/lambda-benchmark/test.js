import { check } from 'k6';
import { Trend } from 'k6/metrics';
import http from 'k6/http';

const RELAY_PORT = __ENV.RELAY_PORT || 7676;
const RELAY_URL = `http://localhost:${RELAY_PORT}`;
const ID = __ENV.ID || 'GithubCommits';

const defaultScenario = {
  executor: 'constant-arrival-rate',
  preAllocatedVUs: 600,
  rate: __ENV.RPS || 1,
  duration: __ENV.DURATION || '10s',
  gracefulStop: '300s',
};

export const options = {
  setupTimeout: '300s',
  teardownTimeout: '300s',
  scenarios: {
    basic: Object.assign(
      Object.assign({}, defaultScenario),
      { exec: 'basic' },
    ),
    lambda: Object.assign(
      Object.assign({}, defaultScenario),
      { exec: 'lambda' },
    ),
  },
};

const basicLatency = new Trend('Latency (Cube.js basic)', true);

export function basic() {
    const res = http.get(`${RELAY_URL}/basic/${ID}`);
    basicLatency.add(res.timings.duration);
    check(res, {
      'is status 200': res => res.status === 200,
    });
}

const lambdaLatency = new Trend('Latency (Cube.js with Postgres)', true);

export function lambda() {
    const res = http.get(`${RELAY_URL}/lambda/${ID}`);
    lambdaLatency.add(res.timings.duration);
    check(res, {
      'is status 200': res => res.status === 200,
    });
}

// First API call creates the pre-aggregation.
function init(endpoint) {
  try {
    console.log('Init', endpoint);
    http.get(`${RELAY_URL}/${endpoint}/${ID}`)
    console.log('Done', endpoint);
  } catch (e) {
    console.log('Error', endpoint, e)
  }
}

export function setup() {
  init('basic');
  init('lambda');
}

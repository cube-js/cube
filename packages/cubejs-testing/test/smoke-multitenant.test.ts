// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { sign } from 'jsonwebtoken';
import { PostgresDBRunner } from '@cubejs-backend/testing-shared';
import type { StartedTestContainer } from 'testcontainers';

import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  stopIfStarted,
} from './smoke-tests';

/**
 * Two tenants served by one Cube process hammer `/v1/load` in parallel, and
 * every response body must belong to the request that received it: valid
 * JSON, only the requesting tenant's members, and only rows matching the
 * request's own filters. Reproduction attempt for a single REST response that
 * carried another tenant's rows wrapped in binary bytes (CUB-6751).
 */

const ROUNDS = parseInt(process.env.SMOKE_MULTITENANT_ROUNDS || '8', 10);
const CONCURRENCY = parseInt(process.env.SMOKE_MULTITENANT_CONCURRENCY || '48', 10);

type Tenant = 'tenant_a' | 'tenant_b';

type TenantSpec = {
  view: string,
  foreignMarkers: string[],
  filterA: { member: string, values: string[] },
  filterB: { member: string, values: string[] },
  rollupQuery: (a: string, b: string, i: number) => Record<string, unknown>,
  rawQuery: (a: string, b: string, i: number) => Record<string, unknown>,
};

const DATE_RANGE = ['2026-09-01', '2026-10-30'];

const TENANTS: Record<Tenant, TenantSpec> = {
  tenant_a: {
    view: 'shipment_events_view',
    foreignMarkers: ['match_stats', 'league', 'team_red', 'player_'],
    filterA: {
      member: 'shipment_events_view.event_name',
      values: ['created', 'shipped', 'delivered', 'returned'],
    },
    filterB: { member: 'shipment_events_view.region', values: ['north', 'south', 'east'] },
    rollupQuery: (a, b) => ({
      measures: ['shipment_events_view.count'],
      dimensions: ['shipment_events_view.event_name', 'shipment_events_view.region'],
      timeDimensions: [{
        dimension: 'shipment_events_view.event_at',
        granularity: 'day',
        dateRange: DATE_RANGE,
      }],
      filters: [
        { member: 'shipment_events_view.event_name', operator: 'equals', values: [a] },
        { member: 'shipment_events_view.region', operator: 'equals', values: [b] },
      ],
    }),
    rawQuery: (a, b, i) => ({
      measures: ['shipment_events_view.avg_duration_seconds'],
      dimensions: [
        'shipment_events_view.id',
        'shipment_events_view.shipment_id',
        'shipment_events_view.event_name',
        'shipment_events_view.region',
      ],
      filters: [
        { member: 'shipment_events_view.event_name', operator: 'equals', values: [a] },
        { member: 'shipment_events_view.region', operator: 'equals', values: [b] },
      ],
      order: { 'shipment_events_view.id': 'asc' },
      // Distinct limits keep most of these out of the result cache
      limit: 2000 + i,
    }),
  },
  tenant_b: {
    view: 'match_stats_view',
    foreignMarkers: ['shipment_events', 'event_name', 'shipment_', 'delivered'],
    filterA: {
      member: 'match_stats_view.league',
      values: ['league_one', 'league_two', 'cup'],
    },
    filterB: {
      member: 'match_stats_view.team',
      values: ['team_red', 'team_blue', 'team_green', 'team_gold'],
    },
    rollupQuery: (a, b) => ({
      measures: ['match_stats_view.total_minutes'],
      dimensions: ['match_stats_view.league', 'match_stats_view.team'],
      timeDimensions: [{
        dimension: 'match_stats_view.played_at',
        granularity: 'day',
        dateRange: DATE_RANGE,
      }],
      filters: [
        { member: 'match_stats_view.league', operator: 'equals', values: [a] },
        { member: 'match_stats_view.team', operator: 'equals', values: [b] },
      ],
    }),
    rawQuery: (a, b, i) => ({
      measures: ['match_stats_view.avg_score'],
      dimensions: [
        'match_stats_view.id',
        'match_stats_view.player_id',
        'match_stats_view.country',
        'match_stats_view.opponent',
        'match_stats_view.league',
        'match_stats_view.team',
      ],
      filters: [
        { member: 'match_stats_view.league', operator: 'equals', values: [a] },
        { member: 'match_stats_view.team', operator: 'equals', values: [b] },
      ],
      order: { 'match_stats_view.id': 'asc' },
      limit: 2000 + i,
    }),
  },
};

function tokenFor(tenant: Tenant) {
  return sign({ tenant }, DEFAULT_CONFIG.CUBEJS_API_SECRET, { expiresIn: '2 days' });
}

type Request = {
  tenant: Tenant,
  kind: 'rollup' | 'raw' | 'multi',
  query: Record<string, unknown> | Record<string, unknown>[],
  expected: { a: string, b: string }[],
};

function buildRequest(tenant: Tenant, i: number): Request {
  const spec = TENANTS[tenant];
  const pick = (n: number) => ({
    a: spec.filterA.values[n % spec.filterA.values.length],
    b: spec.filterB.values[Math.floor(n / spec.filterA.values.length) % spec.filterB.values.length],
  });

  const first = pick(i);
  switch (i % 3) {
    case 0:
      return { tenant, kind: 'rollup', query: spec.rollupQuery(first.a, first.b, i), expected: [first] };
    case 1:
      return { tenant, kind: 'raw', query: spec.rawQuery(first.a, first.b, i), expected: [first] };
    default: {
      const second = pick(i + 1);
      return {
        tenant,
        kind: 'multi',
        // Blending needs every query on the same time dimension granularity
        query: [spec.rollupQuery(first.a, first.b, i), spec.rollupQuery(second.a, second.b, i)],
        expected: [first, second],
      };
    }
  }
}

type Failure = { tenant: Tenant, kind: string, reason: string, sample: string };

function checkRows(
  spec: TenantSpec,
  rows: Record<string, unknown>[],
  expected: { a: string, b: string },
): string | null {
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      // Blended results add a synthetic `time` / `time.<granularity>` column
      if (!key.startsWith(`${spec.view}.`) && key !== 'time' && !key.startsWith('time.')) {
        return `foreign member in row: ${key}`;
      }
    }

    if (row[spec.filterA.member] !== expected.a || row[spec.filterB.member] !== expected.b) {
      return `row does not match this request's filters (${expected.a}/${expected.b}): ${JSON.stringify(row).slice(0, 300)}`;
    }
  }

  return null;
}

const CONTINUE_WAIT = Symbol('Continue wait');

function checkBody(
  request: Request,
  status: number,
  bytes: Buffer,
): Failure | null | typeof CONTINUE_WAIT {
  const spec = TENANTS[request.tenant];
  const text = bytes.toString('utf8');
  const fail = (reason: string): Failure => ({
    tenant: request.tenant, kind: request.kind, reason, sample: text.slice(0, 500),
  });

  for (const marker of spec.foreignMarkers) {
    if (text.includes(marker)) {
      return fail(`body contains another tenant's marker "${marker}"`);
    }
  }

  let body: any;

  try {
    body = JSON.parse(text);
  } catch (e: any) {
    return fail(`body is not valid JSON (${e.message}), ${bytes.length} bytes`);
  }

  if (body.error === 'Continue wait') {
    return CONTINUE_WAIT;
  }

  if (status !== 200 || body.error) {
    return fail(`HTTP ${status}: ${body.error}`);
  }

  const results = Array.isArray(request.query) ? body.results : [body];
  if (!Array.isArray(results) || results.length !== request.expected.length) {
    return fail('unexpected number of results');
  }

  for (let i = 0; i < results.length; i++) {
    if (!Array.isArray(results[i].data) || results[i].data.length === 0) {
      return fail(`result ${i} has no data`);
    }

    const rowError = checkRows(spec, results[i].data, request.expected[i]);
    if (rowError) {
      return fail(rowError);
    }
  }

  return null;
}

async function load(apiUrl: string, request: Request): Promise<Failure | null> {
  for (let attempt = 0; attempt < 120; attempt++) {
    const response = await fetch(`${apiUrl}/load`, {
      method: 'POST',
      headers: {
        Authorization: tokenFor(request.tenant),
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: request.query,
        ...(Array.isArray(request.query) ? { queryType: 'multi' } : {}),
      }),
    });
    const outcome = checkBody(request, response.status, Buffer.from(await response.arrayBuffer()));

    if (outcome !== CONTINUE_WAIT) {
      return outcome;
    }
  }

  return {
    tenant: request.tenant, kind: request.kind, reason: 'still "Continue wait" after 120 attempts', sample: '',
  };
}

describe('multitenant', () => {
  jest.setTimeout(60 * 15 * 1000);

  let db: StartedTestContainer;
  let birdbox: BirdBox;

  beforeAll(async () => {
    db = await PostgresDBRunner.startContainer({});
    birdbox = await getBirdbox(
      'postgres',
      {
        ...DEFAULT_CONFIG,
        CUBEJS_DB_TYPE: 'postgres',
        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'test',
        CUBEJS_DB_USER: 'test',
        CUBEJS_DB_PASS: 'test',
        // Results, queue and pre-aggregations all go through Cube Store,
        // like a Cube Cloud deployment
        CUBEJS_CACHE_AND_QUEUE_DRIVER: 'cubestore',
        CUBEJS_DB_QUERY_TIMEOUT: '10m',
      },
      {
        schemaDir: 'multitenant/schema',
        cubejsConfig: 'multitenant/cube.js',
      }
    );
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await stopIfStarted('birdbox', birdbox);
    await stopIfStarted('db', db);
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('each tenant can load its own view', async () => {
    for (const tenant of Object.keys(TENANTS) as Tenant[]) {
      for (let i = 0; i < 3; i++) {
        expect(await load(birdbox.configuration.apiUrl, buildRequest(tenant, i))).toBeNull();
      }
    }
  });

  test('a tenant cannot query another tenant\'s view', async () => {
    const response = await fetch(`${birdbox.configuration.apiUrl}/load`, {
      method: 'POST',
      headers: { Authorization: tokenFor('tenant_a'), 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: TENANTS.tenant_b.rawQuery('league_one', 'team_red', 0) }),
    });
    const text = await response.text();

    expect(response.status).toBe(400);
    expect(text).not.toContain('team_red');
  });

  test('concurrent loads from both tenants never mix response bodies', async () => {
    const { apiUrl } = birdbox.configuration;
    const failures: Failure[] = [];
    let total = 0;

    for (let round = 0; round < ROUNDS; round++) {
      // Interleave the tenants so their requests share connections, the
      // event loop and the native result pipeline at the same moment
      const requests = Array.from({ length: CONCURRENCY }, (_, i) => buildRequest(
        i % 2 === 0 ? 'tenant_a' : 'tenant_b',
        round * CONCURRENCY + i
      ));
      total += requests.length;

      const results = await Promise.all(requests.map((r) => load(apiUrl, r)));
      failures.push(...results.filter((f): f is Failure => f !== null));
    }

    if (failures.length) {
      console.error(`[multitenant] ${failures.length} of ${total} responses failed:`, failures.slice(0, 10));
    }

    expect(failures).toEqual([]);
  });
});

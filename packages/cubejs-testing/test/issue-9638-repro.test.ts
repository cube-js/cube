/**
 * https://github.com/cube-js/cube/issues/9638
 * rollup_lambda + union_with_source_data + CUBEJS_SCHEDULED_REFRESH_TIMEZONES=Europe/Madrid:
 * the lambda (source data) query lower bound is derived from a *localized* max()
 * of the time dimension that is then treated as UTC, so rows between the real UTC
 * max at build time and the (shifted) lambda lower bound are never returned.
 *
 * Requires: Postgres on localhost:5432 (test/test), Cube Store binary.
 */
import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import { startReproEnv, pgExec, ReproEnv } from './issue-repros-9638-3486-env';

const INITIAL_ROWS = [
  ['1', '2025-01-15T10:23:45'], ['2', '2025-02-05T14:12:30'], ['3', '2025-02-25T08:45:10'],
  ['4', '2025-03-10T11:34:56'], ['5', '2025-03-25T19:05:15'], ['6', '2025-04-01T06:50:00'],
  ['7', '2025-04-15T13:20:40'], ['8', '2025-05-05T17:45:30'], ['9', '2025-05-20T09:10:20'],
  ['10', '2025-05-30T23:59:59'],
];

// Inserted after the pre-aggregation is built. All of them are strictly after
// the max(createdAt) = 2025-05-30T23:59:59Z at build time, so the lambda
// (source-data) part must return them.
const LATE_ROWS = [
  ['11', '2025-05-31T00:30:00'],
  ['12', '2025-05-31T01:30:00'],
  ['13', '2025-05-31T03:00:00'],
  ['14', '2025-05-31T05:00:00'],
];

const values = (rows: string[][]) => rows.map(([id, ts]) => `('${id}', '${ts}')`).join(', ');

describe('issue #9638: rollup_lambda with scheduled refresh timezone', () => {
  jest.setTimeout(5 * 60 * 1000);
  let env: ReproEnv;

  beforeAll(async () => {
    await pgExec('DROP TABLE IF EXISTS public.issue_9638_events');
    await pgExec('CREATE TABLE public.issue_9638_events (id text, created_at timestamp(3))');
    await pgExec(`INSERT INTO public.issue_9638_events VALUES ${values(INITIAL_ROWS)}`);
    env = await startReproEnv('issue-9638', {
      CUBEJS_SCHEDULED_REFRESH_TIMEZONES: 'Europe/Madrid',
      CUBEJS_PRE_AGGREGATIONS_SCHEMA: 'issue_9638_pre',
    });
  });

  afterAll(async () => {
    await env?.stop();
    await pgExec('DROP TABLE IF EXISTS public.issue_9638_events');
  });

  test('lambda result equals source data for rows added after the build', async () => {
    const initial = await env.loadUntilReady({ measures: ['Issue9638.count'], timezone: 'Europe/Madrid' });
    expect(initial.error).toBeUndefined();
    expect(Object.keys(initial.usedPreAggregations || {})).toEqual(['issue_9638_pre.issue9638_main']);
    expect(initial.data).toEqual([{ 'Issue9638.count': '10' }]);

    await pgExec(`INSERT INTO public.issue_9638_events VALUES ${values(LATE_ROWS)}`);
    // default cube refresh key (and thus the lambda source query cache) is 10 seconds
    await new Promise((resolve) => setTimeout(resolve, 12000));

    const source = await pgExec('SELECT id FROM public.issue_9638_events ORDER BY id');
    const expectedIds = source.rows.map((r) => r.id).sort();

    const result = await env.loadUntilReady({
      measures: ['Issue9638.count'],
      dimensions: ['Issue9638.id'],
      timezone: 'Europe/Madrid',
    });
    expect(result.error).toBeUndefined();
    expect(Object.keys(result.usedPreAggregations || {})).toEqual(['issue_9638_pre.issue9638_main']);

    const bounds = [...env.logs.join('').matchAll(/"buildRangeEnd": "([^"]+)"/g)].map((m) => m[1]).sort();
    // eslint-disable-next-line no-console
    console.log('max buildRangeEnd (lambda lower bound):', bounds[bounds.length - 1]);

    const actualIds = result.data.map((r: any) => r['Issue9638.id']).sort();
    expect(actualIds).toEqual(expectedIds);
  });
});

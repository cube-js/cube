/**
 * https://github.com/cube-js/cube/issues/3486
 * A rollup pre-aggregation referencing the same dimension twice compiles fine,
 * then fails at build time in Cube Store. Expected: either a compile-time error
 * for the pre-aggregation, or the duplicate reference is de-duplicated and the
 * query is served correctly.
 *
 * Requires: Postgres on localhost:5432 (test/test), Cube Store binary.
 */
import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import { startReproEnv, pgExec, ReproEnv } from './issue-repros-9638-3486-env';

describe('issue #3486: rollup with a duplicated dimension reference', () => {
  jest.setTimeout(5 * 60 * 1000);
  let env: ReproEnv;

  beforeAll(async () => {
    await pgExec('DROP TABLE IF EXISTS public.issue_3486_orders');
    await pgExec('CREATE TABLE public.issue_3486_orders (id int, status text, created_at timestamp)');
    await pgExec(`INSERT INTO public.issue_3486_orders VALUES
      (1, 'a', '2025-01-01'), (2, 'b', '2025-01-02'), (3, 'a', '2025-01-03')`);
    env = await startReproEnv('issue-3486', {
      CUBEJS_PRE_AGGREGATIONS_SCHEMA: 'issue_3486_pre',
    });
  });

  afterAll(async () => {
    await env?.stop();
    await pgExec('DROP TABLE IF EXISTS public.issue_3486_orders');
  });

  test('duplicate member reference is rejected at compile time or de-duplicated', async () => {
    const meta = await env.meta();
    if (meta.error) {
      // Acceptable fix: compile-time validation error mentioning the duplicate.
      expect(meta.error).toMatch(/Issue3486.*status|duplicate/i);
      return;
    }

    const result = await env.loadUntilReady({
      measures: ['Issue3486.count'],
      dimensions: ['Issue3486.status'],
      order: { 'Issue3486.status': 'asc' },
    });
    expect(result.error).toBeUndefined();
    expect(result.data).toEqual([
      { 'Issue3486.status': 'a', 'Issue3486.count': '2' },
      { 'Issue3486.status': 'b', 'Issue3486.count': '1' },
    ]);
  });
});

import type {
  LoadMethodOptions,
  Query,
  ResultSet,
  SqlQuery,
} from '@cubejs-client/core';
import { get, writable } from 'svelte/store';
import { describe, expect, it, vi } from 'vitest';

import { createQueryRenderer } from '../src/lib';
import type { QueryRendererInput } from '../src/lib/types';
import { cubeApi, flushMicrotasks, resultSet } from './helpers';

describe('createQueryRenderer', () => {
  it('coordinates SQL and result loading for one query', async () => {
    const result = resultSet('main');
    const sqlResult = { sql: ['select 1'] } as unknown as SqlQuery;
    const load = vi.fn().mockResolvedValue(result);
    const sql = vi.fn().mockResolvedValue(sqlResult);
    const store = createQueryRenderer(
      { query: { measures: ['Orders.count'] } },
      { cubeApi: cubeApi({ load, sql }), loadSql: true }
    );
    const unsubscribe = store.subscribe(() => undefined);

    await vi.waitFor(() => expect(get(store).resultSet).toBe(result));

    expect(get(store).sqlQuery).toBe(sqlResult);
    expect(load).toHaveBeenCalledOnce();
    expect(sql).toHaveBeenCalledOnce();
    expect((load.mock.calls[0][1] as LoadMethodOptions).mutexKey).toBe(
      'query'
    );
    expect((sql.mock.calls[0][1] as LoadMethodOptions).mutexKey).toBe('sql');

    unsubscribe();
    await store.destroy();
  });

  it('supports SQL-only mode without loading data', async () => {
    const sqlResult = { sql: ['select 1'] } as unknown as SqlQuery;
    const load = vi.fn();
    const sql = vi.fn().mockResolvedValue(sqlResult);
    const store = createQueryRenderer(
      { query: { measures: ['Orders.count'] } },
      { cubeApi: cubeApi({ load, sql }), loadSql: 'only' }
    );
    const unsubscribe = store.subscribe(() => undefined);

    await vi.waitFor(() => expect(get(store).sqlQuery).toBe(sqlResult));

    expect(load).not.toHaveBeenCalled();
    expect(get(store)).toMatchObject({
      resultSet: null,
      error: null,
      isLoading: false,
    });

    unsubscribe();
    await store.destroy();
  });

  it('loads named queries into a keyed result object', async () => {
    const main = resultSet('main');
    const totals = resultSet('totals');
    const load = vi.fn(
      async (
        query: Query,
        options: LoadMethodOptions
      ): Promise<ResultSet> => {
        void options;
        return query.dimensions?.length ? main : totals;
      }
    );
    const store = createQueryRenderer(
      {
        queries: {
          main: {
            measures: ['Orders.count'],
            dimensions: ['Orders.status'],
          },
          totals: { measures: ['Orders.count'] },
        },
      },
      { cubeApi: cubeApi({ load }) }
    );
    const unsubscribe = store.subscribe(() => undefined);

    await vi.waitFor(() =>
      expect(get(store).resultSet).toEqual({ main, totals })
    );

    expect(load).toHaveBeenCalledTimes(2);
    expect(load.mock.calls.map((call) => call[1].mutexKey)).toEqual([
      'query:main',
      'query:totals',
    ]);

    unsubscribe();
    await store.destroy();
  });

  it('does not reload named queries when only object key insertion order changes', async () => {
    const mainQuery = { measures: ['Orders.count'] };
    const totalsQuery = {
      measures: ['Orders.count'],
      dimensions: ['Orders.status'],
    };
    const input = writable<QueryRendererInput>({
      queries: { main: mainQuery, totals: totalsQuery },
    });
    const load = vi.fn().mockResolvedValue(resultSet('named'));
    const store = createQueryRenderer(input, {
      cubeApi: cubeApi({ load }),
    });
    const unsubscribe = store.subscribe(() => undefined);

    await vi.waitFor(() => expect(load).toHaveBeenCalledTimes(2));
    input.set({ queries: { totals: totalsQuery, main: mainQuery } });
    await flushMicrotasks();

    expect(load).toHaveBeenCalledTimes(2);

    unsubscribe();
    await store.destroy();
  });

  it('rejects incompatible named-query modes before calling core', async () => {
    const load = vi.fn();
    const sql = vi.fn();
    const store = createQueryRenderer(
      { queries: { main: { measures: ['Orders.count'] } } },
      { cubeApi: cubeApi({ load, sql }), loadSql: true }
    );
    const unsubscribe = store.subscribe(() => undefined);

    await vi.waitFor(() =>
      expect(get(store).error?.message).toBe(
        'loadSql is not supported with named queries.'
      )
    );

    expect(load).not.toHaveBeenCalled();
    expect(sql).not.toHaveBeenCalled();

    unsubscribe();
    await store.destroy();
  });
});

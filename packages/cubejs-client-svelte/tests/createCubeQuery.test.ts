import {
  ProgressResult,
  type LoadMethodOptions,
  type Query,
  type ResultSet,
} from '@cubejs-client/core';
import { get, writable } from 'svelte/store';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { createCubeQuery } from '../src/lib';
import type { CubeQueryState } from '../src/lib/types';
import {
  cubeApi,
  deferred,
  flushMicrotasks,
  resultSet,
} from './helpers';

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('createCubeQuery', () => {
  it('loads a query, reports progress, and forwards request options', async () => {
    const result = resultSet('orders');
    const load = vi.fn(
      async (_query: Query, options: LoadMethodOptions): Promise<ResultSet> => {
        options.progressCallback?.(
          new ProgressResult({ stage: 'Executing query', timeElapsed: 42 })
        );
        return result;
      }
    );
    const store = createCubeQuery(
      { measures: ['Orders.count'] },
      {
        cubeApi: cubeApi({ load }),
        castNumerics: true,
        cache: 'no-cache',
        baseRequestId: 'svelte-test',
      }
    );

    let latest!: CubeQueryState;
    const unsubscribe = store.subscribe((value) => {
      latest = value;
    });

    await vi.waitFor(() => expect(latest.resultSet).toBe(result));

    expect(load).toHaveBeenCalledOnce();
    expect(load.mock.calls[0][0]).toEqual({ measures: ['Orders.count'] });
    expect(load.mock.calls[0][1]).toMatchObject({
      mutexKey: 'query',
      castNumerics: true,
      cache: 'no-cache',
      baseRequestId: 'svelte-test',
    });
    expect(latest).toMatchObject({
      resultSet: result,
      error: null,
      isLoading: false,
      progress: null,
    });

    unsubscribe();
    await store.destroy();
  });

  it('aborts superseded requests and ignores their late results', async () => {
    const first = deferred<ResultSet>();
    const second = deferred<ResultSet>();
    const load = vi
      .fn()
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise);
    const query = writable<Query>({ measures: ['Orders.count'] });
    const store = createCubeQuery(query, { cubeApi: cubeApi({ load }) });
    const unsubscribe = store.subscribe(() => undefined);

    await vi.waitFor(() => expect(load).toHaveBeenCalledTimes(1));
    query.set({ measures: ['Orders.total'] });
    await vi.waitFor(() => expect(load).toHaveBeenCalledTimes(2));

    const firstOptions = load.mock.calls[0][1] as LoadMethodOptions;
    expect(firstOptions.signal?.aborted).toBe(true);

    const secondResult = resultSet('second');
    second.resolve(secondResult);
    await vi.waitFor(() => expect(get(store).resultSet).toBe(secondResult));

    first.resolve(resultSet('late-first'));
    await flushMicrotasks();
    expect(get(store).resultSet).toBe(secondResult);
    expect(get(store).previousQuery).toEqual({
      measures: ['Orders.total'],
    });

    unsubscribe();
    await store.destroy();
  });

  it('honors skip automatically but lets an explicit refetch override it', async () => {
    const result = resultSet('manual');
    const load = vi.fn().mockResolvedValue(result);
    const store = createCubeQuery(
      { measures: ['Orders.count'] },
      { cubeApi: cubeApi({ load }), skip: true }
    );
    const unsubscribe = store.subscribe(() => undefined);

    await flushMicrotasks();
    expect(load).not.toHaveBeenCalled();

    await store.refetch();
    expect(load).toHaveBeenCalledOnce();
    expect(get(store).resultSet).toBe(result);

    unsubscribe();
    await store.destroy();
  });

  it('tears down a live subscription when the last consumer leaves', async () => {
    const unsubscribeRequest = vi.fn().mockResolvedValue(undefined);
    let callback!: (error: Error | null, result: ResultSet | null) => void;
    const subscribe = vi.fn(
      (
        _query: Query,
        _options: LoadMethodOptions,
        next: typeof callback
      ) => {
        callback = next;
        return { unsubscribe: unsubscribeRequest };
      }
    );
    const store = createCubeQuery(
      { measures: ['Orders.count'] },
      { cubeApi: cubeApi({ subscribe }), subscribe: true }
    );
    const unsubscribe = store.subscribe(() => undefined);

    await vi.waitFor(() => expect(subscribe).toHaveBeenCalledOnce());
    const result = resultSet('subscription');
    callback(null, result);
    await vi.waitFor(() => expect(get(store).resultSet).toBe(result));

    unsubscribe();
    await vi.waitFor(() => expect(unsubscribeRequest).toHaveBeenCalledOnce());
    expect(get(store).isLoading).toBe(false);
    await store.destroy();
  });

  it('does not auto-start during server rendering', async () => {
    vi.stubGlobal('window', undefined);
    const load = vi.fn().mockResolvedValue(resultSet('server'));
    const store = createCubeQuery(
      { measures: ['Orders.count'] },
      { cubeApi: cubeApi({ load }) }
    );

    const unsubscribe = store.subscribe(() => undefined);
    await flushMicrotasks();

    expect(load).not.toHaveBeenCalled();
    unsubscribe();
    await store.destroy();
  });
});

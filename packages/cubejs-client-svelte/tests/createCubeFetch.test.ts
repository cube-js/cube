import type { Query, SqlQuery } from '@cubejs-client/core';
import { get, writable } from 'svelte/store';
import { describe, expect, it, vi } from 'vitest';

import {
  createCubeMeta,
  createCubeSql,
  createLazyDryRun,
} from '../src/lib';
import {
  cubeApi,
  dryRunResponse,
  flushMicrotasks,
  ordersMeta,
} from './helpers';

describe('Cube fetch helpers', () => {
  it('loads metadata without requiring a query', async () => {
    const response = ordersMeta();
    const meta = vi.fn().mockResolvedValue(response);
    const store = createCubeMeta({ cubeApi: cubeApi({ meta }) });
    const unsubscribe = store.subscribe(() => undefined);

    await vi.waitFor(() => expect(get(store).response).toBe(response));
    expect(meta).toHaveBeenCalledOnce();
    expect(get(store).error).toBeNull();

    unsubscribe();
    await store.destroy();
  });

  it('waits for a present query before loading SQL', async () => {
    const response = { sql: ['select 1'] } as unknown as SqlQuery;
    const sql = vi.fn().mockResolvedValue(response);
    const query = writable<Query>({});
    const store = createCubeSql(query, { cubeApi: cubeApi({ sql }) });
    const unsubscribe = store.subscribe(() => undefined);

    await flushMicrotasks();
    expect(sql).not.toHaveBeenCalled();

    query.set({ measures: ['Orders.count'] });
    await vi.waitFor(() => expect(get(store).response).toBe(response));
    expect(sql).toHaveBeenCalledOnce();

    unsubscribe();
    await store.destroy();
  });

  it('keeps lazy dry runs idle until run is called', async () => {
    const query = { measures: ['Orders.count'] };
    const response = dryRunResponse(query);
    const dryRun = vi.fn().mockResolvedValue(response);
    const store = createLazyDryRun(query, {
      cubeApi: cubeApi({ dryRun }),
      skip: true,
    });
    const unsubscribe = store.subscribe(() => undefined);

    await flushMicrotasks();
    expect(dryRun).not.toHaveBeenCalled();

    await store.run();
    expect(dryRun).toHaveBeenCalledOnce();
    expect(get(store).response).toBe(response);

    unsubscribe();
    await store.destroy();
  });

  it('normalizes Cube response errors', async () => {
    const meta = vi.fn().mockRejectedValue({
      message: 'Request failed',
      response: { plainError: 'Schema compilation failed' },
    });
    const store = createCubeMeta({ cubeApi: cubeApi({ meta }) });
    const unsubscribe = store.subscribe(() => undefined);

    await vi.waitFor(() =>
      expect(get(store).error?.message).toBe('Schema compilation failed')
    );

    expect(get(store).response).toBeNull();
    expect(get(store).isLoading).toBe(false);

    unsubscribe();
    await store.destroy();
  });
});

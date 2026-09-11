import type {
  DryRunResponse,
  LoadMethodOptions,
  Meta,
  Query,
} from '@cubejs-client/core';
import { get, writable } from 'svelte/store';
import { describe, expect, it, vi } from 'vitest';

import { createQueryBuilder } from '../src/lib';
import type {
  QueryBuilderOptions,
  QueryBuilderState,
} from '../src/lib/types';
import {
  ORDERS_COUNT,
  ORDERS_STATUS,
  ORDERS_TOTAL,
  cubeApi,
  deferred,
  dryRunResponse,
  flushMicrotasks,
  ordersMeta,
  resultSet,
} from './helpers';

describe('createQueryBuilder', () => {
  it('loads metadata, derives members, validates, and executes the query', async () => {
    const metadata = ordersMeta();
    const result = resultSet('builder');
    const meta = vi.fn().mockResolvedValue(metadata);
    const dryRun = vi.fn(async (query: Query) => dryRunResponse(query));
    const load = vi.fn().mockResolvedValue(result);
    const builder = createQueryBuilder({
      cubeApi: cubeApi({ meta, dryRun, load }),
      defaultQuery: {
        measures: ['Orders.count'],
        dimensions: ['Orders.status'],
      },
      disableHeuristics: true,
    });
    let latest!: QueryBuilderState;
    const unsubscribe = builder.subscribe((value) => {
      latest = value;
    });

    await vi.waitFor(() => {
      expect(latest.isValidated).toBe(true);
      expect(latest.resultSet).toBe(result);
    });

    expect(meta).toHaveBeenCalledOnce();
    expect(dryRun).toHaveBeenCalledOnce();
    expect(load).toHaveBeenCalledOnce();
    expect(latest.measures[0]).toMatchObject({
      name: 'Orders.count',
      index: 0,
    });
    expect(latest.dimensions[0]).toMatchObject({
      name: 'Orders.status',
      index: 0,
    });
    expect(latest.availableMeasures.map((member) => member.name)).toEqual([
      'Orders.count',
      'Orders.total',
    ]);
    expect(latest.availableTimeDimensions[0].name).toBe('Orders.createdAt');
    expect(latest.missingMembers).toEqual([]);
    expect(latest.error).toBeNull();

    unsubscribe();
    await builder.destroy();
  });

  it('preserves validation and skips dry-run work for paging-only changes', async () => {
    const meta = vi.fn().mockResolvedValue(ordersMeta());
    const dryRun = vi.fn(async (query: Query) => dryRunResponse(query));
    const load = vi.fn().mockResolvedValue(resultSet('paging'));
    const builder = createQueryBuilder({
      cubeApi: cubeApi({ meta, dryRun, load }),
      defaultQuery: { measures: ['Orders.count'] },
      disableHeuristics: true,
    });
    const unsubscribe = builder.subscribe(() => undefined);

    await vi.waitFor(() => expect(get(builder).isValidated).toBe(true));
    const dryRunCount = dryRun.mock.calls.length;
    const loadCount = load.mock.calls.length;

    builder.actions.setLimit(25);
    builder.actions.setOffset(10);

    await vi.waitFor(() => {
      expect(get(builder).query).toMatchObject({ limit: 25, offset: 10 });
      expect(load).toHaveBeenCalledTimes(loadCount + 1);
    });
    expect(dryRun).toHaveBeenCalledTimes(dryRunCount);
    expect(get(builder).isValidated).toBe(true);
    expect(get(builder).validatedQuery).toMatchObject({
      limit: 25,
      offset: 10,
    });

    unsubscribe();
    await builder.destroy();
  });

  it('allows a manual refetch when automatic execution is disabled', async () => {
    const result = resultSet('manual-builder');
    const meta = vi.fn().mockResolvedValue(ordersMeta());
    const dryRun = vi.fn(async (query: Query) => dryRunResponse(query));
    const load = vi.fn().mockResolvedValue(result);
    const builder = createQueryBuilder({
      cubeApi: cubeApi({ meta, dryRun, load }),
      defaultQuery: { measures: ['Orders.count'] },
      disableHeuristics: true,
      executeQuery: false,
    });
    const unsubscribe = builder.subscribe(() => undefined);

    await vi.waitFor(() => expect(get(builder).isValidated).toBe(true));
    expect(load).not.toHaveBeenCalled();

    await builder.actions.refetch();

    expect(load).toHaveBeenCalledOnce();
    expect(get(builder).resultSet).toBe(result);

    unsubscribe();
    await builder.destroy();
  });

  it('updates and removes leaves without flattening nested filters', async () => {
    const meta = vi.fn().mockResolvedValue(ordersMeta());
    const dryRun = vi.fn(async (query: Query) => dryRunResponse(query));
    const builder = createQueryBuilder({
      cubeApi: cubeApi({ meta, dryRun }),
      defaultQuery: {
        measures: ['Orders.count'],
        filters: [
          {
            and: [
              {
                member: 'Orders.status',
                operator: 'equals',
                values: ['new'],
              },
              {
                or: [
                  {
                    member: 'Orders.status',
                    operator: 'equals',
                    values: ['pending'],
                  },
                  {
                    member: 'Orders.status',
                    operator: 'notSet',
                  },
                ],
              },
            ],
          },
        ],
      },
      disableHeuristics: true,
      executeQuery: false,
    });
    const unsubscribe = builder.subscribe(() => undefined);

    await vi.waitFor(() => expect(get(builder).filters).toHaveLength(3));
    const pending = get(builder).filters.find(
      ({ filter }) => 'values' in filter && filter.values?.[0] === 'pending'
    )!;

    builder.actions.updateFilters.update(pending, {
      member: ORDERS_STATUS,
      operator: 'equals',
      values: ['shipped'],
    });

    expect(get(builder).query.filters).toEqual([
      {
        and: [
          {
            member: 'Orders.status',
            operator: 'equals',
            values: ['new'],
          },
          {
            or: [
              {
                member: 'Orders.status',
                operator: 'equals',
                values: ['shipped'],
              },
              { member: 'Orders.status', operator: 'notSet' },
            ],
          },
        ],
      },
    ]);

    const first = get(builder).filters.find(
      ({ filter }) => 'values' in filter && filter.values?.[0] === 'new'
    )!;
    builder.actions.updateFilters.remove(first);

    expect(get(builder).query.filters).toEqual([
      {
        and: [
          {
            or: [
              {
                member: 'Orders.status',
                operator: 'equals',
                values: ['shipped'],
              },
              { member: 'Orders.status', operator: 'notSet' },
            ],
          },
        ],
      },
    ]);

    unsubscribe();
    await builder.destroy();
  });

  it('keeps order and pivot actions synchronized with canonical state', async () => {
    const meta = vi.fn().mockResolvedValue(ordersMeta());
    const dryRun = vi.fn(async (query: Query) => dryRunResponse(query));
    const builder = createQueryBuilder({
      cubeApi: cubeApi({ meta, dryRun }),
      defaultQuery: {
        measures: ['Orders.count'],
        dimensions: ['Orders.status'],
      },
      disableHeuristics: true,
      executeQuery: false,
    });
    const unsubscribe = builder.subscribe(() => undefined);

    await vi.waitFor(() => expect(get(builder).isValidated).toBe(true));
    expect(get(builder).orderMembers.map(({ id }) => id)).toEqual([
      'Orders.count',
      'Orders.status',
    ]);

    builder.actions.updateOrder.set('Orders.count', 'desc');
    expect(get(builder).query.order).toEqual([
      ['Orders.count', 'desc'],
    ]);

    builder.actions.updateOrder.reorder(1, 0);
    expect(get(builder).orderMembers.map(({ id }) => id)).toEqual([
      'Orders.status',
      'Orders.count',
    ]);
    expect(get(builder).query.order).toEqual([
      ['Orders.count', 'desc'],
    ]);

    builder.actions.updateOrder.set('Orders.count', 'none');
    expect(get(builder).query.order).toBeUndefined();

    builder.actions.updatePivotConfig.replace({
      x: ['measures'],
      y: ['Orders.status'],
      fillMissingDates: false,
    });
    expect(get(builder).pivotConfig).toMatchObject({
      x: ['measures'],
      y: ['Orders.status'],
      fillMissingDates: false,
    });

    unsubscribe();
    await builder.destroy();
  });

  it('aborts stale dry runs and reconciles only the newest query', async () => {
    const first = deferred<DryRunResponse>();
    const second = deferred<DryRunResponse>();
    const meta = vi.fn().mockResolvedValue(ordersMeta());
    const dryRun = vi
      .fn()
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise);
    const builder = createQueryBuilder({
      cubeApi: cubeApi({ meta, dryRun }),
      disableHeuristics: true,
      executeQuery: false,
    });
    const unsubscribe = builder.subscribe(() => undefined);

    await vi.waitFor(() => expect(get(builder).meta).not.toBeNull());
    builder.actions.updateMeasures.add(ORDERS_COUNT);
    await vi.waitFor(() => expect(dryRun).toHaveBeenCalledTimes(1));

    builder.actions.updateMeasures.update(
      get(builder).measures[0],
      ORDERS_TOTAL
    );
    await vi.waitFor(() => expect(dryRun).toHaveBeenCalledTimes(2));

    expect(
      (dryRun.mock.calls[0][1] as LoadMethodOptions).signal?.aborted
    ).toBe(true);

    const totalQuery = { measures: ['Orders.total'] };
    const newestResponse = dryRunResponse(totalQuery);
    second.resolve(newestResponse);
    await vi.waitFor(() =>
      expect(get(builder).validatedQuery).toEqual(totalQuery)
    );

    first.resolve(dryRunResponse({ measures: ['Orders.count'] }));
    await flushMicrotasks();
    expect(get(builder).query).toEqual(totalQuery);
    expect(get(builder).dryRunResponse).toBe(newestResponse);

    unsubscribe();
    await builder.destroy();
  });

  it('invalidates in-flight work when the Cube client changes', async () => {
    const oldDryRun = deferred<DryRunResponse>();
    const oldMeta = ordersMeta();
    const newMeta = ordersMeta();
    const firstMeta = vi.fn().mockResolvedValue(oldMeta);
    const firstDryRun = vi.fn().mockReturnValue(oldDryRun.promise);
    const secondMeta = vi.fn().mockResolvedValue(newMeta);
    const secondDryRun = vi.fn(async (query: Query) =>
      dryRunResponse(query)
    );
    const initialOptions: QueryBuilderOptions = {
      cubeApi: cubeApi({ meta: firstMeta, dryRun: firstDryRun }),
      defaultQuery: { measures: ['Orders.count'] },
      disableHeuristics: true,
      executeQuery: false,
    };
    const options = writable<QueryBuilderOptions>(initialOptions);
    const builder = createQueryBuilder(options);
    const unsubscribe = builder.subscribe(() => undefined);

    await vi.waitFor(() => expect(firstDryRun).toHaveBeenCalledOnce());

    options.set({
      ...initialOptions,
      cubeApi: cubeApi({ meta: secondMeta, dryRun: secondDryRun }),
    });

    await vi.waitFor(() => {
      expect(get(builder).meta).toBe(newMeta);
      expect(get(builder).isValidated).toBe(true);
    });
    expect(
      (firstDryRun.mock.calls[0][1] as LoadMethodOptions).signal?.aborted
    ).toBe(true);

    oldDryRun.resolve(
      dryRunResponse({ measures: ['Orders.count'], limit: 999 })
    );
    await flushMicrotasks();
    expect(get(builder).meta).toBe(newMeta);
    expect(get(builder).dryRunResponse).toEqual(
      dryRunResponse({ measures: ['Orders.count'] })
    );

    unsubscribe();
    await builder.destroy();
  });

  it('blocks validation and execution when metadata members are missing', async () => {
    const metadata: Meta = ordersMeta();
    const meta = vi.fn().mockResolvedValue(metadata);
    const dryRun = vi.fn();
    const load = vi.fn();
    const builder = createQueryBuilder({
      cubeApi: cubeApi({ meta, dryRun, load }),
      defaultQuery: { measures: ['Orders.unknown'] },
      disableHeuristics: true,
    });
    const unsubscribe = builder.subscribe(() => undefined);

    await vi.waitFor(() =>
      expect(get(builder).missingMembers).toEqual(['Orders.unknown'])
    );

    expect(get(builder).measures[0]).toMatchObject({
      name: 'Orders.unknown',
      index: 0,
    });
    expect(dryRun).not.toHaveBeenCalled();
    expect(load).not.toHaveBeenCalled();

    unsubscribe();
    await builder.destroy();
  });
});

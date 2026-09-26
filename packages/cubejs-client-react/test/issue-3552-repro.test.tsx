/**
 * Repro for https://github.com/cube-js/cube/issues/3552
 *
 * `QueryBuilder.updateVizState` only fetches a dry run (and only then refreshes
 * `validatedQuery` / `dryRunResponse`) when `measures`, `dimensions` or
 * `timeDimensions` change. Changing filters via `updateFilters` skips the dry
 * run, so `validatedQuery` keeps the stale (filter-less) query.
 */
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';
import { Meta } from '@cubejs-client/core';

import QueryBuilder from '../src/QueryBuilder';
import type { QueryBuilderRenderProps } from '../src/types';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

const meta = new Meta({
  cubes: [
    {
      name: 'Orders',
      title: 'Orders',
      type: 'cube',
      measures: [
        { name: 'Orders.count', title: 'Orders Count', shortTitle: 'Count', type: 'number', aggType: 'count', drillMembers: [], drillMembersGrouped: { measures: [], dimensions: [] } },
      ],
      dimensions: [
        { name: 'Orders.status', title: 'Orders Status', shortTitle: 'Status', type: 'string' },
      ],
      segments: [],
    },
  ],
} as never);

const flush = () => act(async () => {
  for (let i = 0; i < 10; i++) {
    await Promise.resolve();
  }
});

describe('issue #3552: QueryBuilder dry run on filter change', () => {
  let container: HTMLDivElement;
  let root: Root;
  let renderProps: QueryBuilderRenderProps;

  beforeEach(() => {
    container = document.createElement('div');
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('fetches a dry run and refreshes validatedQuery when filters change', async () => {
    const cubeApi = {
      meta: jest.fn().mockResolvedValue(meta),
      dryRun: jest.fn().mockImplementation(async (query) => ({
        queryType: 'regularQuery',
        normalizedQueries: [query],
        pivotQuery: { ...query, queryType: 'regularQuery' },
        queryOrder: [],
      })),
    };

    act(() => {
      root.render(
        <QueryBuilder
          cubeApi={cubeApi as never}
          defaultQuery={{}}
          disableHeuristics
          wrapWithQueryRenderer={false}
          render={(props) => {
            renderProps = props;
            return null;
          }}
        />
      );
    });
    await flush();

    await act(async () => {
      renderProps.updateMeasures.add({ name: 'Orders.count' } as never);
    });
    await flush();

    expect(cubeApi.dryRun).toHaveBeenCalledTimes(1);
    expect(renderProps.validatedQuery).toEqual({ measures: ['Orders.count'] });

    await act(async () => {
      renderProps.updateFilters.add({
        member: { name: 'Orders.status' },
        operator: 'equals',
        values: ['shipped'],
      } as never);
    });
    await flush();

    // Sanity: the builder's own query has the filter
    expect(renderProps.query.filters).toEqual([
      { member: 'Orders.status', operator: 'equals', values: ['shipped'] },
    ]);

    // Expected: a filter change triggers a dry run and refreshes validatedQuery
    expect(cubeApi.dryRun).toHaveBeenCalledTimes(2);
    expect(renderProps.validatedQuery).toEqual({
      measures: ['Orders.count'],
      filters: [{ member: 'Orders.status', operator: 'equals', values: ['shipped'] }],
    });
  });
});

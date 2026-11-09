// Regression test for https://github.com/cube-js/cube/issues/3552
// `shouldFetchDryRun` in QueryBuilder.updateVizState ignores `filters`, so changing
// filters via `updateFilters` neither triggers a dry run nor updates `validatedQuery`.
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';
import { Meta } from '@cubejs-client/core';
import type { Query } from '@cubejs-client/core';

import QueryBuilder from '../src/QueryBuilder';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

const metaResponse = {
  cubes: [
    {
      name: 'Orders',
      title: 'Orders',
      measures: [{ name: 'Orders.count', title: 'Orders Count', shortTitle: 'Count', type: 'number', aggType: 'count' }],
      dimensions: [{ name: 'Orders.status', title: 'Orders Status', shortTitle: 'Status', type: 'string' }],
      segments: [],
    },
  ],
};

const flush = async () => {
  for (let i = 0; i < 5; i++) {
    // eslint-disable-next-line no-await-in-loop
    await act(async () => {
      await new Promise((r) => setTimeout(r, 0));
    });
  }
};

describe('QueryBuilder dry run on filter change (#3552)', () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    container = document.createElement('div');
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('runs a dry run and updates validatedQuery when filters change', async () => {
    const dryRun = jest.fn(async (query: Query) => ({
      queryType: 'regularQuery',
      normalizedQueries: [query],
      pivotQuery: { ...query, queryType: 'regularQuery' },
      queryOrder: [],
    }));
    const cubeApi: any = {
      meta: jest.fn(async () => new (Meta as any)(metaResponse)),
      dryRun,
    };

    let renderProps: any;
    act(() => {
      root.render(
        <QueryBuilder
          cubeApi={cubeApi}
          wrapWithQueryRenderer={false}
          disableHeuristics
          defaultQuery={{}}
          render={(props) => {
            renderProps = props;
            return null;
          }}
        />
      );
    });
    await flush();

    // Changing measures triggers a dry run (baseline: works).
    act(() => {
      renderProps.updateMeasures.add({ name: 'Orders.count' });
    });
    await flush();
    const dryRunsBefore = dryRun.mock.calls.length;
    expect(dryRunsBefore).toBe(1);
    expect(renderProps.validatedQuery.measures).toEqual(['Orders.count']);
    expect(renderProps.validatedQuery.filters).toBeUndefined();

    act(() => {
      renderProps.updateFilters.add({ member: { name: 'Orders.status' }, operator: 'equals', values: ['shipped'] });
    });
    await flush();

    expect(renderProps.query.filters).toEqual([
      { member: 'Orders.status', operator: 'equals', values: ['shipped'] },
    ]);
    // ... and validatedQuery (consumed by QueryRenderer) reflects the new filter.
    expect(renderProps.validatedQuery.filters).toEqual([
      { member: 'Orders.status', operator: 'equals', values: ['shipped'] },
    ]);
    // ... and is backed by a new dry run of the filtered query.
    expect(dryRun.mock.calls.length).toBe(dryRunsBefore + 1);
  });
});

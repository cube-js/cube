import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';

import { Meta } from '@cubejs-client/core';

import QueryBuilder from '../src/QueryBuilder';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

// https://github.com/cube-js/cube/issues/3552
// Guards against QueryBuilder re-running dryRun only on measures/dimensions/timeDimensions
// changes, which left validatedQuery / dryRunResponse stale after updateFilters.
describe('issue #3552: QueryBuilder dry run on filter change', () => {
  let root: Root;

  beforeEach(() => {
    root = createRoot(document.createElement('div'));
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('runs a dry run and refreshes validatedQuery when filters change', async () => {
    const meta = new Meta({
      cubes: [{
        name: 'Orders',
        title: 'Orders',
        measures: [
          { name: 'Orders.count', title: 'Count', shortTitle: 'Count', type: 'number', aggType: 'count' },
          { name: 'Orders.amount', title: 'Amount', shortTitle: 'Amount', type: 'number', aggType: 'sum' },
        ],
        dimensions: [
          { name: 'Orders.status', title: 'Status', shortTitle: 'Status', type: 'string' },
        ],
        segments: [],
      }],
    } as any);
    const cubeApi = {
      meta: jest.fn().mockResolvedValue(meta),
      dryRun: jest.fn().mockImplementation(async (query: any) => ({
        queryType: 'regularQuery',
        normalizedQueries: [query],
        pivotQuery: query,
        queryOrder: [],
      })),
      load: jest.fn(() => new Promise(() => undefined)),
    };

    let props: any;
    await act(async () => {
      root.render(
        <QueryBuilder
          cubeApi={cubeApi as never}
          wrapWithQueryRenderer={false}
          disableHeuristics
          defaultQuery={{ measures: ['Orders.count'] }}
          render={(p) => {
            props = p;
            return null;
          }}
        />
      );
    });

    // Add a measure first so a dry run has happened
    await act(async () => {
      props.updateMeasures.add({ name: 'Orders.amount' });
    });
    const dryRunsBefore = cubeApi.dryRun.mock.calls.length;
    expect(dryRunsBefore).toBeGreaterThan(0);

    const filter = { member: 'Orders.status', operator: 'equals', values: ['completed'] };
    await act(async () => {
      props.updateFilters.add({ member: { name: 'Orders.status' }, operator: 'equals', values: ['completed'] });
    });

    expect(props.query.filters).toEqual([filter]);
    expect(cubeApi.dryRun.mock.calls.length).toBe(dryRunsBefore + 1);
    expect(props.validatedQuery.filters).toEqual([filter]);
  });
});

import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';
import { Meta } from '@cubejs-client/core';
import type { Query } from '@cubejs-client/core';

import { QueryBuilder } from '../src';
import type { QueryBuilderRenderProps } from '../src';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

const metaResponse = {
  cubes: [
    {
      name: 'Orders',
      title: 'Orders',
      measures: [{ name: 'Orders.count', title: 'Orders Count', shortTitle: 'Count', type: 'number', aggType: 'count' }],
      dimensions: [{ name: 'Orders.status', title: 'Orders Status', shortTitle: 'Status', type: 'string' }],
      segments: [{ name: 'Orders.completed', title: 'Orders Completed', shortTitle: 'Completed' }],
    },
  ],
};

async function flush() {
  for (let i = 0; i < 5; i++) {
    // eslint-disable-next-line no-await-in-loop
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
  }
}

describe('QueryBuilder', () => {
  let container: HTMLDivElement;
  let root: Root;
  let renderProps: QueryBuilderRenderProps;
  let dryRun: jest.Mock;

  beforeEach(async () => {
    container = document.createElement('div');
    root = createRoot(container);

    dryRun = jest.fn(async (query: Query) => ({
      queryType: 'regularQuery',
      normalizedQueries: [query],
      pivotQuery: { ...query, queryType: 'regularQuery' },
      queryOrder: [],
    }));
    const cubeApi = {
      meta: jest.fn(async () => new Meta(metaResponse as never)),
      dryRun,
    };

    act(() => {
      root.render(
        <QueryBuilder
          cubeApi={cubeApi as never}
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

    act(() => {
      renderProps.updateMeasures.add({ name: 'Orders.count' } as never);
    });
    await flush();
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('runs a dry run and updates validatedQuery when filters change', async () => {
    expect(dryRun).toHaveBeenCalledTimes(1);
    expect(renderProps.validatedQuery.filters).toBeUndefined();

    act(() => {
      renderProps.updateFilters.add({
        member: { name: 'Orders.status' },
        operator: 'equals',
        values: ['shipped'],
      } as never);
    });
    await flush();

    const filters = [{ member: 'Orders.status', operator: 'equals', values: ['shipped'] }];
    expect(dryRun).toHaveBeenCalledTimes(2);
    expect(dryRun.mock.calls[1][0].filters).toEqual(filters);
    expect(renderProps.validatedQuery.filters).toEqual(filters);
    expect(renderProps.dryRunResponse?.normalizedQueries[0].filters).toEqual(filters);
  });

  it('runs a dry run and updates validatedQuery when segments change', async () => {
    expect(dryRun).toHaveBeenCalledTimes(1);

    act(() => {
      renderProps.updateSegments.add({ name: 'Orders.completed' } as never);
    });
    await flush();

    expect(dryRun).toHaveBeenCalledTimes(2);
    expect(renderProps.validatedQuery.segments).toEqual(['Orders.completed']);
  });

  it('does not run a dry run when the query members are unchanged', async () => {
    act(() => {
      renderProps.updateQuery({});
    });
    await flush();

    expect(dryRun).toHaveBeenCalledTimes(1);
  });
});

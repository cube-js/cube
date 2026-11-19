import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';
import cube, { Meta } from '@cubejs-client/core';
import type { TCubeDimension } from '@cubejs-client/core';

import { QueryBuilder } from '../src';
import type { QueryBuilderRenderProps } from '../src';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

// https://github.com/cube-js/cube/issues/7383
describe('QueryBuilder updateFilters', () => {
  let root: Root;

  beforeEach(() => {
    root = createRoot(document.createElement('div'));
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('adds a filter by name and updates it with a resolved dimension', async () => {
    const city: TCubeDimension = {
      name: 'Orders.city',
      title: 'City',
      shortTitle: 'City',
      type: 'string',
      suggestFilterValues: true,
    };
    const meta = new Meta({
      cubes: [{
        name: 'Orders',
        title: 'Orders',
        measures: [],
        dimensions: [{
          name: 'Orders.status',
          title: 'Status',
          shortTitle: 'Status',
          type: 'string',
          suggestFilterValues: true,
        }, city],
        segments: [],
        folders: [],
        nestedFolders: [],
        hierarchies: [],
      }],
    });
    const cubeApi = Object.assign(cube('token', { apiUrl: 'http://localhost/cubejs-api/v1' }), {
      meta: async () => meta,
    });

    let renderProps!: QueryBuilderRenderProps;
    await act(async () => {
      root.render(
        <QueryBuilder
          cubeApi={cubeApi}
          wrapWithQueryRenderer={false}
          disableHeuristics
          render={(props) => {
            renderProps = props;
            return null;
          }}
        />
      );
    });

    const filter = { member: 'Orders.status', operator: 'equals' as const, values: ['completed'] };
    await act(async () => {
      renderProps.updateFilters.add(filter);
    });

    expect(renderProps.query.filters).toEqual([filter]);

    const [resolvedFilter] = renderProps.filters;
    await act(async () => {
      renderProps.updateFilters.update(resolvedFilter, { ...filter, ...resolvedFilter, dimension: city });
    });

    expect(renderProps.query.filters).toEqual([{ ...filter, member: city.name }]);
  });
});

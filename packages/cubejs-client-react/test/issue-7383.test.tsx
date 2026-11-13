import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';

import { Meta } from '@cubejs-client/core';

import QueryBuilder from '../src/QueryBuilder';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

// https://github.com/cube-js/cube/issues/7383
// FilterUpdateFields declares `member?: string`, but toFilter() reads `member.member?.name`,
// so a filter added with a member name (no resolved member object) ends up with
// `member: undefined`, and client-core later throws "Cannot read properties of undefined
// (reading 'split')".
describe('issue #7383: QueryBuilder updateFilters.add with a member name', () => {
  let root: Root;

  beforeEach(() => {
    root = createRoot(document.createElement('div'));
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('adds a filter whose member is given as a string, as FilterUpdateFields declares', async () => {
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

    const filter = { member: 'Orders.status', operator: 'equals', values: ['completed'] };
    await act(async () => {
      // FilterUpdateFields types `member` as a string; the reporter passed exactly this.
      props.updateFilters.add({ member: 'Orders.status', operator: 'equals', values: ['completed'] });
    });

    expect(props.query.filters).toEqual([filter]);
  });
});

import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import type { Root } from 'react-dom/client';

import { Meta } from '@cubejs-client/core';

import QueryBuilder from '../src/QueryBuilder';

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

// https://github.com/cube-js/cube/issues/7383
// FilterUpdateFields declares `member` as a string, but updateFilters.add only reads
// `member.name` / `dimension.name`, so a filter added by member name ends up with
// `member: undefined`.
describe('issue #7383: QueryBuilder updateFilters.add with a member name', () => {
  let root: Root;

  beforeEach(() => {
    root = createRoot(document.createElement('div'));
  });

  afterEach(() => {
    act(() => root.unmount());
  });

  it('adds a filter for the member passed by name', async () => {
    const meta = new Meta({
      cubes: [{
        name: 'LearningCampaigns',
        title: 'Learning Campaigns',
        measures: [
          { name: 'LearningCampaigns.count', title: 'Count', shortTitle: 'Count', type: 'number', aggType: 'count' },
        ],
        dimensions: [
          { name: 'LearningCampaigns.id', title: 'Id', shortTitle: 'Id', type: 'number' },
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
          defaultQuery={{ measures: ['LearningCampaigns.count'] }}
          render={(p) => {
            props = p;
            return null;
          }}
        />
      );
    });

    await act(async () => {
      props.updateFilters.add({ member: 'LearningCampaigns.id', operator: 'equals', values: ['573'] });
    });

    expect(props.query.filters).toEqual([
      { member: 'LearningCampaigns.id', operator: 'equals', values: ['573'] },
    ]);
  });
});

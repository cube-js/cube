import { shallowMount, flushPromises } from '@vue/test-utils';

import fetchMock, { meta, load } from './__mocks__/responses';
import QueryBuilder from '../../src/QueryBuilder';
import { createCubeApi } from './utils';

// https://github.com/cube-js/cube/issues/7379
// Guards against heuristics dropping an explicit timeDimensions granularity from a
// query that the parent sets or changes through the query prop after mount.
const issueQuery = {
  measures: ['Orders.count'],
  dimensions: ['Orders.status'],
  timeDimensions: [{ dimension: 'Orders.createdAt', granularity: 'day' }],
  filters: [
    { member: 'Orders.createdAt', operator: 'inDateRange', values: ['2021-10-01', '2023-10-07'] },
    { member: 'Orders.status', operator: 'equals', values: ['completed'] },
  ],
};

function setup(query) {
  const cube = createCubeApi();
  jest
    .spyOn(cube, 'request')
    .mockImplementation(fetchMock(load))
    .mockImplementationOnce(fetchMock(meta));
  const dryRun = jest
    .spyOn(cube, 'dryRun')
    .mockImplementation(async (q) => ({ queryType: 'regularQuery', normalizedQueries: [q], pivotQuery: q, queryOrder: [] }));

  const wrapper = shallowMount(QueryBuilder, { props: { cubeApi: cube, query } });
  return { wrapper, dryRun };
}

describe('issue #7379: vue3 QueryBuilder keeps timeDimensions granularity', () => {
  it('keeps granularity for a query passed at mount', async () => {
    const { wrapper } = setup(issueQuery);
    await flushPromises();

    expect(wrapper.vm.validatedQuery.timeDimensions).toEqual([
      { dimension: 'Orders.createdAt', granularity: 'day' },
    ]);
  });

  it('keeps granularity when the query prop is set after mount', async () => {
    const { wrapper, dryRun } = setup({});
    await flushPromises();

    await wrapper.setProps({ query: issueQuery });
    await flushPromises();

    expect(wrapper.vm.validatedQuery.timeDimensions).toEqual([
      { dimension: 'Orders.createdAt', granularity: 'day' },
    ]);
    const lastDryRunQuery = dryRun.mock.calls[dryRun.mock.calls.length - 1][0];
    expect(lastDryRunQuery.timeDimensions).toEqual([
      { dimension: 'Orders.createdAt', granularity: 'day' },
    ]);
  });

  it('keeps granularity when the query prop changes from a query without dimensions', async () => {
    const { wrapper, dryRun } = setup({ measures: ['Orders.count'] });
    await flushPromises();

    await wrapper.setProps({ query: issueQuery });
    await flushPromises();

    expect(wrapper.vm.validatedQuery.timeDimensions).toEqual([
      { dimension: 'Orders.createdAt', granularity: 'day' },
    ]);
    const lastDryRunQuery = dryRun.mock.calls[dryRun.mock.calls.length - 1][0];
    expect(lastDryRunQuery.timeDimensions).toEqual([
      { dimension: 'Orders.createdAt', granularity: 'day' },
    ]);
  });

  it('still applies heuristics to user edits after the query prop is cleared', async () => {
    const { wrapper } = setup(issueQuery);
    await flushPromises();

    await wrapper.setProps({ query: {} });
    await flushPromises();

    wrapper.vm.addMember('measures', 'Orders.count');
    await flushPromises();

    // the "first measure added" heuristic adds the measure's default time dimension
    expect(wrapper.vm.validatedQuery.timeDimensions).toEqual([
      { dimension: 'Orders.createdAt', granularity: 'day' },
    ]);
  });
});

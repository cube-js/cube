import { shallowMount, flushPromises } from '@vue/test-utils';

import fetchMock, { meta, load } from './__mocks__/responses';
import QueryBuilder from '../../src/QueryBuilder';
import { createCubeApi } from './utils';

// https://github.com/cube-js/cube/issues/10027
// A `timezone` passed through the query prop must be kept in the query the
// QueryBuilder builds (and therefore sends to Cube), otherwise time dimensions
// are silently bucketed in UTC.
const issueQuery = {
  measures: ['Orders.count'],
  timeDimensions: [{ dimension: 'Orders.createdAt', granularity: 'day', dateRange: ['2024-01-01', '2024-01-02'] }],
  timezone: 'America/Los_Angeles',
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

describe('issue #10027: vue3 QueryBuilder keeps timezone', () => {
  it('keeps timezone for a query passed at mount', async () => {
    const { wrapper } = setup(issueQuery);
    await flushPromises();

    expect(wrapper.vm.validatedQuery.timezone).toBe('America/Los_Angeles');
  });

  it('keeps timezone when the query prop is set after mount', async () => {
    const { wrapper, dryRun } = setup({});
    await flushPromises();

    await wrapper.setProps({ query: issueQuery });
    await flushPromises();

    expect(wrapper.vm.validatedQuery.timezone).toBe('America/Los_Angeles');
    const lastDryRunQuery = dryRun.mock.calls[dryRun.mock.calls.length - 1][0];
    expect(lastDryRunQuery.timezone).toBe('America/Los_Angeles');
  });
});

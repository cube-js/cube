// https://github.com/cube-js/cube/issues/7379
import { shallowMount, flushPromises } from '@vue/test-utils';

import fetchMock, { meta, load } from './__mocks__/responses';
import QueryBuilder from '../../src/QueryBuilder';
import { createCubeApi } from './utils';

const issueQuery = {
  measures: ['Orders.count'],
  order: [['Orders.createdAt', 'asc']],
  dimensions: ['Orders.status'],
  timeDimensions: [{ dimension: 'Orders.createdAt', granularity: 'day' }],
  filters: [
    { member: 'Orders.createdAt', operator: 'inDateRange', values: ['2021-10-01', '2023-10-07'] },
    { member: 'Orders.status', operator: 'equals', values: ['completed'] },
  ],
};

function sentQueries(spy) {
  return spy.mock.calls
    .map(([method, params]) => ({ method, query: params && params.query }))
    .filter(({ method, query }) => (method === 'load' || method === 'dry-run') && query)
    .map(({ method, query }) => ({ method, query: typeof query === 'string' ? JSON.parse(query) : query }));
}

describe('QueryBuilder granularity (issue #7379)', () => {
  it('keeps granularity when the query is passed on mount', async () => {
    const cube = createCubeApi();
    const spy = jest
      .spyOn(cube, 'request')
      .mockImplementation(fetchMock(load))
      .mockImplementationOnce(fetchMock(meta));

    const wrapper = shallowMount(QueryBuilder, { props: { cubeApi: cube, query: issueQuery } });
    await flushPromises();

    expect(wrapper.vm.validatedQuery.timeDimensions).toEqual([
      expect.objectContaining({ dimension: 'Orders.createdAt', granularity: 'day' }),
    ]);
    sentQueries(spy).forEach(({ query }) => {
      expect(query.timeDimensions[0].granularity).toBe('day');
    });
  });

  it('keeps granularity when the query prop is set after mount', async () => {
    const cube = createCubeApi();
    const spy = jest
      .spyOn(cube, 'request')
      .mockImplementation(fetchMock(load))
      .mockImplementationOnce(fetchMock(meta));

    const wrapper = shallowMount(QueryBuilder, { props: { cubeApi: cube, query: { measures: ['Orders.count'] } } });
    await flushPromises();

    await wrapper.setProps({ query: issueQuery });
    await flushPromises();

    expect(wrapper.vm.validatedQuery.timeDimensions).toEqual([
      expect.objectContaining({ dimension: 'Orders.createdAt', granularity: 'day' }),
    ]);
    const issueRequests = sentQueries(spy).filter(({ query }) => (query.dimensions || []).length > 0);
    expect(issueRequests.length).toBeGreaterThan(0);
    issueRequests.forEach(({ query }) => {
      expect(query.timeDimensions[0].granularity).toBe('day');
    });
  });
});

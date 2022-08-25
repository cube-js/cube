import { shallowMount } from '@vue/test-utils';
import flushPromises from 'flush-promises';
// import { h } from 'vue';

import fetchMock, { meta, load } from './__mocks__/responses';
import QueryBuilder from '../../src/QueryBuilder';
import { createCubejsApi } from './utils';

describe('QueryBuilder.vue', () => {
  it('renders meta information', async () => {
    const cube = createCubejsApi();
    jest
      .spyOn(cube, 'request')
      .mockImplementation(fetchMock(meta))
      .mockImplementationOnce(fetchMock(meta));

    // const wrapper = shallowMount(QueryBuilder, {
    //   props: {
    //     cubejsApi: cube,
    //     query: {},
    //   },
    //   slots: {
    //     empty: h(`div`, {}, `i'm empty`),
    //   },
    // });

    await flushPromises();

    expect(true);
  });

  // it('renders meta information', async () => {
  //   const cube = createCubejsApi();
  //   jest
  //     .spyOn(cube, 'request')
  //     .mockImplementation(fetchMock(load))
  //     .mockImplementationOnce(fetchMock(meta));

  //   let context;

  //   shallowMount(QueryBuilder, {
  //     props: {
  //       cubejsApi: cube,
  //       query: {
  //         measures: ['Orders.count'],
  //       },
  //     },
  //     slots: {
  //       builder: (con) => {
  //         context = con;
  //       },
  //     },
  //   });

  //   await flushPromises();

  //   expect(context.query.measures[0].name).toBe('Orders.count');
  // });

  describe('Update background query members', () => {
    it('adds members', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {},
        },
      });

      await flushPromises();

      expect(wrapper.vm.measures.length).toBe(0);
      wrapper.vm.addMember('measures', 'Orders.count');
      expect(wrapper.vm.measures.length).toBe(1);
      expect(wrapper.vm.measures[0].name).toBe('Orders.count');
    });

    it('updates members', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            measures: ['Orders.count'],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.measures.length).toBe(1);
      expect(wrapper.vm.measures[0].name).toBe('Orders.count');
      wrapper.vm.updateMember('measures', 'Orders.count', 'LineItems.count');
      expect(wrapper.vm.measures.length).toBe(1);
      expect(wrapper.vm.measures[0].name).toBe('LineItems.count');
    });

    it('removes members', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            measures: ['Orders.count'],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.measures.length).toBe(1);
      expect(wrapper.vm.measures[0].name).toBe('Orders.count');
      wrapper.vm.removeMember('measures', 'Orders.count');
      expect(wrapper.vm.measures.length).toBe(0);
    });

    it('sets members', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            measures: ['Orders.count'],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.measures.length).toBe(1);
      expect(wrapper.vm.measures[0].name).toBe('Orders.count');
      wrapper.vm.setMembers('measures', ['LineItems.count']);
      expect(wrapper.vm.measures.length).toBe(1);
      expect(wrapper.vm.measures[0].name).toBe('LineItems.count');
    });
  });

  describe('changes background query timeDimensions', () => {
    it('adds timeeDimensions', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {},
        },
      });

      await flushPromises();

      expect(wrapper.vm.measures.length).toBe(0);
      wrapper.vm.addMember('timeDimensions', {
        dimension: 'Orders.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month',
      });
      expect(wrapper.vm.timeDimensions.length).toBe(1);
      expect(wrapper.vm.timeDimensions[0].name).toBe('Orders.createdAt');
      expect(wrapper.vm.timeDimensions[0].granularity).toBe('month');
    });

    it('updates timeDimensions', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const dimension = {
        dimension: 'Orders.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month',
      };

      const newDimension = {
        dimension: 'LineItems.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'day',
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            timeDimensions: [dimension],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.timeDimensions.length).toBe(1);
      expect(wrapper.vm.timeDimensions[0].dimension.name).toBe('Orders.createdAt');
      expect(wrapper.vm.timeDimensions[0].granularity).toBe('month');
      wrapper.vm.updateMember('timeDimensions', dimension, newDimension);
      expect(wrapper.vm.timeDimensions.length).toBe(1);
      expect(wrapper.vm.timeDimensions[0].dimension.name).toBe('LineItems.createdAt');
      expect(wrapper.vm.timeDimensions[0].granularity).toBe('day');
    });

    it('removes timeDimensions', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const dimension = {
        dimension: 'Orders.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month',
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            timeDimensions: [dimension],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.timeDimensions.length).toBe(1);
      expect(wrapper.vm.timeDimensions[0].dimension.name).toBe('Orders.createdAt');
      wrapper.vm.removeMember('timeDimensions', 'Orders.createdAt');
      expect(wrapper.vm.timeDimensions.length).toBe(0);
    });

    it('sets timeDimensions', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const dimension = {
        dimension: 'Orders.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month',
      };

      const newDimension = {
        dimension: 'LineItems.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'day',
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            timeDimensions: [dimension],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.timeDimensions.length).toBe(1);
      expect(wrapper.vm.timeDimensions[0].dimension.name).toBe('Orders.createdAt');
      expect(wrapper.vm.timeDimensions[0].granularity).toBe('month');
      wrapper.vm.setMembers('timeDimensions', [newDimension]);
      expect(wrapper.vm.timeDimensions.length).toBe(1);
      expect(wrapper.vm.timeDimensions[0].dimension.name).toBe('LineItems.createdAt');
      expect(wrapper.vm.timeDimensions[0].granularity).toBe('day');
    });
  });

  describe('update background query on filters', () => {
    it('adds filters', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {},
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters.length).toBe(0);
      wrapper.vm.addMember('filters', {
        dimension: 'Orders.status',
        operator: 'equals',
        values: ['valid'],
      });
      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].member.name).toBe('Orders.status');
    });

    it('updates filters', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        dimension: 'Orders.status',
        operator: 'equals',
        values: ['invalid'],
      };

      const newFilter = {
        dimension: 'Orders.status',
        operator: 'equals',
        values: ['valid'],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].member.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('invalid');
      wrapper.vm.updateMember('filters', 'Orders.status', newFilter);
      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].member.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('valid');
    });

    it('removes filters', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        dimension: 'Orders.status',
        operator: 'equals',
        values: ['invalid'],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].member.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('invalid');
      wrapper.vm.removeMember('filters', 'Orders.status');
      expect(wrapper.vm.filters.length).toBe(0);
    });

    it('sets filters', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        dimension: 'Orders.status',
        operator: 'equals',
        values: ['invalid'],
      };

      const newFilter = {
        dimension: 'Orders.status',
        operator: 'equals',
        values: ['valid'],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].member.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('invalid');
      wrapper.vm.setMembers('filters', [newFilter]);
      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].member.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('valid');
    });

    it('sets filters with boolean logical operators', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        and: [
          {
            dimension: 'Orders.status',
            operator: 'equals',
            values: ['this'],
          },
          {
            dimension: 'Orders.status',
            operator: 'equals',
            values: ['that'],
          },
        ],
        or: [
          {
            dimension: 'Orders.status',
            operator: 'equals',
            values: ['this'],
          },
          {
            dimension: 'Orders.status',
            operator: 'equals',
            values: ['that'],
          },
          {
            and: [
              {
                dimension: 'Orders.status',
                operator: 'equals',
                values: ['this'],
              },
              {
                dimension: 'Orders.status',
                operator: 'equals',
                values: ['that'],
              },
            ],
          },
        ],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();
      expect(wrapper.vm.filters[0].or.length).toBe(3);
      expect(wrapper.vm.filters[0].and.length).toBe(2);
      wrapper.vm.setMembers('filters', []);
      expect(wrapper.vm.validatedQuery.filters).toBeUndefined();
      wrapper.vm.setMembers('filters', [filter]);
      expect(wrapper.vm.validatedQuery.filters.length).toBe(1);
      expect(wrapper.vm.validatedQuery.filters[0].and[0].member).toBe('Orders.status');
      expect(wrapper.vm.validatedQuery.filters[0].and[0].values).toContain('this');
      expect(wrapper.vm.validatedQuery.filters[0].and[1].values).toContain('that');
      expect(wrapper.vm.validatedQuery.filters[0].or[0].member).toBe('Orders.status');
      expect(wrapper.vm.validatedQuery.filters[0].or[0].values).toContain('this');
      expect(wrapper.vm.validatedQuery.filters[0].or[1].values).toContain('that');
      expect(wrapper.vm.validatedQuery.filters[0].or[2].and[0].member).toBe('Orders.status');
      expect(wrapper.vm.validatedQuery.filters[0].or[2].and[0].values).toContain('this');
      expect(wrapper.vm.validatedQuery.filters[0].or[2].and[1].values).toContain('that');
    });

    it('filters with boolean logical operators without explicit set', async () => {
      const cube = createCubejsApi();
      jest
          .spyOn(cube, 'request')
          .mockImplementation(fetchMock(load))
          .mockImplementationOnce(fetchMock(meta));

      const filter = {
        or: [
          {
            dimension: 'Orders.status',
            operator: 'equals',
            values: ['this'],
          },
          {
            dimension: 'Orders.status',
            operator: 'equals',
            values: ['that'],
          },
          {
            and: [
              {
                dimension: 'Orders.status',
                operator: 'equals',
                values: ['this'],
              },
              {
                dimension: 'Orders.status',
                operator: 'equals',
                values: ['that'],
              },
            ],
          },
        ],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters[0].or.length).toBe(3);
      expect(wrapper.vm.validatedQuery.filters.length).toBe(1);
      expect(wrapper.vm.validatedQuery.filters[0].or[0].member).toBe('Orders.status');
      expect(wrapper.vm.validatedQuery.filters[0].or[0].values).toContain('this');
      expect(wrapper.vm.validatedQuery.filters[0].or[1].values).toContain('that');
      expect(wrapper.vm.validatedQuery.filters[0].or[2].and[0].member).toBe('Orders.status');
      expect(wrapper.vm.validatedQuery.filters[0].or[2].and[0].values).toContain('this');
      expect(wrapper.vm.validatedQuery.filters[0].or[2].and[1].values).toContain('that');
    });

    it.each([
      [
        {
          and: [
            {
              dimension: 'Orders.status',
              values: ['this'],
            },
          ],
        },
        0,
      ],
      [
        {
          or: [
            {
              dimension: 'Orders.status',
              values: ['this'],
            },
          ],
        },
        0,
      ],
      [
        {
          or: [
            {
              dimension: 'Orders.status',
              values: ['this'],
            },
            {
              and: [
                {
                  dimension: 'Orders.status',
                  values: ['this'],
                },
              ],
            },
          ],
        },
        0,
      ],
      [
        {
          and: [
            {
              dimension: 'Orders.status',
              values: ['this'],
            },
          ],
          or: [
            {
              dimension: 'Orders.status',
              operator: 'equals',
              values: ['this'],
            },
          ],
        },
        1,
      ],
      [
        {
          or: [
            {
              dimension: 'Orders.status',
              values: ['this'],
            },
            {
              and: [
                {
                  dimension: 'Orders.status',
                  operator: 'equals',
                  values: ['this'],
                },
              ],
            },
          ],
        },
        1,
      ],
    ])('does not assign boolean logical operators having no operator', async (filter, expected) => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [],
          },
        },
      });

      await flushPromises();

      wrapper.vm.setMembers('filters', [filter]);
      expect(wrapper.vm.validatedQuery.filters.length).toBe(expected);
    });

    it('sets filters when using measure', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        member: 'Orders.number',
        operator: 'gt',
        values: ['1'],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].member.name).toBe('Orders.number');
      expect(wrapper.vm.filters[0].values).toContain('1');
    });

    it('sets limit', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        member: 'Orders.status',
        operator: 'equals',
        values: ['invalid'],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
            limit: 10,
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.limit).toBe(10);
    });

    it('sets offset', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        member: 'Orders.status',
        operator: 'equals',
        values: ['invalid'],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
            offset: 10,
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.offset).toBe(10);
    });

    it('sets renewQuery', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        member: 'Orders.status',
        operator: 'equals',
        values: ['invalid'],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
            renewQuery: true,
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.renewQuery).toBe(true);
    });

    it('ignore order if empty', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        member: 'Orders.status',
        operator: 'equals',
        values: ['invalid'],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.order).toBe(null);
    });

    it('sets order', async () => {
      const cube = createCubejsApi();
      jest
        .spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        member: 'Orders.status',
        operator: 'equals',
        values: ['invalid'],
      };

      const wrapper = shallowMount(QueryBuilder, {
        props: {
          cubejsApi: cube,
          query: {
            dimensions: ['Orders.status'],
            filters: [filter],
            order: {
              'Orders.status': 'desc',
            },
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.order['Orders.status']).toBe('desc');
    });

    // todo: fix later
    // it('is reactive when filter is changed', async () => {
    //   const cube = createCubejsApi();
    //   jest
    //     .spyOn(cube, 'request')
    //     .mockImplementation(fetchMock(load))
    //     .mockImplementationOnce(fetchMock(meta));
    //
    //   const filter = {
    //     member: 'Orders.status',
    //     operator: 'equals',
    //     values: ['invalid'],
    //   };
    //
    //   const newFilter = {
    //     dimension: 'Orders.number',
    //     operator: 'equals',
    //     values: ['1'],
    //   };
    //
    //   const wrapper = mount(QueryBuilder, {
    //     props: {
    //       cubejsApi: cube,
    //       query: {
    //         filters: [filter],
    //       },
    //     },
    //   });
    //
    //   await flushPromises();
    //
    //   expect(wrapper.vm.filters.length).toBe(1);
    //   expect(wrapper.vm.filters[0].member.name).toBe('Orders.status');
    //   expect(wrapper.vm.filters[0].values).toContain('invalid');
    //
    //   wrapper.setProps({
    //     query: {
    //       filters: [newFilter],
    //     },
    //   });
    //
    //   await flushPromises();
    //
    //   expect(wrapper.vm.filters.length).toBe(1);
    //   expect(wrapper.vm.filters[0].member.name).toBe('Orders.number');
    //   expect(wrapper.vm.filters[0].values).toContain('1');
    // });
  });
});

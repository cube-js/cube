import { mount } from '@vue/test-utils';
import CubejsApi from '@cubejs-client/core';
import flushPromises from 'flush-promises';
import fetchMock, { meta, load } from './__mocks__/responses';
import QueryBuilder from '../../src/QueryBuilder';

describe('QueryBuilder.vue', () => {
  it('renders meta information', async () => {
    const cube = CubejsApi('token');
    jest.spyOn(cube, 'request')
      .mockImplementation(fetchMock(meta))
      .mockImplementationOnce(fetchMock(meta));

    const wrapper = mount(QueryBuilder, {
      propsData: {
        cubejsApi: cube,
        query: {},
      },
      slots: {
        empty: `<div>i'm empty</div>`,
      },
    });

    await flushPromises();

    expect(wrapper.text()).toContain(`i'm empty`);
  });

  it('renders meta information', async () => {
    const cube = CubejsApi('token');
    jest.spyOn(cube, 'request')
      .mockImplementation(fetchMock(load))
      .mockImplementationOnce(fetchMock(meta));

    let context;

    mount(QueryBuilder, {
      propsData: {
        cubejsApi: cube,
        query: {
          measures: ['Orders.count'],
        },
      },
      scopedSlots: {
        builder: (con) => {
          context = con;
        },
      }
    });

    await flushPromises();
    expect(context.measures[0].name).toBe('Orders.count');
  });

  describe('Update background query members', () => {
    it('adds members', async () => {
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = mount(QueryBuilder, {
        propsData: {
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
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = mount(QueryBuilder, {
        propsData: {
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
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = mount(QueryBuilder, {
        propsData: {
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
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = mount(QueryBuilder, {
        propsData: {
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
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = mount(QueryBuilder, {
        propsData: {
          cubejsApi: cube,
          query: {},
        },
      });

      await flushPromises();

      expect(wrapper.vm.measures.length).toBe(0);
      wrapper.vm.addMember('timeDimensions',  {
        dimension: 'Orders.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month'
      });
      expect(wrapper.vm.timeDimensions.length).toBe(1);
      expect(wrapper.vm.timeDimensions[0].name).toBe('Orders.createdAt');
      expect(wrapper.vm.timeDimensions[0].granularity).toBe('month');
    });

    it('updates timeDimensions', async () => {
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const dimension = {
        dimension: 'Orders.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month'
      };

      const newDimension = {
        dimension: 'LineItems.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'day'
      };

      const wrapper = mount(QueryBuilder, {
        propsData: {
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
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const dimension = {
        dimension: 'Orders.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month'
      };

      const wrapper = mount(QueryBuilder, {
        propsData: {
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
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const dimension = {
        dimension: 'Orders.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'month'
      };

      const newDimension = {
        dimension: 'LineItems.createdAt',
        dateRange: ['2015-01-01', '2015-12-31'],
        granularity: 'day'
      };

      const wrapper = mount(QueryBuilder, {
        propsData: {
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
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const wrapper = mount(QueryBuilder, {
        propsData: {
          cubejsApi: cube,
          query: {},
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters.length).toBe(0);
      wrapper.vm.addMember('filters', {
        dimension: 'Orders.status',
        operator: 'equals',
        values: ['valid']
      });
      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].dimension.name).toBe('Orders.status');
    });

    it('updates filters', async () => {
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
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

      const wrapper = mount(QueryBuilder, {
        propsData: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].dimension.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('invalid');
      wrapper.vm.updateMember('filters', 'Orders.status', newFilter);
      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].dimension.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('valid');
    });

    it('removes filters', async () => {
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
        .mockImplementation(fetchMock(load))
        .mockImplementationOnce(fetchMock(meta));

      const filter = {
        dimension: 'Orders.status',
        operator: 'equals',
        values: ['invalid'],
      };

      const wrapper = mount(QueryBuilder, {
        propsData: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].dimension.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('invalid');
      wrapper.vm.removeMember('filters', 'Orders.status');
      expect(wrapper.vm.filters.length).toBe(0);
    });

    it('sets filters', async () => {
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request')
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

      const wrapper = mount(QueryBuilder, {
        propsData: {
          cubejsApi: cube,
          query: {
            filters: [filter],
          },
        },
      });

      await flushPromises();

      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].dimension.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('invalid');
      wrapper.vm.setMembers('filters', [newFilter]);
      expect(wrapper.vm.filters.length).toBe(1);
      expect(wrapper.vm.filters[0].dimension.name).toBe('Orders.status');
      expect(wrapper.vm.filters[0].values).toContain('valid');
    });
  });
});

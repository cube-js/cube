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

  xit('renders meta information', async () => {
    const cube = CubejsApi('token');
    jest.spyOn(cube, 'request')
      .mockImplementation(fetchMock(load))
      .mockImplementationOnce(fetchMock(meta));

    let context;

    const wrapper = mount(QueryBuilder, {
      propsData: {
        cubejsApi: cube,
        query: {
          measures: ['Orders.count'],
        },
      },
      scopedSlots: {
        default: (con) => {
          context = con;
          return `<div>${con.measures}</div>`;
        },
      }
    });

    await flushPromises();

    // console.log(context);
    // console.log(cube.request.mock.calls);
    // console.log(wrapper.html());
    expect(wrapper.text()).toContain(`i'm empty`);
  });

  describe('Update background query', () => {
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
      wrapper.vm.updateMember('timeDimensions', dimension, newDimension);
      expect(wrapper.vm.timeDimensions.length).toBe(1);
      expect(wrapper.vm.timeDimensions[0].dimension.name).toBe('LineItems.createdAt');
    });

    xit('removes filters', async () => {
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

    xit('sets filters', async () => {
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

    xit('adds filters', async () => {
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

    xit('updates filters', async () => {
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

    xit('removes filters', async () => {
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

    xit('sets filters', async () => {
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
});

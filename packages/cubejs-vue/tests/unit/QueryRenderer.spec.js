import { shallowMount } from '@vue/test-utils';
import CubejsApi from '@cubejs-client/core';
import flushPromises from 'flush-promises';
import QueryRenderer from '../../src/QueryRenderer';
import fetchMock, { load } from './__mocks__/responses';

describe('QueryRenderer.vue', () => {
  describe('Loads single query from api', () => {
    it('Loads empty state', () => {
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request').mockImplementation(fetchMock(load));

      const wrapper = shallowMount(QueryRenderer, {
        propsData: {
          query: {},
          cubejsApi: cube,
        },
        slots: {
          empty: `<div>i'm empty</div>`,
        },
      });

      expect(wrapper.text()).toContain(`i'm empty`);
      expect(cube.request.mock.calls.length).toBe(0);
    });

    it('Loads error state', async () => {
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request').mockImplementation(fetchMock({ error: 'error message' }, 400));

      const wrapper = shallowMount(QueryRenderer, {
        propsData: {
          query: {
            measures: ['Stories.count'],
          },
          cubejsApi: cube,
        },
        scopedSlots: {
          error: `<div>{{props.error}}</div>`,
        },
      });

      await flushPromises();

      expect(wrapper.text()).toContain('error message');
      expect(cube.request.mock.calls.length).toBe(1);
    });

    it('Loads resultSet', async () => {
      const cube = CubejsApi('token');
      jest.spyOn(cube, 'request').mockImplementation(fetchMock(load));

      const wrapper = shallowMount(QueryRenderer, {
        propsData: {
          query: {
            measures: ['Stories.count'],
          },
          cubejsApi: cube,
        },
        scopedSlots: {
          default: `<div>Result set is loaded</div>`,
        },
      });

      await flushPromises();

      expect(wrapper.text()).toContain('Result set is loaded');
      expect(cube.request.mock.calls.length).toBe(1);
    });
  });
});

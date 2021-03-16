import { shallowMount, mount } from '@vue/test-utils';
import flushPromises from 'flush-promises';

import QueryRenderer from '../../src/QueryRenderer';
import fetchMock, { load, single } from './__mocks__/responses';
import { createCubejsApi } from './utils';

describe('QueryRenderer.vue', () => {
  describe('Loads single query from api', () => {
    it('Loads empty state', () => {
      const cube = createCubejsApi();
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
      const cube = createCubejsApi();
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
      const cube = createCubejsApi();
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

    it('Rerender on query nested property change', async () => {
      const cube = createCubejsApi();
      jest.spyOn(cube, 'request').mockImplementation(fetchMock(single));

      const parent = mount({
        components: {
          QueryRenderer,
        },
        template: `
          <div>
            <query-renderer :cubejs-api="cubejsApi" :query="query" v-slot="{ query }">
              <span class="query">{{query}}</span>
            </query-renderer>
          </div>
        `,
        data() {
          return {
            cubejsApi: cube ,
            query: {
              measures: ['Stories.count'],
              dimensions: [],
              filters: [],
              segments: [],
              timeDimensions: [],
            },
          };
        },
      });

      await flushPromises();

      expect(cube.request.mock.calls.length).toBe(1);
      expect(parent.find('.query').element.textContent).toContain('Stories.count');

      parent.vm.query.measures = ['Users.count'];
      await flushPromises();

      expect(cube.request.mock.calls.length).toBe(2);
      expect(parent.find('.query').element.textContent).toContain('Users.count');

      parent.vm.query.measures.push('Users.count');
      await flushPromises();

      expect(cube.request.mock.calls.length).toBe(3);
      expect(parent.find('.query').element.textContent).toContain('Users.count');

      parent.vm.query.timeDimensions.push({ dimension: 'Users.count', dateRange: 'last 6 days', granularity: 'week' });
      await flushPromises();

      expect(cube.request.mock.calls.length).toBe(4);
      expect(parent.find('.query').element.textContent).toContain('week');
    });
  });
});

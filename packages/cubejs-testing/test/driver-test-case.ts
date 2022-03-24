// eslint-disable-next-line import/no-extraneous-dependencies
import { jest, expect, beforeAll, afterAll } from '@jest/globals';
// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { Query, CubejsApi } from '@cubejs-client/core';
import { BirdBox } from '../src';

// eslint-disable-next-line import/prefer-default-export
export function createBirdBoxTestCase(name: string, entrypoint: () => Promise<BirdBox>) {
  describe(name, () => {
    jest.setTimeout(60 * 5 * 1000);

    let birdbox: BirdBox;
    let httpClient: CubejsApi;

    // eslint-disable-next-line consistent-return
    beforeAll(async () => {
      // Fail fast
      try {
        birdbox = await entrypoint();

        // http clients
        httpClient = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
        });
      } catch (e) {
        console.log(e);
        process.exit(1);
      }
    });

    // eslint-disable-next-line consistent-return
    afterAll(async () => {
      await birdbox.stop();
    });

    it('Orders.totalAmount', async () => {
      const query: Query = {
        measures: [
          'Orders.totalAmount'
        ],
        timeDimensions: [],
        order: {}
      };
      const response = await httpClient.load(query);
      expect(response.rawData()).toMatchSnapshot('Orders.totalAmount');
    });

    it('Events.count', async () => {
      const query: Query = {
        measures: [
          'Events.count'
        ],
        timeDimensions: [],
        order: {},
        dimensions: []
      };
      const response = await httpClient.load(query);
      expect(response.rawData()).toMatchSnapshot('Events.count');
    });

    it('Events.count with Events.type order by Events.count DESC', async () => {
      const query: Query = {
        measures: [
          'Events.count'
        ],
        timeDimensions: [],
        order: {
          'Events.count': 'desc'
        },
        dimensions: [
          'Events.type'
        ]
      };
      const response = await httpClient.load(query);
      expect(response.rawData()).toMatchSnapshot('Events.count with Events.type order by Events.count DESC');
    });
  });
}

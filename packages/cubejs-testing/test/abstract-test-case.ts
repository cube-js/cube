import { expect } from '@jest/globals';
import cubejs from '@cubejs-client/core';
import { BirdBox } from '../src';

// eslint-disable-next-line import/prefer-default-export
export function createBirdBoxTestCase(name: string, entrypoint: () => Promise<BirdBox>) {
  describe(name, () => {
    jest.setTimeout(60 * 5 * 1000);

    let birdbox: BirdBox;

    // eslint-disable-next-line consistent-return
    beforeAll(async () => {
      // Fail fast
      try {
        birdbox = await entrypoint();
      } catch (e) {
        console.log(e);
        process.exit(1);
      }
    });

    // eslint-disable-next-line consistent-return
    afterAll(async () => {
      await birdbox.stop();
    });

    it('Query Orders.totalAmount', async () => {
      const client = cubejs(async () => 'test', {
        apiUrl: birdbox.configuration.apiUrl,
      });

      const response = await client.load({
        measures: [
          'Orders.totalAmount'
        ],
        timeDimensions: [],
        order: {}
      });

      expect(response.rawData()).toEqual([
        {
          'Orders.totalAmount': '1700'
        }
      ]);
    });
  });
}

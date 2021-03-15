import { expect } from '@jest/globals';
import { StartedDockerComposeEnvironment } from 'testcontainers';
import cubejs from '@cubejs-client/core';
import { BirdBoxTestCaseOptions, startBidBoxContainer } from '../src';

// eslint-disable-next-line import/prefer-default-export
export function createBirdBoxTestCase(options: BirdBoxTestCaseOptions) {
  describe(options.name, () => {
    jest.setTimeout(60 * 5 * 1000);

    let env: StartedDockerComposeEnvironment|null = null;
    let config: { apiUrl: string, };

    // eslint-disable-next-line consistent-return
    beforeAll(async () => {
      const birdBox = await startBidBoxContainer(options);

      env = birdBox.env;
      config = birdBox.configuration;
    });

    // eslint-disable-next-line consistent-return
    afterAll(async () => {
      if (env) {
        await env.down();
      }
    });

    it('Query Orders.totalAmount', async () => {
      const client = cubejs(async () => 'test', {
        apiUrl: config.apiUrl,
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

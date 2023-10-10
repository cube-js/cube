import { StartedTestContainer } from 'testcontainers';
import { MaterializeDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { pausePromise } from '@cubejs-backend/shared';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_API_TOKEN, DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

describe('materialize', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    db = await MaterializeDBRunner.startContainer({});
    birdbox = await getBirdbox(
      'materialize',
      {
        CUBEJS_DB_TYPE: 'materialize',

        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(6875)}`,
        CUBEJS_DB_NAME: 'materialize',
        CUBEJS_DB_USER: 'materialize',
        CUBEJS_DB_PASS: 'materialize',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'materialize/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  });

  test('query measure', () => testQueryMeasure(client));

  test('query dimensions', async () => {
    const queryDimensions = async () => {
      const response = await client.load({
        measures: [
          'Orders.totalAmount',
        ],
        dimensions: [
          'Orders.status',
        ],
      });

      expect(response.rawData()).toMatchSnapshot('dimensions');
    };
    await queryDimensions();

    /**
     * Running a query with 2 seconds delay
     * preAggregation has 1 second in the refreshKey
     * Gives times to trigger the action if hasn't been triggered yet.
     */
    await pausePromise(2000);
    await queryDimensions();
  });
});

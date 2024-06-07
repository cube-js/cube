import { StartedTestContainer } from 'testcontainers';
import { QuestDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  testQueryMeasure,
} from './smoke-tests';

describe('questdb', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    db = await QuestDBRunner.startContainer({});
    birdbox = await getBirdbox(
      'questdb',
      {
        CUBEJS_DB_TYPE: 'questdb',

        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(8812)}`,
        CUBEJS_DB_NAME: 'qdb',
        CUBEJS_DB_USER: 'admin',
        CUBEJS_DB_PASS: 'quest',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'questdb/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('query measure', () => testQueryMeasure(client));

  // Error: column type mismatch [index=0, A=STRING, B=INT]
  test.skip('query measure + dimension', async () => {
    const response = await client.load({
      measures: [
        'Orders.totalAmount',
      ],
      dimensions: [
        'Orders.status',
      ],
    });
    expect(response.rawData()).toMatchSnapshot('query');
  });
});

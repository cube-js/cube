import { StartedTestContainer } from 'testcontainers';
import { QuestDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

describe('questdb', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubejsApi;

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
    client = cubejs(async () => 'test', {
      apiUrl: birdbox.configuration.apiUrl,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
    await db.stop();
  });

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

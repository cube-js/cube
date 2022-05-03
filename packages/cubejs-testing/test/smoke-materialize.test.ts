import { StartedTestContainer } from 'testcontainers';
import { MaterializeDBRunner } from '@cubejs-backend/testing-shared';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';

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

        CUBEJS_DEV_MODE: 'true',
        CUBEJS_WEB_SOCKETS: 'false',
        CUBEJS_EXTERNAL_DEFAULT: 'true',
        CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
        CUBEJS_REFRESH_WORKER: 'false',
        CUBEJS_ROLLUP_ONLY: 'false',
      },
      {
        schemaDir: 'postgresql/schema',
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

  test('query measure', async () => {
    const response = await client.load({
      measures: [
        'Orders.totalAmount',
      ],
    });
    expect(response.rawData()).toMatchSnapshot('query');
  });
});

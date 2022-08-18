import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest } from '@jest/globals';
import { StartedTestContainer } from 'testcontainers';
import { CrateDBRunner } from '@cubejs-backend/testing-shared';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

describe('crate', () => {
  jest.setTimeout(60 * 5 * 1000);
  let db: StartedTestContainer;
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    db = await CrateDBRunner.startContainer({});
    birdbox = await getBirdbox(
      'crate',
      {
        CUBEJS_DB_TYPE: 'crate',
        ...DEFAULT_CONFIG,

        CUBEJS_DB_HOST: db.getHost(),
        CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
        CUBEJS_DB_NAME: 'crate',
        CUBEJS_DB_USER: 'crate',
        CUBEJS_DB_PASS: '',
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

  test('query measure', () => testQueryMeasure(client));
});

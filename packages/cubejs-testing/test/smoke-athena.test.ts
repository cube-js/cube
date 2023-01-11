import fetch from 'node-fetch';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest, expect } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

describe('athena', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'athena',
      {
        CUBEJS_DB_TYPE: 'athena',
        CUBEJS_DB_NAME: 'default',

        ...DEFAULT_CONFIG,
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
  });

  test('query measure', () => testQueryMeasure(client));

  test('can list views', async () => {
    await (await fetch(`${birdbox.configuration.systemUrl}/sql-runner`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: {
          query: 'CREATE OR REPLACE VIEW view_fetch_test AS SELECT 1 as order_id, 10 as amount'
        }
      }),
    })).json();

    const schema = await (await fetch(`${birdbox.configuration.playgroundUrl}/playground/db-schema`, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    })).json();

    expect(schema.tablesSchema.default.view_fetch_test).toEqual([
      { name: 'order_id', type: 'integer', attributes: [] },
      { name: 'amount', type: 'integer', attributes: [] }
    ]);

    await (await fetch(`${birdbox.configuration.systemUrl}/sql-runner`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: {
          query: 'DROP VIEW view_fetch_test'
        }
      }),
    })).json();
  });
});

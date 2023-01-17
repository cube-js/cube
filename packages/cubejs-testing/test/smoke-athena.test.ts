import fetch from 'node-fetch';
import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, jest, expect } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_CONFIG, testQueryMeasure } from './smoke-tests';

const delay = (t: number) => new Promise(resolve => setTimeout(() => resolve(null), t));

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
    const result = await (await fetch(`${birdbox.configuration.systemUrl}/sql-runner`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: {
          query: 'CREATE OR REPLACE VIEW default.view_fetch_test AS SELECT 1 as order_id, 10 as amount'
        }
      }),
    }));

    if (result.status !== 200) {
      throw new Error(((await result.json()).error));
    }

    console.log(await result.json());

    await delay(5000);

    const schema = await (await fetch(`${birdbox.configuration.playgroundUrl}/playground/db-schema`, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    })).json();

    console.log(JSON.stringify(schema.tablesSchema, null, 2));

    expect(schema.tablesSchema[Object.keys(schema.tablesSchema)[0]].view_fetch_test).toEqual([
      { name: 'order_id', type: 'integer', attributes: [] },
      { name: 'amount', type: 'integer', attributes: [] }
    ]);

    await (await fetch(`${birdbox.configuration.systemUrl}/sql-runner`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: {
          query: 'DROP VIEW default.view_fetch_test'
        }
      }),
    })).json();
  });
});

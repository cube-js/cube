import cubejs, { CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import { DEFAULT_API_TOKEN, DEFAULT_CONFIG } from './smoke-tests';

const CubeStoreDriver = require('@cubejs-backend/cubestore-driver');

describe('snowflake', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubejsApi;
  let cubeStoreDriver: any;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'snowflake',
      {
        CUBEJS_DB_TYPE: 'snowflake',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'snowflake/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
    // @ts-ignore
    cubeStoreDriver = new CubeStoreDriver({
      host: '127.0.0.1',
      user: undefined,
      password: undefined,
      port: 3030,
    });
  });

  afterAll(async () => {
    await birdbox.stop();
  });

  test('int column data type', async () => {
    const response = await client.load({
      measures: [
        'Orders.totalAmount',
        'Orders.totalFloatAmount',
        'Orders.totalDecimalAmount',
      ],
    });
    expect(response.rawData()).toMatchSnapshot('query');
    const tables = await cubeStoreDriver.getTablesQuery('dev_pre_aggregations');
    console.log(tables[0].table_name);
    console.log(await cubeStoreDriver.tableColumnTypes(`dev_pre_aggregations.${tables[0].table_name}`));
    expect(await cubeStoreDriver.tableColumnTypes(`dev_pre_aggregations.${tables[0].table_name}`)).toMatchSnapshot('preAggTableTypes');
  });
});

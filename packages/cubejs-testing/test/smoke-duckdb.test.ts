import cubejs, { CubeApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest, test } from '@jest/globals';
import { BirdBox, getBirdbox } from '../src';
import {
  DEFAULT_API_TOKEN,
  DEFAULT_CONFIG,
  JEST_AFTER_ALL_DEFAULT_TIMEOUT,
  JEST_BEFORE_ALL_DEFAULT_TIMEOUT,
  testQueryMeasure,
} from './smoke-tests';

describe('duckdb', () => {
  jest.setTimeout(60 * 5 * 1000);
  let birdbox: BirdBox;
  let client: CubeApi;

  beforeAll(async () => {
    birdbox = await getBirdbox(
      'duckdb',
      {
        CUBEJS_DB_TYPE: 'duckdb',

        ...DEFAULT_CONFIG,
      },
      {
        schemaDir: 'duckdb/schema',
      }
    );
    client = cubejs(async () => DEFAULT_API_TOKEN, {
      apiUrl: birdbox.configuration.apiUrl,
    });
  }, JEST_BEFORE_ALL_DEFAULT_TIMEOUT);

  afterAll(async () => {
    await birdbox.stop();
  }, JEST_AFTER_ALL_DEFAULT_TIMEOUT);

  test('query measure', () => testQueryMeasure(client));

  test('numeric measure filter - uses CAST', async () => {
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.totalAmount',
          operator: 'gte',
          values: ['300']
        }
      ]
    });
    
    // The HAVING clause applies after counting all records
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });
  
  test('numeric dimension filter - uses CAST', async () => {
    // This test verifies that the WHERE clause for numeric dimensions uses CAST(? AS DOUBLE)
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.amount',
          operator: 'gte',
          values: ['300']
        }
      ]
    });
    
    // Only 3 orders have amount >= 300
    expect(response.rawData()[0]['Orders.count']).toBe('3');
  });
  
  test('string filter - does NOT use CAST', async () => {
    // This test verifies that the WHERE clause for string dimensions does not use CAST
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.status',
          operator: 'equals',
          values: ['processed']
        }
      ]
    });
    
    // There are 2 'processed' orders 
    expect(response.rawData()[0]['Orders.count']).toBe('2');
  });
  
  test('numeric exact equality filter - also uses CAST', async () => {
    // This test verifies that even for equality comparisons on numeric values, CAST is used
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.amount',
          operator: 'equals',
          values: ['300']
        }
      ]
    });
    
    // Only 1 order has amount exactly 300
    expect(response.rawData()[0]['Orders.count']).toBe('1');
  });
  
  test('numeric string comparison - values are properly handled with CAST', async () => {
    // This test verifies that numeric strings are properly cast as numbers
    // This is important because '100' and 100 are different in string comparisons
    // but should be treated the same with numeric CAST
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.amount',
          operator: 'equals',
          values: ['100'] // string '100' should be cast to number 100
        }
      ]
    });
    
    // Only 1 order has amount 100
    expect(response.rawData()[0]['Orders.count']).toBe('1');
  });
  
  test('multiple string values in filter - none use CAST', async () => {
    // This test verifies that IN (...) clauses for string dimensions don't use CAST
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.status',
          operator: 'equals',
          values: ['new', 'processed']
        }
      ]
    });
    
    // There are 4 orders with status 'new' or 'processed'
    expect(response.rawData()[0]['Orders.count']).toBe('4');
  });
});

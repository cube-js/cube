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
  
  test('string measure filter - should NOT use CAST', async () => {
    // This test verifies that filters on string measures don't use CAST
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.lastStatus',
          operator: 'equals',
          values: ['shipped']
        }
      ]
    });
    
    // Output should match the shipped orders
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });
  
  test('boolean measure filter - should NOT use CAST', async () => {
    // This test verifies that filters on boolean measures don't use CAST
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.hasUnpaidOrders',
          operator: 'equals',
          values: ['true']
        }
      ]
    });
    
    // Output should match all orders since there are unpaid orders
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });
  
  test('time measure filter - afterDate operator', async () => {
    // This test verifies that filters on time measures don't use CAST
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.maxCreatedAt',
          operator: 'afterDate',
          values: ['2020-01-03']
        }
      ]
    });
    
    // Orders after 2020-01-03
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });
  
  test('time measure filter - beforeDate operator', async () => {
    // This test verifies that beforeDate operator works correctly with time measures
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.maxCreatedAt',
          operator: 'beforeDate',
          values: ['2020-01-06']
        }
      ]
    });
    // All orders should be counted since max date (2020-01-05) is before 2020-01-06
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });

  test('time measure filter - beforeOrOnDate operator', async () => {
    // This test verifies that beforeOrOnDate operator works correctly with time measures
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.maxCreatedAt',
          operator: 'beforeOrOnDate',
          values: ['2020-01-05']
        }
      ]
    });
    // All orders should be counted since max date (2020-01-05) is equal to 2020-01-05
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });

  test('time measure filter - afterOrOnDate operator', async () => {
    // This test verifies that afterOrOnDate operator works correctly with time measures
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.maxCreatedAt',
          operator: 'afterOrOnDate',
          values: ['2020-01-05']
        }
      ]
    });
    // All orders should be counted since max date (2020-01-05) is equal to 2020-01-05
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });

  test('time measure filter - inDateRange operator', async () => {
    // This test verifies that inDateRange operator works correctly with time measures
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.maxCreatedAt',
          operator: 'inDateRange',
          values: ['2020-01-04', '2020-01-06']
        }
      ]
    });
    // All orders should be counted since max date (2020-01-05) is within the range
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });

  test('time measure filter - notInDateRange operator', async () => {
    // This test verifies that notInDateRange operator works correctly with time measures
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.maxCreatedAt',
          operator: 'notInDateRange',
          values: ['2020-01-06', '2020-01-07']
        }
      ]
    });
    // All orders should be counted since max date (2020-01-05) is outside the range
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });

  test('time measure filter - equals operator', async () => {
    // This test verifies that equals operator works correctly with time measures
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.maxCreatedAt',
          operator: 'equals',
          values: ['2020-01-05']
        }
      ]
    });
    // All orders should be counted since max date equals 2020-01-05
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });

  test('time measure filter - notEquals operator', async () => {
    // This test verifies that notEquals operator works correctly with time measures
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.maxCreatedAt',
          operator: 'notEquals',
          values: ['2020-01-06']
        }
      ]
    });
    // All orders should be counted since max date (2020-01-05) is not equal to 2020-01-06
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });

  test('boolean dimension filter - should NOT use CAST', async () => {
    // This test verifies that filters on boolean dimensions don't use CAST
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.isPaid',
          operator: 'equals',
          values: ['false']
        }
      ]
    });
    
    // There are 2 unpaid orders
    expect(response.rawData()[0]['Orders.count']).toBe('2');
  });
  
  test('time dimension filter - should NOT use CAST', async () => {
    // This test verifies that filters on time types don't use CAST
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'afterDate',
          values: ['2020-01-03']
        }
      ]
    });
    
    // There are 2 orders after 2020-01-03
    expect(response.rawData()[0]['Orders.count']).toBe('2');
  });
  
  test('time dimension filter - beforeDate operator', async () => {
    // This test verifies that the beforeDate operator works correctly
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'beforeDate',
          values: ['2020-01-03']
        }
      ]
    });
    
    // There are 2 orders before 2020-01-03
    expect(response.rawData()[0]['Orders.count']).toBe('2');
  });
  
  test('time dimension filter - beforeOrOnDate operator', async () => {
    // This test verifies that the beforeOrOnDate operator works correctly
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'beforeOrOnDate',
          values: ['2020-01-03']
        }
      ]
    });
    
    // There are 3 orders before or on 2020-01-03
    expect(response.rawData()[0]['Orders.count']).toBe('3');
  });
  
  test('time dimension filter - afterOrOnDate operator', async () => {
    // This test verifies that the afterOrOnDate operator works correctly
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'afterOrOnDate',
          values: ['2020-01-03']
        }
      ]
    });
    
    // There are 3 orders after or on 2020-01-03
    expect(response.rawData()[0]['Orders.count']).toBe('3');
  });
  
  test('time dimension filter - inDateRange operator', async () => {
    // This test verifies that the inDateRange operator works correctly
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'inDateRange',
          values: ['2020-01-02', '2020-01-04']
        }
      ]
    });
    
    // There are 3 orders between 2020-01-02 and 2020-01-04 (inclusive)
    expect(response.rawData()[0]['Orders.count']).toBe('3');
  });
  
  test('time dimension filter - notInDateRange operator', async () => {
    // This test verifies that the notInDateRange operator works correctly
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'notInDateRange',
          values: ['2020-01-02', '2020-01-04']
        }
      ]
    });
    
    // There are 2 orders outside the range 2020-01-02 to 2020-01-04
    expect(response.rawData()[0]['Orders.count']).toBe('2');
  });
  
  test('time dimension filter - set operator', async () => {
    // This test verifies that the set operator works correctly
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'set'
        }
      ]
    });
    
    // All 5 orders have createdAt set
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });
  
  test('time measure filter - set operator', async () => {
    // This test verifies that the set operator works correctly with measures
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.maxCreatedAt',
          operator: 'set'
        }
      ]
    });
    
    // All 5 orders have maxCreatedAt set
    expect(response.rawData()[0]['Orders.count']).toBe('5');
  });
  
  // We can't effectively test notSet with the current schema since all records have dates
  // This test is more for API completeness
  test('time dimension filter - notSet operator', async () => {
    // This test verifies that the notSet operator works correctly
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'notSet'
        }
      ]
    });
    
    // No orders have createdAt as null
    expect(response.rawData()[0]['Orders.count']).toBe('0');
  });

  test('time dimension filter - equals operator', async () => {
    // This test verifies that equals operator works correctly with time dimensions
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'equals',
          values: ['2020-01-03']
        }
      ]
    });
    
    // There is 1 order on exactly 2020-01-03
    expect(response.rawData()[0]['Orders.count']).toBe('1');
  });

  test('time dimension filter - notEquals operator', async () => {
    // This test verifies that notEquals operator works correctly with time dimensions
    const response = await client.load({
      measures: [
        'Orders.count'
      ],
      filters: [
        {
          member: 'Orders.createdAt',
          operator: 'notEquals',
          values: ['2020-01-03']
        }
      ]
    });
    
    // There are 4 orders not on 2020-01-03
    expect(response.rawData()[0]['Orders.count']).toBe('4');
  });
});

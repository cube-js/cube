// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';
import { driverTest } from './driverTest';

export const customerDimensionsAndLimitTest = driverTest({
  name: 'querying Customers: dimentions + limit',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    limit: 10
  },
  expectArray: [(response) => expect(response.rawData().length).toEqual(10)]
});

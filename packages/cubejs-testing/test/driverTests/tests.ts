// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';
import { driverTest } from './driverTest';

export const queryingCustomersDimensions = driverTest({
  name: 'querying Customers: dimensions',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
  },
});

export const queryingCustomersDimensionsAndOrder = driverTest({
  name: 'querying Customers: dimentions + order',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    order: {
      'Customers.customerId': 'asc',
    }
  },
});

export const queryingCustomerDimensionsAndLimitTest = driverTest({
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

export const queryingCustomersDimensionsAndTotal = driverTest({
  name: 'querying Customers: dimentions + total',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    total: true
  },
  expectArray: [(response) => expect(
    response.serialize().loadResponse.results[0].total
  ).toEqual(41)]
});

export const queryingCustomersDimensionsOrderLimitTotal = driverTest({
  name: 'querying Customers: dimentions + order + limit + total',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    order: {
      'Customers.customerName': 'asc'
    },
    limit: 10,
    total: true
  },
  expectArray: [
    (response) => expect(response.rawData().length).toEqual(10),
    (response) => expect(
      response.serialize().loadResponse.results[0].total
    ).toEqual(41)
  ]
});

export const queryingCustomersDimensionsOrderTotalOffset = driverTest({
  name: 'querying Customers: dimentions + order + total + offset',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    order: {
      'Customers.customerName': 'asc'
    },
    total: true,
    offset: 40
  },
  expectArray: [
    (response) => expect(response.rawData().length).toEqual(1),
    (response) => expect(
      response.serialize().loadResponse.results[0].total
    ).toEqual(41)
  ]

});

export const queryingCustomersDimensionsOrderLimitTotalOffset = driverTest({
  name: 'querying Customers: dimentions + order + limit + total + offset',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    order: {
      'Customers.customerName': 'asc'
    },
    limit: 10,
    total: true,
    offset: 10
  },
  expectArray: [(r) => expect(r.rawData().length).toEqual(10), (r) => expect(
    r.serialize().loadResponse.results[0].total
  ).toEqual(41)]
});

export const filteringCustomersCubeFirst = driverTest(
  {
    name: 'filtering Customers: contains + dimensions, first',
    query: {
      dimensions: [
        'Customers.customerId',
        'Customers.customerName'
      ],
      filters: [
        {
          member: 'Customers.customerName',
          operator: 'contains',
          values: ['tom'],
        },
      ],
    }

  }
);

export const filteringCustomersCubeSecond = driverTest(
  {
    name: 'filtering Customers: contains + dimensions, second',
    query: {
      dimensions: [
        'Customers.customerId',
        'Customers.customerName'
      ],
      filters: [
        {
          member: 'Customers.customerName',
          operator: 'contains',
          values: ['us', 'om'],
        },
      ],
    }
    
  }
);

export const filteringCustomersCubeThird = driverTest(
  {
    name: 'filtering Customers: contains + dimensions, third',
    query: {
      dimensions: [
        'Customers.customerId',
        'Customers.customerName'
      ],
      filters: [
        {
          member: 'Customers.customerName',
          operator: 'contains',
          values: ['non'],
        },
      ],
    }
  }
);

// test(
//   'filtering Customers: startsWith + dimensions',
//   async () => {
//     let response;

//     response = await client.load({
//       dimensions: [
//         'Customers.customerId',
//         'Customers.customerName'
//       ],
//       filters: [
//         {
//           member: 'Customers.customerId',
//           operator: 'startsWith',
//           values: ['A'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');

//     response = await client.load({
//       dimensions: [
//         'Customers.customerId',
//         'Customers.customerName'
//       ],
//       filters: [
//         {
//           member: 'Customers.customerId',
//           operator: 'startsWith',
//           values: ['A', 'B'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');

//     response = await client.load({
//       dimensions: [
//         'Customers.customerId',
//         'Customers.customerName'
//       ],
//       filters: [
//         {
//           member: 'Customers.customerId',
//           operator: 'startsWith',
//           values: ['Z'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');
//   }
// );

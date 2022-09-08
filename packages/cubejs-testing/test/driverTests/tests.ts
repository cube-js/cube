// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';
import { driverTest } from './driverTest';

const commonSchemas = [
  'postgresql/CommonCustomers.js',
  'CAST.js',
  'Customers.sql.js',
  'ECommerce.sql.js',
  'Products.sql.js',
  'Customers.js',
  'ECommerce.js',
  'Products.js',
];

export const queryingCustomersDimensions = driverTest({
  name: 'querying Customers: dimensions',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
  },
  schemas: commonSchemas,
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
  schemas: commonSchemas,
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
  expectArray: [(response) => expect(response.rawData().length).toEqual(10)],
  schemas: commonSchemas,
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
  ).toEqual(41)],
  schemas: commonSchemas,
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
  ],
  schemas: commonSchemas,
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
  ],
  schemas: commonSchemas,

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
  ).toEqual(41)],
  schemas: commonSchemas,
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
    },
    schemas: commonSchemas,

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
    },
    schemas: commonSchemas,
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
    },
    schemas: commonSchemas,
  }
  
);

export const filteringCustomersEndsWithFilterFirst = driverTest({
  name: 'filtering Customers: endsWith filter + dimensions, first',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    filters: [
      {
        member: 'Customers.customerId',
        operator: 'endsWith',
        values: ['0'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringCustomersEndsWithFilterSecond = driverTest({
  name: 'filtering Customers: endsWith filter + dimensions, second',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    filters: [
      {
        member: 'Customers.customerId',
        operator: 'endsWith',
        values: ['0', '5'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringCustomersEndsWithFilterThird = driverTest({
  name: 'filtering Customers: endsWith filter + dimensions, third',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    filters: [
      {
        member: 'Customers.customerId',
        operator: 'endsWith',
        values: ['9'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringCustomersStartsWithAndDimensionsFirst = driverTest({
  name: 'filtering Customers: startsWith + dimensions, first',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    filters: [
      {
        member: 'Customers.customerId',
        operator: 'startsWith',
        values: ['A'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringCustomersStartsWithAndDimensionsSecond = driverTest({
  name: 'filtering Customers: startsWith + dimensions, second',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    filters: [
      {
        member: 'Customers.customerId',
        operator: 'startsWith',
        values: ['A', 'B'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringCustomersStartsWithAndDimensionsThird = driverTest({
  name: 'filtering Customers: startsWith + dimensions, third',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    filters: [
      {
        member: 'Customers.customerId',
        operator: 'startsWith',
        values: ['Z'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringCustomersEndsWithFilterAndDimensionsFirst = driverTest({
  name: 'filtering Customers: endsWith filter + dimensions, first',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    filters: [
      {
        member: 'Customers.customerId',
        operator: 'endsWith',
        values: ['0'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringCustomersEndsWithFilterAndDimensionsSecond = driverTest({
  name: 'filtering Customers: endsWith filter + dimensions, second',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    filters: [
      {
        member: 'Customers.customerId',
        operator: 'endsWith',
        values: ['0', '5'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringCustomersEndsWithFilterAndDimensionsThird = driverTest({
  name: 'filtering Customers: endsWith filter + dimensions, third',
  query: {
    dimensions: [
      'Customers.customerId',
      'Customers.customerName'
    ],
    filters: [
      {
        member: 'Customers.customerId',
        operator: 'endsWith',
        values: ['9'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const queryingProductDimensions = driverTest({
  name: 'querying Products: dimensions -- doesn\'t work wo ordering',
  query: {
    dimensions: [
      'Products.category',
      'Products.subCategory',
      'Products.productName'
    ]
  },
  skip: true,
  schemas: commonSchemas,
});

export const queryingProductsDimensionsAndOrder = driverTest({
  name: 'querying Products: dimentions + order',
  query: {
    dimensions: [
      'Products.category',
      'Products.subCategory',
      'Products.productName'
    ],
    order: {
      'Products.category': 'asc',
      'Products.subCategory': 'asc',
      'Products.productName': 'asc'
    }
  },
  schemas: commonSchemas,
});

export const queryingProductsDimensionsAndOrderAndLimit = driverTest({
  name: 'querying Products: dimentions + order + limit',
  query: {
    dimensions: [
      'Products.category',
      'Products.subCategory',
      'Products.productName'
    ],
    order: {
      'Products.category': 'asc',
      'Products.subCategory': 'asc',
      'Products.productName': 'asc'
    },
    limit: 10
  },
  expectArray: [r => expect(r.rawData().length).toEqual(10)],
  schemas: commonSchemas,
});

export const queryingProductsDimensionsOrderAndTotal = driverTest({
  name: 'querying Products: dimentions + order + total',
  query: {
    dimensions: [
      'Products.category',
      'Products.subCategory',
      'Products.productName'
    ],
    order: {
      'Products.category': 'asc',
      'Products.subCategory': 'asc',
      'Products.productName': 'asc'
    },
    total: true
  },
  expectArray: [
    (r) => expect(
      r.serialize().loadResponse.results[0].total
    ).toEqual(28)
  ],
  schemas: commonSchemas,
});

export const queryingProductsDimensionsOrderAndLimitAndTotal = driverTest({
  name: 'querying Products: dimentions + order + limit + total',
  query: {
    dimensions: [
      'Products.category',
      'Products.subCategory',
      'Products.productName'
    ],
    order: {
      'Products.category': 'asc',
      'Products.subCategory': 'asc',
      'Products.productName': 'asc'
    },
    limit: 10,
    total: true
  },
  expectArray: [
    r => expect(r.rawData().length).toEqual(10),
    r => expect(
      r.serialize().loadResponse.results[0].total
    ).toEqual(28)
  ],
  schemas: commonSchemas,
});

export const filteringProductsContainsAndDimensionsAndOrderFirst = driverTest({
  name: 'filtering Products: contains + dimentions + order, first',
  query: {
    dimensions: [
      'Products.category',
      'Products.subCategory',
      'Products.productName'
    ],
    order: {
      'Products.category': 'asc',
      'Products.subCategory': 'asc',
      'Products.productName': 'asc'
    },
    filters: [
      {
        member: 'Products.subCategory',
        operator: 'contains',
        values: ['able'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringProductsContainsAndDimensionsAndOrderSecond = driverTest({
  name: 'filtering Products: contains + dimentions + order, second',
  query: {
    dimensions: [
      'Products.category',
      'Products.subCategory',
      'Products.productName'
    ],
    order: {
      'Products.category': 'asc',
      'Products.subCategory': 'asc',
      'Products.productName': 'asc'
    },
    filters: [
      {
        member: 'Products.subCategory',
        operator: 'contains',
        values: ['able', 'urn'],
      },
    ],
  },
  schemas: commonSchemas,
});

//     response = await client.load({
//       dimensions: [
//         'Products.category',
//         'Products.subCategory',
//         'Products.productName'
//       ],
//       order: {
//         'Products.category': 'asc',
//         'Products.subCategory': 'asc',
//         'Products.productName': 'asc'
//       },
//       filters: [
//         {
//           member: 'Products.subCategory',
//           operator: 'contains',
//           values: ['notexist'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');
//   }
// );
// test(
//   'filtering Products: startsWith filter + dimentions + order',
//   async () => {
//     let response;

//     response = await client.load({
//       dimensions: [
//         'Products.category',
//         'Products.subCategory',
//         'Products.productName'
//       ],
//       order: {
//         'Products.category': 'asc',
//         'Products.subCategory': 'asc',
//         'Products.productName': 'asc'
//       },
//       filters: [
//         {
//           member: 'Products.productName',
//           operator: 'startsWith',
//           values: ['O'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');

//     response = await client.load({
//       dimensions: [
//         'Products.category',
//         'Products.subCategory',
//         'Products.productName'
//       ],
//       order: {
//         'Products.category': 'asc',
//         'Products.subCategory': 'asc',
//         'Products.productName': 'asc'
//       },
//       filters: [
//         {
//           member: 'Products.productName',
//           operator: 'startsWith',
//           values: ['O', 'K'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');

//     response = await client.load({
//       dimensions: [
//         'Products.category',
//         'Products.subCategory',
//         'Products.productName'
//       ],
//       order: {
//         'Products.category': 'asc',
//         'Products.subCategory': 'asc',
//         'Products.productName': 'asc'
//       },
//       filters: [
//         {
//           member: 'Products.productName',
//           operator: 'startsWith',
//           values: ['noneexist'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');
//   }
// );
// test(
//   'filtering Products: endsWith filter + dimentions + order',
//   async () => {
//     let response;

//     response = await client.load({
//       dimensions: [
//         'Products.category',
//         'Products.subCategory',
//         'Products.productName'
//       ],
//       order: {
//         'Products.category': 'asc',
//         'Products.subCategory': 'asc',
//         'Products.productName': 'asc'
//       },
//       filters: [
//         {
//           member: 'Products.subCategory',
//           operator: 'endsWith',
//           values: ['es'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');

//     response = await client.load({
//       dimensions: [
//         'Products.category',
//         'Products.subCategory',
//         'Products.productName'
//       ],
//       order: {
//         'Products.category': 'asc',
//         'Products.subCategory': 'asc',
//         'Products.productName': 'asc'
//       },
//       filters: [
//         {
//           member: 'Products.subCategory',
//           operator: 'endsWith',
//           values: ['es', 'gs'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');

//     response = await client.load({
//       dimensions: [
//         'Products.category',
//         'Products.subCategory',
//         'Products.productName'
//       ],
//       order: {
//         'Products.category': 'asc',
//         'Products.subCategory': 'asc',
//         'Products.productName': 'asc'
//       },
//       filters: [
//         {
//           member: 'Products.subCategory',
//           operator: 'endsWith',
//           values: ['noneexist'],
//         },
//       ],
//     });
//     expect(response.rawData()).toMatchSnapshot('query');
//   }
// );

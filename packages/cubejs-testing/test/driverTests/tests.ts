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
  }
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
  }
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
  }
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
  }
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
  }
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
  }
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
});

// // querying Products cube
// test.skip(
//   'querying Products: dimensions -- doesn\'t work wo ordering',
//   async () => {
//     const response = await client.load({
//       dimensions: [
//         'Products.category',
//         'Products.subCategory',
//         'Products.productName'
//       ]
//     });
//     expect(response.rawData()).toMatchSnapshot('query');
//   }
// );
// test(
//   'querying Products: dimentions + order',
//   async () => {
//     const response = await client.load({
//       dimensions: [
//         'Products.category',
//         'Products.subCategory',
//         'Products.productName'
//       ],
//       order: {
//         'Products.category': 'asc',
//         'Products.subCategory': 'asc',
//         'Products.productName': 'asc'
//       }
//     });
//     expect(response.rawData()).toMatchSnapshot('query');
//   }
// );
// test(
//   'querying Products: dimentions + order + limit',
//   async () => {
//     const response = await client.load({
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
//       limit: 10
//     });
//     expect(response.rawData()).toMatchSnapshot('query');
//     expect(response.rawData().length).toEqual(10);
//   }
// );
// test(
//   'querying Products: dimentions + order + total',
//   async () => {
//     const response = await client.load({
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
//       total: true
//     });
//     expect(response.rawData()).toMatchSnapshot('query');
//     expect(
//       response.serialize().loadResponse.results[0].total
//     ).toEqual(28);
//   }
// );
// test(
//   'querying Products: dimentions + order + limit + total',
//   async () => {
//     const response = await client.load({
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
//       limit: 10,
//       total: true
//     });
//     expect(response.rawData()).toMatchSnapshot('query');
//     expect(response.rawData().length).toEqual(10);
//     expect(
//       response.serialize().loadResponse.results[0].total
//     ).toEqual(28);
//   },
// );

// // filtering Products cube
// test(
//   'filtering Products: contains + dimentions + order',
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
//           operator: 'contains',
//           values: ['able'],
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
//           operator: 'contains',
//           values: ['able', 'urn'],
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

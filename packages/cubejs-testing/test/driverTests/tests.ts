// eslint-disable-next-line import/no-extraneous-dependencies
import { expect } from '@jest/globals';
import { driverTest, driverTestWithError } from './driverTest';

const commonSchemas = [
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

export const filteringProductsContainsAndDimensionsAndOrderThird = driverTest({
  name: 'filtering Products: contains + dimentions + order, third',
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
        values: ['notexist'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringProductsStartsWithFilterDimensionsOrderFirst = driverTest({
  name: 'filtering Products: startsWith filter + dimentions + order, first',
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
        member: 'Products.productName',
        operator: 'startsWith',
        values: ['O'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringProductsStartsWithFilterDimensionsSecond = driverTest({
  name: 'filtering Products: startsWith filter + dimentions + order, second',
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
        member: 'Products.productName',
        operator: 'startsWith',
        values: ['O', 'K'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringProductsStartsWithFilterDimensionsThird = driverTest({
  name: 'filtering Products: startsWith filter + dimentions + order, third',
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
        member: 'Products.productName',
        operator: 'startsWith',
        values: ['noneexist'],
      },
    ],
  },
  schemas: commonSchemas,
});

export const filteringProductsEndsWithFilterDimensionsFirst = driverTest({
  name: 'filtering Products: endsWith filter + dimentions + order, first',
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
        operator: 'endsWith',
        values: ['es'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringProductsEndsWithFilterDimensionsSecond = driverTest({
  name: 'filtering Products: endsWith filter + dimentions + order, second',
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
        operator: 'endsWith',
        values: ['es', 'gs'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringProductsEndsWithFilterDimensionsThird = driverTest({
  name: 'filtering Products: endsWith filter + dimentions + order, third',
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
        operator: 'endsWith',
        values: ['noneexist'],
      },
    ],
  },
  schemas: commonSchemas
});

export const queryingECommerceDimensions = driverTest({
  name: 'querying ECommerce: dimensions',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ]
  },
  schemas: commonSchemas
});

export const queryingECommerceDimensionsOrder = driverTest({
  name: 'querying ECommerce: dimentions + order',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    order: {
      'ECommerce.rowId': 'asc'
    }
  },
  schemas: commonSchemas
});

export const queryingECommerceDimensionsLimit = driverTest({
  name: 'querying ECommerce: dimentions + limit',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    limit: 10
  },
  expectArray: [r => expect(r.rawData().length).toEqual(10)],
  schemas: commonSchemas
});

export const queryingECommerceDimensionsTotal = driverTest({
  name: 'querying ECommerce: dimentions + total',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    total: true
  },
  expectArray: [(r) => expect(
    r.serialize().loadResponse.results[0].total
  ).toEqual(44)],
  schemas: commonSchemas,
});

export const queryingECommerceDimensionsOrderLimitTotal = driverTest({
  name: 'querying ECommerce: dimentions + order + limit + total',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    order: {
      'ECommerce.rowId': 'asc'
    },
    limit: 10,
    total: true
  },
  expectArray: [r => expect(r.rawData().length).toEqual(10), r => expect(
    r.serialize().loadResponse.results[0].total
  ).toEqual(44)],
  schemas: commonSchemas
});

export const queryingECommerceDimensionsOrderTotalOffset = driverTest({
  name: 'querying ECommerce: dimentions + order + total + offset',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    order: {
      'ECommerce.rowId': 'asc'
    },
    total: true,
    offset: 43
  },
  expectArray: [
    r => expect(r.rawData().length).toEqual(1),
    r => expect(
      r.serialize().loadResponse.results[0].total
    ).toEqual(44)
  ],
  schemas: commonSchemas
});

export const queryingECommerceDimensionsOrderLimitTotalOffset = driverTest({
  name: 'querying ECommerce: dimentions + order + limit + total + offset',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    order: {
      'ECommerce.rowId': 'asc'
    },
    limit: 10,
    total: true,
    offset: 10
  },
  expectArray: [
    r => expect(r.rawData().length).toEqual(10),
    r => expect(
      r.serialize().loadResponse.results[0].total
    ).toEqual(44)
  ],
  schemas: commonSchemas
});

export const queryingECommerceCountByCitiesOrder = driverTest({
  name: 'querying ECommerce: count by cities + order',
  query: {
    dimensions: [
      'ECommerce.city'
    ],
    measures: [
      'ECommerce.count'
    ],
    order: {
      'ECommerce.count': 'desc',
      'ECommerce.city': 'asc',
    },
  },
  schemas: commonSchemas
});

export const queryingECommerceTotalQuantityAvgDiscountTotalSales = driverTest({
  name: 'querying ECommerce: total quantity, avg discount, total sales, ' +
    'total profit by product + order + total -- rounding in athena',
  query: {
    dimensions: [
      'ECommerce.productName'
    ],
    measures: [
      'ECommerce.totalQuantity',
      'ECommerce.avgDiscount',
      'ECommerce.totalSales',
      'ECommerce.totalProfit'
    ],
    order: {
      'ECommerce.totalProfit': 'desc',
      'ECommerce.productName': 'asc'
    },
    total: true
  },
  skip: true,
  schemas: commonSchemas
});

export const queryingECommerceTotalSalesTotalProfitByMonthAndOrder = driverTest({
  name: 'querying ECommerce: total sales, total profit by month + order ' +
  '(date) + total -- doesn\'t work with the BigQuery',
  query: {
    timeDimensions: [{
      dimension: 'ECommerce.orderDate',
      granularity: 'month'
    }],
    measures: [
      'ECommerce.totalSales',
      'ECommerce.totalProfit'
    ],
    order: {
      'ECommerce.orderDate': 'asc'
    },
    total: true
  },
  schemas: commonSchemas,
  skip: true,
});

export const filteringECommerceContainsDimensionsFirst = driverTest({
  name: 'filtering ECommerce: contains dimensions, first',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    filters: [
      {
        member: 'ECommerce.subCategory',
        operator: 'contains',
        values: ['able'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringECommerceContainsDimensionsSecond = driverTest({
  name: 'filtering ECommerce: contains dimensions, second',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    filters: [
      {
        member: 'ECommerce.subCategory',
        operator: 'contains',
        values: ['able', 'urn'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringECommerceContainsDimensionsThird = driverTest({
  name: 'filtering ECommerce: contains dimensions, third',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    filters: [
      {
        member: 'ECommerce.subCategory',
        operator: 'contains',
        values: ['notexist'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringECommerceStartsWithDimensionsFirst = driverTest({
  name: 'filtering ECommerce: startsWith + dimensions, first',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    filters: [
      {
        member: 'ECommerce.customerId',
        operator: 'startsWith',
        values: ['A'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringECommerceStartsWithDimensionsSecond = driverTest({
  name: 'filtering ECommerce: startsWith + dimensions, second',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    filters: [
      {
        member: 'ECommerce.customerId',
        operator: 'startsWith',
        values: ['A', 'B'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringECommerceStartsWithDimensionsThird = driverTest({
  name: 'filtering ECommerce: startsWith + dimensions, third',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    filters: [
      {
        member: 'ECommerce.customerId',
        operator: 'startsWith',
        values: ['Z'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringECommerceEndsWithDimensionsFirst = driverTest({
  name: 'filtering ECommerce: endsWith + dimensions, first',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    filters: [
      {
        member: 'ECommerce.orderId',
        operator: 'endsWith',
        values: ['0'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringECommerceEndsWithDimensionsSecond = driverTest({
  name: 'filtering ECommerce: endsWith + dimensions, second',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    filters: [
      {
        member: 'ECommerce.orderId',
        operator: 'endsWith',
        values: ['1', '2'],
      },
    ],
  },
  schemas: commonSchemas
});

export const filteringECommerceEndsWithDimensionsThird = driverTest({
  name: 'filtering ECommerce: endsWith + dimensions, third',
  query: {
    dimensions: [
      'ECommerce.rowId',
      'ECommerce.orderId',
      'ECommerce.orderDate',
      'ECommerce.customerId',
      'ECommerce.customerName',
      'ECommerce.city',
      'ECommerce.category',
      'ECommerce.subCategory',
      'ECommerce.productName',
      'ECommerce.sales',
      'ECommerce.quantity',
      'ECommerce.discount',
      'ECommerce.profit'
    ],
    filters: [
      {
        member: 'ECommerce.orderId',
        operator: 'endsWith',
        values: ['Z'],
      },
    ],
  },
  schemas: commonSchemas
});

export const queryingEcommerceTotalQuantifyAvgDiscountTotal = driverTestWithError({
  name: 'querying ECommerce: total quantity, avg discount, total ' +
  'sales, total profit by product + order + total -- noisy ' +
  'test',
  query: {
    dimensions: [
      'ECommerce.productName'
    ],
    measures: [
      'ECommerce.totalQuantity',
      'ECommerce.avgDiscount',
      'ECommerce.totalSales',
      'ECommerce.totalProfit'
    ],
    order: {
      'ECommerce.totalProfit': 'desc',
      'ECommerce.productName': 'asc'
    },
    total: true
  },
  expectArray: [(e) => expect(e).toEqual('error')],
  schemas: commonSchemas,
  skip: true
});

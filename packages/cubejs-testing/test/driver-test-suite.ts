// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { CubejsApi } from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { BirdBox, getBirdbox } from '../src';

export function executeTestSuiteFor(type: string) {
  describe(`${type} driver tests`, () => {
    describe(`using ${type} for the pre-aggregations`, () => {
      jest.setTimeout(60 * 5 * 1000);
      let box: BirdBox;
      let client: CubejsApi;
      let transport: WebSocketTransport;

      beforeAll(async () => {
        box = await getBirdbox(type, {
          CUBEJS_DEV_MODE: 'true',
          CUBEJS_WEB_SOCKETS: 'true',
          CUBEJS_EXTERNAL_DEFAULT: 'false',
          CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'true',
          CUBEJS_REFRESH_WORKER: 'true',
          CUBEJS_ROLLUP_ONLY: 'false',
        });
        transport = new WebSocketTransport({
          apiUrl: box.configuration.apiUrl,
        });
        client = cubejs(async () => 'test', {
          apiUrl: box.configuration.apiUrl,
          // transport,
        });
      });
      afterAll(async () => {
        await transport.close();
        await box.stop();
      });

      // querying Customers cube
      test(
        'querying Customers: dimensions',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ]
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Customers: dimentions + order',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ],
            order: {
              'Customers.customerId': 'asc',
            }
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Customers: dimentions + limit',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ],
            limit: 10
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Customers: dimentions + total',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ],
            total: true
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Customers: dimentions + order + limit + total',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ],
            order: {
              'Customers.customerName': 'asc'
            },
            limit: 10,
            total: true
          });
          expect(response.rawData()).toMatchSnapshot('query');
        },
      );
      
      // querying Products cube
      test.skip(
        'querying Products: dimensions -- doesn\'t work wo ordering',
        async () => {
          const response = await client.load({
            dimensions: [
              'Products.category',
              'Products.subCategory',
              'Products.productName'
            ]
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Products: dimentions + order',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Products: dimentions + order + limit',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Products: dimentions + order + total',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Products: dimentions + order + limit + total',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        },
      );
      
      // querying ECommerce cube
      test(
        'querying ECommerce: dimensions',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: dimentions + order',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: dimentions + limit',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: dimentions + total',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: dimentions + order + limit + total',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: count by cities + order',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: total quantity, avg discount, total sales, ' +
        'total profit by product + order + total',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test.skip(
        'querying ECommerce: total sales, total profit by month + order ' +
        '(date) + total -- doesn\'t work with the BigQuery',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
    });
    describe('using cubestore for the pre-aggregations', () => {
      jest.setTimeout(60 * 5 * 1000);
      let box: BirdBox;
      let client: CubejsApi;
      let transport: WebSocketTransport;

      beforeAll(async () => {
        box = await getBirdbox(type, {
          CUBEJS_DEV_MODE: 'true',
          CUBEJS_WEB_SOCKETS: 'true',
          CUBEJS_EXTERNAL_DEFAULT: 'true',
          CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'true',
          CUBEJS_REFRESH_WORKER: 'true',
          CUBEJS_ROLLUP_ONLY: 'false',
        });
        transport = new WebSocketTransport({
          apiUrl: box.configuration.apiUrl,
        });
        client = cubejs(async () => 'test', {
          apiUrl: box.configuration.apiUrl,
          // transport,
        });
      });
      afterAll(async () => {
        await transport.close();
        await box.stop();
      });

      // querying Customers cube
      test(
        'querying Customers: dimensions',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ]
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Customers: dimentions + order',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ],
            order: {
              'Customers.customerId': 'asc',
            }
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Customers: dimentions + limit',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ],
            limit: 10
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Customers: dimentions + total',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ],
            total: true
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Customers: dimentions + order + limit + total',
        async () => {
          const response = await client.load({
            dimensions: [
              'Customers.customerId',
              'Customers.customerName'
            ],
            order: {
              'Customers.customerName': 'asc'
            },
            limit: 10,
            total: true
          });
          expect(response.rawData()).toMatchSnapshot('query');
        },
      );
      
      // querying Products cube
      test.skip(
        'querying Products: dimensions -- doesn\'t work wo ordering',
        async () => {
          const response = await client.load({
            dimensions: [
              'Products.category',
              'Products.subCategory',
              'Products.productName'
            ]
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Products: dimentions + order',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Products: dimentions + order + limit',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Products: dimentions + order + total',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying Products: dimentions + order + limit + total',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        },
      );
      
      // querying ECommerce cube
      test(
        'querying ECommerce: dimensions',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: dimentions + order',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: dimentions + limit',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: dimentions + total',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: dimentions + order + limit + total',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: count by cities + order',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'querying ECommerce: total quantity, avg discount, total sales, ' +
        'total profit by product + order + total',
        async () => {
          let err;
          try {
            await client.load({
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
            });
          } catch (e) {
            err = 'error';
          }
          expect(err).toEqual('error');
        }
      );
      test.skip(
        'querying ECommerce: total sales, total profit by month + order ' +
        '(date) + total -- doesn\'t work with the BigQuery',
        async () => {
          const response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
    });
  });
}

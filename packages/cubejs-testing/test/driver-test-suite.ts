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
            ],
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
          expect(response.rawData().length).toEqual(10);
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
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(41);
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
          expect(response.rawData().length).toEqual(10);
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(41);
        },
      );

      // filtering Customers cube
      test(
        'filtering Customers: contains + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering Customers: startsWith + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering Customers: endsWith filter + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
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
          expect(response.rawData().length).toEqual(10);
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
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(28);
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
          expect(response.rawData().length).toEqual(10);
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(28);
        },
      );

      // filtering Products cube
      test(
        'filtering Products: contains + dimentions + order',
        async () => {
          let response;
          
          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering Products: startsWith filter + dimentions + order',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering Products: endsWith filter + dimentions + order',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
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
          expect(response.rawData().length).toEqual(10);
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
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(44);
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
          expect(response.rawData().length).toEqual(10);
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(44);
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
      test.skip(
        'querying ECommerce: total quantity, avg discount, total sales, ' +
        'total profit by product + order + total -- rounding in athena',
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

      // filtering ECommerce cube
      test(
        'filtering ECommerce: contains dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering ECommerce: startsWith + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering ECommerce: endsWith + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          expect(response.rawData().length).toEqual(10);
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
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(41);
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
          expect(response.rawData().length).toEqual(10);
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(41);
        },
      );

      // filtering Customers cube
      test(
        'filtering Customers: contains + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering Customers: startsWith + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering Customers: endsWith filter + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
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
          expect(response.rawData().length).toEqual(10);
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
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(28);
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
          expect(response.rawData().length).toEqual(10);
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(28);
        },
      );

      // filtering Products cube
      test(
        'filtering Products: contains + dimentions + order',
        async () => {
          let response;
          
          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering Products: startsWith filter + dimentions + order',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering Products: endsWith filter + dimentions + order',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
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
          expect(response.rawData().length).toEqual(10);
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
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(44);
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
          expect(response.rawData().length).toEqual(10);
          expect(
            response.serialize().loadResponse.results[0].total
          ).toEqual(44);
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

      // filtering ECommerce cube
      test(
        'filtering ECommerce: contains dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering ECommerce: startsWith + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
      test(
        'filtering ECommerce: endsWith + dimensions',
        async () => {
          let response;

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');

          response = await client.load({
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
          });
          expect(response.rawData()).toMatchSnapshot('query');
        }
      );
    });
  });
}

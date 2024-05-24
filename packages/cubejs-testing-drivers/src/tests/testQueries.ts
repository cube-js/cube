import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import { randomBytes } from 'crypto';
import { Client as PgClient } from 'pg';
import { BaseDriver } from '@cubejs-backend/base-driver';
import cubejs, { CubeApi } from '@cubejs-client/core';
import { sign } from 'jsonwebtoken';
import { Environment } from '../types/Environment';
import {
  getFixtures,
  getCreateQueries,
  getDriver,
  runEnvironment,
  buildPreaggs,
} from '../helpers';
import { incrementalSchemaLoadingSuite } from './testIncrementalSchemaLoading';

type TestQueriesOptions = {
  includeIncrementalSchemaSuite?: boolean,
  includeHLLSuite?: boolean,
  extendedEnv?: string
};

export function testQueries(type: string, { includeIncrementalSchemaSuite, extendedEnv, includeHLLSuite }: TestQueriesOptions = {}): void {
  describe(`Queries with the @cubejs-backend/${type}-driver${extendedEnv ? ` ${extendedEnv}` : ''}`, () => {
    jest.setTimeout(60 * 5 * 1000);

    const fixtures = getFixtures(type, extendedEnv);
    let client: CubeApi;
    let driver: BaseDriver;
    let queries: string[];
    let env: Environment;

    let connectionId = 0;

    async function createPostgresClient(user: string = 'admin', password: string = 'admin_password', pgPort: number | undefined = env.cube.pgPort) {
      if (!pgPort) {
        throw new Error('port must be defined');
      }

      connectionId++;
      const currentConnId = connectionId;

      console.debug(`[pg] new connection ${currentConnId}`);

      const conn = new PgClient({
        database: 'db',
        port: pgPort,
        host: '127.0.0.1',
        user,
        password,
        ssl: false,
      });
      conn.on('error', (err) => {
        console.log(`[pg] #${currentConnId}`, err);
      });
      conn.on('end', () => {
        console.debug(`[pg] #${currentConnId} end`);
      });

      await conn.connect();

      return conn;
    }

    function execute(name: string, test: () => Promise<void>) {
      if (fixtures.skip && fixtures.skip.indexOf(name) >= 0) {
        it.skip(name, test);
      } else {
        it(name, test);
      }
    }

    function executePg(name: string, test: () => Promise<void>) {
      if (!fixtures.cube.ports[1] || fixtures.skip && fixtures.skip.indexOf(name) >= 0) {
        it.skip(name, test);
      } else {
        it(name, test);
      }
    }

    const apiToken = sign({}, 'mysupersecret');

    const suffix = randomBytes(8).toString('hex');
    const tables = Object
      .keys(fixtures.tables)
      .map((key: string) => `${fixtures.tables[key]}_${suffix}`);

    beforeAll(async () => {
      env = await runEnvironment(type, suffix, { extendedEnv });
      process.env.CUBEJS_REFRESH_WORKER = 'true';
      process.env.CUBEJS_CUBESTORE_HOST = '127.0.0.1';
      process.env.CUBEJS_CUBESTORE_PORT = `${env.store.port}`;
      process.env.CUBEJS_CUBESTORE_USER = 'root';
      process.env.CUBEJS_CUBESTORE_PASS = 'root';
      process.env.CUBEJS_CACHE_AND_QUEUE_DRIVER = 'cubestore'; // memory
      if (env.data) {
        process.env.CUBEJS_DB_HOST = '127.0.0.1';
        process.env.CUBEJS_DB_PORT = `${env.data.port}`;
      }
      client = cubejs(apiToken, {
        apiUrl: `http://127.0.0.1:${env.cube.port}/cubejs-api/v1`,
      });
      driver = (await getDriver(type)).source;
      queries = getCreateQueries(type, suffix);
      console.log(`Creating ${queries.length} fixture tables`);
      try {
        for (const q of queries) {
          await driver.query(q);
        }
        console.log(`Creating ${queries.length} fixture tables completed`);
      } catch (e: any) {
        console.log('Error creating fixtures', e.stack);
        throw e;
      }
    });
  
    afterAll(async () => {
      try {
        console.log(`Dropping ${tables.length} fixture tables`);
        for (const t of tables) {
          await driver.dropTable(t);
        }
        console.log(`Dropping ${tables.length} fixture tables completed`);
      } finally {
        await driver.release();
        await env.stop();
      }
    });

    // MUST be the first test in the list!
    execute('must built pre-aggregations', async () => {
      await buildPreaggs(env.cube.port, apiToken, {
        timezones: ['UTC'],
        preAggregations: ['Customers.RAExternal'],
        contexts: [{ securityContext: { tenant: 't1' } }],
      });

      await buildPreaggs(env.cube.port, apiToken, {
        timezones: ['UTC'],
        preAggregations: ['ECommerce.SAExternal'],
        contexts: [{ securityContext: { tenant: 't1' } }],
      });
      
      await buildPreaggs(env.cube.port, apiToken, {
        timezones: ['UTC'],
        preAggregations: ['ECommerce.TAExternal'],
        contexts: [{ securityContext: { tenant: 't1' } }],
      });

      await buildPreaggs(env.cube.port, apiToken, {
        timezones: ['UTC'],
        preAggregations: ['BigECommerce.TAExternal'],
        contexts: [{ securityContext: { tenant: 't1' } }],
      });

      await buildPreaggs(env.cube.port, apiToken, {
        timezones: ['UTC'],
        preAggregations: ['BigECommerce.MultiTimeDimForCountExternal'],
        contexts: [{ securityContext: { tenant: 't1' } }],
      });

      if (includeHLLSuite) {
        await buildPreaggs(env.cube.port, apiToken, {
          timezones: ['UTC'],
          preAggregations: ['BigECommerce.CountByProductExternal'],
          contexts: [{ securityContext: { tenant: 't1' } }],
        });
      }
    });

    execute('must not fetch a hidden cube', async () => {
      const meta = await client.meta();
      expect(meta.cubes.find(cube => cube.name === 'HiddenECommerce')).toBe(undefined);
    });

    execute('must throw if a hidden member was requested', async () => {
      const promise = async () => {
        await client.load({
          measures: [
            'ECommerce.hiddenSum'
          ]
        });
      };
      promise().catch(e => {
        expect(e.toString()).toMatch(/hidden/);
      });
    });

    execute('querying Customers: dimensions', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying Customers: dimentions + order', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        order: {
          'Customers.customerId': 'asc',
        }
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying Customers: dimentions + limit', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        limit: 10
      });
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(10);
    });

    execute('querying Customers: dimentions + total', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        total: true
      });
      expect(response.rawData()).toMatchSnapshot();
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(41);
    });

    execute('querying Customers: dimentions + order + limit + total', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(10);
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(41);
    });

    execute('querying Customers: dimentions + order + total + offset', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        order: {
          'Customers.customerName': 'asc'
        },
        total: true,
        offset: 40
      });
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(1);
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(41);
    });

    execute('querying Customers: dimentions + order + limit + total + offset', async () => {
      const response = await client.load({
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
      });
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(10);
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(41);
    });

    execute('filtering Customers: contains + dimensions, first', async () => {
      const response = await client.load({
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: contains + dimensions, second', async () => {
      const response = await client.load({
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: contains + dimensions, third', async () => {
      const response = await client.load({
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: endsWith filter + dimensions, first', async () => {
      const response = await client.load({
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: endsWith filter + dimensions, second', async () => {
      const response = await client.load({
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: endsWith filter + dimensions, third', async () => {
      const response = await client.load({
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: notEndsWith filter + dimensions, first', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        filters: [
          {
            member: 'Customers.customerId',
            operator: 'notEndsWith',
            values: ['0'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: notEndsWith filter + dimensions, second', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        filters: [
          {
            member: 'Customers.customerId',
            operator: 'notEndsWith',
            values: ['0', '5'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: notEndsWith filter + dimensions, third', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        filters: [
          {
            member: 'Customers.customerId',
            operator: 'notEndsWith',
            values: ['9'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: startsWith + dimensions, first', async () => {
      const response = await client.load({
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: startsWith + dimensions, second', async () => {
      const response = await client.load({
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: startsWith + dimensions, third', async () => {
      const response = await client.load({
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: notStartsWith + dimensions, first', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        filters: [
          {
            member: 'Customers.customerId',
            operator: 'notStartsWith',
            values: ['A'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: notStartsWith + dimensions, second', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        filters: [
          {
            member: 'Customers.customerId',
            operator: 'notStartsWith',
            values: ['A', 'B'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Customers: notStartsWith + dimensions, third', async () => {
      const response = await client.load({
        dimensions: [
          'Customers.customerId',
          'Customers.customerName'
        ],
        filters: [
          {
            member: 'Customers.customerId',
            operator: 'notStartsWith',
            values: ['Z'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying Products: dimensions -- doesn\'t work wo ordering', async () => {
      const response = await client.load({
        dimensions: [
          'Products.category',
          'Products.subCategory',
          'Products.productName'
        ]
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying Products: dimentions + order', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying Products: dimentions + order + limit', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(10);
    });

    execute('querying Products: dimentions + order + total', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(28);
    });

    execute('querying Products: dimentions + order + limit + total', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(10);
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(28);
    });

    execute('filtering Products: contains + dimentions + order, first', async () => {
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
        filters: [
          {
            member: 'Products.subCategory',
            operator: 'contains',
            values: ['able'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Products: contains + dimentions + order, second', async () => {
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
        filters: [
          {
            member: 'Products.subCategory',
            operator: 'contains',
            values: ['able', 'urn'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Products: contains + dimentions + order, third', async () => {
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
        filters: [
          {
            member: 'Products.subCategory',
            operator: 'contains',
            values: ['notexist'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Products: startsWith filter + dimentions + order, first', async () => {
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
        filters: [
          {
            member: 'Products.productName',
            operator: 'startsWith',
            values: ['O'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Products: startsWith filter + dimentions + order, second', async () => {
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
        filters: [
          {
            member: 'Products.productName',
            operator: 'startsWith',
            values: ['O', 'K'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Products: startsWith filter + dimentions + order, third', async () => {
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
        filters: [
          {
            member: 'Products.productName',
            operator: 'startsWith',
            values: ['noneexist'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Products: endsWith filter + dimentions + order, first', async () => {
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
        filters: [
          {
            member: 'Products.subCategory',
            operator: 'endsWith',
            values: ['es'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Products: endsWith filter + dimentions + order, second', async () => {
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
        filters: [
          {
            member: 'Products.subCategory',
            operator: 'endsWith',
            values: ['es', 'gs'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering Products: endsWith filter + dimentions + order, third', async () => {
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
        filters: [
          {
            member: 'Products.subCategory',
            operator: 'endsWith',
            values: ['noneexist'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying ECommerce: dimensions', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying ECommerce: dimentions + order', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying ECommerce: dimentions + limit', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(10);
    });

    execute('querying ECommerce: dimentions + total', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(44);
    });

    execute('querying ECommerce: dimentions + order + limit + total', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(10);
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(44);
    });

    execute('querying ECommerce: dimentions + order + total + offset', async () => {
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
        total: true,
        offset: 43
      });
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(1);
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(44);
    });

    execute('querying ECommerce: dimentions + order + limit + total + offset', async () => {
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
        total: true,
        offset: 10
      });
      expect(response.rawData()).toMatchSnapshot();
      expect(response.rawData().length).toEqual(10);
      expect(
        response.serialize().loadResponse.results[0].total
      ).toEqual(44);
    });

    execute('querying ECommerce: count by cities + order', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying ECommerce: total quantity, avg discount, total sales, total profit by product + order + total -- rounding in athena', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying ECommerce: total sales, total profit by month + order (date) + total -- doesn\'t work with the BigQuery', async () => {
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
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering ECommerce: contains dimensions, first', async () => {
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
        filters: [
          {
            member: 'ECommerce.subCategory',
            operator: 'contains',
            values: ['able'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering ECommerce: contains dimensions, second', async () => {
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
        filters: [
          {
            member: 'ECommerce.subCategory',
            operator: 'contains',
            values: ['able', 'urn'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering ECommerce: contains dimensions, third', async () => {
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
        filters: [
          {
            member: 'ECommerce.subCategory',
            operator: 'contains',
            values: ['notexist'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering ECommerce: startsWith + dimensions, first', async () => {
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
        filters: [
          {
            member: 'ECommerce.customerId',
            operator: 'startsWith',
            values: ['A'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering ECommerce: startsWith + dimensions, second', async () => {
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
        filters: [
          {
            member: 'ECommerce.customerId',
            operator: 'startsWith',
            values: ['A', 'B'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering ECommerce: startsWith + dimensions, third', async () => {
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
        filters: [
          {
            member: 'ECommerce.customerId',
            operator: 'startsWith',
            values: ['Z'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering ECommerce: endsWith + dimensions, first', async () => {
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
        filters: [
          {
            member: 'ECommerce.orderId',
            operator: 'endsWith',
            values: ['0'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering ECommerce: endsWith + dimensions, second', async () => {
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
        filters: [
          {
            member: 'ECommerce.orderId',
            operator: 'endsWith',
            values: ['1', '2'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('filtering ECommerce: endsWith + dimensions, third', async () => {
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
        filters: [
          {
            member: 'ECommerce.orderId',
            operator: 'endsWith',
            values: ['Z'],
          },
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('pre-aggregations Customers: running total without time dimension', async () => {
      const response = await client.load({
        measures: [
          'Customers.runningTotal'
        ]
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying ECommerce: total quantity, avg discount, total sales, total profit by product + order + total -- noisy test', async () => {
      const promise = async () => {
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
      };
      promise().catch(e => {
        expect(e.toString()).toMatch(/error/);
      });
    });

    execute('querying ECommerce: partitioned pre-agg', async () => {
      const response = await client.load({
        dimensions: [
          'ECommerce.productName'
        ],
        measures: [
          'ECommerce.totalQuantity',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.orderDate',
          granularity: 'month'
        }],
        order: {
          'ECommerce.orderDate': 'asc',
          'ECommerce.totalProfit': 'desc',
          'ECommerce.productName': 'asc'
        },
        total: true
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying ECommerce: partitioned pre-agg higher granularity', async () => {
      const response = await client.load({
        dimensions: [
          'ECommerce.productName'
        ],
        measures: [
          'ECommerce.totalQuantity',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.orderDate',
          granularity: 'year'
        }],
        order: {
          'ECommerce.orderDate': 'asc',
          'ECommerce.totalProfit': 'desc',
          'ECommerce.productName': 'asc'
        },
        total: true
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: partitioned pre-agg with multi time dimension', async () => {
      const response = await client.load({
        dimensions: [],
        measures: [
          'BigECommerce.count',
        ],
        timeDimensions: [
          {
            dimension: 'BigECommerce.completedDate',
            granularity: 'day'
          },
          {
            dimension: 'BigECommerce.orderDate',
            granularity: 'day'
          }
        ],
        order: {
          'BigECommerce.completedDate': 'asc',
          'BigECommerce.orderDate': 'asc',
          'BigECommerce.count': 'asc'
        }
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: partitioned pre-agg', async () => {
      const response = await client.load({
        dimensions: [
          'BigECommerce.productName'
        ],
        measures: [
          'BigECommerce.totalQuantity',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month'
        }],
        order: {
          'BigECommerce.orderDate': 'asc',
          'BigECommerce.totalProfit': 'desc',
          'BigECommerce.productName': 'asc'
        }
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: null sum', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.totalSales',
        ],
        filters: [{
          member: 'BigECommerce.id',
          operator: 'equals',
          values: ['8958']
        }]
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: null boolean', async () => {
      const response = await client.load({
        dimensions: [
          'BigECommerce.returning',
        ],
        filters: [{
          member: 'BigECommerce.id',
          operator: 'equals',
          values: ['8958']
        }]
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window by 2 day', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountBy2Day',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window by 2 week', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountBy2Week',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window by 2 month', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountBy2Month',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: totalProfitYearAgo', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.totalProfitYearAgo',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    if (includeIncrementalSchemaSuite) {
      incrementalSchemaLoadingSuite(execute, () => driver, tables);
    }

    executePg('SQL API: powerbi min max push down', async () => {
      const connection = await createPostgresClient();
      const res = await connection.query(`
      select
  max("rows"."orderDate") as "a0",
  min("rows"."orderDate") as "a1"
from
  (
    select
      "orderDate"
    from
      "public"."ECommerce" "$Table"
  ) "rows"
  `);
      expect(res.rows).toMatchSnapshot('powerbi_min_max_push_down');
    });

    executePg('SQL API: powerbi min max ungrouped flag', async () => {
      const connection = await createPostgresClient();
      const res = await connection.query(`
      select 
  count(distinct("rows"."totalSales")) + max( 
    case 
      when "rows"."totalSales" is null then 1 
      else 0 
    end 
  ) as "a0", 
  min("rows"."totalSales") as "a1", 
  max("rows"."totalSales") as "a2" 
from 
  ( 
    select 
      "totalSales" 
    from 
      "public"."ECommerce" "$Table" 
  ) "rows" 
  `);
      expect(res.rows).toMatchSnapshot('powerbi_min_max_ungrouped_flag');
    });

    executePg('SQL API: ungrouped pre-agg', async () => {
      const connection = await createPostgresClient();
      const res = await connection.query(`
    select
      "productName",
      "totalSales" 
    from 
      "public"."BigECommerce" "$Table" 
    order by 2 desc, 1 asc
  `);
      expect(res.rows).toMatchSnapshot('ungrouped_pre_agg');
    });

    executePg('SQL API: post-aggregate percentage of total', async () => {
      const connection = await createPostgresClient();
      const res = await connection.query(`
    select
      sum("BigECommerce"."percentageOfTotalForStatus")
    from 
      "public"."BigECommerce" "BigECommerce"
  `);
      expect(res.rows).toMatchSnapshot('post_aggregate_percentage_of_total');
    });

    executePg('SQL API: reuse params', async () => {
      const connection = await createPostgresClient();
      const res = await connection.query(`
    select
      date_trunc('year', "orderDate") as "c0",
      round(sum("ECommerce"."totalSales")) as "m0"
    from
      "ECommerce" as "ECommerce"
    where
      date_trunc('year', "orderDate") in (
        CAST('2019-01-01 00:00:00.0' AS timestamp),
        CAST('2020-01-01 00:00:00.0' AS timestamp),
        CAST('2021-01-01 00:00:00.0' AS timestamp),
        CAST('2022-01-01 00:00:00.0' AS timestamp),
        CAST('2023-01-01 00:00:00.0' AS timestamp)
      )
    group by
      date_trunc('year', "orderDate")
  `);
      expect(res.rows).toMatchSnapshot('reuse_params');
    });

    executePg('SQL API: Simple Rollup', async () => {
      const connection = await createPostgresClient();
      const res = await connection.query(`
    select
        rowId, orderId, orderDate, sum(count)
    from
      "ECommerce" as "ECommerce"
    group by
      ROLLUP(1, 2, 3)
    order by 1, 2, 3

  `);
      expect(res.rows).toMatchSnapshot('simple_rollup');
    });

    executePg('SQL API: Complex Rollup', async () => {
      const connection = await createPostgresClient();
      const res = await connection.query(`
    select
        rowId, orderId, orderDate, city, sum(count)
    from
      "ECommerce" as "ECommerce"
    group by
      ROLLUP(1, 2), 3, ROLLUP(4) 
    order by 1, 2, 3, 4

  `);
      expect(res.rows).toMatchSnapshot('complex_rollup');
    });
    executePg('SQL API: Nested Rollup', async () => {
      const connection = await createPostgresClient();
      const res = await connection.query(`
    select rowId, orderId, orderDate, sum(cnt)
    from (
        select
            rowId, orderId, orderDate, sum(count) as cnt
        from
        "ECommerce" as "ECommerce"
        group by 1, 2, 3

    ) a
    group by
      ROLLUP(1, 2, 3)
    order by 1, 2, 3

  `);
      expect(res.rows).toMatchSnapshot('nested_rollup');
    });
  });
}

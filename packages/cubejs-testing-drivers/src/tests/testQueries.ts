import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import { randomBytes } from 'crypto';
import { get } from 'env-var';
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
import { redshiftExternalSchemasSuite } from './testExternalSchemas';

type TestQueriesOptions = {
  includeIncrementalSchemaSuite?: boolean,
  includeHLLSuite?: boolean,
  extendedEnv?: string
  externalSchemaTests?: boolean,
};

export function testQueries(type: string, { includeIncrementalSchemaSuite, extendedEnv, includeHLLSuite, externalSchemaTests }: TestQueriesOptions = {}): void {
  describe(`Queries with the @cubejs-backend/${type}-driver${extendedEnv ? ` ${extendedEnv}` : ''}`, () => {
    jest.setTimeout(60 * 5 * 1000);

    const isTesseractEnv = get('DRIVERS_TESTS_CUBEJS_TESSERACT_SQL_PLANNER').default('false').asBool();

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
      if (!isTesseractEnv && fixtures.skip && fixtures.skip.indexOf(name) >= 0) {
        it.skip(name, test);
      } else if (isTesseractEnv && fixtures.tesseractSkip && fixtures.tesseractSkip.indexOf(name) >= 0) {
        it.skip(name, test);
      } else {
        it(name, test);
      }
    }

    function executePg(name: string, test: (connection: PgClient) => Promise<void>) {
      if (!isTesseractEnv && fixtures.skip && fixtures.skip.indexOf(name) >= 0) {
        it.skip(name, () => {
          // nothing to do
        });
      } else if (isTesseractEnv && fixtures.tesseractSkip && fixtures.tesseractSkip.indexOf(name) >= 0) {
        it.skip(name, () => {
          // nothing to do
        });
      } else {
        it(name, async () => {
          const connection = await createPostgresClient();

          try {
            await test(connection);
          } finally {
            await connection.end();
          }
        });
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
          await driver.createTableRaw(q);
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

    execute('querying Customers: dimensions + order', async () => {
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

    execute('querying Customers: dimensions + limit', async () => {
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

    execute('querying Customers: dimensions + total', async () => {
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

    execute('querying Customers: dimensions + order + limit + total', async () => {
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

    execute('querying Customers: dimensions + order + total + offset', async () => {
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

    execute('querying Customers: dimensions + order + limit + total + offset', async () => {
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

    execute('filtering Products: contains with special chars + dimensions', async () => {
      const response = await client.load({
        dimensions: [
          'Products.productName'
        ],
        filters: [
          {
            member: 'Products.productName',
            operator: 'contains',
            values: ['di_Novo'],
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

    execute('querying Products: dimensions + order', async () => {
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

    execute('querying Products: dimensions + order + limit', async () => {
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

    execute('querying Products: dimensions + order + total', async () => {
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

    execute('querying Products: dimensions + order + limit + total', async () => {
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

    execute('filtering Products: contains + dimensions + order, first', async () => {
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

    execute('filtering Products: contains + dimensions + order, second', async () => {
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

    execute('filtering Products: contains + dimensions + order, third', async () => {
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

    execute('filtering Products: startsWith filter + dimensions + order, first', async () => {
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

    execute('filtering Products: startsWith filter + dimensions + order, second', async () => {
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

    execute('filtering Products: startsWith filter + dimensions + order, third', async () => {
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

    execute('filtering Products: endsWith filter + dimensions + order, first', async () => {
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

    execute('filtering Products: endsWith filter + dimensions + order, second', async () => {
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

    execute('filtering Products: endsWith filter + dimensions + order, third', async () => {
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

    execute('querying ECommerce: dimensions + order', async () => {
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

    execute('querying ECommerce: dimensions + limit', async () => {
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

    execute('querying ECommerce: dimensions + total', async () => {
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

    execute('querying ECommerce: dimensions + order + limit + total', async () => {
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

    execute('querying ECommerce: dimensions + order + total + offset', async () => {
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

    execute('querying ECommerce: dimensions + order + limit + total + offset', async () => {
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

    execute('querying BigECommerce: time series in rolling window', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.customersCountPrev1Month',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: {
          'BigECommerce.orderDate': 'asc',
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

    execute('querying BigECommerce: rolling window by 2 day without date range', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountBy2Day',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
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

    execute('querying BigECommerce: rolling window by 2 month without date range', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountBy2Month',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window YTD (month)', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountYTD',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [
          ['BigECommerce.orderDate', 'asc'],
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window YTD (month + week)', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountYTD',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'BigECommerce.orderDate',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [
          ['BigECommerce.orderDate', 'asc'],
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window YTD (month + week + no gran)', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountYTD',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'BigECommerce.orderDate',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'BigECommerce.orderDate',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [
          ['BigECommerce.orderDate', 'asc'],
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window YTD (month + week + day)', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountYTD',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-03-01'],
        }, {
          dimension: 'BigECommerce.orderDate',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-03-01'],
        }, {
          dimension: 'BigECommerce.orderDate',
          granularity: 'day',
          dateRange: ['2020-01-01', '2020-03-01'],
        }],
        order: [
          ['BigECommerce.orderDate', 'asc'],
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window YTD (month + week + day + no gran)', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountYTD',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-03-01'],
        }, {
          dimension: 'BigECommerce.orderDate',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-03-01'],
        }, {
          dimension: 'BigECommerce.orderDate',
          granularity: 'day',
          dateRange: ['2020-01-01', '2020-03-01'],
        }, {
          dimension: 'BigECommerce.orderDate',
          dateRange: ['2020-01-01', '2020-03-01'],
        }],
        order: [
          ['BigECommerce.orderDate', 'asc'],
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window YTD without date range', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountYTD',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce: rolling window YTD without granularity', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.rollingCountYTD',
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          dateRange: ['2020-01-01', '2020-03-01'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    if (includeHLLSuite) {
      execute('querying BigECommerce: rolling count_distinct_approx window by 2 day', async () => {
        const response = await client.load({
          measures: [
            'BigECommerce.rollingCountApproxBy2Day',
          ],
          timeDimensions: [{
            dimension: 'BigECommerce.orderDate',
            granularity: 'month',
            dateRange: ['2020-01-01', '2020-12-31'],
          }],
        });
        expect(response.rawData()).toMatchSnapshot();
      });

      execute('querying BigECommerce: rolling count_distinct_approx window by 2 week', async () => {
        const response = await client.load({
          measures: [
            'BigECommerce.rollingCountApproxBy2Week',
          ],
          timeDimensions: [{
            dimension: 'BigECommerce.orderDate',
            granularity: 'month',
            dateRange: ['2020-01-01', '2020-12-31'],
          }],
        });
        expect(response.rawData()).toMatchSnapshot();
      });

      execute('querying BigECommerce: rolling count_distinct_approx window by 2 month', async () => {
        const response = await client.load({
          measures: [
            'BigECommerce.rollingCountApproxBy2Month',
          ],
          timeDimensions: [{
            dimension: 'BigECommerce.orderDate',
            granularity: 'month',
            dateRange: ['2020-01-01', '2020-12-31'],
          }],
        });
        expect(response.rawData()).toMatchSnapshot();
      });
    }

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

    execute('querying BigECommerce: filtering with possible casts', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.totalSales',
        ],
        filters: [
          {
            values: ['10'],
            member: 'BigECommerce.sales',
            operator: 'gte'
          },
          {
            values: ['true'],
            member: 'BigECommerce.returning',
            operator: 'equals'
          }
        ],
        timeDimensions: [{
          dimension: 'BigECommerce.orderDate',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: {
          'BigECommerce.orderDate': 'asc',
        }
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce with Retail Calendar: totalCountRetailYearAgo', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.count',
          'BigECommerce.totalCountRetailYearAgo',
        ],
        timeDimensions: [{
          dimension: 'RetailCalendar.retail_date',
          granularity: 'year',
          dateRange: ['2020-02-02', '2021-02-01'],
        }],
        order: {
          'RetailCalendar.retail_date': 'asc',
        }
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce with Retail Calendar: totalCountRetailMonthAgo', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.count',
          'BigECommerce.totalCountRetailMonthAgo',
        ],
        timeDimensions: [{
          dimension: 'RetailCalendar.retail_date',
          granularity: 'month',
          dateRange: ['2020-02-02', '2021-02-01'],
        }],
        order: {
          'RetailCalendar.retail_date': 'asc',
        }
      });
      expect(response.rawData()).toMatchSnapshot();
    });
    execute('Tesseract: querying BigECommerce with Retail Calendar: totalCountRetailMonthAgo', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.count',
          'BigECommerce.totalCountRetailMonthAgo',
        ],
        timeDimensions: [{
          dimension: 'RetailCalendar.retail_date',
          granularity: 'month',
          dateRange: ['2020-02-02', '2021-02-01'],
        }],
        order: {
          'RetailCalendar.retail_date': 'asc',
        }
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying BigECommerce with Retail Calendar: totalCountRetailWeekAgo', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.count',
          'BigECommerce.totalCountRetailWeekAgo',
        ],
        timeDimensions: [{
          dimension: 'RetailCalendar.retail_date',
          granularity: 'week',
          dateRange: ['2020-02-02', '2021-03-07'],
        }],
        order: {
          'RetailCalendar.retail_date': 'asc',
        }
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('Tesseract: querying BigECommerce with Retail Calendar: totalCountRetailWeekAgo', async () => {
      const response = await client.load({
        measures: [
          'BigECommerce.count',
          'BigECommerce.totalCountRetailWeekAgo',
        ],
        timeDimensions: [{
          dimension: 'RetailCalendar.retail_date',
          granularity: 'week',
          dateRange: ['2020-02-02', '2021-03-07'],
        }],
        order: {
          'RetailCalendar.retail_date': 'asc',
        }
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by half_year + no dimension', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.count',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'half_year',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by half_year_by_1st_april + no dimension', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.count',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'half_year_by_1st_april',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by three_months_by_march + no dimension', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.count',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'three_months_by_march',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by half_year + dimension', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.count',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'half_year',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        dimensions: ['ECommerce.city'],
        order: [
          ['ECommerce.customOrderDateNoPreAgg', 'asc'],
          ['ECommerce.city', 'asc']
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by half_year_by_1st_april + dimension', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.count',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'half_year_by_1st_april',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        dimensions: ['ECommerce.city'],
        order: [
          ['ECommerce.customOrderDateNoPreAgg', 'asc'],
          ['ECommerce.city', 'asc']
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by three_months_by_march + dimension', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.count',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'three_months_by_march',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        dimensions: ['ECommerce.city'],
        order: [
          ['ECommerce.customOrderDateNoPreAgg', 'asc'],
          ['ECommerce.city', 'asc']
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by two_mo_by_feb + no dimension + rollingCountByUnbounded', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.rollingCountByUnbounded',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'two_mo_by_feb',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by two_mo_by_feb + no dimension + rollingCountByTrailing', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.rollingCountByTrailing',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'two_mo_by_feb',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by two_mo_by_feb + no dimension + rollingCountByLeading', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.rollingCountByLeading',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'two_mo_by_feb',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities ECommerce: count by two_mo_by_feb + no dimension + rollingCountByLeading without date range', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.rollingCountByLeading',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.customOrderDateNoPreAgg',
          granularity: 'two_mo_by_feb',
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities (with preaggregation) ECommerce: totalQuantity by half_year + no dimension', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.totalQuantity',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.orderDate',
          granularity: 'half_year',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    execute('querying custom granularities (with preaggregation) ECommerce: totalQuantity by half_year + dimension', async () => {
      const response = await client.load({
        measures: [
          'ECommerce.totalQuantity',
        ],
        timeDimensions: [{
          dimension: 'ECommerce.orderDate',
          granularity: 'half_year',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        dimensions: ['ECommerce.productName'],
        order: [
          ['ECommerce.orderDate', 'asc'],
          ['ECommerce.productName', 'asc']
        ],
      });
      expect(response.rawData()).toMatchSnapshot();
    });

    if (includeIncrementalSchemaSuite) {
      describe(`Incremental schema loading with @cubejs-backend/${type}-driver`, () => {
        incrementalSchemaLoadingSuite(execute, () => driver, tables);
      });
    }

    if (externalSchemaTests) {
      describe(`External schema retrospection with @cubejs-backend/${type}-driver`, () => {
        redshiftExternalSchemasSuite(execute, () => driver);
      });
    }

    executePg('SQL API: powerbi min max push down', async (connection) => {
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

    executePg('SQL API: powerbi min max ungrouped flag', async (connection) => {
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

    executePg('SQL API: ungrouped pre-agg', async (connection) => {
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

    executePg('SQL API: post-aggregate percentage of total', async (connection) => {
      const res = await connection.query(`
    select
      sum("BigECommerce"."percentageOfTotalForStatus")
    from
      "public"."BigECommerce" "BigECommerce"
  `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: reuse params', async (connection) => {
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

    executePg('SQL API: Simple Rollup', async (connection) => {
      const res = await connection.query(`
    select
        rowId, orderId, orderDate, sum(count)
    from
      "ECommerce" as "ECommerce"
    group by
      ROLLUP(1, 2, 3)
    order by 1, 2, 3

  `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: Complex Rollup', async (connection) => {
      const res = await connection.query(`
    select
        rowId, orderId, orderDate, city, sum(count)
    from
      "ECommerce" as "ECommerce"
    group by
      ROLLUP(1, 2), 3, ROLLUP(4)
    order by 1, 2, 3, 4

  `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: Rollup with aliases', async (connection) => {
      const res = await connection.query(`
    select
        rowId as "row", orderId as "order", orderDate as "orderData", city as "city", sum(count)
    from
      "ECommerce" as "ECommerce"
    group by
      ROLLUP(rowId, 2), 3, ROLLUP(4)
    order by 1, 2, 3, 4

  `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: Rollup over exprs', async (connection) => {
      const res = await connection.query(`
    select
        rowId + sales * 2 as "order", orderDate as "orderData", city as "city", sum(count)
    from
      "ECommerce" as "ECommerce"
    group by
      ROLLUP(1, 2, 3)
    order by 1, 2, 3

  `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: Nested Rollup', async (connection) => {
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
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: Nested Rollup with aliases', async (connection) => {
      const res = await connection.query(`
    select rowId as "row", orderId as "order", orderDate as "date", sum(cnt)
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
      expect(res.rows).toMatchSnapshot();
    });
    executePg('SQL API: Nested Rollup over asterisk', async (connection) => {
      const res = await connection.query(`
    select rowId as "row", orderId as "order", orderDate as "date", sum(count)
    from (
        select *
        from
        "ECommerce" as "ECommerce"
    ) a
    group by
      ROLLUP(1, 2, 3)
    order by 1, 2, 3

  `);
      expect(res.rows).toMatchSnapshot();
    });
    executePg('SQL API: Extended nested Rollup over asterisk', async (connection) => {
      const res = await connection.query(`
    select * from (
        select * from (
            select rowId as "row", orderId as "order", sum(count)
            from (
                select *
                from
                "ECommerce" as "ECommerce"
            ) a
            group by
            ROLLUP(row, order)
            ORDER BY "rowId" ASC NULLS FIRST, "orderId" ASC NULLS FIRST OFFSET 0 ROWS FETCH FIRST 100 ROWS ONLY
        ) q1
    ) q2 ORDER BY q2.order, q2.row DESC limit 100

  `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: metabase count cast to float32 from push down', async (connection) => {
      const res = await connection.query(`
        select cast(count(*) as float) as "a0" from "Customers"
      `);

      expect(res.rows).toMatchSnapshot('metabase_count_cast_to_float32_from_push_down');
    });

    executePg('SQL API: NULLS FIRST/LAST SQL push down', async (connection) => {
      const res = await connection.query(`
        SELECT CASE WHEN "category" > 'G' THEN "category" ELSE NULL END AS "category"
        FROM "Products"
        WHERE LOWER("category") != 'invalid'
        GROUP BY 1
        ORDER BY 1 ASC NULLS FIRST
        LIMIT 100
      `);
      expect(res.rows).toMatchSnapshot('nulls_first_last_sql_push_down');
    });

    executePg('SQL API: Timeshift measure from cube', async (connection) => {
      const res = await connection.query(`
        SELECT
          DATE_TRUNC('month', orderDate) AS "orderDate",
          MEASURE(totalQuantity) AS "totalQuantity",
          MEASURE(totalQuantityPriorMonth) AS "totalQuantityPriorMonth"
        FROM "ECommerce"
        WHERE orderDate >= CAST('2020-01-01' AS DATE) AND orderDate < CAST('2021-01-01' AS DATE)
        GROUP BY 1
        ORDER BY 1 ASC NULLS FIRST;
      `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('Tesseract: SQL API: Timeshift measure from cube', async (connection) => {
      const res = await connection.query(`
        SELECT
          DATE_TRUNC('month', orderDate) AS "orderDate",
          MEASURE(totalQuantity) AS "totalQuantity",
          MEASURE(totalQuantityPriorMonth) AS "totalQuantityPriorMonth"
        FROM "ECommerce"
        WHERE orderDate >= CAST('2020-01-01' AS DATE) AND orderDate < CAST('2021-01-01' AS DATE)
        GROUP BY 1
        ORDER BY 1 ASC NULLS FIRST;
      `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: Rolling Window YTD (year + month + day + date_trunc equal)', async (connection) => {
      // It's important to use day granularity - it tests for ambiguous names
      const res = await connection.query(`
        SELECT
          DATE_TRUNC('year', orderDate) AS "orderDateY",
          DATE_TRUNC('month', orderDate) AS "orderDateM",
          DATE_TRUNC('day', orderDate) AS "orderDateD",
          MEASURE(rollingCountYTD) AS "rollingCountYTD"
        FROM "BigECommerce"
        WHERE DATE_TRUNC('year', orderDate) = CAST('2020-01-01' AS DATE)
        GROUP BY 1, 2, 3
        ORDER BY 3 ASC NULLS FIRST;
      `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: Rolling Window YTD (year + month + day + date_trunc IN)', async (connection) => {
      // It's important to use day granularity - it tests for ambiguous names
      const res = await connection.query(`
        SELECT
          DATE_TRUNC('year', orderDate) AS "orderDateY",
          DATE_TRUNC('month', orderDate) AS "orderDateM",
          DATE_TRUNC('day', orderDate) AS "orderDateD",
          MEASURE(rollingCountYTD) AS "rollingCountYTD"
        FROM "BigECommerce"
        WHERE DATE_TRUNC('year', orderDate) IN (CAST('2020-01-01' AS DATE))
        GROUP BY 1, 2, 3
        ORDER BY 3 ASC NULLS FIRST;
      `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: SQL push down push to cube quoted alias', async (connection) => {
      const res = await connection.query(`
        SELECT
          (NOT ("t0"."$temp1_output" IS NULL)) AS "result"
        FROM
          "public"."ECommerce" "ECommerce"
          LEFT JOIN (
            SELECT
              CAST("ECommerce"."customerName" AS TEXT) AS "customerName",
              1 AS "$temp1_output",
              MEASURE("ECommerce"."totalQuantity") AS "$__alias__0"
            FROM "public"."ECommerce" "ECommerce"
            GROUP BY 1
            ORDER BY
              3 DESC NULLS LAST,
              1 ASC NULLS FIRST
            LIMIT 3
          ) "t0" ON (
            CAST("ECommerce"."customerName" AS TEXT) IS NOT DISTINCT
            FROM "t0"."customerName"
          )
        GROUP BY 1
      `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: Date/time comparison with SQL push down', async (connection) => {
      const res = await connection.query(`
        SELECT MEASURE(BigECommerce.rollingCountBy2Day)
        FROM BigECommerce
        WHERE BigECommerce.orderDate < CAST('2021-01-01' AS TIMESTAMP) AND
              LOWER("city") = 'columbus'
      `);
      expect(res.rows).toMatchSnapshot();
    });

    executePg('SQL API: Date/time comparison with date_trunc with SQL push down', async (connection) => {
      const res = await connection.query(`
        SELECT MEASURE(BigECommerce.rollingCountBy2Week)
        FROM BigECommerce
        WHERE date_trunc('day', BigECommerce.orderDate) < CAST('2021-01-01' AS TIMESTAMP) AND
              LOWER("city") = 'columbus'
      `);
      expect(res.rows).toMatchSnapshot();
    });
  });
}

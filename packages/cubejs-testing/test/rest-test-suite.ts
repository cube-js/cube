/* eslint-disable import/no-extraneous-dependencies */
import fetch from 'node-fetch';
import { BaseDriver } from '@cubejs-backend/base-driver';
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { BirdBox, Env, getBirdbox } from '../src';

type SupportedDriverType =
  'postgres' |
  'questdb' |
  'firebolt' |
  'bigquery' |
  'athena' |
  'databricks-jdbc';

type TestSuite = {
  type: SupportedDriverType;
  driver: BaseDriver;
  config?: Partial<Env>;
};

// let apiUrl: string = 'http://localhost:4000/cubejs-api/v1';
let systemUrl: string = 'http://localhost:4000/cubejs-system/v1';

export function executeTestSuite({ type, config = {}, driver }: TestSuite) {
  const testSchemas = [
    'CAST.js',
    'Customers.sql.js',
    'ECommerce.sql.js',
    'Products.sql.js',
    'Customers.js',
    'ECommerce.js',
    'Products.js',
  ];

  const overridedConfig = {
    NODE_ENV: 'development',
    CUBEJS_DEV_MODE: 'true',
    CUBEJS_WEB_SOCKETS: 'true',
    CUBEJS_EXTERNAL_DEFAULT: 'false',
    CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'true',
    CUBEJS_REFRESH_WORKER: 'true',
    CUBEJS_ROLLUP_ONLY: 'false',
    CUBEJS_PRE_AGGREGATIONS_SCHEMA: 'preaggs',
    ...config,
  };

  describe(
    `The REST API with the ${
      type
    } driver and the environment variables ${
      JSON.stringify(overridedConfig, undefined, 2)
    }`,
    () => {
      jest.setTimeout(60 * 5 * 1000);
      let box: BirdBox;
      // let client: CubejsApi;
      let transport: WebSocketTransport;

      beforeAll(async () => {
        box = await getBirdbox(
          type,
          overridedConfig,
          {
            schemas: testSchemas
          },
        );
        transport = new WebSocketTransport({
          apiUrl: box.configuration.apiUrl,
        });
        // client = cubejs(async () => 'test', {
        //   apiUrl: box.configuration.apiUrl,
        // // transport,
        // });
        // apiUrl = box.configuration.apiUrl;
        systemUrl = box.configuration.systemUrl;
        // await driver.query(
        //   `create schema ${
        //     overridedConfig.CUBEJS_PRE_AGGREGATIONS_SCHEMA
        //   };`
        // );
      });

      afterAll(async () => {
        // await driver.query(
        //   `drop schema ${
        //     overridedConfig.CUBEJS_PRE_AGGREGATIONS_SCHEMA
        //   } cascade;`
        // );
        await driver.release();
        await transport.close();
        await box.stop();
      });

      test('/cubejs-system/v1/pre-aggregations/jobs', async () => {
        /**
         * Post pre-aggregations job, handle its status and return
         * object to validate execution.
         */
        async function preAggregationsJob(selector: any): Promise<any> {
          type PostResponse = string[];
          type GetResponse = {[token: string]: {
            status: string;
            table: string;
            selector: {
              contexts?: { securityContext: any }[],
              timezones?: string[],
              dataSources?: string[],
              cubes?: string[],
              preAggregations?: string[],
            };
          }};
    
          const url = `${systemUrl}/pre-aggregations/jobs`;
          let response;
    
          // POST action
          response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              action: 'post',
              selector,
            }),
          });
    
          if (!response.ok) {
            return {
              status: response.status,
              message: (await response.json()).error,
            };
          } else {
            const tokens = <PostResponse>(await response.json());
            const tables: {
              [name: string]: {
                [t: string]: {
                  selector: {
                    contexts?: { securityContext: any }[],
                    timezones?: string[],
                    dataSources?: string[],
                    cubes?: string[],
                    preAggregations?: string[],
                  },
                  status: string[],
                }
              }
            } = {};
      
            // GET action
            let getted: GetResponse;
            let iter = true;
            let tbl = true;
            while (iter) {
              iter = false;
              response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  action: 'get',
                  resType: 'object',
                  tokens,
                }),
              });
              getted = <GetResponse>(await response.json());
              // eslint-disable-next-line no-loop-func
              Object.keys(getted).forEach((token) => {
                if (tbl) {
                  tables[getted[token].table] = {};
                }
                if (
                  getted[token].status.indexOf('done') === -1 &&
                  getted[token].status.indexOf('error') === -1
                ) {
                  iter = true;
                }
                tables[getted[token].table][token] =
                  tables[getted[token].table][token] || {
                    selector: getted[token].selector,
                    status: [],
                  };
                if (
                  tables[getted[token].table][token].status
                    .indexOf(getted[token].status) === -1
                ) {
                  tables[getted[token].table][token].status
                    .push(getted[token].status);
                }
              });
              tbl = false;
            }
            return tables;
          }
        }

        /**
         * Detemine whether specified table exist or not.
         */
        async function isExist(table: string) {
          const result = await driver.getTablesQuery(
            overridedConfig.CUBEJS_PRE_AGGREGATIONS_SCHEMA
          );
          const tables = result.map(row => row.table_name);
          return tables.includes(table);
        }

        /**
         * Test cases.
         */
        const statuses = [
          'scheduled',
          'processing',
          'missing_partition', // TODO (buntarb): wtf?!!
          'done',
        ];
        let result: any;
        let tables: string[];

        /**
         * Case 1: two tenants, two timezones.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
            { securityContext: { tenant: 't2' } },
          ],
          timezones: ['UTC', 'America/Los_Angeles'],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual'],
          dataSources: ['default'],
        });
        tables = Object.keys(result);
        // We cant expect predefined number of tables as some of t1 and t2
        // tables will have the same name and will override each other.
        // expect(tables.length).toBe(50);
        tables.forEach((table) => {
          const jobs = Object.keys(result[table]);
          expect(jobs.length).toBe(2);
          jobs.forEach((j) => {
            expect(result[table][j].status.includes('done')).toBeTruthy();
            expect(isExist(table)).toBeTruthy();
            result[table][j].status.forEach((status: string) => {
              expect(statuses.includes(status)).toBeTruthy();
            });
          });
        });

        /**
         * Case 2: one tenant, one timezone.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual'],
          dataSources: ['default'],
        });
        tables = Object.keys(result);
        expect(tables.length).toBe(12);
        tables.forEach((table) => {
          const jobs = Object.keys(result[table]);
          expect(jobs.length).toBe(1);
          expect(result[table][jobs[0]].status.includes('done')).toBeTruthy();
          expect(isExist(table)).toBeTruthy();
          result[table][jobs[0]].status.forEach((status: string) => {
            expect(statuses.includes(status)).toBeTruthy();
          });
        });

        /**
         * Case 3: one tenant, no timezones.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual'],
          dataSources: ['default'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'A user\'s selector must contain at least one time zone.'
        );

        /**
         * Case 4: one tenant, empty timezones.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: [],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual'],
          dataSources: ['default'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'A user\'s selector must contain at least one time zone.'
        );

        /**
         * Case 5: no context, one timezone.
         */
        result = await preAggregationsJob({
          timezones: ['UTC'],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual'],
          dataSources: ['default'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'A user\'s selector must contain at least one context element.',
        );

        /**
         * Case 6: empty context, one timezone.
         */
        result = await preAggregationsJob({
          contexts: [],
          timezones: ['UTC'],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual'],
          dataSources: ['default'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'A user\'s selector must contain at least one context element.',
        );

        /**
         * Case 7: wrong context, one timezone.
         */
        result = await preAggregationsJob({
          contexts: [
            { wrongContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual'],
          dataSources: ['default'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'Every context element must contain the ' +
          '\'securityContext\' property.'
        );

        /**
         * Case 8: one tenant, one timezone, no dataSources.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual']
        });
        tables = Object.keys(result);
        expect(tables.length).toBe(12);
        tables.forEach((table) => {
          const jobs = Object.keys(result[table]);
          expect(jobs.length).toBe(1);
          expect(result[table][jobs[0]].status.includes('done')).toBeTruthy();
          expect(isExist(table)).toBeTruthy();
          result[table][jobs[0]].status.forEach((status: string) => {
            expect(statuses.includes(status)).toBeTruthy();
          });
        });

        /**
         * Case 9: one tenant, one timezone, empty dataSources.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual'],
          dataSources: [],
        });
        tables = Object.keys(result);
        expect(tables.length).toBe(12);
        tables.forEach((table) => {
          const jobs = Object.keys(result[table]);
          expect(jobs.length).toBe(1);
          expect(result[table][jobs[0]].status.includes('done')).toBeTruthy();
          expect(isExist(table)).toBeTruthy();
          result[table][jobs[0]].status.forEach((status: string) => {
            expect(statuses.includes(status)).toBeTruthy();
          });
        });

        /**
         * Case 10: one tenant, one timezone, wrong dataSources.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.manual'],
          dataSources: ['wrongDatasource'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'A user\'s selector doesn\'t match any of the ' +
          'pre-aggregations described by the Cube schemas.'
        );

        /**
         * Case 11: one tenant, one timezone, no dataSources, no
         * cubes.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          preAggregations: ['ECommerce.manual'],
        });
        tables = Object.keys(result);
        expect(tables.length).toBe(12);
        tables.forEach((table) => {
          const jobs = Object.keys(result[table]);
          expect(jobs.length).toBe(1);
          expect(result[table][jobs[0]].status.includes('done')).toBeTruthy();
          expect(isExist(table)).toBeTruthy();
          result[table][jobs[0]].status.forEach((status: string) => {
            expect(statuses.includes(status)).toBeTruthy();
          });
        });

        /**
         * Case 12: one tenant, one timezone, no dataSources, empty
         * cubes.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: [],
          preAggregations: ['ECommerce.manual'],
        });
        tables = Object.keys(result);
        expect(tables.length).toBe(12);
        tables.forEach((table) => {
          const jobs = Object.keys(result[table]);
          expect(jobs.length).toBe(1);
          expect(result[table][jobs[0]].status.includes('done')).toBeTruthy();
          expect(isExist(table)).toBeTruthy();
          result[table][jobs[0]].status.forEach((status: string) => {
            expect(statuses.includes(status)).toBeTruthy();
          });
        });

        /**
         * Case 13: one tenant, one timezone, no dataSources, wrong
         * cubes.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['WrongCube'],
          preAggregations: ['ECommerce.manual'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'A user\'s selector doesn\'t match any of the ' +
          'pre-aggregations described by the Cube schemas.'
        );

        /**
         * Case 14: one tenant, one timezone, no dataSources, no
         * preAggregations.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['ECommerce'],
        });
        tables = Object.keys(result);
        expect(tables.length).toBe(13);
        tables.forEach((table) => {
          const jobs = Object.keys(result[table]);
          expect(jobs.length).toBe(1);
          expect(result[table][jobs[0]].status.includes('done')).toBeTruthy();
          expect(isExist(table)).toBeTruthy();
          result[table][jobs[0]].status.forEach((status: string) => {
            expect(statuses.includes(status)).toBeTruthy();
          });
        });

        /**
         * Case 15: one tenant, one timezone, no dataSources, no
         * preAggregations.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['ECommerce', 'Products', 'Customers'],
        });
        tables = Object.keys(result);
        expect(tables.length).toBe(13);
        tables.forEach((table) => {
          const jobs = Object.keys(result[table]);
          expect(jobs.length).toBe(1);
          expect(result[table][jobs[0]].status.includes('done')).toBeTruthy();
          expect(isExist(table)).toBeTruthy();
          result[table][jobs[0]].status.forEach((status: string) => {
            expect(statuses.includes(status)).toBeTruthy();
          });
        });

        /**
         * Case 16: one tenant, one timezone, no dataSources, no
         * preAggregations.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['Products'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'A user\'s selector doesn\'t match any of the ' +
          'pre-aggregations described by the Cube schemas.'
        );

        /**
         * Case 17: one tenant, one timezone, no dataSources, no
         * preAggregations.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['Customers'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'A user\'s selector doesn\'t match any of the ' +
          'pre-aggregations described by the Cube schemas.'
        );

        /**
         * Case 18: one tenant, one timezone, no dataSources, empty
         * preAggregations.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['ECommerce'],
          preAggregations: [],
        });
        tables = Object.keys(result);
        expect(tables.length).toBe(13);
        tables.forEach((table) => {
          const jobs = Object.keys(result[table]);
          expect(jobs.length).toBe(1);
          expect(result[table][jobs[0]].status.includes('done')).toBeTruthy();
          expect(isExist(table)).toBeTruthy();
          result[table][jobs[0]].status.forEach((status: string) => {
            expect(statuses.includes(status)).toBeTruthy();
          });
        });

        /**
         * Case 19: one tenant, one timezone, no dataSources, wrong
         * preAggregations.
         */
        result = await preAggregationsJob({
          contexts: [
            { securityContext: { tenant: 't1' } },
          ],
          timezones: ['UTC'],
          cubes: ['ECommerce'],
          preAggregations: ['ECommerce.wrong'],
        });
        expect(result.status).toBe(400);
        expect(result.message).toBe(
          'A user\'s selector doesn\'t match any of the ' +
          'pre-aggregations described by the Cube schemas.'
        );
      });
    }
  );
}

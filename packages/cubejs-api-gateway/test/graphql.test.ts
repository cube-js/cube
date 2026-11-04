/* globals describe,test,expect */

import bodyParser from 'body-parser';
// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
import { graphqlHTTP } from 'express-graphql';
import { GraphQLObjectType, printSchema } from 'graphql';
// eslint-disable-next-line import/no-extraneous-dependencies
import fs from 'fs-extra';
import request from 'supertest';

import { makeSchema } from '../src/graphql';

const metaConfig = [
  {
    config: {
      name: 'Orders',
      measures: [
        {
          name: 'Orders.count',
          isVisible: true,
        },
        {
          name: 'Orders.totalAmount',
          isVisible: true,
        },
      ],
      dimensions: [
        {
          name: 'Orders.id',
          isVisible: true,
        },
        {
          name: 'Orders.status',
          type: 'string',
          isVisible: true,
        },
        {
          name: 'Orders.createdAt',
          type: 'time',
          isVisible: true,
        },
      ],
    },
  },
  {
    config: {
      name: 'Users',
      measures: [
        {
          name: 'Users.count',
          isVisible: true,
        },
      ],
      dimensions: [
        {
          name: 'Users.id',
          isVisible: true,
        },
        {
          name: 'Users.city',
          type: 'string',
          isVisible: true,
        },
        {
          name: 'Users.createdAt',
          type: 'time',
          isVisible: true,
        },
      ],
    },
  },
];

const metaConfigSnakeCase = [
  {
    config: {
      name: 'orders',
      measures: [
        {
          name: 'orders.count',
          isVisible: true,
        },
      ],
      dimensions: [
        {
          name: 'orders.id',
          isVisible: true,
        },
        {
          name: 'orders.status',
          type: 'string',
          isVisible: true,
        },
        {
          name: 'orders.created_at',
          type: 'time',
          isVisible: true,
        },
      ],
    },
  },
  {
    config: {
      name: 'users',
      measures: [
        {
          name: 'users.count',
          isVisible: true,
        },
      ],
      dimensions: [
        {
          name: 'users.id',
          isVisible: true,
        },
        {
          name: 'users.city',
          type: 'string',
          isVisible: true,
        },
        {
          name: 'users.created_at',
          type: 'time',
          isVisible: true,
        },
      ],
    },
  },
];

const jsonParser = bodyParser.json({ limit: '1mb' });

function gqlQuery(query: string, variables?: Record<string, string | number>) {
  return JSON.stringify({
    operationName: 'CubeQuery',
    query,
    variables,
  });
}

function expectValidSchema(schema) {
  expect(schema).toBeDefined();
  expect(schema.getTypeMap()).toHaveProperty('OrdersMembers');
  const ordersFields = (schema.getType('OrdersMembers') as GraphQLObjectType).getFields();
  expect(ordersFields).toHaveProperty('id');
  expect(ordersFields).toHaveProperty('status');
  expect(ordersFields).toHaveProperty('createdAt');
}

describe('GraphQL Schema', () => {
  describe('with camelCase', () => {
    const app = express();

    app.use('/graphql', jsonParser, (req, res) => {
      const schema = makeSchema(metaConfig);

      return graphqlHTTP({
        schema,
        context: {
          req,
          res,
          apiGateway: {
            async load({ query, res: response }) {
              expect(query).toMatchSnapshot(req.body.query);

              response({
                query,
                annotation: {},
                data: [],
              });
            },
          },
        },
      })(req, res);
    });

    const GRAPHQL_QUERIES_PATH = `${process.cwd()}/test/graphql-queries/base.gql`;

    app.use('/graphql', jsonParser, (req, res) => {
      const schema = makeSchema(metaConfig);

      return graphqlHTTP({
        schema,
        context: {
          req,
          res,
          apiGateway: {
            async load({ query, res: response }) {
              expect(query).toMatchSnapshot(req.body.query);

              response({
                query,
                annotation: {},
                data: [],
              });
            },
          },
        },
      })(req, res);
    });

    test('should make valid schema', () => {
      const schema = makeSchema(metaConfig);
      expectValidSchema(schema);
    });

    test('should make valid schema when name is not capitalized', async () => {
      const schema = makeSchema(JSON.parse(
        JSON.stringify(metaConfig)
          .replace(/Orders/g, 'orders')
      ));
      expectValidSchema(schema);
    });

    const queries = fs.readFileSync(GRAPHQL_QUERIES_PATH, 'utf-8').split('\n\n');

    queries.forEach((query, index) => {
      test(`GraphQL query ${index}`, async () => {
        const { error } = await request(app)
          .post('/graphql')
          .set('Content-Type', 'application/json')
          .send(gqlQuery(query));

        expect((<any>error)?.text).toBeUndefined();
      });
    });
  });

  describe('with snake_case', () => {
    const app = express();

    app.use('/graphql', jsonParser, (req, res) => {
      const schema = makeSchema(metaConfigSnakeCase);

      return graphqlHTTP({
        schema,
        context: {
          req,
          res,
          apiGateway: {
            async load({ query, res: response }) {
              expect(query).toMatchSnapshot(req.body.query);

              response({
                query,
                annotation: {},
                data: [],
              });
            },
          },
        },
      })(req, res);
    });

    const GRAPHQL_QUERIES_PATH = `${process.cwd()}/test/graphql-queries/base-snake-case.gql`;

    const queries = fs.readFileSync(GRAPHQL_QUERIES_PATH, 'utf-8').split('\n\n');

    queries.forEach((query, index) => {
      test(`GraphQL query ${index}`, async () => {
        const { error } = await request(app)
          .post('/graphql')
          .set('Content-Type', 'application/json')
          .send(gqlQuery(query));

        expect((<any>error)?.text).toBeUndefined();
      });
    });
  });

  describe('extensions', () => {
    const mockAnnotation = {
      measures: {
        'Orders.count': {
          title: 'Orders Count',
          shortTitle: 'Count',
          type: 'number',
        },
        'Orders.totalAmount': {
          title: 'Orders Total Amount',
          shortTitle: 'Total Amount',
          type: 'number',
        },
      },
      dimensions: {
        'Orders.status': {
          title: 'Orders Status',
          shortTitle: 'Status',
          type: 'string',
        },
      },
      segments: {},
      timeDimensions: {},
    };

    const mockLastRefreshTime = '2025-06-01T12:00:00.000Z';

    const app = express();

    app.use('/graphql', jsonParser, (req: any, res: any) => {
      const schema = makeSchema(metaConfig);

      return graphqlHTTP({
        schema,
        context: {
          req,
          res,
          apiGateway: {
            async load({ query, res: response }) {
              response({
                query,
                annotation: mockAnnotation,
                lastRefreshTime: mockLastRefreshTime,
                data: [
                  { 'Orders.count': 10, 'Orders.totalAmount': 500, 'Orders.status': 'completed' },
                  { 'Orders.count': 5, 'Orders.totalAmount': 200, 'Orders.status': 'pending' },
                ],
              });
            },
          },
        },
        extensions: () => res.extensions || {},
      })(req, res);
    });

    test('should return annotation and lastRefreshTime in extensions', async () => {
      const query = `query CubeQuery {
        cube {
          orders { count totalAmount status }
        }
      }`;

      const res = await request(app)
        .post('/graphql')
        .set('Content-Type', 'application/json')
        .send(gqlQuery(query));

      expect(res.body.errors).toBeUndefined();
      expect(res.body.data.cube).toBeDefined();
      expect(res.body.extensions).toBeDefined();

      expect(res.body).toMatchSnapshot();
    });

    // `graphql.ts` copies the same pre-aggregation identity into `extensions`
    // as the REST response carries. Its own app so the snapshot above keeps
    // describing a query that hit no pre-aggregation.
    test('should return usedPreAggregations in extensions', async () => {
      const usedPreAggregations = {
        'schema.orders_main20240101': {
          preAggregationId: 'Orders.main',
          lastUpdatedAt: 1712000000000,
          type: 'rollup',
        },
      };

      const preAggApp = express();

      preAggApp.use('/graphql', jsonParser, (req: any, res: any) => {
        const schema = makeSchema(metaConfig);

        return graphqlHTTP({
          schema,
          context: {
            req,
            res,
            apiGateway: {
              async load({ query, res: response }) {
                response({
                  query,
                  annotation: mockAnnotation,
                  lastRefreshTime: mockLastRefreshTime,
                  usedPreAggregations,
                  data: [
                    { 'Orders.count': 10, 'Orders.totalAmount': 500, 'Orders.status': 'completed' },
                  ],
                });
              },
            },
          },
          extensions: () => res.extensions || {},
        })(req, res);
      });

      const query = `query CubeQuery {
        cube {
          orders { count totalAmount status }
        }
      }`;

      const res = await request(preAggApp)
        .post('/graphql')
        .set('Content-Type', 'application/json')
        .send(gqlQuery(query));

      expect(res.body.errors).toBeUndefined();
      expect(res.body.extensions.usedPreAggregations).toEqual(usedPreAggregations);
    });

    it('should accumulate all measures and dimensions', async () => {
      const query = `query CubeQuery {
        cube {
          orders { count totalAmount status }
        }
      }`;

      const res = await request(app)
        .post('/graphql')
        .set('Content-Type', 'application/json')
        .send(gqlQuery(query));

      expect(res.body.errors).toBeUndefined();

      expect(res.body.data.cube).toEqual([
        { orders: { count: '10', totalAmount: '500', status: 'completed' } },
        { orders: { count: '5', totalAmount: '200', status: 'pending' } },
      ]);
    });
  });

  test('printed SDL stays stable', () => {
    expect(printSchema(makeSchema(metaConfig))).toMatchSnapshot();
  });

  describe('DateTime values', () => {
    const annotation = {
      measures: {},
      dimensions: {
        'Orders.createdAt': { type: 'time' },
      },
      segments: {},
      timeDimensions: {
        'Orders.createdAt': { type: 'time' },
        'Orders.createdAt.day': { type: 'time' },
      },
    };

    function runQuery(query: string, data: Record<string, unknown>[]) {
      const loaded: any[] = [];
      const app = express();

      app.use('/graphql', jsonParser, (req: any, res: any) => graphqlHTTP({
        schema: makeSchema(metaConfig),
        context: {
          req,
          res,
          apiGateway: {
            async load({ query: jsonQuery, res: response }) {
              loaded.push(jsonQuery);
              response({
                query: { timezone: 'UTC', ...jsonQuery },
                annotation,
                data,
              });
            },
          },
        },
      })(req, res));

      return request(app)
        .post('/graphql')
        .set('Content-Type', 'application/json')
        .send(gqlQuery(query))
        .then((res) => ({ body: res.body, status: res.status, loaded }));
    }

    test('returns time dimensions from Cube local-time strings as UTC instants', async () => {
      const { body } = await runQuery(
        'query CubeQuery { cube { orders { createdAt { value day } } } }',
        [{ 'Orders.createdAt': '2026-01-01T00:00:00.000', 'Orders.createdAt.day': '2026-01-02T00:00:00.000' }],
      );

      expect(body).toEqual({
        data: {
          cube: [{
            orders: {
              createdAt: { value: '2026-01-01T00:00:00.000Z', day: '2026-01-02T00:00:00.000Z' },
            },
          }],
        },
      });
    });

    test('shifts time dimensions by the query timezone, across DST', async () => {
      const { body } = await runQuery(
        'query CubeQuery { cube(timezone: "America/Los_Angeles") { orders { createdAt { day } } } }',
        [
          { 'Orders.createdAt.day': '2026-01-01T00:00:00.000' },
          { 'Orders.createdAt.day': '2026-07-01T00:00:00.000' },
        ],
      );

      expect(body.errors).toBeUndefined();
      expect(body.data.cube.map(row => row.orders.createdAt.day)).toEqual([
        '2026-01-01T08:00:00.000Z',
        '2026-07-01T07:00:00.000Z',
      ]);
    });

    test('reports a value the scalar refuses as a field error', async () => {
      const { body } = await runQuery(
        'query CubeQuery { cube { orders { createdAt { day } } } }',
        [{ 'Orders.createdAt.day': '+012026-01-01T00:00:00.000' }],
      );

      expect(body.data.cube).toEqual([{ orders: { createdAt: null } }]);
      expect(body.errors.map(error => error.message)).toEqual([
        'DateTime cannot represent an invalid date-time-string +012026-01-01T00:00:00.000Z.',
      ]);
    });

    test('passes date filters through as strings, not through the scalar', async () => {
      const { body, loaded } = await runQuery(
        `query CubeQuery {
          cube(where: { orders: { createdAt: {
            afterDate: "2026-01-01T00:00:00.000",
            inDateRange: ["2026-01-01", "2026-02-01T00:00:00.000"]
          } } }) {
            orders { createdAt { day } }
          }
        }`,
        [],
      );

      expect(body).toEqual({ data: { cube: [] } });
      expect(loaded).toEqual([{
        timeDimensions: [{
          dimension: 'Orders.createdAt',
          granularity: 'day',
          dateRange: ['2026-01-01', '2026-02-01T00:00:00.000'],
        }],
        filters: [{
          member: 'Orders.createdAt',
          operator: 'afterDate',
          values: ['2026-01-01T00:00:00.000'],
        }],
      }]);
    });

    test('rejects a DateTime variable in a filter position', async () => {
      const { body, status, loaded } = await runQuery(
        `query CubeQuery($from: DateTime) {
          cube(where: { orders: { createdAt: { afterDate: $from } } }) {
            orders { createdAt { day } }
          }
        }`,
        [],
      );

      expect(status).toBe(400);
      expect(loaded).toEqual([]);
      expect(body.errors.map(error => error.message)).toEqual([
        'Variable "$from" of type "DateTime" used in position expecting type "String".',
      ]);
    });
  });
});

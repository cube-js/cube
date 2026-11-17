/* globals describe,test,expect */

import bodyParser from 'body-parser';
// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
import { graphqlHTTP } from 'express-graphql';
import { GraphQLObjectType } from 'graphql';
// eslint-disable-next-line import/no-extraneous-dependencies
import fs from 'fs-extra';
import request from 'supertest';

import { getJsonQueryFromGraphQLQuery, makeSchema, parseDocument } from '../src/graphql';

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

  describe('without queryable members', () => {
    const queryField = (cubes: any[]) => {
      const fields = (makeSchema(cubes).getQueryType() as GraphQLObjectType).getFields();
      expect(Object.keys(fields)).toEqual(['no_queries_available']);
      return fields.no_queries_available;
    };

    test('says the data model is empty when there are no cubes', () => {
      expect(queryField([]).description).toEqual('The data model has no cubes or views.');
    });

    test('points at access policies when every member is hidden', () => {
      const field = queryField([{
        config: {
          name: 'Orders',
          measures: [{ name: 'Orders.count', type: 'number', isVisible: false }],
          dimensions: [],
          segments: [],
        },
      }]);
      expect(field.description).toContain('No cubes or views have members visible to you');
    });
  });
});

describe('getJsonQueryFromGraphQLQuery', () => {
  it('translates queries that differ only by a comment-ending newline', () => {
    const where = '(where: { orders: { id: { equals: "1" } } })';
    const commented = `query { cube # note ${where}\n{ orders { count } } }`;
    const filtered = `query { cube # note\n${where}\n{ orders { count } } }`;

    expect(getJsonQueryFromGraphQLQuery(commented, metaConfig)).toEqual({
      measures: ['Orders.count'],
    });
    expect(getJsonQueryFromGraphQLQuery(filtered, metaConfig)).toEqual({
      measures: ['Orders.count'],
      filters: [{ member: 'Orders.id', operator: 'equals', values: ['1'] }],
    });
    expect(getJsonQueryFromGraphQLQuery(commented, metaConfig)).toEqual({
      measures: ['Orders.count'],
    });
  });

  it('accepts a query wrapped in an array', () => {
    expect(getJsonQueryFromGraphQLQuery(['query { cube { orders { count } } }'], metaConfig)).toEqual({
      measures: ['Orders.count'],
    });
  });
});

describe('parseDocument', () => {
  // 160 accounted bytes per character: 12K characters stay under the 2 MB entry limit.
  const largeQuery = (id: number) => `{ cube { orders { ${'count '.repeat(2000)} } } } # ${id}`;

  afterEach(() => {
    jest.useRealTimers();
  });

  it('reuses the parsed document of a repeated query', () => {
    const query = '{ cube { orders { count } } } # reuse';

    expect(parseDocument(query)).toBe(parseDocument(query));
  });

  it('does not cache a query above the entry size limit', () => {
    const query = `{ cube { orders { ${'count '.repeat(3000)} } } }`;

    expect(parseDocument(query)).not.toBe(parseDocument(query));
  });

  it('evicts the least recently used documents past the total size limit', () => {
    const first = parseDocument(largeQuery(0));
    expect(parseDocument(largeQuery(0))).toBe(first);

    for (let id = 1; id <= 20; id++) {
      parseDocument(largeQuery(id));
    }

    expect(parseDocument(largeQuery(20))).toBe(parseDocument(largeQuery(20)));
    expect(parseDocument(largeQuery(0))).not.toBe(first);
  });

  it('expires a document an hour after its last use', () => {
    jest.useFakeTimers();

    // lru-cache reads time from the `performance` object it saw at load, and fake
    // timers install a new one, so only a copy loaded now follows the fake clock.
    let parseWithFakeClock = parseDocument;
    jest.isolateModules(() => {
      // eslint-disable-next-line global-require
      parseWithFakeClock = require('../src/graphql').parseDocument;
    });

    // The fake clock starts at 0, which lru-cache reads as an entry without a TTL.
    jest.advanceTimersByTime(1);

    const query = '{ cube { orders { count } } }';
    const document = parseWithFakeClock(query);

    jest.advanceTimersByTime(59 * 60 * 1000);
    expect(parseWithFakeClock(query)).toBe(document);

    jest.advanceTimersByTime(59 * 60 * 1000);
    expect(parseWithFakeClock(query)).toBe(document);

    jest.advanceTimersByTime(61 * 60 * 1000);
    expect(parseWithFakeClock(query)).not.toBe(document);
  });
});

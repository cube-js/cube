/* globals describe,test,expect */

import bodyParser from 'body-parser';
// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
import { graphqlHTTP } from 'express-graphql';
import { GraphQLObjectType } from 'graphql';
// eslint-disable-next-line import/no-extraneous-dependencies
import fs from 'fs-extra';
// eslint-disable-next-line import/no-extraneous-dependencies
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
});

// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
// eslint-disable-next-line import/no-extraneous-dependencies
import fs from 'fs-extra';
import { graphqlHTTP } from 'express-graphql';
import bodyParser from 'body-parser';
// eslint-disable-next-line import/no-extraneous-dependencies
import request from 'supertest';

import { makeSchema } from '../src/graphql';

function gqlQuery(query: string, variables?: Record<string, string | number>) {
  return JSON.stringify({
    operationName: 'CubeQuery',
    query,
    variables,
  });
}

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

const app = express();

const jsonParser = bodyParser.json({ limit: '1mb' });

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

describe('xxx', () => {
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

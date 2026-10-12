// https://github.com/cube-js/cube/issues/6972
// Every field of the GraphQL `TimeDimension` object type is non-nullable, so a
// row whose time dimension is NULL fails with
// "Cannot return null for non-nullable field TimeDimension.day".

import bodyParser from 'body-parser';
// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
import { graphqlHTTP } from 'express-graphql';
// eslint-disable-next-line import/no-extraneous-dependencies
import request from 'supertest';

import { makeSchema } from '../src/graphql';

const metaConfig = [
  {
    config: {
      name: 'Orders',
      measures: [
        { name: 'Orders.count', type: 'number', isVisible: true },
      ],
      dimensions: [
        { name: 'Orders.shippedDate', type: 'time', isVisible: true },
      ],
    },
  },
];

describe('issue #6972: nullable time dimension in GraphQL', () => {
  const app = express();

  app.use('/graphql', bodyParser.json(), (req: any, res: any) => graphqlHTTP({
    schema: makeSchema(metaConfig),
    context: {
      req,
      res,
      apiGateway: {
        async load({ query, res: response }) {
          response({
            query,
            annotation: {
              measures: {
                'Orders.count': { title: 'Orders Count', shortTitle: 'Count', type: 'number' },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {
                'Orders.shippedDate.day': { title: 'Orders Shipped Date', shortTitle: 'Shipped Date', type: 'time' },
                'Orders.shippedDate': { title: 'Orders Shipped Date', shortTitle: 'Shipped Date', type: 'time' },
              },
            },
            data: [
              { 'Orders.count': 1, 'Orders.shippedDate.day': null, 'Orders.shippedDate': null },
              { 'Orders.count': 1, 'Orders.shippedDate.day': '2023-07-01T00:00:00.000', 'Orders.shippedDate': '2023-07-01T00:00:00.000' },
            ],
          });
        },
      },
    },
  })(req, res));

  test('returns null for a NULL time dimension instead of a GraphQL error', async () => {
    const res = await request(app)
      .post('/graphql')
      .set('Content-Type', 'application/json')
      .send({ query: 'query { cube { orders { count shippedDate { day } } } }' });

    expect(res.body.errors).toBeUndefined();
    expect(res.body.data.cube[0].orders.shippedDate.day).toBeNull();
    expect(res.body.data.cube[1].orders.shippedDate.day).toBeTruthy();
  });
});

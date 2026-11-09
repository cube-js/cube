// Regression test for https://github.com/cube-js/cube/issues/6972
// A time dimension that is NULL in the data must not produce
// "Cannot return null for non-nullable field TimeDimension.<granularity>" errors.
import bodyParser from 'body-parser';
// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
import { graphqlHTTP } from 'express-graphql';
import request from 'supertest';

import { makeSchema } from '../src/graphql';

const metaConfig = [
  {
    config: {
      name: 'Orders',
      measures: [{ name: 'Orders.count', isVisible: true }],
      dimensions: [{ name: 'Orders.shippedDate', type: 'time', isVisible: true }],
    },
  },
];

const timeAnnotation = { title: 'Orders Shipped Date', shortTitle: 'Shipped Date', type: 'time' };

function makeApp(data: Record<string, any>[], annotation: any) {
  const app = express();
  app.use('/graphql', bodyParser.json(), (req: any, res: any) => graphqlHTTP({
    schema: makeSchema(metaConfig),
    context: {
      req,
      res,
      apiGateway: {
        async load({ query, res: response }) {
          response({ query, annotation, data });
        },
      },
    },
  })(req, res));
  return app;
}

describe('GraphQL nullable time dimension (#6972)', () => {
  test('granularity field of a NULL time dimension resolves to null without errors', async () => {
    const app = makeApp(
      [
        { 'Orders.shippedDate.day': '2023-07-01T00:00:00.000', 'Orders.shippedDate': '2023-07-01T00:00:00.000', 'Orders.count': 1 },
        { 'Orders.shippedDate.day': null, 'Orders.shippedDate': null, 'Orders.count': 1 },
      ],
      {
        measures: { 'Orders.count': { type: 'number' } },
        dimensions: {},
        segments: {},
        timeDimensions: {
          'Orders.shippedDate.day': { ...timeAnnotation, granularity: { name: 'day' } },
          'Orders.shippedDate': timeAnnotation,
        },
      }
    );

    const res = await request(app)
      .post('/graphql')
      .set('Content-Type', 'application/json')
      .send(JSON.stringify({ query: '{ cube { orders { count shippedDate { day } } } }' }));

    expect(res.body.errors).toBeUndefined();
    expect(res.body.data.cube[1].orders.shippedDate).toEqual({ day: null });
  });

  test('value field of a NULL time dimension resolves to null without errors', async () => {
    const app = makeApp(
      [
        { 'Orders.shippedDate': '2023-07-01T12:00:00.000', 'Orders.count': 1 },
        { 'Orders.shippedDate': null, 'Orders.count': 1 },
      ],
      {
        measures: { 'Orders.count': { type: 'number' } },
        dimensions: { 'Orders.shippedDate': timeAnnotation },
        segments: {},
        timeDimensions: {},
      }
    );

    const res = await request(app)
      .post('/graphql')
      .set('Content-Type', 'application/json')
      .send(JSON.stringify({ query: '{ cube { orders { count shippedDate { value } } } }' }));

    expect(res.body.errors).toBeUndefined();
    expect(res.body.data.cube[1].orders.shippedDate).toEqual({ value: null });
  });
});

/**
 * https://github.com/cube-js/cube/issues/5652
 *
 * The issue reports that a `min` measure over a timestamp column fails in the
 * GraphQL API with "Float cannot represent non numeric value". `min`/`max` are
 * documented as numeric-only (and are exposed as GraphQL `Float`), so the
 * supported way to get the earliest timestamp is a measure of `type: time`
 * (e.g. `sql: MIN(created_at)`).
 *
 * That supported form is broken in GraphQL as well: `mapType('time')` turns a
 * `time` measure into the `TimeDimension` object type, but the resolver only
 * nests values under `value` for *dimensions*, so every field of the measure
 * resolves to null and the query fails with
 * "Cannot return null for non-nullable field TimeDimension.value".
 *
 * Reproduced end to end on Cube v1.7.49 with Postgres.
 *
 * The test is agnostic of how the fix exposes the measure: as a scalar
 * (`minDate`) or as an object (`minDate { value }`).
 */

import bodyParser from 'body-parser';
// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
import { graphqlHTTP } from 'express-graphql';
import { GraphQLObjectType, getNamedType, isScalarType } from 'graphql';
import request from 'supertest';

import { makeSchema } from '../src/graphql';

const metaConfig = [
  {
    config: {
      name: 'Orders',
      measures: [
        {
          name: 'Orders.count',
          type: 'number',
          isVisible: true,
        },
        {
          name: 'Orders.minDate',
          type: 'time',
          isVisible: true,
        },
      ],
      dimensions: [
        {
          name: 'Orders.status',
          type: 'string',
          isVisible: true,
        },
      ],
    },
  },
];

describe('Issue #5652: time-typed measures in GraphQL', () => {
  const schema = makeSchema(metaConfig);

  const app = express();
  app.use('/graphql', bodyParser.json(), (req: any, res: any) => graphqlHTTP({
    schema,
    context: {
      req,
      res,
      apiGateway: {
        async load({ query, res: response }) {
          response({
            query,
            annotation: {
              measures: {
                'Orders.minDate': {
                  title: 'Orders Min Date',
                  shortTitle: 'Min Date',
                  type: 'time',
                },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {},
            },
            data: [
              { 'Orders.minDate': '2020-06-25T14:00:00.000' },
            ],
          });
        },
      },
    },
  })(req, res));

  test('returns the value of a `type: time` measure', async () => {
    const field = (schema.getType('OrdersMembers') as GraphQLObjectType).getFields().minDate;
    expect(field).toBeDefined();

    const selection = isScalarType(getNamedType(field.type)) ? 'minDate' : 'minDate { value }';

    const res = await request(app)
      .post('/graphql')
      .set('Content-Type', 'application/json')
      .send(JSON.stringify({
        query: `query CubeQuery { cube { orders { ${selection} } } }`,
      }));

    expect(res.body.errors).toBeUndefined();

    const { minDate } = res.body.data.cube[0].orders;
    const value = typeof minDate === 'object' && minDate !== null ? minDate.value : minDate;
    expect(new Date(value).toISOString()).toEqual('2020-06-25T14:00:00.000Z');
  });
});

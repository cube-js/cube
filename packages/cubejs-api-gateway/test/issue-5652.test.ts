// https://github.com/cube-js/cube/issues/5652
// A `min`/`max` measure over a time column returns a timestamp string, but the
// meta layer reports every aggregate measure as `type: 'number'`, so the
// GraphQL schema types it as `Float` and serialization fails with
// "Float cannot represent non numeric value".

import bodyParser from 'body-parser';
// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
import { graphqlHTTP } from 'express-graphql';
// eslint-disable-next-line import/no-extraneous-dependencies
import request from 'supertest';

import { makeSchema } from '../src/graphql';

// Shape produced by CubeToMetaTransformer for `minDate: { sql: 'created_at', type: 'min' }`:
// `type` goes through CubeSymbols.toMemberDataType('min') === 'number'.
const metaConfig = [
  {
    config: {
      name: 'GeneralLedgerIncome',
      measures: [
        {
          name: 'GeneralLedgerIncome.minDate',
          type: 'number',
          aggType: 'min',
          isVisible: true,
        },
      ],
      dimensions: [],
    },
  },
];

describe('issue #5652: GraphQL min measure on a time column', () => {
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
                'GeneralLedgerIncome.minDate': { title: 'Min Date', shortTitle: 'Min Date', type: 'number' },
              },
              dimensions: {},
              segments: {},
              timeDimensions: {},
            },
            data: [{ 'GeneralLedgerIncome.minDate': '2020-06-25T14:00:00.000' }],
          });
        },
      },
    },
  })(req, res));

  test('returns the min date without a Float serialization error', async () => {
    const res = await request(app)
      .post('/graphql')
      .set('Content-Type', 'application/json')
      .send({ query: 'query { cube { generalLedgerIncome { minDate } } }' });

    expect(res.body.errors).toBeUndefined();
    expect(res.body.data.cube[0].generalLedgerIncome.minDate).not.toBeNull();
  });
});

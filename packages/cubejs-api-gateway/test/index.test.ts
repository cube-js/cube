/* eslint-disable @typescript-eslint/no-shadow */

// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
// eslint-disable-next-line import/no-extraneous-dependencies
import request from 'supertest';
import jwt from 'jsonwebtoken';

import * as console from 'console';
import { ApiGateway, ApiGatewayOptions, Query, QueryRequest, Request } from '../src';
import { generateAuthToken } from './utils';
import {
  preAggregationsResultFactory,
  preAggregationPartitionsResultFactory,
  compilerApi,
  RefreshSchedulerMock,
  DataSourceStorageMock,
  AdapterApiMock
} from './mocks';
import { ApiScopesTuple } from '../src/types/auth';

const logger = (type, message) => console.log({ type, ...message });

async function requestBothGetAndPost(app, { url, query, body }, assert) {
  {
    const res = await request(app)
      .get(url)
      .query(query)
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);

    assert(res);
  }

  {
    const res = await request(app)
      .post(url)
      .set('Content-type', 'application/json')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .send(body)
      .expect(200);

    assert(res);
  }
}

const API_SECRET = 'secret';
async function createApiGateway(
  adapterApi: any = new AdapterApiMock(),
  dataSourceStorage: any = new DataSourceStorageMock(),
  options: Partial<ApiGatewayOptions> = {}
) {
  process.env.NODE_ENV = 'production';

  const apiGateway = new ApiGateway(API_SECRET, compilerApi, async () => adapterApi, logger, {
    standalone: true,
    dataSourceStorage,
    basePath: '/cubejs-api',
    refreshScheduler: {},
    ...options,
  });

  process.env.NODE_ENV = 'unknown';
  const app = express();
  app.use(express.json());
  apiGateway.initApp(app);

  return {
    app,
    apiGateway,
    dataSourceStorage,
    adapterApi
  };
}

describe('API Gateway', () => {
  test('bad token', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}')
      .set('Authorization', 'foo')
      .expect(403);
    expect(res.body && res.body.error).toStrictEqual('Invalid token');
  });

  test('bad token with schema', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}')
      .set('Authorization', 'Bearer foo')
      .expect(403);
    expect(res.body && res.body.error).toStrictEqual('Invalid token');
  });

  test('query field is empty', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query=')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(400);

    expect(res.body && res.body.error).toStrictEqual(
      'Query param is required'
    );
  });

  test('incorrect json for query field', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query=NOT_A_JSON')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(400);

    expect(res.body && res.body.error).toContain(
      // different JSON.parse errors between Node.js versions
      'Unable to decode query param as JSON, error: Unexpected token'
    );
  });

  test('requires auth', async () => {
    const { app } = await createApiGateway();

    const res = await request(app).get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}').expect(403);
    expect(res.body && res.body.error).toStrictEqual('Authorization header isn\'t set');
  });

  test('passes correct token', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={}')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(400);
    expect(res.body && res.body.error).toStrictEqual(
      'Query should contain either measures, dimensions or timeDimensions with granularities in order to be valid'
    );
  });

  test('passes correct token with auth schema', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={}')
      .set('Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(400);

    expect(res.body && res.body.error).toStrictEqual(
      'Query should contain either measures, dimensions or timeDimensions with granularities in order to be valid'
    );
  });

  test('catch error requestContextMiddleware', async () => {
    const { app } = await createApiGateway(
      new AdapterApiMock(),
      new DataSourceStorageMock(),
      {
        extendContext: (_req) => {
          throw new Error('Server should not crash');
        }
      }
    );

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(500);

    expect(res.body && res.body.error).toStrictEqual('Error: Server should not crash');
  });

  test('query transform with checkAuth', async () => {
    const queryRewrite = jest.fn(async (query: Query, context) => {
      expect(context.securityContext).toEqual({
        exp: 2475857705,
        iat: 1611857705,
        uid: 5
      });

      expect(context.authInfo).toEqual({
        exp: 2475857705,
        iat: 1611857705,
        uid: 5
      });

      return query;
    });

    const { app } = await createApiGateway(
      new AdapterApiMock(),
      new DataSourceStorageMock(),
      {
        checkAuth: (req: Request, authorization) => {
          if (authorization) {
            req.authInfo = jwt.verify(authorization, API_SECRET);
          }
        },
        queryRewrite
      }
    );

    const res = await request(app)
      .get(
        '/cubejs-api/v1/load?query={"measures":["Foo.bar"],"filters":[{"dimension":"Foo.id","operator":"equals","values":[null]}]}'
      )
      // console.log(generateAuthToken({ uid: 5, }));
      .set('Authorization', 'Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g')
      .expect(200);

    console.log(res.body);
    expect(res.body && res.body.data).toStrictEqual([{ 'Foo.bar': 42 }]);

    expect(queryRewrite.mock.calls.length).toEqual(1);
  });

  test('query transform with checkAuth (return securityContext as string)', async () => {
    const queryRewrite = jest.fn(async (query: Query, context) => {
      expect(context.securityContext).toEqual(
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g'
      );

      expect(context.authInfo).toEqual(
        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g'
      );

      return query;
    });

    const { app } = await createApiGateway(
      new AdapterApiMock(),
      new DataSourceStorageMock(),
      {
        checkAuth: (req: Request, authorization) => {
          if (authorization) {
            jwt.verify(authorization, API_SECRET);
            req.authInfo = authorization;
          }
        },
        queryRewrite
      }
    );

    const res = await request(app)
      .get(
        '/cubejs-api/v1/load?query={"measures":["Foo.bar"],"filters":[{"dimension":"Foo.id","operator":"equals","values":[null]}]}'
      )
      // console.log(generateAuthToken({ uid: 5, }));
      .set('Authorization', 'Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g')
      .expect(200);

    console.log(res.body);
    expect(res.body && res.body.data).toStrictEqual([{ 'Foo.bar': 42 }]);

    expect(queryRewrite.mock.calls.length).toEqual(1);
  });

  test('query transform with checkAuth with return', async () => {
    const queryRewrite = jest.fn(async (query: Query, context) => {
      expect(context.securityContext).toEqual({
        exp: 2475857705,
        iat: 1611857705,
        uid: 5
      });

      expect(context.authInfo).toEqual({
        exp: 2475857705,
        iat: 1611857705,
        uid: 5
      });

      return query;
    });

    const { app } = await createApiGateway(
      new AdapterApiMock(),
      new DataSourceStorageMock(),
      {
        checkAuth: (req: Request, authorization) => {
          if (authorization) {
            return {
              security_context: jwt.verify(authorization, API_SECRET),
            };
          }

          return {};
        },
        queryRewrite
      }
    );

    const res = await request(app)
      .get(
        '/cubejs-api/v1/load?query={"measures":["Foo.bar"],"filters":[{"dimension":"Foo.id","operator":"equals","values":[null]}]}'
      )
      // console.log(generateAuthToken({ uid: 5, }));
      .set('Authorization', 'Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOjUsImlhdCI6MTYxMTg1NzcwNSwiZXhwIjoyNDc1ODU3NzA1fQ.tTieqdIcxDLG8fHv8YWwfvg_rPVe1XpZKUvrCdzVn3g')
      .expect(200);

    console.log(res.body);
    expect(res.body && res.body.data).toStrictEqual([{ 'Foo.bar': 42 }]);

    expect(queryRewrite.mock.calls.length).toEqual(1);
  });

  test('null filter values', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get(
        '/cubejs-api/v1/load?query={"measures":["Foo.bar"],"filters":[{"dimension":"Foo.id","operator":"equals","values":[null]}]}'
      )
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);
    console.log(res.body);
    expect(res.body && res.body.data).toStrictEqual([{ 'Foo.bar': 42 }]);
  });

  test('custom granularities in annotation from timeDimensions', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get(
        '/cubejs-api/v1/load?query={"measures":["Foo.bar"],"timeDimensions":[{"dimension":"Foo.timeGranularities","granularity":"half_year_by_1st_april"}]}'
      )
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);
    console.log(res.body);
    expect(res.body && res.body.data).toStrictEqual([{ 'Foo.bar': 42 }]);
    expect(res.body.annotation.timeDimensions['Foo.timeGranularities.half_year_by_1st_april'])
      .toStrictEqual({
        granularity: {
          name: 'half_year_by_1st_april',
          title: 'Half Year By1 St April',
          interval: '6 months',
          offset: '3 months',
        }
      });
  });

  test('custom granularities in annotation from dimensions', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get(
        '/cubejs-api/v1/load?query={"measures":["Foo.bar"],"dimensions":["Foo.timeGranularities.half_year_by_1st_april"]}'
      )
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);
    console.log(res.body);
    expect(res.body && res.body.data).toStrictEqual([{ 'Foo.bar': 42 }]);
    expect(res.body.annotation.timeDimensions['Foo.timeGranularities.half_year_by_1st_april'])
      .toStrictEqual({
        granularity: {
          name: 'half_year_by_1st_april',
          title: 'Half Year By1 St April',
          interval: '6 months',
          offset: '3 months',
        }
      });
  });

  test('dry-run', async () => {
    const { app } = await createApiGateway();

    const query = {
      measures: ['Foo.bar']
    };

    return requestBothGetAndPost(
      app,
      { url: '/cubejs-api/v1/dry-run', query: { query: JSON.stringify(query) }, body: { query } },
      (res) => {
        expect(res.body).toStrictEqual({
          queryType: 'regularQuery',
          normalizedQueries: [
            {
              measures: ['Foo.bar'],
              timezone: 'UTC',
              filters: [],
              rowLimit: 10000,
              limit: 10000,
              dimensions: [],
              timeDimensions: [],
              queryType: 'regularQuery'
            }
          ],
          queryOrder: [{ id: 'desc' }],
          pivotQuery: {
            measures: ['Foo.bar'],
            timezone: 'UTC',
            filters: [],
            rowLimit: 10000,
            limit: 10000,
            dimensions: [],
            timeDimensions: [],
            queryType: 'regularQuery'
          },
          transformedQueries: [null]
        });
      }
    );
  });

  test('normalize filter number values', async () => {
    const { app } = await createApiGateway();

    const query = {
      measures: ['Foo.bar'],
      filters: [{
        member: 'Foo.bar',
        operator: 'gte',
        values: [10.5]
      }, {
        member: 'Foo.bar',
        operator: 'gte',
        values: [0]
      }, {
        or: [{
          member: 'Foo.bar',
          operator: 'gte',
          values: [10.5]
        }, {
          member: 'Foo.bar',
          operator: 'gte',
          values: [0]
        }]
      }]
    };

    return requestBothGetAndPost(
      app,
      { url: '/cubejs-api/v1/dry-run', query: { query: JSON.stringify(query) }, body: { query } },
      (res) => {
        expect(res.body.normalizedQueries).toStrictEqual([
          {
            measures: ['Foo.bar'],
            timezone: 'UTC',
            filters: [{
              member: 'Foo.bar',
              operator: 'gte',
              values: ['10.5']
            }, {
              member: 'Foo.bar',
              operator: 'gte',
              values: ['0']
            }, {
              or: [{
                member: 'Foo.bar',
                operator: 'gte',
                values: ['10.5']
              }, {
                member: 'Foo.bar',
                operator: 'gte',
                values: ['0']
              }]
            }],
            rowLimit: 10000,
            limit: 10000,
            dimensions: [],
            timeDimensions: [],
            queryType: 'regularQuery'
          }
        ]);
      }
    );
  });

  test('normalize empty filters', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get(
        '/cubejs-api/v1/load?query={"measures":["Foo.bar"],"filters":[{"member":"Foo.bar","operator":"equals","values":[]}]}'
      )
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(400);
    console.log(res.body);
    expect(res.body.error).toMatch(/Values required for filter/);
  });

  test('normalize queryRewrite limit', async () => {
    const { app } = await createApiGateway(
      new AdapterApiMock(),
      new DataSourceStorageMock(),
      {
        checkAuth: (req: Request, authorization) => {
          if (authorization) {
            jwt.verify(authorization, API_SECRET);
            req.authInfo = authorization;
          }
        },
        queryRewrite: async (query, _context) => {
          query.limit = 2;
          return query;
        },
      }
    );

    const query = {
      measures: ['Foo.bar']
    };

    return requestBothGetAndPost(
      app,
      { url: '/cubejs-api/v1/dry-run', query: { query: JSON.stringify(query) }, body: { query } },
      (res) => {
        expect(res.body).toStrictEqual({
          queryType: 'regularQuery',
          normalizedQueries: [
            {
              measures: ['Foo.bar'],
              timezone: 'UTC',
              filters: [],
              rowLimit: 2,
              limit: 2,
              dimensions: [],
              timeDimensions: [],
              queryType: 'regularQuery'
            }
          ],
          queryOrder: [{ id: 'desc' }],
          pivotQuery: {
            measures: ['Foo.bar'],
            timezone: 'UTC',
            filters: [],
            rowLimit: 2,
            limit: 2,
            dimensions: [],
            timeDimensions: [],
            queryType: 'regularQuery'
          },
          transformedQueries: [null]
        });
      }
    );
  });

  test('normalize order', async () => {
    const { app } = await createApiGateway();

    const query = {
      measures: ['Foo.bar'],
      order: {
        'Foo.bar': 'desc'
      }
    };

    return requestBothGetAndPost(
      app,
      { url: '/cubejs-api/v1/dry-run', query: { query: JSON.stringify(query) }, body: { query } },
      (res) => {
        expect(res.body).toStrictEqual({
          queryType: 'regularQuery',
          normalizedQueries: [
            {
              measures: ['Foo.bar'],
              order: [{ id: 'Foo.bar', desc: true }],
              timezone: 'UTC',
              filters: [],
              rowLimit: 10000,
              limit: 10000,
              dimensions: [],
              timeDimensions: [],
              queryType: 'regularQuery'
            }
          ],
          queryOrder: [{ id: 'desc' }],
          pivotQuery: {
            measures: ['Foo.bar'],
            order: [{ id: 'Foo.bar', desc: true }],
            timezone: 'UTC',
            filters: [],
            rowLimit: 10000,
            limit: 10000,
            dimensions: [],
            timeDimensions: [],
            queryType: 'regularQuery'
          },
          transformedQueries: [null]
        });
      }
    );
  });

  test('date range padding', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get(
        '/cubejs-api/v1/load?query={"measures":["Foo.bar"],"timeDimensions":[{"dimension":"Foo.time","granularity":"hour","dateRange":["2020-01-01","2020-01-01"]}]}'
      )
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);
    console.log(res.body);
    expect(res.body.query.timeDimensions[0].dateRange).toStrictEqual([
      '2020-01-01T00:00:00.000',
      '2020-01-01T23:59:59.999',
    ]);
  });

  test('order support object format', async () => {
    const { app } = await createApiGateway();

    const query = {
      measures: ['Foo.bar'],
      order: {
        'Foo.bar': 'asc',
      },
    };
    const res = await request(app)
      .get(`/cubejs-api/v1/load?query=${JSON.stringify(query)}`)
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);

    expect(res.body.query.order).toStrictEqual([{ id: 'Foo.bar', desc: false }]);
  });

  test('order support array of tuples', async () => {
    const { app } = await createApiGateway();

    const query = {
      measures: ['Foo.bar'],
      order: [
        ['Foo.bar', 'asc'],
        ['Foo.foo', 'desc'],
      ],
    };
    const res = await request(app)
      .get(`/cubejs-api/v1/load?query=${JSON.stringify(query)}`)
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);

    expect(res.body.query.order).toStrictEqual([
      { id: 'Foo.bar', desc: false },
      { id: 'Foo.foo', desc: true },
    ]);
  });

  test('post http method for load route', async () => {
    const { app } = await createApiGateway();

    const query = {
      measures: ['Foo.bar'],
      order: [
        ['Foo.bar', 'asc'],
        ['Foo.foo', 'desc'],
      ],
    };
    const res = await request(app)
      .post('/cubejs-api/v1/load')
      .set('Content-type', 'application/json')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .send({ query })
      .expect(200);

    expect(res.body.query.order).toStrictEqual([
      { id: 'Foo.bar', desc: false },
      { id: 'Foo.foo', desc: true },
    ]);
    expect(res.body.query.measures).toStrictEqual(['Foo.bar']);
  });

  test('meta endpoint to get schema information', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/meta')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);
    expect(res.body).toHaveProperty('cubes');
    expect(res.body.cubes[0]?.name).toBe('Foo');
    expect(res.body.cubes[0]?.description).toBe('cube from compilerApi mock');
    expect(res.body.cubes[0]?.hasOwnProperty('sql')).toBe(false);
    expect(res.body.cubes[0]?.dimensions.find(dimension => dimension.name === 'Foo.id').description).toBe('id dimension from compilerApi mock');
    expect(res.body.cubes[0]?.measures.find(measure => measure.name === 'Foo.bar').description).toBe('measure from compilerApi mock');
    expect(res.body.cubes[0]?.segments.find(segment => segment.name === 'Foo.quux').description).toBe('segment from compilerApi mock');
  });

  test('meta endpoint extended to get schema information with additional data', async () => {
    const { app } = await createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/meta?extended')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);

    expect(res.body).toHaveProperty('cubes');
    expect(res.body.cubes[0]?.name).toBe('Foo');
    expect(res.body.cubes[0]?.description).toBe('cube from compilerApi mock');
    expect(res.body.cubes[0]?.hasOwnProperty('sql')).toBe(true);
    expect(res.body.cubes[0]?.dimensions.find(dimension => dimension.name === 'Foo.id').description).toBe('id dimension from compilerApi mock');
    expect(res.body.cubes[0]?.measures.find(measure => measure.name === 'Foo.bar').description).toBe('measure from compilerApi mock');
    expect(res.body.cubes[0]?.segments.find(segment => segment.name === 'Foo.quux').description).toBe('segment from compilerApi mock');
  });

  describe('multi query support', () => {
    const searchParams = new URLSearchParams({
      query: JSON.stringify({
        measures: ['Foo.bar'],
        timeDimensions: [
          {
            dimension: 'Foo.time',
            granularity: 'day',
            compareDateRange: ['last week', 'this week'],
          },
        ],
      }),
      queryType: 'multi',
    });

    test('multi query with a flag', async () => {
      const { app } = await createApiGateway();

      const res = await request(app)
        .get(`/cubejs-api/v1/load?${searchParams.toString()}`)
        .set('Content-type', 'application/json')
        .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
        .expect(200);

      expect(res.body).toMatchObject({
        queryType: 'compareDateRangeQuery',
        pivotQuery: {
          measures: ['Foo.bar'],
          dimensions: ['compareDateRange'],
        },
      });
    });

    test('multi query without a flag', async () => {
      const { app } = await createApiGateway();

      searchParams.delete('queryType');

      await request(app)
        .get(`/cubejs-api/v1/load?${searchParams.toString()}`)
        .set('Content-type', 'application/json')
        .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
        .expect(400);
    });

    test('regular query', async () => {
      const { app } = await createApiGateway();

      const query = JSON.stringify({
        measures: ['Foo.bar'],
        timeDimensions: [
          {
            dimension: 'Foo.time',
            granularity: 'day',
          },
        ],
      });

      const res = await request(app)
        .get(`/cubejs-api/v1/load?query=${query}`)
        .set('Content-type', 'application/json')
        .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
        .expect(200);

      expect(res.body).toMatchObject({
        query: {
          measures: ['Foo.bar'],
          timeDimensions: [{ dimension: 'Foo.time', granularity: 'day' }],
        },
        data: [{ 'Foo.bar': 42 }],
      });
    });
  });

  describe('sql api member expressions evaluations', () => {
    const query = {
      measures: [
        // eslint-disable-next-line no-template-curly-in-string
        '{"cubeName":"sales","alias":"sum_sales_line_i","expr":{"type":"SqlFunction","cubeParams":["sales"],"sql":"SUM(${sales.line_items_price})"},"groupingSet":null}'
      ],
      dimensions: [
        // eslint-disable-next-line no-template-curly-in-string
        '{"cubeName":"sales","alias":"users_age","expr":{"type":"SqlFunction","cubeParams":["sales"],"sql":"${sales.users_age}"},"groupingSet":null}',
        // eslint-disable-next-line no-template-curly-in-string
        '{"cubeName":"sales","alias":"cast_sales_users","expr":{"type":"SqlFunction","cubeParams":["sales"],"sql":"CAST(${sales.users_first_name} AS TEXT)"},"groupingSet":null}'
      ],
      segments: [],
      order: []
    };

    test('throw error if expressions are not allowed', async () => {
      const { apiGateway } = await createApiGateway();
      const request: QueryRequest = {
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        res(message) {
          const errorMessage = message as { error: string };
          expect(errorMessage.error).toEqual('Error: Expressions are not allowed in this context');
        },
        query,
        expressionParams: [],
        exportAnnotatedSql: true,
        memberExpressions: false,
        disableExternalPreAggregations: true,
        queryType: 'multi',
        disableLimitEnforcing: true,
        context: {
          securityContext: {},
          signedWithPlaygroundAuthSecret: false,
          requestId: 'd592f44e-9c26-4187-aa09-e9d39ca19a88-span-1',
          protocol: 'postgres',
          apiType: 'sql',
          appName: 'NULL'
        },
        apiType: 'sql'
      };

      await apiGateway.sql(request);
    });

    test('no error if expressions are allowed', async () => {
      const { apiGateway } = await createApiGateway();
      const request: QueryRequest = {
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        res(message) {
          expect(message.hasOwnProperty('sql')).toBe(true);
        },
        query,
        expressionParams: [],
        exportAnnotatedSql: true,
        memberExpressions: true,
        disableExternalPreAggregations: true,
        queryType: 'multi',
        disableLimitEnforcing: true,
        context: {
          securityContext: {},
          signedWithPlaygroundAuthSecret: false,
          requestId: 'd592f44e-9c26-4187-aa09-e9d39ca19a88-span-1',
          protocol: 'postgres',
          apiType: 'sql',
          appName: 'NULL'
        },
        apiType: 'sql'
      };

      await apiGateway.sql(request);
    });
  });

  describe('/cubejs-system/v1', () => {
    const scheduledRefreshContextsFactory = () => ([
      { securityContext: { foo: 'bar' } },
      { securityContext: { bar: 'foo' } }
    ]);

    const scheduledRefreshTimeZonesFactory = () => (['UTC', 'America/Los_Angeles']);

    const appPrepareFactory = async (scope?: string[]) => {
      const playgroundAuthSecret = 'test12345';
      const { app } = await createApiGateway(
        new AdapterApiMock(),
        new DataSourceStorageMock(),
        {
          basePath: 'awesomepathtotest',
          playgroundAuthSecret,
          refreshScheduler: () => new RefreshSchedulerMock(),
          scheduledRefreshContexts: () => Promise.resolve(scheduledRefreshContextsFactory()),
          scheduledRefreshTimeZones: scheduledRefreshTimeZonesFactory
        }
      );
      const token = generateAuthToken({ uid: 5, scope }, {}, playgroundAuthSecret);
      const tokenUser = generateAuthToken({ uid: 5, }, {}, API_SECRET);

      return { app, token, tokenUser };
    };

    const notAllowedTestFactory = ({ route, method = 'get' }) => async () => {
      const { app } = await appPrepareFactory();
      return request(app)[method](`/cubejs-system/v1/${route}`)
        .set('Content-type', 'application/json')
        .expect(403);
    };

    const notAllowedWithUserTokenTestFactory = ({ route, method = 'get' }) => async () => {
      const { app, tokenUser } = await appPrepareFactory();

      return request(app)[method](`/cubejs-system/v1/${route}`)
        .set('Content-type', 'application/json')
        .set('Authorization', `Bearer ${tokenUser}`)
        .expect(403);
    };

    const notExistsTestFactory = ({ route, method = 'get' }) => async () => {
      const { app } = await createApiGateway();

      return request(app)[method](`/cubejs-system/v1/${route}`)
        .set('Content-type', 'application/json')
        .expect(404);
    };

    const successTestFactory = ({ route, method = 'get', successBody = {}, successResult, scope = [''] }) => async () => {
      const { app, token } = await appPrepareFactory(scope);

      const req = request(app)[method](`/cubejs-system/v1/${route}`)
        .set('Content-type', 'application/json')
        .set('Authorization', `Bearer ${token}`)
        .expect(200);

      if (method === 'post') req.send(successBody);

      const res = await req;
      expect(res.body).toMatchObject(successResult);
    };

    /*
     Test using this is commented out below
    const wrongPayloadsTestFactory = ({ route, wrongPayloads, scope }: {
      route: string,
      method: string,
      scope?: string[],
      wrongPayloads: {
        result: {
          status: number,
          error: string
        },
        body: {},
      }[]
    }) => async () => {
      const { app, token } = await appPrepareFactory(scope);

      for (const payload of wrongPayloads) {
        const req = request(app).post(`/cubejs-system/v1/${route}`)
          .set('Content-type', 'application/json')
          .set('Authorization', `Bearer ${token}`)
          .expect(payload.result.status);

        req.send(payload.body);
        const res = await req;
        expect(res.body.error).toBe(payload.result.error);
      }
    };
    */

    const testConfigs = [
      { route: 'context', successResult: { basePath: 'awesomepathtotest' } },
      { route: 'pre-aggregations', successResult: { preAggregations: preAggregationsResultFactory() } },
      { route: 'pre-aggregations/security-contexts', successResult: { securityContexts: scheduledRefreshContextsFactory().map(obj => obj.securityContext) } },
      { route: 'pre-aggregations/timezones', successResult: { timezones: scheduledRefreshTimeZonesFactory() } },
      {
        route: 'pre-aggregations/partitions',
        method: 'post',
        successBody: {
          query: {
            timezones: ['UTC'],
            preAggregations: [
              {
                id: 'cube.preAggregationName'
              }
            ]
          }
        },
        successResult: { preAggregationPartitions: preAggregationPartitionsResultFactory() }
      }
    ];

    testConfigs.forEach((config) => {
      describe(`/cubejs-system/v1/${config.route}`, () => {
        test('not allowed', notAllowedTestFactory(config));
        test('not allowed with user token', notAllowedWithUserTokenTestFactory(config));
        test('not route (works only with playgroundAuthSecret)', notExistsTestFactory(config));
        test('success', successTestFactory(config));
        /* if (config.method === 'post' && config.wrongPayloads?.length) {
          test('wrong params', wrongPayloadsTestFactory(config));
        } */
      });
    });
  });

  describe('/v1/pre-aggregations/jobs', () => {
    const scheduledRefreshContextsFactory = () => ([
      { securityContext: { foo: 'bar' } },
      { securityContext: { bar: 'foo' } }
    ]);

    const scheduledRefreshTimeZonesFactory = () => (['UTC', 'America/Los_Angeles']);

    const appPrepareFactory = async (scope: string[]) => {
      const playgroundAuthSecret = 'test12345';
      const { app } = await createApiGateway(
        new AdapterApiMock(),
        new DataSourceStorageMock(),
        {
          basePath: '/test',
          playgroundAuthSecret,
          refreshScheduler: () => new RefreshSchedulerMock(),
          scheduledRefreshContexts: () => Promise.resolve(scheduledRefreshContextsFactory()),
          scheduledRefreshTimeZones: scheduledRefreshTimeZonesFactory,
          contextToApiScopes: () => Promise.resolve(<ApiScopesTuple>scope)
        }
      );
      const token = generateAuthToken({ uid: 5, scope }, {}, playgroundAuthSecret);
      const tokenUser = generateAuthToken({ uid: 5, scope }, {}, API_SECRET);

      return { app, token, tokenUser };
    };

    test('no input', async () => {
      const { app, tokenUser } = await appPrepareFactory(['graphql', 'data', 'meta', 'jobs']);

      const req = request(app).post('/test/v1/pre-aggregations/jobs')
        .set('Content-type', 'application/json')
        .set('Authorization', `Bearer ${tokenUser}`);

      const res = await req;
      expect(res.status).toEqual(400);
      expect(res.body.error).toEqual('No job description provided');
    });

    test('invalid input action', async () => {
      const { app, tokenUser } = await appPrepareFactory(['graphql', 'data', 'meta', 'jobs']);

      const req = request(app).post('/test/v1/pre-aggregations/jobs')
        .set('Content-type', 'application/json')
        .set('Authorization', `Bearer ${tokenUser}`)
        .send({ action: 'patch' });

      const res = await req;
      expect(res.status).toEqual(400);
      expect(res.body.error.includes('Invalid Job query format')).toBeTruthy();
    });

    test('invalid input date range', async () => {
      const { app, tokenUser } = await appPrepareFactory(['graphql', 'data', 'meta', 'jobs']);

      let req = request(app).post('/test/v1/pre-aggregations/jobs')
        .set('Content-type', 'application/json')
        .set('Authorization', `Bearer ${tokenUser}`)
        .send({
          action: 'post',
          selector: {
            contexts: [{ securityContext: {} }],
            timezones: ['UTC', 'America/Los_Angeles'],
            dateRange: ['invalid string', '2020-02-20']
          }
        });

      let res = await req;
      expect(res.status).toEqual(400);
      expect(res.body.error.includes('Cannot parse selector date range')).toBeTruthy();

      req = request(app).post('/test/v1/pre-aggregations/jobs')
        .set('Content-type', 'application/json')
        .set('Authorization', `Bearer ${tokenUser}`)
        .send({
          action: 'post',
          selector: {
            contexts: [{ securityContext: {} }],
            timezones: ['UTC', 'America/Los_Angeles'],
            dateRange: ['2020-02-20', 'invalid string']
          }
        });

      res = await req;
      expect(res.status).toEqual(400);
      expect(res.body.error.includes('Cannot parse selector date range')).toBeTruthy();
    });
  });

  describe('healtchecks', () => {
    test('readyz (standalone)', async () => {
      const { app, adapterApi } = await createApiGateway();

      const res = await request(app)
        .get('/readyz')
        .set('Content-type', 'application/json')
        .expect(200);

      expect(res.body).toMatchObject({ health: 'HEALTH' });

      console.log(adapterApi);
      expect(adapterApi.$testConnectionsDone).toEqual(true);
      expect(adapterApi.$testOrchestratorConnectionsDone).toEqual(true);
    });

    test('readyz (standalone)', async () => {
      const { app, adapterApi } = await createApiGateway();

      const res = await request(app)
        .get('/readyz')
        .set('Content-type', 'application/json')
        .expect(200);

      expect(res.body).toMatchObject({ health: 'HEALTH' });

      console.log(adapterApi);
      expect(adapterApi.$testConnectionsDone).toEqual(true);
      expect(adapterApi.$testOrchestratorConnectionsDone).toEqual(true);
    });

    test('readyz (standalone) partial outage', async () => {
      class AdapterApiUnhealthyMock extends AdapterApiMock {
        public async testConnection() {
          this.$testConnectionsDone = true;

          throw new Error('It\'s expected exception for testing');

          return [];
        }
      }

      const { app, adapterApi } = await createApiGateway(new AdapterApiUnhealthyMock());

      const res = await request(app)
        .get('/readyz')
        .set('Content-type', 'application/json')
        .expect(500);

      expect(res.body).toMatchObject({ health: 'DOWN' });

      console.log(adapterApi);
      expect(adapterApi.$testConnectionsDone).toEqual(true);
      expect(adapterApi.$testOrchestratorConnectionsDone).toEqual(false);
    });

    test('livez (standalone) partial outage', async () => {
      class DataSourceStorageUnhealthyMock extends DataSourceStorageMock {
        public async testConnections() {
          this.$testConnectionsDone = true;

          throw new Error('It\'s expected exception for testing');

          return [];
        }
      }

      const { app, dataSourceStorage } = await createApiGateway(new AdapterApiMock(), new DataSourceStorageUnhealthyMock());

      const res = await request(app)
        .get('/livez')
        .set('Content-type', 'application/json')
        .expect(500);

      expect(res.body).toMatchObject({ health: 'DOWN' });

      expect(dataSourceStorage.$testConnectionsDone).toEqual(true);
      expect(dataSourceStorage.$testOrchestratorConnectionsDone).toEqual(false);
    });
  });
});

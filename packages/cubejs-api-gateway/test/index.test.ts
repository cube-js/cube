/* eslint-disable @typescript-eslint/no-shadow */
import express from 'express';
import request from 'supertest';

import { ApiGateway } from '../src';

const compilerApi = jest.fn().mockImplementation(() => ({
  async getSql() {
    return {
      sql: ['SELECT * FROM test', []],
      aliasNameToMember: {
        foo__bar: 'Foo.bar',
        foo__time: 'Foo.time',
      },
      order: [{ id: 'id', desc: true, }]
    };
  },

  async metaConfig() {
    return [
      {
        config: {
          name: 'Foo',
          measures: [
            {
              name: 'Foo.bar',
            },
          ],
          dimensions: [
            {
              name: 'Foo.id',
            },
            {
              name: 'Foo.time',
            },
          ],
        },
      },
    ];
  },
}));

class DataSourceStorageMock {
  public $testConnectionsDone: boolean = false;

  public $testOrchestratorConnectionsDone: boolean = false;

  public async testConnections() {
    this.$testConnectionsDone = true;

    return [];
  }

  public async testOrchestratorConnections() {
    this.$testOrchestratorConnectionsDone = true;

    return [];
  }
}

class AdapterApiMock {
  public $testConnectionsDone: boolean = false;

  public $testOrchestratorConnectionsDone: boolean = false;

  public async testConnection() {
    this.$testConnectionsDone = true;

    return [];
  }

  public async testOrchestratorConnections() {
    this.$testOrchestratorConnectionsDone = true;

    return [];
  }

  public async executeQuery() {
    return {
      data: [{ foo__bar: 42 }]
    };
  }
}

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

function createApiGateway(
  adapterApi: any = new AdapterApiMock(),
  dataSourceStorage: any = new DataSourceStorageMock(),
) {
  process.env.NODE_ENV = 'production';

  const apiGateway = new ApiGateway('secret', compilerApi, () => adapterApi, logger, {
    standalone: true,
    dataSourceStorage,
    basePath: '/cubejs-api',
    refreshScheduler: {},
  });

  process.env.NODE_ENV = 'unknown';
  const app = express();
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
    const { app } = createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}')
      .set('Authorization', 'foo')
      .expect(403);
    expect(res.body && res.body.error).toStrictEqual('Invalid token');
  });

  test('bad token with schema', async () => {
    const { app } = createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}')
      .set('Authorization', 'Bearer foo')
      .expect(403);
    expect(res.body && res.body.error).toStrictEqual('Invalid token');
  });

  test('requires auth', async () => {
    const { app } = createApiGateway();

    const res = await request(app).get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}').expect(403);
    expect(res.body && res.body.error).toStrictEqual('Authorization header isn\'t set');
  });

  test('passes correct token', async () => {
    const { app } = createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={}')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(400);
    expect(res.body && res.body.error).toStrictEqual(
      'Query should contain either measures, dimensions or timeDimensions with granularities in order to be valid'
    );
  });

  test('passes correct token with auth schema', async () => {
    const { app } = createApiGateway();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={}')
      .set('Authorization', 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(400);

    expect(res.body && res.body.error).toStrictEqual(
      'Query should contain either measures, dimensions or timeDimensions with granularities in order to be valid'
    );
  });

  test('null filter values', async () => {
    const { app } = createApiGateway();

    const res = await request(app)
      .get(
        '/cubejs-api/v1/load?query={"measures":["Foo.bar"],"filters":[{"dimension":"Foo.id","operator":"equals","values":[null]}]}'
      )
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);
    console.log(res.body);
    expect(res.body && res.body.data).toStrictEqual([{ 'Foo.bar': 42 }]);
  });

  test('dry-run', async () => {
    const { app } = createApiGateway();

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
              order: [],
              filters: [],
              dimensions: [],
              timeDimensions: [],
              queryType: 'regularQuery'
            }
          ],
          queryOrder: [{ id: 'desc' }],
          pivotQuery: {
            measures: ['Foo.bar'],
            timezone: 'UTC',
            order: [],
            filters: [],
            dimensions: [],
            timeDimensions: [],
            queryType: 'regularQuery'
          }
        });
      }
    );
  });

  test('date range padding', async () => {
    const { app } = createApiGateway();

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
    const { app } = createApiGateway();

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
    const { app } = createApiGateway();

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
    const { app } = createApiGateway();

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
      const { app } = createApiGateway();

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
      const { app } = createApiGateway();

      searchParams.delete('queryType');

      await request(app)
        .get(`/cubejs-api/v1/load?${searchParams.toString()}`)
        .set('Content-type', 'application/json')
        .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
        .expect(400);
    });

    test('regular query', async () => {
      const { app } = createApiGateway();

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

  describe('healtchecks', () => {
    test('readyz (standalone)', async () => {
      const { app, adapterApi } = createApiGateway();

      const res = await request(app)
        .get('/readyz')
        .set('Content-type', 'application/json')
        .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
        .expect(200);

      expect(res.body).toMatchObject({ health: 'HEALTH' });

      console.log(adapterApi);
      expect(adapterApi.$testConnectionsDone).toEqual(true);
      expect(adapterApi.$testOrchestratorConnectionsDone).toEqual(true);
    });

    test('readyz (standalone)', async () => {
      const { app, adapterApi } = createApiGateway();

      const res = await request(app)
        .get('/readyz')
        .set('Content-type', 'application/json')
        .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
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

      const { app, adapterApi } = createApiGateway(new AdapterApiUnhealthyMock());

      const res = await request(app)
        .get('/readyz')
        .set('Content-type', 'application/json')
        .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
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

      const { app, dataSourceStorage } = createApiGateway(new AdapterApiMock(), new DataSourceStorageUnhealthyMock());

      const res = await request(app)
        .get('/livez')
        .set('Content-type', 'application/json')
        .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
        .expect(500);

      expect(res.body).toMatchObject({ health: 'DOWN' });

      expect(dataSourceStorage.$testConnectionsDone).toEqual(true);
      expect(dataSourceStorage.$testOrchestratorConnectionsDone).toEqual(false);
    });
  });
});

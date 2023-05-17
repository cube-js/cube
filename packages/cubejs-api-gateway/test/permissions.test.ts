import express from 'express';
import request from 'supertest';
import { ApiGateway, ApiGatewayOptions, Query, Request } from '../src';
import {
  compilerApi,
  DataSourceStorageMock,
  AdapterApiMock
} from './mocks';

const API_SECRET = 'secret';
const AUTH_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M';
const logger = () => undefined;
function createApiGateway(
  options: Partial<ApiGatewayOptions> = {}
) {
  process.env.NODE_ENV = 'production';

  const app = express();
  const adapterApi: any = new AdapterApiMock();
  const dataSourceStorage: any = new DataSourceStorageMock();
  const apiGateway = new ApiGateway(API_SECRET, compilerApi, () => adapterApi, logger, {
    standalone: true,
    dataSourceStorage,
    basePath: '/cubejs-api',
    refreshScheduler: {},
    ...options,
  });
  apiGateway.initApp(app);
  return {
    app,
    apiGateway,
    dataSourceStorage,
    adapterApi
  };
}

describe('Gateway Api Scopes', () => {
  test('CUBEJS_DEFAULT_API_SCOPES', async () => {
    process.env.CUBEJS_DEFAULT_API_SCOPES = '';
    
    let res: request.Response;
    const { app, apiGateway } = createApiGateway();

    res = await request(app)
      .get('/cubejs-api/graphql')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);
    expect(res.body && res.body.error)
      .toStrictEqual('Api scope is missed: graphql');

    res = await request(app)
      .get('/cubejs-api/v1/meta')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);
    expect(res.body && res.body.error)
      .toStrictEqual('Api scope is missed: meta');

    res = await request(app)
      .get('/cubejs-api/v1/load')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);
    expect(res.body && res.body.error)
      .toStrictEqual('Api scope is missed: data');

    res = await request(app)
      .post('/cubejs-api/v1/pre-aggregations/jobs')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);
    expect(res.body && res.body.error)
      .toStrictEqual('Api scope is missed: jobs');

    delete process.env.CUBEJS_DEFAULT_API_SCOPES;
    apiGateway.release();
  });

  test('Liveliness declined', async () => {
    const { app, apiGateway } = createApiGateway({
      contextToApiScopes: async () => new Promise((resolve) => {
        resolve(['graphql', 'meta', 'data', 'jobs']);
      }),
    });
  
    await request(app)
      .get('/readyz')
      .set('Authorization', AUTH_TOKEN)
      .expect(200);
  
    await request(app)
      .get('/livez')
      .set('Authorization', AUTH_TOKEN)
      .expect(200);
  
    apiGateway.release();
  });

  test('GraphQL declined', async () => {
    const { app, apiGateway } = createApiGateway({
      contextToApiScopes: async () => new Promise((resolve) => {
        resolve(['liveliness', 'meta', 'data', 'jobs']);
      }),
    });

    const res = await request(app)
      .get('/cubejs-api/graphql')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res.body && res.body.error)
      .toStrictEqual('Api scope is missed: graphql');

    apiGateway.release();
  });

  test('Meta declined', async () => {
    const { app, apiGateway } = createApiGateway({
      contextToApiScopes: async () => new Promise((resolve) => {
        resolve(['liveliness', 'graphql', 'data', 'jobs']);
      }),
    });

    const res1 = await request(app)
      .get('/cubejs-api/v1/meta')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res1.body && res1.body.error)
      .toStrictEqual('Api scope is missed: meta');

    const res2 = await request(app)
      .post('/cubejs-api/v1/pre-aggregations/can-use')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res2.body && res2.body.error)
      .toStrictEqual('Api scope is missed: meta');

    apiGateway.release();
  });

  test('Data declined', async () => {
    const { app, apiGateway } = createApiGateway({
      contextToApiScopes: async () => new Promise((resolve) => {
        resolve(['liveliness', 'graphql', 'meta', 'jobs']);
      }),
    });

    const res1 = await request(app)
      .get('/cubejs-api/v1/load')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res1.body && res1.body.error)
      .toStrictEqual('Api scope is missed: data');

    const res2 = await request(app)
      .post('/cubejs-api/v1/load')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res2.body && res2.body.error)
      .toStrictEqual('Api scope is missed: data');

    const res3 = await request(app)
      .get('/cubejs-api/v1/subscribe')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res3.body && res3.body.error)
      .toStrictEqual('Api scope is missed: data');

    const res4 = await request(app)
      .get('/cubejs-api/v1/sql')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res4.body && res4.body.error)
      .toStrictEqual('Api scope is missed: data');

    const res5 = await request(app)
      .post('/cubejs-api/v1/sql')
      .set('Content-type', 'application/json')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res5.body && res5.body.error)
      .toStrictEqual('Api scope is missed: data');

    const res6 = await request(app)
      .get('/cubejs-api/v1/dry-run')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res6.body && res6.body.error)
      .toStrictEqual('Api scope is missed: data');

    const res7 = await request(app)
      .post('/cubejs-api/v1/dry-run')
      .set('Content-type', 'application/json')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res7.body && res7.body.error)
      .toStrictEqual('Api scope is missed: data');

    apiGateway.release();
  });

  test('Jobs declined', async () => {
    const { app, apiGateway } = createApiGateway({
      contextToApiScopes: async () => new Promise((resolve) => {
        resolve(['liveliness', 'graphql', 'data', 'meta']);
      }),
    });

    const res1 = await request(app)
      .post('/cubejs-api/v1/pre-aggregations/jobs')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res1.body && res1.body.error)
      .toStrictEqual('Api scope is missed: jobs');

    const res2 = await request(app)
      .get('/cubejs-api/v1/run-scheduled-refresh')
      .set('Authorization', AUTH_TOKEN)
      .expect(403);

    expect(res2.body && res2.body.error)
      .toStrictEqual('Api scope is missed: jobs');

    apiGateway.release();
  });
});

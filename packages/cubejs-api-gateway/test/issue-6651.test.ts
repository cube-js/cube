// https://github.com/cube-js/cube/issues/6651
// REST `/v1/load` answers a "Continue wait" with HTTP 200 (documented polling
// contract). The GraphQL endpoint rejects the resolver instead; since the root
// `cube` field is non-null, `data` becomes null and express-graphql answers 500.

// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
// eslint-disable-next-line import/no-extraneous-dependencies
import request from 'supertest';

import { ApiGateway } from '../src';
import { compilerApi, DataSourceStorageMock, AdapterApiMock } from './mocks';

const AUTH = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M';

class ContinueWaitAdapterApiMock extends AdapterApiMock {
  public async executeQuery(_query): Promise<any> {
    // What QueryOrchestrator throws while a query is still running past continueWaitTimeout
    // eslint-disable-next-line no-throw-literal
    throw { error: 'Continue wait' };
  }
}

async function createApp() {
  process.env.NODE_ENV = 'production';
  const withGraphQLSchemaCache = jest.fn().mockImplementation(async (...args: any[]) => {
    const api = await compilerApi(...args);
    let schema;
    return {
      ...api,
      getGraphQLSchema: () => schema,
      setGraphQLSchema: (s) => { schema = s; },
    };
  });

  const apiGateway = new ApiGateway('secret', withGraphQLSchemaCache, async () => new ContinueWaitAdapterApiMock(), () => undefined, {
    standalone: true,
    dataSourceStorage: new DataSourceStorageMock(),
    basePath: '/cubejs-api',
    refreshScheduler: {},
  } as any);
  process.env.NODE_ENV = 'unknown';

  const app = express();
  app.use(express.json());
  apiGateway.initApp(app);
  return app;
}

describe('issue #6651: GraphQL Continue wait status code', () => {
  test('REST /v1/load answers Continue wait with 200 (baseline)', async () => {
    const app = await createApp();
    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}')
      .set('Authorization', AUTH);

    expect(res.body.error).toBe('Continue wait');
    expect(res.status).toBe(200);
  });

  test('GraphQL answers Continue wait with 200 like REST', async () => {
    const app = await createApp();
    const res = await request(app)
      .post('/cubejs-api/graphql')
      .set('Content-Type', 'application/json')
      .set('Authorization', AUTH)
      .send({ query: 'query { cube { foo { bar } } }' });

    expect(res.body.errors?.[0]?.message).toBe('Continue wait');
    expect(res.status).toBe(200);
  });
});

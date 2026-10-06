// https://github.com/cube-js/cube/issues/6651
// A "Continue wait" answer is returned with HTTP 200 by the REST API, but the
// GraphQL API turns it into HTTP 500.

// eslint-disable-next-line import/no-extraneous-dependencies
import express from 'express';
// eslint-disable-next-line import/no-extraneous-dependencies
import request from 'supertest';

import { ApiGateway } from '../src';
import { generateAuthToken } from './utils';
import { AdapterApiMock, compilerApi, DataSourceStorageMock } from './mocks';

const API_SECRET = 'secret';
const logger = () => undefined;

class ContinueWaitAdapterApiMock extends AdapterApiMock {
  public async executeQuery(_query: any): Promise<any> {
    // The shape the query orchestrator throws for an in-flight query
    // (`ContinueWaitError`)
    // eslint-disable-next-line no-throw-literal
    throw { error: 'Continue wait' };
  }
}

async function createApp() {
  const graphqlCompilerApi = async (...args: any[]) => {
    const api = await (compilerApi as any)(...args);
    let schema;

    return {
      ...api,
      getGraphQLSchema: () => schema,
      setGraphQLSchema: (s) => {
        schema = s;
      },
    };
  };

  const apiGateway = new ApiGateway(
    API_SECRET,
    graphqlCompilerApi as any,
    async () => new ContinueWaitAdapterApiMock() as any,
    logger,
    {
      standalone: true,
      dataSourceStorage: new DataSourceStorageMock() as any,
      basePath: '/cubejs-api',
      refreshScheduler: {} as any,
    }
  );

  const app = express();
  app.use(express.json());
  apiGateway.initApp(app);

  return app;
}

describe('issue #6651: Continue wait over GraphQL', () => {
  const token = generateAuthToken({ uid: 5 }, {}, API_SECRET);

  test('REST API answers Continue wait with HTTP 200 (control)', async () => {
    const app = await createApp();

    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}')
      .set('Authorization', token);

    expect(res.body).toMatchObject({ error: 'Continue wait' });
    expect(res.status).toBe(200);
  });

  test('GraphQL API answers Continue wait with HTTP 200, like REST', async () => {
    const app = await createApp();

    const res = await request(app)
      .post('/cubejs-api/graphql')
      .set('Content-Type', 'application/json')
      .set('Authorization', token)
      .send({ query: '{ cube { foo { bar } } }' });

    expect(res.body.errors?.[0]?.message).toBe('Continue wait');
    expect(res.status).toBe(200);
  });
});

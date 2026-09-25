import express from 'express';
import jwt from 'jsonwebtoken';
import request from 'supertest';
import { ApiGateway } from '../src';
import { SQLServer, SQLServerConstructorOptions } from '../src/sql-server';
import { compilerApi, AdapterApiMock, DataSourceStorageMock } from './mocks';

// Taken off the SQLServer surface rather than imported from the native
// package, which would load the binary just to read two types
type SqlFiltersResponse = Awaited<ReturnType<SQLServer['getSqlFilters']>>;
type SqlFilterItem = Parameters<SQLServer['addSqlFilters']>[1][number];

const logger = (type: any, message: any) => console.log({ type, ...message });

const OK_RESPONSE: SqlFiltersResponse = {
  status: 'ok',
  sql: 'SELECT 1',
  filters: [],
};

/**
 * Stands in for the native layer: records what the gateway passed down and
 * answers with whatever the test asked for, so that the checks the gateway
 * makes before the call can be told apart from the ones it doesn't.
 */
class SqlServerMock extends SQLServer {
  public calls: { method: string, args: any[] }[] = [];

  public response: SqlFiltersResponse = OK_RESPONSE;

  /** Thrown instead of answering, the way an internal native failure is. */
  public failure: Error | null = null;

  private record(method: string, args: any[]): Promise<SqlFiltersResponse> {
    this.calls.push({ method, args });

    return this.failure ? Promise.reject(this.failure) : Promise.resolve(this.response);
  }

  public async getSqlFilters(sqlQuery: string, securityContext?: unknown) {
    return this.record('getSqlFilters', [sqlQuery, securityContext]);
  }

  public async addSqlFilters(sqlQuery: string, filters: SqlFilterItem[], securityContext?: unknown) {
    return this.record('addSqlFilters', [sqlQuery, filters, securityContext]);
  }

  public async setSqlFilters(sqlQuery: string, filters: SqlFilterItem[], securityContext?: unknown) {
    return this.record('setSqlFilters', [sqlQuery, filters, securityContext]);
  }

  public async deleteSqlFilters(sqlQuery: string, filters: SqlFilterItem[], securityContext?: unknown) {
    return this.record('deleteSqlFilters', [sqlQuery, filters, securityContext]);
  }

  public async replaceSqlFilters(
    sqlQuery: string,
    oldFilters: SqlFilterItem[],
    newFilters: SqlFilterItem[],
    securityContext?: unknown,
  ) {
    return this.record('replaceSqlFilters', [sqlQuery, oldFilters, newFilters, securityContext]);
  }
}

class TestApiGateway extends ApiGateway {
  protected createSQLServerInstance(options: SQLServerConstructorOptions): SQLServer {
    return new SqlServerMock(this, options);
  }

  // Read back off the gateway rather than kept in a field of its own: the
  // base constructor builds the server, and a field declared here would be
  // defined after that and clear it
  public get sqlServerMock(): SqlServerMock {
    return this.getSQLServer() as SqlServerMock;
  }

  public async getSqlFiltersPublic(args: any) {
    return super.getSqlFilters(args);
  }

  public async modifySqlFiltersPublic(args: any) {
    return super.modifySqlFilters(args);
  }
}

function createGateway(options: { scopes?: string[] } = {}) {
  return new TestApiGateway('secret', compilerApi, async () => new AdapterApiMock(), logger, {
    standalone: true,
    dataSourceStorage: new DataSourceStorageMock(),
    basePath: '/cubejs-api',
    refreshScheduler: {},
    ...(options.scopes ? { contextToApiScopes: async () => options.scopes } : {}),
  } as any);
}

async function read(gateway: TestApiGateway): Promise<Answer> {
  let answer: Answer = { result: undefined, status: 200 };

  await gateway.getSqlFiltersPublic({
    query: 'SELECT 1',
    context: { requestId: 'sql-filters-test', securityContext: {} },
    res: (result: any, options?: { status?: number }) => {
      answer = { result, status: options?.status ?? 200 };
    },
  });

  return answer;
}

type Answer = { result: any, status: number };

async function modify(gateway: TestApiGateway, body: Record<string, unknown>): Promise<Answer> {
  let answer: Answer = { result: undefined, status: 200 };

  await gateway.modifySqlFiltersPublic({
    query: 'SELECT 1',
    ...body,
    context: { requestId: 'sql-filters-test', securityContext: {} },
    res: (result: any, options?: { status?: number }) => {
      answer = { result, status: options?.status ?? 200 };
    },
  });

  return answer;
}

describe('sql filters endpoint', () => {
  test('both routes require the sql scope before anything reaches the native layer', async () => {
    const gateway = createGateway({ scopes: ['data', 'meta'] });

    for (const answer of [await modify(gateway, { set: [] }), await read(gateway)]) {
      expect(answer.status).toBe(403);
      expect(answer.result.error).toBe('API scope is missing: sql');
    }
    expect(gateway.sqlServerMock.calls).toHaveLength(0);
  });

  test('exactly one operation is required', async () => {
    const gateway = createGateway();

    const none = await modify(gateway, {});
    expect(none.status).toBe(400);
    expect(none.result.error).toContain('Exactly one of add, set, delete or replace');

    const two = await modify(gateway, { add: [], set: [] });
    expect(two.status).toBe(400);
    expect(two.result.error).toContain('Exactly one of add, set, delete or replace');

    expect(gateway.sqlServerMock.calls).toHaveLength(0);
  });

  test('each operation reaches its own native call', async () => {
    const gateway = createGateway();
    const filter = { member: 'Foo.bar', operator: 'equals', values: ['1'] };

    await modify(gateway, { add: [filter] });
    await modify(gateway, { set: [filter] });
    await modify(gateway, { delete: [filter] });
    await modify(gateway, { replace: { old: [filter], new: [filter] } });

    expect(gateway.sqlServerMock.calls.map(({ method }) => method)).toEqual([
      'addSqlFilters',
      'setSqlFilters',
      'deleteSqlFilters',
      'replaceSqlFilters',
    ]);
  });

  test('a query is required', async () => {
    const gateway = createGateway();

    const answer = await modify(gateway, { query: '   ', add: [] });

    expect(answer.status).toBe(400);
    expect(answer.result.error).toContain('query parameter must be a non-empty string');
    expect(gateway.sqlServerMock.calls).toHaveLength(0);
  });

  test('filters must be an array', async () => {
    const gateway = createGateway();

    const answer = await modify(gateway, { add: { member: 'Foo.bar' } });

    expect(answer.status).toBe(400);
    expect(answer.result.error).toContain('add parameter must be an array of filters');

    // and each entry, at any depth, an object
    for (const filters of [['oops'], [null], [{ or: [42] }]]) {
      const rejected = await modify(gateway, { add: filters });
      expect(rejected.status).toBe(400);
      expect(rejected.result.error).toBe('each filter must be an object');
    }

    // and the groups nest to a bounded depth
    let deep: any = { member: 'Foo.bar', operator: 'set' };

    for (let i = 0; i < 40; i++) {
      deep = { or: [deep] };
    }

    const tooDeep = await modify(gateway, { add: [deep] });
    expect(tooDeep.status).toBe(400);
    expect(tooDeep.result.error).toBe('filter groups may nest at most 32 levels deep');
  });

  test('numbers and booleans in values are handed on as strings, as /v1/load takes them', async () => {
    const gateway = createGateway();

    await modify(gateway, {
      add: [
        { member: 'Foo.price', operator: 'gt', values: [100] },
        { or: [{ member: 'Foo.flag', operator: 'equals', values: [true, 'x'] }, { member: 'Foo.n', operator: 'lt', values: [2.5] }] },
      ],
    });

    expect(gateway.sqlServerMock.calls[0].args[1]).toEqual([
      { member: 'Foo.price', operator: 'gt', values: ['100'] },
      { or: [{ member: 'Foo.flag', operator: 'equals', values: ['true', 'x'] }, { member: 'Foo.n', operator: 'lt', values: ['2.5'] }] },
    ]);

    const rejected = await modify(gateway, { add: [{ member: 'Foo.bar', operator: 'equals', values: [null] }] });
    expect(rejected.status).toBe(400);
    expect(rejected.result.error).toContain('filter values must be strings, numbers or booleans');
  });

  test('replace must carry both filter arrays', async () => {
    const gateway = createGateway();

    const notAnObject = await modify(gateway, { replace: [] });
    expect(notAnObject.status).toBe(400);
    expect(notAnObject.result.error).toContain('replace parameter must be an object');

    const missingNew = await modify(gateway, { replace: { old: [] } });
    expect(missingNew.status).toBe(400);
    expect(missingNew.result.error).toContain('replace.new parameter must be an array of filters');

    expect(gateway.sqlServerMock.calls).toHaveLength(0);
  });

  test('a planning failure reported in-band is answered as a bad request', async () => {
    const gateway = createGateway();
    gateway.sqlServerMock.response = {
      status: 'error',
      error: 'Failed to plan the query: no such member',
    };

    const answer = await modify(gateway, { add: [] });

    expect(answer.status).toBe(400);
    expect(answer.result.error).toContain('Failed to plan the query');
  });

  test('a successful result is answered as it stands', async () => {
    const gateway = createGateway();
    gateway.sqlServerMock.response = {
      status: 'ok',
      sql: 'SELECT 1 WHERE x = 1',
      filters: [{ member: 'Foo.bar', operator: 'equals', values: ['1'] }],
    };

    const answer = await modify(gateway, { set: [] });

    expect(answer.status).toBe(200);
    expect(answer.result.sql).toBe('SELECT 1 WHERE x = 1');
    expect(answer.result.filters).toHaveLength(1);
  });

  test('a failure thrown by the native layer is not a bad request', async () => {
    const gateway = createGateway();
    gateway.sqlServerMock.failure = new Error('Failed to plan the rewritten query: rewrite bug');

    const answer = await modify(gateway, { add: [] });

    expect(answer.status).toBe(500);
    expect(answer.result.error).toContain('rewrite bug');
  });

  test('reading filters passes the query down and answers with the result', async () => {
    const gateway = createGateway();
    const answer = await read(gateway);

    expect(gateway.sqlServerMock.calls).toEqual([
      { method: 'getSqlFilters', args: ['SELECT 1', {}] },
    ]);
    expect(answer.status).toBe(200);
    expect(answer.result.status).toBe('ok');
  });
});

describe('sql filters routes', () => {
  // The handlers above are called with an assembled argument object; these
  // go through express, so the request-to-argument mapping is what is tested
  // An empty claim set signed with the secret the gateway is built with
  const AUTH_TOKEN = jwt.sign({}, 'secret', { algorithm: 'HS256', noTimestamp: true });
  let nodeEnv: string | undefined;

  beforeAll(() => {
    nodeEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = 'production';
  });

  afterAll(() => {
    process.env.NODE_ENV = nodeEnv;
  });

  function createApp() {
    const gateway = createGateway({ scopes: ['sql'] });
    const app = express();
    gateway.initApp(app);

    return { app, gateway };
  }

  test('GET reads the query from the query string', async () => {
    const { app, gateway } = createApp();

    const res = await request(app)
      .get('/cubejs-api/v1/sql-filters')
      .query({ query: 'SELECT 1' })
      .set('Authorization', AUTH_TOKEN)
      .expect(200);

    expect(res.body.status).toBe('ok');
    expect(gateway.sqlServerMock.calls).toEqual([{ method: 'getSqlFilters', args: ['SELECT 1', {}] }]);
  });

  test('POST reads the query and each operation from the body', async () => {
    const { app, gateway } = createApp();
    const filter = { member: 'Foo.bar', operator: 'equals', values: ['1'] };

    for (const body of [
      { query: 'SELECT 1', add: [filter] },
      { query: 'SELECT 1', set: [filter] },
      { query: 'SELECT 1', delete: [filter] },
      { query: 'SELECT 1', replace: { old: [filter], new: [filter] } },
    ]) {
      await request(app)
        .post('/cubejs-api/v1/sql-filters')
        .set('Authorization', AUTH_TOKEN)
        .send(body)
        .expect(200);
    }

    expect(gateway.sqlServerMock.calls).toEqual([
      { method: 'addSqlFilters', args: ['SELECT 1', [filter], {}] },
      { method: 'setSqlFilters', args: ['SELECT 1', [filter], {}] },
      { method: 'deleteSqlFilters', args: ['SELECT 1', [filter], {}] },
      { method: 'replaceSqlFilters', args: ['SELECT 1', [filter], [filter], {}] },
    ]);
  });

  test('a rejected POST is a 400 through the route too', async () => {
    const { app } = createApp();

    const res = await request(app)
      .post('/cubejs-api/v1/sql-filters')
      .set('Authorization', AUTH_TOKEN)
      .send({ query: 'SELECT 1' })
      .expect(400);

    expect(res.body.error).toBe('Exactly one of add, set, delete or replace parameters is required');
  });
});

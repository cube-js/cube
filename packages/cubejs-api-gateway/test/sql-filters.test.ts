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

  private record(method: string, args: any[]): Promise<SqlFiltersResponse> {
    this.calls.push({ method, args });

    return Promise.resolve(this.response);
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

function createGateway() {
  return new TestApiGateway('secret', compilerApi, async () => new AdapterApiMock(), logger, {
    standalone: true,
    dataSourceStorage: new DataSourceStorageMock(),
    basePath: '/cubejs-api',
    refreshScheduler: {},
  });
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

/** A group holding `count` leaves in one entry of the filter array. */
function groupOf(count: number) {
  return {
    or: Array.from({ length: count }, (_, i) => ({
      member: 'Foo.bar',
      operator: 'equals',
      values: [`${i}`],
    })),
  };
}

describe('sql filters endpoint', () => {
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

  test('the limit counts the leaves of a group, not the entries of the array', async () => {
    const gateway = createGateway();

    // One array entry, 501 leaves inside it
    const overTheLimit = await modify(gateway, { add: [groupOf(501)] });
    expect(overTheLimit.status).toBe(400);
    expect(overTheLimit.result.error).toContain('at most 500 filters');

    const atTheLimit = await modify(gateway, { add: [groupOf(500)] });
    expect(atTheLimit.status).toBe(200);

    // Nesting is walked to the bottom rather than counted a level at a time
    const nested = await modify(gateway, { add: [{ and: [groupOf(250), groupOf(251)] }] });
    expect(nested.status).toBe(400);
    expect(nested.result.error).toContain('at most 500 filters');
  });

  test('a group holding nothing still counts as a filter', async () => {
    const gateway = createGateway();

    const filters = Array.from({ length: 501 }, () => ({ and: [] }));
    const answer = await modify(gateway, { add: filters });

    expect(answer.status).toBe(400);
    expect(answer.result.error).toContain('at most 500 filters');
  });

  test('a planning failure reported in-band is answered as a bad request', async () => {
    const gateway = createGateway();
    gateway.sqlServerMock.response = {
      status: 'error',
      error: 'Failed to plan the query: no such member',
    } as SqlFiltersResponse;

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
    } as SqlFiltersResponse;

    const answer = await modify(gateway, { set: [] });

    expect(answer.status).toBe(200);
    expect(answer.result.sql).toBe('SELECT 1 WHERE x = 1');
    expect(answer.result.filters).toHaveLength(1);
  });

  test('reading filters passes the query down and answers with the result', async () => {
    const gateway = createGateway();
    let answer: Answer = { result: undefined, status: 200 };

    await gateway.getSqlFiltersPublic({
      query: 'SELECT 1',
      context: { requestId: 'sql-filters-test', securityContext: {} },
      res: (result: any, options?: { status?: number }) => {
        answer = { result, status: options?.status ?? 200 };
      },
    });

    expect(gateway.sqlServerMock.calls).toEqual([
      { method: 'getSqlFilters', args: ['SELECT 1', {}] },
    ]);
    expect(answer.status).toBe(200);
    expect(answer.result.status).toBe('ok');
  });
});

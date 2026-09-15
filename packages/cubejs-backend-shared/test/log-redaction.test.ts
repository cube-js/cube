/* eslint-disable quotes */
import {
  REDACTED,
  redactLogParams,
  withLogRedaction,
} from '../src/log-redaction';
import { getEnv } from '../src/env';

describe('redactLogParams', () => {
  const securityContext = { tenant: 'acme', scope: ['agents-config'] };

  it('swaps in the redacted twins cubesql attaches beside a SQL API statement and its error', () => {
    const params = {
      query: { sql: "SELECT * FROM Orders WHERE email = 'john@example.com' LIMIT 10" },
      redactedQuery: { sql: "SELECT * FROM Orders WHERE email = 'redacted' LIMIT 10" },
      error: "Unsupported query type: SELECT 'john@example.com'",
      redactedError: "Unsupported query type: SELECT 'redacted'",
      apiType: 'sql',
      isDataQuery: true,
      securityContext,
    };

    expect(redactLogParams(params)).toEqual({
      query: { sql: "SELECT * FROM Orders WHERE email = 'redacted' LIMIT 10" },
      error: "Unsupported query type: SELECT 'redacted'",
      apiType: 'sql',
      isDataQuery: true,
      securityContext,
    });
  });

  it('drops whatever sits under query.sql when no redacted twin came with it', () => {
    // A producer that logs a SQL API statement without attaching its twin
    expect(redactLogParams({ query: { sql: "SELECT * FROM Orders WHERE email = 'john@example.com'" }, error: 'boom' }))
      .toEqual({ query: { sql: REDACTED }, error: 'boom' });
    // A malformed request body logged in place of a statement
    expect(redactLogParams({ query: { sql: { email: 'john@example.com' } }, error: 'Invalid query format' }))
      .toEqual({ query: { sql: REDACTED }, error: 'Invalid query format' });
  });

  it('redacts filter values in a Cube query, and nothing else', () => {
    const query = {
      measures: ['Orders.count'],
      dimensions: ['Orders.status'],
      segments: ['Orders.paid'],
      timeDimensions: [{ dimension: 'Orders.createdAt', granularity: 'day', dateRange: ['2024-01-01', '2024-01-31'] }],
      filters: [
        { member: 'Orders.email', operator: 'equals', values: ['john@example.com'] },
        {
          or: [
            { member: 'Orders.card', operator: 'equals', values: ['4111111111111111', '4222222222222222'] },
            { and: [{ dimension: 'Orders.phone', operator: 'contains', values: ['555'] }] },
          ],
        },
        { member: 'Orders.note', operator: 'equals', values: 'bare value' as any },
      ],
      order: [['Orders.count', 'desc']],
      limit: 100,
    };

    expect(redactLogParams({ query, requestId: 'r1' })).toEqual({
      query: {
        ...query,
        filters: [
          { member: 'Orders.email', operator: 'equals', values: [REDACTED] },
          {
            or: [
              { member: 'Orders.card', operator: 'equals', values: [REDACTED, REDACTED] },
              { and: [{ dimension: 'Orders.phone', operator: 'contains', values: [REDACTED] }] },
            ],
          },
          { member: 'Orders.note', operator: 'equals', values: REDACTED },
        ],
      },
      requestId: 'r1',
    });
  });

  it('redacts every query of a blending request', () => {
    const params = {
      normalizedQueries: [
        { measures: ['A.count'], filters: [{ member: 'A.x', operator: 'equals', values: ['pii-1'] }] },
        { measures: ['B.count'], filters: [{ member: 'B.x', operator: 'equals', values: ['pii-2'] }] },
      ],
    };

    expect(JSON.stringify(redactLogParams(params))).not.toContain('pii');
  });

  it('redacts the parameters of a statement logged next to its SQL, keeping the SQL', () => {
    const params = {
      query: "SELECT * FROM orders WHERE email = $1 AND created_at > '2024-01-01'",
      values: ['john@example.com'],
      cacheKey: ['SELECT 1', ['john@example.com']],
      dataSource: 'default',
      requestId: 'r1',
    };

    expect(redactLogParams(params)).toEqual({
      query: "SELECT * FROM orders WHERE email = $1 AND created_at > '2024-01-01'",
      values: [REDACTED],
      cacheKey: ['SELECT 1', [REDACTED]],
      dataSource: 'default',
      requestId: 'r1',
    });
  });

  it('redacts the params of [sql, params] tuples: sqlQuery.sql, pre-aggregation loadSql, queryKey', () => {
    const params = {
      sqlQuery: {
        sql: ['SELECT * FROM orders WHERE email = ? AND status = ?', ['john@example.com', 'shipped']],
        preAggregations: [{
          loadSql: ['SELECT * FROM orders WHERE created_at >= ?', ['2024-01-01']],
          invalidateKeyQueries: [['SELECT MAX(updated_at) FROM orders WHERE id = ?', [7], { renewalThreshold: 10 }]],
        }],
        external: false,
      },
      queryKey: ['SELECT 1 WHERE a = ?', ['secret'], [['SELECT 2', ['also secret']]]],
    };

    expect(redactLogParams(params)).toEqual({
      sqlQuery: {
        sql: ['SELECT * FROM orders WHERE email = ? AND status = ?', [REDACTED, REDACTED]],
        preAggregations: [{
          loadSql: ['SELECT * FROM orders WHERE created_at >= ?', [REDACTED]],
          invalidateKeyQueries: [['SELECT MAX(updated_at) FROM orders WHERE id = ?', [REDACTED], { renewalThreshold: 10 }]],
        }],
        external: false,
      },
      queryKey: ['SELECT 1 WHERE a = ?', [REDACTED], [['SELECT 2', [REDACTED]]]],
    });
  });

  it('redacts statement parameters under every key producers use', () => {
    // OrchestratorApi: Query started / Query completed / Continue wait / Error querying db
    expect(redactLogParams({ query: 'SELECT * FROM orders WHERE email = ?', params: ['john@example.com'], requestId: 'r1' }))
      .toEqual({ query: 'SELECT * FROM orders WHERE email = ?', params: [REDACTED], requestId: 'r1' });
    // QueryCache: Streaming done with error / Error while renew cycle
    expect(redactLogParams({ query: 'SELECT ?', query_values: ['john@example.com'], error: 'boom' }))
      .toEqual({ query: 'SELECT ?', query_values: [REDACTED], error: 'boom' });
    // PreAggregationLoader: Executing Load Pre Aggregation SQL, no SQL text beside the params
    const build = {
      queryKeyMd5: 'abc',
      values: ['2024-01-01', 'john@example.com'],
      targetTableName: 'dev_pre_aggregations.orders_main_abc',
      requestId: 'r1',
      newVersionEntry: { table_name: 'dev_pre_aggregations.orders_main', content_version: 'v1' },
    };
    expect(redactLogParams(build)).toEqual({ ...build, values: [REDACTED, REDACTED] });
  });

  it('leaves a values key alone when it is neither a filter leaf nor statement parameters', () => {
    const params = { values: [1, 2], meta: { values: ['a'] } };
    expect(redactLogParams(params)).toEqual(params);
  });

  it('redacts the rows of an inline table, keeping its shape', () => {
    // A lambda pre-aggregation's downloaded rows travel with the statement that reads them
    const params = {
      query: 'SELECT * FROM orders_lambda WHERE status = ?',
      values: ['shipped'],
      inlineTables: [{ name: 'orders_lambda', columns: [{ name: 'email', type: 'text' }], csvRows: 'email\njohn@example.com' }],
    };

    expect(redactLogParams(params)).toEqual({
      query: 'SELECT * FROM orders_lambda WHERE status = ?',
      values: [REDACTED],
      inlineTables: [{ name: 'orders_lambda', columns: [{ name: 'email', type: 'text' }], csvRows: REDACTED }],
    });
  });

  it('redacts params and query_values whatever sits beside them', () => {
    expect(redactLogParams({ params: ['john@example.com'], requestId: 'r1' }))
      .toEqual({ params: [REDACTED], requestId: 'r1' });
    expect(redactLogParams({ error: 'boom', query_values: ['john@example.com'] }))
      .toEqual({ error: 'boom', query_values: [REDACTED] });
  });

  it('does not mutate the input', () => {
    const params = {
      query: { filters: [{ member: 'A.x', operator: 'equals', values: ['pii'] }] },
      values: ['pii'],
      queryKey: ['SELECT ?', ['pii']],
    };
    const snapshot = JSON.parse(JSON.stringify(params));

    redactLogParams(params);

    expect(params).toEqual(snapshot);
  });

  it('survives shared references and cycles', () => {
    const leaf = { member: 'A.x', operator: 'equals', values: ['pii'] };
    const params: Record<string, any> = { first: leaf, second: leaf };
    params.self = params;

    const redacted = redactLogParams(params);

    expect(redacted.first.values).toEqual([REDACTED]);
    expect(redacted.second.values).toEqual([REDACTED]);
    expect(redacted.self).toBe(redacted);
  });

  it('passes through primitives and non-plain objects', () => {
    const error = new Error('boom');
    const startedAt = new Date();
    const params = { error, startedAt, duration: 12, flag: true, nothing: null, missing: undefined };

    const redacted = redactLogParams(params);

    expect(redacted.error).toBe(error);
    expect(redacted.startedAt).toBe(startedAt);
    expect(redacted).toEqual(params);
  });
});

describe('withLogRedaction', () => {
  it('hands the wrapped logger the message and redacted params', () => {
    const logger = jest.fn();
    const redacting = withLogRedaction(logger);

    redacting('Executing SQL', { query: 'SELECT ?', values: ['pii'], requestId: 'r1' });
    redacting('Server Start', undefined as any);

    expect(logger).toHaveBeenNthCalledWith(1, 'Executing SQL', { query: 'SELECT ?', values: [REDACTED], requestId: 'r1' });
    expect(logger).toHaveBeenNthCalledWith(2, 'Server Start', undefined);
  });
});

describe('CUBEJS_LOG_REDACTION', () => {
  const nodeEnv = process.env.NODE_ENV;

  afterEach(() => {
    delete process.env.CUBEJS_LOG_REDACTION;
    delete process.env.CUBEJS_DEV_MODE;
    process.env.NODE_ENV = nodeEnv;
  });

  it('is on by default in production', () => {
    process.env.NODE_ENV = 'production';
    expect(getEnv('logRedaction')).toBe(true);
  });

  it('is off by default in development mode, as the dev server decides it', () => {
    process.env.NODE_ENV = 'production';
    process.env.CUBEJS_DEV_MODE = 'true';
    expect(getEnv('logRedaction')).toBe(false);

    delete process.env.CUBEJS_DEV_MODE;
    process.env.NODE_ENV = 'development';
    expect(getEnv('logRedaction')).toBe(false);

    delete process.env.NODE_ENV;
    expect(getEnv('logRedaction')).toBe(false);
  });

  it('follows an explicit value in either mode', () => {
    process.env.NODE_ENV = 'production';
    process.env.CUBEJS_LOG_REDACTION = 'false';
    expect(getEnv('logRedaction')).toBe(false);

    process.env.NODE_ENV = 'development';
    process.env.CUBEJS_LOG_REDACTION = 'true';
    expect(getEnv('logRedaction')).toBe(true);
  });
});

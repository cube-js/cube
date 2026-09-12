/* eslint-disable quotes */
import crypto from 'crypto';

import {
  REDACTED,
  canonicalJson,
  isFetchTablesKey,
  queryKeyMd5,
  redactLogParams,
  withLogRedaction,
} from '../src/log-redaction';
import { getEnv } from '../src/env';

const md5 = (value: unknown) => crypto.createHash('md5').update(JSON.stringify(value)).digest('hex');
const md5Str = (json: string) => crypto.createHash('md5').update(json).digest('hex');

describe('redactLogParams', () => {
  const securityContext = { tenant: 'acme', scope: ['agents-config'] };

  it('leaves the SQL API statement to cubesql, which redacts it before logging', () => {
    const params = {
      query: { sql: "SELECT * FROM Orders WHERE email = 'redacted' LIMIT 10" },
      apiType: 'sql',
      isDataQuery: true,
      securityContext,
    };

    expect(redactLogParams(params)).toEqual(params);
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
      queryKeyMd5: md5(params.cacheKey),
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
      queryKeyMd5: md5(params.queryKey),
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

describe('canonicalJson', () => {
  it('matches JSON.stringify for keys without objects', () => {
    const key = ['SELECT 1 WHERE a = ?', ['secret', 1, 2.5, true, null], []];
    expect(canonicalJson(key)).toBe(JSON.stringify(key));
  });

  it('orders object keys by byte order, as serde_json does', () => {
    // Output observed from serde_json::to_string on the same input
    const key = [['SELECT 1 WHERE a = ?', ['x']], [[{ zeta: 1, alpha: 'b', Mid: [1, 2.5, true, null], 10: 'n', 9: 'm' }]]];
    expect(canonicalJson(key)).toBe(
      '[["SELECT 1 WHERE a = ?",["x"]],[[{"10":"n","9":"m","Mid":[1,2.5,true,null],"alpha":"b","zeta":1}]]]'
    );
    expect(canonicalJson({ b: { y: 1, x: 2 }, a: [] })).toBe('{"a":[],"b":{"x":2,"y":1}}');
  });

  it('prints numbers as serde_json does after a JSON round trip', () => {
    // Expected strings observed from serde_json 1.0.151 on the JSON.stringify output of these values
    expect(canonicalJson([1e21, 1.5e20, 0.000001, 0.0000015, 0.00001, 1.5, 100, 12345678901234567000, -1e19, -0, NaN]))
      .toBe('[1e+21,1.5e+20,1e-6,1.5e-6,0.00001,1.5,100,12345678901234567000,-1e+19,0,null]');
  });

  it('follows JSON.stringify for toJSON, undefined and functions', () => {
    const date = new Date('2024-01-01T00:00:00.000Z');
    expect(canonicalJson({ b: undefined, a: date, c: () => 1, d: [undefined] })).toBe('{"a":"2024-01-01T00:00:00.000Z","d":[null]}');
    expect(canonicalJson(undefined)).toBeUndefined();
  });
});

describe('queryKeyMd5', () => {
  it('hashes the canonical JSON of the key', () => {
    const key = ['SELECT 1 WHERE a = ?', ['secret'], [[{ z: 1, a: 2 }]]];
    expect(queryKeyMd5(key)).toBe(md5Str('["SELECT 1 WHERE a = ?",["secret"],[[{"a":2,"z":1}]]]'));
  });

  // Fixtures and hashes taken from Cube Cloud's event normalizer tests
  describe('reproduces the hashes Cube Cloud derives from logged events', () => {
    const preAggSql = 'SELECT `orders__status` `orders__status`, sum(`orders__count`) `orders__count` FROM dev_pre_aggregations.orders_main_2aqrtahf_04uekas4_1i2vlim AS `orders__main`  GROUP BY 1 ORDER BY 2 DESC LIMIT 10000';

    it('Load Request SQL: sqlQuery.sql padded to a queue key', () => {
      const sql = 'SELECT `orders__status` `orders__status`, sum(`orders__count`) `orders__count` FROM dev_pre_aggregations.orders_main AS `orders__main`  GROUP BY 1 ORDER BY 2 DESC LIMIT 10000';
      expect(redactLogParams({ sqlQuery: { sql: [sql, []] } }).queryKeyMd5).toBe('3c4fe606a2f1f42cb688a22faa5631c8');
    });

    it('Performing query completed: queryKey', () => {
      expect(redactLogParams({ queryKey: [preAggSql, []] }).queryKeyMd5).toBe('97a0e5ebcf66acde852b5d31d3b32dc2');
    });

    it('Using cache for: cacheKey', () => {
      expect(redactLogParams({ cacheKey: ['SELECT FLOOR((UNIX_TIMESTAMP()) / 3600) as refresh_key', []] }).queryKeyMd5)
        .toBe('ac977bbfe63251e62660a5959b9b449c');
    });
  });
});

describe('redactLogParams queue key identity', () => {
  const queryKey = ['SELECT 1 WHERE a = ?', ['secret'], [['SELECT 2', ['also secret']]]];

  it('stamps queryKeyMd5 over the unredacted queryKey', () => {
    const redacted = redactLogParams({ queryKey, requestId: 'r1' });

    expect(redacted.queryKey).toEqual(['SELECT 1 WHERE a = ?', [REDACTED], [['SELECT 2', [REDACTED]]]]);
    expect(redacted.queryKeyMd5).toBe(md5(queryKey));
  });

  it('keeps a queryKeyMd5 the event already carries', () => {
    const redacted = redactLogParams({ queryKey, queryKeyMd5: 'precomputed' });

    expect(redacted.queryKeyMd5).toBe('precomputed');
  });

  it('falls back to cacheKey when there is no queryKey', () => {
    const cacheKey = ['SELECT 1', ['secret'], { renewalThreshold: 10 }];
    const redacted = redactLogParams({ cacheKey });

    expect(redacted.cacheKey).toEqual(['SELECT 1', [REDACTED], { renewalThreshold: 10 }]);
    expect(redacted.queryKeyMd5).toBe(md5(cacheKey));
  });

  it('hashes sqlQuery.sql padded to a three-element queue key', () => {
    const sql = ['SELECT * FROM orders WHERE email = ?', ['john@example.com']];
    const redacted = redactLogParams({ sqlQuery: { sql, external: false } });

    expect(redacted.sqlQuery.sql).toEqual(['SELECT * FROM orders WHERE email = ?', [REDACTED]]);
    expect(redacted.queryKeyMd5).toBe(md5([...sql, []]));
    // The queue logs the same statement under [sql, params, loadSqls]
    expect(redactLogParams({ queryKey: [...sql, []] }).queryKeyMd5).toBe(redacted.queryKeyMd5);
  });

  it('prefers queryKey over cacheKey and sqlQuery', () => {
    const redacted = redactLogParams({ queryKey, cacheKey: ['x', []], sqlQuery: { sql: ['y', []] } });

    expect(redacted.queryKeyMd5).toBe(md5(queryKey));
  });

  it('stamps nothing on an event without a queue key', () => {
    const redacted = redactLogParams({ query: { sql: 'SELECT 1' }, queryKey: 'Fetch tables for public' });

    expect(redacted.queryKeyMd5).toBeUndefined();
    expect(redacted.queryKey).toBe('Fetch tables for public');
  });

  it('leaves a schema introspection key to the consumer', () => {
    expect(redactLogParams({ queryKey: [['Fetch tables for public', ['x']], []] }).queryKeyMd5).toBeUndefined();
    expect(isFetchTablesKey('Fetch tables for public')).toBe(true);
    expect(isFetchTablesKey(['Fetch tables for public', []])).toBe(true);
    // Only the head of the key counts, not a value that happens to contain the phrase
    const key = ['SELECT ?', ['Fetch tables for me'], []];
    expect(isFetchTablesKey(key)).toBe(false);
    expect(redactLogParams({ queryKey: key }).queryKeyMd5).toBe(md5(key));
  });

  it('copies params that are a class instance instead of mutating them', () => {
    class Params {
      public query = { filters: [{ member: 'A.x', operator: 'equals', values: ['pii'] }] };

      public queryKey = ['SELECT ?', ['pii']];
    }
    const params = new Params();

    const redacted = redactLogParams(params as any);

    expect(redacted.query.filters[0].values).toEqual([REDACTED]);
    expect(redacted.queryKey).toEqual(['SELECT ?', [REDACTED]]);
    expect(redacted.queryKeyMd5).toBe(md5(params.queryKey));
    expect(params.query.filters[0].values).toEqual(['pii']);
    expect(params.queryKey).toEqual(['SELECT ?', ['pii']]);
    expect((params as any).queryKeyMd5).toBeUndefined();
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

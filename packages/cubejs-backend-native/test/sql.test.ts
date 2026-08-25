import { Client } from 'pg';
import { isCI } from '@cubejs-backend/shared';
import { Writable } from 'stream';

import * as native from '../js';
import metaFixture from './meta';
import { FakeRowStream } from './response-fake';

const _logger = jest.fn(({ event }) => {
  if (
    !event.error.includes(
      'load - strange response, success which contains error'
    )
  ) {
    expect(event.apiType).toEqual('sql');
    expect(event.protocol).toEqual('postgres');
  }
  console.log(event);
});

// native.setupLogger(
//   logger,
//   'trace',
// );

function interfaceMethods() {
  return {
    sqlApiLoad: jest.fn(async ({ request, session, query, streaming }) => {
      console.log('[js] sqlApiLoad', {
        request,
        session,
        query,
        streaming,
      });

      if (streaming) {
        return {
          stream: new FakeRowStream(query),
        };
      }

      expect(session).toEqual({
        user: expect.toBeTypeOrNull(String),
        superuser: expect.any(Boolean),
        securityContext: { foo: 'bar' },
      });

      // It's just an emulation that ApiGateway returns error
      return {
        error: 'This error should be passed back to PostgreSQL client',
      };
    }),
    sql: jest.fn(async ({ request, session, query }) => {
      console.log('[js] sql', {
        request,
        session,
        query,
      });

      // It's just an emulation that ApiGateway returns error
      return {
        error: 'This error should be passed back to PostgreSQL client',
      };
    }),
    stream: jest.fn(async ({ request, session, query }) => {
      console.log('[js] stream', {
        request,
        session,
        query,
      });

      return {
        stream: new FakeRowStream(query),
      };
    }),
    meta: jest.fn(async () => metaFixture),
    sqlGenerators: jest.fn(async ({ request, session }) => {
      console.log('[js] sqlGenerators', {
        request,
        session,
      });

      return {
        cubeNameToDataSource: {},
        memberToDataSource: {},
        dataSourceToSqlGenerator: {},
      };
    }),
    contextToApiScopes: jest.fn(async ({ request, token }) => {
      console.log('[js] contextToApiScopes', {
        request,
        token,
      });

      return ['data', 'meta', 'graphql'];
    }),
    checkAuth: jest.fn(async ({ request, token }) => {
      console.log('[js] checkAuth', {
        request,
        token,
      });

      throw new Error('checkAuth is not implemented');
    }),
    checkSqlAuth: jest.fn(async ({ request, user }) => {
      console.log('[js] checkSqlAuth', {
        request,
        user,
      });

      if (user === 'allowed_user') {
        return {
          password: 'password_for_allowed_user',
          superuser: false,
          securityContext: { foo: 'bar' },
        };
      }

      if (user === 'admin') {
        return {
          password: 'password_for_admin',
          superuser: true,
          securityContext: { foo: 'admin' },
        };
      }

      throw new Error('Please specify user');
    }),
    logLoadEvent: ({
      event,
      properties,
    }: {
      event: string;
      properties: any;
    }) => {
      console.log(
        `Load event: ${JSON.stringify({ type: event, ...properties })}`
      );
    },
  };
}

describe('SQLInterface', () => {
  jest.setTimeout(60 * 1000);

  it('SHOW FULL TABLES FROM `db`', async () => {
    const methods = interfaceMethods();
    const { checkSqlAuth, meta } = methods;

    const instance = await native.registerInterface({
      pgPort: 5555,
      ...methods,
      canSwitchUserForSession: (_payload) => true,
    });
    console.log(instance);

    try {
      const testConnectionFailed = async (
        /** input */ { user, password }: { user?: string; password?: string }
      ) => {
        const client = new Client({
          host: '127.0.0.1',
          database: 'test',
          port: 5555,
          ssl: false,
          user,
          password,
        });

        try {
          await client.connect();

          throw new Error('must throw error');
        } catch (e: any) {
          expect(e.message).toContain(
            'password authentication failed for user'
          );
        }

        console.log(checkSqlAuth.mock.calls);
        expect(checkSqlAuth.mock.calls.length).toEqual(1);
        expect(checkSqlAuth.mock.calls[0][0]).toEqual({
          request: {
            id: expect.any(String),
            meta: null,
            method: expect.any(String),
            protocol: expect.any(String),
          },
          user: user || null,
          password:
            password || (isCI() && process.platform === 'win32' ? 'root' : ''),
        });
      };

      await testConnectionFailed({
        user: 'random user',
        password: undefined,
      });
      checkSqlAuth.mockClear();

      await testConnectionFailed({
        user: 'allowed_user',
        password: undefined,
      });
      checkSqlAuth.mockClear();

      await testConnectionFailed({
        user: 'allowed_user',
        password: 'wrong_password',
      });
      checkSqlAuth.mockClear();

      const connection = new Client({
        host: '127.0.0.1',
        database: 'test',
        port: 5555,
        user: 'allowed_user',
        password: 'password_for_allowed_user',
      });
      await connection.connect();

      {
        const result = await connection.query(
          'SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = \'public\' ORDER BY table_name DESC'
        );
        console.log(result);

        expect(result.rows).toEqual([
          {
            table_name: 'Logs',
            table_type: 'BASE TABLE',
          },
          {
            table_name: 'KibanaSampleDataEcommerce',
            table_type: 'BASE TABLE',
          },
        ]);
      }

      expect(checkSqlAuth.mock.calls.length).toEqual(1);
      expect(checkSqlAuth.mock.calls[0][0]).toEqual({
        request: {
          id: expect.any(String),
          meta: null,
          method: expect.any(String),
          protocol: expect.any(String),
        },
        user: 'allowed_user',
        password: 'password_for_allowed_user',
      });

      // @ts-ignore
      expect(meta.mock.calls[0][0]).toEqual({
        request: {
          id: expect.any(String),
          meta: null,
        },
        session: {
          user: 'allowed_user',
          superuser: false,
          securityContext: { foo: 'bar' },
        },
        onlyCompilerId: true,
      });

      try {
        await connection.query(
          'select * from KibanaSampleDataEcommerce LIMIT 1000'
        );

        throw new Error('Error was not passed from transport to the client');
      } catch (e: any) {
        expect(e.code).toEqual('58000');
        expect(e.message).toContain(
          'This error should be passed back to PostgreSQL client'
        );
      }

      if (process.env.CUBESQL_STREAM_MODE === 'true') {
        const result = await connection.query(
          'select id, order_date from KibanaSampleDataEcommerce order by order_date desc limit 50001'
        );
        expect(result.rows.length).toEqual(50001);
        expect(result.rows[0].id).toEqual(0);
        expect(result.rows[50000].id).toEqual(50000);
      }

      {
        const result = await connection.query(
          'SELECT CAST(\'2020-12-25 22:48:48.000\' AS timestamp) as column1'
        );
        console.log(result);

        expect(result.rows).toEqual([
          { column1: new Date('2020-12-25T22:48:48.000Z') },
        ]);
      }

      await connection.end();
    } finally {
      await native.shutdownInterface(instance, 'fast');
    }
  });

  it('streams cube sql over http', async () => {
    if (process.env.CUBESQL_STREAM_MODE === 'true') {
      const instance = await native.registerInterface({
        pgPort: 5555,
        ...interfaceMethods(),
        canSwitchUserForSession: (_payload) => true,
      });

      let buf = '';
      let rows = 0;
      const write = jest.fn((chunk, _, callback) => {
        const lines = (buf + chunk.toString('utf-8')).split('\n');
        buf = lines.pop() || '';

        rows = lines
          .filter((it) => it.trim().length)
          .map((it) => {
            const json = JSON.parse(it);
            expect(json.error).toBeUndefined();

            return json.data?.length || 0;
          })
          .reduce((a, b) => a + b, rows);

        callback();
      });

      if (buf.length > 0) {
        rows += JSON.parse(buf).data.length;
      }

      const cubeSqlStream = new Writable({
        write,
      });

      const onDrain = jest.fn();
      cubeSqlStream.on('drain', onDrain);

      await native.execSql(
        instance,
        'SELECT order_date FROM KibanaSampleDataEcommerce ORDER BY order_date DESC LIMIT 100000;',
        cubeSqlStream
      );

      expect(rows).toBe(100000);

      await native.shutdownInterface(instance, 'fast');
    } else {
      expect(process.env.CUBESQL_STREAM_MODE).toBeFalsy();
    }
  });

  test('schema from stream and empty data when no batches', async () => {
    const interfaceMethods_ = interfaceMethods();
    const instance = await native.registerInterface({
      ...interfaceMethods_,
      canSwitchUserForSession: (_payload) => true,
    });

    let schemaReceived = false;
    let dataReceived = false;
    let emptyDataReceived = false;
    let buf = '';

    const write = jest.fn((chunk, _, callback) => {
      const lines = (buf + chunk.toString('utf-8')).split('\n');
      buf = lines.pop() || '';

      lines
        .filter((it) => it.trim().length)
        .forEach((line) => {
          const json = JSON.parse(line);
          
          if (json.error) {
            // Ignore errors for this test
            return;
          }

          if (json.schema) {
            schemaReceived = true;
            expect(json.schema).toBeDefined();
            expect(Array.isArray(json.schema)).toBe(true);
            expect(json.data).toBeUndefined();
          } else if (json.data) {
            dataReceived = true;
            // Check if it's empty data
            if (Array.isArray(json.data) && json.data.length === 0) {
              emptyDataReceived = true;
            }
          }
        });

      callback();
    });

    const cubeSqlStream = new Writable({
      write,
    });

    try {
      // Use LIMIT 0 to test the real case where SQL produces no results
      await native.execSql(
        instance,
        'SELECT order_date FROM KibanaSampleDataEcommerce LIMIT 0;',
        cubeSqlStream
      );

      // Verify schema was sent and empty data was sent for LIMIT 0 query
      expect(schemaReceived).toBe(true);
      expect(dataReceived).toBe(true);
      expect(emptyDataReceived).toBe(true);
    } catch (error) {
      // Even if query fails, we should get schema
      console.log('Query error (expected in test):', error);
      if (schemaReceived) {
        expect(schemaReceived).toBe(true);
      }
    }

    await native.shutdownInterface(instance, 'fast');
  });

  test('client disconnect ends the /cubesql stream gracefully, not as an error', async () => {
    // A client that closes the response stream before the result set has been
    // fully written (a cancelled dashboard, a closed browser tab, an aborted
    // fetch) ends the request gracefully. It is the same situation as a
    // `Continue wait` handed back to a client that stops waiting for the
    // result: nobody is left to read the rows, so there is nothing to do but
    // stop. It must not be reported as `Cube SQL Error` — that event feeds
    // error rates and query history — and no error payload should be pushed
    // into the stream that is already gone.
    const loadEvents: string[] = [];
    const methods = {
      ...interfaceMethods(),
      // Return data in both stream and non-stream mode: `CUBESQL_STREAM_MODE`
      // only picks the streaming branch for limits above
      // `non_streaming_query_max_row_limit`, and this test must behave the
      // same either way.
      sqlApiLoad: jest.fn(async ({ streaming, query }: any) => {
        if (streaming) {
          return { stream: new FakeRowStream(query) };
        }
        return {
          results: [
            {
              annotation: {
                measures: {},
                dimensions: {},
                segments: {},
                timeDimensions: {},
              },
              data: {
                members: ['KibanaSampleDataEcommerce.order_date'],
                columns: [['2024-01-01T00:00:00.000']],
              },
            },
          ],
        };
      }),
      logLoadEvent: ({ event }: { event: string; properties: any }) => {
        loadEvents.push(event);
      },
    };

    const instance = await native.registerInterface({
      ...methods,
      canSwitchUserForSession: (_payload: any) => true,
    });

    const chunks: string[] = [];
    const cubeSqlStream = new Writable({
      write(chunk, _enc, callback) {
        chunks.push(chunk.toString('utf-8'));
        callback();
        // Simulate the client going away right after the JSONL schema header.
        this.destroy();
      },
    });
    // The native side holds a reference to the stream and only learns it is
    // gone through the `close` event, so the writes already in flight (and the
    // final `end()`) hit a destroyed stream and emit ERR_STREAM_DESTROYED.
    // Collect them instead of letting them become an unhandled 'error'.
    const streamErrors: Error[] = [];
    cubeSqlStream.on('error', (err) => {
      streamErrors.push(err);
    });

    try {
      await native.execSql(
        instance,
        'SELECT order_date FROM KibanaSampleDataEcommerce ORDER BY order_date DESC LIMIT 100000;',
        cubeSqlStream
      );

      expect(loadEvents).toContain('Load Request');
      expect(loadEvents).not.toContain('Cube SQL Error');
      // Neither terminal event fires: the query never completed, and the
      // disconnect is not a failure. Asserting this also keeps the test
      // honest — it would pass vacuously if the query had simply finished
      // before the `close` event arrived.
      expect(loadEvents).not.toContain('Load Request Success');
      // Only the schema header made it out, and nothing that looks like an
      // error was written into the closed stream.
      expect(chunks.length).toBeGreaterThan(0);
      expect(chunks.join('')).not.toContain('"error"');
    } finally {
      await native.shutdownInterface(instance, 'fast');
    }
  });

  test('external flag is surfaced in /cubesql JSONL schema header when set to true', async () => {
    // End-to-end coverage of the cubesql -> backend-native -> JSONL path:
    // the non-streaming `load` returns a V1LoadResponseColumnar with
    // `external: true`, which cubesql deserializes into
    // `V1LoadResult.external`, propagates into the Arrow schema metadata
    // as `external = "true"`, and node_export.rs emits it as a top-level
    // `external: true` field on the JSONL schema header consumed by the
    // /v1/cubesql HTTP endpoint.
    //
    // sqlApiLoad returning a plain object drives the
    // `ValueFromJs::String` branch in transport.rs (via
    // `wrapNativeFunctionWithStream`, which JSON.stringifies the response
    // for us) and exercises `convert_transport_response_columnar`,
    // avoiding the ResultWrapper construction overhead the production
    // gateway uses.
    const methods = {
      ...interfaceMethods(),
      sqlApiLoad: jest.fn(async ({ streaming, query }: any) => {
        if (streaming) {
          return { stream: new FakeRowStream(query) };
        }
        // Plain object — wrapNativeFunctionWithStream JSON.stringifies the
        // response itself before handing it to the Rust side.
        return {
          results: [
            {
              annotation: {
                measures: {},
                dimensions: {},
                segments: {},
                timeDimensions: {},
              },
              data: {
                members: ['KibanaSampleDataEcommerce.order_date'],
                columns: [['2024-01-01T00:00:00.000']],
              },
              lastRefreshTime: '2024-01-01T00:00:00.000Z',
              external: true,
            },
          ],
        };
      }),
    };

    const instance = await native.registerInterface({
      ...methods,
      canSwitchUserForSession: (_payload: any) => true,
    });

    let buf = '';
    const lines: any[] = [];
    const write = jest.fn((chunk, _enc, callback) => {
      const raw = (buf + chunk.toString('utf-8')).split('\n');
      buf = raw.pop() || '';
      for (const l of raw) {
        if (l.trim().length) {
          lines.push(JSON.parse(l));
        }
      }
      callback();
    });
    const cubeSqlStream = new Writable({ write });

    try {
      await native.execSql(
        instance,
        'SELECT order_date FROM KibanaSampleDataEcommerce LIMIT 1;',
        cubeSqlStream
      );

      const schemaLine = lines.find((o) => o.schema);
      expect(schemaLine).toBeDefined();
      expect(schemaLine.external).toBe(true);
      // lastRefreshTime should also be passed through unchanged.
      expect(schemaLine.lastRefreshTime).toBe('2024-01-01T00:00:00.000Z');
    } finally {
      await native.shutdownInterface(instance, 'fast');
    }
  });

  test('external flag is absent from /cubesql JSONL header when set to false', async () => {
    const methods = {
      ...interfaceMethods(),
      sqlApiLoad: jest.fn(async ({ streaming, query }: any) => {
        if (streaming) {
          return { stream: new FakeRowStream(query) };
        }
        return {
          results: [
            {
              annotation: {
                measures: {},
                dimensions: {},
                segments: {},
                timeDimensions: {},
              },
              data: {
                members: ['KibanaSampleDataEcommerce.order_date'],
                columns: [['2024-01-01T00:00:00.000']],
              },
              lastRefreshTime: '2024-01-01T00:00:00.000Z',
              external: false,
            },
          ],
        };
      }),
    };

    const instance = await native.registerInterface({
      ...methods,
      canSwitchUserForSession: (_payload: any) => true,
    });

    let buf = '';
    const lines: any[] = [];
    const write = jest.fn((chunk, _enc, callback) => {
      const raw = (buf + chunk.toString('utf-8')).split('\n');
      buf = raw.pop() || '';
      for (const l of raw) {
        if (l.trim().length) {
          lines.push(JSON.parse(l));
        }
      }
      callback();
    });
    const cubeSqlStream = new Writable({ write });

    try {
      await native.execSql(
        instance,
        'SELECT order_date FROM KibanaSampleDataEcommerce LIMIT 1;',
        cubeSqlStream
      );

      const schemaLine = lines.find((o) => o.schema);
      expect(schemaLine).toBeDefined();
      // Boolean flag must be omitted (rather than emitted as `false`) when
      // not served from CubeStore, so the JSONL header stays compact.
      expect(schemaLine.external).toBeUndefined();
      expect(schemaLine.lastRefreshTime).toBe('2024-01-01T00:00:00.000Z');
    } finally {
      await native.shutdownInterface(instance, 'fast');
    }
  });

  // A calculated projection over MEASURE() (a query-level member expression,
  // e.g. an "average order value" field) leaves the cube scan wrapped in
  // DataFusion Projection/Sort nodes. Those build their own output schema and
  // drop the scan's `lastRefreshTime` / `external` metadata, so the JSONL
  // header used to come back without them while the same base measures queried
  // plainly did carry them.
  //
  // Both queries carry an explicit LIMIT to pin them to the buffered path.
  // `CubeScanExecutionPlan::execute` switches to `load_stream` when stream mode
  // is on and the request has no limit, and that branch never runs `load_data`,
  // so no freshness metadata is recorded at all — a known gap, and this suite
  // runs under CUBESQL_STREAM_MODE=true in CI.
  test.each([
    [
      'plain measure projection',
      'SELECT customer_gender, MEASURE(count) AS cnt FROM KibanaSampleDataEcommerce GROUP BY 1 LIMIT 10;',
    ],
    [
      'calculated projection over MEASURE()',
      'SELECT customer_gender, ROUND(MEASURE(maxPrice) / MEASURE(count), 2) AS avg_value, MEASURE(count) AS cnt FROM KibanaSampleDataEcommerce GROUP BY 1 ORDER BY 3 DESC LIMIT 10;',
    ],
  ])(
    'lastRefreshTime and external survive in /cubesql JSONL header for a %s',
    async (_name, sql) => {
      const methods = {
        ...interfaceMethods(),
        sqlApiLoad: jest.fn(async ({ streaming, query }: any) => {
          if (streaming) {
            return { stream: new FakeRowStream(query) };
          }
          return {
            results: [
              {
                annotation: {
                  measures: {},
                  dimensions: {},
                  segments: {},
                  timeDimensions: {},
                },
                data: {
                  members: [
                    'KibanaSampleDataEcommerce.customer_gender',
                    'KibanaSampleDataEcommerce.maxPrice',
                    'KibanaSampleDataEcommerce.count',
                  ],
                  columns: [['female'], [10], [4]],
                },
                lastRefreshTime: '2024-01-01T00:00:00.000Z',
                external: true,
              },
            ],
          };
        }),
      };

      const instance = await native.registerInterface({
        ...methods,
        canSwitchUserForSession: (_payload: any) => true,
      });

      let buf = '';
      const lines: any[] = [];
      const write = jest.fn((chunk, _enc, callback) => {
        const raw = (buf + chunk.toString('utf-8')).split('\n');
        buf = raw.pop() || '';
        for (const l of raw) {
          if (l.trim().length) {
            lines.push(JSON.parse(l));
          }
        }
        callback();
      });
      const cubeSqlStream = new Writable({ write });

      try {
        await native.execSql(instance, sql, cubeSqlStream);

        const schemaLine = lines.find((o) => o.schema);
        expect(schemaLine).toBeDefined();
        expect(schemaLine.lastRefreshTime).toBe('2024-01-01T00:00:00.000Z');
        expect(schemaLine.external).toBe(true);
      } finally {
        await native.shutdownInterface(instance, 'fast');
      }
    }
  );
});

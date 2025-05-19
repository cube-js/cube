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
    load: jest.fn(async ({ request, session, query }) => {
      console.log('[js] load', {
        request,
        session,
        query,
      });

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
    sqlApiLoad: jest.fn(async ({ request, session, query, streaming }) => {
      console.log('[js] load', {
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
        // limit is used to router query to load method instead of stream
        await connection.query(
          'select * from KibanaSampleDataEcommerce LIMIT 1000'
        );

        throw new Error('Error was not passed from transport to the client');
      } catch (e: any) {
        expect(e.code).toEqual('XX000');
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
});

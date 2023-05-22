import mysql from 'mysql2/promise';

import * as native from '../js';
import metaFixture from './meta';
import { FakeRowStream } from './response-fake';

const logger = jest.fn(({ event }) => {
  if (!event.error.includes('load - strange response, success which contains error')) {
    expect(event.apiType).toEqual('sql');
    expect(event.protocol).toEqual('mysql');
  }
  console.log(event);
});

native.setupLogger(
  logger,
  'trace',
);

describe('SQLInterface', () => {
  jest.setTimeout(10 * 1000);

  it('SHOW FULL TABLES FROM `db`', async () => {
    const load = jest.fn(async ({ request, session, query }) => {
      console.log('[js] load', {
        request,
        session,
        query
      });

      expect(session).toEqual({
        user: expect.toBeTypeOrNull(String),
        superuser: expect.any(Boolean),
      });

      // It's just an emulation that ApiGateway returns error
      return {
        error: 'This error should be passed back to MySQL client'
      };
    });

    const stream = jest.fn(async ({ request, session, query }) => {
      console.log('[js] stream', {
        request,
        session,
        query
      });

      return {
        stream: new FakeRowStream(query),
      };
    });

    const meta = jest.fn(async ({ request, session }) => {
      console.log('[js] meta', {
        request,
        session,
      });

      expect(session).toEqual({
        user: expect.toBeTypeOrNull(String),
        superuser: expect.any(Boolean),
      });

      return metaFixture;
    });

    const sqlGenerators = jest.fn(async ({ request, session }) => {
      console.log('[js] sqlGenerators',  {
        request,
        session,
      });

      return {};
    });

    const checkAuth = jest.fn(async ({ request, user }) => {
      console.log('[js] checkAuth', {
        request,
        user,
      });

      if (user === 'allowed_user') {
        return {
          password: 'password_for_allowed_user',
          superuser: false,
        };
      }

      if (user === 'admin') {
        return {
          password: 'password_for_admin',
          superuser: true,
        };
      }

      throw new Error('Please specify user');
    });

    const instance = await native.registerInterface({
      // nonce: '12345678910111213141516'.substring(0, 20),
      port: 4545,
      checkAuth,
      load,
      meta,
      stream,
      sqlGenerators,
    });
    console.log(instance);

    try {
      const testConnectionFailed = async (/** input */ { user, password }: { user?: string, password?: string }) => {
        try {
          await mysql.createConnection({
            host: '127.0.0.1',
            port: 4545,
            user,
            password,
          });

          throw new Error('must throw error');
        } catch (e: any) {
          expect(e.message).toContain('Incorrect user name or password');
        }

        console.log(checkAuth.mock.calls);
        expect(checkAuth.mock.calls.length).toEqual(1);
        expect(checkAuth.mock.calls[0][0]).toEqual({
          request: {
            id: expect.any(String),
            meta: null,
          },
          user: user || null,
        });
      };

      await testConnectionFailed({
        user: undefined,
        password: undefined
      });
      checkAuth.mockClear();

      await testConnectionFailed({
        user: 'allowed_user',
        password: undefined,
      });
      checkAuth.mockClear();

      await testConnectionFailed({
        user: 'allowed_user',
        password: 'wrong_password'
      });
      checkAuth.mockClear();

      const connection = await mysql.createConnection({
        host: '127.0.0.1',
        port: 4545,
        user: 'allowed_user',
        password: 'password_for_allowed_user'
      });

      {
        const [result] = await connection.query('SHOW FULL TABLES FROM `db`');
        console.log(result);

        expect(result).toEqual([
          {
            Tables_in_db: 'KibanaSampleDataEcommerce',
            Table_type: 'BASE TABLE',
          },
          {
            Tables_in_db: 'Logs',
            Table_type: 'BASE TABLE',
          },
        ]);
      }

      expect(checkAuth.mock.calls.length).toEqual(1);
      expect(checkAuth.mock.calls[0][0]).toEqual({
        request: {
          id: expect.any(String),
          meta: null,
        },
        user: 'allowed_user',
      });

      expect(meta.mock.calls.length).toEqual(1);
      expect(meta.mock.calls[0][0]).toEqual({
        request: {
          id: expect.any(String),
          meta: null,
        },
        session: {
          user: 'allowed_user',
          superuser: false,
        }
      });

      try {
        // limit is used to router query to load method instead of stream
        await connection.query('select * from KibanaSampleDataEcommerce LIMIT 1000');

        throw new Error('Error was not passed from transport to the client');
      } catch (e: any) {
        expect(e.sqlState).toEqual('HY000');
        expect(e.sqlMessage).toContain('This error should be passed back to MySQL client');
      }

      if (process.env.CUBESQL_STREAM_MODE === 'true') {
        const [result, _columns] = (await connection.query({
          sql: 'select id, order_date from KibanaSampleDataEcommerce order by order_date desc limit 50001',
          rowsAsArray: false,
        })) as any;
        expect(result.length).toEqual(50001);
        expect(result[0].id).toEqual(0);
        expect(result[50000].id).toEqual(50000);
      }

      {
        const [result] = await connection.query('select CAST(\'2020-12-25 22:48:48.000\' AS timestamp)');
        console.log(result);

        expect(result).toEqual([{ 'TimestampNanosecond(1608936528000000000, None)': '2020-12-25T22:48:48.000' }]);
      }

      // Increment it in case you throw Error
      setTimeout(_ => {
        expect(logger.mock.calls.length).toEqual(1);
      }, 2000);

      connection.destroy();
    } finally {
      await native.shutdownInterface(instance);
    }
  });
});

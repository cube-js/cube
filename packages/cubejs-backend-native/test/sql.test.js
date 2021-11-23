const mysql = require('mysql2/promise');
const util = require('util');

const native = require('../dist/js/index');
const meta_fixture = require('./meta');

native.setLogLevel('trace');

describe('SQLInteface', () => {
  jest.setTimeout(10 * 1000);

  it('SHOW FULL TABLES FROM `db`', async () => {
    const load = jest.fn(async ({ request, user }) => {
      console.log('[js] load',  {
        request,
        user
      });

      throw new Error('Unsupported');
    });

    const meta = jest.fn(async ({ request, user }) => {
      console.log('[js] meta',  {
        request,
        user,
      });

      return meta_fixture;
    });

    const checkAuth = jest.fn(async ({ request, user }) => {
      console.log('[js] checkAuth',  {
        request,
        user,
      });

      if (user === 'allowed_user') {
        return {
          password: 'password_for_allowed_user'
        }
      }

      throw new Error('Please specify user');
    });

    const instance = await native.registerInterface({
      // nonce: '12345678910111213141516'.substring(0, 20),
      port: 4545,
      checkAuth,
      load,
      meta,
    });
    console.log(instance);

    const testConnectionFailed = async (/** input */ { user, password }) =>{
      try {
        await mysql.createConnection({
          host: '127.0.0.1',
          port: 4545,
          user,
          password,
        });;

        throw new Error('must throw error');
      } catch (e) {
        expect(e.message).toContain('Incorrect user name or password');
      }

      console.log(checkAuth.mock.calls)
      expect(checkAuth.mock.calls.length).toEqual(1);
      expect(checkAuth.mock.calls[0][0]).toEqual({
        request: {
          id: expect.any(String)
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

    expect(checkAuth.mock.calls.length).toEqual(1);
    expect(checkAuth.mock.calls[0][0]).toEqual({
      request: {
        id: expect.any(String)
      },
      user: 'allowed_user',
    });

    expect(meta.mock.calls.length).toEqual(1);
    expect(meta.mock.calls[0][0]).toEqual({
      request: {
        id: expect.any(String)
      },
      user: 'allowed_user',
    });

    connection.destroy();
  });
});

const mysql = require('mysql');
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
      checkAuth,
      load,
      meta,
    });
    console.log(instance);

    const testConnectionFailed = async (/** input */ { user, password }) =>{
      const connection = mysql.createConnection({
        host: 'localhost',
        user,
        password,
      });
      const pingAsync = util.promisify(connection.ping.bind(connection));

      try {
        await pingAsync();

        throw new Error('must throw error');
      } catch (e) {
        expect(e.message).toContain('ER_PASSWORD_NO_MATCH: Incorrect user name or password');
      }

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

    const connection = mysql.createConnection({
      host : 'localhost',
      user: 'allowed_user',
      password: 'password_for_allowed_user'
    });
    const queryAsync = util.promisify(connection.query.bind(connection));

    const result = await queryAsync('SHOW FULL TABLES FROM `db`');
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

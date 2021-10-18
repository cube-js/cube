const mysql = require('mysql');
const util = require('util');

const native = require('../dist/lib/index');
const meta_fixture = require('./meta');

describe('SQLInteface', () => {
  jest.setTimeout(10 * 1000);

  beforeAll(() => {
    native.setLogLevel('trace');
  });

  // it('Failed auth', async () => {
  //   const load = async (extra) => {
  //     console.log('[js] load',  {
  //       extra,
  //     });

  //     throw new Error('Unsupported');
  //   };

  //   const meta = async (extra) => {
  //       console.log('[js] meta',  {
  //         extra,
  //       });

  //       throw new Error('Unsupported');
  //   };

  //   const checkAuth = jest.fn(async (extra) => {
  //     console.log('[js] checkAuth',  {
  //       extra,
  //     });

  //     return false;
  //   });

  //   await native.registerInterface({
  //     checkAuth,
  //     load,
  //     meta,
  //   });

  //   const connection = mysql.createConnection({
  //     host : 'localhost',
  //     user: 'eyJhbGciOiJIUzI1NiJ9.e30.pLPm89qEsoPg-66NIfEJjRQFiW5PYyjfferd4sBx5IU'
  //   });
  //   const pingAsync = util.promisify(connection.ping.bind(connection));
    
  //   try {
  //     await pingAsync();

  //     throw new Error('must throw error');
  //   } catch (e) {
  //     expect(e.message).toContain('ER_PASSWORD_NO_MATCH: Incorrect user name or password');
  //   }

  //   expect(checkAuth.mock.calls.length).toEqual(1);
  //   expect(checkAuth.mock.calls[0][0]).toEqual('eyJhbGciOiJIUzI1NiJ9.e30.pLPm89qEsoPg-66NIfEJjRQFiW5PYyjfferd4sBx5IU');

  //   connection.destroy();
  // });

  it('SHOW FULL TABLES FROM `db`', async () => {
    const load = jest.fn(async (extra) => {
      console.log('[js] load',  {
        extra,
      });

      throw new Error('Unsupported');
    });

    const meta = jest.fn(async (extra) => {
      console.log('[js] meta',  {
        extra,
      });

      return meta_fixture;
    });

    const checkAuth = jest.fn(async (extra) => {
      console.log('[js] checkAuth',  {
        extra,
      });

      return true;
    });

    await native.registerInterface({
      checkAuth,
      load,
      meta,
    });

    const connection = mysql.createConnection({
      host : 'localhost',
      user: 'eyJhbGciOiJIUzI1NiJ9.e30.pLPm89qEsoPg-66NIfEJjRQFiW5PYyjfferd4sBx5IU'
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
      authorization: 'eyJhbGciOiJIUzI1NiJ9.e30.pLPm89qEsoPg-66NIfEJjRQFiW5PYyjfferd4sBx5IU',
    });

    expect(meta.mock.calls.length).toEqual(1);
    expect(meta.mock.calls[0][0]).toEqual({
      authorization: 'eyJhbGciOiJIUzI1NiJ9.e30.pLPm89qEsoPg-66NIfEJjRQFiW5PYyjfferd4sBx5IU'
    });

    connection.destroy();
  });
});

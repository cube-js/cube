const mysql = require('mysql');
const util = require('util');

const native = require('..');
const meta = require('./meta');

describe('SQLInteface', () => {
  jest.setTimeout(10 * 1000);

  it('can start', async () => {
    let transport_load = async (extra, channel) => {
      console.log('[js] transport_load',  {
        extra,
        channel
      });

      native.channel_reject(channel);
    };

    let transport_meta = async (extra, channel) => {
        console.log('[js] transport_meta',  {
          extra,
          channel
        });

        try {
          native.channel_resolve(channel, JSON.stringify(meta));
        } catch (e) {
          console.log(e);

          native.channel_reject(channel);
        }
    };

    await native.registerInterface(
      transport_load,
      transport_meta,
    );

    const connection = mysql.createConnection({
      host : 'localhost',
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
    ])

    connection.destroy();
  })
});

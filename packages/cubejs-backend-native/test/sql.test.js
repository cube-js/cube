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
  })
});

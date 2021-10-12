const native = require('..');
const meta = require('./meta');

(async () => {
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

    let promise = native.registerInterface(
      transport_load,
      transport_meta,
    );

    try {
        console.log(await promise);
        console.log('await');
    } catch (e) {
        console.log("Error", e);
    }
})();

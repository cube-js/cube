const native = require('../dist/lib/index');
const meta_fixture = require('./meta');

(async () => {
    const load = async (extra) => {
      console.log('[js] load',  {
        extra,
      });

      throw new Error('Unsupported');
    };

    const meta = async (extra) => {
        console.log('[js] meta',  {
          extra
        });

        return meta_fixture;
    };

    const checkAuth = async (extra) => {
      console.log('[js] checkAuth',  {
        extra,
      });

      return true;
    };

    native.setLogLevel('trace');

    const interface = await native.registerInterface({
      checkAuth,
      load,
      meta,
    });
    console.log({
      interface
    });

    // block
    await new Promise(() => {});
})();

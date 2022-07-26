const native = require('../dist/js/index');
const meta_fixture = require('./meta');

(async () => {
    const load = async ({ request, session, query }) => {
      console.log('[js] load',  {
        request,
        session,
        query ,
      });

      throw new Error('Unsupported');
    };

    const meta = async ({ request, session }) => {
        console.log('[js] meta',  {
          request,
          session
        });

        return meta_fixture;
    };

    const checkAuth = async ({ request, user }) => {
      console.log('[js] checkAuth',  {
        request,
        user,
      });

      if (user) {
        // without password
        if (user === 'wp') {
          return {
            password: null,
            superuser: false,
          };
        }

        if (user === 'admin') {
          return {
            password: null,
            superuser: true,
          };
        }

        return {
          password: 'test',
          superuser: false,
        }
      }

      throw new Error('Please specify password');
    };

    native.setupLogger(
      ({ event }) => console.log(event),
      'trace',
    );

    const interface = await native.registerInterface({
      // nonce: '12345678910111213141516'.substring(0, 20),
      checkAuth,
      load,
      meta,
    });
    console.log({
      interface
    });

    process.on('SIGINT', async () => {
      console.log('SIGINT signal');

      try {
        await native.shutdownInterface(interface);
      } catch (e) {
        console.log(e);
      } finally {
        process.exit(1);
      }
    });

    // block
    await new Promise(() => {});
})();

import { FakeRowStream } from './response-fake';

const native = require('../js/index');
const meta_fixture = require('./meta');

(async () => {
  const load = async ({ request, session, query }) => {
    console.log('[js] load', {
      request,
      session,
      query,
    });

    throw new Error('load is not implemented');
  };

  const sqlApiLoad = async ({ request, session, query, streaming }) => {
    console.log('[js] sqlApiLoad', {
      request,
      session,
      query,
      streaming
    });

    if (streaming) {
      return {
        stream: new FakeRowStream(query),
      };
    }

    throw new Error('sqlApiLoad is not implemented');
  };

  const sql = async () => {
    console.log('[js] sql');

    throw new Error('sql is not implemented');
  };

  const meta = async ({ request, session }) => {
    console.log('[js] meta', {
      request,
      session
    });

    return meta_fixture;
  };

  const stream = async ({ request, session, query }) => {
    console.log('[js] stream', {
      request,
      session,
      query,
    });

    return {
      stream: new FakeRowStream(query),
    };
  };

  const checkAuth = async ({ request, user, password }) => {
    console.log('[js] checkAuth', {
      request,
      user,
      password
    });

    if (user) {
      // without password
      if (user === 'wp') {
        return {
          password,
          superuser: false,
        };
      }

      if (user === 'admin') {
        return {
          password,
          superuser: true,
        };
      }

      return {
        password: 'test',
        superuser: false,
      };
    }

    throw new Error('Please specify password');
  };

  const sqlGenerators = async ({ request, session }) => {
    console.log('[js] sqlGenerators', {
      request,
      session,
    });

    return {
      cubeNameToDataSource: {},
      memberToDataSource: {},
      dataSourceToSqlGenerator: {},
    };
  };

  const logLoadEvent = async () => {
    console.log('[js] logLoadEvent');
  };

  const canSwitchUserForSession = async () => {
    console.log('[js] canSwitchUserForSession');

    return true;
  };

  native.setupLogger(
    ({ event }) => console.log(event),
    'trace',
  );

  const server = await native.registerInterface({
    // nonce: '12345678910111213141516'.substring(0, 20),
    checkAuth,
    load,
    sql,
    meta,
    stream,
    sqlApiLoad,
    sqlGenerators,
    logLoadEvent,
    canSwitchUserForSession,
    pgPort: '5555',
  });
  console.log({
    server
  });

  process.on('SIGINT', async () => {
    console.log('SIGINT signal');

    try {
      await native.shutdownInterface(server, 'fast');
    } catch (e) {
      console.log(e);
    } finally {
      process.exit(1);
    }
  });

  // block
  await new Promise(() => {});
})();

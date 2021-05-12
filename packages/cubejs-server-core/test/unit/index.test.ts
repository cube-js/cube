/* eslint-disable @typescript-eslint/no-empty-function */

import { withTimeout } from '@cubejs-backend/shared';
import { CubejsServerCore } from '../../src';
import { DatabaseType } from '../../src/core/types';

class CubejsServerCoreOpen extends CubejsServerCore {
  public detectScheduledRefreshTimer(scheduledRefreshTimer: number|boolean) {
    return super.detectScheduledRefreshTimer(scheduledRefreshTimer);
  }

  public getRefreshScheduler() {
    return super.getRefreshScheduler();
  }
}

describe('index.test', () => {
  beforeEach(() => {
    delete process.env.CUBEJS_EXT_DB_TYPE;
    delete process.env.CUBEJS_DEV_MODE;
    delete process.env.CUBEJS_DB_TYPE;

    process.env.NODE_ENV = 'development';
    process.env.CUBEJS_API_SECRET = 'api-secret';
  });

  test('Should create instance of CubejsServerCore, dbType as string', () => {
    expect(new CubejsServerCore({
      dbType: 'mysql'
    })).toBeInstanceOf(CubejsServerCore);
  });

  test('Should create instance of CubejsServerCore, dbType as func', () => {
    const options = { dbType: () => <DatabaseType>'postgres' };

    expect(new CubejsServerCore(options))
      .toBeInstanceOf(CubejsServerCore);
  });

  test('Should throw error, unknown dbType', () => {
    const options = { dbType: <any>'unknown-db' };

    expect(() => new CubejsServerCore(options))
      .toThrowError(/"dbType" must be one of/);
  });

  test('Should throw error, invalid options', () => {
    const options = {
      dbType: <DatabaseType>'mysql',
      externalDbType: <DatabaseType>'mysql',
      schemaPath: '/test/path/test/',
      basePath: '/basePath',
      webSocketsBasePath: '/webSocketsBasePath',
      devServer: true,
      compilerCacheSize: -10,
    };

    expect(() => new CubejsServerCore(options))
      .toThrowError(/"compilerCacheSize" must be larger than or equal to 0/);
  });

  test('Should create instance of CubejsServerCore, orchestratorOptions as func', () => {
    const options = { dbType: <DatabaseType>'mysql', orchestratorOptions: () => <any>{} };

    expect(new CubejsServerCore(options))
      .toBeInstanceOf(CubejsServerCore);
  });

  test('Should create instance of CubejsServerCore, pass all options', async () => {
    const queueOptions = {
      concurrency: 3,
      continueWaitTimeout: 5,
      executionTimeout: 600,
      orphanedTimeout: 120,
      heartBeatInterval: 500,
      sendProcessMessageFn: () => {},
      sendCancelMessageFn: () => {}
    };

    const options = {
      dbType: <any>'mysql',
      externalDbType: <any>'mysql',
      schemaPath: '/test/path/test/',
      basePath: '/basePath',
      webSocketsBasePath: '/webSocketsBasePath',
      initApp: () => {},
      processSubscriptionsInterval: 5000,
      devServer: false,
      apiSecret: 'randomstring',
      logger: () => {},
      driverFactory: () => {},
      externalDriverFactory: () => {},
      contextToAppId: () => 'STANDALONE',
      contextToOrchestratorId: () => 'EMPTY',
      repositoryFactory: () => {},
      checkAuth: () => {},
      checkAuthMiddleware: () => {},
      queryTransformer: () => {},
      preAggregationsSchema: () => {},
      schemaVersion: () => {},
      extendContext: () => {},
      scheduledRefreshTimer: true,
      compilerCacheSize: 1000,
      maxCompilerCacheKeepAlive: 10,
      updateCompilerCacheKeepAlive: true,
      telemetry: false,
      allowUngroupedWithoutPrimaryKey: true,
      scheduledRefreshConcurrency: 4,
      orchestratorOptions: {
        continueWaitTimeout: 10,
        redisPrefix: 'some-prefix',
        queryCacheOptions: {
          refreshKeyRenewalThreshold: 1000,
          backgroundRenew: true,
          queueOptions,
          externalQueueOptions: {
            ...queueOptions
          }
        },
        preAggregationsOptions: {
          queueOptions
        },
        rollupOnlyMode: false
      },
      allowJsDuplicatePropsInSchema: true,
      jwt: {
        // JWK options
        jwkRetry: 5,
        jwkDefaultExpire: 5 * 60,
        jwkUrl: () => '',
        jwkRefetchWindow: 5 * 60,
        // JWT options
        key: 'string',
        algorithms: ['RS256'],
        issuer: ['http://localhost:4000'],
        audience: 'http://localhost:4000/v1',
        subject: 'http://localhost:4000',
        claimsNamespace: 'http://localhost:4000',
      },
      livePreview: true
    };

    const cubejsServerCore = new CubejsServerCore(<any>options);
    expect(cubejsServerCore).toBeInstanceOf(CubejsServerCore);
    await cubejsServerCore.releaseConnections();
  });

  test('Should create instance of CubejsServerCore, dbType from process.env.CUBEJS_DB_TYPE', () => {
    process.env.CUBEJS_DB_TYPE = 'mysql';

    expect(new CubejsServerCore({}))
      .toBeInstanceOf(CubejsServerCore);
  });

  test('Should create instance of CubejsServerCore, on unsupported platform for Cube Store', async () => {
    const originalPlatform = process.platform;

    const logger = jest.fn(() => {
      //
    });

    class CubejsServerCoreLocalMock extends CubejsServerCore {
      // Just return true to emulate that package exists
      protected isCubeStoreDriverInstalled = () => <any>true;
    }

    try {
      process.env.CUBEJS_DB_TYPE = 'mysql';
      process.env.CUBEJS_DEV_MODE = 'true';

      Object.defineProperty(process, 'platform', {
        value: 'MockOS'
      });

      const cubejsServerCore = new CubejsServerCoreLocalMock({ logger });
      await cubejsServerCore.beforeShutdown();
      await cubejsServerCore.shutdown();
    } finally {
      Object.defineProperty(process, 'platform', {
        value: originalPlatform
      });
    }

    expect(logger.mock.calls).toEqual([
      [
        'Cube Store is not supported on your system',
        {
          warning: 'You are using MockOS platform with x64 architecture, which is not supported by Cube Store.'
        }
      ]
    ]);
  });

  test('Should throw error, options are required (dev mode)', () => {
    delete process.env.CUBEJS_API_SECRET;
    process.env.CUBEJS_DEV_MODE = 'true';

    expect(() => {
      jest.spyOn(CubejsServerCore.prototype, 'configFileExists').mockImplementation(() => true);
      // eslint-disable-next-line
      new CubejsServerCore({});
      jest.restoreAllMocks();
    })
      .toThrowError(/dbType is required/);
  });

  test('Pass all required (dev mode) without apiSecret (should be autogenerated)', () => {
    delete process.env.CUBEJS_API_SECRET;

    process.env.CUBEJS_DEV_MODE = 'true';
    process.env.CUBEJS_DB_TYPE = 'mysql';

    expect(new CubejsServerCore({ jwt: { jwkUrl: 'https://test.com/j.json' } })).toBeInstanceOf(CubejsServerCore);
  });

  test('Should throw error, options are required (production mode)', () => {
    delete process.env.CUBEJS_API_SECRET;
    process.env.NODE_ENV = 'production';

    expect(() => {
      jest.spyOn(CubejsServerCore.prototype, 'configFileExists').mockImplementation(() => true);
      // eslint-disable-next-line
      new CubejsServerCore({});
      jest.restoreAllMocks();
    })
      .toThrowError(/dbType, apiSecret are required/);
  });

  test('Should throw error, options are required (production mode with jwkUrl)', () => {
    process.env.NODE_ENV = 'production';

    expect(() => {
      jest.spyOn(CubejsServerCore.prototype, 'configFileExists').mockImplementation(() => true);
      // eslint-disable-next-line
      new CubejsServerCore({ jwt: { jwkUrl: 'https://test.com/j.json' } });
      jest.restoreAllMocks();
    })
      .toThrowError(/dbType is required/);
  });

  test('Pass all required props (production mode with JWK URL)', () => {
    delete process.env.CUBEJS_API_SECRET;

    process.env.NODE_ENV = 'production';
    process.env.CUBEJS_DB_TYPE = 'mysql';

    expect(new CubejsServerCore({ jwt: { jwkUrl: 'https://test.com/j.json' } })).toBeInstanceOf(CubejsServerCore);
  });

  test('Should not throw when the required options are missing in dev mode and no config file exists', () => {
    expect(() => {
      jest.spyOn(CubejsServerCore.prototype, 'configFileExists').mockImplementation(() => false);
      // eslint-disable-next-line
      new CubejsServerCore({});
      jest.restoreAllMocks();
    })
      .not.toThrow();
  });

  const expectRefreshTimerOption = (input, output, setProduction: boolean = false) => {
    test(`scheduledRefreshTimer option ${input}`, async () => {
      if (setProduction) {
        process.env.NODE_ENV = 'production';
      }

      const cubejsServerCore = new CubejsServerCoreOpen({
        dbType: 'mysql',
        apiSecret: 'secret',
        scheduledRefreshTimer: input
      });
      expect(cubejsServerCore).toBeInstanceOf(CubejsServerCore);
      expect(cubejsServerCore.detectScheduledRefreshTimer(input)).toBe(output);

      await cubejsServerCore.beforeShutdown();
      await cubejsServerCore.shutdown();
    });
  };

  expectRefreshTimerOption(0, false);
  expectRefreshTimerOption(1, 1000);
  expectRefreshTimerOption(10, 10000);
  expectRefreshTimerOption(true, 30000);
  expectRefreshTimerOption(false, false);

  test('scheduledRefreshContexts option', async () => {
    const cubejsServerCore = new CubejsServerCoreOpen({
      dbType: 'mysql',
      apiSecret: 'secret',
      // 250ms
      scheduledRefreshTimer: 1,
      scheduledRefreshConcurrency: 2,
      scheduledRefreshContexts: async () => [
        {
          securityContext: {
            appid: 'test1',
            u: {
              prop1: 'value1'
            }
          }
        },
        // securityContext is required in typings, but can be empty in user-space
        <any>{
          // Renamed to securityContext, let's test that it migrate automatically
          authInfo: {
            appid: 'test2',
            u: {
              prop2: 'value2'
            }
          },
        },
        // Null is a default placeholder
        null
      ],
    });
    expect(cubejsServerCore).toBeInstanceOf(CubejsServerCoreOpen);

    const timeoutKiller = withTimeout(
      () => {
        throw new Error('runScheduledRefresh was not called');
      },
      2 * 1000,
    );

    const refreshSchedulerMock = {
      runScheduledRefresh: jest.fn(async () => {
        await timeoutKiller.cancel();

        return {
          finished: true,
        };
      })
    };

    jest.spyOn(cubejsServerCore, 'getRefreshScheduler').mockImplementation(() => <any>refreshSchedulerMock);

    await timeoutKiller;

    expect(refreshSchedulerMock.runScheduledRefresh.mock.calls.length).toEqual(3);
    expect(refreshSchedulerMock.runScheduledRefresh.mock.calls[0]).toEqual([
      {
        authInfo: { appid: 'test1', u: { prop1: 'value1' } },
        securityContext: { appid: 'test1', u: { prop1: 'value1' } },
      },
      { concurrency: 2 },
    ]);
    expect(refreshSchedulerMock.runScheduledRefresh.mock.calls[1]).toEqual([
      {
        authInfo: { appid: 'test2', u: { prop2: 'value2' } },
        securityContext: { appid: 'test2', u: { prop2: 'value2' } },
      },
      { concurrency: 2 },
    ]);
    expect(refreshSchedulerMock.runScheduledRefresh.mock.calls[2]).toEqual([
      // RefreshScheduler will populate it
      null,
      { concurrency: 2 },
    ]);

    await cubejsServerCore.beforeShutdown();
    await cubejsServerCore.shutdown();
  });
});

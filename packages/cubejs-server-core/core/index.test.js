/* eslint-disable no-new */
/* globals describe,test,expect */

const CubejsServerCore = require('./index');

process.env.CUBEJS_API_SECRET = 'api-secret';

describe('index.test', () => {
  test('Should create instance of CubejsServerCore, dbType as string', () => {
    expect(new CubejsServerCore({
      dbType: 'mysql'
    })).toBeInstanceOf(CubejsServerCore);
  });

  test('Should create instance of CubejsServerCore, dbType as func', () => {
    const options = { dbType: () => {} };

    expect(new CubejsServerCore(options))
      .toBeInstanceOf(CubejsServerCore);
  });

  test('Should throw error, unknown dbType', () => {
    const options = { dbType: 'unknown-db' };

    expect(() => { new CubejsServerCore(options); })
      .toThrowError(/"dbType" must be one of/);
  });

  test('Should throw error, unknown options property', () => {
    const options = { dbType: 'mysql', unknown: 'some-value' };

    expect(() => { new CubejsServerCore(options); })
      .toThrowError(/"unknown" is not allowed/);
  });

  test('Should throw error, invalid options', () => {
    const options = {
      dbType: 'mysql',
      externalDbType: 'mysql',
      schemaPath: '/test/path/test/',
      basePath: '/basePath',
      webSocketsBasePath: '/webSocketsBasePath',
      devServer: true,
      compilerCacheSize: -10,
    };

    expect(() => { new CubejsServerCore(options); })
      .toThrowError(/"compilerCacheSize" must be larger than or equal to 0/);
  });

  test('Should create instance of CubejsServerCore, orchestratorOptions as func', () => {
    const options = { dbType: 'mysql', orchestratorOptions: () => {} };

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
      dbType: 'mysql',
      externalDbType: 'mysql',
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
      contextToAppId: () => {},
      contextToDataSourceId: () => {},
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
      orchestratorOptions: {
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
      allowJsDuplicatePropsInSchema: true
    };

    const cubejsServerCore = new CubejsServerCore(options);
    expect(cubejsServerCore)
      .toBeInstanceOf(CubejsServerCore);
    await cubejsServerCore.releaseConnections();
  });

  test('Should create instance of CubejsServerCore, dbType from process.env.CUBEJS_DB_TYPE', () => {
    process.env.CUBEJS_DB_TYPE = 'mysql';

    expect(new CubejsServerCore({}))
      .toBeInstanceOf(CubejsServerCore);
  });

  test('Should throw error, dbType is required', () => {
    delete process.env.CUBEJS_DB_TYPE;

    expect(() => { new CubejsServerCore({}); })
      .toThrowError(/driverFactory, apiSecret, dbType are required options/);
  });

  const expectRefreshTimerOption = (input, output, setProduction) => {
    test(`scheduledRefreshTimer option ${input}`, async () => {
      if (setProduction) {
        process.env.NODE_ENV = 'production';
      }
      const cubejsServerCore = new CubejsServerCore({
        dbType: 'mysql',
        apiSecret: 'secret',
        scheduledRefreshTimer: input
      });
      expect(cubejsServerCore).toBeInstanceOf(CubejsServerCore);
      expect(cubejsServerCore.scheduledRefreshTimer).toBe(output);

      await cubejsServerCore.releaseConnections();
      delete process.env.NODE_ENV;
    });
  };

  expectRefreshTimerOption(10, 10000);
  expectRefreshTimerOption('9', 9000);
  expectRefreshTimerOption(true, 5000);
  expectRefreshTimerOption(false, false);
  expectRefreshTimerOption('false', false);
  expectRefreshTimerOption(undefined, 5000);
  expectRefreshTimerOption(undefined, false, true);
});

/* globals jest, describe, beforeEach, test, expect */

import type { BaseDriver } from '@cubejs-backend/query-orchestrator';
import type {
  DatabaseType,
  DbTypeFn,
  DbTypeAsyncFn,
  ExternalDbTypeFn,
  DriverFactoryFn,
  DriverContext,
  RequestContext,
  ServerCoreInitializedOptions,
} from '../../src/core/types';
import type { OptsHandler } from '../../src/core/OptsHandler';
import { lookupDriverClass } from '../../src/core/DriverResolvers';
import { CubejsServerCore } from '../../src/core/server';

class CubejsServerCoreExposed extends CubejsServerCore {
  public options: ServerCoreInitializedOptions;

  public optsHandler: OptsHandler;

  public contextToDbType: DbTypeAsyncFn;

  public contextToExternalDbType: ExternalDbTypeFn;

  public reloadEnvVariables = super.reloadEnvVariables;
}

let message: string;

const logger = (msg: string) => {
  message = msg;
};

describe('OptsHandler class', () => {
  beforeEach(() => {
    message = '';
  });

  test.skip(
    'deprecation warning must be printed if dbType was specified -- ' +
    'need to be restored after documentation will be added',
    async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const core = new CubejsServerCore({
        logger,
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        dbType: ((context: DriverContext) => 'postgres'),
      });
      expect(message).toEqual('Cube.js `CreateOptions.dbType` Property Deprecation');

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test('must handle vanila CreateOptions', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    let core;

    // Case 1
    core = new CubejsServerCoreExposed({
      logger,
      dbType: undefined,
      driverFactory: undefined,
    });
    
    expect(core.options.dbType).toBeDefined();
    expect(typeof core.options.dbType).toEqual('function');
    expect(await core.options.dbType({} as DriverContext))
      .toEqual(process.env.CUBEJS_DB_TYPE);

    expect(core.options.driverFactory).toBeDefined();
    expect(typeof core.options.driverFactory).toEqual('function');
    expect(await core.options.driverFactory({} as DriverContext)).toEqual({
      type: process.env.CUBEJS_DB_TYPE,
    });

    await core.beforeShutdown();
    await core.shutdown();

    // Case 2
    core = new CubejsServerCoreExposed({
      logger,
      dbType: 'postgres',
      driverFactory: () => CubejsServerCore.createDriver('postgres'),
    });

    expect(core.options.dbType).toBeDefined();
    expect(typeof core.options.dbType).toEqual('function');
    expect(await core.options.dbType({} as DriverContext))
      .toEqual(process.env.CUBEJS_DB_TYPE);

    expect(core.options.driverFactory).toBeDefined();
    expect(typeof core.options.driverFactory).toEqual('function');
    expect(
      JSON.stringify(await core.options.driverFactory({} as DriverContext)),
    ).toEqual(
      JSON.stringify(CubejsServerCore.createDriver('postgres')),
    );

    await core.beforeShutdown();
    await core.shutdown();

    // Case 3
    core = new CubejsServerCoreExposed({
      logger,
      dbType: () => 'postgres',
      driverFactory: () => CubejsServerCore.createDriver('postgres'),
    });

    expect(core.options.dbType).toBeDefined();
    expect(typeof core.options.dbType).toEqual('function');
    expect(await core.options.dbType({} as DriverContext))
      .toEqual(process.env.CUBEJS_DB_TYPE);

    expect(core.options.driverFactory).toBeDefined();
    expect(typeof core.options.driverFactory).toEqual('function');
    expect(
      JSON.stringify(await core.options.driverFactory({} as DriverContext)),
    ).toEqual(
      JSON.stringify(CubejsServerCore.createDriver('postgres')),
    );

    await core.beforeShutdown();
    await core.shutdown();

    // Case 4
    core = new CubejsServerCoreExposed({
      logger,
      dbType: () => 'postgres',
      driverFactory: async () => CubejsServerCore.createDriver('postgres'),
    });

    expect(core.options.dbType).toBeDefined();
    expect(typeof core.options.dbType).toEqual('function');
    expect(await core.options.dbType({} as DriverContext))
      .toEqual(process.env.CUBEJS_DB_TYPE);

    expect(core.options.driverFactory).toBeDefined();
    expect(typeof core.options.driverFactory).toEqual('function');
    expect(
      JSON.stringify(await core.options.driverFactory({} as DriverContext)),
    ).toEqual(
      JSON.stringify(CubejsServerCore.createDriver('postgres')),
    );

    await core.beforeShutdown();
    await core.shutdown();
  });

  test('must handle valid CreateOptions', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    let core;

    // Case 1
    core = new CubejsServerCoreExposed({
      logger,
      dbType: undefined,
      driverFactory: () => ({
        type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
      }),
    });
    
    expect(core.options.dbType).toBeDefined();
    expect(typeof core.options.dbType).toEqual('function');
    expect(await core.options.dbType({} as DriverContext))
      .toEqual(process.env.CUBEJS_DB_TYPE);

    expect(core.options.driverFactory).toBeDefined();
    expect(typeof core.options.driverFactory).toEqual('function');
    expect(await core.options.driverFactory({} as DriverContext)).toEqual({
      type: process.env.CUBEJS_DB_TYPE,
    });

    await core.beforeShutdown();
    await core.shutdown();

    // Case 2
    core = new CubejsServerCoreExposed({
      logger,
      dbType: 'postgres',
      driverFactory: () => ({
        type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
      }),
    });
    
    expect(core.options.dbType).toBeDefined();
    expect(typeof core.options.dbType).toEqual('function');
    expect(await core.options.dbType({} as DriverContext))
      .toEqual(process.env.CUBEJS_DB_TYPE);

    expect(core.options.driverFactory).toBeDefined();
    expect(typeof core.options.driverFactory).toEqual('function');
    expect(await core.options.driverFactory({} as DriverContext)).toEqual({
      type: process.env.CUBEJS_DB_TYPE,
    });

    await core.beforeShutdown();
    await core.shutdown();

    // Case 3
    core = new CubejsServerCoreExposed({
      logger,
      dbType: 'postgres',
      driverFactory: async () => ({
        type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
      }),
    });
    
    expect(core.options.dbType).toBeDefined();
    expect(typeof core.options.dbType).toEqual('function');
    expect(await core.options.dbType({} as DriverContext))
      .toEqual(process.env.CUBEJS_DB_TYPE);

    expect(core.options.driverFactory).toBeDefined();
    expect(typeof core.options.driverFactory).toEqual('function');
    expect(await core.options.driverFactory({} as DriverContext)).toEqual({
      type: process.env.CUBEJS_DB_TYPE,
    });

    await core.beforeShutdown();
    await core.shutdown();

    // Case 4
    core = new CubejsServerCoreExposed({
      logger,
      dbType: <DbTypeFn>(async () => 'postgres'),
      driverFactory: async () => ({
        type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
      }),
    });
    
    expect(core.options.dbType).toBeDefined();
    expect(typeof core.options.dbType).toEqual('function');
    expect(await core.options.dbType({} as DriverContext))
      .toEqual(process.env.CUBEJS_DB_TYPE);

    expect(core.options.driverFactory).toBeDefined();
    expect(typeof core.options.driverFactory).toEqual('function');
    expect(await core.options.driverFactory({} as DriverContext)).toEqual({
      type: process.env.CUBEJS_DB_TYPE,
    });

    await core.beforeShutdown();
    await core.shutdown();
  });

  test('must throw if CreateOptions invalid', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    let core;

    // Case 1
    await expect(async () => {
      core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: (() => true) as unknown as DriverFactoryFn,
      });
      await core.options.driverFactory(<DriverContext>{ dataSource: 'default' });
    }).rejects.toThrow(
      'Unexpected CreateOptions.driverFactory result value. ' +
      'Must be either DriverConfig or driver instance: <boolean>true'
    );

    await core.beforeShutdown();
    await core.shutdown();

    // Case 2
    await expect(async () => {
      core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: 1 as unknown as DriverFactoryFn,
      });
      await core.options.driverFactory(<DriverContext>{ dataSource: 'default' });
    }).rejects.toThrow(
      'Invalid cube-server-core options: child "driverFactory" fails because ' +
      '["driverFactory" must be a Function]'
    );

    await core.beforeShutdown();
    await core.shutdown();

    // Case 3 -- need to be restored after assertion will be restored.
    //
    // await expect(async () => {
    //   const core = new CubejsServerCoreExposed({
    //     logger,
    //     dbType: undefined,
    //     driverFactory: () => CubejsServerCore.createDriver('postgres'),
    //   });
    //   await core.options.driverFactory(<DriverContext>{ dataSource: 'default' });
    // }).rejects.toThrow(
    //   'CreateOptions.dbType is required if CreateOptions.driverFactory ' +
    //   'returns driver instance'
    // );

    // Case 4
    await expect(async () => {
      core = new CubejsServerCoreExposed({
        logger,
        dbType: (() => true) as unknown as DbTypeFn,
        driverFactory: async () => ({
          type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
        }),
      });
      await core.options.dbType(<DriverContext>{ dataSource: 'default' });
    }).rejects.toThrow(
      'Unexpected CreateOptions.dbType result type: <boolean>true'
    );

    await core.beforeShutdown();
    await core.shutdown();

    // Case 5
    await expect(async () => {
      core = new CubejsServerCoreExposed({
        logger,
        dbType: true as unknown as DbTypeFn,
        driverFactory: async () => ({
          type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
        }),
      });
      await core.options.dbType(<DriverContext>{ dataSource: 'default' });
    }).rejects.toThrow(
      'Invalid cube-server-core options: child "dbType" fails because ' +
      '["dbType" must be a string, "dbType" must be a Function]'
    );

    await core.beforeShutdown();
    await core.shutdown();

    // Case 6
    expect(() => {
      process.env.CUBEJS_DB_TYPE = undefined;
      process.env.NODE_ENV = 'production';
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: undefined,
      });
    }).toThrow(
      'apiSecret is required option(s)'
    );

    // Case 7
    expect(() => {
      delete process.env.CUBEJS_DB_TYPE;
      process.env.NODE_ENV = 'production';
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      core = new CubejsServerCoreExposed({
        logger,
        apiSecret: 'apiSecret',
        dbType: undefined,
        driverFactory: undefined,
      });
    }).toThrow(
      'Either CUBEJS_DB_TYPE, CreateOptions.dbType or ' +
      'CreateOptions.driverFactory must be specified'
    );

    delete process.env.NODE_ENV;
  });

  test('must configure/reconfigure contextToDbType', async () => {
    // TODO (buntarb): find out a way to check contextToExternalDbType reloading
    // without driver instantiation, which causes the test failing.
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const core = new CubejsServerCoreExposed({
      logger,
      dbType: undefined,
      driverFactory: undefined,
    });

    expect(await core.contextToDbType({} as DriverContext)).toEqual('postgres');
    expect(core.contextToExternalDbType({} as DriverContext)).toBeUndefined();

    process.env.CUBEJS_DB_TYPE = 'mysql';

    core.reloadEnvVariables();

    expect(await core.contextToDbType({} as DriverContext)).toEqual('mysql');
    expect(core.contextToExternalDbType({} as DriverContext)).toBeUndefined();

    process.env.CUBEJS_DB_TYPE = 'postgres';

    core.reloadEnvVariables();

    expect(await core.contextToDbType({} as DriverContext)).toEqual('postgres');
    expect(core.contextToExternalDbType({} as DriverContext)).toBeUndefined();

    await core.beforeShutdown();
    await core.shutdown();
  });

  test(
    'must configure queueOptions without orchestratorOptions, ' +
    'without CUBEJS_CONCURRENCY and without default driver concurrency',
    async () => {
      delete process.env.CUBEJS_CONCURRENCY;
      process.env.CUBEJS_DB_TYPE = 'cubestore';

      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: {},
      });

      const opts = (<any>core.getOrchestratorApi(<RequestContext>{})).options;
      
      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: 2,
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency: 2,
      });

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must configure queueOptions with empty orchestratorOptions object, ' +
    'without CUBEJS_CONCURRENCY and without default driver concurrency',
    async () => {
      delete process.env.CUBEJS_CONCURRENCY;
      process.env.CUBEJS_DB_TYPE = 'cubestore';

      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: {},
      });

      const opts = (<any>core.getOrchestratorApi(<RequestContext>{})).options;
      
      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: 2,
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency: 2,
      });

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must configure queueOptions with empty orchestratorOptions function, ' +
    'without CUBEJS_CONCURRENCY and without default driver concurrency',
    async () => {
      delete process.env.CUBEJS_CONCURRENCY;
      process.env.CUBEJS_DB_TYPE = 'cubestore';

      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: () => ({}),
      });

      const opts = (<any>core.getOrchestratorApi(<RequestContext>{})).options;
      
      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: 2,
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency: 2,
      });

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must configure queueOptions with empty orchestratorOptions function, ' +
    'without CUBEJS_CONCURRENCY and with default driver concurrency',
    async () => {
      delete process.env.CUBEJS_CONCURRENCY;
      process.env.CUBEJS_DB_TYPE = 'postgres';

      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: () => ({}),
      });

      const opts = (<any>core.getOrchestratorApi(<RequestContext>{})).options;
      
      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: lookupDriverClass(process.env.CUBEJS_DB_TYPE).getDefaultConcurrency(),
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency: lookupDriverClass(process.env.CUBEJS_DB_TYPE).getDefaultConcurrency(),
      });

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must configure queueOptions with empty orchestratorOptions function, ' +
    'with CUBEJS_CONCURRENCY and with default driver concurrency',
    async () => {
      process.env.CUBEJS_CONCURRENCY = '10';
      process.env.CUBEJS_DB_TYPE = 'postgres';

      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: () => ({}),
      });

      const opts = (<any>core.getOrchestratorApi(<RequestContext>{})).options;
      
      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: parseInt(process.env.CUBEJS_CONCURRENCY, 10),
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency: parseInt(process.env.CUBEJS_CONCURRENCY, 10),
      });

      delete process.env.CUBEJS_CONCURRENCY;

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must configure queueOptions with conficured orchestratorOptions function, ' +
    'with CUBEJS_CONCURRENCY and with default driver concurrency',
    async () => {
      process.env.CUBEJS_CONCURRENCY = '10';
      process.env.CUBEJS_DB_TYPE = 'postgres';

      const concurrency = 15;
      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: () => ({
          queryCacheOptions: {
            queueOptions: {
              concurrency,
            },
          },
          preAggregationsOptions: {
            queueOptions: () => ({
              concurrency,
            }),
          },
        }),
      });

      const opts = (<any>core.getOrchestratorApi(<RequestContext>{})).options;
      
      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency,
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency,
      });

      delete process.env.CUBEJS_CONCURRENCY;

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test('must configure driver pool', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const concurrency1 = 15;
    const concurrency2 = 25;
    let core;
    let opts;
    let driver;

    // Case 1
    core = new CubejsServerCoreExposed({
      logger,
      dbType: undefined,
      driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
      orchestratorOptions: () => ({
        queryCacheOptions: {
          queueOptions: {
            concurrency: concurrency1,
          },
        },
        preAggregationsOptions: {
          queueOptions: () => ({
            concurrency: concurrency2,
          }),
        },
      }),
    });
    opts = (<any>core.getOrchestratorApi(<RequestContext>{})).options;
    driver = <any>(await core.resolveDriver(<DriverContext>{}, opts));
    
    expect(driver.pool.options.max).toEqual(2 * (concurrency1 + concurrency2));

    await core.beforeShutdown();
    await core.shutdown();

    // Case 2
    core = new CubejsServerCoreExposed({
      logger,
      dbType: undefined,
      driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
      orchestratorOptions: () => ({
        queryCacheOptions: {
          queueOptions: {
            concurrency: concurrency1,
          },
        },
        preAggregationsOptions: {
          queueOptions: () => ({
            concurrency: concurrency2,
          }),
        },
      }),
    });
    opts = (<any>core.getOrchestratorApi(<RequestContext>{})).options;
    driver = <any>(await core.resolveDriver(<DriverContext>{}));
    
    expect(driver.pool.options.max).toEqual(8);

    await core.beforeShutdown();
    await core.shutdown();
  });

  test.only(
    'must set preAggregationsOptions.externalRefresh to false and test ' +
    'driver connection for dev server',
    async () => {
      process.env.CUBEJS_DEV_MODE = 'true';
      const core = new CubejsServerCoreExposed({
        logger,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
        externalDbType: 'postgres',
        externalDriverFactory: async () => <BaseDriver>({
          testConnection: async () => undefined,
        }),
      });

      const oapi = <any>(core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(opts.rollupOnlyMode).toBe(false);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(false);
      await expect(async () => {
        await oapi.testConnection();
      }).rejects.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(2);

      testDriverConnectionSpy.mockRestore();
    }
  );

  test(
    'must set preAggregationsOptions.externalRefresh to true and doesn`t ' +
    'test driver connection for dev server with externalRefresh set to true',
    async () => {
      process.env.CUBEJS_DEV_MODE = 'true';
      const core = new CubejsServerCoreExposed({
        logger,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
        orchestratorOptions: () => ({
          preAggregationsOptions: {
            externalRefresh: true,
          },
        }),
      });

      const oapi = <any>(core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(opts.rollupOnlyMode).toBe(false);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(true);
      expect(async () => {
        await oapi.testConnection();
      }).not.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(1);

      testDriverConnectionSpy.mockRestore();

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must set preAggregationsOptions.externalRefresh to false and doesn`t ' +
    'test driver connection for dev server with rollupOnlyMode set to true',
    async () => {
      process.env.CUBEJS_DEV_MODE = 'true';
      const core = new CubejsServerCoreExposed({
        logger,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
        orchestratorOptions: () => ({
          rollupOnlyMode: true,
        }),
      });

      const oapi = <any>(core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(opts.rollupOnlyMode).toBe(true);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(false);
      expect(async () => {
        await oapi.testConnection();
      }).not.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(1);

      testDriverConnectionSpy.mockRestore();

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must set preAggregationsOptions.externalRefresh to false and test ' +
    'driver connection for refresh worker in the production mode',
    async () => {
      process.env.CUBEJS_DEV_MODE = 'false';
      const core = new CubejsServerCoreExposed({
        logger,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        scheduledRefreshTimer: true,
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
      });

      const oapi = <any>(core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(opts.rollupOnlyMode).toBe(false);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(false);
      await expect(async () => {
        await oapi.testConnection();
      }).rejects.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(2);

      testDriverConnectionSpy.mockRestore();

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must set preAggregationsOptions.externalRefresh to false and test ' +
    'driver connection for api worker in the production mode if ' +
    'CUBEJS_PRE_AGGREGATIONS_BUILDER is set',
    async () => {
      process.env.CUBEJS_DEV_MODE = 'false';
      process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'true';
      const core = new CubejsServerCoreExposed({
        logger,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        scheduledRefreshTimer: false,
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
      });

      const oapi = <any>(core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(opts.rollupOnlyMode).toBe(false);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(false);
      await expect(async () => {
        await oapi.testConnection();
      }).rejects.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(2);

      testDriverConnectionSpy.mockRestore();

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must set preAggregationsOptions.externalRefresh to true and do not test ' +
    'driver connection for api worker in the production mode if specified in' +
    'preAggregationsOptions.externalRefresh',
    async () => {
      process.env.CUBEJS_DEV_MODE = 'false';
      process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'true';
      const core = new CubejsServerCoreExposed({
        logger,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        scheduledRefreshTimer: false,
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
        orchestratorOptions: () => ({
          preAggregationsOptions: {
            externalRefresh: true,
          },
        }),
      });

      const oapi = <any>(core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(opts.rollupOnlyMode).toBe(false);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(true);
      expect(async () => {
        await oapi.testConnection();
      }).not.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(1);

      testDriverConnectionSpy.mockRestore();

      await core.beforeShutdown();
      await core.shutdown();
    }
  );

  test(
    'must set preAggregationsOptions.externalRefresh to true and do not test ' +
    'driver connection for api worker in the production mode if ' +
    'CUBEJS_PRE_AGGREGATIONS_BUILDER is unset',
    async () => {
      process.env.CUBEJS_DEV_MODE = 'false';
      process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'false';
      const core = new CubejsServerCoreExposed({
        logger,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        scheduledRefreshTimer: false,
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
      });

      const oapi = <any>(core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(opts.rollupOnlyMode).toBe(false);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(true);
      expect(async () => {
        await oapi.testConnection();
      }).not.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(1);

      testDriverConnectionSpy.mockRestore();

      await core.beforeShutdown();
      await core.shutdown();
    }
  );
});

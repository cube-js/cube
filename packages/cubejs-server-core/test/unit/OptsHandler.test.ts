/* globals jest, describe, test, expect */

import { BaseDriver as OriginalBaseDriver } from '@cubejs-backend/query-orchestrator';
import type {
  DatabaseType,
  DbTypeInternalFn,
  ExternalDbTypeFn,
  DriverFactoryFn,
  DriverContext,
  RequestContext,
  ServerCoreInitializedOptions,
} from '../../src/core/types';
import type { OptsHandler } from '../../src/core/OptsHandler';
import { releasePreAggregationsSchemaPin } from '@cubejs-backend/shared';
import { lookupDriverClass } from '../../src/core/DriverResolvers';
import { CubejsServerCore } from '../../src/core/server';
import { CreateOptions, SystemOptions } from '../../src/core/types';

class CubejsServerCoreExposed extends CubejsServerCore {
  public declare options: ServerCoreInitializedOptions;

  public declare optsHandler: OptsHandler;

  public declare contextToDbType: DbTypeInternalFn;

  public declare contextToExternalDbType: ExternalDbTypeFn;

  public declare apiGateway;

  public declare reloadEnvVariables;

  public constructor(
    opts: CreateOptions = {},
    systemOptions?: SystemOptions,
  ) {
    // disable telemetry while testing
    super({ ...opts, telemetry: false, }, systemOptions);
  }

  public startScheduledRefreshTimer() {
    // disabling interval
    return null;
  }
}

const conf = {
  apiSecret: 'testApiSecretToSuppressWarning',
  logger: (_msg: string) => {
    // noop
  },
  externalDbType: <DatabaseType>'postgres',
  externalDriverFactory: async () => <OriginalBaseDriver>({
    testConnection: async () => undefined,
  }),
  orchestratorOptions: () => ({}),
};

describe('OptsHandler class', () => {
  afterEach(() => {
    delete process.env.CUBEJS_DEV_MODE;
    delete process.env.CUBEJS_DB_TYPE;
    // The variable and the module-level latch behind it. Clearing only the variable
    // leaves userPreAggregationsSchema() comparing against the previous case's pin,
    // so a case using a schema an earlier one pinned would silently stop testing itself
    releasePreAggregationsSchemaPin();
    delete process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA;
  });

  test('must throw if CreateOptions.dbType is specified', () => {
    expect(() => new CubejsServerCoreExposed(<any>{
      ...conf,
      dbType: (() => 'postgres'),
    })).toThrow('CreateOptions.dbType was removed in v1.7.0');
  });

  test('must handle vanilla CreateOptions', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    // Case 1
    {
      const core = new CubejsServerCoreExposed({
        ...conf,
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
    }

    class MockDriver extends OriginalBaseDriver {
      public readonly dbType: string;

      public readonly dataSource: string;

      public constructor(dbType: string, dataSource = 'default') {
        super();
        this.dbType = dbType;
        this.dataSource = dataSource;
      }

      public async testConnection() {
        // nothing
      }

      public async release() {
        // nothing
      }

      public async query() {
        return [];
      }
    }

    const createMockDriver = (dataSource: string) => new MockDriver('postgres', dataSource);

    // Case 2
    {
      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: ({ dataSource }) => createMockDriver(dataSource),
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
        JSON.stringify(createMockDriver('default')),
      );
    }

    // Case 3
    {
      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: ({ dataSource }) => createMockDriver(dataSource),
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
        JSON.stringify(createMockDriver('default')),
      );
    }

    // Case 4
    {
      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: async ({ dataSource }) => createMockDriver(dataSource),
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
        JSON.stringify(createMockDriver('default')),
      );
    }
  });

  test('must handle valid CreateOptions', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    let core;

    // Case 1
    core = new CubejsServerCoreExposed({
      ...conf,
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

    // Case 2
    core = new CubejsServerCoreExposed({
      ...conf,
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

    // Case 3
    core = new CubejsServerCoreExposed({
      ...conf,
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

    // Case 4
    core = new CubejsServerCoreExposed({
      ...conf,
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
  });

  test('must throw if CreateOptions invalid', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    let core;

    // Case 1
    await expect(async () => {
      core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: (() => true) as unknown as DriverFactoryFn,
      });
      await core.options.driverFactory(<DriverContext>{ dataSource: 'default' });
    }).rejects.toThrow(
      'Unexpected CreateOptions.driverFactory result value. ' +
      'Must be either DriverConfig or driver instance: <boolean>true'
    );

    // Case 2
    await expect(async () => {
      core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: 1 as unknown as DriverFactoryFn,
      });
      await core.options.driverFactory(<DriverContext>{ dataSource: 'default' });
    }).rejects.toThrow(
      'Invalid cube-server-core options: "driverFactory" must be of type function'
    );

    // Case 3
    expect(() => {
      process.env.CUBEJS_DB_TYPE = undefined;
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      core = new CubejsServerCoreExposed({
        ...conf,
        apiSecret: undefined,
        driverFactory: undefined,
      });
    }).toThrow(
      'apiSecret is required option(s)'
    );

    // Case 4
    expect(() => {
      delete process.env.CUBEJS_DB_TYPE;
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      core = new CubejsServerCoreExposed({
        ...conf,
        apiSecret: 'apiSecret',
        driverFactory: undefined,
      });
    }).toThrow(
      'Either CUBEJS_DB_TYPE or CreateOptions.driverFactory must be specified'
    );
  });

  test('must configure/reconfigure contextToDbType', async () => {
    // Outside of dev mode CUBEJS_DB_TYPE or a driverFactory is required upfront,
    // and this case is about resolving the type after the instance was created
    process.env.CUBEJS_DEV_MODE = 'true';

    const core = new CubejsServerCoreExposed({
      ...conf,
      driverFactory: undefined,
    });

    process.env.CUBEJS_DB_TYPE = 'postgres';
    expect(await core.contextToDbType({} as DriverContext)).toEqual('postgres');

    process.env.CUBEJS_DB_TYPE = 'mysql';
    core.reloadEnvVariables();
    expect(await core.contextToDbType({} as DriverContext)).toEqual('mysql');

    process.env.CUBEJS_DB_TYPE = 'postgres';
    core.reloadEnvVariables();
    expect(await core.contextToDbType({} as DriverContext)).toEqual('postgres');
  });

  test('must treat CreateOptions.devServer as dev mode without CUBEJS_DEV_MODE', async () => {
    // The gateway resolves devServer the same way, so server-core must not answer
    // "production" for an instance whose playground it just mounted
    process.env.CUBEJS_DB_TYPE = 'postgres';

    // `conf` pins externalDbType, which would mask the default this asserts
    const { externalDbType, externalDriverFactory, ...confWithoutExternal } = conf;

    const core = new CubejsServerCoreExposed({
      ...confWithoutExternal,
      devServer: true,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    expect(core.options.devServer).toBe(true);
    expect(core.options.preAggregationsSchema).toEqual('dev_pre_aggregations');
    // A driver reads the variable and falls back to CUBEJS_DEV_MODE, which is unset
    // here, so without the pin DatabricksDriver would answer `prod_pre_aggregations`
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('dev_pre_aggregations');
    // Without this the instance gets no external DB at all, so the first
    // pre-aggregation build fails with `externalDriverFactory is not provided`
    expect(core.options.externalDbType).toEqual('cubestore');
  });

  test('must not treat an explicit devServer: false as dev mode', async () => {
    process.env.CUBEJS_DEV_MODE = 'true';
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const core = new CubejsServerCoreExposed({
      ...conf,
      devServer: false,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    expect(core.options.devServer).toBe(false);
    expect(core.options.preAggregationsSchema).toEqual('prod_pre_aggregations');
    // The mirror of the case above, and the one master kept in step: the variable says
    // dev mode, the option overrules it, and a driver left on the variable would build
    // its catalog-qualifying regex from `dev_pre_aggregations` while this instance
    // names `prod_pre_aggregations` in the statement
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('prod_pre_aggregations');
  });

  test('must leave an explicit CUBEJS_PRE_AGGREGATIONS_SCHEMA alone', async () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA = 'my_schema';
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const core = new CubejsServerCoreExposed({
      ...conf,
      devServer: true,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    expect(core.options.preAggregationsSchema).toEqual('my_schema');
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('my_schema');
  });

  test('must pin CreateOptions.preAggregationsSchema, not the default it overrides', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const core = new CubejsServerCoreExposed({
      ...conf,
      preAggregationsSchema: 'analytics_preaggs',
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    // `...opts` wins over the dev/prod default, so pinning before the merge would put a
    // driver on `prod_pre_aggregations` while this instance names `analytics_preaggs`
    expect(core.options.preAggregationsSchema).toEqual('analytics_preaggs');
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('analytics_preaggs');
  });

  test('must not pin a per-tenant preAggregationsSchema function', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const core = new CubejsServerCoreExposed({
      ...conf,
      preAggregationsSchema: (ctx) => `preaggs_${ctx.securityContext?.tenantId}`,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    // No single schema to pin, so the variable is left unset and a driver falls back to
    // its own CUBEJS_DEV_MODE reading. Pinning any one tenant's schema would be worse
    expect(typeof core.options.preAggregationsSchema).toEqual('function');
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toBeUndefined();
  });

  test('must not pin for a construction that throws', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    // Reaches initializeCoreOptions and then fails its required-option check. A pin
    // taken before that has no owner: shutdown never runs, so nothing releases it, and
    // the next instance is told to align with a schema a dead attempt chose
    expect(() => new CubejsServerCoreExposed({
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    })).toThrow('required option(s)');

    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toBeUndefined();
  });

  test('must not pin for a throw after the options are resolved', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const { externalDbType, externalDriverFactory, ...confWithoutExternal } = conf;

    // Options resolve, and the constructor throws further down. Validating inside
    // OptsHandler is not enough for that reason: the pin has to be the last thing the
    // constructor does, or an attempt that never became an instance holds it forever
    expect(() => new CubejsServerCoreExposed(<CreateOptions>{
      ...confWithoutExternal,
      devServer: true,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
      contextToDataSourceId: () => 'tenant',
    })).toThrow('contextToDataSourceId has been deprecated');

    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toBeUndefined();
  });

  test('must not let a repeated shutdown release another instance\'s share', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const { externalDbType, externalDriverFactory, ...confWithoutExternal } = conf;

    const first = new CubejsServerCoreExposed({
      ...confWithoutExternal,
      devServer: true,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    const second = new CubejsServerCoreExposed({
      ...confWithoutExternal,
      devServer: true,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    await first.shutdown();
    // shutdown() is public and unguarded, so a host that calls it on a signal and again
    // on exit gets here. The pin counts holders rather than naming them, so the second
    // call would spend `second`'s share and delete the variable it is still serving on
    await first.shutdown();

    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('dev_pre_aggregations');

    await second.shutdown();

    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toBeUndefined();
  });

  test('must keep the pin while a second instance on the same schema is up', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const { externalDbType, externalDriverFactory, ...confWithoutExternal } = conf;

    const first = new CubejsServerCoreExposed({
      ...confWithoutExternal,
      devServer: true,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    const second = new CubejsServerCoreExposed({
      ...confWithoutExternal,
      devServer: true,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    await first.shutdown();

    // `second` is still serving, and its driver reads the variable directly: losing it
    // here sends the driver to `prod_pre_aggregations` while the plan names `dev_`
    expect(second.options.preAggregationsSchema).toEqual('dev_pre_aggregations');
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('dev_pre_aggregations');

    await second.shutdown();

    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toBeUndefined();
  });

  test('must let an instance that shut down hand the pin to the next', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const { externalDbType, externalDriverFactory, ...confWithoutExternal } = conf;

    const dev = new CubejsServerCoreExposed({
      ...confWithoutExternal,
      devServer: true,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('dev_pre_aggregations');

    await dev.shutdown();

    // Without the release the next instance's drivers stay on `dev_pre_aggregations`
    // while it names `prod_pre_aggregations` in the statement
    const prod = new CubejsServerCoreExposed({
      ...conf,
      devServer: false,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    expect(prod.options.preAggregationsSchema).toEqual('prod_pre_aggregations');
    expect(process.env.CUBEJS_PRE_AGGREGATIONS_SCHEMA).toEqual('prod_pre_aggregations');
  });

  test('must not let one instance pin the schema for the next', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const { externalDbType, externalDriverFactory, ...confWithoutExternal } = conf;

    const dev = new CubejsServerCoreExposed({
      ...confWithoutExternal,
      devServer: true,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    const prod = new CubejsServerCoreExposed({
      ...conf,
      devServer: false,
      driverFactory: () => ({ type: <DatabaseType>'postgres' }),
    });

    // The pin is a process-wide variable written for drivers, which have no default of
    // their own. Reading it back as if the user had set it would hand the second
    // instance the first's schema, silently overruling its own dev mode
    expect(dev.options.preAggregationsSchema).toEqual('dev_pre_aggregations');
    expect(prod.options.preAggregationsSchema).toEqual('prod_pre_aggregations');
  });

  test('must determine custom drivers from the cube.js file', async () => {
    class BaseDriver {
      public async testConnection() {
        throw new Error('UT exception');
      }

      public async release() {
        //
      }
    }

    class CustomDriver extends BaseDriver {
      //
    }

    process.env.CUBEJS_DB_TYPE = 'postgres';
    process.env.NODE_ENV = 'test';
    process.env.CUBEJS_DEV_MODE = 'true';
    const core = new CubejsServerCoreExposed({
      ...conf,
      apiSecret: '44b87d4309471e5d9d18738450db0e49',
      driverFactory: async () => (new CustomDriver()) as unknown as OriginalBaseDriver,
      orchestratorOptions: {},
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
      JSON.stringify(new CustomDriver()),
    );

    const oapi = (<any> await core.getOrchestratorApi(<RequestContext>{}));
    const opts = oapi.options;
    const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
    oapi.seenDataSources = ['default'];

    expect(core.optsHandler.configuredForScheduledRefresh()).toBe(true);
    expect(opts.rollupOnlyMode).toBe(false);
    expect(opts.preAggregationsOptions.externalRefresh).toBe(false);
    await expect(async () => {
      await oapi.testConnection();
    }).rejects.toThrow('UT exception');
    expect(testDriverConnectionSpy.mock.calls.length).toEqual(2);

    testDriverConnectionSpy.mockRestore();
  });

  test('must determine correct driver type by the query context', async () => {
    class Driver1 extends OriginalBaseDriver {
      public async testConnection() {
        //
      }

      public async release() {
        //
      }

      public query() {
        return Promise.resolve([]);
      }
    }

    class Driver2 extends OriginalBaseDriver {
      public async testConnection() {
        //
      }

      public async release() {
        //
      }

      public query() {
        return Promise.resolve([]);
      }
    }

    process.env.CUBEJS_DB_TYPE = 'postgres';
    process.env.NODE_ENV = 'test';
    process.env.CUBEJS_DEV_MODE = 'true';

    const core = new CubejsServerCoreExposed({
      ...conf,
      apiSecret: '44b87d4309471e5d9d18738450db0e49',
      contextToOrchestratorId: ({ securityContext }) => (
        `ID_${securityContext.tenantId}`
      ),
      driverFactory: ({ securityContext }) => {
        if (securityContext.tenantId === 1) {
          return new Driver1();
        } else if (securityContext.tenantId === 2) {
          return new Driver2();
        } else {
          return new Driver2();
        }
      },
    });

    const oapi1 = (<any> await core.getOrchestratorApi({
      authInfo: {},
      securityContext: { tenantId: 1 },
      requestId: '1',
    }));
    oapi1.seenDataSources = ['default'];
    const driver11 = await oapi1.driverFactory('default');
    const driver12 = await oapi1.driverFactory('default');
    expect(driver11 instanceof Driver1).toBeTruthy();
    expect(driver12 instanceof Driver1).toBeTruthy();

    const oapi2 = (<any> await core.getOrchestratorApi({
      authInfo: {},
      securityContext: { tenantId: 2 },
      requestId: '2',
    }));
    oapi2.seenDataSources = ['default'];
    const driver21 = await oapi2.driverFactory('default');
    const driver22 = await oapi2.driverFactory('default');
    expect(driver21 instanceof Driver2).toBeTruthy();
    expect(driver22 instanceof Driver2).toBeTruthy();
  });

  test(
    'must configure queueOptions without orchestratorOptions, ' +
    'without CUBEJS_CONCURRENCY and without default driver concurrency',
    async () => {
      delete process.env.CUBEJS_CONCURRENCY;
      process.env.CUBEJS_DB_TYPE = 'cubestore';

      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: {},
      });

      const opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;

      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: 5,
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency: 5,
      });
    }
  );

  test(
    'must configure queueOptions with empty orchestratorOptions object, ' +
    'without CUBEJS_CONCURRENCY and without default driver concurrency',
    async () => {
      delete process.env.CUBEJS_CONCURRENCY;
      process.env.CUBEJS_DB_TYPE = 'cubestore';

      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: {},
      });

      const opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;

      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: 5,
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency: 5,
      });
    }
  );

  test(
    'must configure queueOptions with empty orchestratorOptions function, ' +
    'without CUBEJS_CONCURRENCY and without default driver concurrency',
    async () => {
      delete process.env.CUBEJS_CONCURRENCY;
      process.env.CUBEJS_DB_TYPE = 'cubestore';

      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: () => ({}),
      });

      const opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;

      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: 5,
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency: 5,
      });
    }
  );

  test(
    'must configure queueOptions with empty orchestratorOptions function, ' +
    'without CUBEJS_CONCURRENCY and with default driver concurrency',
    async () => {
      delete process.env.CUBEJS_CONCURRENCY;
      process.env.CUBEJS_DB_TYPE = 'postgres';

      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: () => ({}),
      });

      const opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;

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
    }
  );

  test(
    'must configure queueOptions with empty orchestratorOptions function, ' +
    'with CUBEJS_CONCURRENCY and with default driver concurrency',
    async () => {
      process.env.CUBEJS_CONCURRENCY = '10';
      process.env.CUBEJS_DB_TYPE = 'postgres';

      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: () => ({}),
      });

      const opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;

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
    }
  );

  test(
    'must configure queueOptions with empty orchestratorOptions function, ' +
    'with CUBEJS_REFRESH_WORKER_CONCURRENCY, CUBEJS_CONCURRENCY and with default driver concurrency',
    async () => {
      process.env.CUBEJS_CONCURRENCY = '11';
      process.env.CUBEJS_REFRESH_WORKER_CONCURRENCY = '22';
      process.env.CUBEJS_DB_TYPE = 'postgres';

      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: () => ({}),
      });

      const opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;

      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: parseInt(process.env.CUBEJS_CONCURRENCY, 10),
      });

      expect(opts.preAggregationsOptions.queueOptions).toBeDefined();
      expect(typeof opts.preAggregationsOptions.queueOptions).toEqual('function');
      expect(await opts.preAggregationsOptions.queueOptions()).toEqual({
        concurrency: parseInt(process.env.CUBEJS_REFRESH_WORKER_CONCURRENCY, 10),
      });

      delete process.env.CUBEJS_CONCURRENCY;
      delete process.env.CUBEJS_REFRESH_WORKER_CONCURRENCY;
      delete process.env.CUBEJS_DB_TYPE;
    }
  );

  test(
    'multi data source concurrency',
    async () => {
      process.env.CUBEJS_DATASOURCES = 'default,postgres';
      process.env.CUBEJS_DS_POSTGRES_CONCURRENCY = '10';
      process.env.CUBEJS_DS_POSTGRES_DB_TYPE = 'postgres';
      process.env.CUBEJS_DB_TYPE = 'postgres';

      const core = new CubejsServerCoreExposed({
        ...conf,
        driverFactory: () => ({ type: <DatabaseType>process.env.CUBEJS_DB_TYPE }),
        orchestratorOptions: () => ({}),
      });

      const opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;

      expect(opts.queryCacheOptions.queueOptions).toBeDefined();
      expect(typeof opts.queryCacheOptions.queueOptions).toEqual('function');
      expect(await opts.queryCacheOptions.queueOptions()).toEqual({
        concurrency: 2,
      });
      expect(await opts.queryCacheOptions.queueOptions('postgres')).toEqual({
        concurrency: 10,
      });

      delete process.env.CUBEJS_DATASOURCES;
      delete process.env.CUBEJS_DS_POSTGRES_CONCURRENCY;
      delete process.env.CUBEJS_DS_POSTGRES_DB_TYPE;
      delete process.env.CUBEJS_DB_TYPE;
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
        ...conf,
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

      const opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;

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
    }
  );

  test('must configure driver pool', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    const testConnectionTimeout = 60000;
    const concurrency1 = 15;
    const concurrency2 = 25;
    let core;
    let opts;
    let driver;

    // Case 1
    core = new CubejsServerCoreExposed({
      ...conf,
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
        testConnectionTimeout,
      }),
    });
    opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;
    driver = <any>(await core.resolveDriver(<DriverContext>{}, opts));

    expect(driver.pool.options.max).toEqual(2 * (concurrency1 + concurrency2));
    expect(driver.testConnectionTimeout()).toEqual(testConnectionTimeout);

    // Case 2
    core = new CubejsServerCoreExposed({
      ...conf,
      driverFactory: () => ({
        type: <DatabaseType>process.env.CUBEJS_DB_TYPE,
        testConnectionTimeout,
      }),
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
    opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;
    driver = <any>(await core.resolveDriver(<DriverContext>{}));

    expect(driver.pool.options.max).toEqual(8);
    expect(driver.testConnectionTimeout()).toEqual(testConnectionTimeout);

    // Case 3
    core = new CubejsServerCoreExposed({
      ...conf,
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
    opts = (<any> await core.getOrchestratorApi(<RequestContext>{})).options;
    driver = <any>(await core.resolveDriver(<DriverContext>{}));

    expect(driver.pool.options.max).toEqual(8);
    expect(driver.testConnectionTimeout()).toEqual(10000);
  });

  test(
    'must set preAggregationsOptions.externalRefresh to false and test ' +
    'driver connection for dev server',
    async () => {
      process.env.NODE_ENV = 'test';
      process.env.CUBEJS_DEV_MODE = 'true';
      const core = new CubejsServerCoreExposed({
        ...conf,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
      });

      const oapi = <any>(await core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(core.optsHandler.configuredForScheduledRefresh()).toBe(true);
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
    'must set preAggregationsOptions.externalRefresh to true and ' +
    'test driver connection for dev server with preAggregationsOptions.' +
    'externalRefresh set to true',
    async () => {
      process.env.NODE_ENV = 'test';
      process.env.CUBEJS_DEV_MODE = 'true';
      const core = new CubejsServerCoreExposed({
        ...conf,
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

      const oapi = <any>(await core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(core.optsHandler.configuredForScheduledRefresh()).toBe(true);
      expect(opts.rollupOnlyMode).toBe(false);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(true);
      await expect(async () => {
        await oapi.testConnection();
      }).rejects.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(2);

      testDriverConnectionSpy.mockRestore();
    }
  );

  test(
    'must set preAggregationsOptions.externalRefresh to false and doesn\'t' +
    'test driver connection for dev server with rollupOnlyMode set to true',
    async () => {
      process.env.NODE_ENV = 'test';
      process.env.CUBEJS_DEV_MODE = 'true';
      const core = new CubejsServerCoreExposed({
        ...conf,
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

      const oapi = <any>(await core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(core.optsHandler.configuredForScheduledRefresh()).toBe(true);
      expect(opts.rollupOnlyMode).toBe(true);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(false);
      expect(async () => {
        await oapi.testConnection();
      }).not.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(1);

      testDriverConnectionSpy.mockRestore();
    }
  );

  test(
    'must set preAggregationsOptions.externalRefresh to false and test ' +
    'driver connection for refresh worker in the production mode',
    async () => {
      process.env.NODE_ENV = 'production';
      process.env.CUBEJS_DEV_MODE = 'false';
      const core = new CubejsServerCoreExposed({
        ...conf,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        scheduledRefreshTimer: true,
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
      });

      const oapi = <any>(await core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(core.optsHandler.configuredForScheduledRefresh()).toBe(true);
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
    'must set preAggregationsOptions.externalRefresh to false and test ' +
    'driver connection for api worker in the production mode if ' +
    'CUBEJS_PRE_AGGREGATIONS_BUILDER is set',
    async () => {
      process.env.NODE_ENV = 'production';
      process.env.CUBEJS_DEV_MODE = 'false';
      process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'true';
      const core = new CubejsServerCoreExposed({
        ...conf,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        scheduledRefreshTimer: false,
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
      });

      const oapi = <any>(await core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(core.optsHandler.configuredForScheduledRefresh()).toBe(false);
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
    'must set preAggregationsOptions.externalRefresh to true and test ' +
    'driver connection for api worker in the production mode if specified in' +
    'preAggregationsOptions.externalRefresh',
    async () => {
      process.env.NODE_ENV = 'production';
      process.env.CUBEJS_DEV_MODE = 'false';
      process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'true';
      const core = new CubejsServerCoreExposed({
        ...conf,
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

      const oapi = <any>(await core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(core.optsHandler.configuredForScheduledRefresh()).toBe(false);
      expect(opts.rollupOnlyMode).toBe(false);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(true);
      await expect(async () => {
        await oapi.testConnection();
      }).rejects.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(2);

      testDriverConnectionSpy.mockRestore();
    }
  );

  test(
    'must set preAggregationsOptions.externalRefresh to true and test ' +
    'driver connection for api worker if CUBEJS_PRE_AGGREGATIONS_BUILDER is unset',
    async () => {
      process.env.NODE_ENV = 'production';
      process.env.CUBEJS_DEV_MODE = 'false';
      process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'false';

      const core = new CubejsServerCoreExposed({
        ...conf,
        apiSecret: '44b87d4309471e5d9d18738450db0e49',
        scheduledRefreshTimer: false,
        driverFactory: () => ({
          type: 'postgres',
          user: 'user',
          password: 'password',
          database: 'database',
        }),
      });

      const oapi = <any>(await core.getOrchestratorApi(<RequestContext>{}));
      const opts = oapi.options;
      const testDriverConnectionSpy = jest.spyOn(oapi, 'testDriverConnection');
      oapi.seenDataSources = ['default'];

      expect(core.optsHandler.configuredForScheduledRefresh()).toBe(false);
      expect(opts.rollupOnlyMode).toBe(false);
      expect(opts.preAggregationsOptions.externalRefresh).toBe(true);
      await expect(async () => {
        await oapi.testConnection();
      }).rejects.toThrow();
      expect(testDriverConnectionSpy.mock.calls.length).toEqual(2);

      testDriverConnectionSpy.mockRestore();
    }
  );

  test('must set default api scopes if fn and env not specified', async () => {
    process.env.NODE_ENV = 'production';
    process.env.CUBEJS_DEV_MODE = 'false';
    process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'false';

    const core = new CubejsServerCoreExposed({
      ...conf,
      apiSecret: '44b87d4309471e5d9d18738450db0e49',
      scheduledRefreshTimer: false,
      driverFactory: () => ({
        type: 'postgres',
        user: 'user',
        password: 'password',
        database: 'database',
      }),
    });

    const gateway = <any>core.apiGateway();
    const permissions = await gateway.contextToApiScopesFn();
    expect(permissions).toBeDefined();
    expect(Array.isArray(permissions)).toBeTruthy();
    expect(permissions).toEqual(['graphql', 'meta', 'data', 'sql']);
  });

  test('must set env api scopes if fn not specified', async () => {
    process.env.NODE_ENV = 'production';
    process.env.CUBEJS_DEV_MODE = 'false';
    process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'false';
    process.env.CUBEJS_DEFAULT_API_SCOPES = 'graphql,meta';

    const core = new CubejsServerCoreExposed({
      ...conf,
      apiSecret: '44b87d4309471e5d9d18738450db0e49',
      scheduledRefreshTimer: false,
      driverFactory: () => ({
        type: 'postgres',
        user: 'user',
        password: 'password',
        database: 'database',
      }),
    });

    const gateway = <any>core.apiGateway();
    const permissions = await gateway.contextToApiScopesFn();

    expect(permissions).toBeDefined();
    expect(Array.isArray(permissions)).toBeTruthy();
    expect(permissions).toEqual(['graphql', 'meta']);

    delete process.env.CUBEJS_DEFAULT_API_SCOPES;
  });

  test('must throw if contextToApiScopes returns wrong type', async () => {
    process.env.NODE_ENV = 'production';
    process.env.CUBEJS_DEV_MODE = 'false';
    process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'false';

    type ApiScopes =
      'graphql' |
      'meta' |
      'data' |
      'jobs';
    const core = new CubejsServerCoreExposed({
      ...conf,
      apiSecret: '44b87d4309471e5d9d18738450db0e49',
      scheduledRefreshTimer: false,
      driverFactory: () => ({
        type: 'postgres',
        user: 'user',
        password: 'password',
        database: 'database',
      }),
      contextToApiScopes: async () => new Promise((resolve) => {
        resolve('jobs' as unknown as ApiScopes[]);
      }),
    });

    const gateway = <any>core.apiGateway();
    await expect(async () => gateway.contextToApiScopesFn()).rejects.toThrow(
      'A user-defined contextToApiScopes function returns an inconsistent type.'
    );
  });

  test('must throw if contextToApiScopes returns wrong permission value', async () => {
    process.env.NODE_ENV = 'production';
    process.env.CUBEJS_DEV_MODE = 'false';
    process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'false';

    type ApiScopes =
      'graphql' |
      'meta' |
      'data' |
      'jobs';
    const core = new CubejsServerCoreExposed({
      ...conf,
      apiSecret: '44b87d4309471e5d9d18738450db0e49',
      scheduledRefreshTimer: false,
      driverFactory: () => ({
        type: 'postgres',
        user: 'user',
        password: 'password',
        database: 'database',
      }),
      contextToApiScopes: async () => new Promise((resolve) => {
        resolve(['graphql', 'meta', 'data', 'job'] as unknown as ApiScopes[]);
      }),
    });

    const gateway = <any>core.apiGateway();
    await expect(async () => gateway.contextToApiScopesFn()).rejects.toThrow(
      'A user-defined contextToApiScopes function returns a wrong scope: job'
    );
  });

  test('must set api scopes if specified', async () => {
    process.env.NODE_ENV = 'production';
    process.env.CUBEJS_DEV_MODE = 'false';
    process.env.CUBEJS_PRE_AGGREGATIONS_BUILDER = 'false';

    const core = new CubejsServerCoreExposed({
      ...conf,
      apiSecret: '44b87d4309471e5d9d18738450db0e49',
      scheduledRefreshTimer: false,
      driverFactory: () => ({
        type: 'postgres',
        user: 'user',
        password: 'password',
        database: 'database',
      }),
      contextToApiScopes: async () => new Promise((resolve) => {
        resolve(['graphql', 'meta', 'data', 'jobs']);
      }),
    });

    const gateway = <any>core.apiGateway();
    const permissions = await gateway.contextToApiScopesFn();
    expect(permissions).toBeDefined();
    expect(Array.isArray(permissions)).toBeTruthy();
    expect(permissions).toEqual(['graphql', 'meta', 'data', 'jobs']);
  });
});

describe('OptsHandler timezone env validation', () => {
  beforeEach(() => {
    process.env.CUBEJS_DB_TYPE = 'postgres';
  });

  afterEach(() => {
    delete process.env.CUBEJS_DEFAULT_TIMEZONE;
    delete process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES;
  });

  test('must throw at construction if CUBEJS_DEFAULT_TIMEZONE is not a valid zone', () => {
    process.env.CUBEJS_DEFAULT_TIMEZONE = 'Europ/Berlin';

    expect(() => new CubejsServerCoreExposed(conf))
      .toThrow(/CUBEJS_DEFAULT_TIMEZONE/);
  });

  test('must construct when CUBEJS_DEFAULT_TIMEZONE is unset or valid in any case', () => {
    delete process.env.CUBEJS_DEFAULT_TIMEZONE;
    expect(() => new CubejsServerCoreExposed(conf)).not.toThrow();

    process.env.CUBEJS_DEFAULT_TIMEZONE = 'america/new_york';
    expect(() => new CubejsServerCoreExposed(conf)).not.toThrow();
  });

  test('must throw at construction if CUBEJS_SCHEDULED_REFRESH_TIMEZONES has an invalid entry', () => {
    process.env.CUBEJS_SCHEDULED_REFRESH_TIMEZONES = 'UTC,Nope/Zone';

    expect(() => new CubejsServerCoreExposed(conf))
      .toThrow(/CUBEJS_SCHEDULED_REFRESH_TIMEZONES/);
  });

  test('must throw at construction if CreateOptions.scheduledRefreshTimeZones has an invalid entry', () => {
    expect(() => new CubejsServerCoreExposed({
      ...conf,
      scheduledRefreshTimeZones: ['UTC', 'Nope/Zone'],
    })).toThrow(/valid IANA time zone name/);
  });

  test('must canonicalize CreateOptions.scheduledRefreshTimeZones', () => {
    const core = new CubejsServerCoreExposed({
      ...conf,
      scheduledRefreshTimeZones: ['utc', 'america/new_york'],
    });

    expect(core.options.scheduledRefreshTimeZones).toEqual(['UTC', 'America/New_York']);
  });
});

describe('OptsHandler compilerCacheSize', () => {
  beforeEach(() => {
    process.env.CUBEJS_DB_TYPE = 'postgres';
  });

  afterEach(() => {
    delete process.env.CUBEJS_COMPILER_CACHE_SIZE;
  });

  test('must default to 250 when neither the option nor the env variable is set', () => {
    const core = new CubejsServerCoreExposed(conf);

    expect(core.options.compilerCacheSize).toBe(250);
  });

  test('must take the value from CUBEJS_COMPILER_CACHE_SIZE', () => {
    process.env.CUBEJS_COMPILER_CACHE_SIZE = '42';

    const core = new CubejsServerCoreExposed(conf);

    expect(core.options.compilerCacheSize).toBe(42);
  });

  test('must prefer CreateOptions.compilerCacheSize over the env variable', () => {
    process.env.CUBEJS_COMPILER_CACHE_SIZE = '42';

    const core = new CubejsServerCoreExposed({
      ...conf,
      compilerCacheSize: 7,
    });

    expect(core.options.compilerCacheSize).toBe(7);
  });

  test('must throw at construction if CUBEJS_COMPILER_CACHE_SIZE is not a valid size', () => {
    process.env.CUBEJS_COMPILER_CACHE_SIZE = 'abc';

    expect(() => new CubejsServerCoreExposed(conf))
      .toThrow(/CUBEJS_COMPILER_CACHE_SIZE/);
  });
});

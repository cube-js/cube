/* globals describe, jest, beforeEach, test, expect */

import {
  CreateOptions,
  LoggerFn,
  DbTypeFn,
  DriverFactoryFn,
  DriverContext,
  DatabaseType,
  DriverConfig,
  OrchestratorOptions,
  ServerCoreInitializedOptions,
} from '../../src/core/types';
import { CubejsServerCore } from '../../src/core/server';

import { OptsHandler } from '../../src/core/OptsHandler';

class CubejsServerCoreExposed extends CubejsServerCore {
  public options: ServerCoreInitializedOptions;
}

let message: string;
let parameters: Record<string, any>;

const logger = (msg: string, params: Record<string, any>) => {
  message = msg;
  parameters = params;
};

describe('OptsHandler class', () => {
  beforeEach(() => {
    message = '';
    parameters = {};
  });

  test('deprecation warning must be printed if dbType was specified', () => {
    const core = new CubejsServerCore({
      logger,
      dbType: ((context: DriverContext) => 'postgres'),
    });
    expect(message).toEqual('Cube.js `CreateOptions.dbType` Property Deprecation');
  });
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
  });

  test('must throw if CreateOptions invalid', async () => {
    process.env.CUBEJS_DB_TYPE = 'postgres';

    // Case 1
    await expect(async () => {
      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: (() => true) as unknown as DriverFactoryFn,
      });
      await core.options.driverFactory(<DriverContext>{ dataSource: 'default' });
    }).rejects.toThrow(
      'Unexpected CreateOptions.driverFactory result value. ' +
      'Must be either DriverConfig or driver instance: <boolean>true'
    );

    // Case 2
    await expect(async () => {
      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: 1 as unknown as DriverFactoryFn,
      });
      await core.options.driverFactory(<DriverContext>{ dataSource: 'default' });
    }).rejects.toThrow(
      'Invalid cube-server-core options: child "driverFactory" fails because ' +
      '["driverFactory" must be a Function]'
    );

    // Case 3
    await expect(async () => {
      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: () => CubejsServerCore.createDriver('postgres'),
      });
      await core.options.driverFactory(<DriverContext>{ dataSource: 'default' });
    }).rejects.toThrow(
      'CreateOptions.dbType is required if CreateOptions.driverFactory ' +
      'returns driver instance'
    );

    // Case 4
    await expect(async () => {
      const core = new CubejsServerCoreExposed({
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

    // Case 4
    await expect(async () => {
      const core = new CubejsServerCoreExposed({
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

    // Case 5
    expect(() => {
      process.env.CUBEJS_DB_TYPE = undefined;
      process.env.NODE_ENV = 'production';
      const core = new CubejsServerCoreExposed({
        logger,
        dbType: undefined,
        driverFactory: undefined,
      });
    }).toThrow(
      'apiSecret is required option(s)'
    );

    // Case 6
    expect(() => {
      delete process.env.CUBEJS_DB_TYPE;
      process.env.NODE_ENV = 'production';
      const core = new CubejsServerCoreExposed({
        logger,
        apiSecret: 'apiSecret',
        dbType: undefined,
        driverFactory: undefined,
      });
    }).toThrow(
      'Either CUBEJS_DB_TYPE, CreateOptions.dbType or ' +
      'CreateOptions.driverFactory must be specified'
    );
  });

  // test('must calculate driver max pool', () => {
  //   expect(
  //     driverService.getDriverMaxPool(<DriverContext>{})
  //   ).toBeUndefined();

  //   expect(
  //     driverService.getDriverMaxPool(
  //       <DriverContext>{},
  //       {
  //         queryCacheOptions: {
  //           queueOptions: () => ({
  //             concurrency: 1,
  //           }),
  //         },
  //         preAggregationsOptions: {
  //           queueOptions: () => ({
  //             concurrency: 2,
  //           }),
  //         },
  //       },
  //     )
  //   ).toEqual(6);

  //   expect(
  //     driverService.getDriverMaxPool(
  //       <DriverContext>{},
  //       {
  //         queryCacheOptions: {
  //           queueOptions: () => ({
  //             concurrency: 3,
  //           }),
  //         },
  //         preAggregationsOptions: {
  //           queueOptions: () => ({
  //             concurrency: 4,
  //           }),
  //         },
  //       },
  //     )
  //   ).toEqual(14);
  // });

  // test('must return driver concurrency value if specified', () => {
  //   driverService.decorateOpts(<CreateOptions>{
  //     dbType: undefined,
  //     driverFactory: () => ({
  //       type: 'postgres',
  //       options: {},
  //     }),
  //   });
  //   expect(
  //     driverService.getDriverConcurrency(<DriverContext>{}),
  //   ).toEqual(2);

  //   driverService.decorateOpts(<CreateOptions>{
  //     dbType: undefined,
  //     driverFactory: () => ({
  //       type: 'cubestore',
  //       options: {},
  //     }),
  //   });
  //   expect(
  //     driverService.getDriverConcurrency(<DriverContext>{}),
  //   ).toBeUndefined();
  // });

  // test('must resolve driver', async () => {
  //   // Case 1
  //   driverService.decorateOpts(<CreateOptions>{
  //     dbType: undefined,
  //     driverFactory: () => ({
  //       type: 'postgres',
  //       options: {},
  //     }),
  //   });
  //   expect(
  //     JSON.stringify(await driverService.resolveDriver(<DriverContext>{})),
  //   ).toEqual(
  //     JSON.stringify(CubejsServerCore.createDriver('postgres')),
  //   );

  //   // Case 2
  //   driverService.decorateOpts(<CreateOptions>{
  //     dbType: 'postgres',
  //     driverFactory: () => CubejsServerCore.createDriver('postgres'),
  //   });
  //   expect(
  //     JSON.stringify(await driverService.resolveDriver(<DriverContext>{})),
  //   ).toEqual(
  //     JSON.stringify(CubejsServerCore.createDriver('postgres')),
  //   );

  //   // Case 3
  //   driverService.decorateOpts(<CreateOptions>{
  //     dbType: 'postgres',
  //     driverFactory: async () => CubejsServerCore.createDriver('postgres'),
  //   });
  //   expect(
  //     JSON.stringify(await driverService.resolveDriver(<DriverContext>{})),
  //   ).toEqual(
  //     JSON.stringify(CubejsServerCore.createDriver('postgres')),
  //   );
  // });
});
